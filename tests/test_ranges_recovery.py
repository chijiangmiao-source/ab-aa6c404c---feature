"""Byte-range durability: restart recovery, crash commit points, old-volume upgrade."""

from __future__ import annotations

import hashlib
import sqlite3
from datetime import datetime, timedelta, timezone

from fastapi.testclient import TestClient

from app.bitmap import new_bitmap, set_bit
from app.config import Settings
from app.main import create_app


def make_bytes(size: int, seed: str = "payload") -> bytes:
    out = bytearray()
    counter = 0
    while len(out) < size:
        out += hashlib.sha256(f"{seed}:{counter}".encode()).digest()
        counter += 1
    return bytes(out[:size])


def sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def create_session(client, payload: bytes, chunk_size: int, *, expires_at=None) -> dict:
    resp = client.post(
        "/sessions",
        json={
            "file_size": len(payload),
            "chunk_size": chunk_size,
            "file_sha256": sha256(payload),
            "expires_at": expires_at
            or (datetime.now(timezone.utc) + timedelta(hours=1)).isoformat(),
        },
    )
    assert resp.status_code == 201, resp.text
    return resp.json()


def put_chunk(client, sid, index, body):
    return client.put(
        f"/sessions/{sid}/chunks/{index}", content=body, headers={"X-Chunk-SHA256": sha256(body)}
    )


def put_range(client, sid, start, end, payload, *, body=None):
    data = payload[start : end + 1] if body is None else body
    return client.put(
        f"/sessions/{sid}/ranges",
        content=data,
        headers={"Content-Range": f"bytes {start}-{end}/{len(payload)}", "X-Range-SHA256": sha256(data)},
    )


# ---------- restart ----------

def test_restart_keeps_segments_and_resumes(data_dir):
    payload = make_bytes(500)
    app1 = create_app(Settings(data_dir=data_dir))
    with TestClient(app1) as c1:
        sid = create_session(c1, payload, 100)["session_id"]
        assert put_range(c1, sid, 0, 149, payload).status_code == 201
        assert put_chunk(c1, sid, 3, payload[300:400]).status_code == 201

    app2 = create_app(Settings(data_dir=data_dir))
    with TestClient(app2) as c2:
        status = c2.get(f"/sessions/{sid}").json()
        assert status["missing_ranges"] == [{"start": 150, "end": 299}, {"start": 400, "end": 499}]
        assert status["missing_chunks"] == [1, 2, 4]
        assert put_range(c2, sid, 150, 299, payload).status_code == 201
        assert put_range(c2, sid, 400, 499, payload).status_code == 201
        resp = c2.post(f"/sessions/{sid}/finalize")
        assert resp.status_code == 200
        assert c2.get(f"/sessions/{sid}/artifact").content == payload


def test_restart_cleans_uncommitted_segment_files(data_dir):
    payload = make_bytes(300)
    app1 = create_app(Settings(data_dir=data_dir))
    with TestClient(app1) as c1:
        sid = create_session(c1, payload, 100)["session_id"]
        assert put_range(c1, sid, 0, 49, payload).status_code == 201

    seg_dir = data_dir / "segments" / sid
    committed = list(seg_dir.glob("*.seg"))
    assert len(committed) == 1
    # crash leftovers: a segment file that never reached the DB, plus a temp file
    (seg_dir / "deadbeef.seg").write_bytes(payload[50:100])
    (seg_dir / ".partial.tmp").write_bytes(b"junk")

    app2 = create_app(Settings(data_dir=data_dir))
    with TestClient(app2) as c2:
        status = c2.get(f"/sessions/{sid}").json()
        # uncommitted bytes must not enter coverage
        assert status["missing_ranges"] == [{"start": 50, "end": 299}]
        assert not (seg_dir / "deadbeef.seg").exists()
        assert not (seg_dir / ".partial.tmp").exists()
        assert committed[0].exists()
        # the retried upload commits normally
        assert put_range(c2, sid, 50, 99, payload).status_code == 201


def test_committed_range_replays_as_duplicate_after_restart(data_dir):
    payload = make_bytes(300)
    app1 = create_app(Settings(data_dir=data_dir))
    with TestClient(app1) as c1:
        sid = create_session(c1, payload, 100)["session_id"]
        assert put_range(c1, sid, 0, 99, payload).status_code == 201
        # process exits right after the metadata commit: the range is durable

    app2 = create_app(Settings(data_dir=data_dir))
    with TestClient(app2) as c2:
        replay = put_range(c2, sid, 0, 99, payload)
        assert replay.status_code == 200
        assert replay.json()["duplicate"] is True
        status = c2.get(f"/sessions/{sid}").json()
        assert status["missing_ranges"] == [{"start": 100, "end": 299}]


def test_finalize_after_restart_with_mixed_coverage(data_dir):
    payload = make_bytes(250)
    app1 = create_app(Settings(data_dir=data_dir))
    with TestClient(app1) as c1:
        sid = create_session(c1, payload, 100)["session_id"]
        assert put_chunk(c1, sid, 0, payload[0:100]).status_code == 201
        assert put_range(c1, sid, 100, 249, payload).status_code == 201

    app2 = create_app(Settings(data_dir=data_dir))
    with TestClient(app2) as c2:
        resp = c2.post(f"/sessions/{sid}/finalize")
        assert resp.status_code == 200
        assert resp.json()["final_sha256"] == sha256(payload)
        assert c2.get(f"/sessions/{sid}/artifact").content == payload


# ---------- old-volume upgrade ----------

OLD_SCHEMA = """
CREATE TABLE sessions (
    session_id    TEXT PRIMARY KEY,
    file_size     INTEGER NOT NULL,
    chunk_size    INTEGER NOT NULL,
    total_chunks  INTEGER NOT NULL,
    file_sha256   TEXT NOT NULL,
    status        TEXT NOT NULL DEFAULT 'active',
    bitmap        BLOB NOT NULL,
    expires_at    TEXT NOT NULL,
    created_at    TEXT NOT NULL,
    completed_at  TEXT,
    final_sha256  TEXT,
    artifact_path TEXT
);
CREATE TABLE chunks (
    session_id  TEXT NOT NULL,
    chunk_index INTEGER NOT NULL,
    size        INTEGER NOT NULL,
    sha256      TEXT NOT NULL,
    path        TEXT NOT NULL,
    received_at TEXT NOT NULL,
    PRIMARY KEY (session_id, chunk_index)
);
"""


def _old_insert_session(conn, data_dir, sid, payload, chunk_size, confirmed, *, expired=False, completed=False):
    total = -(-len(payload) // chunk_size)
    bitmap = new_bitmap(total)
    chunk_dir = data_dir / "chunks" / sid
    chunk_dir.mkdir(parents=True, exist_ok=True)
    now = datetime.now(timezone.utc)
    expires = now + timedelta(hours=-1 if expired else 1)
    for i in confirmed:
        body = payload[i * chunk_size : (i + 1) * chunk_size]
        path = chunk_dir / f"{i:08d}.chunk"
        path.write_bytes(body)
        set_bit(bitmap, i)
        conn.execute(
            "INSERT INTO chunks (session_id, chunk_index, size, sha256, path, received_at)"
            " VALUES (?,?,?,?,?,?)",
            (sid, i, len(body), sha256(body), str(path), now.isoformat()),
        )
    artifact_path = None
    if completed:
        artifact_path = str(data_dir / "artifacts" / f"{sid}.bin")
        (data_dir / "artifacts").mkdir(parents=True, exist_ok=True)
        (data_dir / "artifacts" / f"{sid}.bin").write_bytes(payload)
    conn.execute(
        "INSERT INTO sessions (session_id, file_size, chunk_size, total_chunks, file_sha256,"
        " status, bitmap, expires_at, created_at, completed_at, final_sha256, artifact_path)"
        " VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
        (
            sid,
            len(payload),
            chunk_size,
            total,
            sha256(payload),
            "completed" if completed else "active",
            bytes(bitmap),
            expires.isoformat(),
            now.isoformat(),
            now.isoformat() if completed else None,
            sha256(payload) if completed else None,
            artifact_path,
        ),
    )


def test_upgrade_from_old_volume(data_dir):
    """A pre-upgrade data dir keeps working without re-upload or bulk rewriting."""
    active_payload = make_bytes(250)
    expired_payload = make_bytes(200, "expired")
    done_payload = make_bytes(150, "done")
    active_sid, expired_sid, done_sid = "a" * 32, "b" * 32, "c" * 32

    (data_dir / "chunks").mkdir(parents=True)
    conn = sqlite3.connect(data_dir / "db.sqlite3")
    conn.executescript(OLD_SCHEMA)
    _old_insert_session(conn, data_dir, active_sid, active_payload, 100, [0, 2])
    _old_insert_session(conn, data_dir, expired_sid, expired_payload, 100, [0], expired=True)
    _old_insert_session(conn, data_dir, done_sid, done_payload, 100, [0, 1], completed=True)
    conn.commit()
    conn.close()

    app = create_app(Settings(data_dir=data_dir))
    with TestClient(app) as client:
        # active session: queryable, resumable with both protocols, publishable
        status = client.get(f"/sessions/{active_sid}").json()
        assert status["status"] == "active"
        assert status["missing_chunks"] == [1]
        assert status["missing_ranges"] == [{"start": 100, "end": 199}]
        assert put_range(client, active_sid, 100, 149, active_payload).status_code == 201
        assert put_chunk(client, active_sid, 1, active_payload[100:200]).status_code == 201
        resp = client.post(f"/sessions/{active_sid}/finalize")
        assert resp.status_code == 200
        assert client.get(f"/sessions/{active_sid}/artifact").content == active_payload

        # expired session: identical replays only
        status = client.get(f"/sessions/{expired_sid}").json()
        assert status["status"] == "expired"
        assert status["missing_ranges"] == [{"start": 100, "end": 199}]
        assert put_chunk(client, expired_sid, 0, expired_payload[0:100]).status_code == 200
        assert put_range(client, expired_sid, 0, 99, expired_payload).status_code == 200
        resp = put_range(client, expired_sid, 100, 199, expired_payload)
        assert resp.status_code == 410
        assert resp.json()["error"]["code"] == "SESSION_EXPIRED"

        # completed session: still downloadable
        status = client.get(f"/sessions/{done_sid}").json()
        assert status["status"] == "completed"
        assert status["missing_ranges"] == []
        download = client.get(f"/sessions/{done_sid}/artifact")
        assert download.status_code == 200
        assert download.content == done_payload

    # the migration added the segments table without touching pre-existing rows
    conn = sqlite3.connect(data_dir / "db.sqlite3")
    assert conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='segments'"
    ).fetchone()
    assert conn.execute("SELECT COUNT(*) FROM sessions").fetchone()[0] == 3
    # 5 pre-existing chunk rows survived the upgrade; +1 was resumed afterwards
    assert conn.execute("SELECT COUNT(*) FROM chunks").fetchone()[0] == 6
    conn.close()


# ---------- large sparse ranges ----------

def test_large_sparse_ranges(data_dir):
    chunk_size = 1024 * 1024
    file_size = 8 * chunk_size + 7777
    payload = make_bytes(file_size, "sparse")
    app = create_app(Settings(data_dir=data_dir))
    with TestClient(app) as client:
        sid = create_session(client, payload, chunk_size)["session_id"]

        spans = []
        off = 13
        while off < file_size - 65536:
            spans.append((off, off + 65535))
            off += 65536 * 3 + 7
        spans.append((0, 0))  # single first byte
        spans.append((file_size - 1, file_size - 1))  # single last byte
        for a, b in spans:
            resp = put_range(client, sid, a, b, payload)
            assert resp.status_code == 201, (a, b, resp.text)

        expected_missing = []
        pos = 0
        for a, b in sorted(spans):
            if a > pos:
                expected_missing.append({"start": pos, "end": a - 1})
            pos = max(pos, b + 1)
        if pos < file_size:
            expected_missing.append({"start": pos, "end": file_size - 1})
        status = client.get(f"/sessions/{sid}").json()
        assert status["missing_ranges"] == expected_missing
        assert status["missing_chunks"] == list(range(9))  # no chunk fully covered yet

        # fill the gaps; each gap is well under the 8 MiB request limit
        for gap in expected_missing:
            resp = put_range(client, sid, gap["start"], gap["end"], payload)
            assert resp.status_code == 201, (gap, resp.text)
        status = client.get(f"/sessions/{sid}").json()
        assert status["missing_ranges"] == []
        assert status["missing_chunks"] == []

        resp = client.post(f"/sessions/{sid}/finalize")
        assert resp.status_code == 200
        download = client.get(f"/sessions/{sid}/artifact")
        assert sha256(download.content) == sha256(payload)
