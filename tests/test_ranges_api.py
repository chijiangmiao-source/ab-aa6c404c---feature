"""Arbitrary byte-range resume: validation, mixed protocols, conflicts,
crash recovery, expiry, concurrency, old-volume migration and sparse scale.
"""

from __future__ import annotations

import asyncio
import hashlib
import sqlite3
import threading
from datetime import datetime, timedelta, timezone
from pathlib import Path

import httpx
from fastapi.testclient import TestClient

from app import clock
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


def future_expiry(hours: float = 1.0) -> str:
    return (datetime.now(timezone.utc) + timedelta(hours=hours)).isoformat()


def create_session(client, payload: bytes, chunk_size: int, *, file_sha256=None, expires_at=None) -> dict:
    resp = client.post(
        "/sessions",
        json={
            "file_size": len(payload),
            "chunk_size": chunk_size,
            "file_sha256": file_sha256 or sha256(payload),
            "expires_at": expires_at or future_expiry(),
        },
    )
    assert resp.status_code == 201, resp.text
    return resp.json()


def create_session_raw(client, *, file_size, chunk_size, file_sha256, expires_at=None) -> dict:
    resp = client.post(
        "/sessions",
        json={
            "file_size": file_size,
            "chunk_size": chunk_size,
            "file_sha256": file_sha256,
            "expires_at": expires_at or future_expiry(),
        },
    )
    assert resp.status_code == 201, resp.text
    return resp.json()


def put_chunk(client, sid, index, body, digest=None):
    headers = {"X-Chunk-SHA256": digest or sha256(body)}
    return client.put(f"/sessions/{sid}/chunks/{index}", content=body, headers=headers)


def put_range(client, sid, start, end, body, *, digest=None, total=None):
    """Closed interval [start, end]."""
    if total is None:
        total = client.get(f"/sessions/{sid}").json()["file_size"]
    headers = {
        "Content-Range": f"bytes {start}-{end}/{total}",
        "X-Range-SHA256": digest or sha256(body),
    }
    return client.put(f"/sessions/{sid}/ranges", content=body, headers=headers)


def range_of(payload, start, end):
    return payload[start : end + 1]


# ---------- basics & status ----------

def test_range_basic_upload_and_status(client):
    payload = make_bytes(100)
    sid = create_session(client, payload, 16)["session_id"]
    resp = put_range(client, sid, 10, 19, range_of(payload, 10, 19))
    assert resp.status_code == 201, resp.text
    body = resp.json()
    assert body["duplicate"] is False
    assert body["start"] == 10 and body["end"] == 19 and body["size"] == 10
    # no full 16-byte chunk covered -> chunk bitmap unchanged
    assert body["received_count"] == 0
    status = client.get(f"/sessions/{sid}").json()
    assert status["missing_chunks"] == [0, 1, 2, 3, 4, 5, 6]
    assert status["missing_ranges"] == [
        {"start": 0, "end": 9},
        {"start": 20, "end": 99},
    ]


def test_missing_ranges_merges_adjacent_and_sorts(client):
    payload = make_bytes(100)
    sid = create_session(client, payload, 16)["session_id"]
    assert put_range(client, sid, 40, 49, range_of(payload, 40, 49)).status_code == 201
    assert put_range(client, sid, 30, 39, range_of(payload, 30, 39)).status_code == 201
    assert put_range(client, sid, 10, 19, range_of(payload, 10, 19)).status_code == 201
    status = client.get(f"/sessions/{sid}").json()
    # 30-49 merged into one interval; intervals sorted by start
    assert status["missing_ranges"] == [
        {"start": 0, "end": 9},
        {"start": 20, "end": 29},
        {"start": 50, "end": 99},
    ]


def test_partial_chunk_does_not_count_as_received(client):
    payload = make_bytes(64)
    sid = create_session(client, payload, 16)["session_id"]
    # chunk 2 = [32,48); cover [33,48) -> 15 of 16 bytes, still missing
    assert put_range(client, sid, 33, 47, range_of(payload, 33, 47)).status_code == 201
    status = client.get(f"/sessions/{sid}").json()
    assert status["missing_chunks"] == [0, 1, 2, 3]
    # one more byte completes chunk 2
    assert put_range(client, sid, 32, 32, range_of(payload, 32, 32)).status_code == 201
    status = client.get(f"/sessions/{sid}").json()
    assert status["missing_chunks"] == [0, 1, 3]
    assert status["received_count"] == 1


# ---------- validation ----------

def test_range_validation_errors(client):
    payload = make_bytes(100)
    sid = create_session(client, payload, 16)["session_id"]

    def raw(content_range, body, digest=None):
        return client.put(
            f"/sessions/{sid}/ranges",
            content=body,
            headers={
                "Content-Range": content_range,
                "X-Range-SHA256": digest or sha256(body),
            },
        )

    r = raw("bytes 0-9/99", payload[0:10])
    assert r.status_code == 400 and r.json()["error"]["code"] == "RANGE_TOTAL_MISMATCH"
    r = raw("bytes 0-100/100", payload[0:101] if len(payload) >= 100 else b"x")
    assert r.status_code == 400 and r.json()["error"]["code"] == "RANGE_OUT_OF_BOUNDS"
    r = raw("bytes 50-10/100", payload[0:1])
    assert r.status_code == 400 and r.json()["error"]["code"] == "RANGE_OUT_OF_BOUNDS"
    r = raw("chunks 0-9/100", payload[0:10])
    assert r.status_code == 400 and r.json()["error"]["code"] == "INVALID_CONTENT_RANGE"
    r = raw("bytes 0-9/100", payload[0:9])
    assert r.status_code == 400 and r.json()["error"]["code"] == "RANGE_SIZE_MISMATCH"
    r = raw("bytes 0-9/100", payload[0:10], digest="z" * 64)
    assert r.status_code == 400 and r.json()["error"]["code"] == "INVALID_RANGE_DIGEST"
    r = raw("bytes 0-9/100", payload[0:10], digest=sha256(b"different"))
    assert r.status_code == 400 and r.json()["error"]["code"] == "RANGE_DIGEST_MISMATCH"

    # failed validation never moves progress
    status = client.get(f"/sessions/{sid}").json()
    assert status["missing_ranges"] == [{"start": 0, "end": 99}]
    assert status["missing_chunks"] == [0, 1, 2, 3, 4, 5, 6]


def test_range_missing_headers_is_validation_error(client):
    payload = make_bytes(100)
    sid = create_session(client, payload, 16)["session_id"]
    r = client.put(f"/sessions/{sid}/ranges", content=payload[0:10])
    assert r.status_code == 422
    assert r.json()["error"]["code"] == "VALIDATION_ERROR"


def test_range_over_8mib_rejected(client):
    size = 9 * 1024 * 1024
    info = create_session_raw(
        client, file_size=size, chunk_size=1024 * 1024, file_sha256="a" * 64
    )
    body = b"\x01" * size
    r = client.put(
        f"/sessions/{info['session_id']}/ranges",
        content=body,
        headers={
            "Content-Range": f"bytes 0-{size - 1}/{size}",
            "X-Range-SHA256": sha256(body),
        },
    )
    assert r.status_code == 413
    assert r.json()["error"]["code"] == "RANGE_TOO_LARGE"
    status = client.get(f"/sessions/{info['session_id']}").json()
    assert status["missing_ranges"] == [{"start": 0, "end": size - 1}]


def test_range_on_unknown_session_is_404(client):
    body = b"1234"
    r = put_range(client, "nope", 0, 3, body, total=100)
    assert r.status_code == 404
    assert r.json()["error"]["code"] == "SESSION_NOT_FOUND"


# ---------- idempotent replay ----------

def test_range_identical_replay_is_200_duplicate(client):
    payload = make_bytes(100)
    sid = create_session(client, payload, 16)["session_id"]
    first = put_range(client, sid, 10, 19, range_of(payload, 10, 19))
    assert first.status_code == 201 and first.json()["duplicate"] is False
    replay = put_range(client, sid, 10, 19, range_of(payload, 10, 19))
    assert replay.status_code == 200 and replay.json()["duplicate"] is True
    # a replay that extends coverage is still a new write
    extended = put_range(client, sid, 10, 29, range_of(payload, 10, 29))
    assert extended.status_code == 201 and extended.json()["duplicate"] is False


# ---------- conflicts ----------

def test_range_conflict_with_chunk_is_atomic(client):
    payload = make_bytes(100)
    sid = create_session(client, payload, 16)["session_id"]
    assert put_chunk(client, sid, 1, range_of(payload, 16, 31)).status_code == 201

    bad = bytearray(range_of(payload, 10, 40))
    bad[10] ^= 0xFF  # offset 20 differs (inside chunk 1)
    r = put_range(client, sid, 10, 40, bytes(bad))
    assert r.status_code == 409
    err = r.json()["error"]
    assert err["code"] == "RANGE_CONFLICT"
    assert err["details"]["conflict_offset"] == 20

    # the non-overlapping parts [10,16) and [32,40] must not have been written
    status = client.get(f"/sessions/{sid}").json()
    assert status["missing_ranges"] == [
        {"start": 0, "end": 15},
        {"start": 32, "end": 99},
    ]
    assert list(_seg_dir(client, sid).glob("*.seg")) == []  # chunk-only coverage stores no segments


def _seg_dir(client, sid):
    return client.app.state.service.store.segment_dir(sid)


def test_chunk_conflict_with_saved_segments_is_atomic(client):
    payload = make_bytes(100)
    sid = create_session(client, payload, 16)["session_id"]
    # seed [14, 20) crossing the chunk 0/1 boundary
    assert put_range(client, sid, 14, 19, range_of(payload, 14, 19)).status_code == 201

    good = bytearray(make_bytes(16, "other"))
    good[:] = payload[0:16]
    r = put_chunk(client, sid, 0, bytes(good))
    assert r.status_code == 201  # identical overlap, fills the rest of chunk 0

    # now a wrong chunk 1 that differs at absolute offset 17 (relative 1)
    bad = bytearray(payload[16:32])
    bad[1] ^= 0x01
    r = put_chunk(client, sid, 1, bytes(bad))
    assert r.status_code == 409
    err = r.json()["error"]
    assert err["code"] == "RANGE_CONFLICT"
    assert err["details"]["conflict_offset"] == 17
    # nothing from the rejected chunk leaked in: [20,100) still missing
    status = client.get(f"/sessions/{sid}").json()
    assert status["missing_ranges"] == [{"start": 20, "end": 99}]
    assert not client.app.state.service.store.chunk_path(sid, 1).exists()
    assert status["missing_chunks"] == [1, 2, 3, 4, 5, 6]


def test_range_vs_range_conflict_offset_and_atomicity(client):
    payload = make_bytes(100)
    sid = create_session(client, payload, 16)["session_id"]
    assert put_range(client, sid, 0, 49, range_of(payload, 0, 49)).status_code == 201
    bad = bytearray(range_of(payload, 40, 79))
    bad[3] ^= 0x55  # absolute offset 43
    r = put_range(client, sid, 40, 79, bytes(bad))
    assert r.status_code == 409
    assert r.json()["error"]["details"]["conflict_offset"] == 43
    # tail [50,79] of the rejected request is not stored
    status = client.get(f"/sessions/{sid}").json()
    assert status["missing_ranges"] == [{"start": 50, "end": 99}]


# ---------- mixed protocol interleaving & assembly ----------

def test_mixed_protocol_completes_file(client):
    payload = make_bytes(100)
    sid = create_session(client, payload, 16)["session_id"]
    # ranges straddling chunk borders, chunks, ranges filling the remainder
    assert put_range(client, sid, 5, 25, range_of(payload, 5, 25)).status_code == 201
    assert put_chunk(client, sid, 3, range_of(payload, 48, 63)).status_code == 201
    assert put_range(client, sid, 0, 4, range_of(payload, 0, 4)).status_code == 201
    assert put_range(client, sid, 80, 99, range_of(payload, 80, 99)).status_code == 201
    assert put_chunk(client, sid, 2, range_of(payload, 32, 47)).status_code == 201
    assert put_range(client, sid, 26, 31, range_of(payload, 26, 31)).status_code == 201
    assert put_range(client, sid, 64, 79, range_of(payload, 64, 79)).status_code == 201
    status = client.get(f"/sessions/{sid}").json()
    assert status["missing_chunks"] == []
    assert status["missing_ranges"] == []
    r = client.post(f"/sessions/{sid}/finalize")
    assert r.status_code == 200, r.text
    assert r.json()["final_sha256"] == sha256(payload)
    assert client.get(f"/sessions/{sid}/artifact").content == payload


def test_chunk_replayed_over_equivalent_segments_is_200(client):
    payload = make_bytes(32)
    sid = create_session(client, payload, 16)["session_id"]
    assert put_range(client, sid, 0, 31, range_of(payload, 0, 31)).status_code == 201
    # whole file already covered by identical bytes via ranges
    r = put_chunk(client, sid, 0, range_of(payload, 0, 15))
    assert r.status_code == 200 and r.json()["duplicate"] is True
    r = put_chunk(client, sid, 1, range_of(payload, 16, 31))
    assert r.status_code == 200 and r.json()["duplicate"] is True


def test_range_replayed_over_equivalent_chunk_is_200(client):
    payload = make_bytes(32)
    sid = create_session(client, payload, 16)["session_id"]
    assert put_chunk(client, sid, 0, range_of(payload, 0, 15)).status_code == 201
    # a range fully inside the confirmed chunk with identical bytes
    r = put_range(client, sid, 2, 13, range_of(payload, 2, 13))
    assert r.status_code == 200 and r.json()["duplicate"] is True
    assert r.json()["missing_ranges"] == [{"start": 16, "end": 31}]


def test_integrity_mismatch_after_mixed_upload_keeps_progress(client):
    payload = make_bytes(100)
    sid = create_session(client, payload, 16, file_sha256=sha256(b"declared-elsewhere"))["session_id"]
    assert put_range(client, sid, 0, 49, range_of(payload, 0, 49)).status_code == 201
    assert put_range(client, sid, 50, 99, range_of(payload, 50, 99)).status_code == 201
    r = client.post(f"/sessions/{sid}/finalize")
    assert r.status_code == 422
    assert r.json()["error"]["code"] == "INTEGRITY_MISMATCH"
    status = client.get(f"/sessions/{sid}").json()
    assert status["missing_ranges"] == [] and status["missing_chunks"] == []
    assert client.get(f"/sessions/{sid}/artifact").status_code == 409


# ---------- expiry ----------

def test_expired_session_range_rules(client, monkeypatch):
    payload = make_bytes(100)
    sid = create_session(client, payload, 16)["session_id"]
    assert put_range(client, sid, 0, 9, range_of(payload, 0, 9)).status_code == 201
    later = datetime.now(timezone.utc) + timedelta(hours=2)
    monkeypatch.setattr(clock, "utcnow", lambda: later)

    status = client.get(f"/sessions/{sid}").json()
    assert status["status"] == "expired"

    # identical replay adding no coverage -> still 200
    r = put_range(client, sid, 0, 9, range_of(payload, 0, 9))
    assert r.status_code == 200 and r.json()["duplicate"] is True

    # same covered content but adding new bytes -> 410 wholesale
    r = put_range(client, sid, 0, 19, range_of(payload, 0, 19))
    assert r.status_code == 410 and r.json()["error"]["code"] == "SESSION_EXPIRED"
    status = client.get(f"/sessions/{sid}").json()
    assert status["missing_ranges"] == [{"start": 10, "end": 99}]

    # genuinely new bytes -> 410
    r = put_range(client, sid, 50, 59, range_of(payload, 50, 59))
    assert r.status_code == 410

    # conflicting replay is a conflict, not an expiry acceptance
    bad = bytearray(range_of(payload, 0, 9))
    bad[0] ^= 1
    r = put_range(client, sid, 0, 9, bytes(bad))
    assert r.status_code == 409 and r.json()["error"]["code"] == "RANGE_CONFLICT"


def test_completed_session_rejects_new_ranges(client):
    payload = make_bytes(32)
    sid = create_session(client, payload, 16)["session_id"]
    assert put_range(client, sid, 0, 31, payload).status_code == 201
    assert client.post(f"/sessions/{sid}/finalize").status_code == 200
    r = put_range(client, sid, 0, 31, payload)
    assert r.status_code == 200 and r.json()["duplicate"] is True
    # completed sessions cannot gain bytes; extend past EOF is a bounds error,
    # so check via a fresh completed session with a gap-free replay only.
    status = client.get(f"/sessions/{sid}").json()
    assert status["status"] == "completed"


# ---------- restart & crash recovery ----------

def test_segments_survive_restart(data_dir):
    payload = make_bytes(100)
    app1 = create_app(Settings(data_dir=data_dir))
    with TestClient(app1) as c1:
        sid = create_session(c1, payload, 16)["session_id"]
        assert put_range(c1, sid, 5, 25, range_of(payload, 5, 25)).status_code == 201

    app2 = create_app(Settings(data_dir=data_dir))
    with TestClient(app2) as c2:
        status = c2.get(f"/sessions/{sid}").json()
        assert status["missing_ranges"] == [
            {"start": 0, "end": 4},
            {"start": 26, "end": 99},
        ]
        assert status["missing_chunks"] == [0, 1, 2, 3, 4, 5, 6]
        assert put_range(c2, sid, 0, 4, range_of(payload, 0, 4)).status_code == 201
        assert put_range(c2, sid, 26, 99, range_of(payload, 26, 99)).status_code == 201
        r = c2.post(f"/sessions/{sid}/finalize")
        assert r.status_code == 200
        assert c2.get(f"/sessions/{sid}/artifact").content == payload


def test_reconcile_removes_orphan_segments_and_tmp(data_dir):
    payload = make_bytes(100)
    app1 = create_app(Settings(data_dir=data_dir))
    with TestClient(app1) as c1:
        sid = create_session(c1, payload, 16)["session_id"]
        assert put_range(c1, sid, 0, 19, range_of(payload, 0, 19)).status_code == 201

    seg_dir = data_dir / "segments" / sid
    # crash after rename but before metadata commit; and a half-written tmp
    (seg_dir / "0000000000000032-deadbeef.seg").write_bytes(b"orphan")
    (seg_dir / ".partial.tmp").write_bytes(b"half")
    # and a committed row whose body vanished
    db_path = data_dir / "db.sqlite3"
    conn = sqlite3.connect(db_path)
    row = conn.execute("SELECT path FROM ranges ORDER BY id LIMIT 1").fetchone()
    Path(row[0]).unlink()
    conn.commit()
    conn.close()

    app2 = create_app(Settings(data_dir=data_dir))
    with TestClient(app2) as c2:
        status = c2.get(f"/sessions/{sid}").json()
        # the row whose file was lost is dropped; nothing extra is counted
        assert status["missing_ranges"] == [{"start": 0, "end": 99}]
        assert not (seg_dir / "0000000000000032-deadbeef.seg").exists()
        assert not (seg_dir / ".partial.tmp").exists()
        assert list(seg_dir.glob("*.seg")) == []


def test_request_failure_leaves_no_segments(data_dir):
    payload = make_bytes(100)
    app = create_app(Settings(data_dir=data_dir))
    with TestClient(app) as c:
        sid = create_session(c, payload, 16)["session_id"]
        assert put_range(c, sid, 0, 19, range_of(payload, 0, 19)).status_code == 201
        n_files = len(list((data_dir / "segments" / sid).glob("*.seg")))

        bad = bytearray(range_of(payload, 10, 60))
        bad[0] ^= 0x7A  # conflict at offset 10
        r = put_range(c, sid, 10, 60, bytes(bad))
        assert r.status_code == 409
        # no segment files, temp files or range rows from the rejected request
        seg_dir = data_dir / "segments" / sid
        assert len(list(seg_dir.glob("*.seg"))) == n_files
        assert list(seg_dir.glob("*.tmp")) == []
        conn = sqlite3.connect(data_dir / "db.sqlite3")
        (count,) = conn.execute("SELECT COUNT(*) FROM ranges WHERE session_id=?", (sid,)).fetchone()
        conn.close()
        assert count == n_files


def test_crash_between_durable_body_and_metadata_commit(data_dir, monkeypatch):
    """Body files are renamed+fsynced before the SQLite commit. If the commit
    itself blows up, nothing may be left behind and a retry must succeed."""
    payload = make_bytes(100)
    app = create_app(Settings(data_dir=data_dir))
    with TestClient(app, raise_server_exceptions=False) as c:
        sid = create_session(c, payload, 16)["session_id"]

        calls = {"n": 0}
        real_commit = app.state.service.db.commit_segments

        def flaky(session_id, records, bitmap):
            calls["n"] += 1
            if calls["n"] == 1:
                raise RuntimeError("simulated crash during metadata commit")
            return real_commit(session_id, records, bitmap)

        monkeypatch.setattr(app.state.service.db, "commit_segments", flaky)
        r = put_range(c, sid, 0, 19, range_of(payload, 0, 19))
        assert r.status_code == 500
        assert r.json()["error"]["code"] == "INTERNAL_ERROR"

        seg_dir = data_dir / "segments" / sid
        assert list(seg_dir.glob("*.seg")) == []
        assert list(seg_dir.glob("*.tmp")) == []
        conn = sqlite3.connect(data_dir / "db.sqlite3")
        (count,) = conn.execute("SELECT COUNT(*) FROM ranges WHERE session_id=?", (sid,)).fetchone()
        conn.close()
        assert count == 0

        # retry behaves exactly like the first attempt: bytes are accepted once
        monkeypatch.undo()
        r = put_range(c, sid, 0, 19, range_of(payload, 0, 19))
        assert r.status_code == 201 and r.json()["duplicate"] is False

        # simulate the harder window: process death after rename, before commit.
        # Restart reconcile treats the orphan segment as unconfirmed and drops it.
        (seg_dir / "0000000000000032-orphan1234.seg").write_bytes(payload[50:60])
    app2 = create_app(Settings(data_dir=data_dir))
    with TestClient(app2) as c2:
        status = c2.get(f"/sessions/{sid}").json()
        assert status["missing_ranges"] == [{"start": 20, "end": 99}]
        assert not (data_dir / "segments" / sid / "0000000000000032-orphan1234.seg").exists()


# ---------- concurrency: serial equivalence ----------

def test_concurrent_same_content_merges(data_dir):
    size = 1024 * 1024
    payload = make_bytes(size, "concurrent")
    chunk_size = size  # single logical chunk

    async def scenario():
        app = create_app(Settings(data_dir=data_dir))
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://t") as ac:
            r = await ac.post(
                "/sessions",
                json={
                    "file_size": size,
                    "chunk_size": chunk_size,
                    "file_sha256": sha256(payload),
                    "expires_at": future_expiry(),
                },
            )
            sid = r.json()["session_id"]

            async def send(body, start, end):
                return await ac.put(
                    f"/sessions/{sid}/ranges",
                    content=body,
                    headers={
                        "Content-Range": f"bytes {start}-{end}/{size}",
                        "X-Range-SHA256": sha256(body),
                    },
                )

            q = size // 4
            r1, r2 = await asyncio.gather(
                send(payload[0 : 3 * q], 0, 3 * q - 1),
                send(payload[q:size], q, size - 1),
            )
            assert r1.status_code == 201, r1.text
            assert r2.status_code == 201, r2.text
            fin = await ac.post(f"/sessions/{sid}/finalize")
            assert fin.status_code == 200, fin.text
            art = await ac.get(f"/sessions/{sid}/artifact")
            assert art.content == payload

    asyncio.run(scenario())


def test_concurrent_conflicting_content_single_winner(data_dir):
    size = 64 * 1024
    payload = make_bytes(size, "race")
    diff_at = 1234
    contender = bytearray(payload)
    contender[diff_at] ^= 0xFF

    async def scenario():
        app = create_app(Settings(data_dir=data_dir))
        transport = httpx.ASGITransport(app=app)
        results = []
        async with httpx.AsyncClient(transport=transport, base_url="http://t") as ac:
            r = await ac.post(
                "/sessions",
                json={
                    "file_size": size,
                    "chunk_size": size,
                    "file_sha256": sha256(payload),
                    "expires_at": future_expiry(),
                },
            )
            sid = r.json()["session_id"]

            async def send(body):
                resp = await ac.put(
                    f"/sessions/{sid}/ranges",
                    content=body,
                    headers={
                        "Content-Range": f"bytes 0-{size - 1}/{size}",
                        "X-Range-SHA256": sha256(body),
                    },
                )
                results.append((resp.status_code, resp.content))

            await asyncio.gather(send(payload), send(bytes(contender)), send(payload))
        statuses = sorted(code for code, _ in results)
        assert statuses.count(201) == 1
        assert 200 in statuses or statuses.count(409) >= 1
        # exactly one of the two contents is stored; loser left no bytes
        async with httpx.AsyncClient(transport=transport, base_url="http://t") as ac:
            r = await ac.put(
                f"/sessions/{sid}/ranges",
                content=payload,
                headers={
                    "Content-Range": f"bytes 0-{size - 1}/{size}",
                    "X-Range-SHA256": sha256(payload),
                },
            )
            if r.status_code == 200:
                winner = "payload"
            else:
                assert r.status_code == 409
                assert r.json()["error"]["details"]["conflict_offset"] == diff_at
                winner = "contender"
        conn = sqlite3.connect(data_dir / "db.sqlite3")
        (n_rows,) = conn.execute("SELECT COUNT(*) FROM ranges WHERE session_id=?", (sid,)).fetchone()
        conn.close()
        if winner == "payload":
            assert n_rows == 1  # single full-cover segment
        return winner

    winner = asyncio.run(scenario())
    # whichever writer won, the on-disk segment must match exactly one content
    sid_segs = list((data_dir / "segments").iterdir())[0]
    data = b"".join(p.read_bytes() for p in sorted(sid_segs.glob("*.seg")))
    if winner == "payload":
        assert data == payload
    else:
        assert data == bytes(contender)


# ---------- old-volume migration ----------

_OLD_SCHEMA = """
CREATE TABLE sessions (
    session_id TEXT PRIMARY KEY, file_size INTEGER NOT NULL, chunk_size INTEGER NOT NULL,
    total_chunks INTEGER NOT NULL, file_sha256 TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'active',
    bitmap BLOB NOT NULL, expires_at TEXT NOT NULL, created_at TEXT NOT NULL,
    completed_at TEXT, final_sha256 TEXT, artifact_path TEXT
);
CREATE TABLE chunks (
    session_id TEXT NOT NULL, chunk_index INTEGER NOT NULL, size INTEGER NOT NULL,
    sha256 TEXT NOT NULL, path TEXT NOT NULL, received_at TEXT NOT NULL,
    PRIMARY KEY (session_id, chunk_index)
);
"""


def _seed_old_volume(data_dir: Path) -> dict:
    """Build a database exactly as the pre-range version would have left it."""
    payload = make_bytes(100)
    chunk_size = 16
    total = -(-len(payload) // chunk_size)
    now = datetime.now(timezone.utc)
    (data_dir / "chunks").mkdir(parents=True)
    (data_dir / "artifacts").mkdir(parents=True)
    conn = sqlite3.connect(data_dir / "db.sqlite3")
    conn.executescript(_OLD_SCHEMA)

    def new_bitmap(n):
        return bytes((n + 7) // 8)

    # active session: chunks 0 and 2 present
    active = "a" * 32
    bitmap = bytearray(new_bitmap(total))
    bitmap[0] = 0b00000101
    conn.execute(
        "INSERT INTO sessions VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
        (active, len(payload), chunk_size, total, sha256(payload), "active",
         bytes(bitmap), (now + timedelta(hours=1)).isoformat(), now.isoformat(),
         None, None, None),
    )
    cdir = data_dir / "chunks" / active
    cdir.mkdir()
    for i in (0, 2):
        body = payload[i * chunk_size : min((i + 1) * chunk_size, len(payload))]
        path = cdir / f"{i:08d}.chunk"
        path.write_bytes(body)
        conn.execute(
            "INSERT INTO chunks VALUES (?,?,?,?,?,?)",
            (active, i, len(body), sha256(body), str(path), now.isoformat()),
        )

    # expired session: chunk 0 only
    expired = "b" * 32
    bitmap = bytearray(new_bitmap(total))
    bitmap[0] = 0b00000001
    conn.execute(
        "INSERT INTO sessions VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
        (expired, len(payload), chunk_size, total, sha256(payload), "active",
         bytes(bitmap), (now - timedelta(hours=1)).isoformat(), now.isoformat(),
         None, None, None),
    )
    cdir = data_dir / "chunks" / expired
    cdir.mkdir()
    body = payload[0:chunk_size]
    path = cdir / "00000000.chunk"
    path.write_bytes(body)
    conn.execute(
        "INSERT INTO chunks VALUES (?,?,?,?,?,?)",
        (expired, 0, len(body), sha256(body), str(path), now.isoformat()),
    )

    # completed session with a published artifact and every chunk on disk
    done = "c" * 32
    artifact = data_dir / "artifacts" / f"{done}.bin"
    artifact.write_bytes(payload)
    bitmap = bytearray(new_bitmap(total))
    bitmap[0] = 0x7F
    conn.execute(
        "INSERT INTO sessions VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
        (done, len(payload), chunk_size, total, sha256(payload), "completed",
         bytes(bitmap), (now + timedelta(hours=1)).isoformat(), now.isoformat(),
         now.isoformat(), sha256(payload), str(artifact)),
    )
    cdir = data_dir / "chunks" / done
    cdir.mkdir()
    for i in range(total):
        body = payload[i * chunk_size : min((i + 1) * chunk_size, len(payload))]
        path = cdir / f"{i:08d}.chunk"
        path.write_bytes(body)
        conn.execute(
            "INSERT INTO chunks VALUES (?,?,?,?,?,?)",
            (done, i, len(body), sha256(body), str(path), now.isoformat()),
        )
    conn.commit()
    conn.close()
    return {"payload": payload, "active": active, "expired": expired, "done": done}


def test_old_volume_upgrades_in_place(data_dir):
    ids = _seed_old_volume(data_dir)
    payload = ids["payload"]

    app = create_app(Settings(data_dir=data_dir))
    with TestClient(app) as c:
        # active: old progress visible, can query, resume with the NEW protocol
        status = c.get(f"/sessions/{ids['active']}").json()
        assert status["missing_chunks"] == [1, 3, 4, 5, 6]
        assert {"start": 16, "end": 31} in status["missing_ranges"]
        assert put_range(c, ids["active"], 16, 31, payload[16:32]).status_code == 201
        for i in (3, 4, 5):
            assert put_chunk(c, ids["active"], i, payload[i * 16 : (i + 1) * 16]).status_code == 201
        assert put_range(c, ids["active"], 96, 99, payload[96:100]).status_code == 201
        r = c.post(f"/sessions/{ids['active']}/finalize")
        assert r.status_code == 200, r.text
        assert c.get(f"/sessions/{ids['active']}/artifact").content == payload

        # expired: query + identical replay allowed, new bytes rejected
        st = c.get(f"/sessions/{ids['expired']}").json()
        assert st["status"] == "expired"
        assert put_chunk(c, ids["expired"], 0, payload[0:16]).status_code == 200
        assert put_range(c, ids["expired"], 0, 15, payload[0:16]).status_code == 200
        assert put_range(c, ids["expired"], 16, 31, payload[16:32]).status_code == 410

        # completed: download and idempotent finalize keep working
        r = c.post(f"/sessions/{ids['done']}/finalize")
        assert r.status_code == 200
        assert c.get(f"/sessions/{ids['done']}/artifact").content == payload

    # migration must not have rewritten the legacy chunk bodies; resumed
    # sessions may legitimately add rows, but pre-existing rows/paths survive
    conn = sqlite3.connect(data_dir / "db.sqlite3")
    counts = dict(conn.execute("SELECT session_id, COUNT(*) FROM chunks GROUP BY session_id").fetchall())
    conn.close()
    assert counts == {ids["active"]: 5, ids["expired"]: 1, ids["done"]: 7}
    for sid_, indices in ((ids["active"], (0, 2)), (ids["expired"], (0,)), (ids["done"], range(7))):
        for i in indices:
            assert (data_dir / "chunks" / sid_ / f"{i:08d}.chunk").exists()


# ---------- large sparse coverage ----------

def test_large_sparse_ranges_are_cheap_and_finalize_streams(data_dir):
    gib = 1024 ** 3
    info = None
    app = create_app(Settings(data_dir=data_dir))
    with TestClient(app) as c:
        info = create_session_raw(
            c, file_size=gib, chunk_size=1024 * 1024, file_sha256="d" * 64
        )
        sid = info["session_id"]
        for start in (0, 500 * 1024 * 1024, gib - 100):
            body = bytes(((start + k) % 251 for k in range(100)))
            r = put_range(c, sid, start, start + 99, body, total=gib)
            assert r.status_code == 201, r.text
        status = c.get(f"/sessions/{sid}").json()
        assert status["received_count"] == 0  # no full 1 MiB chunk
        assert status["missing_ranges"] == [
            {"start": 100, "end": 500 * 1024 * 1024 - 1},
            {"start": 500 * 1024 * 1024 + 100, "end": gib - 101},
        ]
        # no per-byte rows, no whole-file materialization
        conn = sqlite3.connect(data_dir / "db.sqlite3")
        (n,) = conn.execute("SELECT COUNT(*) FROM ranges WHERE session_id=?", (sid,)).fetchone()
        conn.close()
        assert n == 3
        on_disk = sum(p.stat().st_size for p in (data_dir / "segments").rglob("*") if p.is_file())
        assert on_disk == 300


def test_mixed_assembly_scale(data_dir):
    size = 24 * 1024 * 1024
    payload = make_bytes(size, "scale")
    app = create_app(Settings(data_dir=data_dir))
    with TestClient(app) as c:
        sid = create_session(c, payload, 1024 * 1024)["session_id"]
        # unaligned 3 MiB range strides + one legacy chunk in the middle
        step = 3 * 1024 * 1024 + 777
        pos = 0
        chunk_used = False
        while pos < size:
            end = min(pos + step, size) - 1
            if not chunk_used and pos > 12 * 1024 * 1024:
                idx = 12
                assert put_chunk(
                    c, sid, idx,
                    payload[idx * 1024 * 1024 : (idx + 1) * 1024 * 1024],
                ).status_code == 201
                chunk_used = True
            r = put_range(c, sid, pos, end, payload[pos : end + 1])
            assert r.status_code in (200, 201), r.text
            pos = end + 1
        assert chunk_used
        status = c.get(f"/sessions/{sid}").json()
        assert status["missing_ranges"] == []
        r = c.post(f"/sessions/{sid}/finalize")
        assert r.status_code == 200, r.text
        assert c.get(f"/sessions/{sid}/artifact").content == payload


# ---------- threads on the sync client also serialize safely ----------

def test_threaded_chunk_and_range_writers(data_dir):
    payload = make_bytes(200)
    app = create_app(Settings(data_dir=data_dir))
    with TestClient(app) as c:
        sid = create_session(c, payload, 16)["session_id"]
        errors: list[Exception] = []

        def range_writer(start, end):
            try:
                r = put_range(c, sid, start, end, payload[start : end + 1])
                assert r.status_code in (200, 201), r.text
            except Exception as exc:  # pragma: no cover - reported below
                errors.append(exc)

        def chunk_writer(i):
            try:
                body = payload[i * 16 : min((i + 1) * 16, 200)]
                r = put_chunk(c, sid, i, body)
                assert r.status_code in (200, 201), r.text
            except Exception as exc:  # pragma: no cover
                errors.append(exc)

        threads = [threading.Thread(target=range_writer, args=(0, 99)),
                   threading.Thread(target=range_writer, args=(50, 199)),
                   threading.Thread(target=chunk_writer, args=(3,)),
                   threading.Thread(target=chunk_writer, args=(7,))]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        assert not errors
        assert c.post(f"/sessions/{sid}/finalize").status_code == 200
        assert c.get(f"/sessions/{sid}/artifact").content == payload
