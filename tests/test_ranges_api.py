"""Arbitrary byte-range resume: validation, conflicts, mixed-protocol interop."""

from __future__ import annotations

import hashlib
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone

from app import clock


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


def put_chunk(client, sid, index, body, digest=None):
    return client.put(
        f"/sessions/{sid}/chunks/{index}",
        content=body,
        headers={"X-Chunk-SHA256": digest or sha256(body)},
    )


def put_range(client, sid, start, end, payload, *, total=None, body=None, digest="auto"):
    data = payload[start : end + 1] if body is None else body
    total = len(payload) if total is None else total
    headers = {"Content-Range": f"bytes {start}-{end}/{total}"}
    if digest == "auto":
        digest = sha256(data)
    if digest is not None:
        headers["X-Range-SHA256"] = digest
    return client.put(f"/sessions/{sid}/ranges", content=data, headers=headers)


def missing_ranges(client, sid):
    return client.get(f"/sessions/{sid}").json()["missing_ranges"]


# ---------- status / coverage semantics ----------

def test_range_upload_updates_missing_ranges_and_chunks(client):
    payload = make_bytes(1000)
    sid = create_session(client, payload, 100)["session_id"]
    status = client.get(f"/sessions/{sid}").json()
    assert status["missing_ranges"] == [{"start": 0, "end": 999}]

    resp = put_range(client, sid, 0, 49, payload)
    assert resp.status_code == 201
    body = resp.json()
    assert body["duplicate"] is False
    assert body["range"] == {"start": 0, "end": 49}
    assert body["stored_ranges"] == [{"start": 0, "end": 49}]
    status = client.get(f"/sessions/{sid}").json()
    assert status["missing_ranges"] == [{"start": 50, "end": 999}]
    # partial coverage of a logical chunk does not count as received
    assert status["missing_chunks"] == list(range(10))
    assert status["received_count"] == 0

    assert put_range(client, sid, 50, 99, payload).status_code == 201
    status = client.get(f"/sessions/{sid}").json()
    assert status["missing_chunks"] == list(range(1, 10))
    assert status["received_count"] == 1
    assert status["missing_ranges"] == [{"start": 100, "end": 999}]

    # gaps split and merge as coverage grows
    assert put_range(client, sid, 200, 299, payload).status_code == 201
    assert put_range(client, sid, 400, 499, payload).status_code == 201
    assert missing_ranges(client, sid) == [
        {"start": 100, "end": 199},
        {"start": 300, "end": 399},
        {"start": 500, "end": 999},
    ]
    assert put_range(client, sid, 300, 349, payload).status_code == 201
    assert put_range(client, sid, 350, 399, payload).status_code == 201
    assert missing_ranges(client, sid) == [
        {"start": 100, "end": 199},
        {"start": 500, "end": 999},
    ]


def test_single_range_can_cover_whole_file(client):
    payload = make_bytes(250)
    sid = create_session(client, payload, 100)["session_id"]
    resp = put_range(client, sid, 0, 249, payload)
    assert resp.status_code == 201
    status = client.get(f"/sessions/{sid}").json()
    assert status["missing_chunks"] == []
    assert status["missing_ranges"] == []
    assert client.post(f"/sessions/{sid}/finalize").status_code == 200
    assert client.get(f"/sessions/{sid}/artifact").content == payload


def test_many_small_segments_merge_in_status(client):
    payload = make_bytes(1000)
    sid = create_session(client, payload, 100)["session_id"]
    for base in range(0, 1000, 20):
        assert put_range(client, sid, base, base + 9, payload).status_code == 201
    assert missing_ranges(client, sid) == [
        {"start": base + 10, "end": base + 19} for base in range(0, 1000, 20)
    ]
    for base in range(0, 1000, 20):
        assert put_range(client, sid, base + 10, base + 19, payload).status_code == 201
    status = client.get(f"/sessions/{sid}").json()
    assert status["missing_ranges"] == []
    assert status["missing_chunks"] == []


# ---------- validation ----------

def test_range_header_validation_changes_nothing(client):
    payload = make_bytes(500)
    sid = create_session(client, payload, 100)["session_id"]
    body = payload[0:10]

    for bad in (
        "bytes 0-9",
        "0-9/500",
        "bytes 9-0/500",
        "items 0-9/500",
        "bytes 0-9/*",
        "bytes -1-9/500",
        "bytes 0--9/500",
        "bytes 0-9/500/extra",
    ):
        resp = client.put(
            f"/sessions/{sid}/ranges",
            content=body,
            headers={"Content-Range": bad, "X-Range-SHA256": sha256(body)},
        )
        assert resp.status_code == 400, bad
        assert resp.json()["error"]["code"] == "INVALID_CONTENT_RANGE", bad

    resp = put_range(client, sid, 0, 9, payload, total=499)
    assert resp.status_code == 400
    assert resp.json()["error"]["code"] == "RANGE_TOTAL_MISMATCH"

    resp = put_range(client, sid, 490, 500, payload, body=payload[490:500] + b"x")
    assert resp.status_code == 400
    assert resp.json()["error"]["code"] == "RANGE_OUT_OF_BOUNDS"
    resp = put_range(client, sid, 500, 500, payload, body=b"x")
    assert resp.status_code == 400
    assert resp.json()["error"]["code"] == "RANGE_OUT_OF_BOUNDS"

    resp = put_range(client, sid, 0, 9, payload, digest="zz")
    assert resp.status_code == 400
    assert resp.json()["error"]["code"] == "INVALID_RANGE_DIGEST"

    resp = put_range(client, sid, 0, 9, payload, digest=sha256(b"other"))
    assert resp.status_code == 400
    assert resp.json()["error"]["code"] == "RANGE_DIGEST_MISMATCH"

    # body shorter than the declared range
    short = payload[0:5]
    resp = client.put(
        f"/sessions/{sid}/ranges",
        content=short,
        headers={"Content-Range": "bytes 0-9/500", "X-Range-SHA256": sha256(short)},
    )
    assert resp.status_code == 400
    assert resp.json()["error"]["code"] == "RANGE_SIZE_MISMATCH"

    # body longer than the declared range
    long_body = payload[0:20]
    resp = client.put(
        f"/sessions/{sid}/ranges",
        content=long_body,
        headers={"Content-Range": "bytes 0-9/500", "X-Range-SHA256": sha256(long_body)},
    )
    assert resp.status_code == 400
    assert resp.json()["error"]["code"] == "RANGE_SIZE_MISMATCH"

    # missing headers -> validation envelope
    resp = client.put(f"/sessions/{sid}/ranges", content=body)
    assert resp.status_code == 422
    assert resp.json()["error"]["code"] == "VALIDATION_ERROR"

    status = client.get(f"/sessions/{sid}").json()
    assert status["missing_ranges"] == [{"start": 0, "end": 499}]
    assert status["received_count"] == 0


def test_range_size_limit(client):
    limit = 8 * 1024 * 1024
    payload = make_bytes(limit + 100, "big")
    sid = create_session(client, payload, 1024 * 1024)["session_id"]
    # exactly 8 MiB is accepted
    resp = put_range(client, sid, 0, limit - 1, payload)
    assert resp.status_code == 201, resp.text
    # 8 MiB + 1 declared is rejected before the body is read
    resp = client.put(
        f"/sessions/{sid}/ranges",
        content=b"x",
        headers={"Content-Range": f"bytes 0-{limit}/{len(payload)}", "X-Range-SHA256": sha256(b"x")},
    )
    assert resp.status_code == 413
    assert resp.json()["error"]["code"] == "RANGE_TOO_LARGE"


def test_range_unknown_session(client):
    resp = client.put(
        "/sessions/does-not-exist/ranges",
        content=b"ab",
        headers={"Content-Range": "bytes 0-1/100", "X-Range-SHA256": sha256(b"ab")},
    )
    assert resp.status_code == 404
    assert resp.json()["error"]["code"] == "SESSION_NOT_FOUND"


# ---------- idempotency / conflicts ----------

def test_range_duplicate_replay(client):
    payload = make_bytes(300)
    sid = create_session(client, payload, 100)["session_id"]
    assert put_range(client, sid, 10, 99, payload).status_code == 201
    replay = put_range(client, sid, 10, 99, payload)
    assert replay.status_code == 200
    assert replay.json()["duplicate"] is True
    assert replay.json()["stored_ranges"] == []
    # a sub-range of committed coverage is a duplicate too
    assert put_range(client, sid, 20, 29, payload).status_code == 200
    assert missing_ranges(client, sid) == [{"start": 0, "end": 9}, {"start": 100, "end": 299}]


def test_range_conflict_with_chunk_is_atomic(client):
    payload = make_bytes(300)
    sid = create_session(client, payload, 100)["session_id"]
    assert put_chunk(client, sid, 0, payload[0:100]).status_code == 201
    bad = bytearray(payload[50:150])
    bad[10] ^= 0xFF  # absolute offset 60
    resp = put_range(client, sid, 50, 149, payload, body=bytes(bad))
    assert resp.status_code == 409
    err = resp.json()["error"]
    assert err["code"] == "RANGE_CONFLICT"
    assert err["details"]["first_conflict_offset"] == 60
    # the non-overlapping part [100, 149] must not have been written either
    assert missing_ranges(client, sid) == [{"start": 100, "end": 299}]
    # a clean retry succeeds and stores only the novel part
    ok = put_range(client, sid, 50, 149, payload)
    assert ok.status_code == 201
    assert ok.json()["stored_ranges"] == [{"start": 100, "end": 149}]


def test_range_conflict_with_segment(client):
    payload = make_bytes(300)
    sid = create_session(client, payload, 100)["session_id"]
    assert put_range(client, sid, 100, 199, payload).status_code == 201
    bad = bytearray(payload[150:250])
    bad[49] ^= 0xFF  # absolute offset 199
    resp = put_range(client, sid, 150, 249, payload, body=bytes(bad))
    assert resp.status_code == 409
    assert resp.json()["error"]["details"]["first_conflict_offset"] == 199
    assert missing_ranges(client, sid) == [{"start": 0, "end": 99}, {"start": 200, "end": 299}]


def test_first_conflict_offset_across_multiple_sources(client):
    payload = make_bytes(300)
    sid = create_session(client, payload, 100)["session_id"]
    assert put_chunk(client, sid, 0, payload[0:100]).status_code == 201
    assert put_range(client, sid, 150, 199, payload).status_code == 201
    bad = bytearray(payload[0:200])
    bad[95] ^= 0xFF
    bad[155] ^= 0xFF
    resp = put_range(client, sid, 0, 199, payload, body=bytes(bad))
    assert resp.status_code == 409
    assert resp.json()["error"]["details"]["first_conflict_offset"] == 95
    assert missing_ranges(client, sid) == [{"start": 100, "end": 149}, {"start": 200, "end": 299}]


def test_range_spanning_chunk_and_segment(client):
    payload = make_bytes(300)
    sid = create_session(client, payload, 100)["session_id"]
    assert put_chunk(client, sid, 0, payload[0:100]).status_code == 201
    assert put_range(client, sid, 150, 199, payload).status_code == 201
    resp = put_range(client, sid, 50, 249, payload)
    assert resp.status_code == 201
    assert resp.json()["stored_ranges"] == [{"start": 100, "end": 149}, {"start": 200, "end": 249}]
    assert missing_ranges(client, sid) == [{"start": 250, "end": 299}]


# ---------- chunk PUT over segments ----------

def test_chunk_put_over_partial_segments(client):
    payload = make_bytes(300)
    sid = create_session(client, payload, 100)["session_id"]
    assert put_range(client, sid, 40, 99, payload).status_code == 201
    bad = bytearray(payload[0:100])
    bad[50] ^= 0xFF
    resp = put_chunk(client, sid, 0, bytes(bad))
    assert resp.status_code == 409
    assert resp.json()["error"]["code"] == "RANGE_CONFLICT"
    assert resp.json()["error"]["details"]["first_conflict_offset"] == 50
    # matching content fills the rest of the chunk
    resp = put_chunk(client, sid, 0, payload[0:100])
    assert resp.status_code == 201
    assert resp.json()["duplicate"] is False
    status = client.get(f"/sessions/{sid}").json()
    assert status["missing_chunks"] == [1, 2]
    assert status["missing_ranges"] == [{"start": 100, "end": 299}]


def test_chunk_put_duplicate_via_full_segment_coverage(client):
    payload = make_bytes(300)
    sid = create_session(client, payload, 100)["session_id"]
    assert put_range(client, sid, 0, 99, payload).status_code == 201
    resp = put_chunk(client, sid, 0, payload[0:100])
    assert resp.status_code == 200
    assert resp.json()["duplicate"] is True
    bad = bytearray(payload[0:100])
    bad[0] ^= 0xFF
    resp = put_chunk(client, sid, 0, bytes(bad))
    assert resp.status_code == 409
    assert resp.json()["error"]["code"] == "RANGE_CONFLICT"
    assert resp.json()["error"]["details"]["first_conflict_offset"] == 0


def test_chunk_put_consolidates_contained_segments(client, data_dir):
    payload = make_bytes(300)
    sid = create_session(client, payload, 100)["session_id"]
    assert put_range(client, sid, 10, 59, payload).status_code == 201  # inside chunk 0
    assert put_range(client, sid, 90, 109, payload).status_code == 201  # straddles 0/1
    seg_dir = data_dir / "segments" / sid
    assert len(list(seg_dir.glob("*.seg"))) == 2
    assert put_chunk(client, sid, 0, payload[0:100]).status_code == 201
    # the contained segment is superseded; the straddling one is kept
    assert len(list(seg_dir.glob("*.seg"))) == 1
    assert missing_ranges(client, sid) == [{"start": 110, "end": 299}]


# ---------- mixed finalize ----------

def test_mixed_protocol_finalize(client):
    payload = make_bytes(350)
    sid = create_session(client, payload, 100)["session_id"]  # chunks 100/100/100/50
    assert put_chunk(client, sid, 0, payload[0:100]).status_code == 201
    assert put_range(client, sid, 90, 209, payload).status_code == 201  # straddles 0/1/2
    assert put_chunk(client, sid, 2, payload[200:300]).status_code == 201
    assert put_range(client, sid, 300, 349, payload).status_code == 201
    status = client.get(f"/sessions/{sid}").json()
    assert status["missing_chunks"] == []
    assert status["missing_ranges"] == []
    resp = client.post(f"/sessions/{sid}/finalize")
    assert resp.status_code == 200
    assert resp.json()["final_sha256"] == sha256(payload)
    again = client.post(f"/sessions/{sid}/finalize")
    assert again.status_code == 200
    download = client.get(f"/sessions/{sid}/artifact")
    assert download.content == payload
    assert download.headers["x-file-sha256"] == sha256(payload)


def test_integrity_mismatch_with_mixed_coverage_keeps_progress(client):
    payload = make_bytes(250)
    declared = sha256(b"declared-content-differs")
    sid = create_session(client, payload, 100, file_sha256=declared)["session_id"]
    assert put_chunk(client, sid, 0, payload[0:100]).status_code == 201
    assert put_range(client, sid, 100, 249, payload).status_code == 201
    resp = client.post(f"/sessions/{sid}/finalize")
    assert resp.status_code == 422
    assert resp.json()["error"]["code"] == "INTEGRITY_MISMATCH"
    status = client.get(f"/sessions/{sid}").json()
    assert status["status"] == "active"
    assert status["missing_chunks"] == []
    assert status["missing_ranges"] == []


# ---------- expiry / completed ----------

def test_expired_session_range_rules(client, monkeypatch):
    payload = make_bytes(300)
    sid = create_session(client, payload, 100)["session_id"]
    assert put_range(client, sid, 0, 99, payload).status_code == 201
    later = datetime.now(timezone.utc) + timedelta(hours=2)
    monkeypatch.setattr(clock, "utcnow", lambda: later)
    # identical replay that adds no coverage stays a no-op success
    replay = put_range(client, sid, 0, 99, payload)
    assert replay.status_code == 200
    assert replay.json()["duplicate"] is True
    assert put_range(client, sid, 10, 19, payload).status_code == 200
    # any request containing new bytes is rejected as a whole
    resp = put_range(client, sid, 50, 149, payload)
    assert resp.status_code == 410
    assert resp.json()["error"]["code"] == "SESSION_EXPIRED"
    # conflicting bytes are reported as conflicts
    bad = bytearray(payload[0:100])
    bad[5] ^= 0xFF
    resp = put_range(client, sid, 0, 99, payload, body=bytes(bad))
    assert resp.status_code == 409
    assert resp.json()["error"]["code"] == "RANGE_CONFLICT"
    assert missing_ranges(client, sid) == [{"start": 100, "end": 299}]


def test_completed_session_range_replay(client):
    payload = make_bytes(200)
    sid = create_session(client, payload, 100)["session_id"]
    assert put_range(client, sid, 0, 199, payload).status_code == 201
    assert client.post(f"/sessions/{sid}/finalize").status_code == 200
    replay = put_range(client, sid, 0, 199, payload)
    assert replay.status_code == 200
    assert replay.json()["duplicate"] is True
    bad = bytearray(payload[0:200])
    bad[7] ^= 0xFF
    resp = put_range(client, sid, 0, 199, payload, body=bytes(bad))
    assert resp.status_code == 409
    assert resp.json()["error"]["code"] == "RANGE_CONFLICT"


# ---------- concurrency ----------

def test_concurrent_identical_ranges_jointly_cover(client):
    payload = make_bytes(2000)
    sid = create_session(client, payload, 100)["session_id"]

    with ThreadPoolExecutor(max_workers=2) as pool:
        first = pool.submit(put_range, client, sid, 0, 999, payload)
        second = pool.submit(put_range, client, sid, 500, 1499, payload)
        resp1, resp2 = first.result(), second.result()

    assert resp1.status_code in (200, 201), resp1.text
    assert resp2.status_code in (200, 201), resp2.text
    assert missing_ranges(client, sid) == [{"start": 1500, "end": 1999}]


def test_concurrent_conflicting_ranges_exactly_one_commits(client):
    payload = make_bytes(1000)
    sid = create_session(client, payload, 100)["session_id"]
    other = bytearray(payload)
    other[100] ^= 0xFF

    with ThreadPoolExecutor(max_workers=2) as pool:
        fut_a = pool.submit(put_range, client, sid, 0, 499, payload)
        fut_b = pool.submit(put_range, client, sid, 0, 499, payload, body=bytes(other[0:500]))
        resp_a, resp_b = fut_a.result(), fut_b.result()

    codes = sorted((resp_a.status_code, resp_b.status_code))
    assert codes == [201, 409], (resp_a.text, resp_b.text)
    loser = resp_a if resp_a.status_code == 409 else resp_b
    assert loser.json()["error"]["code"] == "RANGE_CONFLICT"
    assert loser.json()["error"]["details"]["first_conflict_offset"] == 100
    # the failed request left no bytes behind: winner content replays as duplicate
    winner_payload = payload if resp_a.status_code == 201 else bytes(other)
    replay = put_range(client, sid, 0, 499, payload, body=winner_payload[0:500])
    assert replay.status_code == 200
    assert missing_ranges(client, sid) == [{"start": 500, "end": 999}]


def test_concurrent_chunk_and_range_conflict(client):
    payload = make_bytes(1000)
    sid = create_session(client, payload, 100)["session_id"]
    bad_chunk = bytearray(payload[0:100])
    bad_chunk[10] ^= 0xFF

    with ThreadPoolExecutor(max_workers=2) as pool:
        fut_chunk = pool.submit(put_chunk, client, sid, 0, bytes(bad_chunk))
        fut_range = pool.submit(put_range, client, sid, 0, 99, payload)
        resp_chunk, resp_range = fut_chunk.result(), fut_range.result()

    codes = sorted((resp_chunk.status_code, resp_range.status_code))
    assert codes == [201, 409], (resp_chunk.text, resp_range.text)
    loser = resp_chunk if resp_chunk.status_code == 409 else resp_range
    assert loser.json()["error"]["code"] == "RANGE_CONFLICT"
    assert loser.json()["error"]["details"]["first_conflict_offset"] == 10
    # state matches exactly one serial order: the winner's bytes are stored
    winner_body = bytes(bad_chunk) if resp_chunk.status_code == 201 else payload[0:100]
    replay = put_range(client, sid, 0, 99, payload, body=winner_body)
    assert replay.status_code == 200
