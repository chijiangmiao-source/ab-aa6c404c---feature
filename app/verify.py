"""One-shot acceptance client: exercises the full resume flow against a live API.

Two scenarios run end to end against the same API:

1. legacy fixed-chunk protocol (unchanged behaviour);
2. mixed protocol: arbitrary byte ranges interleaved with fixed chunks,
   partial-cover semantics, idempotent replays and a byte-exact 409 conflict.

Run with:  API_BASE_URL=http://api:8000 python -m app.verify
Exit code 0 means every check passed.
"""

from __future__ import annotations

import hashlib
import os
import sys
import time
from datetime import datetime, timedelta, timezone

import httpx

BASE_URL = os.environ.get("API_BASE_URL", "http://127.0.0.1:8000").rstrip("/")
CHUNK_SIZE = 1024 * 1024
FILE_SIZE = CHUNK_SIZE * 4 + 12345  # 5 chunks, last one short
MAX_RANGE = 8 * 1024 * 1024


class VerifyFailure(Exception):
    pass


def check(condition: bool, message: str) -> None:
    if not condition:
        raise VerifyFailure(message)


def payload(size: int) -> bytes:
    out = bytearray()
    counter = 0
    while len(out) < size:
        out += hashlib.sha256(f"verify-payload:{counter}".encode()).digest()
        counter += 1
    return bytes(out[:size])


def sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def wait_for_api(client: httpx.Client, timeout: float = 60.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            if client.get("/healthz").status_code == 200:
                return
        except httpx.HTTPError:
            pass
        time.sleep(0.5)
    raise VerifyFailure(f"API at {BASE_URL} did not become healthy within {timeout:.0f}s")


def expect_error(resp: httpx.Response, status: int, code: str) -> dict:
    check(resp.status_code == status, f"expected HTTP {status}, got {resp.status_code}: {resp.text}")
    body = resp.json()
    check(isinstance(body.get("error"), dict), f"error envelope missing: {body}")
    check(
        body["error"].get("code") == code,
        f"expected error code {code}, got {body['error'].get('code')}",
    )
    return body


def put_chunk(client: httpx.Client, sid: str, index: int, body: bytes, digest: str) -> httpx.Response:
    return client.put(f"/sessions/{sid}/chunks/{index}", content=body, headers={"X-Chunk-SHA256": digest})


def put_range(client: httpx.Client, sid: str, start: int, end: int, body: bytes, total: int) -> httpx.Response:
    return client.put(
        f"/sessions/{sid}/ranges",
        content=body,
        headers={
            "Content-Range": f"bytes {start}-{end}/{total}",
            "X-Range-SHA256": sha256(body),
        },
    )


def create(client: httpx.Client, data: bytes, chunk_size: int) -> str:
    resp = client.post(
        "/sessions",
        json={
            "file_size": len(data),
            "chunk_size": chunk_size,
            "file_sha256": sha256(data),
            "expires_at": (datetime.now(timezone.utc) + timedelta(minutes=15)).isoformat(),
        },
    )
    check(resp.status_code == 201, f"create session failed: {resp.status_code} {resp.text}")
    return resp.json()["session_id"]


def scenario_legacy_chunks(client: httpx.Client) -> None:
    data = payload(FILE_SIZE)
    chunks = [data[i * CHUNK_SIZE : (i + 1) * CHUNK_SIZE] for i in range(-(-FILE_SIZE // CHUNK_SIZE))]
    digests = [sha256(c) for c in chunks]
    sid = create(client, data, CHUNK_SIZE)
    print(f"[verify] legacy session {sid} created ({len(chunks)} chunks, {FILE_SIZE} bytes)")

    # Invalid chunks must be rejected and never recorded.
    expect_error(put_chunk(client, sid, 0, chunks[0], digests[1]), 400, "CHUNK_DIGEST_MISMATCH")
    expect_error(put_chunk(client, sid, len(chunks), chunks[0], digests[0]), 400, "CHUNK_INDEX_OUT_OF_RANGE")
    truncated = chunks[0][:-1]
    expect_error(put_chunk(client, sid, 0, truncated, sha256(truncated)), 400, "CHUNK_SIZE_MISMATCH")
    status = client.get(f"/sessions/{sid}").json()
    check(status["received_count"] == 0, "rejected chunks must not be recorded")

    # Simulate a dropped connection: only the first 3 chunks go out.
    for i in range(3):
        resp = put_chunk(client, sid, i, chunks[i], digests[i])
        check(resp.status_code == 201, f"chunk {i} upload failed: {resp.text}")
    status = client.get(f"/sessions/{sid}").json()
    check(status["missing_chunks"] == [3, 4], f"expected missing [3, 4], got {status['missing_chunks']}")
    check(
        status["missing_ranges"] == [{"start": 3 * CHUNK_SIZE, "end": FILE_SIZE - 1}],
        f"bad missing_ranges: {status['missing_ranges']}",
    )

    # Idempotent replay of the same bytes; conflicting bytes must get 409.
    resp = put_chunk(client, sid, 1, chunks[1], digests[1])
    check(resp.status_code == 200 and resp.json()["duplicate"] is True,
          f"idempotent replay failed: {resp.status_code} {resp.text}")
    other = bytes(len(chunks[1]))
    expect_error(put_chunk(client, sid, 1, other, sha256(other)), 409, "CHUNK_CONFLICT")

    # Finalize too early must fail and list the missing chunks.
    body = expect_error(client.post(f"/sessions/{sid}/finalize"), 409, "CHUNKS_INCOMPLETE")
    check(body["error"]["details"]["missing_chunks"] == [3, 4], "finalize error must list missing chunks")

    # Resume, finalize (idempotent), download and verify.
    for i in (3, 4):
        resp = put_chunk(client, sid, i, chunks[i], digests[i])
        check(resp.status_code == 201, f"resume chunk {i} failed: {resp.text}")
    check(client.get(f"/sessions/{sid}").json()["missing_chunks"] == [], "bitmap should be complete")
    resp = client.post(f"/sessions/{sid}/finalize")
    check(resp.status_code == 200, f"finalize failed: {resp.text}")
    check(resp.json()["final_sha256"] == sha256(data), "final SHA-256 mismatch")
    again = client.post(f"/sessions/{sid}/finalize")
    check(again.status_code == 200 and again.json()["final_sha256"] == sha256(data),
          "finalize must be idempotent")
    resp = client.get(f"/sessions/{sid}/artifact")
    check(resp.status_code == 200 and resp.content == data, "artifact bytes differ")
    check(resp.headers.get("x-file-sha256") == sha256(data), "artifact digest header mismatch")
    print("[verify] legacy fixed-chunk scenario passed")


def scenario_mixed_ranges(client: httpx.Client) -> None:
    # 7 MiB so two 8 MiB-capable ranges suffice, straddling 1 MiB chunk borders.
    size = 7 * 1024 * 1024 + 333
    data = payload(size)
    chunk = 1024 * 1024
    sid = create(client, data, chunk)

    status = client.get(f"/sessions/{sid}").json()
    check(status["missing_ranges"] == [{"start": 0, "end": size - 1}], "fresh session misses everything")
    check("missing_chunks" in status and len(status["missing_chunks"]) == 8, "legacy fields retained")

    # Header / boundary / digest / size validation never moves progress.
    expect_error(
        put_range(client, sid, 0, 9, data[0:10], size + 1),
        400, "RANGE_TOTAL_MISMATCH",
    )
    expect_error(
        put_range(client, sid, size - 1, size + 9, data[0:11], size),
        400, "RANGE_OUT_OF_BOUNDS",
    )
    bad_digest = client.put(
        f"/sessions/{sid}/ranges",
        content=data[0:10],
        headers={"Content-Range": f"bytes 0-9/{size}", "X-Range-SHA256": "z" * 64},
    )
    expect_error(bad_digest, 400, "INVALID_RANGE_DIGEST")
    wrong_digest = client.put(
        f"/sessions/{sid}/ranges",
        content=data[0:10],
        headers={"Content-Range": f"bytes 0-9/{size}", "X-Range-SHA256": sha256(b"x" * 10)},
    )
    expect_error(wrong_digest, 400, "RANGE_DIGEST_MISMATCH")
    short_body = client.put(
        f"/sessions/{sid}/ranges",
        content=data[0:9],
        headers={"Content-Range": f"bytes 0-9/{size}", "X-Range-SHA256": sha256(data[0:9])},
    )
    expect_error(short_body, 400, "RANGE_SIZE_MISMATCH")
    check(client.get(f"/sessions/{sid}").json()["missing_ranges"] == [{"start": 0, "end": size - 1}],
          "rejected range requests must not move progress")

    # Range 1: unaligned across several chunk boundaries.
    r = put_range(client, sid, 500_000, 4_500_000, data[500_000 : 4_500_001], size)
    check(r.status_code == 201 and r.json()["duplicate"] is False, f"range 1 failed: {r.text}")
    status = client.get(f"/sessions/{sid}").json()
    # chunks 1..3 ([1 MiB, 4 MiB)) are fully inside the range; chunks 0 and 4
    # are only partially covered and must still count as missing
    check(status["missing_chunks"] == [0, 4, 5, 6, 7],
          f"fully covered chunks should complete, partial ones stay missing: {status['missing_chunks']}")
    check(status["missing_ranges"] == [
        {"start": 0, "end": 499_999},
        {"start": 4_500_001, "end": size - 1},
    ], f"merged/sorted gaps wrong: {status['missing_ranges']}")

    # A legacy chunk fully inside the range: identical overlap -> 200 duplicate,
    # and a conflicting chunk -> 409 RANGE_CONFLICT with the exact offset.
    r = put_chunk(client, sid, 2, data[2 * chunk : 3 * chunk], sha256(data[2 * chunk : 3 * chunk]))
    check(r.status_code == 200 and r.json()["duplicate"] is True, f"chunk-in-range replay: {r.text}")
    wrong = bytearray(data[2 * chunk : 3 * chunk])
    wrong[123] ^= 0xFF
    err = expect_error(
        put_chunk(client, sid, 2, bytes(wrong), sha256(bytes(wrong))),
        409, "RANGE_CONFLICT",
    )
    check(err["error"]["details"]["conflict_offset"] == 2 * chunk + 123,
          f"conflict offset wrong: {err['error']['details']}")

    # Idempotent range replay is 200; a range with one differing byte is 409,
    # reports the first absolute conflict offset and stores nothing new.
    r = put_range(client, sid, 500_000, 4_500_000, data[500_000 : 4_500_001], size)
    check(r.status_code == 200 and r.json()["duplicate"] is True, f"range replay: {r.text}")
    clash = bytearray(data[4_000_000 : 5_000_000])  # overlaps [4.0M, 4.5M]
    clash[123_456] ^= 0x01
    err = expect_error(
        put_range(client, sid, 4_000_000, 4_999_999, bytes(clash), size),
        409, "RANGE_CONFLICT",
    )
    check(err["error"]["details"]["conflict_offset"] == 4_123_456,
          f"range conflict offset wrong: {err['error']['details']}")
    status = client.get(f"/sessions/{sid}").json()
    check({"start": 4_500_001, "end": size - 1} in status["missing_ranges"],
          "rejected range must not leave its non-overlapping bytes behind")

    # Fill the head with an overlapping range + a legacy chunk interleaved.
    r = put_range(client, sid, 0, 1_500_000, data[0 : 1_500_001], size)
    check(r.status_code == 201, f"head range failed: {r.text}")
    r = put_chunk(client, sid, 4, data[4 * chunk : 5 * chunk], sha256(data[4 * chunk : 5 * chunk]))
    check(r.status_code in (200, 201), f"chunk 4 failed: {r.text}")

    # Tail: one range completes the file (adjacent intervals must merge away).
    r = put_range(client, sid, 4_500_001, size - 1, data[4_500_001:size], size)
    check(r.status_code == 201, f"tail range failed: {r.text}")
    status = client.get(f"/sessions/{sid}").json()
    check(status["missing_chunks"] == [] and status["missing_ranges"] == [],
          f"coverage should be complete: {status}")
    check(status["received_count"] == 8, "all logical chunks must count as received")

    resp = client.post(f"/sessions/{sid}/finalize")
    check(resp.status_code == 200, f"mixed finalize failed: {resp.text}")
    check(resp.json()["final_sha256"] == sha256(data), "mixed-assembly digest mismatch")
    art = client.get(f"/sessions/{sid}/artifact")
    check(art.status_code == 200 and art.content == data, "mixed-assembly bytes differ")
    check(art.headers.get("x-file-sha256") == sha256(data), "artifact digest header mismatch")

    # A replay after completion adds nothing; 8 MiB cap enforced up front.
    r = put_range(client, sid, 0, 9, data[0:10], size)
    check(r.status_code == 200 and r.json()["duplicate"] is True, f"post-complete replay: {r.text}")
    big = b"\x00" * (MAX_RANGE + 1)
    expect_error(
        put_range(client, sid, 0, MAX_RANGE, big, size),
        400, "RANGE_OUT_OF_BOUNDS",  # past EOF for this file; cap is checked on in-file ranges
    )
    print("[verify] mixed range/chunk scenario passed")


def scenario_range_size_cap(client: httpx.Client) -> None:
    # A file larger than 8 MiB so an in-bounds over-cap request is possible.
    size = 16 * 1024 * 1024
    sid = create(client, b"\x00" * size, 1024 * 1024)
    body = b"\xab" * (MAX_RANGE + 1)
    resp = client.put(
        f"/sessions/{sid}/ranges",
        content=body,
        headers={
            "Content-Range": f"bytes 0-{MAX_RANGE}/{size}",
            "X-Range-SHA256": sha256(body),
        },
    )
    expect_error(resp, 413, "RANGE_TOO_LARGE")
    status = client.get(f"/sessions/{sid}").json()
    check(status["missing_ranges"] == [{"start": 0, "end": size - 1}], "oversized body must not be stored")
    print("[verify] 8 MiB range cap enforced")


def main() -> int:
    started = time.monotonic()
    with httpx.Client(base_url=BASE_URL, timeout=60.0) as client:
        wait_for_api(client)
        print(f"[verify] API healthy at {BASE_URL}")
        scenario_legacy_chunks(client)
        scenario_mixed_ranges(client)
        scenario_range_size_cap(client)

    print(f"[verify] ALL CHECKS PASSED in {time.monotonic() - started:.1f}s")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except VerifyFailure as exc:
        print(f"[verify] FAILED: {exc}", file=sys.stderr)
        sys.exit(1)
    except httpx.HTTPError as exc:
        print(f"[verify] FAILED: HTTP error: {exc}", file=sys.stderr)
        sys.exit(1)
