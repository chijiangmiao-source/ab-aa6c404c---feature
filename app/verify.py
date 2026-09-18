"""One-shot acceptance client: exercises the full resume flow against a live API.

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


def put_range(
    client: httpx.Client, sid: str, start: int, end: int, total: int, body: bytes, digest: str
) -> httpx.Response:
    return client.put(
        f"/sessions/{sid}/ranges",
        content=body,
        headers={"Content-Range": f"bytes {start}-{end}/{total}", "X-Range-SHA256": digest},
    )


def new_session(client: httpx.Client, file_size: int, chunk_size: int, file_sha: str) -> dict:
    resp = client.post(
        "/sessions",
        json={
            "file_size": file_size,
            "chunk_size": chunk_size,
            "file_sha256": file_sha,
            "expires_at": (datetime.now(timezone.utc) + timedelta(minutes=15)).isoformat(),
        },
    )
    check(resp.status_code == 201, f"create session failed: {resp.status_code} {resp.text}")
    return resp.json()


def main() -> int:
    started = time.monotonic()
    with httpx.Client(base_url=BASE_URL, timeout=30.0) as client:
        wait_for_api(client)
        print(f"[verify] API healthy at {BASE_URL}")

        data = payload(FILE_SIZE)
        file_sha = sha256(data)
        chunks = [data[i * CHUNK_SIZE : (i + 1) * CHUNK_SIZE] for i in range(-(-FILE_SIZE // CHUNK_SIZE))]
        digests = [sha256(c) for c in chunks]

        session = new_session(client, FILE_SIZE, CHUNK_SIZE, file_sha)
        sid = session["session_id"]
        check(session["total_chunks"] == len(chunks), "total_chunks mismatch")
        check(session["missing_chunks"] == list(range(len(chunks))), "fresh session must miss every chunk")
        check(
            session["missing_ranges"] == [{"start": 0, "end": FILE_SIZE - 1}],
            "fresh session must miss the whole byte range",
        )
        print(f"[verify] session {sid} created ({len(chunks)} chunks, {FILE_SIZE} bytes)")

        # Invalid chunks must be rejected and never recorded.
        expect_error(put_chunk(client, sid, 0, chunks[0], digests[1]), 400, "CHUNK_DIGEST_MISMATCH")
        expect_error(put_chunk(client, sid, len(chunks), chunks[0], digests[0]), 400, "CHUNK_INDEX_OUT_OF_RANGE")
        truncated = chunks[0][:-1]
        expect_error(put_chunk(client, sid, 0, truncated, sha256(truncated)), 400, "CHUNK_SIZE_MISMATCH")
        status = client.get(f"/sessions/{sid}").json()
        check(status["received_count"] == 0, "rejected chunks must not be recorded")
        print("[verify] digest/size/index violations rejected and not recorded")

        # Simulate a dropped connection: only the first 3 chunks go out.
        for i in range(3):
            resp = put_chunk(client, sid, i, chunks[i], digests[i])
            check(resp.status_code == 201, f"chunk {i} upload failed: {resp.text}")
        status = client.get(f"/sessions/{sid}").json()
        check(status["missing_chunks"] == [3, 4], f"expected missing [3, 4], got {status['missing_chunks']}")
        check(
            status["missing_ranges"] == [{"start": 3 * CHUNK_SIZE, "end": FILE_SIZE - 1}],
            f"missing_ranges must mirror the missing chunks, got {status['missing_ranges']}",
        )
        print("[verify] partial upload visible after 'interruption': missing [3, 4]")

        # Idempotent replay of the same bytes; conflicting bytes must get 409.
        resp = put_chunk(client, sid, 1, chunks[1], digests[1])
        check(resp.status_code == 200 and resp.json()["duplicate"] is True,
              f"idempotent replay failed: {resp.status_code} {resp.text}")
        other = bytes(len(chunks[1]))  # same length, different content
        expect_error(put_chunk(client, sid, 1, other, sha256(other)), 409, "CHUNK_CONFLICT")
        print("[verify] idempotent replay accepted, conflicting content rejected with 409")

        # Finalize too early must fail and list the missing chunks.
        body = expect_error(client.post(f"/sessions/{sid}/finalize"), 409, "CHUNKS_INCOMPLETE")
        check(body["error"]["details"]["missing_chunks"] == [3, 4], "finalize error must list missing chunks")

        # Resume: upload the remaining chunks.
        for i in (3, 4):
            resp = put_chunk(client, sid, i, chunks[i], digests[i])
            check(resp.status_code == 201, f"resume chunk {i} failed: {resp.text}")
        status = client.get(f"/sessions/{sid}").json()
        check(status["missing_chunks"] == [], "all chunks should be received now")
        print("[verify] resumed upload completed the bitmap")

        # Finalize, re-finalize (idempotent), download and verify the artifact.
        resp = client.post(f"/sessions/{sid}/finalize")
        check(resp.status_code == 200, f"finalize failed: {resp.text}")
        check(resp.json()["final_sha256"] == file_sha, "final SHA-256 mismatch")
        again = client.post(f"/sessions/{sid}/finalize")
        check(again.status_code == 200 and again.json()["final_sha256"] == file_sha,
              "finalize must be idempotent")
        resp = client.get(f"/sessions/{sid}/artifact")
        check(resp.status_code == 200, f"artifact download failed: {resp.text}")
        check(resp.content == data, "artifact bytes differ from the original file")
        check(resp.headers.get("x-file-sha256") == file_sha, "artifact digest header mismatch")
        status = client.get(f"/sessions/{sid}").json()
        check(status["status"] == "completed", "session must be completed")
        check(status["missing_ranges"] == [], "completed session must have no missing ranges")
        print(f"[verify] artifact published and verified (sha256={file_sha[:16]}...)")

        # ---- arbitrary byte-range resume, interleaved with fixed chunks ----
        r_size = 2 * CHUNK_SIZE + 300_000  # 3 logical chunks
        r_data = payload(r_size)
        r_sha = sha256(r_data)
        r_session = new_session(client, r_size, CHUNK_SIZE, r_sha)
        rsid = r_session["session_id"]
        check(r_session["missing_ranges"] == [{"start": 0, "end": r_size - 1}],
              "fresh range session must miss the whole byte range")
        print(f"[verify] range session {rsid} created ({r_size} bytes)")

        # Header/digest/size violations must not move any progress.
        expect_error(put_range(client, rsid, 0, 9, 123, r_data[0:10], sha256(r_data[0:10])),
                     400, "RANGE_TOTAL_MISMATCH")
        expect_error(
            client.put(f"/sessions/{rsid}/ranges", content=r_data[0:10],
                       headers={"Content-Range": "bytes 0-9", "X-Range-SHA256": sha256(r_data[0:10])}),
            400, "INVALID_CONTENT_RANGE")
        expect_error(put_range(client, rsid, r_size - 1, r_size, r_size, r_data[-1:] + b"x",
                               sha256(r_data[-1:] + b"x")),
                     400, "RANGE_OUT_OF_BOUNDS")
        expect_error(put_range(client, rsid, 0, 9, r_size, r_data[0:10], sha256(b"wrong")),
                     400, "RANGE_DIGEST_MISMATCH")
        short = r_data[0:5]
        expect_error(
            client.put(f"/sessions/{rsid}/ranges", content=short,
                       headers={"Content-Range": f"bytes 0-9/{r_size}", "X-Range-SHA256": sha256(short)}),
            400, "RANGE_SIZE_MISMATCH")
        status = client.get(f"/sessions/{rsid}").json()
        check(status["missing_ranges"] == [{"start": 0, "end": r_size - 1}],
              "rejected range requests must not change progress")
        print("[verify] range header/digest/size violations rejected and not recorded")

        # Commit a byte range, replay it idempotently.
        resp = put_range(client, rsid, 0, 99, r_size, r_data[0:100], sha256(r_data[0:100]))
        check(resp.status_code == 201 and resp.json()["duplicate"] is False,
              f"range commit failed: {resp.text}")
        replay = put_range(client, rsid, 0, 99, r_size, r_data[0:100], sha256(r_data[0:100]))
        check(replay.status_code == 200 and replay.json()["duplicate"] is True,
              "identical range replay must be a duplicate no-op")
        status = client.get(f"/sessions/{rsid}").json()
        check(status["missing_ranges"][0]["start"] == 100, "missing_ranges must advance")
        check(status["missing_chunks"] == [0, 1, 2],
              "partially covered chunk must stay in missing_chunks")

        # A conflicting range is rejected atomically with the first conflicting offset.
        bad = bytearray(r_data[50:200])
        bad[10] ^= 0xFF  # absolute offset 60
        resp = put_range(client, rsid, 50, 199, r_size, bytes(bad), sha256(bytes(bad)))
        check(resp.status_code == 409, f"expected 409, got {resp.status_code}: {resp.text}")
        check(resp.json()["error"]["code"] == "RANGE_CONFLICT", "conflict must use RANGE_CONFLICT")
        check(resp.json()["error"]["details"]["first_conflict_offset"] == 60,
              "first_conflict_offset must be the absolute offset of the first differing byte")
        status = client.get(f"/sessions/{rsid}").json()
        check(status["missing_ranges"][0]["start"] == 100,
              "conflicting request must not leave bytes behind")
        print("[verify] idempotent range replay accepted; conflicting range rejected atomically")

        # Interleave protocols: a fixed chunk PUT fills the rest of chunk 0.
        resp = put_chunk(client, rsid, 0, r_data[0:CHUNK_SIZE], sha256(r_data[0:CHUNK_SIZE]))
        check(resp.status_code == 201, f"chunk PUT over segments failed: {resp.text}")
        status = client.get(f"/sessions/{rsid}").json()
        check(status["missing_chunks"] == [1, 2], "chunk 0 must be complete after the fill")

        # A chunk PUT that disagrees with stored segments gets 409 RANGE_CONFLICT.
        seg_start = CHUNK_SIZE + 500
        resp = put_range(client, rsid, seg_start, seg_start + 99, r_size,
                         r_data[seg_start:seg_start + 100], sha256(r_data[seg_start:seg_start + 100]))
        check(resp.status_code == 201, f"range commit failed: {resp.text}")
        bad_chunk = bytearray(r_data[CHUNK_SIZE:2 * CHUNK_SIZE])
        bad_chunk[500] ^= 0xFF  # absolute offset seg_start
        resp = put_chunk(client, rsid, 1, bytes(bad_chunk), sha256(bytes(bad_chunk)))
        check(resp.status_code == 409 and resp.json()["error"]["code"] == "RANGE_CONFLICT",
              f"conflicting chunk PUT must be rejected: {resp.status_code} {resp.text}")
        check(resp.json()["error"]["details"]["first_conflict_offset"] == seg_start,
              "chunk conflict must report the absolute offset")
        resp = put_chunk(client, rsid, 1, r_data[CHUNK_SIZE:2 * CHUNK_SIZE],
                         sha256(r_data[CHUNK_SIZE:2 * CHUNK_SIZE]))
        check(resp.status_code == 201, f"matching chunk PUT failed: {resp.text}")
        print("[verify] chunk PUT over stored segments: conflict 409, matching content fills")

        # Finish the file with one range and finalize from mixed coverage.
        resp = put_range(client, rsid, 2 * CHUNK_SIZE, r_size - 1, r_size,
                         r_data[2 * CHUNK_SIZE:], sha256(r_data[2 * CHUNK_SIZE:]))
        check(resp.status_code == 201, f"final range failed: {resp.text}")
        status = client.get(f"/sessions/{rsid}").json()
        check(status["missing_ranges"] == [] and status["missing_chunks"] == [],
              "mixed coverage must complete the session")
        resp = client.post(f"/sessions/{rsid}/finalize")
        check(resp.status_code == 200 and resp.json()["final_sha256"] == r_sha,
              f"mixed finalize failed: {resp.text}")
        check(client.get(f"/sessions/{rsid}/artifact").content == r_data,
              "mixed-coverage artifact bytes differ")
        print("[verify] mixed chunk+range coverage finalized and verified")

        # A single request may write at most 8 MiB.
        big = new_session(client, 9 * 1024 * 1024, CHUNK_SIZE, sha256(b"big-file-placeholder"))
        bsid = big["session_id"]
        resp = client.put(
            f"/sessions/{bsid}/ranges",
            content=b"x",
            headers={
                "Content-Range": f"bytes 0-{8 * 1024 * 1024}/{9 * 1024 * 1024}",
                "X-Range-SHA256": sha256(b"x"),
            },
        )
        check(resp.status_code == 413 and resp.json()["error"]["code"] == "RANGE_TOO_LARGE",
              f"oversize range must be rejected: {resp.status_code} {resp.text}")
        print("[verify] 8 MiB per-request limit enforced")

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
