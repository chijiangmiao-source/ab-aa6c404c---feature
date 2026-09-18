"""Core upload/resume/finalize logic shared by the HTTP protocols.

Two upload protocols interoperate on one session:

- fixed chunks:  ``PUT /sessions/{id}/chunks/{index}`` (legacy semantics kept);
- arbitrary byte ranges: ``PUT /sessions/{id}/ranges`` with ``Content-Range``.

Authoritative coverage is the union of confirmed chunk intervals and confirmed
segment intervals. Both protocols run the same whole-request conflict check
under one in-process lock: every overlapping byte is compared before anything
is committed, so a rejected request leaves no bytes and no progress behind.

Body files (chunks / immutable range segments) are fsynced and renamed into
place *before* the SQLite commit; a crash in that window leaves an orphan file
that reconcile deletes, while a committed row always points at durable bytes.
"""

from __future__ import annotations

import bisect
import re
import uuid
from datetime import datetime
from pathlib import Path
from typing import AsyncIterable

from . import clock
from .bitmap import new_bitmap, set_bit
from .coverage import complement, covered_chunk_indices, merge_intervals, uncovered_parts
from .db import Database
from .errors import ApiError
from .schemas import CreateSessionRequest
from .storage import ChunkStore

_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_CONTENT_RANGE_RE = re.compile(r"^\s*bytes\s+(\d+)\s*-\s*(\d+)\s*/\s*(\d+)\s*$", re.IGNORECASE)
MAX_RANGE_BYTES = 8 * 1024 * 1024


class Coverage:
    """Read-only view over the confirmed coverage of one session."""

    def __init__(self, session: dict, chunk_rows: list[dict], seg_rows: list[dict]):
        self.chunk_size = session["chunk_size"]
        self.file_size = session["file_size"]
        self.total_chunks = session["total_chunks"]
        self.chunks = {row["chunk_index"]: row for row in chunk_rows}
        self.segments = sorted(seg_rows, key=lambda r: (r["start_off"], r["end_off"]))
        self._seg_starts = [r["start_off"] for r in self.segments]

    def chunk_bounds(self, index: int) -> tuple[int, int]:
        start = index * self.chunk_size
        return start, min(start + self.chunk_size, self.file_size)

    def intervals(self) -> list[tuple[int, int]]:
        iv = [self.chunk_bounds(i) for i in self.chunks]
        iv.extend((r["start_off"], r["end_off"]) for r in self.segments)
        return merge_intervals(iv)

    def covered_indices(self) -> set[int]:
        return covered_chunk_indices(self.intervals(), self.chunk_size, self.file_size)

    def _locate(self, offset: int) -> tuple[Path, int, int]:
        """Locate ``offset`` in a backing file.

        Returns ``(path, relative_offset, source_end_absolute)``; the last item
        is the first offset not inside that file, so callers can stream one
        contiguous run per read.
        """
        index = offset // self.chunk_size
        chunk = self.chunks.get(index)
        if chunk is not None:
            start, end = self.chunk_bounds(index)
            if start <= offset < end:
                return Path(chunk["path"]), offset - start, end
        pos = bisect.bisect_right(self._seg_starts, offset) - 1
        if pos >= 0:
            seg = self.segments[pos]
            if seg["start_off"] <= offset < seg["end_off"]:
                return Path(seg["path"]), offset - seg["start_off"], seg["end_off"]
        raise LookupError(f"offset {offset} is not covered by any confirmed upload")

    def read(self, start: int, length: int) -> bytes:
        """Read ``length`` confirmed bytes starting at ``start``."""
        parts: list[bytes] = []
        cursor = start
        end = start + length
        while cursor < end:
            path, rel, source_end = self._locate(cursor)
            run_end = min(end, source_end)
            parts.append(ChunkStore.read_at(path, rel, run_end - cursor))
            cursor = run_end
        return b"".join(parts)


class UploadService:
    def __init__(self, db: Database, store: ChunkStore):
        self.db = db
        self.store = store

    # ---- sessions ----

    def create_session(self, req: CreateSessionRequest) -> dict:
        now = clock.utcnow()
        if req.expires_at <= now:
            raise ApiError(
                422,
                "SESSION_EXPIRES_IN_PAST",
                "expires_at must be in the future",
                {"expires_at": req.expires_at.isoformat()},
            )
        total = -(-req.file_size // req.chunk_size)  # ceil division
        session_id = uuid.uuid4().hex
        self.db.create_session(
            {
                "session_id": session_id,
                "file_size": req.file_size,
                "chunk_size": req.chunk_size,
                "total_chunks": total,
                "file_sha256": req.file_sha256,
                "status": "active",
                "bitmap": bytes(new_bitmap(total)),
                "expires_at": req.expires_at.isoformat(),
                "created_at": now.isoformat(),
            }
        )
        return self.public_session(self.get_session_or_404(session_id))

    def status(self, session_id: str) -> dict:
        return self.public_session(self.get_session_or_404(session_id))

    # ---- fixed chunks (legacy protocol) ----

    async def upload_chunk(
        self,
        session_id: str,
        raw_index: str,
        declared_digest: str,
        stream: AsyncIterable[bytes],
    ) -> tuple[dict, int]:
        session = self.get_session_or_404(session_id)
        total = session["total_chunks"]
        try:
            index = int(raw_index)
        except ValueError:
            index = -1
        if index < 0 or index >= total:
            raise ApiError(
                400,
                "CHUNK_INDEX_OUT_OF_RANGE",
                f"chunk index {raw_index!r} is out of range; valid indices are 0..{total - 1}",
                {"chunk_index": raw_index, "total_chunks": total},
            )
        digest = declared_digest.strip().lower()
        if not _SHA256_RE.fullmatch(digest):
            raise ApiError(
                400,
                "INVALID_CHUNK_DIGEST",
                "X-Chunk-SHA256 must be 64 hexadecimal characters",
                {"received": declared_digest},
            )

        tmp, size, actual = await self.store.write_body_tmp(self.store.chunk_dir(session_id), stream)
        committed = False
        try:
            expected = self.expected_chunk_size(session, index)
            if size != expected:
                raise ApiError(
                    400,
                    "CHUNK_SIZE_MISMATCH",
                    f"chunk {index} must be exactly {expected} bytes, got {size}",
                    {"chunk_index": index, "expected_size": expected, "actual_size": size},
                )
            if actual != digest:
                raise ApiError(
                    400,
                    "CHUNK_DIGEST_MISMATCH",
                    "chunk body SHA-256 does not match X-Chunk-SHA256; chunk was discarded",
                    {"chunk_index": index, "declared_sha256": digest, "actual_sha256": actual},
                )
            with self.db.lock:
                session = self.db.get_session(session_id)
                start, end = self.chunk_byte_span(session, index)
                coverage = self._coverage(session)

                # Legacy idempotency/conflict against an already confirmed chunk.
                existing = self.db.get_chunk(session_id, index)
                if existing is not None:
                    if existing["sha256"] == digest:
                        return self._chunk_receipt(session, existing, duplicate=True), 200
                    raise ApiError(
                        409,
                        "CHUNK_CONFLICT",
                        "chunk index already holds different content; the stored chunk is unchanged",
                        {
                            "chunk_index": index,
                            "stored_sha256": existing["sha256"],
                            "rejected_sha256": digest,
                        },
                    )

                # Whole-request atomic check against saved byte segments:
                # any single differing byte rejects the entire request.
                conflict = self._first_conflict(tmp, start, end, coverage)
                if conflict is not None:
                    raise self._range_conflict_error(conflict)

                covered = merge_intervals(coverage.intervals())
                adds = uncovered_parts(start, end, covered)
                if not adds:
                    record = {
                        "chunk_index": index,
                        "size": size,
                        "sha256": digest,
                    }
                    return self._chunk_receipt(session, record, duplicate=True), 200
                if self.is_expired(session):
                    raise ApiError(
                        410,
                        "SESSION_EXPIRED",
                        "session has expired; new bytes are rejected",
                        {"expires_at": session["expires_at"]},
                    )
                if session["status"] != "active":
                    raise ApiError(
                        409,
                        "SESSION_ALREADY_COMPLETED",
                        "session is already completed and immutable",
                    )

                final_path = self.store.chunk_path(session_id, index)
                self.store.commit_tmp(tmp, final_path)
                committed = True
                record = {
                    "session_id": session_id,
                    "chunk_index": index,
                    "size": size,
                    "sha256": digest,
                    "path": str(final_path),
                    "received_at": clock.utcnow().isoformat(),
                }
                bitmap = self._bitmap_for(session, coverage, [(start, end)])
                self.db.insert_chunk_with_bitmap(record, bitmap)
                session = self.db.get_session(session_id)
                return self._chunk_receipt(session, record, duplicate=False), 201
        finally:
            if not committed:
                self.store.discard(tmp)

    # ---- arbitrary byte ranges (new protocol) ----

    async def upload_range(
        self,
        session_id: str,
        content_range: str,
        declared_digest: str,
        stream: AsyncIterable[bytes],
    ) -> tuple[dict, int]:
        session = self.get_session_or_404(session_id)

        match = _CONTENT_RANGE_RE.fullmatch(content_range or "")
        if not match:
            raise ApiError(
                400,
                "INVALID_CONTENT_RANGE",
                "Content-Range must look like 'bytes start-end/total'",
                {"received": content_range},
            )
        start, end, total = (int(match.group(1)), int(match.group(2)), int(match.group(3)))
        digest = declared_digest.strip().lower()
        if not _SHA256_RE.fullmatch(digest):
            raise ApiError(
                400,
                "INVALID_RANGE_DIGEST",
                "X-Range-SHA256 must be 64 hexadecimal characters",
                {"received": declared_digest},
            )
        if total != session["file_size"]:
            raise ApiError(
                400,
                "RANGE_TOTAL_MISMATCH",
                "Content-Range total must equal the session file size",
                {"declared_total": total, "file_size": session["file_size"]},
            )
        if start > end or end >= total:
            raise ApiError(
                400,
                "RANGE_OUT_OF_BOUNDS",
                "range bounds must satisfy 0 <= start <= end < total",
                {"start": start, "end": end, "file_size": total},
            )
        declared_length = end - start + 1
        if declared_length > MAX_RANGE_BYTES:
            raise ApiError(
                413,
                "RANGE_TOO_LARGE",
                f"a single range request may carry at most {MAX_RANGE_BYTES} bytes",
                {"max_bytes": MAX_RANGE_BYTES, "declared_length": declared_length},
            )

        tmp, size, actual = await self.store.write_body_tmp(
            self.store.segment_dir(session_id), stream, max_bytes=MAX_RANGE_BYTES
        )
        try:
            if size != declared_length:
                raise ApiError(
                    400,
                    "RANGE_SIZE_MISMATCH",
                    "body length must equal end-start+1 from Content-Range",
                    {"declared_length": declared_length, "actual_size": size},
                )
            if actual != digest:
                raise ApiError(
                    400,
                    "RANGE_DIGEST_MISMATCH",
                    "body SHA-256 does not match X-Range-SHA256; range was discarded",
                    {"declared_sha256": digest, "actual_sha256": actual},
                )

            with self.db.lock:
                session = self.db.get_session(session_id)
                coverage = self._coverage(session)
                covered = merge_intervals(coverage.intervals())
                req_end = end + 1

                conflict = self._first_conflict(tmp, start, req_end, coverage)
                if conflict is not None:
                    raise self._range_conflict_error(conflict)

                missing_slices = uncovered_parts(start, req_end, covered)
                if not missing_slices:
                    # The whole range was already covered by identical bytes:
                    # idempotent replay, valid even when expired/completed.
                    return self._range_receipt(session, start, end, digest, duplicate=True), 200
                if self.is_expired(session):
                    raise ApiError(
                        410,
                        "SESSION_EXPIRED",
                        "session has expired; only identical replays are accepted",
                        {"expires_at": session["expires_at"]},
                    )
                if session["status"] != "active":
                    raise ApiError(
                        409,
                        "SESSION_ALREADY_COMPLETED",
                        "session is already completed and immutable",
                    )

                slices = [
                    (abs_start, abs_start - start, abs_end - abs_start)
                    for abs_start, abs_end in missing_slices
                ]
                records = self.store.persist_slices(tmp, slices, session_id)
                now = clock.utcnow().isoformat()
                for rec in records:
                    rec["session_id"] = session_id
                    rec["sha256"] = digest
                    rec["received_at"] = now
                try:
                    bitmap = self._bitmap_for(
                        session, coverage, [(r["start_off"], r["end_off"]) for r in records]
                    )
                    self.db.commit_segments(session_id, records, bitmap)
                except BaseException:
                    # Metadata never committed: the renamed slices are orphans.
                    for rec in records:
                        self.store.discard(Path(rec["path"]))
                    raise
                session = self.db.get_session(session_id)
                return self._range_receipt(session, start, end, digest, duplicate=False), 201
        finally:
            self.store.discard(tmp)

    # ---- finalize / artifact ----

    def finalize(self, session_id: str) -> dict:
        session = self.get_session_or_404(session_id)
        if session["status"] == "completed" and self.store.artifact_path(session_id).exists():
            return self._finalize_receipt(session)

        coverage = self._coverage(session)
        covered = coverage.intervals()
        missing = [
            i for i in range(session["total_chunks"]) if i not in coverage.covered_indices()
        ]
        if missing:
            details = {
                "missing_chunks": missing,
                "received_count": session["total_chunks"] - len(missing),
                "total_chunks": session["total_chunks"],
            }
            if self.is_expired(session):
                raise ApiError(
                    410,
                    "SESSION_EXPIRED",
                    "session expired with bytes still missing",
                    {**details, "expires_at": session["expires_at"]},
                )
            raise ApiError(409, "CHUNKS_INCOMPLETE", "cannot finalize; chunks are missing", details)

        # Stream the authoritative coverage, interval by interval, straight into
        # the artifact temp file. The plan covers each offset exactly once and
        # depends only on the coverage set, not on arrival order or restarts.
        plan: list[tuple[Path, int, int]] = []
        for iv_start, iv_end in covered:
            cursor = iv_start
            while cursor < iv_end:
                path, rel, source_end = coverage._locate(cursor)
                run_end = min(iv_end, source_end)
                if not path.exists():
                    raise ApiError(
                        409,
                        "CHUNKS_INCOMPLETE",
                        "stored upload data is missing on disk; progress metadata retained",
                        {"missing_path": str(path)},
                    )
                plan.append((path, rel, run_end - cursor))
                cursor = run_end

        tmp, size, digest = self.store.assemble_plan_to_tmp(plan)
        if size != session["file_size"] or digest != session["file_sha256"]:
            self.store.discard(tmp)
            raise ApiError(
                422,
                "INTEGRITY_MISMATCH",
                "assembled file does not match the declared SHA-256; uploaded bytes are kept",
                {
                    "declared_sha256": session["file_sha256"],
                    "assembled_sha256": digest,
                    "declared_size": session["file_size"],
                    "assembled_size": size,
                },
            )
        final = self.store.publish(tmp, session_id)
        completed_at = clock.utcnow().isoformat()
        self.db.mark_completed(session_id, completed_at, digest, str(final))
        return self._finalize_receipt(self.get_session_or_404(session_id))

    def artifact_file(self, session_id: str) -> tuple[Path, str]:
        session = self.get_session_or_404(session_id)
        path = self.store.artifact_path(session_id)
        if session["status"] != "completed" or not path.exists():
            raise ApiError(
                409,
                "ARTIFACT_NOT_READY",
                "no published artifact for this session",
                {"status": self._derived_status(session)},
            )
        return path, session["final_sha256"]

    # ---- helpers ----

    def get_session_or_404(self, session_id: str) -> dict:
        session = self.db.get_session(session_id)
        if session is None:
            raise ApiError(
                404,
                "SESSION_NOT_FOUND",
                f"no such session: {session_id}",
                {"session_id": session_id},
            )
        return session

    def _coverage(self, session: dict) -> Coverage:
        return Coverage(
            session,
            self.db.list_chunks(session["session_id"]),
            self.db.list_ranges(session["session_id"]),
        )

    @staticmethod
    def chunk_byte_span(session: dict, index: int) -> tuple[int, int]:
        start = index * session["chunk_size"]
        return start, min(start + session["chunk_size"], session["file_size"])

    @staticmethod
    def expected_chunk_size(session: dict, index: int) -> int:
        if index == session["total_chunks"] - 1:
            return session["file_size"] - session["chunk_size"] * (session["total_chunks"] - 1)
        return session["chunk_size"]

    @staticmethod
    def is_expired(session: dict) -> bool:
        return clock.utcnow() >= datetime.fromisoformat(session["expires_at"])

    def _first_conflict(
        self, body_tmp: Path, start: int, end: int, coverage: Coverage
    ) -> int | None:
        """Absolute offset of the first overlapping byte that differs, else None.

        Reads only the request body and the overlapping confirmed runs, never
        the whole file.
        """
        for a, b in coverage.intervals():
            ov_start, ov_end = max(a, start), min(b, end)
            if ov_start >= ov_end:
                continue
            length = ov_end - ov_start
            offered = ChunkStore.read_at(body_tmp, ov_start - start, length)
            stored = coverage.read(ov_start, length)
            if offered != stored:
                for k in range(length):
                    if offered[k] != stored[k]:
                        return ov_start + k
        return None

    @staticmethod
    def _range_conflict_error(offset: int) -> ApiError:
        return ApiError(
            409,
            "RANGE_CONFLICT",
            f"overlapping byte at offset {offset} differs; the request was rejected wholesale",
            {"conflict_offset": offset},
        )

    def _bitmap_for(
        self, session: dict, coverage: Coverage, extra_intervals: list[tuple[int, int]]
    ) -> bytes:
        intervals = coverage.intervals() + list(extra_intervals)
        indices = covered_chunk_indices(intervals, session["chunk_size"], session["file_size"])
        bitmap = new_bitmap(session["total_chunks"])
        for index in indices:
            set_bit(bitmap, index)
        return bytes(bitmap)

    def _derived_status(self, session: dict) -> str:
        if session["status"] == "completed":
            return "completed"
        return "expired" if self.is_expired(session) else "active"

    def public_session(self, session: dict) -> dict:
        total = session["total_chunks"]
        coverage = self._coverage(session)
        covered = coverage.intervals()
        missing_indices = [
            i for i in range(total) if i not in coverage.covered_indices()
        ]
        missing_ranges = [
            {"start": a, "end": b - 1} for a, b in complement(covered, session["file_size"])
        ]
        status = self._derived_status(session)
        return {
            "session_id": session["session_id"],
            "status": status,
            "file_size": session["file_size"],
            "chunk_size": session["chunk_size"],
            "total_chunks": total,
            "file_sha256": session["file_sha256"],
            "received_count": total - len(missing_indices),
            "missing_chunks": missing_indices,
            "missing_ranges": missing_ranges,
            "expires_at": session["expires_at"],
            "created_at": session["created_at"],
            "completed_at": session["completed_at"],
            "final_sha256": session["final_sha256"],
            "artifact_url": f"/sessions/{session['session_id']}/artifact" if status == "completed" else None,
        }

    def _chunk_receipt(self, session: dict, record: dict, duplicate: bool) -> dict:
        total = session["total_chunks"]
        received = len(self._coverage(session).covered_indices())
        return {
            "session_id": session["session_id"],
            "chunk_index": record["chunk_index"],
            "size": record["size"],
            "sha256": record["sha256"],
            "duplicate": duplicate,
            "received_count": received,
            "total_chunks": total,
        }

    def _range_receipt(
        self, session: dict, start: int, end: int, digest: str, duplicate: bool
    ) -> dict:
        coverage = self._coverage(session)
        total = session["total_chunks"]
        received = len(coverage.covered_indices())
        missing_ranges = [
            {"start": a, "end": b - 1}
            for a, b in complement(coverage.intervals(), session["file_size"])
        ]
        return {
            "session_id": session["session_id"],
            "start": start,
            "end": end,
            "size": end - start + 1,
            "sha256": digest,
            "duplicate": duplicate,
            "received_count": received,
            "total_chunks": total,
            "missing_ranges": missing_ranges,
        }

    @staticmethod
    def _finalize_receipt(session: dict) -> dict:
        session_id = session["session_id"]
        return {
            "session_id": session_id,
            "status": "completed",
            "file_size": session["file_size"],
            "final_sha256": session["final_sha256"],
            "artifact_size": session["file_size"],
            "artifact_url": f"/sessions/{session_id}/artifact",
            "completed_at": session["completed_at"],
        }


def reconcile(db: Database, store: ChunkStore) -> None:
    """Rebuild durable state after a (possibly unclean) restart.

    - chunk/segment rows whose files vanished or have a wrong size are dropped;
    - body files without a matching row (crashed before commit) are removed;
    - the persisted bitmap is rebuilt from surviving chunks *and* segments;
    - leftover temp files are removed.

    Net effect: confirmed bytes are never reported missing, and unconfirmed
    bytes are never reported as received. Databases from before range support
    simply have an (empty) ``ranges`` table created in place: no bodies are
    rewritten and existing sessions of every status keep working.
    """
    for session in db.list_sessions():
        session_id = session["session_id"]

        confirmed_chunks: set[int] = set()
        for row in db.list_chunks(session_id):
            path = Path(row["path"])
            if path.exists() and path.stat().st_size == row["size"]:
                confirmed_chunks.add(row["chunk_index"])
            else:
                db.delete_chunk(session_id, row["chunk_index"])

        chunk_dir = store.chunk_dir(session_id)
        if chunk_dir.exists():
            for entry in chunk_dir.iterdir():
                if entry.suffix == ".tmp":
                    entry.unlink()
                elif entry.suffix == ".chunk":
                    try:
                        index = int(entry.stem)
                    except ValueError:
                        entry.unlink()
                        continue
                    if index not in confirmed_chunks:
                        entry.unlink()

        confirmed_segments: set[Path] = set()
        for row in db.list_ranges(session_id):
            path = Path(row["path"])
            if path.exists() and path.stat().st_size == row["size"]:
                confirmed_segments.add(path)
            else:
                db.delete_range(row["id"])

        seg_dir = store.segment_dir(session_id)
        if seg_dir.exists():
            for entry in seg_dir.iterdir():
                if entry.suffix in (".tmp", ".seg") and entry not in confirmed_segments:
                    entry.unlink()

        rows = db.list_chunks(session_id) + db.list_ranges(session_id)
        intervals = [
            (
                row["chunk_index"] * session["chunk_size"],
                min(
                    (row["chunk_index"] + 1) * session["chunk_size"],
                    session["file_size"],
                ),
            )
            if "chunk_index" in row
            else (row["start_off"], row["end_off"])
            for row in rows
        ]
        indices = covered_chunk_indices(
            intervals, session["chunk_size"], session["file_size"]
        )
        bitmap = new_bitmap(session["total_chunks"])
        for index in indices:
            set_bit(bitmap, index)
        db.update_bitmap(session_id, bytes(bitmap))

    store.purge_tmp()
