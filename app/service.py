"""Core upload/resume/finalize logic shared by the HTTP routes.

Authoritative byte coverage of a session is the union of:

- confirmed chunk rows (each covers its whole logical-chunk interval), and
- committed byte-segment rows (arbitrary [start, end] extents).

The persisted bitmap is a derived cache: bit ``i`` is set iff chunk ``i``'s
interval is fully covered.  It is maintained in the same SQLite transaction as
every coverage change and rebuilt from rows by :func:`reconcile` on startup.

Commit protocol for both upload protocols (file bodies and SQLite cannot share
a transaction):

1. stream the body to a fsynced temp file (never visible in coverage);
2. under the process-wide DB lock: re-read fresh state, run the whole-request
   conflict check against stored bytes, then write *novel* bytes to fsynced
   immutable files (``os.replace`` into place);
3. insert rows + update the bitmap in one SQLite transaction — only after this
   commit is the request confirmed;
4. a crash anywhere before step 3 leaves orphan files that ``reconcile``
   removes; a crash after step 3 is fully recovered and replays return the
   idempotent duplicate result.
"""

from __future__ import annotations

import re
import uuid
from datetime import datetime
from pathlib import Path
from typing import AsyncIterable

from . import clock
from .bitmap import count_set, is_set, missing_indices, new_bitmap, set_bit
from .coverage import Piece, Source, complement, interval_covers, merge_intervals, plan_pieces
from .db import Database
from .errors import ApiError
from .schemas import CreateSessionRequest
from .storage import ChunkStore, SizeLimitExceeded

_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_CONTENT_RANGE_RE = re.compile(r"^bytes[ \t]+(\d+)-(\d+)/(\d+)$", re.IGNORECASE)

#: A single range request may persist at most this many bytes (8 MiB).
MAX_RANGE_BYTES = 8 * 1024 * 1024


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

    # ---- chunks (fixed-boundary protocol) ----

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

        tmp, size, actual = await self.store.write_chunk_tmp(session_id, stream)
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
                fresh = self._fresh_session(session_id)
                existing = self.db.get_chunk(session_id, index)
                if existing is not None:
                    if existing["sha256"] == digest:
                        return self._chunk_receipt(fresh, existing, duplicate=True), 200
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
                # The chunk row is absent, but byte segments may already cover
                # part (or all) of this interval: the whole request must agree
                # with them before anything is written.
                c_start = index * session["chunk_size"]
                c_end = c_start + expected - 1
                seg_rows = self.db.list_segments(session_id)
                sources = [
                    Source(s["start_off"], s["end_off"], Path(s["path"]))
                    for s in seg_rows
                    if s["end_off"] >= c_start and s["start_off"] <= c_end
                ]
                pieces = plan_pieces(sources, c_start, c_end)
                conflict = self._first_conflict(tmp, pieces, c_start)
                if conflict is not None:
                    raise ApiError(
                        409,
                        "RANGE_CONFLICT",
                        "chunk bytes differ from already stored byte ranges;"
                        " nothing was written",
                        {"chunk_index": index, "first_conflict_offset": conflict},
                    )
                if not complement(pieces, c_start, c_end):
                    # Identical segments already cover the whole interval.
                    record = {"chunk_index": index, "size": size, "sha256": digest}
                    return self._chunk_receipt(fresh, record, duplicate=True), 200
                if self.is_expired(fresh):
                    raise ApiError(
                        410,
                        "SESSION_EXPIRED",
                        "session has expired; new chunks are rejected",
                        {"expires_at": fresh["expires_at"]},
                    )
                if fresh["status"] != "active":
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
                bitmap = bytearray(fresh["bitmap"])
                set_bit(bitmap, index)
                fresh["bitmap"] = bytes(bitmap)
                # Segments fully contained in this chunk are now redundant:
                # drop their rows in the same transaction, their files after.
                contained = [
                    s["seg_id"]
                    for s in seg_rows
                    if s["start_off"] >= c_start and s["end_off"] <= c_end
                ]
                self.db.insert_chunk_with_bitmap(record, fresh["bitmap"], remove_segment_ids=contained)
                for seg_id in contained:
                    self.store.discard(self.store.segment_path(session_id, seg_id))
                return self._chunk_receipt(fresh, record, duplicate=False), 201
        finally:
            if not committed:
                self.store.discard(tmp)

    # ---- ranges (arbitrary byte-range protocol) ----

    async def upload_range(
        self,
        session_id: str,
        content_range: str,
        declared_digest: str,
        stream: AsyncIterable[bytes],
    ) -> tuple[dict, int]:
        session = self.get_session_or_404(session_id)
        file_size = session["file_size"]
        start, end, length = self._parse_content_range(content_range, file_size)
        digest = declared_digest.strip().lower()
        if not _SHA256_RE.fullmatch(digest):
            raise ApiError(
                400,
                "INVALID_RANGE_DIGEST",
                "X-Range-SHA256 must be 64 hexadecimal characters",
                {"received": declared_digest},
            )
        try:
            tmp, size, actual = await self.store.write_range_tmp(session_id, stream, length)
        except SizeLimitExceeded:
            raise ApiError(
                400,
                "RANGE_SIZE_MISMATCH",
                f"body is larger than the declared range of {length} bytes",
                {"start": start, "end": end, "declared_length": length},
            )
        try:
            if size != length:
                raise ApiError(
                    400,
                    "RANGE_SIZE_MISMATCH",
                    f"range declares {length} bytes but the body carried {size}",
                    {
                        "start": start,
                        "end": end,
                        "declared_length": length,
                        "actual_size": size,
                    },
                )
            if actual != digest:
                raise ApiError(
                    400,
                    "RANGE_DIGEST_MISMATCH",
                    "body SHA-256 does not match X-Range-SHA256; nothing was stored",
                    {
                        "start": start,
                        "end": end,
                        "declared_sha256": digest,
                        "actual_sha256": actual,
                    },
                )
            with self.db.lock:
                fresh = self._fresh_session(session_id)
                chunk_rows = self.db.list_chunks(session_id)
                seg_rows = self.db.list_segments(session_id)
                sources = self._row_sources(fresh, chunk_rows, seg_rows)
                pieces = plan_pieces(sources, start, end)
                conflict = self._first_conflict(tmp, pieces, start)
                if conflict is not None:
                    raise ApiError(
                        409,
                        "RANGE_CONFLICT",
                        "overlapping bytes differ from stored content; nothing was written",
                        {"start": start, "end": end, "first_conflict_offset": conflict},
                    )
                novel = complement(pieces, start, end)
                if not novel:
                    return self._range_receipt(fresh, start, end, length, digest, [], duplicate=True), 200
                if self.is_expired(fresh):
                    raise ApiError(
                        410,
                        "SESSION_EXPIRED",
                        "session has expired; only identical replays are accepted",
                        {"expires_at": fresh["expires_at"]},
                    )
                if fresh["status"] != "active":
                    raise ApiError(
                        409,
                        "SESSION_ALREADY_COMPLETED",
                        "session is already completed and immutable",
                    )
                received_at = clock.utcnow().isoformat()
                records = []
                for ns, ne in novel:
                    path, seg_id, seg_sha = self.store.write_segment_file(
                        session_id, tmp, ns - start, ne - ns + 1
                    )
                    records.append(
                        {
                            "session_id": session_id,
                            "seg_id": seg_id,
                            "start_off": ns,
                            "end_off": ne,
                            "size": ne - ns + 1,
                            "sha256": seg_sha,
                            "path": str(path),
                            "received_at": received_at,
                        }
                    )
                # Flip bits of chunks that just became fully covered.
                bitmap = bytearray(fresh["bitmap"])
                merged = merge_intervals(
                    [(s["start_off"], s["end_off"]) for s in seg_rows] + novel
                )
                starts = [s for s, _ in merged]
                chunk_size = fresh["chunk_size"]
                for i in range(start // chunk_size, end // chunk_size + 1):
                    if is_set(bitmap, i):
                        continue
                    c_start = i * chunk_size
                    c_end = min((i + 1) * chunk_size, file_size) - 1
                    if interval_covers(merged, starts, c_start, c_end):
                        set_bit(bitmap, i)
                fresh["bitmap"] = bytes(bitmap)
                self.db.insert_segments_with_bitmap(session_id, records, fresh["bitmap"])
                return self._range_receipt(fresh, start, end, length, digest, novel, duplicate=False), 201
        finally:
            self.store.discard(tmp)

    # ---- finalize / artifact ----

    def finalize(self, session_id: str) -> dict:
        session = self.get_session_or_404(session_id)
        if session["status"] == "completed" and self.store.artifact_path(session_id).exists():
            return self._finalize_receipt(session)

        total = session["total_chunks"]
        missing = missing_indices(session["bitmap"], total)
        if missing:
            details = {
                "missing_chunks": missing,
                "received_count": total - len(missing),
                "total_chunks": total,
            }
            if self.is_expired(session):
                raise ApiError(
                    410,
                    "SESSION_EXPIRED",
                    "session expired with chunks still missing",
                    {**details, "expires_at": session["expires_at"]},
                )
            raise ApiError(409, "CHUNKS_INCOMPLETE", "cannot finalize; chunks are missing", details)

        # Coverage is complete, so the committed file set is frozen: no upload
        # can add novel bytes any more, and assembly reads immutable files.
        chunk_rows = self.db.list_chunks(session_id)
        seg_rows = self.db.list_segments(session_id)
        lost_chunks = [r["chunk_index"] for r in chunk_rows if not Path(r["path"]).exists()]
        lost_segments = [r["seg_id"] for r in seg_rows if not Path(r["path"]).exists()]
        if lost_chunks or lost_segments:
            raise ApiError(
                409,
                "CHUNKS_INCOMPLETE",
                "confirmed files are missing on disk",
                {
                    "missing_chunks": lost_chunks,
                    "missing_segments": lost_segments,
                    "total_chunks": total,
                },
            )

        sources = self._row_sources(session, chunk_rows, seg_rows)
        pieces = plan_pieces(sources, 0, session["file_size"] - 1)
        if sum(p.length for p in pieces) != session["file_size"]:
            raise ApiError(
                409,
                "CHUNKS_INCOMPLETE",
                "coverage rows do not span the whole file",
                {"total_chunks": total},
            )
        tmp, size, digest = self.store.assemble_pieces_to_tmp(pieces)
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

    def _fresh_session(self, session_id: str) -> dict:
        """Re-read the session row inside the DB lock (sessions are never deleted)."""
        session = self.db.get_session(session_id)
        if session is None:
            raise ApiError(
                404,
                "SESSION_NOT_FOUND",
                f"no such session: {session_id}",
                {"session_id": session_id},
            )
        return session

    @staticmethod
    def _parse_content_range(header: str, file_size: int) -> tuple[int, int, int]:
        """Validate Content-Range against the session; returns (start, end, length)."""
        match = _CONTENT_RANGE_RE.fullmatch(header.strip())
        if not match:
            raise ApiError(
                400,
                "INVALID_CONTENT_RANGE",
                "Content-Range must have the form 'bytes <start>-<end>/<total>'",
                {"received": header},
            )
        start, end, total = (int(group) for group in match.groups())
        if start > end:
            raise ApiError(
                400,
                "INVALID_CONTENT_RANGE",
                "range start must not exceed range end",
                {"received": header},
            )
        if total != file_size:
            raise ApiError(
                400,
                "RANGE_TOTAL_MISMATCH",
                "Content-Range total must equal the session file_size",
                {"declared_total": total, "file_size": file_size},
            )
        if end >= file_size:
            raise ApiError(
                400,
                "RANGE_OUT_OF_BOUNDS",
                f"range end {end} is outside the {file_size}-byte file",
                {"start": start, "end": end, "file_size": file_size},
            )
        length = end - start + 1
        if length > MAX_RANGE_BYTES:
            raise ApiError(
                413,
                "RANGE_TOO_LARGE",
                f"a single range request may write at most {MAX_RANGE_BYTES} bytes",
                {"declared_length": length, "max_range_bytes": MAX_RANGE_BYTES},
            )
        return start, end, length

    @staticmethod
    def _row_sources(session: dict, chunk_rows: list[dict], seg_rows: list[dict]) -> list[Source]:
        """Coverage sources backed by real files, from confirmed rows."""
        file_size = session["file_size"]
        chunk_size = session["chunk_size"]
        sources = []
        for row in chunk_rows:
            c_start = row["chunk_index"] * chunk_size
            c_end = min((row["chunk_index"] + 1) * chunk_size, file_size) - 1
            sources.append(Source(c_start, c_end, Path(row["path"])))
        for seg in seg_rows:
            sources.append(Source(seg["start_off"], seg["end_off"], Path(seg["path"])))
        return sources

    def _first_conflict(self, body_tmp: Path, pieces: list[Piece], body_base: int) -> int | None:
        """First absolute offset where the request body disagrees with stored bytes."""
        for piece in pieces:
            rel = self.store.first_mismatch(
                body_tmp, piece.start - body_base, piece.path, piece.src_offset, piece.length
            )
            if rel is not None:
                return piece.start + rel
        return None

    @staticmethod
    def expected_chunk_size(session: dict, index: int) -> int:
        if index == session["total_chunks"] - 1:
            return session["file_size"] - session["chunk_size"] * (session["total_chunks"] - 1)
        return session["chunk_size"]

    @staticmethod
    def is_expired(session: dict) -> bool:
        return clock.utcnow() >= datetime.fromisoformat(session["expires_at"])

    def _derived_status(self, session: dict) -> str:
        if session["status"] == "completed":
            return "completed"
        return "expired" if self.is_expired(session) else "active"

    def _missing_ranges(self, session: dict, seg_rows: list[dict]) -> list[tuple[int, int]]:
        """Uncovered absolute intervals: sorted, disjoint, adjacency-merged."""
        file_size = session["file_size"]
        chunk_size = session["chunk_size"]
        bitmap = session["bitmap"]
        sources = []
        for i in range(session["total_chunks"]):
            if is_set(bitmap, i):
                sources.append(
                    Source(i * chunk_size, min((i + 1) * chunk_size, file_size) - 1, None)
                )
        for seg in seg_rows:
            sources.append(Source(seg["start_off"], seg["end_off"], None))
        pieces = plan_pieces(sources, 0, file_size - 1)
        return complement(pieces, 0, file_size - 1)

    def public_session(self, session: dict) -> dict:
        total = session["total_chunks"]
        missing = missing_indices(session["bitmap"], total)
        status = self._derived_status(session)
        seg_rows = self.db.list_segments(session["session_id"])
        missing_ranges = self._missing_ranges(session, seg_rows)
        return {
            "session_id": session["session_id"],
            "status": status,
            "file_size": session["file_size"],
            "chunk_size": session["chunk_size"],
            "total_chunks": total,
            "file_sha256": session["file_sha256"],
            "received_count": total - len(missing),
            "missing_chunks": missing,
            "missing_ranges": [{"start": s, "end": e} for s, e in missing_ranges],
            "expires_at": session["expires_at"],
            "created_at": session["created_at"],
            "completed_at": session["completed_at"],
            "final_sha256": session["final_sha256"],
            "artifact_url": f"/sessions/{session['session_id']}/artifact" if status == "completed" else None,
        }

    def _chunk_receipt(self, session: dict, record: dict, duplicate: bool) -> dict:
        total = session["total_chunks"]
        return {
            "session_id": session["session_id"],
            "chunk_index": record["chunk_index"],
            "size": record["size"],
            "sha256": record["sha256"],
            "duplicate": duplicate,
            "received_count": count_set(session["bitmap"], total),
            "total_chunks": total,
        }

    def _range_receipt(
        self,
        session: dict,
        start: int,
        end: int,
        size: int,
        digest: str,
        stored: list[tuple[int, int]],
        duplicate: bool,
    ) -> dict:
        total = session["total_chunks"]
        return {
            "session_id": session["session_id"],
            "range": {"start": start, "end": end},
            "size": size,
            "sha256": digest,
            "duplicate": duplicate,
            "stored_ranges": [{"start": s, "end": e} for s, e in stored],
            "received_count": count_set(session["bitmap"], total),
            "total_chunks": total,
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
    - chunk/segment files without a matching row (crashed before commit) are removed;
    - the persisted bitmap is rebuilt from the surviving rows: a chunk's bit is
      set iff its interval is fully covered by surviving chunks and/or segments;
    - leftover temp files are removed.

    Net effect: confirmed bytes are never reported missing, and unconfirmed
    bytes are never reported as received.
    """
    for session in db.list_sessions():
        session_id = session["session_id"]
        file_size = session["file_size"]
        chunk_size = session["chunk_size"]
        total = session["total_chunks"]
        confirmed: set[int] = set()
        for row in db.list_chunks(session_id):
            path = Path(row["path"])
            if path.exists() and path.stat().st_size == row["size"]:
                confirmed.add(row["chunk_index"])
            else:
                db.delete_chunk(session_id, row["chunk_index"])
        confirmed_segments: list[dict] = []
        for row in db.list_segments(session_id):
            path = Path(row["path"])
            if path.exists() and path.stat().st_size == row["size"]:
                confirmed_segments.append(row)
            else:
                db.delete_segment(session_id, row["seg_id"])
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
                    if index not in confirmed:
                        entry.unlink()
        segments_dir = store.segments_dir(session_id)
        if segments_dir.exists():
            live = {row["seg_id"] for row in confirmed_segments}
            for entry in segments_dir.iterdir():
                if entry.suffix == ".tmp":
                    entry.unlink()
                elif entry.suffix == ".seg" and entry.stem not in live:
                    entry.unlink()
        bitmap = new_bitmap(total)
        for index in confirmed:
            set_bit(bitmap, index)
        merged = merge_intervals([(r["start_off"], r["end_off"]) for r in confirmed_segments])
        if merged:
            starts = [s for s, _ in merged]
            for index in range(total):
                if is_set(bitmap, index):
                    continue
                c_start = index * chunk_size
                c_end = min((index + 1) * chunk_size, file_size) - 1
                if interval_covers(merged, starts, c_start, c_end):
                    set_bit(bitmap, index)
        db.update_bitmap(session_id, bytes(bitmap))
    store.purge_tmp()
