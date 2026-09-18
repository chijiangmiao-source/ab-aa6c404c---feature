"""On-disk layout for chunk bodies, range segments and published artifacts.

Every write goes to a temp file, is fsynced, and is then moved into place with
os.replace so a crash never leaves a half-written file at a final path.

Range uploads are stored as *immutable segments* that hold only the bytes not
already covered by earlier confirmed writes. A single accepted request may
produce several segment files (one per uncovered slice); each file contains a
contiguous byte run and is named after its absolute start offset. Segments are
never modified in place, which makes the body/metadata crash windows
deterministic: a segment that never reached the database is an orphan removed by
reconcile, and a segment referenced by the database is durable data.
"""

from __future__ import annotations

import hashlib
import os
import uuid
from pathlib import Path
from typing import AsyncIterable, Iterable

from .errors import ApiError

_COPY_BUFFER = 1024 * 1024


class ChunkStore:
    def __init__(self, root: Path):
        self.root = root
        self.chunks_root = root / "chunks"
        self.segments_root = root / "segments"
        self.artifacts_dir = root / "artifacts"
        self.chunks_root.mkdir(parents=True, exist_ok=True)
        self.segments_root.mkdir(parents=True, exist_ok=True)
        self.artifacts_dir.mkdir(parents=True, exist_ok=True)

    def chunk_dir(self, session_id: str) -> Path:
        return self.chunks_root / session_id

    def chunk_path(self, session_id: str, index: int) -> Path:
        return self.chunk_dir(session_id) / f"{index:08d}.chunk"

    def segment_dir(self, session_id: str) -> Path:
        return self.segments_root / session_id

    def segment_path(self, session_id: str, start: int, token: str) -> Path:
        return self.segment_dir(session_id) / f"{start:016x}-{token}.seg"

    def artifact_path(self, session_id: str) -> Path:
        return self.artifacts_dir / f"{session_id}.bin"

    async def write_body_tmp(
        self,
        target_dir: Path,
        stream: AsyncIterable[bytes],
        max_bytes: int | None = None,
    ) -> tuple[Path, int, str]:
        """Stream a request body to a temp file; returns (tmp_path, size, sha256).

        The caller validates size/digest and overlaps before publishing anything
        from the temp file; nothing is visible at a final path until then. When
        ``max_bytes`` is given the stream is aborted as soon as it exceeds it.
        """
        target_dir.mkdir(parents=True, exist_ok=True)
        tmp = target_dir / f".{uuid.uuid4().hex}.tmp"
        hasher = hashlib.sha256()
        size = 0
        try:
            with open(tmp, "wb") as fh:
                async for part in stream:
                    if not part:
                        continue
                    size += len(part)
                    if max_bytes is not None and size > max_bytes:
                        raise ApiError(
                            413,
                            "RANGE_TOO_LARGE",
                            f"a single range request may carry at most {max_bytes} bytes",
                            {"max_bytes": max_bytes},
                        )
                    hasher.update(part)
                    fh.write(part)
                fh.flush()
                os.fsync(fh.fileno())
        except BaseException:
            tmp.unlink(missing_ok=True)
            raise
        return tmp, size, hasher.hexdigest()

    def persist_slices(
        self, tmp: Path, slices: list[tuple[int, int, int]], session_id: str
    ) -> list[dict]:
        """Copy uncovered slices of a request-body temp file into segment files.

        ``slices`` items are ``(abs_start, rel_start, length)`` with offsets
        relative to the request body. Each slice becomes one fsynced immutable
        segment, moved into place atomically. Returns metadata rows (without
        session id / received_at) for the caller to commit transactionally.
        """
        if not slices:
            return []
        target_dir = self.segment_dir(session_id)
        target_dir.mkdir(parents=True, exist_ok=True)
        records: list[dict] = []
        created: list[Path] = []
        part_tmp: Path | None = None
        try:
            with open(tmp, "rb") as src:
                for abs_start, rel_start, length in slices:
                    token = uuid.uuid4().hex
                    final = self.segment_path(session_id, abs_start, token)
                    part_tmp = final.with_name(f".{uuid.uuid4().hex}.tmp")
                    src.seek(rel_start)
                    remaining = length
                    with open(part_tmp, "wb") as out:
                        while remaining:
                            block = src.read(min(_COPY_BUFFER, remaining))
                            if not block:
                                raise EOFError("request temp file ended before slice was copied")
                            out.write(block)
                            remaining -= len(block)
                        out.flush()
                        os.fsync(out.fileno())
                    os.replace(part_tmp, final)
                    part_tmp = None
                    created.append(final)
                    records.append(
                        {
                            "start_off": abs_start,
                            "end_off": abs_start + length,
                            "path": str(final),
                            "size": length,
                        }
                    )
            _fsync_dir(target_dir)
        except BaseException:
            if part_tmp is not None:
                self.discard(part_tmp)
            for path in created:
                self.discard(path)
            raise
        return records

    def commit_tmp(self, tmp: Path, final: Path) -> None:
        os.replace(tmp, final)
        _fsync_dir(final.parent)

    @staticmethod
    def read_at(path: Path, offset: int, length: int) -> bytes:
        """Read an exact byte run from an immutable chunk/segment file."""
        if length <= 0:
            return b""
        with open(path, "rb") as fh:
            fh.seek(offset)
            data = fh.read(length)
        if len(data) != length:
            raise EOFError(f"{path} is short: wanted {length} bytes at {offset}, got {len(data)}")
        return data

    def assemble_plan_to_tmp(self, plan: Iterable[tuple[Path, int, int]]) -> tuple[Path, int, str]:
        """Stream ``(path, offset, length)`` reads in order; returns (tmp, size, sha256).

        The plan is derived from authoritative coverage and covers the whole
        file exactly once, so no second full copy of the input is ever made.
        """
        tmp = self.artifacts_dir / f".{uuid.uuid4().hex}.tmp"
        hasher = hashlib.sha256()
        size = 0
        try:
            with open(tmp, "wb") as out:
                for path, offset, length in plan:
                    with open(path, "rb") as src:
                        src.seek(offset)
                        remaining = length
                        while remaining:
                            block = src.read(min(_COPY_BUFFER, remaining))
                            if not block:
                                raise EOFError(
                                    f"{path} ended during assembly; wanted {length} at {offset}"
                                )
                            hasher.update(block)
                            out.write(block)
                            written = len(block)
                            size += written
                            remaining -= written
                out.flush()
                os.fsync(out.fileno())
        except BaseException:
            tmp.unlink(missing_ok=True)
            raise
        return tmp, size, hasher.hexdigest()

    def publish(self, tmp: Path, session_id: str) -> Path:
        final = self.artifact_path(session_id)
        os.replace(tmp, final)
        _fsync_dir(final.parent)
        return final

    @staticmethod
    def discard(path: Path) -> None:
        try:
            path.unlink(missing_ok=True)
        except OSError:
            pass

    def purge_tmp(self) -> None:
        for directory in (self.artifacts_dir, self.chunks_root, self.segments_root):
            for entry in directory.rglob("*.tmp"):
                entry.unlink(missing_ok=True)


def _fsync_dir(path: Path) -> None:
    flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
    fd = os.open(path, flags)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)
