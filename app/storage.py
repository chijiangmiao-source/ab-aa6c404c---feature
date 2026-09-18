"""On-disk layout for chunk bodies, byte segments and published artifacts.

Every write goes to a temp file, is fsynced, and is then moved into place with
os.replace so a crash never leaves a half-written file at a final path.  Byte
segments are immutable once committed; only rows in SQLite grant them
coverage, so an orphaned segment file is harmless and is reclaimed by
``reconcile`` on the next startup.
"""

from __future__ import annotations

import hashlib
import os
import uuid
from pathlib import Path
from typing import AsyncIterable

from .coverage import Piece

_COPY_BUFFER = 1024 * 1024


class SizeLimitExceeded(Exception):
    """Raised while streaming a body that grew past its declared limit."""


class ChunkStore:
    def __init__(self, root: Path):
        self.root = root
        self.chunks_root = root / "chunks"
        self.segments_root = root / "segments"
        self.artifacts_dir = root / "artifacts"
        self.chunks_root.mkdir(parents=True, exist_ok=True)
        self.segments_root.mkdir(parents=True, exist_ok=True)
        self.artifacts_dir.mkdir(parents=True, exist_ok=True)

    # ---- paths ----

    def chunk_dir(self, session_id: str) -> Path:
        return self.chunks_root / session_id

    def chunk_path(self, session_id: str, index: int) -> Path:
        return self.chunk_dir(session_id) / f"{index:08d}.chunk"

    def segments_dir(self, session_id: str) -> Path:
        return self.segments_root / session_id

    def segment_path(self, session_id: str, seg_id: str) -> Path:
        return self.segments_dir(session_id) / f"{seg_id}.seg"

    def artifact_path(self, session_id: str) -> Path:
        return self.artifacts_dir / f"{session_id}.bin"

    # ---- request bodies ----

    async def write_chunk_tmp(self, session_id: str, stream: AsyncIterable[bytes]) -> tuple[Path, int, str]:
        """Stream a chunk body to a temp file; returns (tmp_path, size, sha256)."""
        return await self._write_body_tmp(self.chunk_dir(session_id), stream, max_bytes=None)

    async def write_range_tmp(
        self, session_id: str, stream: AsyncIterable[bytes], max_bytes: int
    ) -> tuple[Path, int, str]:
        """Stream a range body to a temp file, aborting past ``max_bytes``.

        The caller validates size/digest before any byte is committed; nothing
        is visible at a final path until then.
        """
        return await self._write_body_tmp(self.segments_dir(session_id), stream, max_bytes=max_bytes)

    @staticmethod
    async def _write_body_tmp(
        target_dir: Path, stream: AsyncIterable[bytes], max_bytes: int | None
    ) -> tuple[Path, int, str]:
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
                        raise SizeLimitExceeded(max_bytes)
                    hasher.update(part)
                    fh.write(part)
                fh.flush()
                os.fsync(fh.fileno())
        except BaseException:
            tmp.unlink(missing_ok=True)
            raise
        return tmp, size, hasher.hexdigest()

    def commit_tmp(self, tmp: Path, final: Path) -> None:
        os.replace(tmp, final)
        _fsync_dir(final.parent)

    # ---- byte segments ----

    def write_segment_file(
        self, session_id: str, body_tmp: Path, body_offset: int, length: int
    ) -> tuple[Path, str, str]:
        """Durably persist one novel sub-range of a request body as a segment file.

        Only the uncovered slice ``body_tmp[body_offset : body_offset + length]``
        is written, so the I/O stays proportional to the genuinely new bytes.
        Returns (final_path, seg_id, sha256-of-slice).
        """
        seg_id = uuid.uuid4().hex
        seg_dir = self.segments_dir(session_id)
        seg_dir.mkdir(parents=True, exist_ok=True)
        tmp = seg_dir / f".{uuid.uuid4().hex}.tmp"
        final = self.segment_path(session_id, seg_id)
        hasher = hashlib.sha256()
        try:
            with open(body_tmp, "rb") as src, open(tmp, "wb") as out:
                src.seek(body_offset)
                remaining = length
                while remaining > 0:
                    block = src.read(min(_COPY_BUFFER, remaining))
                    if not block:
                        raise IOError(f"short read from body temp file {body_tmp}")
                    hasher.update(block)
                    out.write(block)
                    remaining -= len(block)
                out.flush()
                os.fsync(out.fileno())
            os.replace(tmp, final)
            _fsync_dir(seg_dir)
        except BaseException:
            tmp.unlink(missing_ok=True)
            raise
        return final, seg_id, hasher.hexdigest()

    @staticmethod
    def first_mismatch(a_path: Path, a_off: int, b_path: Path, b_off: int, length: int) -> int | None:
        """First differing byte of two equal-length file regions, or None if identical.

        The return value is relative to the start of the regions.  Reads are
        bounded by ``length`` (the actual overlap), never whole files.
        """
        done = 0
        with open(a_path, "rb") as fa, open(b_path, "rb") as fb:
            fa.seek(a_off)
            fb.seek(b_off)
            while done < length:
                step = min(_COPY_BUFFER, length - done)
                ba = fa.read(step)
                bb = fb.read(step)
                if ba != bb:
                    common = min(len(ba), len(bb))
                    for i in range(common):
                        if ba[i] != bb[i]:
                            return done + i
                    if len(ba) != len(bb):
                        return done + common  # one side truncated on disk
                    return None  # both ended together, identical
                done += len(ba)
                if len(ba) < step:
                    break  # both files ended together
        return None

    # ---- assembly / publish ----

    def assemble_pieces_to_tmp(self, pieces: list[Piece]) -> tuple[Path, int, str]:
        """Stream ``pieces`` (absolute-offset order) into one temp artifact.

        Sources are read directly at their offsets — no intermediate copy of
        the whole input is made.  Returns (tmp_path, size, sha256).
        """
        tmp = self.artifacts_dir / f".{uuid.uuid4().hex}.tmp"
        hasher = hashlib.sha256()
        size = 0
        handles: dict[Path, object] = {}
        try:
            with open(tmp, "wb") as out:
                for piece in pieces:
                    fh = handles.get(piece.path)
                    if fh is None:
                        fh = open(piece.path, "rb")
                        handles[piece.path] = fh
                    fh.seek(piece.src_offset)
                    remaining = piece.length
                    while remaining > 0:
                        block = fh.read(min(_COPY_BUFFER, remaining))
                        if not block:
                            break  # truncated source; the final size check catches it
                        hasher.update(block)
                        out.write(block)
                        size += len(block)
                        remaining -= len(block)
                out.flush()
                os.fsync(out.fileno())
        except BaseException:
            tmp.unlink(missing_ok=True)
            raise
        finally:
            for fh in handles.values():
                fh.close()
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
        for entry in self.artifacts_dir.glob("*.tmp"):
            entry.unlink(missing_ok=True)


def _fsync_dir(path: Path) -> None:
    flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
    fd = os.open(path, flags)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)
