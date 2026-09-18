"""Interval math over absolute file offsets (closed intervals, inclusive ends).

The authoritative byte coverage of a session is the union of confirmed chunk
intervals and committed byte-segment intervals.  These helpers turn that union
into read plans (for conflict checks and finalize assembly) and gap lists (for
``missing_ranges`` and novelty detection) without ever touching file contents.
"""

from __future__ import annotations

import heapq
from bisect import bisect_right
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class Source:
    """A stored byte extent: absolute [start, end] maps to file ``path`` at ``src_start``."""

    start: int
    end: int
    path: Path | None
    src_start: int = 0


@dataclass(frozen=True)
class Piece:
    """One read plan entry: absolute [start, end] read from ``path`` at ``src_offset``."""

    start: int
    end: int
    path: Path | None
    src_offset: int

    @property
    def length(self) -> int:
        return self.end - self.start + 1


def plan_pieces(sources: list[Source], lo: int, hi: int) -> list[Piece]:
    """Cover the [lo, hi] intersection of ``sources`` with sorted, disjoint pieces.

    At every position the source reaching furthest is chosen, so each source
    contributes at most one piece and the result is deterministic for a given
    set of sources.  Gaps in coverage are simply absent from the result; the
    caller derives them with :func:`complement` if needed.
    """
    relevant = sorted(
        (s for s in sources if s.end >= lo and s.start <= hi),
        key=lambda s: (s.start, -s.end),
    )
    pieces: list[Piece] = []
    heap: list[tuple[int, int]] = []  # (-end, index) → furthest-reaching source on top
    idx = 0
    pos = lo
    n = len(relevant)
    while pos <= hi:
        while idx < n and relevant[idx].start <= pos:
            heapq.heappush(heap, (-relevant[idx].end, idx))
            idx += 1
        while heap and -heap[0][0] < pos:
            heapq.heappop(heap)  # source ends before pos: useless from here on
        if not heap:
            if idx < n:
                pos = relevant[idx].start  # skip a gap to the next source
                continue
            break
        best = relevant[heap[0][1]]
        piece_end = min(best.end, hi)
        pieces.append(Piece(pos, piece_end, best.path, best.src_start + (pos - best.start)))
        pos = piece_end + 1
    return pieces


def complement(pieces: list[Piece], lo: int, hi: int) -> list[tuple[int, int]]:
    """Sorted, disjoint, non-adjacent gaps of ``pieces`` within [lo, hi]."""
    gaps: list[tuple[int, int]] = []
    pos = lo
    for piece in pieces:
        if piece.start > pos:
            gaps.append((pos, piece.start - 1))
        pos = max(pos, piece.end + 1)
        if pos > hi:
            break
    if pos <= hi:
        gaps.append((pos, hi))
    return gaps


def merge_intervals(intervals: list[tuple[int, int]]) -> list[tuple[int, int]]:
    """Sort and merge overlapping *and adjacent* closed intervals."""
    merged: list[list[int]] = []
    for start, end in sorted(intervals):
        if merged and start <= merged[-1][1] + 1:
            if end > merged[-1][1]:
                merged[-1][1] = end
        else:
            merged.append([start, end])
    return [(s, e) for s, e in merged]


def interval_covers(merged: list[tuple[int, int]], starts: list[int], lo: int, hi: int) -> bool:
    """True iff some interval in ``merged`` (sorted/disjoint) contains [lo, hi].

    ``starts`` is the precomputed list of interval starts (see :func:`merge_intervals`).
    """
    j = bisect_right(starts, lo) - 1
    return j >= 0 and merged[j][1] >= hi
