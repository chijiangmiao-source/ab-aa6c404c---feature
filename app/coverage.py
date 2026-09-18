"""Pure interval/coverage math shared by the upload protocols.

All intervals are half-open ``[start, end)`` with absolute file offsets.
Authoritative byte coverage is the union of two interval sources:

- fixed logical chunks that have a confirmed row;
- arbitrary byte segments accepted via ``PUT .../ranges``.
"""

from __future__ import annotations


def merge_intervals(intervals: list[tuple[int, int]]) -> list[tuple[int, int]]:
    """Merge overlapping/adjacent intervals; result is sorted and disjoint."""
    ordered = sorted((a, b) for a, b in intervals if b > a)
    merged: list[tuple[int, int]] = []
    for a, b in ordered:
        if merged and a <= merged[-1][1]:
            if b > merged[-1][1]:
                merged[-1] = (merged[-1][0], b)
        else:
            merged.append((a, b))
    return merged


def complement(intervals: list[tuple[int, int]], size: int) -> list[tuple[int, int]]:
    """Sorted merged gaps of ``intervals`` inside ``[0, size)``."""
    gaps: list[tuple[int, int]] = []
    cursor = 0
    for a, b in merge_intervals(intervals):
        if a > cursor:
            gaps.append((cursor, a))
        if b > cursor:
            cursor = b
    if cursor < size:
        gaps.append((cursor, size))
    return gaps


def uncovered_parts(
    start: int, end: int, covered: list[tuple[int, int]]
) -> list[tuple[int, int]]:
    """Sub-intervals of ``[start, end)`` not present in (merged) ``covered``."""
    parts: list[tuple[int, int]] = []
    cursor = start
    for a, b in covered:
        if b <= cursor:
            continue
        if a >= end:
            break
        if a > cursor:
            parts.append((cursor, min(a, end)))
        cursor = max(cursor, b)
        if cursor >= end:
            break
    if cursor < end:
        parts.append((cursor, end))
    return parts


def covered_chunk_indices(
    intervals: list[tuple[int, int]], chunk_size: int, file_size: int
) -> set[int]:
    """Indices of logical chunks *fully* contained in the union of ``intervals``.

    A partially covered chunk is not returned: per the mixed-protocol contract
    partial coverage does not count as a received chunk.
    """
    total = -(-file_size // chunk_size)
    last = total - 1
    last_starts_short = total and file_size - last * chunk_size < chunk_size
    indices: set[int] = set()
    for a, b in merge_intervals(intervals):
        # regular full-size chunk i needs [i*cs, (i+1)*cs) within [a, b)
        lo = (a + chunk_size - 1) // chunk_size
        hi = b // chunk_size - 1
        for i in range(lo, min(last, hi) + 1):
            indices.add(i)
        # the final chunk may be shorter than chunk_size
        if last_starts_short and lo <= last and a <= last * chunk_size and b >= file_size:
            indices.add(last)
    return indices
