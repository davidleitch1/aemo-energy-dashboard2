"""Largest-Triangle-Three-Buckets time-series downsampling.

Used by /v1/prices/spot and other time-series endpoints to keep payload size
sane on long lookback windows. LTTB preserves visually-significant points
(peaks, troughs) by maximising the area of the triangle formed with the
previously-selected point and the next bucket's centroid.

Reference: Sveinn Steinarsson, 'Downsampling Time Series for Visual
Representation' (2013). Pure-Python implementation, no numpy dependency
to keep the API process light.
"""
from __future__ import annotations

from typing import Sequence


def lttb(
    timestamps: Sequence[float],
    values: Sequence[float],
    target: int,
) -> tuple[list[float], list[float]]:
    """Return (timestamps, values) downsampled to at most  points.

    Endpoints are always preserved.  must be >= 2 to use the
    triangle algorithm; smaller targets are clamped or returned verbatim.

    The two input sequences must be the same length and  must
    be monotonic non-decreasing. Timestamps are treated as floats (seconds-
    since-epoch is fine).
    """
    n = len(timestamps)
    if n != len(values):
        raise ValueError("timestamps and values length mismatch")

    if target >= n or n <= 2:
        return list(timestamps), list(values)
    if target == 2:
        return [float(timestamps[0]), float(timestamps[-1])], [float(values[0]), float(values[-1])]
    if target < 2:
        # Degenerate ask — preserve at least the first point so callers
        # don't fall over downstream.
        return [timestamps[0]], [values[0]]

    bucket_size = (n - 2) / (target - 2)

    out_t: list[float] = [timestamps[0]]
    out_v: list[float] = [values[0]]

    a_t = float(timestamps[0])
    a_v = float(values[0])

    for i in range(target - 2):
        # Range of points in the *next* bucket (used for centroid).
        next_lo = int((i + 1) * bucket_size) + 1
        next_hi = int((i + 2) * bucket_size) + 1
        next_hi = min(next_hi, n)
        if next_lo >= next_hi:
            next_lo = next_hi - 1
        avg_t = sum(float(timestamps[k]) for k in range(next_lo, next_hi)) / (next_hi - next_lo)
        avg_v = sum(float(values[k]) for k in range(next_lo, next_hi)) / (next_hi - next_lo)

        # Range of points in the *current* bucket — pick one that maximises
        # triangle area with (a_t, a_v) and (avg_t, avg_v).
        cur_lo = int(i * bucket_size) + 1
        cur_hi = int((i + 1) * bucket_size) + 1
        cur_hi = min(cur_hi, n - 1)

        max_area = -1.0
        chosen = cur_lo
        for k in range(cur_lo, cur_hi):
            area = abs(
                (a_t - avg_t) * (float(values[k]) - a_v)
                - (a_t - float(timestamps[k])) * (avg_v - a_v)
            )
            if area > max_area:
                max_area = area
                chosen = k

        out_t.append(float(timestamps[chosen]))
        out_v.append(float(values[chosen]))
        a_t = float(timestamps[chosen])
        a_v = float(values[chosen])

    out_t.append(float(timestamps[-1]))
    out_v.append(float(values[-1]))
    return out_t, out_v
