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


import numpy as np


def loess(x: Sequence[float], y: Sequence[float], frac: float = 0.05) -> list[float]:
    """Locally-weighted regression smoother (degree-1, tricube weights).

    Returns a list of smoothed y-values at the same x positions. `x` must be
    monotonic non-decreasing (we exploit this for an O(n*r) sliding window
    rather than O(n^2)).

    `frac` is the fraction of points used in each local fit; 0.05 ≈ "1/20th
    of the data" which feels like a few cycles' average for AEMO spot prices.
    """
    n = len(x)
    if n != len(y):
        raise ValueError("x and y length mismatch")
    if n < 3:
        return list(map(float, y))
    r = max(3, int(round(frac * n)))
    if r >= n:
        r = n

    xa = np.asarray(x, dtype=float)
    ya = np.asarray(y, dtype=float)
    out = np.empty(n, dtype=float)

    for i in range(n):
        # Symmetric window of size r centred on i, clamped to [0, n).
        half = r // 2
        lo = max(0, i - half)
        hi = min(n, lo + r)
        lo = max(0, hi - r)
        xs = xa[lo:hi]
        ys = ya[lo:hi]
        d = np.abs(xs - xa[i])
        h = max(d.max(), 1e-12)
        w = (1.0 - (d / h) ** 3) ** 3
        sw = w.sum()
        if sw < 1e-12:
            out[i] = ya[i]
            continue
        wx = (w * xs).sum() / sw
        wy = (w * ys).sum() / sw
        b_num = (w * (xs - wx) * (ys - wy)).sum()
        b_den = (w * (xs - wx) ** 2).sum()
        if abs(b_den) < 1e-12:
            out[i] = wy
        else:
            b = b_num / b_den
            out[i] = wy - b * wx + b * xa[i]

    return out.tolist()
