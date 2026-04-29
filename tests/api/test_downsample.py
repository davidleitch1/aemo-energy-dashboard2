"""LTTB invariant tests — pure-function downsampling."""
from __future__ import annotations

import math

import pytest

from aemo_dashboard.api.downsample import lttb


def _ts(n: int) -> list[float]:
    return [float(i) for i in range(n)]


def test_passthrough_when_target_geq_n():
    t = _ts(10)
    v = [float(i) for i in range(10)]
    rt, rv = lttb(t, v, 100)
    assert rt == t and rv == v


def test_endpoints_preserved():
    t = _ts(1000)
    v = [math.sin(i / 50.0) for i in range(1000)]
    rt, rv = lttb(t, v, 50)
    assert rt[0] == t[0] and rt[-1] == t[-1]
    assert rv[0] == v[0] and rv[-1] == v[-1]


def test_returned_count_at_most_target():
    t = _ts(10000)
    v = [float(i % 31) for i in range(10000)]
    for target in (50, 100, 500, 1500):
        rt, _ = lttb(t, v, target)
        assert len(rt) <= target


def test_timestamps_monotonic_non_decreasing():
    t = _ts(5000)
    v = [math.sin(i / 30.0) * (i % 7) for i in range(5000)]
    rt, _ = lttb(t, v, 200)
    for prev, cur in zip(rt, rt[1:]):
        assert cur >= prev, "LTTB must preserve x-axis ordering"


def test_preserves_global_max_within_neighbourhood():
    """LTTB doesn't promise the absolute max survives, but a sharp single-point
    spike should be selected over a flat line: triangle area is maximised at
    the spike index. Looser guarantee: the max value of the output should be
    within a small distance of the true max.
    """
    t = _ts(2000)
    v = [0.0] * 2000
    v[800] = 1000.0  # tall spike
    _, rv = lttb(t, v, 50)
    assert max(rv) == 1000.0, "spike must be retained by LTTB"


def test_target_two_returns_endpoints_only():
    t = _ts(100)
    v = [float(i) * 2 for i in range(100)]
    rt, rv = lttb(t, v, 2)
    assert rt == [t[0], t[-1]]
    assert rv == [v[0], v[-1]]


def test_length_mismatch_raises():
    with pytest.raises(ValueError):
        lttb([1.0, 2.0, 3.0], [1.0, 2.0], 2)


def test_loess_passthrough_when_n_lt_3():
    from aemo_dashboard.api.downsample import loess
    out = loess([1.0, 2.0], [1.0, 2.0], 0.5)
    assert out == [1.0, 2.0]


def test_loess_smooths_alternating_noise():
    """Sin wave plus alternating +/- spikes should come back closer to sin."""
    import math
    from aemo_dashboard.api.downsample import loess
    xs = [float(i) for i in range(200)]
    ys = [math.sin(i / 10.0) + 0.5 * ((-1) ** i) for i in range(200)]
    out = loess(xs, ys, frac=0.1)
    # Variance of smoothed - sin should be less than variance of raw - sin.
    raw_resid = sum((y - math.sin(i / 10.0)) ** 2 for i, y in enumerate(ys))
    sm_resid  = sum((s - math.sin(i / 10.0)) ** 2 for i, s in enumerate(out))
    assert sm_resid < raw_resid * 0.3, 'LOESS should reduce noise variance'


def test_loess_preserves_length():
    from aemo_dashboard.api.downsample import loess
    out = loess([float(i) for i in range(50)], [float(i) for i in range(50)], 0.1)
    assert len(out) == 50


def test_loess_monotonic_input_produces_monotonic_output():
    from aemo_dashboard.api.downsample import loess
    out = loess([float(i) for i in range(50)], [float(i) for i in range(50)], 0.2)
    for prev, cur in zip(out, out[1:]):
        assert cur >= prev - 1e-6
