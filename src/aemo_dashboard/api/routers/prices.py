"""GET /v1/prices/spot — spot price time series, single or multi region.

Multi-region usage:
  GET /v1/prices/spot?regions=NSW1,QLD1,VIC1&from=...&to=...

Server-side LTTB downsampling caps each region at MAX_POINTS so 1Y/All
windows return a manageable payload.
"""
from __future__ import annotations

import base64
import binascii
from datetime import datetime, timedelta, timezone
from typing import Optional

from fastapi import APIRouter, HTTPException, Query

from ..db import get_connection, nem_naive_to_utc, utc_to_nem_naive
from ..downsample import loess, lttb

router = APIRouter()

VALID_REGIONS = {"NSW1", "QLD1", "SA1", "TAS1", "VIC1"}
MAX_POINTS = 1500  # per region
THIRTY_DAYS_S = 30 * 86400


def _pick_price_table(span_seconds: float) -> tuple[str, str]:
    """Choose prices5 vs prices30 based on the requested window.

    Convention: > 30 days uses 30-min data. The chart is downsampled to
    the same 1500 points per series either way, so visual quality is
    identical and the SQL is ~6x cheaper on prices30.
    """
    if span_seconds > THIRTY_DAYS_S:
        return ("prices_30min", "30min")
    return ("prices5", "5min")


def _pick_gen_table(span_seconds: float) -> tuple[str, str]:
    if span_seconds > THIRTY_DAYS_S:
        return ("generation_by_fuel_30min", "30min")
    return ("generation_by_fuel_5min", "5min")


def _utc_iso(dt: datetime) -> str:
    return nem_naive_to_utc(dt).isoformat().replace("+00:00", "Z")


def _resolve_regions(region: Optional[str], regions: Optional[str]) -> list[str]:
    if regions:
        out = [r.strip().upper() for r in regions.split(",") if r.strip()]
    elif region:
        out = [region.strip().upper()]
    else:
        raise HTTPException(
            status_code=400,
            detail={"code": "MISSING_REGION", "message": "region or regions required"},
        )
    bad = [r for r in out if r not in VALID_REGIONS]
    if bad:
        raise HTTPException(
            status_code=400,
            detail={"code": "INVALID_REGION", "message": f"Unknown region(s): {bad}"},
        )
    return out


@router.get("/prices/spot")
def spot_price(
    region: Optional[str] = Query(None, min_length=2, max_length=8),
    regions: Optional[str] = Query(None),
    from_: Optional[datetime] = Query(None, alias="from"),
    to: Optional[datetime] = Query(None),
    resolution: str = Query("auto"),
    smoothing: Optional[str] = Query(None),
) -> dict:
    region_list = _resolve_regions(region, regions)
    smoothing = smoothing.lower().strip() if smoothing else None
    if smoothing not in (None, "loess"):
        raise HTTPException(400, detail={"code": "INVALID_SMOOTHING", "message": f"unknown smoothing: {smoothing}"})

    now_utc = datetime.now(timezone.utc)

    to_utc = to.astimezone(timezone.utc) if to and to.tzinfo else (
        to.replace(tzinfo=timezone.utc) if to else now_utc
    )

    if from_ is None:
        # "All data" — anchor from the earliest available timestamp. Clients
        # commonly send only `to=now` for the All chip, so don't require both
        # to be omitted.
        conn0 = get_connection()
        try:
            min_dt = conn0.execute(
                "SELECT MIN(settlementdate) FROM prices_30min"
            ).fetchone()[0]
        finally:
            conn0.close()
        from_utc = nem_naive_to_utc(min_dt) if min_dt else (to_utc - timedelta(hours=24))
    else:
        from_utc = from_.astimezone(timezone.utc) if from_.tzinfo else from_.replace(tzinfo=timezone.utc)

    span_seconds = (to_utc - from_utc).total_seconds()
    src_table, res_label = _pick_price_table(span_seconds)

    from_nem = utc_to_nem_naive(from_utc)
    to_nem = utc_to_nem_naive(to_utc)

    placeholders = ",".join(["?"] * len(region_list))
    if src_table == "prices_30min" or src_table == "prices30":
        # Both "30-min" tables hold a mix of 5-min and 30-min rows for 2022+;
        # force uniform 30-min cadence so LTTB sees evenly-spaced input.
        sql = f"""
            SELECT time_bucket(INTERVAL '30 minutes', settlementdate) AS settlementdate,
                   regionid,
                   AVG(rrp) AS rrp
            FROM {src_table}
            WHERE regionid IN ({placeholders})
              AND settlementdate >= ?
              AND settlementdate <= ?
            GROUP BY 1, regionid
            ORDER BY regionid, 1
        """
    else:
        sql = f"""
            SELECT settlementdate, regionid, rrp
            FROM {src_table}
            WHERE regionid IN ({placeholders})
              AND settlementdate >= ?
              AND settlementdate <= ?
            ORDER BY regionid, settlementdate
        """
    params = list(region_list) + [from_nem, to_nem]

    conn = get_connection()
    try:
        rows = conn.execute(sql, params).fetchall()
    finally:
        conn.close()

    # Group by region
    by_region: dict[str, list[tuple[datetime, float]]] = {r: [] for r in region_list}
    for ts, regid, rrp in rows:
        by_region.setdefault(regid, []).append((ts, float(rrp)))

    source_rows = sum(len(v) for v in by_region.values())

    data: list[dict] = []
    downsampled = False
    smoothed_meta = False
    # Pick a LOESS bandwidth: 0.05 of the visible series for everything
    # except All (>= ~1 yr lookback), where 0.08 widens to a few months.
    span_seconds = (to_utc - from_utc).total_seconds()
    loess_frac = 0.08 if span_seconds > 366 * 86400 else 0.05

    for regid in region_list:
        series = by_region.get(regid, [])
        if not series:
            continue

        if smoothing == "loess":
            # Smoothing path: use mean-binning (not LTTB) so price spikes are
            # averaged into their bucket rather than preserved as max-triangle
            # outliers. Then LOESS for trend on top of the bucket means.
            if len(series) > MAX_POINTS:
                bin_size = max(1, len(series) // MAX_POINTS)
                binned: list[tuple[datetime, float]] = []
                for start in range(0, len(series), bin_size):
                    chunk = series[start:start + bin_size]
                    if not chunk:
                        continue
                    mid_ts = chunk[len(chunk) // 2][0]
                    mean_v = sum(s[1] for s in chunk) / len(chunk)
                    binned.append((mid_ts, mean_v))
                ts_v_pairs = binned
                downsampled = True
            else:
                ts_v_pairs = list(series)

            if len(ts_v_pairs) >= 5:
                xs = [float(i) for i in range(len(ts_v_pairs))]
                ys = [v for _, v in ts_v_pairs]
                ys_smoothed = loess(xs, ys, frac=loess_frac)
                ts_v_pairs = [(t, sv) for (t, _), sv in zip(ts_v_pairs, ys_smoothed)]
                smoothed_meta = True
        else:
            # Raw path: LTTB preserves spikes for the unsmoothed view.
            if len(series) > MAX_POINTS:
                ts_idx = [float(i) for i in range(len(series))]
                vals = [s[1] for s in series]
                idx_out, v_out = lttb(ts_idx, vals, MAX_POINTS)
                downsampled = True
                ts_v_pairs = [(series[int(ix)][0], v) for ix, v in zip(idx_out, v_out)]
            else:
                ts_v_pairs = list(series)

        for ts, v in ts_v_pairs:
            data.append({
                "timestamp": _utc_iso(ts),
                "region": regid,
                "price": v,
            })

    return {
        "data": data,
        "meta": {
            "from": from_utc.isoformat().replace("+00:00", "Z"),
            "to": to_utc.isoformat().replace("+00:00", "Z"),
            "resolution": res_label,
            "regions": region_list,
            "downsampled": downsampled,
            "smoothed": smoothed_meta,
            "source_rows": source_rows,
            "returned_rows": len(data),
            "as_of": datetime.now(timezone.utc).isoformat(),
        },
    }


@router.get('/prices/time-of-day')
def time_of_day(
    region: Optional[str] = Query(None, min_length=2, max_length=8),
    regions: Optional[str] = Query(None),
    days: int = Query(7, ge=1, le=365),
    from_: Optional[datetime] = Query(None, alias='from'),
    to: Optional[datetime] = Query(None),
) -> dict:
    region_list = _resolve_regions(region, regions)

    now_utc = datetime.now(timezone.utc)
    to_utc = to.astimezone(timezone.utc) if to and to.tzinfo else (
        to.replace(tzinfo=timezone.utc) if to else now_utc
    )
    from_utc = from_.astimezone(timezone.utc) if from_ and from_.tzinfo else (
        from_.replace(tzinfo=timezone.utc) if from_ else (to_utc - timedelta(days=days))
    )
    from_nem = utc_to_nem_naive(from_utc)
    to_nem = utc_to_nem_naive(to_utc)
    span_seconds = (to_utc - from_utc).total_seconds()
    tod_src, _ = _pick_price_table(span_seconds)

    placeholders = ','.join(['?'] * len(region_list))
    sql = f'''
        SELECT regionid,
               EXTRACT(HOUR FROM settlementdate)::INT AS hour,
               AVG(rrp) AS avg_price
        FROM {tod_src}
        WHERE regionid IN ({placeholders})
          AND settlementdate >= ?
          AND settlementdate <= ?
        GROUP BY regionid, hour
        ORDER BY regionid, hour
    '''
    params = list(region_list) + [from_nem, to_nem]

    conn = get_connection()
    try:
        rows = conn.execute(sql, params).fetchall()
    finally:
        conn.close()

    data = [
        {'region': r[0], 'hour': int(r[1]), 'avg_price': float(r[2])}
        for r in rows
    ]

    return {
        'data': data,
        'meta': {
            'regions': region_list,
            'lookback_days': days,
            'from': from_utc.isoformat().replace('+00:00', 'Z'),
            'to': to_utc.isoformat().replace('+00:00', 'Z'),
            'as_of': datetime.now(timezone.utc).isoformat(),
        },
    }

@router.get("/prices/by-fuel")
def by_fuel(
    region: Optional[str] = Query(None, min_length=2, max_length=8),
    regions: Optional[str] = Query(None),
    days: int = Query(7, ge=1, le=365),
    from_: Optional[datetime] = Query(None, alias="from"),
    to: Optional[datetime] = Query(None),
) -> dict:
    region_list = _resolve_regions(region, regions)

    now_utc = datetime.now(timezone.utc)
    to_utc = to.astimezone(timezone.utc) if to and to.tzinfo else (
        to.replace(tzinfo=timezone.utc) if to else now_utc
    )
    from_utc = from_.astimezone(timezone.utc) if from_ and from_.tzinfo else (
        from_.replace(tzinfo=timezone.utc) if from_ else (to_utc - timedelta(days=days))
    )
    from_nem = utc_to_nem_naive(from_utc)
    to_nem = utc_to_nem_naive(to_utc)
    span_seconds = (to_utc - from_utc).total_seconds()
    bf_price, _ = _pick_price_table(span_seconds)
    bf_gen, _ = _pick_gen_table(span_seconds)

    placeholders = ",".join(["?"] * len(region_list))

    # Utility-scale fuels: drop Biomass; merge CCGT + OCGT + Gas other -> "Gas".
    # Pre-aggregate per (settlementdate, fuel) so multi-region picks up the
    # NEM-aggregate semantics (sum across regions, then average over time).
    util_sql = f"""
        WITH joined AS (
            SELECT g.settlementdate, g.region,
                   CASE WHEN g.fuel_type IN ('CCGT','OCGT','Gas other') THEN 'Gas'
                        ELSE g.fuel_type END AS fuel,
                   GREATEST(g.total_generation_mw, 0) AS gen_mw,
                   p.rrp
            FROM {bf_gen} g
            JOIN {bf_price} p
              ON g.settlementdate = p.settlementdate
             AND g.region = p.regionid
            WHERE g.region IN ({placeholders})
              AND g.settlementdate >= ? AND g.settlementdate <= ?
              AND g.fuel_type != 'Biomass'
              AND GREATEST(g.total_generation_mw, 0) > 0
        ),
        per_period_fuel AS (
            SELECT settlementdate, fuel,
                   SUM(gen_mw) AS total_mw,
                   SUM(gen_mw * rrp) / NULLIF(SUM(gen_mw), 0) AS period_vwap
            FROM joined
            GROUP BY 1, 2
        )
        SELECT fuel,
               SUM(total_mw) / 12.0 AS volume_mwh,
               SUM(total_mw * period_vwap) / NULLIF(SUM(total_mw), 0) AS vwap,
               AVG(total_mw) AS avg_mw
        FROM per_period_fuel
        GROUP BY fuel
        ORDER BY volume_mwh DESC
    """
    util_params = list(region_list) + [from_nem, to_nem]

    # Rooftop: rooftop30 joined to prices5 at the exact 30-min boundary
    # timestamp. End-of-window price; close enough for VWAP averaging.
    roof_sql = f"""
        WITH joined AS (
            SELECT rs.settlementdate, rs.regionid AS region,
                   rs.power AS gen_mw, p.rrp
            FROM rooftop30 rs
            JOIN prices5 p
              ON rs.settlementdate = p.settlementdate
             AND rs.regionid = p.regionid
            WHERE rs.regionid IN ({placeholders})
              AND rs.settlementdate >= ? AND rs.settlementdate <= ?
              AND rs.power > 0
        ),
        per_period AS (
            SELECT settlementdate,
                   SUM(gen_mw) AS total_mw,
                   SUM(gen_mw * rrp) / NULLIF(SUM(gen_mw), 0) AS period_vwap
            FROM joined
            GROUP BY 1
        )
        SELECT SUM(total_mw) / 2.0 AS volume_mwh,
               SUM(total_mw * period_vwap) / NULLIF(SUM(total_mw), 0) AS vwap,
               AVG(total_mw) AS avg_mw
        FROM per_period
    """
    roof_params = list(region_list) + [from_nem, to_nem]

    conn = get_connection()
    try:
        util_rows = conn.execute(util_sql, util_params).fetchall()
        roof_row = conn.execute(roof_sql, roof_params).fetchone()
    finally:
        conn.close()

    raw = [
        {
            "fuel": fuel,
            "volume_mwh": float(vol or 0.0),
            "vwap": float(vwap or 0.0),
            "avg_mw": float(avg or 0.0),
        }
        for fuel, vol, vwap, avg in util_rows
    ]

    if roof_row and roof_row[0] is not None and float(roof_row[0]) > 0:
        raw.append({
            "fuel": "Rooftop",
            "volume_mwh": float(roof_row[0]),
            "vwap": float(roof_row[1] or 0.0),
            "avg_mw": float(roof_row[2] or 0.0),
        })

    raw.sort(key=lambda r: -r["volume_mwh"])
    total_volume = sum(r["volume_mwh"] for r in raw)

    data = []
    for r in raw:
        share = (r["volume_mwh"] / total_volume * 100.0) if total_volume > 0 else 0.0
        data.append({
            "fuel": r["fuel"],
            "volume_mwh": round(r["volume_mwh"], 1),
            "share_pct": round(share, 2),
            "vwap": round(r["vwap"], 2),
            "avg_mw": round(r["avg_mw"], 1),
        })

    return {
        "data": data,
        "meta": {
            "regions": region_list,
            "lookback_days": days,
            "total_volume_mwh": round(total_volume, 1),
            "from": from_utc.isoformat().replace("+00:00", "Z"),
            "to": to_utc.isoformat().replace("+00:00", "Z"),
            "as_of": datetime.now(timezone.utc).isoformat(),
        },
    }


@router.get("/prices/stats")
def prices_stats(
    region: Optional[str] = Query(None, min_length=2, max_length=8),
    regions: Optional[str] = Query(None),
    days: int = Query(7, ge=1, le=365),
    from_: Optional[datetime] = Query(None, alias="from"),
    to: Optional[datetime] = Query(None),
) -> dict:
    region_list = _resolve_regions(region, regions)

    now_utc = datetime.now(timezone.utc)
    to_utc = to.astimezone(timezone.utc) if to and to.tzinfo else (
        to.replace(tzinfo=timezone.utc) if to else now_utc
    )
    from_utc = from_.astimezone(timezone.utc) if from_ and from_.tzinfo else (
        from_.replace(tzinfo=timezone.utc) if from_ else (to_utc - timedelta(days=days))
    )
    from_nem = utc_to_nem_naive(from_utc)
    to_nem = utc_to_nem_naive(to_utc)

    span_seconds = (to_utc - from_utc).total_seconds()
    src_table, _ = _pick_price_table(span_seconds)

    placeholders = ",".join(["?"] * len(region_list))
    sql = f"""
        SELECT regionid,
               AVG(rrp) AS mean,
               MEDIAN(rrp) AS median,
               QUANTILE_CONT(rrp, 0.10) AS p10,
               QUANTILE_CONT(rrp, 0.90) AS p90,
               MIN(rrp) AS min_p,
               MAX(rrp) AS max_p,
               COUNT(*) AS n
        FROM {src_table}
        WHERE regionid IN ({placeholders})
          AND settlementdate >= ? AND settlementdate <= ?
        GROUP BY regionid
        ORDER BY regionid
    """
    params = list(region_list) + [from_nem, to_nem]

    conn = get_connection()
    try:
        rows = conn.execute(sql, params).fetchall()
    finally:
        conn.close()

    data = []
    # Preserve the request order so the iOS chart-card list lines up with chip order.
    by_region = {r[0]: r for r in rows}
    for code in region_list:
        r = by_region.get(code)
        if r is None:
            continue
        data.append({
            "region": r[0],
            "mean":   round(float(r[1]), 2),
            "median": round(float(r[2]), 2),
            "p10":    round(float(r[3]), 2),
            "p90":    round(float(r[4]), 2),
            "min":    round(float(r[5]), 2),
            "max":    round(float(r[6]), 2),
            "sample_count": int(r[7]),
        })

    return {
        "data": data,
        "meta": {
            "regions": region_list,
            "lookback_days": days,
            "from": from_utc.isoformat().replace("+00:00", "Z"),
            "to": to_utc.isoformat().replace("+00:00", "Z"),
            "as_of": datetime.now(timezone.utc).isoformat(),
        },
    }


# --- Bands subpage --------------------------------------------------------

DEFAULT_BAND_BINS = [-1000.0, 0.0, 25.0, 50.0, 100.0, 200.0, 300.0, 500.0, 1000.0, 5000.0, 17500.0]


def _resolve_bins(bins_q: Optional[str]) -> list[float]:
    if not bins_q:
        return list(DEFAULT_BAND_BINS)
    try:
        out = [float(x) for x in bins_q.split(',') if x.strip()]
    except ValueError:
        raise HTTPException(400, detail={"code": "INVALID_BINS", "message": "bins must be comma-separated numbers"})
    if len(out) < 2:
        raise HTTPException(400, detail={"code": "INVALID_BINS", "message": "bins needs at least 2 boundary values"})
    for a, b in zip(out, out[1:]):
        if a >= b:
            raise HTTPException(400, detail={"code": "INVALID_BINS", "message": "bins must be strictly ascending"})
    return out


@router.get("/prices/bands")
def prices_bands(
    region: Optional[str] = Query(None, min_length=2, max_length=8),
    regions: Optional[str] = Query(None),
    from_: Optional[datetime] = Query(None, alias="from"),
    to: Optional[datetime] = Query(None),
    bins: Optional[str] = Query(None),
) -> dict:
    """Distribution of 5-min (or 30-min for >30d) prices per region into bands.

    Returns one row per (region × band) with `count` of intervals and
    `share` of the window. Default bins:
        [-1000, 0, 25, 50, 100, 200, 300, 500, 1000, 5000, 17500]
    Override via `bins=` (comma-separated, strictly ascending).
    """
    region_list = _resolve_regions(region, regions)
    bin_edges = _resolve_bins(bins)

    now_utc = datetime.now(timezone.utc)
    to_utc = to.astimezone(timezone.utc) if to and to.tzinfo else (
        to.replace(tzinfo=timezone.utc) if to else now_utc
    )
    from_utc = from_.astimezone(timezone.utc) if from_ and from_.tzinfo else (
        from_.replace(tzinfo=timezone.utc) if from_ else (to_utc - timedelta(days=30))
    )
    from_nem = utc_to_nem_naive(from_utc)
    to_nem = utc_to_nem_naive(to_utc)
    span_seconds = (to_utc - from_utc).total_seconds()
    src_table, _ = _pick_price_table(span_seconds)

    # Build a values list for the bins CTE: (idx, lower, upper) per band.
    bands_values_sql = ", ".join(
        f"({i + 1}, {lo}, {hi})"
        for i, (lo, hi) in enumerate(zip(bin_edges, bin_edges[1:]))
    )
    placeholders = ",".join(["?"] * len(region_list))
    sql = f"""
        WITH bands(idx, lower, upper) AS (
            VALUES {bands_values_sql}
        ),
        prices AS (
            SELECT regionid, rrp
            FROM {src_table}
            WHERE regionid IN ({placeholders})
              AND settlementdate >= ? AND settlementdate <= ?
              AND rrp IS NOT NULL
        ),
        counted AS (
            SELECT p.regionid, b.idx, b.lower, b.upper,
                   COUNT(*) AS cnt,
                   SUM(p.rrp) AS sum_rrp
            FROM prices p
            JOIN bands b ON p.rrp >= b.lower AND p.rrp < b.upper
            GROUP BY p.regionid, b.idx, b.lower, b.upper
        ),
        totals AS (
            SELECT regionid,
                   COUNT(*) AS total,
                   SUM(rrp) AS total_sum_rrp
            FROM prices GROUP BY regionid
        )
        SELECT b.idx,
               b.lower,
               b.upper,
               r.regionid AS regionid,
               COALESCE(c.cnt, 0) AS cnt,
               COALESCE(c.cnt::DOUBLE / NULLIF(t.total, 0), 0.0) AS share,
               COALESCE(c.sum_rrp, 0.0) AS sum_rrp,
               COALESCE(c.sum_rrp / NULLIF(t.total_sum_rrp, 0), 0.0) AS contribution_share
        FROM bands b
        CROSS JOIN (SELECT DISTINCT regionid FROM prices) r
        LEFT JOIN counted c ON c.regionid = r.regionid AND c.idx = b.idx
        LEFT JOIN totals t ON t.regionid = r.regionid
        ORDER BY r.regionid, b.idx
    """
    params = list(region_list) + [from_nem, to_nem]

    conn = get_connection()
    try:
        rows = conn.execute(sql, params).fetchall()
    finally:
        conn.close()

    data = [
        {
            "region": regid,
            "lower": float(lo),
            "upper": float(hi),
            "count": int(cnt),
            "share": round(float(share), 6),
            "sum_rrp": round(float(sum_rrp), 4),
            "contribution_share": round(float(contrib), 6),
        }
        for _idx, lo, hi, regid, cnt, share, sum_rrp, contrib in rows
    ]

    return {
        "data": data,
        "meta": {
            "regions": region_list,
            "bins": bin_edges,
            "from": from_utc.isoformat().replace("+00:00", "Z"),
            "to": to_utc.isoformat().replace("+00:00", "Z"),
            "resolution": "5min" if span_seconds <= THIRTY_DAYS_S else "30min",
            "as_of": datetime.now(timezone.utc).isoformat(),
        },
    }


# --- High-price events ----------------------------------------------------

DEFAULT_HIGH_THRESHOLD = 300.0


def _encode_cursor(ts_iso: str, region: str) -> str:
    raw = f"{ts_iso}|{region}".encode("utf-8")
    return base64.urlsafe_b64encode(raw).decode("ascii")


def _decode_cursor(cursor: str) -> tuple[str, str]:
    try:
        raw = base64.urlsafe_b64decode(cursor.encode("ascii")).decode("utf-8")
    except (binascii.Error, UnicodeDecodeError, ValueError):
        raise HTTPException(400, detail={"code": "INVALID_CURSOR", "message": "cursor is not valid base64"})
    if "|" not in raw:
        raise HTTPException(400, detail={"code": "INVALID_CURSOR", "message": "cursor payload malformed"})
    ts_iso, region = raw.split("|", 1)
    return ts_iso, region


@router.get("/prices/high-events")
def prices_high_events(
    region: Optional[str] = Query(None, min_length=2, max_length=8),
    regions: Optional[str] = Query(None),
    from_: Optional[datetime] = Query(None, alias="from"),
    to: Optional[datetime] = Query(None),
    threshold: float = Query(DEFAULT_HIGH_THRESHOLD),
    limit: int = Query(50, ge=1, le=500),
    cursor: Optional[str] = Query(None),
) -> dict:
    """5-min intervals at or above `threshold`, ordered most-recent first.

    duration_minutes is the length (in minutes) of the consecutive run of
    at-or-above-threshold intervals to which this row belongs (window
    function). Cursor is base64 of `<timestamp>|<region>` of the last row
    in the previous page; pagination uses lexicographic comparison so
    pages are guaranteed disjoint.
    """
    region_list = _resolve_regions(region, regions)

    now_utc = datetime.now(timezone.utc)
    to_utc = to.astimezone(timezone.utc) if to and to.tzinfo else (
        to.replace(tzinfo=timezone.utc) if to else now_utc
    )
    # Default lookback for high-events is 1Y — spike-watching wants history.
    from_utc = from_.astimezone(timezone.utc) if from_ and from_.tzinfo else (
        from_.replace(tzinfo=timezone.utc) if from_ else (to_utc - timedelta(days=365))
    )
    from_nem = utc_to_nem_naive(from_utc)
    to_nem = utc_to_nem_naive(to_utc)

    # High-events always reads 5-min (prices5) so durations are reported
    # in 5-min steps. For pre-2024 windows where 5-min data is sparse,
    # this still works — empty regions just contribute no rows.
    src_table = "prices5"

    cursor_ts: Optional[str] = None
    cursor_region: Optional[str] = None
    if cursor:
        cursor_ts, cursor_region = _decode_cursor(cursor)

    placeholders = ",".join(["?"] * len(region_list))
    # Build ordered → flagged → grouped → runs CTE chain.
    # `is_start` flags the first row of each consecutive at-or-above run.
    # `group_id = SUM(is_start) OVER (...)` assigns the same id to every
    # row in the run. duration = COUNT(*) OVER (PARTITION BY region, group_id).
    sql = f"""
        WITH ordered AS (
            SELECT settlementdate, regionid, rrp,
                   LAG(rrp) OVER (PARTITION BY regionid ORDER BY settlementdate) AS prev_rrp
            FROM {src_table}
            WHERE regionid IN ({placeholders})
              AND settlementdate >= ? AND settlementdate <= ?
        ),
        flagged AS (
            SELECT settlementdate, regionid, rrp,
                   CASE WHEN rrp >= ? AND (prev_rrp IS NULL OR prev_rrp < ?)
                        THEN 1 ELSE 0 END AS is_start
            FROM ordered
        ),
        grouped AS (
            SELECT settlementdate, regionid, rrp,
                   SUM(is_start) OVER (PARTITION BY regionid ORDER BY settlementdate) AS group_id
            FROM flagged
        ),
        runs AS (
            SELECT settlementdate, regionid, rrp, group_id,
                   COUNT(*) OVER (PARTITION BY regionid, group_id) * 5 AS duration_minutes
            FROM grouped
            WHERE rrp >= ?
        )
        SELECT settlementdate, regionid, rrp, duration_minutes
        FROM runs
        ORDER BY settlementdate DESC, regionid ASC
    """
    params = list(region_list) + [from_nem, to_nem, threshold, threshold, threshold]

    conn = get_connection()
    try:
        rows = conn.execute(sql, params).fetchall()
    finally:
        conn.close()

    # Convert all rows to UTC ISO once so cursor comparison is on the
    # canonical string we'll return to the client.
    serialised = [
        {
            "timestamp": _utc_iso(ts),
            "region": regid,
            "price": float(rrp),
            "duration_minutes": int(dur),
        }
        for ts, regid, rrp, dur in rows
    ]

    # Apply cursor: locate the cursor row in the ordered list and slice
    # everything after it. The list is sorted DESC by timestamp, ASC by
    # region; lex comparison on (ts, region) doesn't match this order
    # when rows share a timestamp, so we use index-based slicing.
    if cursor_ts is not None and cursor_region is not None:
        found_at: Optional[int] = None
        for i, r in enumerate(serialised):
            if r["timestamp"] == cursor_ts and r["region"] == cursor_region:
                found_at = i
                break
        page_pool = serialised[found_at + 1:] if found_at is not None else serialised
    else:
        page_pool = serialised

    page = page_pool[:limit]
    has_more = len(page_pool) > limit
    next_cursor = (
        _encode_cursor(page[-1]["timestamp"], page[-1]["region"]) if has_more and page else None
    )

    return {
        "data": page,
        "meta": {
            "regions": region_list,
            "from": from_utc.isoformat().replace("+00:00", "Z"),
            "to": to_utc.isoformat().replace("+00:00", "Z"),
            "threshold": threshold,
            "next_cursor": next_cursor,
            "has_more": has_more,
            "as_of": datetime.now(timezone.utc).isoformat(),
        },
    }
