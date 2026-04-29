"""GET /v1/prices/spot — spot price time series, single or multi region.

Multi-region usage:
  GET /v1/prices/spot?regions=NSW1,QLD1,VIC1&from=...&to=...

Server-side LTTB downsampling caps each region at MAX_POINTS so 1Y/All
windows return a manageable payload.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Optional

from fastapi import APIRouter, HTTPException, Query

from ..db import get_connection, nem_naive_to_utc, utc_to_nem_naive
from ..downsample import lttb

router = APIRouter()

VALID_REGIONS = {"NSW1", "QLD1", "SA1", "TAS1", "VIC1"}
MAX_POINTS = 1500  # per region


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
async def spot_price(
    region: Optional[str] = Query(None, min_length=2, max_length=8),
    regions: Optional[str] = Query(None),
    from_: Optional[datetime] = Query(None, alias="from"),
    to: Optional[datetime] = Query(None),
    resolution: str = Query("auto"),
) -> dict:
    region_list = _resolve_regions(region, regions)

    now_utc = datetime.now(timezone.utc)
    to_utc = to.astimezone(timezone.utc) if to and to.tzinfo else (
        to.replace(tzinfo=timezone.utc) if to else now_utc
    )
    from_utc = from_.astimezone(timezone.utc) if from_ and from_.tzinfo else (
        from_.replace(tzinfo=timezone.utc) if from_ else (to_utc - timedelta(hours=24))
    )

    from_nem = utc_to_nem_naive(from_utc)
    to_nem = utc_to_nem_naive(to_utc)

    placeholders = ",".join(["?"] * len(region_list))
    sql = f"""
        SELECT settlementdate, regionid, rrp
        FROM prices5
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
    for regid in region_list:
        series = by_region.get(regid, [])
        if not series:
            continue
        if len(series) > MAX_POINTS:
            # Use float-index as the LTTB x-axis (timestamps are monotonic
            # so indices are equivalent for triangle-area selection); then
            # map chosen indices back to the original datetimes to avoid
            # any tz round-trip.
            ts_idx = [float(i) for i in range(len(series))]
            vals = [s[1] for s in series]
            idx_out, v_out = lttb(ts_idx, vals, MAX_POINTS)
            downsampled = True
            for ix, v in zip(idx_out, v_out):
                ts = series[int(ix)][0]
                data.append({
                    "timestamp": _utc_iso(ts),
                    "region": regid,
                    "price": v,
                })
        else:
            for ts, v in series:
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
            "resolution": "5min",
            "regions": region_list,
            "downsampled": downsampled,
            "source_rows": source_rows,
            "returned_rows": len(data),
            "as_of": datetime.now(timezone.utc).isoformat(),
        },
    }


@router.get('/prices/time-of-day')
async def time_of_day(
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

    placeholders = ','.join(['?'] * len(region_list))
    sql = f'''
        SELECT regionid,
               EXTRACT(HOUR FROM settlementdate)::INT AS hour,
               AVG(rrp) AS avg_price
        FROM prices5
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


@router.get('/prices/by-fuel')
async def by_fuel(
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

    placeholders = ','.join(['?'] * len(region_list))
    sql = f'''
        WITH gen AS (
            SELECT settlementdate, fuel_type, region,
                   GREATEST(total_generation_mw, 0) AS gen_mw
            FROM generation_by_fuel_5min
            WHERE region IN ({placeholders})
              AND settlementdate >= ? AND settlementdate <= ?
        ),
        prices AS (
            SELECT settlementdate, regionid, rrp
            FROM prices5
            WHERE regionid IN ({placeholders})
              AND settlementdate >= ? AND settlementdate <= ?
        ),
        joined AS (
            SELECT g.fuel_type, g.gen_mw, p.rrp
            FROM gen g
            JOIN prices p
              ON g.settlementdate = p.settlementdate
             AND g.region = p.regionid
            WHERE g.gen_mw > 0
        )
        SELECT fuel_type AS fuel,
               SUM(gen_mw) / 12.0 AS volume_mwh,
               SUM(gen_mw * rrp) / NULLIF(SUM(gen_mw), 0) AS vwap,
               AVG(gen_mw) AS avg_mw
        FROM joined
        GROUP BY fuel_type
        HAVING SUM(gen_mw) > 0
        ORDER BY volume_mwh DESC
    '''
    params = list(region_list) + [from_nem, to_nem] + list(region_list) + [from_nem, to_nem]

    conn = get_connection()
    try:
        rows = conn.execute(sql, params).fetchall()
    finally:
        conn.close()

    total_volume = sum(float(r[1]) for r in rows) if rows else 0.0

    data = []
    for fuel, volume_mwh, vwap, avg_mw in rows:
        share = (float(volume_mwh) / total_volume * 100.0) if total_volume > 0 else 0.0
        data.append({
            'fuel': fuel,
            'volume_mwh': round(float(volume_mwh), 1),
            'share_pct': round(share, 2),
            'vwap': round(float(vwap or 0.0), 2),
            'avg_mw': round(float(avg_mw or 0.0), 1),
        })

    return {
        'data': data,
        'meta': {
            'regions': region_list,
            'lookback_days': days,
            'total_volume_mwh': round(total_volume, 1),
            'from': from_utc.isoformat().replace('+00:00', 'Z'),
            'to': to_utc.isoformat().replace('+00:00', 'Z'),
            'as_of': datetime.now(timezone.utc).isoformat(),
        },
    }
