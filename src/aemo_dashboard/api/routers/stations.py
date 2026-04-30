"""GET /v1/stations* — physical-station-level rollups (sums DUIDs).

  /stations              full list, joined from duid_info — name, region,
                         fuel, owner, summed capacity, DUID count.
  /stations/time-series  per-30min station total dispatch + price
                         over the requested window. Uses the pre-built
                         station_time_series_30min when present, falls
                         back to scada30 × duid_info × prices30 join.
  /stations/tod          hour-of-day average station-total dispatch.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Literal, Optional

import duckdb
from fastapi import APIRouter, HTTPException, Query

from ..db import get_connection
from ..downsample import lttb

router = APIRouter()

VALID_REGIONS = {'NSW1', 'QLD1', 'VIC1', 'SA1', 'TAS1'}
MAX_POINTS = 1500

TSFreq = Literal['30m', '1h', 'D']
FREQ_BUCKET = {'1h': "INTERVAL '1 hour'", 'D': "INTERVAL '1 day'"}


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _utc_iso(dt: datetime) -> str:
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc).isoformat()
    return dt.astimezone(timezone.utc).isoformat()


def _naive(dt: Optional[datetime]) -> Optional[datetime]:
    if dt is None:
        return None
    return dt.replace(tzinfo=None) if dt.tzinfo else dt


def _resolve_station(conn, station: str) -> Optional[dict]:
    """Look up station metadata + summed capacity. Returns None if unknown."""
    rows = conn.execute(
        '''
        SELECT "Site Name" AS station_name,
               MAX("Region")  AS region,
               MAX("Owner")   AS owner,
               MAX("Fuel")    AS fuel,
               SUM("Capacity(MW)") AS capacity_mw,
               COUNT(*) AS duid_count,
               LIST("DUID") AS duids
        FROM duid_info
        WHERE "Site Name" = ?
        GROUP BY 1
        ''',
        [station],
    ).fetchall()
    if not rows:
        return None
    name, region, owner, fuel, cap, n_duids, duids = rows[0]
    return {
        'station_name': name,
        'region':       region,
        'owner':        owner,
        'fuel':         fuel,
        'capacity_mw':  float(cap) if cap is not None else 0.0,
        'duid_count':   int(n_duids),
        'duids':        list(duids) if duids else [],
    }


# ----------------------------------------------------------------------
# /v1/stations — list
# ----------------------------------------------------------------------

@router.get('/stations')
async def stations_list(
    region: Optional[str] = Query(None),
) -> dict:
    region = region.upper() if region else None
    if region is not None and region not in VALID_REGIONS:
        raise HTTPException(
            status_code=400,
            detail={'code': 'INVALID_REGION', 'message': f'Unknown region: {region}'},
        )

    where = ['"Site Name" IS NOT NULL', "\"Site Name\" <> ''"]
    params: list = []
    if region:
        where.append('"Region" = ?')
        params.append(region)
    sql = f'''
        SELECT "Site Name" AS station_name,
               MAX("Region")  AS region,
               MAX("Owner")   AS owner,
               MAX("Fuel")    AS fuel,
               SUM("Capacity(MW)") AS capacity_mw,
               COUNT(*) AS duid_count
        FROM duid_info
        WHERE {' AND '.join(where)}
        GROUP BY 1
        ORDER BY capacity_mw DESC NULLS LAST, station_name
    '''
    conn = get_connection()
    try:
        rows = conn.execute(sql, params).fetchall()
    finally:
        conn.close()

    data = [
        {
            'station_name': r[0],
            'region':       r[1],
            'owner':        r[2],
            'fuel':         r[3],
            'capacity_mw':  float(r[4]) if r[4] is not None else 0.0,
            'duid_count':   int(r[5]),
        }
        for r in rows
    ]
    return {
        'data': data,
        'meta': {
            'count':  len(data),
            'region': region,
            'as_of':  _now_iso(),
        },
    }


# ----------------------------------------------------------------------
# /v1/stations/time-series
# ----------------------------------------------------------------------

@router.get('/stations/time-series')
async def stations_time_series(
    station:     str  = Query(..., min_length=1),
    period_days: int  = Query(30, ge=1, le=365),
    frequency:   TSFreq = Query('30m'),
) -> dict:
    """Per-period station total (sum across DUIDs) + averaged price."""
    conn = get_connection()
    try:
        meta = _resolve_station(conn, station)
        if meta is None:
            raise HTTPException(
                status_code=404,
                detail={'code': 'STATION_NOT_FOUND', 'message': f'No station named {station!r}'},
            )

        end_excl = datetime.now()
        start = end_excl - timedelta(days=period_days)
        rows = _query_time_series(conn, station, start, end_excl, frequency, meta)
    finally:
        conn.close()

    # LTTB cap (per series — only one here)
    src_rows = len(rows)
    downsampled = False
    if src_rows > MAX_POINTS:
        ts_idx = list(range(src_rows))
        vals   = [r[1] for r in rows]
        keep_idx, _ = lttb(ts_idx, vals, MAX_POINTS)
        keep = {int(i) for i in keep_idx}
        rows = [rows[i] for i in range(src_rows) if i in keep]
        downsampled = True

    data = [
        {
            't':       _utc_iso(r[0]),
            'gen_mw':  None if r[1] is None else round(float(r[1]), 2),
            'price':   None if r[2] is None else round(float(r[2]), 2),
        }
        for r in rows
    ]

    return {
        'data': data,
        'meta': {
            'station_name':  meta['station_name'],
            'region':        meta['region'],
            'fuel':          meta['fuel'],
            'owner':         meta['owner'],
            'capacity_mw':   meta['capacity_mw'],
            'duid_count':    meta['duid_count'],
            'frequency':     frequency,
            'period_days':   period_days,
            'from':          _utc_iso(start),
            'to':            _utc_iso(end_excl),
            'source_rows':   src_rows,
            'returned_rows': len(data),
            'downsampled':   downsampled,
            'as_of':         _now_iso(),
        },
    }


def _query_time_series(conn, station, start, end_excl, frequency, meta) -> list:
    """Return list of (t, gen_mw, price). Prefer station_time_series_30min,
    fall back to scada30 × duid_info × prices30 join."""
    bucket = FREQ_BUCKET.get(frequency)
    select_t = (
        f'time_bucket({bucket}, settlementdate)' if bucket else 'settlementdate'
    )

    # Preferred: pre-built station table.
    sql_fast = f'''
        SELECT {select_t} AS t, SUM(scadavalue) AS gen_mw, AVG(price) AS price
        FROM station_time_series_30min
        WHERE station_name = ?
          AND settlementdate >= ? AND settlementdate < ?
        GROUP BY 1
        ORDER BY 1
    '''
    try:
        return conn.execute(sql_fast, [station, start, end_excl]).fetchall()
    except duckdb.CatalogException:
        pass  # Fall through to scada30-based query.

    duid_ph = ','.join(['?'] * len(meta['duids']))
    sql_fb = f'''
        SELECT {select_t.replace('settlementdate', 's.settlementdate')} AS t,
               SUM(s.scadavalue) AS gen_mw,
               AVG(p.rrp)        AS price
        FROM scada30 s
        LEFT JOIN prices30 p
          ON s.settlementdate = p.settlementdate AND p.regionid = ?
        WHERE s.duid IN ({duid_ph})
          AND s.settlementdate >= ? AND s.settlementdate < ?
        GROUP BY 1
        ORDER BY 1
    '''
    params: list = [meta['region']] + list(meta['duids']) + [start, end_excl]
    return conn.execute(sql_fb, params).fetchall()


# ----------------------------------------------------------------------
# /v1/stations/tod — hour-of-day station total
# ----------------------------------------------------------------------

@router.get('/stations/tod')
async def stations_tod(
    station:     str = Query(..., min_length=1),
    period_days: int = Query(30, ge=1, le=365),
) -> dict:
    conn = get_connection()
    try:
        meta = _resolve_station(conn, station)
        if meta is None:
            raise HTTPException(
                status_code=404,
                detail={'code': 'STATION_NOT_FOUND', 'message': f'No station named {station!r}'},
            )

        end_excl = datetime.now()
        start = end_excl - timedelta(days=period_days)
        rows = _query_tod(conn, station, start, end_excl, meta)
    finally:
        conn.close()

    data = [
        {'hour': int(h), 'avg_gen_mw': None if v is None else round(float(v), 2)}
        for (h, v) in rows
    ]
    return {
        'data': data,
        'meta': {
            'station_name': meta['station_name'],
            'capacity_mw':  meta['capacity_mw'],
            'period_days':  period_days,
            'from':         _utc_iso(start),
            'to':           _utc_iso(end_excl),
            'as_of':        _now_iso(),
        },
    }


def _query_tod(conn, station, start, end_excl, meta) -> list:
    """List of (hour, avg_gen_mw). Prefer pre-built table; else compute."""
    sql_fast = '''
        WITH per_period AS (
            SELECT settlementdate, SUM(scadavalue) AS gen_mw
            FROM station_time_series_30min
            WHERE station_name = ?
              AND settlementdate >= ? AND settlementdate < ?
            GROUP BY 1
        )
        SELECT EXTRACT(hour FROM settlementdate)::INTEGER AS hour,
               AVG(gen_mw) AS avg_gen_mw
        FROM per_period
        GROUP BY 1
        ORDER BY 1
    '''
    try:
        return conn.execute(sql_fast, [station, start, end_excl]).fetchall()
    except duckdb.CatalogException:
        pass

    duid_ph = ','.join(['?'] * len(meta['duids']))
    sql_fb = f'''
        WITH per_period AS (
            SELECT settlementdate, SUM(scadavalue) AS gen_mw
            FROM scada30
            WHERE duid IN ({duid_ph})
              AND settlementdate >= ? AND settlementdate < ?
            GROUP BY 1
        )
        SELECT EXTRACT(hour FROM settlementdate)::INTEGER AS hour,
               AVG(gen_mw) AS avg_gen_mw
        FROM per_period
        GROUP BY 1
        ORDER BY 1
    '''
    params: list = list(meta['duids']) + [start, end_excl]
    return conn.execute(sql_fb, params).fetchall()
