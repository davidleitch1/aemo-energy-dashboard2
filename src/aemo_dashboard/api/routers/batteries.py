"""GET /v1/batteries/* — battery dispatch & revenue analytics.

Endpoints (filled in across phases B1-B5):

  /overview          — system-wide top-N by metric (B1)
  /owners            — distinct owners across battery DUIDs (B3)
  /list              — DUIDs filtered by regions[] x owners[] (B3)
  /fleet-timeseries  — per-DUID series with LTTB downsampling (B4)
  /fleet-tod         — hour-of-day average per DUID (B5)

All numeric metrics derive from scada30 (settlementdate, duid, scadavalue)
joined to prices30 (settlementdate, regionid, rrp), with battery metadata
from duid_info (DUID, Site Name, Owner, Region, Capacity(MW), Storage(MWh),
Fuel='Battery Storage'). Discharge = scadavalue > 0; charge = scadavalue < 0.
Energy = MW * 0.5 hrs (30-min cadence).
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

import duckdb
from typing import Literal, Optional

from fastapi import APIRouter, HTTPException, Query

from ..db import get_connection, nem_naive_to_utc

router = APIRouter()

VALID_REGIONS = {'NEM', 'NSW1', 'QLD1', 'SA1', 'TAS1', 'VIC1'}

METRIC_LABELS = {
    'discharge_revenue': 'Discharge Revenue',
    'charge_cost':       'Charge Cost',
    'discharge_price':   'Discharge Price',
    'charge_price':      'Charge Price',
    'discharge_energy':  'Discharge Energy',
    'charge_energy':     'Charge Energy',
    'price_spread':      'Price Spread',
}

METRIC_UNITS = {
    'discharge_revenue': '$',
    'charge_cost':       '$',
    'discharge_price':   '$/MWh',
    'charge_price':      '$/MWh',
    'discharge_energy':  'MWh',
    'charge_energy':     'MWh',
    'price_spread':      '$/MWh',
}

OverviewMetric = Literal[
    'discharge_revenue', 'charge_cost', 'discharge_price', 'charge_price',
    'discharge_energy', 'charge_energy', 'price_spread',
]


def _utc_iso(dt: datetime) -> str:
    return nem_naive_to_utc(dt).isoformat().replace('+00:00', 'Z')


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _naive(dt: Optional[datetime]) -> Optional[datetime]:
    if dt is None:
        return None
    return dt.replace(tzinfo=None) if dt.tzinfo else dt


@router.get('/batteries/overview')
async def batteries_overview(
    region: str = Query('NEM'),
    metric: OverviewMetric = Query('discharge_revenue'),
    from_: Optional[datetime] = Query(None, alias='from'),
    to: Optional[datetime] = Query(None),
    limit: int = Query(20, ge=1, le=100),
) -> dict:
    """Top-N batteries by selected metric across the date window.

    Per-DUID metrics computed from scada30 x prices30 joined on
    (settlementdate, region). Ordered desc by metric, capped at limit.
    """
    if region not in VALID_REGIONS:
        raise HTTPException(
            status_code=400,
            detail={'code': 'INVALID_REGION', 'message': f'Unknown region: {region}'},
        )

    to_naive = _naive(to) or datetime.now()
    from_naive = _naive(from_) or (to_naive - timedelta(days=30))

    region_filter = '' if region == 'NEM' else 'AND di."Region" = ?'
    sql = f"""
        WITH joined AS (
            SELECT s.duid,
                   s.scadavalue,
                   di."Site Name" AS site_name,
                   di."Owner"     AS owner,
                   di."Region"    AS region,
                   di."Capacity(MW)"  AS capacity_mw,
                   di."Storage(MWh)" AS storage_mwh,
                   p.rrp
            FROM scada30 s
            JOIN duid_info di
              ON s.duid = di."DUID" AND di."Fuel" = 'Battery Storage'
            JOIN prices30 p
              ON s.settlementdate = p.settlementdate AND p.regionid = di."Region"
            WHERE s.settlementdate >= ? AND s.settlementdate <= ?
              {region_filter}
        )
        SELECT duid, site_name, owner, region, capacity_mw, storage_mwh,
               SUM(CASE WHEN scadavalue > 0 THEN scadavalue ELSE 0 END) / 2.0
                 AS discharge_energy,
               SUM(CASE WHEN scadavalue > 0 THEN scadavalue * rrp ELSE 0 END) / 2.0
                 AS discharge_revenue,
               ABS(SUM(CASE WHEN scadavalue < 0 THEN scadavalue ELSE 0 END)) / 2.0
                 AS charge_energy,
               ABS(SUM(CASE WHEN scadavalue < 0 THEN scadavalue * rrp ELSE 0 END)) / 2.0
                 AS charge_cost
        FROM joined
        GROUP BY duid, site_name, owner, region, capacity_mw, storage_mwh
    """

    params: list = [from_naive, to_naive]
    if region != 'NEM':
        params.append(region)

    conn = get_connection()
    try:
        rows = conn.execute(sql, params).fetchall()
    finally:
        conn.close()

    out: list[dict] = []
    for (duid, site, owner, reg, cap, stor, de, dr, ce, cc) in rows:
        de = float(de or 0.0); dr = float(dr or 0.0)
        ce = float(ce or 0.0); cc = float(cc or 0.0)
        dp = dr / de if de > 0 else 0.0
        cp = cc / ce if ce > 0 else 0.0
        values = {
            'discharge_revenue': dr,
            'charge_cost':       cc,
            'discharge_price':   dp,
            'charge_price':      cp,
            'discharge_energy':  de,
            'charge_energy':     ce,
            'price_spread':      dp - cp,
        }
        out.append({
            'duid':         duid,
            'site_name':    site,
            'owner':        owner,
            'region':       reg,
            'capacity_mw':  float(cap) if cap is not None else 0.0,
            'storage_mwh':  float(stor) if stor is not None else 0.0,
            'value':        round(values[metric], 4),
        })

    out.sort(key=lambda r: r['value'], reverse=True)
    total_count = len(out)
    out = out[:limit]

    return {
        'data': out,
        'meta': {
            'metric':       metric,
            'metric_label': METRIC_LABELS[metric],
            'units':        METRIC_UNITS[metric],
            'region':       region,
            'limit':        limit,
            'total_count':  total_count,
            'from':         _utc_iso(from_naive),
            'to':           _utc_iso(to_naive),
            'as_of':        _now_iso(),
        },
    }


# ----------------------------------------------------------------------
# /v1/batteries/owners — distinct owner list (Fleet filter sheet)
# ----------------------------------------------------------------------

@router.get('/batteries/owners')
async def batteries_owners() -> dict:
    """Distinct owners across battery DUIDs, alphabetical."""
    sql = '''
        SELECT DISTINCT "Owner" AS owner
        FROM duid_info
        WHERE "Fuel" = 'Battery Storage' AND "Owner" IS NOT NULL AND "Owner" <> ''
        ORDER BY 1
    '''
    conn = get_connection()
    try:
        rows = [r[0] for r in conn.execute(sql).fetchall()]
    finally:
        conn.close()
    return {'data': rows, 'meta': {'count': len(rows), 'as_of': _now_iso()}}


# ----------------------------------------------------------------------
# /v1/batteries/list — DUIDs filtered by regions[] x owners[]
# ----------------------------------------------------------------------

PHYSICAL_REGIONS = {'NSW1', 'QLD1', 'SA1', 'TAS1', 'VIC1'}


@router.get('/batteries/list')
async def batteries_list(
    regions: Optional[str] = Query(None),
    owners: Optional[str] = Query(None),
) -> dict:
    """Filtered battery list ordered by capacity descending.

    Display string mirrors desktop `insights_tab.py`:
        '{Site} ({DUID}) — {capacity:.0f} MW / {storage:.0f} MWh ({duration:.1f}h)'
    """
    region_list = [r.strip().upper() for r in (regions or '').split(',') if r.strip()]
    owner_list  = [o.strip()         for o in (owners  or '').split(',') if o.strip()]

    bad = [r for r in region_list if r not in PHYSICAL_REGIONS]
    if bad:
        raise HTTPException(
            status_code=400,
            detail={'code': 'INVALID_REGION', 'message': f'Unknown region(s): {bad}'},
        )

    where = ['"Fuel" = \'Battery Storage\'']
    params: list = []
    if region_list:
        ph = ','.join(['?'] * len(region_list))
        where.append(f'"Region" IN ({ph})')
        params.extend(region_list)
    if owner_list:
        ph = ','.join(['?'] * len(owner_list))
        where.append(f'"Owner" IN ({ph})')
        params.extend(owner_list)

    sql = f'''
        SELECT "DUID" AS duid, "Site Name" AS site_name, "Owner" AS owner,
               "Region" AS region, "Capacity(MW)" AS capacity_mw,
               "Storage(MWh)" AS storage_mwh
        FROM duid_info
        WHERE {' AND '.join(where)}
        ORDER BY "Capacity(MW)" DESC NULLS LAST, "DUID"
    '''

    conn = get_connection()
    try:
        rows = conn.execute(sql, params).fetchall()
    finally:
        conn.close()

    out: list[dict] = []
    for (duid, site, owner, region, cap, stor) in rows:
        cap = float(cap or 0.0); stor = float(stor or 0.0)
        dur = stor / cap if cap > 0 else 0.0
        display = f'{site} ({duid}) — {cap:.0f} MW / {stor:.0f} MWh ({dur:.1f}h)'
        out.append({
            'duid':         duid,
            'site_name':    site,
            'owner':        owner,
            'region':       region,
            'capacity_mw':  cap,
            'storage_mwh':  stor,
            'display':      display,
        })

    return {
        'data': out,
        'meta': {
            'count':   len(out),
            'regions': region_list or None,
            'owners':  owner_list or None,
            'as_of':   _now_iso(),
        },
    }


# ----------------------------------------------------------------------
# /v1/batteries/fleet-timeseries — per-DUID series, LTTB-downsampled (B4)
# ----------------------------------------------------------------------

from ..downsample import lttb

FREQ_TO_TABLE = {'5m': 'scada5', '30m': 'scada30', '1h': 'scada30', 'D': 'scada30'}
FREQ_BUCKET   = {'1h': "INTERVAL '1 hour'", 'D': "INTERVAL '1 day'"}
MAX_POINTS    = 1500  # per DUID

FleetFrequency = Literal['5m', '30m', '1h', 'D']


def _validate_regions(region_list: list[str]) -> None:
    bad = [r for r in region_list if r not in PHYSICAL_REGIONS]
    if bad:
        raise HTTPException(
            status_code=400,
            detail={'code': 'INVALID_REGION', 'message': f'Unknown region(s): {bad}'},
        )


def _csv(value: Optional[str]) -> list[str]:
    return [v.strip() for v in (value or '').split(',') if v.strip()]


def _resolve_target_duids(
    conn,
    regions: list[str],
    owners: list[str],
    batteries: list[str],
) -> list[str]:
    """Apply region/owner/battery filters against duid_info, return DUIDs."""
    where = ['"Fuel" = \'Battery Storage\'']
    params: list = []
    if regions:
        where.append(f'"Region" IN ({",".join(["?"] * len(regions))})')
        params.extend(regions)
    if owners:
        where.append(f'"Owner" IN ({",".join(["?"] * len(owners))})')
        params.extend(owners)
    if batteries:
        where.append(f'"DUID" IN ({",".join(["?"] * len(batteries))})')
        params.extend(batteries)
    sql = f'SELECT "DUID" FROM duid_info WHERE {" AND ".join(where)}'
    return [r[0] for r in conn.execute(sql, params).fetchall()]


@router.get('/batteries/fleet-timeseries')
async def batteries_fleet_timeseries(
    regions: Optional[str] = Query(None),
    owners: Optional[str] = Query(None),
    batteries: Optional[str] = Query(None),
    from_: Optional[datetime] = Query(None, alias='from'),
    to: Optional[datetime] = Query(None),
    frequency: FleetFrequency = Query('30m'),
) -> dict:
    """Per-DUID scada time series, optionally aggregated and LTTB-downsampled.

    5m source: scada5 (history from 2024-08-23).
    30m / 1h / D source: scada30 (history from 2020-02-01).
    """
    region_list = [r.upper() for r in _csv(regions)]
    owner_list = _csv(owners)
    battery_list = [b.upper() for b in _csv(batteries)]
    _validate_regions(region_list)

    to_naive = _naive(to) or datetime.now()
    from_naive = _naive(from_) or (to_naive - timedelta(days=30))

    conn = get_connection()
    try:
        target_duids = _resolve_target_duids(conn, region_list, owner_list, battery_list)
        if not target_duids:
            return _ts_empty(frequency, from_naive, to_naive, [])

        table = FREQ_TO_TABLE[frequency]
        duid_ph = ','.join(['?'] * len(target_duids))
        if frequency in FREQ_BUCKET:
            select_t = f'time_bucket({FREQ_BUCKET[frequency]}, s.settlementdate)'
            sql = f'''
                SELECT {select_t} AS t, s.duid, AVG(s.scadavalue) AS value
                FROM {table} s
                WHERE s.duid IN ({duid_ph})
                  AND s.settlementdate >= ? AND s.settlementdate <= ?
                GROUP BY 1, 2
                ORDER BY s.duid, 1
            '''
        else:
            sql = f'''
                SELECT s.settlementdate AS t, s.duid, s.scadavalue AS value
                FROM {table} s
                WHERE s.duid IN ({duid_ph})
                  AND s.settlementdate >= ? AND s.settlementdate <= ?
                ORDER BY s.duid, s.settlementdate
            '''
        params = list(target_duids) + [from_naive, to_naive]
        try:
            rows = conn.execute(sql, params).fetchall()
        except duckdb.CatalogException:
            return _ts_empty(frequency, from_naive, to_naive, target_duids)
    finally:
        conn.close()

    # Group by duid; LTTB-downsample each series; flatten back.
    by_duid: dict[str, list[tuple[datetime, float]]] = {}
    for (t, duid, value) in rows:
        if value is None:
            continue
        by_duid.setdefault(duid, []).append((t, float(value)))

    out: list[dict] = []
    downsampled = False
    source_rows = sum(len(v) for v in by_duid.values())
    for duid in sorted(by_duid):
        series = by_duid[duid]
        if len(series) > MAX_POINTS:
            ts_idx = list(range(len(series)))
            vals = [v for (_, v) in series]
            keep_idx, _ = lttb(ts_idx, vals, MAX_POINTS)
            keep = {int(i) for i in keep_idx}
            series = [series[i] for i in range(len(series)) if i in keep]
            downsampled = True
        for (t, value) in series:
            out.append({'duid': duid, 't': _utc_iso(t), 'value': round(value, 4)})

    return {
        'data': out,
        'meta': {
            'frequency':     frequency,
            'duids':         sorted(by_duid.keys()) or sorted(target_duids),
            'from':          _utc_iso(from_naive),
            'to':            _utc_iso(to_naive),
            'as_of':         _now_iso(),
            'source_rows':   source_rows,
            'returned_rows': len(out),
            'downsampled':   downsampled,
        },
    }


def _ts_empty(frequency: str, from_dt: datetime, to_dt: datetime, duids: list[str]) -> dict:
    return {
        'data': [],
        'meta': {
            'frequency':     frequency,
            'duids':         duids,
            'from':          _utc_iso(from_dt),
            'to':            _utc_iso(to_dt),
            'as_of':         _now_iso(),
            'source_rows':   0,
            'returned_rows': 0,
            'downsampled':   False,
        },
    }


# ----------------------------------------------------------------------
# /v1/batteries/fleet-tod — hour-of-day average per DUID (B5)
# ----------------------------------------------------------------------

@router.get('/batteries/fleet-tod')
async def batteries_fleet_tod(
    regions: Optional[str] = Query(None),
    owners: Optional[str] = Query(None),
    batteries: Optional[str] = Query(None),
    from_: Optional[datetime] = Query(None, alias='from'),
    to: Optional[datetime] = Query(None),
) -> dict:
    """Hour-of-day average scada (MW) per DUID over the date window.

    Always reads scada30 — TOD aggregates to hours, so 30-min cadence is
    sufficient and the long history is preferable.
    """
    region_list = [r.upper() for r in _csv(regions)]
    owner_list = _csv(owners)
    battery_list = [b.upper() for b in _csv(batteries)]
    _validate_regions(region_list)

    to_naive = _naive(to) or datetime.now()
    from_naive = _naive(from_) or (to_naive - timedelta(days=30))

    conn = get_connection()
    try:
        target_duids = _resolve_target_duids(conn, region_list, owner_list, battery_list)
        if not target_duids:
            return {
                'data': [],
                'meta': {
                    'duids': [],
                    'from':  _utc_iso(from_naive),
                    'to':    _utc_iso(to_naive),
                    'as_of': _now_iso(),
                },
            }

        duid_ph = ','.join(['?'] * len(target_duids))
        sql = f'''
            SELECT EXTRACT(hour FROM s.settlementdate)::INTEGER AS hour,
                   s.duid,
                   AVG(s.scadavalue) AS value
            FROM scada30 s
            WHERE s.duid IN ({duid_ph})
              AND s.settlementdate >= ? AND s.settlementdate <= ?
            GROUP BY 1, 2
            ORDER BY s.duid, 1
        '''
        params = list(target_duids) + [from_naive, to_naive]
        rows = conn.execute(sql, params).fetchall()
    finally:
        conn.close()

    out = [
        {'duid': duid, 'hour': int(hour), 'value': round(float(value), 4)}
        for (hour, duid, value) in rows
        if value is not None
    ]
    return {
        'data': out,
        'meta': {
            'duids': sorted({r['duid'] for r in out}) or sorted(target_duids),
            'from':  _utc_iso(from_naive),
            'to':    _utc_iso(to_naive),
            'as_of': _now_iso(),
        },
    }
