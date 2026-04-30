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
