"""GET /v1/predispatch and /v1/notices — Today / Gauges-tab supporting cards.

  /predispatch  Latest predispatch run for one region: per-30min price /
                demand / wind / solar forecast across the ~34h horizon.
                Source: predispatch table (DuckDB).
  /notices      Recent price-relevant AEMO market notices (RESERVE, POWER,
                INTER-REGIONAL, LOR/RERT keywords). Wraps the desktop
                helper at nem_dash/market_notices.fetch_market_notices,
                cached in-process for 5 minutes.
"""
from __future__ import annotations

import time
from datetime import datetime, timezone
from typing import Optional

from fastapi import APIRouter, HTTPException, Query

from ..db import get_connection

router = APIRouter()

VALID_REGIONS = {'NSW1', 'QLD1', 'VIC1', 'SA1', 'TAS1'}


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _utc_iso(dt: datetime) -> str:
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc).isoformat()
    return dt.astimezone(timezone.utc).isoformat()


# ----------------------------------------------------------------------
# /v1/predispatch
# ----------------------------------------------------------------------

@router.get('/predispatch')
async def predispatch(
    region: str = Query('NSW1'),
) -> dict:
    """Latest predispatch run for one region. Returns ~68 30-min slots
    spanning ~34 hours forward from the run time."""
    region = region.upper()
    if region not in VALID_REGIONS:
        raise HTTPException(
            status_code=400,
            detail={'code': 'INVALID_REGION', 'message': f'Unknown region: {region}'},
        )

    conn = get_connection()
    try:
        run_row = conn.execute('SELECT MAX(run_time) FROM predispatch').fetchone()
        run_time = run_row[0] if run_row else None
        if run_time is None:
            return {
                'data': [],
                'meta': {'region': region, 'run_time': None,
                         'horizon_slots': 0, 'as_of': _now_iso()},
            }

        rows = conn.execute(
            """SELECT settlementdate, price_forecast, demand_forecast,
                       solar_forecast, wind_forecast
                  FROM predispatch
                 WHERE run_time = ? AND regionid = ?
                 ORDER BY settlementdate""",
            [run_time, region],
        ).fetchall()
    finally:
        conn.close()

    data = [
        {
            'settlementdate':  _utc_iso(r[0]),
            'price_forecast':  None if r[1] is None else round(float(r[1]), 2),
            'demand_forecast': None if r[2] is None else round(float(r[2]), 1),
            'solar_forecast':  None if r[3] is None else round(float(r[3]), 1),
            'wind_forecast':   None if r[4] is None else round(float(r[4]), 1),
        }
        for r in rows
    ]
    return {
        'data': data,
        'meta': {
            'region':        region,
            'run_time':      _utc_iso(run_time),
            'horizon_slots': len(data),
            'as_of':         _now_iso(),
        },
    }


# ----------------------------------------------------------------------
# /v1/notices  (in-process TTL cache, 5 minutes)
# ----------------------------------------------------------------------

_notices_cache: dict = {'fetched_at': 0.0, 'limit': 0, 'data': []}
_NOTICES_TTL_SEC = 300


def _fetch_market_notices(limit: int) -> list[dict]:
    """Wrap the desktop helper, normalising datetimes to UTC ISO strings."""
    try:
        from aemo_dashboard.nem_dash.market_notices import fetch_market_notices
    except Exception:
        return []
    raw = fetch_market_notices(limit=limit) or []
    out = []
    for n in raw:
        cd = n.get('creation_date')
        cd_iso = _utc_iso(cd) if isinstance(cd, datetime) else None
        out.append({
            'id':                n.get('notice_id'),
            'type':              n.get('notice_type_id'),
            'type_description':  n.get('notice_type_description'),
            'creation_date':     cd_iso,
            'reason_short':      n.get('reason_short'),
            'reason_full':       n.get('reason_full'),
        })
    return out


@router.get('/notices')
async def notices(
    limit: int = Query(10, ge=1, le=20),
) -> dict:
    """Recent price-relevant AEMO market notices, last 48h.

    Falls back to an empty list if NEMweb is unreachable. Cached for 5 min
    so the iOS poll doesn't spam external HTTP.
    """
    now = time.time()
    cached_fresh = (
        now - _notices_cache['fetched_at'] < _NOTICES_TTL_SEC
        and _notices_cache['limit'] >= limit
    )
    if cached_fresh:
        return {
            'data': _notices_cache['data'][:limit],
            'meta': {
                'count':  min(limit, len(_notices_cache['data'])),
                'cached': True,
                'as_of':  _now_iso(),
            },
        }

    data = _fetch_market_notices(limit=limit)
    _notices_cache.update({'fetched_at': now, 'limit': limit, 'data': data})
    return {
        'data': data,
        'meta': {
            'count':  len(data),
            'cached': False,
            'as_of':  _now_iso(),
        },
    }
