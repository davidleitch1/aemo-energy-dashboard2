"""GET /v1/evening-peak — current vs PCP comparison for the 17:00-22:00 window.

Mirrors evening_peak/evening_analysis.get_evening_data: averages 30-min
generation by fuel + price + demand across a period_days window, and
compares with the same window 365 days earlier (PCP).

Response packages four cards' data:
  - current.fuel_mix / pcp.fuel_mix   stacked-area sources
  - current.price   / pcp.price       price-comparison line
  - fuel_changes                      waterfall deltas (current - pcp)
  - totals                            summary stats
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Optional

import pandas as pd
from fastapi import APIRouter, HTTPException, Query

from ..db import get_connection

router = APIRouter()

VALID_REGIONS = {'NEM', 'NSW1', 'QLD1', 'VIC1', 'SA1', 'TAS1'}
PHYSICAL_REGIONS = ['NSW1', 'QLD1', 'VIC1', 'SA1', 'TAS1']

# Stack ordering bottom -> top (matches desktop FUEL_ORDER).
FUEL_ORDER = ['Net Imports', 'Coal', 'Gas', 'Hydro', 'Wind', 'Solar', 'Rooftop Solar', 'Battery', 'Other']

# Waterfall components: skip Other and Rooftop Solar, keep Net Imports.
WATERFALL_FUELS = ['Net Imports', 'Coal', 'Gas', 'Hydro', 'Wind', 'Solar', 'Battery']

# Raw fuel_type from duid_info -> stack-bucket label.
FUEL_MAP = {
    'Coal':            'Coal',
    'CCGT':            'Gas',
    'OCGT':            'Gas',
    'Gas other':       'Gas',
    'Water':           'Hydro',
    'Wind':            'Wind',
    'Solar':           'Solar',
    'Battery Storage': 'Battery',
    'Biomass':         'Other',
}

# 10 thirty-minute slots covering 17:00-21:30 (the AEMO 30-min stamp at 17:00
# represents the period 16:30-17:00, so we use the desktop's [17,22) hour
# filter and label by start-of-period clock time).
SLOT_HOURS = [17.0, 17.5, 18.0, 18.5, 19.0, 19.5, 20.0, 20.5, 21.0, 21.5]
SLOT_LABELS = ['17:00', '17:30', '18:00', '18:30', '19:00',
               '19:30', '20:00', '20:30', '21:00', '21:30']


def _utc_iso(dt: datetime) -> str:
    return dt.replace(tzinfo=timezone.utc).isoformat() if dt.tzinfo is None else dt.astimezone(timezone.utc).isoformat()


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _slot_hour_from_ts(ts) -> float:
    """settlementdate -> hour-of-day Float (17.0..21.5)."""
    return float(ts.hour) + (0.5 if ts.minute >= 30 else 0.0)


def _load_window(conn, start: datetime, end: datetime, region: str) -> dict:
    """Return averaged fuel_mix + price for one window. Returns empty
    structures if no data overlaps the window."""
    region_filter = '' if region == 'NEM' else 'AND di."Region" = ?'
    region_params: list = [] if region == 'NEM' else [region]

    # ----- Fuel-bucket generation per (settlementdate, fuel) -----
    scada_sql = f'''
        WITH labelled AS (
            SELECT s.settlementdate,
                   GREATEST(s.scadavalue, 0) AS gen,
                   CASE
                       WHEN di."Fuel" IN ('CCGT','OCGT','Gas other') THEN 'Gas'
                       WHEN di."Fuel" = 'Water'           THEN 'Hydro'
                       WHEN di."Fuel" = 'Battery Storage' THEN 'Battery'
                       WHEN di."Fuel" IN ('Coal','Wind','Solar') THEN di."Fuel"
                       ELSE 'Other'
                   END AS fuel
            FROM scada30 s
            JOIN duid_info di ON s.duid = di."DUID"
            WHERE s.settlementdate >= ? AND s.settlementdate < ?
              AND EXTRACT(HOUR FROM s.settlementdate) >= 17
              AND EXTRACT(HOUR FROM s.settlementdate) < 22
              {region_filter}
        )
        SELECT settlementdate, fuel, SUM(gen) AS mw
        FROM labelled
        GROUP BY 1, 2
    '''
    scada = pd.DataFrame(
        conn.execute(scada_sql, [start, end, *region_params]).fetchall(),
        columns=['settlementdate', 'fuel', 'mw'],
    )

    # ----- Rooftop solar -----
    if region == 'NEM':
        rt_filter = "AND regionid IN ('NSW1','QLD1','VIC1','SA1','TAS1')"
        rt_params: list = [start, end]
    else:
        rt_filter = 'AND regionid = ?'
        rt_params = [start, end, region]
    rooftop_sql = f'''
        SELECT settlementdate, SUM(power) AS mw
        FROM rooftop30
        WHERE settlementdate >= ? AND settlementdate < ?
          AND EXTRACT(HOUR FROM settlementdate) >= 17
          AND EXTRACT(HOUR FROM settlementdate) < 22
          {rt_filter}
        GROUP BY 1
    '''
    rooftop = pd.DataFrame(
        conn.execute(rooftop_sql, rt_params).fetchall(),
        columns=['settlementdate', 'mw'],
    )
    rooftop['fuel'] = 'Rooftop Solar'

    # ----- Demand -> Net Imports (single-region only; NEM = 0) -----
    if region == 'NEM':
        demand = pd.DataFrame(columns=['settlementdate', 'demand'])
    else:
        demand_sql = '''
            SELECT settlementdate, demand
            FROM demand30
            WHERE settlementdate >= ? AND settlementdate < ?
              AND EXTRACT(HOUR FROM settlementdate) >= 17
              AND EXTRACT(HOUR FROM settlementdate) < 22
              AND regionid = ?
        '''
        demand = pd.DataFrame(
            conn.execute(demand_sql, [start, end, region]).fetchall(),
            columns=['settlementdate', 'demand'],
        )

    # ----- Prices (demand-weighted average) -----
    if region == 'NEM':
        price_sql = '''
            SELECT p.settlementdate,
                   SUM(p.rrp * d.demand) / NULLIF(SUM(d.demand), 0) AS vwap
            FROM prices30 p JOIN demand30 d
              ON p.settlementdate = d.settlementdate AND p.regionid = d.regionid
            WHERE p.settlementdate >= ? AND p.settlementdate < ?
              AND EXTRACT(HOUR FROM p.settlementdate) >= 17
              AND EXTRACT(HOUR FROM p.settlementdate) < 22
            GROUP BY 1
        '''
        price_params: list = [start, end]
    else:
        price_sql = '''
            SELECT settlementdate, rrp AS vwap
            FROM prices30
            WHERE settlementdate >= ? AND settlementdate < ?
              AND EXTRACT(HOUR FROM settlementdate) >= 17
              AND EXTRACT(HOUR FROM settlementdate) < 22
              AND regionid = ?
        '''
        price_params = [start, end, region]
    prices = pd.DataFrame(
        conn.execute(price_sql, price_params).fetchall(),
        columns=['settlementdate', 'vwap'],
    )

    # ----- Combine + average by slot -----
    gen = pd.concat([scada, rooftop[['settlementdate', 'fuel', 'mw']]], ignore_index=True)

    if gen.empty:
        return _empty_window()

    gen['slot'] = gen['settlementdate'].map(_slot_hour_from_ts)
    pivot = gen.pivot_table(
        index=['settlementdate', 'slot'], columns='fuel', values='mw',
        aggfunc='sum', fill_value=0,
    ).reset_index()

    # Net imports per period (single region only)
    if region != 'NEM' and not demand.empty:
        pivot = pivot.merge(demand, on='settlementdate', how='left')
        gen_cols = [c for c in FUEL_ORDER if c in pivot.columns and c != 'Net Imports']
        pivot['Net Imports'] = pivot['demand'].fillna(0) - pivot[gen_cols].sum(axis=1)
        pivot.drop(columns=['demand'], inplace=True)
    else:
        pivot['Net Imports'] = 0.0

    # Average by slot across the period_days window
    slot_avg = pivot.groupby('slot').mean(numeric_only=True).reset_index()

    fuel_mix: list[dict] = []
    for _, row in slot_avg.iterrows():
        for fuel in FUEL_ORDER:
            if fuel in slot_avg.columns:
                fuel_mix.append({
                    'time': float(row['slot']),
                    'fuel': fuel,
                    'mw':   round(float(row[fuel]), 2),
                })

    # Per-slot price
    if not prices.empty:
        prices['slot'] = prices['settlementdate'].map(_slot_hour_from_ts)
        price_avg = prices.groupby('slot')['vwap'].mean().reset_index()
        price_list = [
            {'time': float(r['slot']), 'vwap': round(float(r['vwap']), 2)}
            for _, r in price_avg.iterrows()
        ]
    else:
        price_list = []

    # Per-fuel totals (for waterfall + summary)
    fuel_totals: list[dict] = []
    for fuel in FUEL_ORDER:
        if fuel in slot_avg.columns:
            fuel_totals.append({
                'fuel': fuel,
                'mw':   round(float(slot_avg[fuel].sum()), 2),
            })

    gen_total = sum(ft['mw'] for ft in fuel_totals if ft['fuel'] != 'Net Imports')
    vwap_total = round(float(prices['vwap'].mean()), 2) if not prices.empty else None

    return {
        'fuel_mix':   fuel_mix,
        'price':      price_list,
        'fuel_totals': fuel_totals,
        'totals':     {'generation_mw': round(gen_total, 2), 'vwap': vwap_total},
    }


def _empty_window() -> dict:
    return {
        'fuel_mix':    [],
        'price':       [],
        'fuel_totals': [],
        'totals':      {'generation_mw': 0.0, 'vwap': None},
    }


@router.get('/evening-peak')
async def evening_peak(
    region: str = Query('NEM'),
    period_days: int = Query(30, ge=7, le=365),
) -> dict:
    """17:00-22:00 fuel mix + price comparison vs 365-day prior PCP."""
    if region not in VALID_REGIONS:
        raise HTTPException(
            status_code=400,
            detail={'code': 'INVALID_REGION', 'message': f'Unknown region: {region}'},
        )

    conn = get_connection()
    try:
        latest = conn.execute('SELECT MAX(settlementdate) FROM scada30').fetchone()[0]
        if latest is None:
            return _empty_response(region, period_days)

        cur_end_excl = latest + timedelta(days=1)
        cur_start = latest - timedelta(days=period_days - 1)
        # Strip to date boundaries; AEMO half-hours are naive datetimes.
        cur_end_excl = datetime(cur_end_excl.year, cur_end_excl.month, cur_end_excl.day)
        cur_start    = datetime(cur_start.year,    cur_start.month,    cur_start.day)

        pcp_end_excl = cur_end_excl - timedelta(days=365)
        pcp_start    = cur_start    - timedelta(days=365)

        current = _load_window(conn, cur_start, cur_end_excl, region)
        pcp     = _load_window(conn, pcp_start, pcp_end_excl, region)
    finally:
        conn.close()

    # ----- Fuel changes (waterfall) -----
    cur_map = {ft['fuel']: ft['mw'] for ft in current['fuel_totals']}
    pcp_map = {ft['fuel']: ft['mw'] for ft in pcp['fuel_totals']}
    fuel_changes = []
    for fuel in WATERFALL_FUELS:
        cur = cur_map.get(fuel, 0.0)
        prv = pcp_map.get(fuel, 0.0)
        fuel_changes.append({
            'fuel':       fuel,
            'current_mw': round(cur, 2),
            'pcp_mw':     round(prv, 2),
            'delta_mw':   round(cur - prv, 2),
        })

    return {
        'data': {
            'current': {k: current[k] for k in ('fuel_mix', 'price', 'totals')},
            'pcp':     {k: pcp[k]     for k in ('fuel_mix', 'price', 'totals')},
            'fuel_changes': fuel_changes,
        },
        'meta': {
            'region':         region,
            'period_days':    period_days,
            'current_window': {'start': cur_start.date().isoformat(), 'end': cur_end_excl.date().isoformat()},
            'pcp_window':     {'start': pcp_start.date().isoformat(), 'end': pcp_end_excl.date().isoformat()},
            'fuel_order':     FUEL_ORDER,
            'as_of':          _now_iso(),
        },
    }


def _empty_response(region: str, period_days: int) -> dict:
    return {
        'data': {
            'current': {'fuel_mix': [], 'price': [], 'totals': {'generation_mw': 0.0, 'vwap': None}},
            'pcp':     {'fuel_mix': [], 'price': [], 'totals': {'generation_mw': 0.0, 'vwap': None}},
            'fuel_changes': [],
        },
        'meta': {
            'region': region, 'period_days': period_days,
            'current_window': None, 'pcp_window': None,
            'fuel_order': FUEL_ORDER, 'as_of': _now_iso(),
        },
    }
