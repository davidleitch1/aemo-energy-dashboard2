"""GET /v1/futures/forward-curve — Today + 3-month-ago + 1-year-ago snapshots.

Reads ASX Energy weekly settlement futures from $AEMO_DATA_PATH/futures.csv.
Wide columns of the form 'ASX Energy Contract NSW 2026 Q3 Base Load Futures
Price ($)' are parsed to (region, year, quarter). For each snapshot date we
take the closest weekly row at or before, then collect future quarters
(>= current quarter) for the requested region.
"""
from __future__ import annotations

import os
import re
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

import pandas as pd
from fastapi import APIRouter, HTTPException, Query

router = APIRouter()

# ASX has no Tasmania contract.
VALID_REGIONS = {"NSW1", "QLD1", "SA1", "VIC1"}
REGION_TRIM = {"NSW1": "NSW", "QLD1": "QLD", "SA1": "SA", "VIC1": "VIC"}

DEFAULT_DATA_PATH = Path(os.environ.get(
    "AEMO_DATA_PATH",
    "/Users/davidleitch/aemo_production/data",
))

COL_RE = re.compile(
    r"(?:ASX Energy Contract\s+)?(\w+)\s+(\d{4})\s+Q(\d)(?:\s+Base Load Futures Price.*)?$"
)


def _read_futures_csv() -> pd.DataFrame:
    path = Path(os.environ.get("AEMO_DATA_PATH", str(DEFAULT_DATA_PATH))) / "futures.csv"
    if not path.exists():
        return pd.DataFrame()
    df = pd.read_csv(path, parse_dates=["Time (UTC+10)"])
    df = df.rename(columns={"Time (UTC+10)": "date"}).set_index("date").sort_index()
    return df


def _parse_columns(columns) -> dict[str, dict[tuple[int, int], str]]:
    """Returns {short_region: {(year, quarter): full_column_name}}."""
    out: dict[str, dict[tuple[int, int], str]] = {}
    for col in columns:
        m = COL_RE.match(col)
        if not m:
            continue
        region, year, q = m.group(1), int(m.group(2)), int(m.group(3))
        out.setdefault(region, {})[(year, q)] = col
    return out


def _quarter_label(year: int, q: int) -> str:
    return f"{year} Q{q}"


@router.get("/futures/forward-curve")
async def forward_curve(
    region: str = Query(..., min_length=2, max_length=8),
) -> dict:
    region = region.upper()
    if region not in VALID_REGIONS:
        raise HTTPException(
            status_code=400,
            detail={"code": "INVALID_REGION",
                    "message": f"futures available for NSW1/QLD1/SA1/VIC1 only"},
        )
    short = REGION_TRIM[region]

    df = _read_futures_csv()
    if df.empty:
        return {
            "data": {"snapshots": []},
            "meta": {
                "region": region,
                "as_of": datetime.now(timezone.utc).isoformat(),
                "data_available": False,
            },
        }

    contract_map = _parse_columns(df.columns)
    contracts = contract_map.get(short, {})

    today = df.index[-1]
    now_year = today.year
    now_quarter = (today.month - 1) // 3 + 1

    snapshot_specs = [
        ("Today",        today),
        ("3 months ago", today - timedelta(days=91)),
        ("1 year ago",   today - timedelta(days=365)),
    ]

    future_keys = sorted(
        (y, q) for (y, q) in contracts if (y, q) >= (now_year, now_quarter)
    )

    snapshots = []
    for label, target_date in snapshot_specs:
        idx = df.index.searchsorted(target_date)
        idx = min(idx, len(df) - 1)
        idx = max(idx, 0)
        actual_date = df.index[idx]
        row = df.iloc[idx]

        points = []
        for y, q in future_keys:
            col = contracts[(y, q)]
            val = row.get(col)
            if pd.notna(val):
                points.append({
                    "quarter": _quarter_label(y, q),
                    "year": y,
                    "q": q,
                    "price": round(float(val), 2),
                })

        snapshots.append({
            "label": label,
            "as_of": actual_date.isoformat() + "Z" if actual_date.tzinfo is None else actual_date.isoformat(),
            "points": points,
        })

    return {
        "data": {"snapshots": snapshots},
        "meta": {
            "region": region,
            "as_of": datetime.now(timezone.utc).isoformat(),
            "data_available": True,
            "latest_settlement": today.isoformat() + "Z" if today.tzinfo is None else today.isoformat(),
        },
    }


# ----------------------------------------------------------------------
# /v1/futures/expectations — Cal+1 and Cal+2 forward averages over time
# ----------------------------------------------------------------------

CUTOFF = pd.Timestamp('2024-01-01')


def _cal_year_avg(df: pd.DataFrame, contracts: dict, year: int) -> pd.Series:
    cols = [contracts.get((year, q)) for q in (1, 2, 3, 4)]
    cols = [c for c in cols if c and c in df.columns]
    if not cols:
        return pd.Series(dtype=float)
    return df[cols].mean(axis=1)


@router.get('/futures/expectations')
async def futures_expectations(
    region: str = Query(..., min_length=2, max_length=8),
) -> dict:
    region = region.upper()
    if region not in VALID_REGIONS:
        raise HTTPException(
            status_code=400,
            detail={'code': 'INVALID_REGION',
                    'message': 'futures available for NSW1/QLD1/SA1/VIC1 only'},
        )
    short = REGION_TRIM[region]

    df = _read_futures_csv()
    if df.empty:
        return {
            'data': {'cal1': [], 'cal2': []},
            'meta': {'region': region, 'data_available': False,
                     'as_of': datetime.now(timezone.utc).isoformat()},
        }

    contracts = _parse_columns(df.columns).get(short, {})
    today = df.index[-1]
    cal1_year = today.year + 1
    cal2_year = today.year + 2

    cal1 = _cal_year_avg(df, contracts, cal1_year)
    cal2 = _cal_year_avg(df, contracts, cal2_year)
    cal1 = cal1[cal1.index >= CUTOFF].dropna()
    cal2 = cal2[cal2.index >= CUTOFF].dropna()

    def _series(s: pd.Series) -> list[dict]:
        return [
            {'date': pd.Timestamp(d).isoformat() + 'Z',
             'price': round(float(v), 2)}
            for d, v in s.items()
        ]

    return {
        'data':  {'cal1': _series(cal1), 'cal2': _series(cal2)},
        'meta': {
            'region':         region,
            'cal1_year':      cal1_year,
            'cal2_year':      cal2_year,
            'cutoff':         CUTOFF.date().isoformat(),
            'data_available': True,
            'as_of':          datetime.now(timezone.utc).isoformat(),
        },
    }


# ----------------------------------------------------------------------
# /v1/futures/contracts — list of (year, quarter) available across all regions
# ----------------------------------------------------------------------

@router.get('/futures/contracts')
async def futures_contracts() -> dict:
    df = _read_futures_csv()
    if df.empty:
        return {'data': [], 'meta': {'as_of': datetime.now(timezone.utc).isoformat()}}
    parsed = _parse_columns(df.columns)
    keys: set = set()
    for region_contracts in parsed.values():
        keys.update(region_contracts.keys())
    items = [{'year': y, 'quarter': q, 'label': _quarter_label(y, q)}
             for (y, q) in sorted(keys)]
    return {'data': items, 'meta': {'count': len(items),
                                    'as_of': datetime.now(timezone.utc).isoformat()}}


# ----------------------------------------------------------------------
# /v1/futures/contract — single (year, quarter) settlement history per region
# ----------------------------------------------------------------------

@router.get('/futures/contract')
async def futures_contract(
    year: int = Query(...),
    quarter: int = Query(..., ge=1, le=4),
) -> dict:
    df = _read_futures_csv()
    if df.empty:
        return {'data': {}, 'meta': {'year': year, 'quarter': quarter,
                                     'data_available': False,
                                     'as_of': datetime.now(timezone.utc).isoformat()}}

    parsed = _parse_columns(df.columns)
    series: dict[str, list[dict]] = {}
    for short_region, contracts in parsed.items():
        col = contracts.get((year, quarter))
        if not col or col not in df.columns:
            continue
        s = df[col].dropna()
        if s.empty:
            continue
        # Map short region back to NEM region code
        full = next((k for k, v in REGION_TRIM.items() if v == short_region), short_region)
        series[full] = [
            {'date':  pd.Timestamp(d).isoformat() + 'Z',
             'price': round(float(v), 2)}
            for d, v in s.items()
        ]

    return {
        'data': series,
        'meta': {
            'year':           year,
            'quarter':        quarter,
            'label':          _quarter_label(year, quarter),
            'regions':        sorted(series.keys()),
            'data_available': bool(series),
            'as_of':          datetime.now(timezone.utc).isoformat(),
        },
    }
