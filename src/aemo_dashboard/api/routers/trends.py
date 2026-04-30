"""GET /v1/trends/* — long-run renewable penetration trends.

Three endpoints, all server-side aggregated to daily means then smoothed
in pandas (rolling MA), annualised as TWh = mw * 24 * 365 / 1e6:

  /vre-production           — current + 2 prior years overlaid by day-of-year
                              (30-day MA, 1 fuel: VRE/Solar/Wind/Rooftop)
  /vre-by-fuel              — daily, 2018–now, 3 fuels (Rooftop/Solar/Wind)
                              (30-day MA)
  /thermal-vs-renewables    — daily, 2018–now, 2 categories
                              (180-day MA)
"""
from __future__ import annotations

from datetime import date, datetime, timezone
from typing import Optional

import pandas as pd
from fastapi import APIRouter, HTTPException, Query

from ..db import get_connection

router = APIRouter()

VALID_REGIONS = {"NEM", "NSW1", "QLD1", "SA1", "TAS1", "VIC1"}
PHYSICAL_REGIONS = ["NSW1", "QLD1", "SA1", "TAS1", "VIC1"]

# Sets matching the dashboard's penetration_tab.py.
RENEWABLE_FUELS = {"Wind", "Solar", "Rooftop", "Water"}  # Hydro = Water
THERMAL_FUELS = {"Coal", "CCGT", "OCGT", "Gas other"}

START_YEAR = 2018  # Charts 2 + 3 always start here.


def _validate_region(region: str) -> list[str]:
    """Return the SQL region filter list — single region, or all 5 for NEM."""
    if region not in VALID_REGIONS:
        raise HTTPException(
            status_code=400,
            detail={"code": "INVALID_REGION", "message": f"Unknown region: {region}"},
        )
    return PHYSICAL_REGIONS if region == "NEM" else [region]


def _annualise_twh(mw_series: pd.Series) -> pd.Series:
    """mw average → TWh annualised.  mw * 24 * 365 / 1e6."""
    return mw_series * 24 * 365 / 1_000_000


def _load_daily_utility(
    regions: list[str], fuels: set[str], start: datetime
) -> pd.DataFrame:
    """Daily mean utility-scale generation (MW) for the given fuels and regions.

    Returned columns: date, fuel, mw_daily.  Gas types merged into 'Gas';
    Biomass dropped.  Rows where the calling fuel set excludes a row are
    filtered upstream in SQL.
    """
    placeholders = ",".join(["?"] * len(regions))
    fuel_placeholders = ",".join(["?"] * len(fuels))
    sql = f"""
        WITH labeled AS (
            SELECT settlementdate,
                   region,
                   CASE WHEN fuel_type IN ('CCGT','OCGT','Gas other') THEN 'Gas'
                        ELSE fuel_type END AS fuel,
                   total_generation_mw AS gen_mw
            FROM generation_by_fuel_30min
            WHERE region IN ({placeholders})
              AND settlementdate >= ?
              AND fuel_type IN ({fuel_placeholders})
        ),
        per_period AS (
            SELECT settlementdate, fuel, SUM(gen_mw) AS mw
            FROM labeled
            GROUP BY 1, 2
        )
        SELECT date_trunc('day', settlementdate)::DATE AS day,
               fuel,
               AVG(mw) AS mw_daily
        FROM per_period
        GROUP BY 1, 2
        ORDER BY 1, 2
    """
    params = list(regions) + [start] + list(fuels)
    conn = get_connection()
    try:
        rows = conn.execute(sql, params).fetchall()
    finally:
        conn.close()
    return pd.DataFrame(rows, columns=["date", "fuel", "mw_daily"])


def _load_daily_rooftop(regions: list[str], start: datetime) -> pd.DataFrame:
    """Daily mean rooftop solar (MW) for the given regions.  Returns date, mw_daily."""
    placeholders = ",".join(["?"] * len(regions))
    sql = f"""
        WITH per_period AS (
            SELECT settlementdate, SUM(GREATEST(power, 0)) AS mw
            FROM rooftop30
            WHERE regionid IN ({placeholders})
              AND settlementdate >= ?
            GROUP BY 1
        )
        SELECT date_trunc('day', settlementdate)::DATE AS day,
               AVG(mw) AS mw_daily
        FROM per_period
        GROUP BY 1
        ORDER BY 1
    """
    params = list(regions) + [start]
    conn = get_connection()
    try:
        rows = conn.execute(sql, params).fetchall()
    finally:
        conn.close()
    df = pd.DataFrame(rows, columns=["date", "mw_daily"])
    df["fuel"] = "Rooftop"
    return df[["date", "fuel", "mw_daily"]]


def _now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


# ----------------------------------------------------------------------
# /v1/trends/vre-production
# ----------------------------------------------------------------------

VALID_VREPROD_FUELS = ("VRE", "Solar", "Wind", "Rooftop")


@router.get("/trends/vre-production")
async def vre_production(
    region: str = Query("NEM", min_length=2, max_length=8),
    fuel: str = Query("VRE"),
) -> dict:
    """Current + 2 prior years of VRE production overlaid by day-of-year.

    Each line = one calendar year.  Y = TWh annualised, smoothed via
    30-day rolling mean on the daily mean MW series.
    """
    if fuel not in VALID_VREPROD_FUELS:
        raise HTTPException(
            status_code=422,
            detail={
                "code": "INVALID_FUEL",
                "message": f"fuel must be one of {VALID_VREPROD_FUELS}",
            },
        )
    regions = _validate_region(region)

    today = date.today()
    years = [today.year - 2, today.year - 1, today.year]
    # Load 1 buffer year so the rolling 30d MA at start of `years[0]` is warm.
    start = datetime(years[0] - 1, 1, 1)

    if fuel == "Rooftop":
        df = _load_daily_rooftop(regions, start)
    elif fuel == "VRE":
        util = _load_daily_utility(regions, {"Wind", "Solar"}, start)
        roof = _load_daily_rooftop(regions, start)
        df = pd.concat([util, roof], ignore_index=True)
    else:
        df = _load_daily_utility(regions, {fuel}, start)

    data: list[dict] = []
    if not df.empty:
        # Sum across fuels per date (for VRE, this is Wind+Solar+Rooftop daily).
        daily = df.groupby("date")["mw_daily"].sum().sort_index()
        # 30-day rolling MA, min_periods half-window so endpoints aren't NaN.
        smoothed = daily.rolling(window=30, min_periods=15).mean()
        twh = _annualise_twh(smoothed)
        for d, v in twh.items():
            if pd.isna(v):
                continue
            y = d.year if hasattr(d, "year") else pd.Timestamp(d).year
            if y not in years:
                continue
            doy = d.timetuple().tm_yday if hasattr(d, "timetuple") else pd.Timestamp(d).dayofyear
            data.append({"year": int(y), "dayofyear": int(doy), "twh": round(float(v), 4)})

    return {
        "data": data,
        "meta": {
            "region": region,
            "fuel": fuel,
            "years": years,
            "smoothing": "30d MA",
            "as_of": _now_utc_iso(),
        },
    }


# ----------------------------------------------------------------------
# /v1/trends/vre-by-fuel
# ----------------------------------------------------------------------


@router.get("/trends/vre-by-fuel")
async def vre_by_fuel(
    region: str = Query("NEM", min_length=2, max_length=8),
) -> dict:
    """VRE production by fuel (Rooftop/Solar/Wind), 2018-now, 30-day MA."""
    regions = _validate_region(region)
    start = datetime(START_YEAR, 1, 1)

    util = _load_daily_utility(regions, {"Wind", "Solar"}, start)
    roof = _load_daily_rooftop(regions, start)
    df = pd.concat([util, roof], ignore_index=True)

    data: list[dict] = []
    # Tag naive `from` with UTC so iOS's strict ISO8601 decoder accepts it.
    from_iso = start.replace(tzinfo=timezone.utc).isoformat()
    to_iso = _now_utc_iso()
    if not df.empty:
        for fuel in ("Rooftop", "Solar", "Wind"):
            sub = df[df["fuel"] == fuel].set_index("date").sort_index()
            if sub.empty:
                continue
            smoothed = sub["mw_daily"].rolling(window=30, min_periods=15).mean()
            twh = _annualise_twh(smoothed)
            for d, v in twh.items():
                if pd.isna(v):
                    continue
                data.append({
                    "date": pd.Timestamp(d).date().isoformat(),
                    "fuel": fuel,
                    "twh": round(float(v), 4),
                })

    return {
        "data": data,
        "meta": {
            "region": region,
            "fuels": ["Rooftop", "Solar", "Wind"],
            "smoothing": "30d MA",
            "from": from_iso,
            "to": to_iso,
            "as_of": _now_utc_iso(),
        },
    }


# ----------------------------------------------------------------------
# /v1/trends/thermal-vs-renewables
# ----------------------------------------------------------------------


@router.get("/trends/thermal-vs-renewables")
async def thermal_vs_renewables(
    region: str = Query("NEM", min_length=2, max_length=8),
) -> dict:
    """Thermal (Coal+Gas) vs Renewable (Wind+Solar+Rooftop+Hydro), 2018-now,
    180-day MA."""
    regions = _validate_region(region)
    start = datetime(START_YEAR, 1, 1)

    # Utility: Wind, Solar, Water, Coal, Gas (merged from CCGT/OCGT/Gas other).
    fuel_filter = {"Wind", "Solar", "Water", "Coal", "CCGT", "OCGT", "Gas other"}
    util = _load_daily_utility(regions, fuel_filter, start)
    roof = _load_daily_rooftop(regions, start)
    df = pd.concat([util, roof], ignore_index=True)
    # Tag naive `from` with UTC so iOS's strict ISO8601 decoder accepts it.
    from_iso = start.replace(tzinfo=timezone.utc).isoformat()
    to_iso = _now_utc_iso()

    # Categorise.
    renewable_set = {"Wind", "Solar", "Rooftop", "Water"}
    thermal_set = {"Coal", "Gas"}

    def categorise(f: str) -> Optional[str]:
        if f in renewable_set:
            return "renewable"
        if f in thermal_set:
            return "thermal"
        return None

    data: list[dict] = []
    if not df.empty:
        df["category"] = df["fuel"].map(categorise)
        df = df.dropna(subset=["category"])
        # Sum across fuels within each category per date.
        per_day_cat = df.groupby(["date", "category"])["mw_daily"].sum().reset_index()
        for cat in ("renewable", "thermal"):
            sub = per_day_cat[per_day_cat["category"] == cat].set_index("date").sort_index()
            if sub.empty:
                continue
            smoothed = sub["mw_daily"].rolling(window=180, min_periods=90).mean()
            twh = _annualise_twh(smoothed)
            for d, v in twh.items():
                if pd.isna(v):
                    continue
                data.append({
                    "date": pd.Timestamp(d).date().isoformat(),
                    "category": cat,
                    "twh": round(float(v), 4),
                })

    return {
        "data": data,
        "meta": {
            "region": region,
            "categories": ["renewable", "thermal"],
            "smoothing": "180d MA",
            "from": from_iso,
            "to": to_iso,
            "as_of": _now_utc_iso(),
        },
    }
