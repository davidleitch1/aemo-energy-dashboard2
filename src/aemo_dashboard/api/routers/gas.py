"""GET /v1/gas/* — STTM gas prices and demand.

Sources `sttm_expost` from DuckDB (10+ years of daily SYD/BRI/ADL ex-post
prices and network allocation volumes).

  /prices    daily ex-post for one hub (or AVG = mean across SYD+BRI+ADL),
             last 3 calendar years overlaid by day-of-year, plus
             latest/mean/peak summary stats
  /demand    7-day MA TJ/day, last 3 calendar years overlaid by day-of-year,
             single-hub or ALL (sum across hubs)
"""
from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from typing import Optional

import pandas as pd
from fastapi import APIRouter, HTTPException, Query

from ..db import get_connection

router = APIRouter()

VALID_PRICE_HUBS = ("SYD", "BRI", "ADL", "AVG")
VALID_DEMAND_HUBS = ("ALL", "SYD", "BRI", "ADL")
HUB_LABEL = {"SYD": "Sydney", "BRI": "Brisbane", "ADL": "Adelaide", "AVG": "Avg"}


def _now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _date_iso(d) -> str:
    """Always return YYYY-MM-DD, regardless of whether d is a date,
    datetime, or pandas Timestamp."""
    return pd.Timestamp(d).date().isoformat()


def _safe_query(sql: str, params: list) -> list[tuple]:
    """Run a query; if sttm_expost is absent (test fixture), return [] gracefully."""
    conn = get_connection()
    try:
        return conn.execute(sql, params).fetchall()
    except Exception:
        return []
    finally:
        conn.close()


# ----------------------------------------------------------------------
# /v1/gas/prices
# ----------------------------------------------------------------------


@router.get("/gas/prices")
async def gas_prices(
    hub: str = Query("AVG"),
) -> dict:
    """Daily ex-post gas price for the selected hub, 3 calendar years
    overlaid by day-of-year (mirrors the demand chart)."""
    if hub not in VALID_PRICE_HUBS:
        raise HTTPException(
            status_code=422,
            detail={"code": "INVALID_HUB",
                    "message": f"hub must be one of {VALID_PRICE_HUBS}"},
        )

    if hub == "AVG":
        # Average across SYD/BRI/ADL per day, only days where all 3 are present.
        sql = """
            SELECT gas_date, AVG(expost_price) AS price
            FROM sttm_expost
            WHERE hub IN ('SYD','BRI','ADL')
              AND expost_price IS NOT NULL
            GROUP BY 1
            HAVING COUNT(DISTINCT hub) = 3
            ORDER BY 1
        """
        params: list = []
    else:
        sql = """
            SELECT gas_date, expost_price AS price
            FROM sttm_expost
            WHERE hub = ?
              AND expost_price IS NOT NULL
            ORDER BY gas_date
        """
        params = [hub]

    rows = _safe_query(sql, params)

    data: list[dict] = []
    years: list[int] = []
    latest_price = None
    latest_date = None
    mean_price = None
    peak_price = None
    peak_date = None

    if rows:
        df = pd.DataFrame(rows, columns=["date", "price"])
        df["date"] = pd.to_datetime(df["date"])
        df = df.sort_values("date")
        df["year"] = df["date"].dt.year
        df["dayofyear"] = df["date"].dt.dayofyear

        all_years = sorted(df["year"].unique())
        years = [int(y) for y in all_years[-3:]]

        sub = df[df["year"].isin(years)]
        for _, row in sub.iterrows():
            data.append({
                "year": int(row["year"]),
                "dayofyear": int(row["dayofyear"]),
                "price": round(float(row["price"]), 4),
            })

        # Stats over the displayed 3-year window.
        prices = sub["price"].tolist()
        if prices:
            latest_row = sub.iloc[-1]
            peak_row = sub.loc[sub["price"].idxmax()]
            latest_price = round(float(latest_row["price"]), 2)
            latest_date = _date_iso(latest_row["date"])
            mean_price = round(sum(prices) / len(prices), 2)
            peak_price = round(float(peak_row["price"]), 2)
            peak_date = _date_iso(peak_row["date"])

    return {
        "data": data,
        "meta": {
            "hub": hub,
            "hub_label": HUB_LABEL.get(hub, hub),
            "years": years,
            "latest": latest_price,
            "latest_date": latest_date,
            "mean": mean_price,
            "peak": peak_price,
            "peak_date": peak_date,
            "as_of": _now_utc_iso(),
        },
    }


# ----------------------------------------------------------------------
# /v1/gas/demand
# ----------------------------------------------------------------------


@router.get("/gas/demand")
async def gas_demand(
    hub: str = Query("ALL"),
) -> dict:
    """7-day MA TJ/day, last 3 calendar years overlaid by day-of-year.

    hub=ALL sums across SYD+BRI+ADL.
    """
    if hub not in VALID_DEMAND_HUBS:
        raise HTTPException(
            status_code=422,
            detail={"code": "INVALID_HUB",
                    "message": f"hub must be one of {VALID_DEMAND_HUBS}"},
        )

    if hub == "ALL":
        sql = """
            SELECT gas_date, SUM(network_allocation) / 1000.0 AS tj
            FROM sttm_expost
            WHERE network_allocation IS NOT NULL
              AND hub IN ('SYD','BRI','ADL')
            GROUP BY 1
            ORDER BY 1
        """
        params: list = []
    else:
        sql = """
            SELECT gas_date, network_allocation / 1000.0 AS tj
            FROM sttm_expost
            WHERE network_allocation IS NOT NULL
              AND hub = ?
            ORDER BY gas_date
        """
        params = [hub]

    rows = _safe_query(sql, params)

    data: list[dict] = []
    years: list[int] = []
    if rows:
        df = pd.DataFrame(rows, columns=["date", "tj"])
        df["date"] = pd.to_datetime(df["date"])
        df = df.sort_values("date")
        df["ma7"] = df["tj"].rolling(7, min_periods=1).mean()
        df["year"] = df["date"].dt.year
        df["dayofyear"] = df["date"].dt.dayofyear

        all_years = sorted(df["year"].unique())
        years = [int(y) for y in all_years[-3:]]  # last 3 calendar years

        sub = df[df["year"].isin(years)]
        for _, row in sub.iterrows():
            data.append({
                "year": int(row["year"]),
                "dayofyear": int(row["dayofyear"]),
                "tj": round(float(row["ma7"]), 2),
            })

    return {
        "data": data,
        "meta": {
            "hub": hub,
            "hub_label": "All hubs" if hub == "ALL" else HUB_LABEL.get(hub, hub),
            "years": years,
            "smoothing": "7d MA",
            "as_of": _now_utc_iso(),
        },
    }
