"""GET /v1/pasa/generator-outages — current generator outages by fuel.

Replicates the Today-tab outages panel from
nem_dash_tab.py / pasa_tab.create_generator_fuel_summary.

  - Reads ST-PASA parquet (preferred) or MT-PASA fallback at $AEMO_DATA_PATH.
  - Per-DUID near-term (next 48h) min PASA availability vs max max-availability.
  - Reduction = max - current; only DUIDs with reduction >= 50 MW kept.
  - Special case: if PASA reports 0/0 we fall back to duid_info Capacity(MW).
  - Joined to duid_info (DuckDB) for Fuel + Region; aggregated by fuel.
"""
from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pandas as pd
from fastapi import APIRouter

from ..db import get_connection

router = APIRouter()

DEFAULT_DATA_PATH = Path(os.environ.get("AEMO_DATA_PATH", "/Users/davidleitch/aemo_production/data"))
MIN_REDUCTION_MW = 50.0
HORIZON_HOURS = 48
REGION_TRIM = {"NSW1": "NSW", "QLD1": "QLD", "VIC1": "VIC", "SA1": "SA", "TAS1": "TAS"}


def _trim_region(region: str | None) -> str:
    if not region:
        return "Unknown"
    return REGION_TRIM.get(region, region.rstrip("1") if region.endswith("1") else region)


def _load_outages_df() -> tuple[pd.DataFrame, datetime | None]:
    """Return (DataFrame[DUID, max_mw, current_mw, reduction_mw], data_as_of).

    Empty DataFrame when neither parquet exists.
    """
    base = Path(os.environ.get("AEMO_DATA_PATH", str(DEFAULT_DATA_PATH)))
    stpasa = base / "outages_stpasa.parquet"
    mtpasa = base / "outages_mtpasa.parquet"

    empty = pd.DataFrame(columns=["DUID", "max_mw", "current_mw", "reduction_mw"])

    if stpasa.exists():
        df = pd.read_parquet(stpasa)
        if not df.empty and "GENERATION_PASA_AVAILABILITY" in df.columns:
            now = pd.Timestamp.now()
            horizon = now + pd.Timedelta(hours=HORIZON_HOURS)
            near = df[(df["INTERVAL_DATETIME"] >= now) & (df["INTERVAL_DATETIME"] <= horizon)]
            if near.empty:
                latest = df["INTERVAL_DATETIME"].max()
                near = df[df["INTERVAL_DATETIME"] >= latest - pd.Timedelta(hours=6)]
            if not near.empty:
                summary = near.groupby("DUID").agg(
                    current_mw=("GENERATION_PASA_AVAILABILITY", "min"),
                    max_mw=("GENERATION_MAX_AVAILABILITY", "max"),
                ).reset_index()
                summary["reduction_mw"] = summary["max_mw"] - summary["current_mw"]
                outages = summary[summary["reduction_mw"] >= MIN_REDUCTION_MW].copy()
                data_as_of = df["RUN_DATETIME"].max() if "RUN_DATETIME" in df else None
                return (outages.sort_values("reduction_mw", ascending=False).reset_index(drop=True),
                        data_as_of.to_pydatetime() if data_as_of is not None else None)

    if mtpasa.exists():
        df = pd.read_parquet(mtpasa)
        if not df.empty and "PASAAVAILABILITY" in df.columns:
            now = pd.Timestamp.now()
            horizon = now + pd.Timedelta(days=7)
            latest_pub = df["PUBLISH_DATETIME"].max()
            near = df[
                (df["PUBLISH_DATETIME"] == latest_pub)
                & (df["DAY"] >= now)
                & (df["DAY"] <= horizon)
            ]
            if not near.empty:
                summary = near.groupby("DUID").agg(
                    current_mw=("PASAAVAILABILITY", "min"),
                    max_mw=("AGGREGATECAPACITY", "max") if "AGGREGATECAPACITY" in df.columns
                    else ("PASAAVAILABILITY", "max"),
                ).reset_index()
                summary["reduction_mw"] = summary["max_mw"] - summary["current_mw"]
                outages = summary[summary["reduction_mw"] >= MIN_REDUCTION_MW].copy()
                return (outages.sort_values("reduction_mw", ascending=False).reset_index(drop=True),
                        latest_pub.to_pydatetime() if hasattr(latest_pub, "to_pydatetime") else None)

    return empty, None


def _load_duid_info(conn) -> dict[str, dict[str, Any]]:
    rows = conn.execute(
        'SELECT DUID, Fuel, Region, "Capacity(MW)" FROM duid_info'
    ).fetchall()
    return {r[0]: {"Fuel": r[1], "Region": r[2], "Capacity(MW)": r[3]} for r in rows}


@router.get("/pasa/generator-outages")
async def generator_outages() -> dict:
    outages, data_as_of = _load_outages_df()
    conn = get_connection()
    try:
        duid_info = _load_duid_info(conn)
    finally:
        conn.close()

    fuel_rows: dict[str, dict[str, Any]] = {}

    for _, r in outages.iterrows():
        duid = r["DUID"]
        info = duid_info.get(duid, {})
        fuel = info.get("Fuel") or "Unknown"
        region = _trim_region(info.get("Region"))

        reduction = float(r["reduction_mw"])
        if reduction <= 0 and float(r["current_mw"]) == 0:
            cap = info.get("Capacity(MW)") or 0
            reduction = float(cap)
        if reduction <= 0:
            continue

        bucket = fuel_rows.setdefault(fuel, {"fuel": fuel, "total_mw": 0.0, "units": []})
        bucket["total_mw"] += reduction
        bucket["units"].append({"duid": duid, "mw": reduction, "region": region})

    fuels = sorted(fuel_rows.values(), key=lambda x: -x["total_mw"])
    for f in fuels:
        f["units"].sort(key=lambda u: -u["mw"])
        f["total_mw"] = round(f["total_mw"], 1)
        for u in f["units"]:
            u["mw"] = round(u["mw"], 1)

    max_fuel_mw = fuels[0]["total_mw"] if fuels else 0.0
    total_mw = round(sum(f["total_mw"] for f in fuels), 1)
    as_of = data_as_of.replace(tzinfo=timezone.utc).isoformat().replace("+00:00", "Z") if data_as_of else None

    return {
        "data": {
            "fuels": fuels,
            "max_fuel_mw": max_fuel_mw,
            "total_mw": total_mw,
            "as_of": as_of,
        },
        "meta": {"as_of": datetime.now(timezone.utc).isoformat()},
    }
