"""GET /v1/generation/mix — stacked-area generation by fuel."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Optional

from fastapi import APIRouter, HTTPException, Query

from ..db import get_connection, nem_naive_to_utc, utc_to_nem_naive

router = APIRouter()

VALID_REGIONS = {"NSW1", "QLD1", "SA1", "TAS1", "VIC1"}

# Display order, bottom -> top in the stack: dispatchable first, variable above.
FUEL_ORDER = ["Coal", "Gas", "Water", "Wind", "Solar", "Rooftop", "Battery Storage", "Other"]


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


def _pick_resolution(seconds: float) -> tuple[str, str]:
    """Return (label, duckdb interval) for the requested span."""
    hours = seconds / 3600
    if hours <= 24.5:
        return ("5min", "5 minutes")
    if hours <= 24 * 7.5:
        return ("30min", "30 minutes")
    if hours <= 24 * 31:
        return ("1h", "1 hour")
    if hours <= 24 * 366:
        return ("6h", "6 hours")
    return ("1d", "1 day")


@router.get("/generation/mix")
async def generation_mix(
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
    span_seconds = (to_utc - from_utc).total_seconds()
    if resolution == "auto":
        res_label, ddb_interval = _pick_resolution(span_seconds)
    elif resolution in ("5min", "30min", "1h", "6h", "1d"):
        # Honour the explicit ask
        res_label = resolution
        ddb_interval = {"5min": "5 minutes", "30min": "30 minutes",
                        "1h": "1 hour", "6h": "6 hours", "1d": "1 day"}[resolution]
    else:
        raise HTTPException(400, detail={"code": "INVALID_RESOLUTION", "message": resolution})

    from_nem = utc_to_nem_naive(from_utc)
    to_nem = utc_to_nem_naive(to_utc)

    placeholders = ",".join(["?"] * len(region_list))

    # Utility-scale fuels with gas merged and biomass dropped, then time-bucketed.
    # SUM across regions and fuels-mapped-to-same-bucket per period.
    util_sql = f"""
        WITH labeled AS (
            SELECT settlementdate,
                   CASE WHEN fuel_type IN ('CCGT','OCGT','Gas other') THEN 'Gas'
                        ELSE fuel_type END AS fuel,
                   GREATEST(total_generation_mw, 0) AS gen_mw
            FROM generation_by_fuel_5min
            WHERE region IN ({placeholders})
              AND settlementdate >= ? AND settlementdate <= ?
              AND fuel_type != 'Biomass'
        )
        SELECT time_bucket(INTERVAL '{ddb_interval}', settlementdate) AS bucket,
               fuel,
               AVG(per_period_mw) AS mw
        FROM (
            SELECT settlementdate, fuel, SUM(gen_mw) AS per_period_mw
            FROM labeled
            GROUP BY 1, 2
        )
        GROUP BY 1, 2
        ORDER BY 1, 2
    """
    util_params = list(region_list) + [from_nem, to_nem]

    # Rooftop (30-min source). Bucket the same way; rooftop has no fuel-type
    # distinction, just rolled-up MW per region per period.
    roof_sql = f"""
        WITH labeled AS (
            SELECT settlementdate, GREATEST(power, 0) AS gen_mw
            FROM rooftop30
            WHERE regionid IN ({placeholders})
              AND settlementdate >= ? AND settlementdate <= ?
        )
        SELECT time_bucket(INTERVAL '{ddb_interval}', settlementdate) AS bucket,
               'Rooftop' AS fuel,
               AVG(per_period_mw) AS mw
        FROM (
            SELECT settlementdate, SUM(gen_mw) AS per_period_mw
            FROM labeled
            GROUP BY 1
        )
        GROUP BY 1
        ORDER BY 1
    """
    roof_params = list(region_list) + [from_nem, to_nem]

    conn = get_connection()
    try:
        util_rows = conn.execute(util_sql, util_params).fetchall()
        roof_rows = conn.execute(roof_sql, roof_params).fetchall()
    finally:
        conn.close()

    seen_fuels: set[str] = set()
    data: list[dict] = []
    for ts, fuel, mw in util_rows + roof_rows:
        if mw is None or float(mw) <= 0.01:
            continue
        seen_fuels.add(fuel)
        data.append({
            "timestamp": _utc_iso(ts),
            "fuel": fuel,
            "mw": round(float(mw), 1),
        })

    # Sort: timestamp asc, then fuel in display order.
    fuel_rank = {f: i for i, f in enumerate(FUEL_ORDER)}
    data.sort(key=lambda p: (p["timestamp"], fuel_rank.get(p["fuel"], 99)))

    fuels_displayed = [f for f in FUEL_ORDER if f in seen_fuels]
    # Anything outside FUEL_ORDER falls to the end alphabetically.
    fuels_displayed += sorted(seen_fuels - set(FUEL_ORDER))

    return {
        "data": data,
        "meta": {
            "regions": region_list,
            "resolution": res_label,
            "fuels": fuels_displayed,
            "from": from_utc.isoformat().replace("+00:00", "Z"),
            "to": to_utc.isoformat().replace("+00:00", "Z"),
            "as_of": datetime.now(timezone.utc).isoformat(),
        },
    }
