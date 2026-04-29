"""GET /v1/generation/mix — stacked-area generation by fuel.

For a single physical region, the response also includes:
  - Transmission Imports (positive, top of stack)
  - Transmission Exports (negative, below zero)
  - Battery Charging (negative, below zero)
Battery Storage above zero is discharge only.

For NEM (all 5 regions), interconnectors net to zero and are omitted.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Optional

from fastapi import APIRouter, HTTPException, Query

from ..db import get_connection, nem_naive_to_utc, utc_to_nem_naive

router = APIRouter()

VALID_REGIONS = {"NSW1", "QLD1", "SA1", "TAS1", "VIC1"}

# Stack order, bottom -> top above zero. Negative bands stack from
# zero downward (Swift Charts handles this automatically).
FUEL_ORDER = [
    "Coal", "Gas", "Water", "Wind", "Solar", "Rooftop",
    "Battery Storage", "Transmission Imports",
    "Battery Charging", "Transmission Exports",
]

# From dashboard's calculate_regional_transmission_flows: per-region map of
# interconnector -> direction ("to_X" = import to X is positive; "from_X" =
# export from X is positive in raw flow, so we negate).
INTERCONNECTOR_MAP = {
    "NSW1": {"NSW1-QLD1": "from", "VIC1-NSW1": "to",   "N-Q-MNSP1": "from"},
    "QLD1": {"NSW1-QLD1": "to",   "N-Q-MNSP1": "to"},
    "VIC1": {"VIC1-NSW1": "from", "V-SA": "from",      "V-S-MNSP1": "from", "T-V-MNSP1": "to"},
    "SA1":  {"V-SA": "to",        "V-S-MNSP1": "to"},
    "TAS1": {"T-V-MNSP1": "from"},
}


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
    hours = seconds / 3600
    if hours <= 24.5:        return ("5min",  "5 minutes")
    if hours <= 24 * 7.5:    return ("30min", "30 minutes")
    if hours <= 24 * 31:     return ("1h",    "1 hour")
    if hours <= 24 * 366:    return ("6h",    "6 hours")
    return ("1d",  "1 day")


@router.get("/generation/mix")
async def generation_mix(
    region: Optional[str] = Query(None, min_length=2, max_length=8),
    regions: Optional[str] = Query(None),
    from_: Optional[datetime] = Query(None, alias="from"),
    to: Optional[datetime] = Query(None),
    resolution: str = Query("auto"),
) -> dict:
    region_list = _resolve_regions(region, regions)
    is_single_region = (len(region_list) == 1)

    now_utc = datetime.now(timezone.utc)
    to_utc = to.astimezone(timezone.utc) if to and to.tzinfo else (
        to.replace(tzinfo=timezone.utc) if to else now_utc
    )
    from_utc = from_.astimezone(timezone.utc) if from_ and from_.tzinfo else (
        from_.replace(tzinfo=timezone.utc) if from_ else (to_utc - timedelta(hours=24))
    )
    span = (to_utc - from_utc).total_seconds()
    if resolution == "auto":
        res_label, ddb_interval = _pick_resolution(span)
    elif resolution in ("5min", "30min", "1h", "6h", "1d"):
        res_label = resolution
        ddb_interval = {"5min": "5 minutes", "30min": "30 minutes",
                        "1h": "1 hour", "6h": "6 hours", "1d": "1 day"}[resolution]
    else:
        raise HTTPException(400, detail={"code": "INVALID_RESOLUTION", "message": resolution})

    from_nem = utc_to_nem_naive(from_utc)
    to_nem = utc_to_nem_naive(to_utc)

    placeholders = ",".join(["?"] * len(region_list))

    # Utility-scale: Coal/Gas (merged)/Water/Wind/Solar/Other; biomass dropped.
    # Battery Storage is split into discharge (positive) and charging (negative)
    # post-aggregation in Python so the stacked area renders correctly.
    util_sql = f"""
        WITH labeled AS (
            SELECT settlementdate,
                   CASE WHEN fuel_type IN ('CCGT','OCGT','Gas other') THEN 'Gas'
                        ELSE fuel_type END AS fuel,
                   total_generation_mw AS gen_mw
            FROM generation_by_fuel_5min
            WHERE region IN ({placeholders})
              AND settlementdate >= ? AND settlementdate <= ?
              AND fuel_type != 'Biomass'
        ),
        per_period AS (
            -- Sum across regions for each (settlementdate, fuel)
            SELECT settlementdate, fuel, SUM(gen_mw) AS mw
            FROM labeled
            GROUP BY 1, 2
        )
        SELECT time_bucket(INTERVAL '{ddb_interval}', settlementdate) AS bucket,
               fuel,
               -- Battery: keep raw avg so positive/negative parts split below
               AVG(mw) AS mw
        FROM per_period
        GROUP BY 1, 2
        ORDER BY 1, 2
    """
    util_params = list(region_list) + [from_nem, to_nem]

    roof_sql = f"""
        WITH per_period AS (
            SELECT settlementdate, SUM(GREATEST(power, 0)) AS mw
            FROM rooftop30
            WHERE regionid IN ({placeholders})
              AND settlementdate >= ? AND settlementdate <= ?
            GROUP BY 1
        )
        SELECT time_bucket(INTERVAL '{ddb_interval}', settlementdate) AS bucket,
               'Rooftop' AS fuel,
               AVG(mw) AS mw
        FROM per_period
        GROUP BY 1
        ORDER BY 1
    """
    roof_params = list(region_list) + [from_nem, to_nem]

    # Transmission only when a single physical region is selected.
    trans_rows: list[tuple] = []
    if is_single_region:
        reg = region_list[0]
        ic_map = INTERCONNECTOR_MAP.get(reg, {})
        if ic_map:
            ic_ids = list(ic_map.keys())
            ic_placeholders = ",".join(["?"] * len(ic_ids))
            # CASE WHEN clause to flip sign for 'from' interconnectors
            from_set = {k for k, v in ic_map.items() if v == "from"}
            from_list = list(from_set)
            from_placeholders = ",".join(["?"] * len(from_list)) if from_list else "NULL"
            sign_clause = (
                f"CASE WHEN interconnectorid IN ({from_placeholders}) THEN -meteredmwflow ELSE meteredmwflow END"
                if from_list else "meteredmwflow"
            )
            trans_sql = f"""
                WITH per_period AS (
                    SELECT settlementdate,
                           SUM({sign_clause}) AS net_mw
                    FROM transmission5
                    WHERE interconnectorid IN ({ic_placeholders})
                      AND settlementdate >= ? AND settlementdate <= ?
                    GROUP BY 1
                )
                SELECT time_bucket(INTERVAL '{ddb_interval}', settlementdate) AS bucket,
                       AVG(net_mw) AS mw
                FROM per_period
                GROUP BY 1
                ORDER BY 1
            """
            trans_params = from_list + ic_ids + [from_nem, to_nem]

    conn = get_connection()
    try:
        util = conn.execute(util_sql, util_params).fetchall()
        roof = conn.execute(roof_sql, roof_params).fetchall()
        if is_single_region and ic_map:
            try:
                trans_rows = conn.execute(trans_sql, trans_params).fetchall()
            except Exception:
                # transmission5 absent (e.g., test fixture) — silently omit
                trans_rows = []
    finally:
        conn.close()

    seen: set[str] = set()
    data: list[dict] = []

    # Battery: always emit BOTH discharge and charging rows at every battery
    # timestamp (one will be 0). Otherwise sparse data causes Swift Charts'''
    # AreaMark to draw connecting diagonals across gaps.
    battery_seen_any = any(fuel == "Battery Storage" for _, fuel, _ in util)
    for ts, fuel, mw in util:
        if mw is None:
            continue
        m = float(mw)
        if fuel == "Battery Storage":
            data.append({"timestamp": _utc_iso(ts), "fuel": "Battery Storage",  "mw": round(max(m, 0.0), 1)})
            data.append({"timestamp": _utc_iso(ts), "fuel": "Battery Charging", "mw": round(min(m, 0.0), 1)})
            seen.add("Battery Storage")
            seen.add("Battery Charging")
        else:
            if abs(m) > 0.01:
                data.append({"timestamp": _utc_iso(ts), "fuel": fuel, "mw": round(max(m, 0.0), 1)})
                seen.add(fuel)

    for ts, fuel, mw in roof:
        if mw is None:
            continue
        m = float(mw)
        if m > 0.01:
            data.append({"timestamp": _utc_iso(ts), "fuel": fuel, "mw": round(m, 1)})
            seen.add(fuel)

    # Transmission: emit BOTH imports and exports rows every timestamp.
    for ts, mw in trans_rows:
        if mw is None:
            continue
        m = float(mw)
        data.append({"timestamp": _utc_iso(ts), "fuel": "Transmission Imports", "mw": round(max(m, 0.0), 1)})
        data.append({"timestamp": _utc_iso(ts), "fuel": "Transmission Exports", "mw": round(min(m, 0.0), 1)})
        seen.add("Transmission Imports")
        seen.add("Transmission Exports")

    fuel_rank = {f: i for i, f in enumerate(FUEL_ORDER)}
    data.sort(key=lambda p: (p["timestamp"], fuel_rank.get(p["fuel"], 99)))

    fuels_displayed = [f for f in FUEL_ORDER if f in seen]
    fuels_displayed += sorted(seen - set(FUEL_ORDER))

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
