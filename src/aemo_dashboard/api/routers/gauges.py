"""GET /v1/gauges/today — single-fetch payload for the Today tab 3-gauge carousel.

Definitions match the existing Panel dashboard's nem_dash_tab.py:
  - demand     = SUM(demand30.demand) + SUM(rooftop30.power) across 5 mainland regions.
                 alltime / hour records computed over all history of demand30+rooftop30.
                 forecast_peak_mw = MAX over latest predispatch run of summed demand_forecast.
  - renewable  = (Hydro + Wind + Solar + Rooftop) / total at latest generation_by_fuel_5min,
                 excluding Battery Storage and Transmission. Biomass is omitted from
                 the displayed breakdown to match the stacked-gauge convention.
                 Rooftop is sourced from rooftop30 at the latest 30-min bucket on
                 or before the generation timestamp.
  - battery    = SUM(bdu_energy_storage) at latest bdu5 settlement, mainland only
                 (TAS is NaN). 1h-ago = closest period >= 55 min earlier.
                 capacity = 30-day rolling max of the same sum.
"""
from __future__ import annotations

from datetime import datetime, timezone

from fastapi import APIRouter

from ..db import get_connection, nem_naive_to_utc

router = APIRouter()

REGIONS_5 = ("NSW1", "QLD1", "VIC1", "SA1", "TAS1")
MAINLAND = ("NSW1", "QLD1", "VIC1", "SA1")
RENEWABLE_FUELS = ("Wind", "Solar", "Water")  # Rooftop added separately from rooftop30
EXCLUDED_FROM_TOTAL = ("Battery Storage", "Transmission")


def _utc_iso(dt: datetime | None) -> str | None:
    if dt is None:
        return None
    return nem_naive_to_utc(dt).isoformat().replace("+00:00", "Z")


def _load_demand(conn) -> dict:
    regs = "('" + "','".join(REGIONS_5) + "')"

    # Latest demand + rooftop, with hour
    row = conn.execute(f"""
        WITH latest AS (
            SELECT MAX(settlementdate) AS ts FROM demand30 WHERE regionid IN {regs}
        ),
        dem AS (
            SELECT SUM(demand) AS op_demand
            FROM demand30, latest
            WHERE settlementdate = latest.ts AND regionid IN {regs}
        ),
        roof AS (
            SELECT COALESCE(SUM(power), 0) AS rooftop
            FROM rooftop30, latest
            WHERE settlementdate = latest.ts AND regionid IN {regs}
        )
        SELECT dem.op_demand + roof.rooftop, EXTRACT(HOUR FROM latest.ts), latest.ts
        FROM dem, roof, latest
    """).fetchone()
    current_mw = float(row[0]) if row and row[0] is not None else 0.0
    current_hour = int(row[1]) if row and row[1] is not None else 0
    latest_ts = row[2] if row else None

    hour_rec = conn.execute(f"""
        SELECT MAX(period_total) FROM (
            SELECT d.settlementdate,
                   SUM(d.demand) + COALESCE(SUM(r.power), 0) AS period_total
            FROM demand30 d
            LEFT JOIN rooftop30 r
              ON d.settlementdate = r.settlementdate AND d.regionid = r.regionid
            WHERE d.regionid IN {regs}
              AND EXTRACT(HOUR FROM d.settlementdate) = ?
            GROUP BY d.settlementdate
        )
    """, [current_hour]).fetchone()
    hour_record_mw = float(hour_rec[0]) if hour_rec and hour_rec[0] is not None else 0.0

    rng = conn.execute(f"""
        SELECT MAX(period_total), MIN(period_total) FROM (
            SELECT d.settlementdate,
                   SUM(d.demand) + COALESCE(SUM(r.power), 0) AS period_total
            FROM demand30 d
            LEFT JOIN rooftop30 r
              ON d.settlementdate = r.settlementdate AND d.regionid = r.regionid
            WHERE d.regionid IN {regs}
            GROUP BY d.settlementdate
            HAVING SUM(d.demand) > 0
        )
    """).fetchone()
    alltime_record = float(rng[0]) if rng and rng[0] is not None else current_mw
    alltime_min = float(rng[1]) if rng and rng[1] is not None else 0.0

    forecast_peak_mw = None
    try:
        fp = conn.execute(f"""
            WITH latest_run AS (
                SELECT MAX(run_time) AS rt FROM predispatch
            ),
            per_ts AS (
                SELECT settlementdate, SUM(demand_forecast) AS demand_total
                FROM predispatch, latest_run
                WHERE run_time = latest_run.rt AND regionid IN {regs}
                GROUP BY settlementdate
            )
            SELECT MAX(demand_total) FROM per_ts
        """).fetchone()
        if fp and fp[0] is not None:
            forecast_peak_mw = float(fp[0])
    except Exception:
        # predispatch table may be absent in some environments
        forecast_peak_mw = None

    return {
        "current_mw": current_mw,
        "alltime_record_mw": alltime_record,
        "alltime_min_mw": alltime_min,
        "hour_record_mw": hour_record_mw,
        "current_hour": current_hour,
        "forecast_peak_mw": forecast_peak_mw,
        "as_of": _utc_iso(latest_ts),
    }


def _load_renewable(conn) -> dict:
    regs = "('" + "','".join(REGIONS_5) + "')"

    rows = conn.execute(f"""
        WITH latest AS (
            SELECT MAX(settlementdate) AS ts FROM generation_by_fuel_5min
            WHERE region IN {regs}
        )
        SELECT fuel_type, SUM(total_generation_mw)
        FROM generation_by_fuel_5min, latest
        WHERE settlementdate = latest.ts AND region IN {regs}
        GROUP BY fuel_type
    """).fetchall()

    fuel_mw = {f: float(mw or 0.0) for f, mw in rows}

    latest_ts_row = conn.execute("SELECT MAX(settlementdate) FROM generation_by_fuel_5min").fetchone()
    latest_ts = latest_ts_row[0] if latest_ts_row else None

    # Pull rooftop at the most-recent 30-min bucket on/before latest_ts.
    rooftop_mw = 0.0
    if latest_ts is not None:
        roof = conn.execute(f"""
            WITH latest_roof AS (
                SELECT MAX(settlementdate) AS ts FROM rooftop30
                WHERE settlementdate <= ? AND regionid IN {regs}
            )
            SELECT COALESCE(SUM(power), 0) FROM rooftop30, latest_roof
            WHERE settlementdate = latest_roof.ts AND regionid IN {regs}
        """, [latest_ts]).fetchone()
        rooftop_mw = float(roof[0]) if roof and roof[0] is not None else 0.0

    total_gen = sum(mw for f, mw in fuel_mw.items() if f not in EXCLUDED_FROM_TOTAL and mw > 0)
    total_gen += rooftop_mw

    def pct(x: float) -> float:
        return (x / total_gen * 100.0) if total_gen > 0 else 0.0

    hydro_pct = pct(max(fuel_mw.get("Water", 0.0), 0.0))
    wind_pct = pct(max(fuel_mw.get("Wind", 0.0), 0.0))
    solar_pct = pct(max(fuel_mw.get("Solar", 0.0), 0.0))
    rooftop_pct = pct(rooftop_mw)
    renewable_pct = hydro_pct + wind_pct + solar_pct + rooftop_pct

    return {
        "renewable_pct": round(renewable_pct, 4),
        "hydro_pct": round(hydro_pct, 4),
        "wind_pct": round(wind_pct, 4),
        "solar_pct": round(solar_pct, 4),
        "rooftop_pct": round(rooftop_pct, 4),
        "as_of": _utc_iso(latest_ts),
    }


def _load_battery(conn) -> dict:
    regs = "('" + "','".join(MAINLAND) + "')"

    latest = conn.execute(f"""
        SELECT settlementdate, SUM(bdu_energy_storage)
        FROM bdu5
        WHERE settlementdate = (SELECT MAX(settlementdate) FROM bdu5)
          AND regionid IN {regs}
        GROUP BY settlementdate
    """).fetchone()
    stored_mwh = float(latest[1]) if latest and latest[1] is not None else 0.0
    latest_ts = latest[0] if latest else None

    one_hr = conn.execute(f"""
        SELECT settlementdate, SUM(bdu_energy_storage)
        FROM bdu5
        WHERE regionid IN {regs}
          AND settlementdate <= (
              SELECT MAX(settlementdate) - INTERVAL '55 minutes' FROM bdu5
          )
        GROUP BY settlementdate
        ORDER BY settlementdate DESC
        LIMIT 1
    """).fetchone()
    stored_1h_ago = float(one_hr[1]) if one_hr and one_hr[1] is not None else 0.0

    cap_row = conn.execute(f"""
        SELECT MAX(total) FROM (
            SELECT SUM(bdu_energy_storage) AS total
            FROM bdu5
            WHERE regionid IN {regs}
              AND settlementdate >= CAST(NOW() - INTERVAL '30 days' AS TIMESTAMP)
            GROUP BY settlementdate
        )
    """).fetchone()
    capacity_mwh = float(cap_row[0]) if cap_row and cap_row[0] is not None else max(stored_mwh, 1.0)

    return {
        "stored_mwh": stored_mwh,
        "stored_1h_ago_mwh": stored_1h_ago,
        "capacity_mwh": capacity_mwh,
        "as_of": _utc_iso(latest_ts),
    }


@router.get("/gauges/today")
async def gauges_today() -> dict:
    conn = get_connection()
    try:
        demand = _load_demand(conn)
        renewable = _load_renewable(conn)
        battery = _load_battery(conn)
    finally:
        conn.close()

    return {
        "data": {
            "demand": demand,
            "renewable_share": renewable,
            "battery_soc": battery,
        },
        "meta": {"as_of": datetime.now(timezone.utc).isoformat()},
    }
