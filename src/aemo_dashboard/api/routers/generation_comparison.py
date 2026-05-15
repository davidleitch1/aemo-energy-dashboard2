"""GET /v1/generation/comparison — period-on-period fuel + price comparison.

Mirrors AEMO_spot/analysis_code/fuel_ytd_annualised_table.py:
  - per-fuel annualised TWh and generation-weighted VWAP
  - NEM / region demand-weighted TWAP (the QED "average wholesale price")
  - Battery price metric is the spread (discharge VWAP − charge VWAP)

The response packages two windows (current + prior, one year apart) plus
delta values per row, so the iOS table view never has to do arithmetic.

Period semantics (current window):
  * 7d / 30d / 90d / 1y  → end = MAX(scada30), start = end − period
  * ytd                  → start = Jan 1 of end's year (interval-ending);
                           the half-hour at 00:30 is the first row.

Prior window: same MM-DD HH:MM, year − 1 (Feb-29 falls back to Feb-28).
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Optional

from fastapi import APIRouter, HTTPException, Query

from ..db import get_connection

router = APIRouter()

VALID_REGIONS = {"NEM", "NSW1", "QLD1", "VIC1", "SA1", "TAS1"}
NEM_REGIONS = ("NSW1", "QLD1", "SA1", "TAS1", "VIC1")
VALID_PERIODS = ("7d", "30d", "90d", "ytd", "1y")

# Pump-load DUIDs report pumping consumption as positive scada — exclude
# from Water totals (matches the PNG generator).
PUMP_LOAD_DUIDS = ("KIDSPHL1", "KIDSPHL2", "SHPUMP", "PUMP1", "PUMP2", "SNOWYP")

# Display order for the table rows. Each entry: (group_key, display_label,
# raw fuel labels from duid_info.Fuel that map into the bucket).
FUEL_GROUPS = [
    ("Coal",    "Coal",            ("Coal",)),
    ("Wind",    "Wind",            ("Wind",)),
    ("Rooftop", "Rooftop solar",   ("ROOFTOP",)),
    ("Solar",   "Utility solar",   ("Solar",)),
    ("Water",   "Hydro",           ("Water",)),
    ("Gas",     "Gas",             ("CCGT", "OCGT", "Gas other")),
    ("Battery", "Battery",         ("Battery Storage",)),
    ("Other",   "Biomass + other", ("Biomass", "Other")),
]
FUEL_TO_GROUP: dict[str, str] = {
    fuel: gkey for gkey, _, fuels in FUEL_GROUPS for fuel in fuels
}
GROUP_DISPLAY = {gkey: gdisp for gkey, gdisp, _ in FUEL_GROUPS}
GROUP_ORDER = [gkey for gkey, _, _ in FUEL_GROUPS]


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _safe_year_minus_one(dt: datetime) -> datetime:
    """Return dt with year−1, falling back from Feb-29 to Feb-28."""
    try:
        return dt.replace(year=dt.year - 1)
    except ValueError:
        return dt.replace(year=dt.year - 1, day=dt.day - 1)


def _resolve_window(period: str, end: datetime) -> tuple[datetime, datetime]:
    """(start, end) for the requested period. start is exclusive in SQL,
    end is inclusive."""
    if period == "ytd":
        # First half-hour of the year is 00:30; subtract 30 min to make
        # `> start` include it.
        start = datetime(end.year, 1, 1) - timedelta(minutes=30)
        return start, end
    days_for = {"7d": 7, "30d": 30, "90d": 90, "1y": 365}
    days = days_for[period]
    return end - timedelta(days=days), end


def _period_days(start: datetime, end: datetime) -> float:
    return (end - start).total_seconds() / 86400.0


def _region_clause(region: str, alias: str = "di") -> tuple[str, list]:
    """Build a region filter for queries that join via duid_info."""
    if region == "NEM":
        placeholders = ", ".join(["?"] * len(NEM_REGIONS))
        return f'AND {alias}."Region" IN ({placeholders})', list(NEM_REGIONS)
    return f'AND {alias}."Region" = ?', [region]


def _rooftop_region_clause(region: str) -> tuple[str, list]:
    if region == "NEM":
        placeholders = ", ".join(["?"] * len(NEM_REGIONS))
        return f"AND r.regionid IN ({placeholders})", list(NEM_REGIONS)
    return "AND r.regionid = ?", [region]


def _demand_region_clause(region: str, alias: str = "d") -> tuple[str, list]:
    if region == "NEM":
        placeholders = ", ".join(["?"] * len(NEM_REGIONS))
        return f"AND {alias}.regionid IN ({placeholders})", list(NEM_REGIONS)
    return f"AND {alias}.regionid = ?", [region]


def _fetch_fuel_window(conn, start: datetime, end: datetime, region: str) -> dict:
    """Per-fuel MWh + revenue + charge MWh + charge cost. Keyed by raw fuel
    label from duid_info; rooftop appended as 'ROOFTOP'."""
    pump_placeholders = ", ".join(["?"] * len(PUMP_LOAD_DUIDS))
    region_sql, region_params = _region_clause(region, alias="di")

    sql = f"""
        WITH disp AS (
            SELECT s.settlementdate,
                   di."Region" AS regionid,
                   di."Fuel"   AS fuel,
                   CASE WHEN s.scadavalue > 0 THEN  s.scadavalue ELSE 0 END * 0.5 AS dis_mwh,
                   CASE WHEN s.scadavalue < 0 THEN -s.scadavalue ELSE 0 END * 0.5 AS chg_mwh
            FROM scada30 s
            JOIN duid_info di ON s.duid = di."DUID"
            WHERE s.settlementdate >  ?
              AND s.settlementdate <= ?
              AND s.duid NOT IN ({pump_placeholders})
              {region_sql}
        )
        SELECT d.fuel,
               SUM(d.dis_mwh)               AS mwh,
               SUM(d.dis_mwh * p.rrp)       AS revenue,
               SUM(d.chg_mwh)               AS chg_mwh,
               SUM(d.chg_mwh * p.rrp)       AS chg_cost
        FROM disp d
        JOIN prices30 p
          ON p.settlementdate = d.settlementdate
         AND p.regionid       = d.regionid
        GROUP BY d.fuel
    """
    params: list = [start, end, *PUMP_LOAD_DUIDS, *region_params]
    rows = conn.execute(sql, params).fetchall()

    out: dict[str, dict] = {}
    for fuel, mwh, revenue, chg_mwh, chg_cost in rows:
        out[fuel] = {
            "mwh":      float(mwh) if mwh is not None else 0.0,
            "revenue":  float(revenue) if revenue is not None else 0.0,
            "chg_mwh":  float(chg_mwh) if chg_mwh is not None else 0.0,
            "chg_cost": float(chg_cost) if chg_cost is not None else 0.0,
        }

    # Rooftop is a separate table (rooftop30, column `power`).
    rt_sql_filter, rt_params = _rooftop_region_clause(region)
    rt_sql = f"""
        SELECT SUM(r.power) * 0.5                       AS mwh,
               SUM(r.power * 0.5 * p.rrp)               AS revenue
        FROM rooftop30 r
        JOIN prices30 p
          ON p.settlementdate = r.settlementdate
         AND p.regionid       = r.regionid
        WHERE r.settlementdate >  ?
          AND r.settlementdate <= ?
          {rt_sql_filter}
    """
    rt_row = conn.execute(rt_sql, [start, end, *rt_params]).fetchone()
    rt_mwh = float(rt_row[0]) if rt_row and rt_row[0] is not None else 0.0
    rt_rev = float(rt_row[1]) if rt_row and rt_row[1] is not None else 0.0
    if rt_mwh > 0:
        out["ROOFTOP"] = {
            "mwh":      rt_mwh,
            "revenue":  rt_rev,
            "chg_mwh":  0.0,
            "chg_cost": 0.0,
        }
    return out


def _fetch_twap(conn, start: datetime, end: datetime, region: str) -> Optional[float]:
    """Demand-weighted time-weighted average pool price for the period.

    Per-interval: weight regional RRP by regional demand. Then simple AVG
    across intervals. Independent of which fuels dispatched."""
    region_sql, region_params = _demand_region_clause(region, alias="d")
    sql = f"""
        WITH per_int AS (
            SELECT d.settlementdate,
                   SUM(d.demand * p.rrp) / NULLIF(SUM(d.demand), 0) AS dw_price
            FROM demand30 d
            JOIN prices30 p
              ON p.settlementdate = d.settlementdate
             AND p.regionid       = d.regionid
            WHERE d.settlementdate >  ?
              AND d.settlementdate <= ?
              {region_sql}
            GROUP BY d.settlementdate
        )
        SELECT AVG(dw_price) FROM per_int
    """
    val = conn.execute(sql, [start, end, *region_params]).fetchone()[0]
    return float(val) if val is not None else None


def _aggregate_groups(fuel_data: dict, days: float) -> dict[str, dict]:
    """Roll raw-fuel data up into display groups; compute TWh annualised + VWAP.

    For Battery, vwap is the spread (discharge VWAP − charge VWAP); is_spread
    flag set."""
    groups: dict[str, dict] = {
        gkey: {"mwh": 0.0, "revenue": 0.0, "chg_mwh": 0.0, "chg_cost": 0.0}
        for gkey in GROUP_ORDER
    }
    for fuel, vals in fuel_data.items():
        gkey = FUEL_TO_GROUP.get(fuel)
        if gkey is None:
            continue  # unknown fuel — silently drop
        g = groups[gkey]
        g["mwh"]      += vals["mwh"]
        g["revenue"]  += vals["revenue"]
        g["chg_mwh"]  += vals["chg_mwh"]
        g["chg_cost"] += vals["chg_cost"]

    out: dict[str, dict] = {}
    for gkey in GROUP_ORDER:
        g = groups[gkey]
        twh = (g["mwh"] / days * 365.0 / 1_000_000.0) if days > 0 else 0.0
        vwap_dis = (g["revenue"] / g["mwh"]) if g["mwh"] > 0 else None
        vwap_chg = (g["chg_cost"] / g["chg_mwh"]) if g["chg_mwh"] > 0 else None

        is_spread = (gkey == "Battery")
        if is_spread and vwap_dis is not None and vwap_chg is not None:
            vwap = vwap_dis - vwap_chg
        else:
            vwap = vwap_dis  # discharge VWAP for everything else (or None)

        out[gkey] = {
            "twh":       twh,
            "vwap":      vwap,
            "is_spread": is_spread,
            "_mwh":      g["mwh"],  # used to detect "no data"
        }
    return out


def _round_or_none(v: Optional[float], ndigits: int) -> Optional[float]:
    return None if v is None else round(v, ndigits)


def _delta(a: Optional[float], b: Optional[float]) -> Optional[float]:
    if a is None or b is None:
        return None
    return a - b


@router.get("/generation/comparison")
def generation_comparison(
    region: str = Query("NEM"),
    period: str = Query("ytd"),
) -> dict:
    """Period-on-period fuel + price comparison, current vs same period 1y ago."""
    if region not in VALID_REGIONS:
        raise HTTPException(
            status_code=400,
            detail={"code": "INVALID_REGION", "message": f"Unknown region: {region}"},
        )
    if period not in VALID_PERIODS:
        raise HTTPException(
            status_code=400,
            detail={"code": "INVALID_PERIOD",
                    "message": f"period must be one of {list(VALID_PERIODS)}"},
        )

    conn = get_connection()
    try:
        end_curr = conn.execute("SELECT MAX(settlementdate) FROM scada30").fetchone()[0]
        if end_curr is None:
            return _empty_response(region, period)

        start_curr, end_curr = _resolve_window(period, end_curr)
        end_prev   = _safe_year_minus_one(end_curr)
        start_prev = _safe_year_minus_one(start_curr)

        days_curr = _period_days(start_curr, end_curr)
        days_prev = _period_days(start_prev, end_prev)

        cur_raw = _fetch_fuel_window(conn, start_curr, end_curr, region)
        prv_raw = _fetch_fuel_window(conn, start_prev, end_prev, region)

        twap_curr = _fetch_twap(conn, start_curr, end_curr, region)
        twap_prev = _fetch_twap(conn, start_prev, end_prev, region)
    finally:
        conn.close()

    cur_groups = _aggregate_groups(cur_raw, days_curr)
    prv_groups = _aggregate_groups(prv_raw, days_prev)

    rows: list[dict] = []
    for gkey in GROUP_ORDER:
        c = cur_groups[gkey]
        p = prv_groups[gkey]
        # Hide rows where neither window has any volume (e.g. Other in a
        # region where biomass is absent).
        if c["_mwh"] == 0 and p["_mwh"] == 0:
            continue
        rows.append({
            "fuel":         GROUP_DISPLAY[gkey],
            "group":        gkey,
            "is_spread":    c["is_spread"],
            "current_twh":  _round_or_none(c["twh"], 1),
            "prior_twh":    _round_or_none(p["twh"], 1),
            "delta_twh":    _round_or_none(_delta(c["twh"], p["twh"]), 1),
            "current_vwap": _round_or_none(c["vwap"], 0),
            "prior_vwap":   _round_or_none(p["vwap"], 0),
            "delta_vwap":   _round_or_none(_delta(c["vwap"], p["vwap"]), 0),
        })

    # Total row: sum of TWh across groups (excluding battery for the
    # "generation total" view? The PNG sums everything. We match.). Price
    # column on the total row is the demand-weighted TWAP, not gen-weighted.
    total_twh_cur = sum((g["twh"] or 0.0) for g in cur_groups.values())
    total_twh_prv = sum((g["twh"] or 0.0) for g in prv_groups.values())
    total_label = "NEM total" if region == "NEM" else f"{region} total"
    rows.append({
        "fuel":         total_label,
        "group":        "TOTAL",
        "is_spread":    False,
        "current_twh":  round(total_twh_cur, 1),
        "prior_twh":    round(total_twh_prv, 1),
        "delta_twh":    round(total_twh_cur - total_twh_prv, 1),
        "current_vwap": _round_or_none(twap_curr, 0),
        "prior_vwap":   _round_or_none(twap_prev, 0),
        "delta_vwap":   _round_or_none(_delta(twap_curr, twap_prev), 0),
    })

    return {
        "data": {"rows": rows},
        "meta": {
            "region": region,
            "period": period,
            "current_window": {
                "start": start_curr.isoformat(),
                "end":   end_curr.isoformat(),
                "days":  round(days_curr, 2),
            },
            "prior_window": {
                "start": start_prev.isoformat(),
                "end":   end_prev.isoformat(),
                "days":  round(days_prev, 2),
            },
            "fuel_order": [GROUP_DISPLAY[g] for g in GROUP_ORDER] + [total_label],
            "as_of": _now_iso(),
        },
    }


def _empty_response(region: str, period: str) -> dict:
    total_label = "NEM total" if region == "NEM" else f"{region} total"
    return {
        "data": {"rows": []},
        "meta": {
            "region": region,
            "period": period,
            "current_window": None,
            "prior_window":   None,
            "fuel_order": [GROUP_DISPLAY[g] for g in GROUP_ORDER] + [total_label],
            "as_of": _now_iso(),
        },
    }
