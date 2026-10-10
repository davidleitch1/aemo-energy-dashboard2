"""PASA outage logic for the PASA tab and the Today outage tile.

Pure pandas: no FastAPI imports. Definitions:

- Scope: scheduled plant only (Coal, CCGT, OCGT, Gas other, Water).
- MW out at an interval = max(0, capacity_mw - GENERATION_PASA_AVAILABILITY).
  PASA availability is physical (includes recallable plant); MAX availability
  is 0 for units offline for economic reasons, so it is not used.
- A unit is out when MW out >= threshold.
- "Now" is the first interval at or after the current NEM time, else the
  latest interval in the store. Intervals come from PD-PASA (every 30 minutes,
  run time to the end of the next trading day) spliced with ST-PASA for the
  intervals after PD-PASA ends; ST-PASA alone starts at the next trading day.
- Outage type and the longer return date come from MT-PASA unit state.
"""
from __future__ import annotations

import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import numpy as np
import pandas as pd

DATA_DIR = Path("/Users/davidleitch/aemo_production/data")
STPASA_PATH = DATA_DIR / "outages_stpasa.parquet"
PDPASA_PATH = DATA_DIR / "outages_pdpasa.parquet"
MTPASA_PATH = DATA_DIR / "outages_mtpasa.parquet"

NEM_TZ = timezone(timedelta(hours=10))
SCHEDULED_FUELS = ("Coal", "CCGT", "OCGT", "Gas other", "Water")
FUEL_DISPLAY = {"Coal": "Coal", "CCGT": "Gas", "OCGT": "Gas",
                "Gas other": "Gas", "Water": "Hydro"}
LONG_TERM_RECALL_MIN = 24000
DEFAULT_THRESHOLD_MW = 50.0

_CACHE_TTL = 300.0
_cache: dict[str, tuple[float, pd.DataFrame]] = {}


def _cached(key: str, loader) -> pd.DataFrame:
    now = time.time()
    hit = _cache.get(key)
    if hit and now - hit[0] < _CACHE_TTL:
        return hit[1]
    df = loader()
    _cache[key] = (now, df)
    return df


def nem_now() -> pd.Timestamp:
    return pd.Timestamp(datetime.now(NEM_TZ).replace(tzinfo=None))


# ---------------------------------------------------------------------------
# Loading
# ---------------------------------------------------------------------------

def _latest_stpasa(df: pd.DataFrame) -> pd.DataFrame:
    return (df.sort_values("RUN_DATETIME")
              .drop_duplicates(["DUID", "INTERVAL_DATETIME"], keep="last"))


def load_stpasa(path: Path | str | None = None) -> pd.DataFrame:
    """Latest run per (DUID, interval). Cached for 5 minutes."""
    p = Path(path) if path else STPASA_PATH
    cols = ["RUN_DATETIME", "DUID", "INTERVAL_DATETIME",
            "GENERATION_MAX_AVAILABILITY", "GENERATION_PASA_AVAILABILITY",
            "GENERATION_RECALL_PERIOD"]
    return _cached(f"st:{p}", lambda: _latest_stpasa(
        pd.read_parquet(p, columns=cols)))


def load_pdpasa(path: Path | str | None = None) -> pd.DataFrame:
    """Latest PD-PASA run (the collector keeps one run). Cached for 5 minutes."""
    p = Path(path) if path else PDPASA_PATH
    cols = ["RUN_DATETIME", "DUID", "INTERVAL_DATETIME",
            "GENERATION_MAX_AVAILABILITY", "GENERATION_PASA_AVAILABILITY",
            "GENERATION_RECALL_PERIOD"]
    return _cached(f"pd:{p}", lambda: _latest_run(
        pd.read_parquet(p, columns=cols)))


def _latest_run(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    return df[df["RUN_DATETIME"] == df["RUN_DATETIME"].max()]


def combine_pasa(pd_df: pd.DataFrame, st_df: pd.DataFrame) -> pd.DataFrame:
    """PD-PASA rows of its latest run, plus ST-PASA rows (latest run per DUID
    and interval) for intervals after PD-PASA's last. Adds SOURCE ('PD'/'ST').
    ST-PASA rows inside PD-PASA's range are dropped, so downstream dedup keeps
    one row per (DUID, interval) and PD values win."""
    st = _latest_stpasa(st_df) if len(st_df) else st_df.copy()
    pdl = _latest_run(pd_df) if len(pd_df) else pd_df
    if pdl.empty:
        return st.assign(SOURCE="ST")
    pd_end = pdl["INTERVAL_DATETIME"].max()
    if st.empty:
        return pdl.assign(SOURCE="PD").reset_index(drop=True)
    st = st[st["INTERVAL_DATETIME"] > pd_end]
    return pd.concat([pdl.assign(SOURCE="PD"), st.assign(SOURCE="ST")],
                     ignore_index=True)


def load_mtpasa(path: Path | str | None = None) -> pd.DataFrame:
    """Newest publish per (DUID, DAY). Cached for 5 minutes."""
    p = Path(path) if path else MTPASA_PATH
    cols = ["PUBLISH_DATETIME", "DAY", "DUID", "PASAAVAILABILITY",
            "PASAUNITSTATE"]
    return _cached(f"mt:{p}", lambda: _latest_mtpasa(
        pd.read_parquet(p, columns=cols)))


def _latest_mtpasa(df: pd.DataFrame) -> pd.DataFrame:
    return (df.sort_values("PUBLISH_DATETIME")
              .drop_duplicates(["DUID", "DAY"], keep="last"))


def load_scheduled_units(conn) -> pd.DataFrame:
    """Scheduled plant from duid_mapping (raw fuel labels kept)."""
    fuels = ", ".join(f"'{f}'" for f in SCHEDULED_FUELS)
    return conn.execute(
        f'SELECT duid, "site name" AS site_name, region, fuel, capacity_mw '
        f'FROM duid_mapping WHERE fuel IN ({fuels})'
    ).df()


# ---------------------------------------------------------------------------
# Outage calculations
# ---------------------------------------------------------------------------

def _joined(stpasa: pd.DataFrame, units: pd.DataFrame) -> pd.DataFrame:
    u = units[units["fuel"].isin(SCHEDULED_FUELS)].copy()
    u["capacity_mw"] = u["capacity_mw"].astype(float)
    st = _latest_stpasa(stpasa)
    df = st.merge(u, left_on="DUID", right_on="duid", how="inner")
    df["available_mw"] = df["GENERATION_PASA_AVAILABILITY"].astype(float)
    df["mw_out"] = (df["capacity_mw"] - df["available_mw"]).clip(lower=0.0)
    df["fuel"] = df["fuel"].map(FUEL_DISPLAY)
    return df


def _now_interval(stpasa: pd.DataFrame, now: pd.Timestamp | None):
    now = now if now is not None else nem_now()
    iv = stpasa["INTERVAL_DATETIME"]
    future = iv[iv >= now]
    return future.min() if len(future) else iv.max()


def current_outages(stpasa: pd.DataFrame, units: pd.DataFrame,
                    threshold: float = DEFAULT_THRESHOLD_MW,
                    now: pd.Timestamp | None = None) -> pd.DataFrame:
    """Units out at the 'now' interval, sorted by MW out descending."""
    cols = ["duid", "site_name", "region", "fuel", "capacity_mw",
            "available_mw", "mw_out", "recall_h", "interval"]
    if stpasa.empty:
        return pd.DataFrame(columns=cols)
    t0 = _now_interval(stpasa, now)
    df = _joined(stpasa, units)
    df = df[(df["INTERVAL_DATETIME"] == t0) & (df["mw_out"] >= threshold)].copy()
    df["recall_h"] = df["GENERATION_RECALL_PERIOD"] / 60.0
    df["interval"] = df["INTERVAL_DATETIME"]
    return (df[cols].sort_values("mw_out", ascending=False)
              .reset_index(drop=True))


def outage_timeseries(stpasa: pd.DataFrame, units: pd.DataFrame,
                      threshold: float = DEFAULT_THRESHOLD_MW,
                      region: str | None = None,
                      now: pd.Timestamp | None = None) -> pd.DataFrame:
    """MW out per interval (index) by display fuel (columns), from 'now' on.
    Only units at or above the threshold in that interval count."""
    if stpasa.empty:
        return pd.DataFrame()
    t0 = _now_interval(stpasa, now)
    df = _joined(stpasa, units)
    if region and region != "NEM":
        df = df[df["region"] == region]
    df = df[df["INTERVAL_DATETIME"] >= t0]
    all_iv = pd.Index(sorted(df["INTERVAL_DATETIME"].unique()),
                      name="INTERVAL_DATETIME")
    df = df[df["mw_out"] >= threshold]
    ts = (df.pivot_table(index="INTERVAL_DATETIME", columns="fuel",
                         values="mw_out", aggfunc="sum")
            .reindex(all_iv).fillna(0.0))
    return ts


def outage_type(state) -> str:
    s = state if isinstance(state, str) else ""
    if s.startswith(("OUTAGEPLAN", "DERATINGPLAN")):
        return "Planned"
    if s.startswith(("OUTAGEUNPLAN", "DERATINGUNPLAN")):
        return "Unplanned"
    if s in ("", "NODERATINGS"):
        return "Not in MT-PASA"
    return s.title()   # e.g. MOTHBALLED, RETIRED


def _mtpasa_lookup(mtpasa: pd.DataFrame | None, today: pd.Timestamp) -> dict:
    """duid -> (raw state on the first DAY >= today, first DAY >= today with
    NODERATINGS). A publish starts about two days after its publish date, so
    today usually has no row; the first available day stands in for it."""
    if mtpasa is None or mtpasa.empty:
        return {}
    mt = _latest_mtpasa(mtpasa)
    mt = mt[mt["DAY"] >= today]
    out = {}
    for duid, g in mt.groupby("DUID"):
        g = g.sort_values("DAY")
        state = g["PASAUNITSTATE"].iloc[0]
        state = state if isinstance(state, str) else ""
        clear = g[g["PASAUNITSTATE"] == "NODERATINGS"]
        ret = clear["DAY"].min() if len(clear) else pd.NaT
        out[duid] = (state, ret)
    return out


def outage_table(stpasa: pd.DataFrame, units: pd.DataFrame,
                 mtpasa: pd.DataFrame | None,
                 threshold: float = DEFAULT_THRESHOLD_MW,
                 now: pd.Timestamp | None = None) -> pd.DataFrame:
    """Units out now with type, in-week return and MT-PASA return."""
    cur = current_outages(stpasa, units, threshold, now)
    cur["raw_state"] = ""
    cur["type"] = "Not in MT-PASA"
    cur["mtpasa_return"] = pd.NaT
    cur["back_within_week"] = pd.NaT
    if cur.empty:
        return cur

    # Expected return: first interval after now where the unit is below threshold.
    df = _joined(stpasa, units)
    df = df[df["DUID"].isin(cur["duid"])]
    t0 = cur["interval"].iloc[0]
    later = df[(df["INTERVAL_DATETIME"] > t0) & (df["mw_out"] < threshold)]
    back = later.groupby("DUID")["INTERVAL_DATETIME"].min()
    cur["back_within_week"] = pd.to_datetime(cur["duid"].map(back))

    today = (now if now is not None else nem_now()).normalize()
    lookup = _mtpasa_lookup(mtpasa, today)
    states = cur["duid"].map(lambda d: lookup.get(d, ("", pd.NaT))[0])
    cur["raw_state"] = states
    cur["type"] = states.map(outage_type)
    has_outage = cur["type"] != "Not in MT-PASA"
    rets = cur["duid"].map(lambda d: lookup.get(d, ("", pd.NaT))[1])
    cur["mtpasa_return"] = pd.to_datetime(rets).where(has_outage, pd.NaT)
    return cur


# ---------------------------------------------------------------------------
# MT-PASA outage days, episodes and revision history
# ---------------------------------------------------------------------------

HISTORY_PATH = DATA_DIR / "outages_mtpasa_history.parquet"
OUTAGE_STATE_PREFIXES = ("OUTAGE", "DERATING")
NOT_COUNTED_STATES = ("MOTHBALLED", "RETIRED")
_KEY = ["DUID", "DAY"]


def load_mtpasa_history(path: Path | str | None = None) -> pd.DataFrame:
    """Change-compressed revision history; see mtpasa_asof()."""
    p = Path(path) if path else HISTORY_PATH
    return _cached(f"mth:{p}", lambda: pd.read_parquet(p))


def mtpasa_asof(history: pd.DataFrame, publish_ts) -> pd.DataFrame:
    """Full view as at publish_ts: latest row per (DUID, DAY) published at or
    before publish_ts."""
    h = history[history["PUBLISH_DATETIME"] <= pd.Timestamp(publish_ts)]
    if h.empty:
        return h.copy()
    h = h.sort_values("PUBLISH_DATETIME", kind="stable")
    return h.drop_duplicates(_KEY, keep="last").reset_index(drop=True)


def outage_days(view: pd.DataFrame, units: pd.DataFrame,
                threshold: float = DEFAULT_THRESHOLD_MW) -> pd.DataFrame:
    """Scheduled-unit days in an OUTAGE*/DERATING* state with MW out >=
    threshold. Columns: duid, day, mw_out, state, type."""
    u = units[units["fuel"].isin(SCHEDULED_FUELS)][["duid", "capacity_mw"]]
    df = view.merge(u, left_on="DUID", right_on="duid", how="inner")
    state = df["PASAUNITSTATE"].fillna("")
    df = df[state.str.startswith(OUTAGE_STATE_PREFIXES)].copy()
    df["mw_out"] = (df["capacity_mw"].astype(float)
                    - df["PASAAVAILABILITY"].astype(float)).clip(lower=0.0)
    df = df[df["mw_out"] >= threshold]
    df["state"] = df["PASAUNITSTATE"]
    df["type"] = df["state"].map(outage_type)
    return (df.rename(columns={"DAY": "day"})
              [["duid", "day", "mw_out", "state", "type"]]
              .reset_index(drop=True))


def episodes(days: pd.DataFrame, max_gap_days: int = 1) -> pd.DataFrame:
    """Maximal runs of outage days per unit; a gap of up to max_gap_days
    missing days is merged. mw = max MW out; type/state from the first day;
    return_date = day after the last outage day."""
    cols = ["duid", "start", "end", "return_date", "mw", "n_days", "type",
            "state"]
    if days.empty:
        return pd.DataFrame(columns=cols)
    d = days.sort_values(["duid", "day"]).reset_index(drop=True)
    gap = d.groupby("duid")["day"].diff().dt.days
    d["ep"] = ((gap.isna()) | (gap > max_gap_days + 1)).cumsum()
    g = d.groupby("ep")
    out = pd.DataFrame({
        "duid": g["duid"].first(), "start": g["day"].min(),
        "end": g["day"].max(), "mw": g["mw_out"].max(),
        "type": g["type"].first(), "state": g["state"].first(),
    }).reset_index(drop=True)
    out["return_date"] = out["end"] + pd.Timedelta(days=1)
    out["n_days"] = (out["end"] - out["start"]).dt.days + 1
    return out[cols]


def mothballed(view: pd.DataFrame, units: pd.DataFrame,
               today: pd.Timestamp | None = None,
               threshold: float = DEFAULT_THRESHOLD_MW) -> pd.DataFrame:
    """Scheduled units whose MT-PASA state on their first day >= today is
    MOTHBALLED or RETIRED, with the MW they would otherwise show as out."""
    cols = ["duid", "site_name", "region", "fuel", "capacity_mw", "state", "mw"]
    today = (today if today is not None else nem_now()).normalize()
    v = view[view["DAY"] >= today].sort_values("DAY").drop_duplicates("DUID")
    v = v[v["PASAUNITSTATE"].isin(NOT_COUNTED_STATES)]
    u = units[units["fuel"].isin(SCHEDULED_FUELS)]
    df = v.merge(u, left_on="DUID", right_on="duid", how="inner")
    df["mw"] = (df["capacity_mw"].astype(float)
                - df["PASAAVAILABILITY"].astype(float)).clip(lower=0.0)
    df["state"] = df["PASAUNITSTATE"]
    df["fuel"] = df["fuel"].map(FUEL_DISPLAY)
    df = df[df["mw"] >= threshold]
    return df[cols].sort_values("mw", ascending=False).reset_index(drop=True)


def mothballed_note(moth: pd.DataFrame) -> str:
    if moth.empty:
        return ""
    items = ", ".join(f"{r.duid} {r.mw:,.0f} MW" for r in moth.itertuples())
    return f"Mothballed or retired, not counted: {items}"


def weekly_publishes(history: pd.DataFrame, n_weeks: int = 26) -> list:
    """Last publish of each ISO week for the last n_weeks weeks, plus the
    latest publish (which is the last of its own week)."""
    pubs = pd.Series(sorted(history["PUBLISH_DATETIME"].unique()))
    pubs = pd.to_datetime(pubs)
    iso = pubs.dt.isocalendar()
    key = iso["year"].astype(int) * 100 + iso["week"].astype(int)
    last = pubs.groupby(key.values).max().sort_index()
    return [pd.Timestamp(t) for t in last.iloc[-n_weeks:]]


def _asof_sorted(hist_sorted: pd.DataFrame, ts) -> pd.DataFrame:
    h = hist_sorted[hist_sorted["PUBLISH_DATETIME"] <= ts]
    return h.drop_duplicates(_KEY, keep="last")


def _pub_before(pubs: list, latest, days: int):
    cands = [p for p in pubs if p <= latest - pd.Timedelta(days=days)]
    return cands[-1] if cands else None


def _match(eps_by_duid: dict, duid, start, end):
    """Episode of `duid` with the largest date overlap with [start, end];
    eps_by_duid maps duid -> that unit's episodes."""
    e = eps_by_duid.get(duid)
    if e is None:
        return None
    e = e[(e["start"] <= end) & (e["end"] >= start)]
    if e.empty:
        return None
    ov = ((e["end"].clip(upper=end) - e["start"].clip(lower=start))
          .dt.days)
    return e.loc[ov.idxmax()]


def slippage_table(history: pd.DataFrame, units: pd.DataFrame, pubs: list,
                   window_days: int = 30,
                   threshold: float = DEFAULT_THRESHOLD_MW):
    """Return-date changes for episodes out now or starting within
    window_days of the latest publish's first day.

    Returns (table, paths). Table columns: duid, start, end, mw, type, state,
    return_now, ret_1w, ret_4w, new_1w, new_4w, first_listed, orig_return,
    slip_days (NaN when open-ended), open_ended, newly_open (open-ended now,
    not 4 weeks ago), pre_open_return (return before it turned open-ended),
    moved_later, is_new, changed, first_is_bound (first listed = earliest
    publish compared, so slip is a lower bound), start_bound (outage begins
    at or before the start of the history), horizon (last MT-PASA day of the
    latest view), slip_chart (slip with return capped at the horizon).
    Sorted: moved later, newly open-ended, other changes, long-standing
    open-ended last. paths: duid, start, publish, return_date.
    """
    hist = history.sort_values("PUBLISH_DATETIME", kind="stable")
    hist_min_day = history["DAY"].min()
    eps_by_pub, horizon_open, horizon_max = {}, {}, {}
    for p in pubs:
        v = _asof_sorted(hist, p)
        horizon_max[p] = v["DAY"].max()
        horizon_open[p] = horizon_max[p] - pd.Timedelta(days=7)  # last week counts as open-ended
        eps = episodes(outage_days(v, units, threshold))
        eps_by_pub[p] = {d: g for d, g in eps.groupby("duid")}
    latest = pubs[-1]
    first_day = latest.normalize()
    cur = pd.concat(eps_by_pub[latest].values()) if eps_by_pub[latest] \
        else pd.DataFrame(columns=episodes(pd.DataFrame()).columns)
    cur = cur[(cur["end"] >= first_day)
              & (cur["start"] <= first_day + pd.Timedelta(days=window_days))]
    p1w, p4w = _pub_before(pubs, latest, 7), _pub_before(pubs, latest, 28)
    hz = horizon_max[latest]

    rows, path_rows = [], []
    for e in cur.itertuples():
        matches = {}
        for p in pubs:
            m = _match(eps_by_pub[p], e.duid, e.start, e.end)
            if m is not None:
                matches[p] = (m["return_date"], bool(m["end"] >= horizon_open[p]))
        for p, (r, _) in matches.items():
            path_rows.append({"duid": e.duid, "start": e.start,
                              "publish": p, "return_date": r})
        first = min(matches)             # always includes latest
        orig, orig_open = matches[first]
        open_now = matches[latest][1]
        r1w = matches[p1w][0] if p1w in matches else pd.NaT
        r4w = matches[p4w][0] if p4w in matches else pd.NaT
        open_1w = p1w in matches and matches[p1w][1]
        open_4w = p4w in matches and matches[p4w][1]
        newly_open = bool(open_now and p4w is not None and not open_4w)
        pre_open = pd.NaT
        if newly_open:
            closed = [p for p in matches if not matches[p][1]]
            if closed:
                pre_open = matches[max(closed)][0]
        moved_later = bool(pd.notna(r1w) and e.return_date > r1w
                           and not (open_now and open_1w))
        slip = (np.nan if open_now or orig_open
                else float((e.return_date - orig).days))
        is_new = first == latest
        capped = min(e.return_date, hz)
        rows.append({
            "duid": e.duid, "start": e.start, "end": e.end, "mw": e.mw,
            "type": e.type, "state": e.state, "return_now": e.return_date,
            "ret_1w": r1w, "ret_4w": r4w,
            "new_1w": p1w is not None and pd.isna(r1w),
            "new_4w": p4w is not None and pd.isna(r4w),
            "first_listed": first.normalize(), "orig_return": orig,
            "open_ended": bool(open_now), "newly_open": newly_open,
            "pre_open_return": pre_open, "slip_days": slip,
            "moved_later": moved_later, "is_new": is_new,
            "changed": bool(moved_later or newly_open or is_new
                            or (pd.notna(slip) and slip != 0)),
            "first_is_bound": bool(first == pubs[0] and not is_new),
            "start_bound": bool(e.start <= hist_min_day),
            "horizon": hz,
            "slip_chart": float((capped - orig).days),
        })
    table = pd.DataFrame(rows)
    if not table.empty:
        long_open = table["open_ended"] & ~table["newly_open"]
        group = np.select(
            [table["moved_later"], table["newly_open"], long_open],
            [0, 1, 3], default=2)
        table = (table.assign(_g=group, _n=~table["newly_open"])
                 .sort_values(["_g", "_n", "slip_days", "mw"],
                              ascending=[True, True, False, False],
                              na_position="last")
                 .drop(columns=["_g", "_n"]).reset_index(drop=True))
    return table, pd.DataFrame(path_rows)


def unit_rows(eps: pd.DataFrame, min_mw: float = 0.0) -> pd.DataFrame:
    """One row per unit from an episode frame: first start, max episode MW.
    Units whose largest episode is below min_mw are dropped; sorted by first
    start."""
    if eps.empty:
        return pd.DataFrame(columns=["duid", "site_name", "start", "mw"])
    g = eps.groupby("duid").agg(site_name=("site_name", "first"),
                                start=("start", "min"), mw=("mw", "max"))
    g = g[g["mw"] >= min_mw].reset_index()
    return g.sort_values(["start", "duid"]).reset_index(drop=True)


def with_units(df: pd.DataFrame, units: pd.DataFrame) -> pd.DataFrame:
    """Add site_name, region and display fuel to a frame keyed by `duid`."""
    info = units[["duid", "site_name", "region", "fuel"]].copy()
    info["fuel"] = info["fuel"].map(FUEL_DISPLAY)
    return df.merge(info, on="duid", how="left")


def extended_episodes(view: pd.DataFrame, units: pd.DataFrame,
                      today: pd.Timestamp, min_days: int = 7,
                      horizon_days: int = 365,
                      threshold: float = DEFAULT_THRESHOLD_MW) -> pd.DataFrame:
    """Episodes of >= min_days that overlap [today, today + horizon_days]."""
    eps = episodes(outage_days(view, units, threshold))
    end = today + pd.Timedelta(days=horizon_days)
    eps = eps[(eps["n_days"] >= min_days) & (eps["end"] >= today)
              & (eps["start"] <= end)]
    return (with_units(eps, units).sort_values(["start", "duid"])
            .reset_index(drop=True))


def weekly_mw_out(days: pd.DataFrame, units: pd.DataFrame,
                  today: pd.Timestamp, weeks: int = 52,
                  region: str | None = None) -> pd.DataFrame:
    """Mean daily MW out per week (index = week start) by display fuel,
    counting every outage day, not only long episodes."""
    d = with_units(days, units)
    if region and region != "NEM":
        d = d[d["region"] == region]
    idx = pd.date_range(today.normalize(), periods=weeks * 7, freq="D")
    daily = (d[d["day"].isin(idx)]
             .pivot_table(index="day", columns="fuel", values="mw_out",
                          aggfunc="sum")
             .reindex(idx).fillna(0.0))
    if daily.empty or daily.shape[1] == 0:
        daily = pd.DataFrame(index=idx)
    wk = np.arange(len(idx)) // 7
    out = daily.groupby(wk).mean()
    out.index = idx[::7][:len(out)]
    return out


# ---------------------------------------------------------------------------
# Supply impact: scheduled outages against demand and price
# ---------------------------------------------------------------------------

REGIONS = ("NSW1", "QLD1", "VIC1", "SA1", "TAS1")
DISPLAY_FUELS = ("Coal", "Gas", "Hydro")


def supply_impact(cur: pd.DataFrame, units: pd.DataFrame,
                  demand_by_region: dict, price_by_region: dict) -> pd.DataFrame:
    """One row per region plus 'NEM'.

    cur: current_outages() output (display fuels). units: in-scope scheduled
    units (raw fuel labels; coal capacity is fuel == 'Coal'). demand and price
    are {region: value}. NEM demand is the sum over regions; NEM price is the
    demand-weighted mean over regions that have both. coal_pct is NaN where a
    region has no coal capacity."""
    rows = {}
    for r in REGIONS:
        c = cur[cur["region"] == r]
        coal_cap = float(units.loc[(units["region"] == r)
                                   & (units["fuel"] == "Coal"),
                                   "capacity_mw"].astype(float).sum())
        row = {"coal_capacity_mw": coal_cap,
               "coal_out_mw": float(c.loc[c["fuel"] == "Coal", "mw_out"].sum()),
               "sched_out_mw": float(c["mw_out"].sum()),
               "demand_mw": float(demand_by_region.get(r, np.nan)),
               "price": float(price_by_region.get(r, np.nan))}
        for f in DISPLAY_FUELS:
            row[f"out_{f}"] = float(c.loc[c["fuel"] == f, "mw_out"].sum())
        rows[r] = row
    df = pd.DataFrame.from_dict(rows, orient="index")

    both = df[df["demand_mw"].notna() & df["price"].notna()]
    nem = df.drop(columns=["demand_mw", "price"]).sum()
    nem["demand_mw"] = (df["demand_mw"].sum(min_count=1))
    nem["price"] = (float((both["price"] * both["demand_mw"]).sum()
                          / both["demand_mw"].sum())
                    if len(both) and both["demand_mw"].sum() > 0 else np.nan)
    df.loc["NEM"] = nem
    cap = df["coal_capacity_mw"].where(df["coal_capacity_mw"] > 0)
    df["coal_pct"] = df["coal_out_mw"] / cap * 100.0
    dem = df["demand_mw"].where(df["demand_mw"] > 0)
    df["out_pct_demand"] = df["sched_out_mw"] / dem * 100.0
    return df
