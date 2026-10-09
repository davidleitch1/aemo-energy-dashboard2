"""PASA outage logic for the PASA tab and the Today outage tile.

Pure pandas: no FastAPI imports. Definitions:

- Scope: scheduled plant only (Coal, CCGT, OCGT, Gas other, Water).
- MW out at an interval = max(0, capacity_mw - GENERATION_PASA_AVAILABILITY).
  PASA availability is physical (includes recallable plant); MAX availability
  is 0 for units offline for economic reasons, so it is not used.
- A unit is out when MW out >= threshold.
- "Now" is the first ST-PASA interval at or after the current NEM time, else
  the latest interval in the store.
- Outage type and the longer return date come from MT-PASA unit state.
"""
from __future__ import annotations

import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd

DATA_DIR = Path("/Users/davidleitch/aemo_production/data")
STPASA_PATH = DATA_DIR / "outages_stpasa.parquet"
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
