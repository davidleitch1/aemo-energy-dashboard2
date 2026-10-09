"""AEMO High Impact Outages (transmission) selection for the PASA tab.

Pure pandas. Ported from the Panel dashboard's pasa/analyzer.py and
pasa_tab.consolidate_outages; the latest weekly report only.
"""
from __future__ import annotations

from pathlib import Path

import pandas as pd

try:
    from .pasa_data import DATA_DIR, _cached, nem_now
except ImportError:    # run as a top-level module by uvicorn
    from pasa_data import DATA_DIR, _cached, nem_now

HIGH_IMPACT_PATH = DATA_DIR / "outages_high_impact.parquet"
_WITHDRAWN = "Withdrawn|Cancel"


def load_high_impact(path: Path | str | None = None) -> pd.DataFrame:
    p = Path(path) if path else HIGH_IMPACT_PATH
    return _cached(f"hi:{p}", lambda: latest_report(pd.read_parquet(p)))


def latest_report(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty or "report_date" not in df.columns:
        return df
    return df[df["report_date"] == df["report_date"].max()].copy()


def report_date(df: pd.DataFrame):
    return df["report_date"].max() if len(df) else pd.NaT


def _flag(df: pd.DataFrame, col: str) -> pd.Series:
    if col not in df.columns:
        return pd.Series(False, index=df.index)
    return df[col].fillna("").astype(str).str.strip().str.upper() == "T"


def _live(df: pd.DataFrame) -> pd.Series:
    return ~df["Status"].fillna("").str.contains(_WITHDRAWN, case=False)


def in_progress(df: pd.DataFrame, now: pd.Timestamp | None = None) -> pd.DataFrame:
    now = now if now is not None else nem_now()
    has_region = df["Region"].notna()
    by_dates = (df["Start"] <= now) & (df["Finish"] >= now)
    by_status = df["Status"].fillna("").str.contains("In Progress|PTP",
                                                     case=False)
    return (df[has_region & (by_dates | by_status)]
            .sort_values("Start", ascending=False))


def unplanned(df: pd.DataFrame, now: pd.Timestamp | None = None) -> pd.DataFrame:
    now = now if now is not None else nem_now()
    m = _flag(df, "Unplanned?") & (df["Finish"] >= now) & df["Region"].notna()
    return df[m].sort_values("Start")


def upcoming(df: pd.DataFrame, now: pd.Timestamp | None = None,
             days: int = 30) -> pd.DataFrame:
    now = now if now is not None else nem_now()
    m = ((df["Start"] >= now) & (df["Start"] <= now + pd.Timedelta(days=days))
         & df["Region"].notna() & _live(df))
    return df[m].sort_values("Start")


def inter_regional(df: pd.DataFrame,
                   now: pd.Timestamp | None = None) -> pd.DataFrame:
    now = now if now is not None else nem_now()
    m = (_flag(df, "Inter-Regional") & (df["Finish"] >= now)
         & df["Region"].notna() & _live(df))
    return df[m].sort_values("Start")


def filter_region(df: pd.DataFrame, region: str) -> pd.DataFrame:
    """region is a dashboard region id (NEM, NSW1, ...); data uses NSW, QLD."""
    if region == "NEM" or df.empty:
        return df
    return df[df["Region"] == region[:-1]]


def consolidate(df: pd.DataFrame) -> pd.DataFrame:
    """Merge outages of the same asset starting within 2 days of the previous
    finish into one date range."""
    cols = ["Region", "NSP", "Network Asset", "Start", "Finish", "Status"]
    if df.empty:
        return pd.DataFrame(columns=cols)
    out = []
    for (region, asset), g in df.groupby(["Region", "Network Asset"]):
        cur = None
        for _, r in g.sort_values("Start").iterrows():
            if cur is not None and (r["Start"] - cur["Finish"]).days <= 2:
                cur["Finish"] = max(cur["Finish"], r["Finish"])
                cur["Count"] += 1
                continue
            if cur is not None:
                out.append(cur)
            cur = {"Region": region, "NSP": r.get("NSP", ""),
                   "Network Asset": asset, "Start": r["Start"],
                   "Finish": r["Finish"], "Status": r.get("Status", ""),
                   "Count": 1}
        out.append(cur)
    res = pd.DataFrame(out)
    multi = res["Count"] > 1
    res.loc[multi, "Network Asset"] = res.loc[multi].apply(
        lambda r: f"{r['Network Asset']} ({r['Count']} periods)", axis=1)
    return res.drop(columns="Count")[cols].sort_values("Start").reset_index(drop=True)


def within(df: pd.DataFrame, now: pd.Timestamp | None = None,
           days: int = 365) -> pd.DataFrame:
    """Rows starting no later than now + days."""
    now = now if now is not None else nem_now()
    return df[df["Start"] <= now + pd.Timedelta(days=days)]
