"""
WTI crude oil futures data: fetch, backfill, update, load.

Source: Yahoo Finance monthly NYMEX WTI contracts (ticker pattern
``CL<monthcode><yy>.NYM``) via the ``yfinance`` package. Each monthly
contract carries its own daily settlement history, so the *entire* forward
curve as it appeared on any past trading day can be reconstructed — that is
what makes a one-time backfill (today / 1 month ago / 1 year ago) possible.

Storage: a tidy Parquet file ``oil_futures.parquet`` in ``AEMO_DATA_PATH``
with one row per (observation_date, delivery_month):

    observation_date : datetime64[ns]  the trading day the price was observed
    delivery_month   : datetime64[ns]  first day of the contract delivery month
    price            : float64         settlement / close, USD per barrel
    benchmark        : str             'WTI' (reserved for future Brent support)

The updater is idempotent: re-runs merge on (observation_date,
delivery_month) keeping the latest price, so it is safe to run as often as
you like (e.g. daily via cron).

This module makes network calls to Yahoo Finance. Run ``backfill`` / ``update``
in an environment with outbound access to ``*.finance.yahoo.com``.
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd

logger = logging.getLogger(__name__)

# ── Configuration ──────────────────────────────────────────────────────

BENCHMARK = "WTI"
ROOT_SYMBOL = "CL"          # NYMEX WTI Crude
EXCHANGE_SUFFIX = ".NYM"    # Yahoo Finance NYMEX suffix

# Number of contract months to expose on the curve (spot ≈ M1 out to ~1yr).
CURVE_TENORS = 13           # M1..M13 → "spot to a year out" with a little slack

DATA_DIR = Path(os.getenv(
    "AEMO_DATA_PATH",
    "/Users/davidleitch/aemo_production/data",
))
PARQUET_NAME = "oil_futures.parquet"

# CME/NYMEX month codes.
MONTH_CODES = {
    1: "F", 2: "G", 3: "H", 4: "J", 5: "K", 6: "M",
    7: "N", 8: "Q", 9: "U", 10: "V", 11: "X", 12: "Z",
}


def data_path() -> Path:
    """Full path to the Parquet store."""
    return DATA_DIR / PARQUET_NAME


# ── Contract helpers ───────────────────────────────────────────────────

def contract_ticker(year: int, month: int) -> str:
    """Yahoo Finance ticker for the WTI contract delivering in year/month."""
    return f"{ROOT_SYMBOL}{MONTH_CODES[month]}{year % 100:02d}{EXCHANGE_SUFFIX}"


def _month_floor(ts: pd.Timestamp) -> pd.Timestamp:
    return pd.Timestamp(year=ts.year, month=ts.month, day=1)


def _add_months(ts: pd.Timestamp, n: int) -> pd.Timestamp:
    """Add n calendar months to a month-floored timestamp."""
    total = (ts.year * 12 + (ts.month - 1)) + n
    return pd.Timestamp(year=total // 12, month=total % 12 + 1, day=1)


def _contract_months(start: pd.Timestamp, end: pd.Timestamp):
    """Yield first-of-month timestamps from start..end inclusive."""
    cur = _month_floor(start)
    end = _month_floor(end)
    while cur <= end:
        yield cur
        cur = _add_months(cur, 1)


# ── Fetch ──────────────────────────────────────────────────────────────

def _download_close(ticker: str, start: str) -> pd.Series:
    """Return a date-indexed Series of daily closes for one contract.

    Returns an empty Series if the contract has no data (not yet listed,
    already delisted for the requested window, or a bad symbol).
    """
    import yfinance as yf

    try:
        df = yf.download(
            ticker, start=start, progress=False,
            auto_adjust=False, threads=False,
        )
    except Exception as e:  # network / parse errors — skip this contract
        logger.warning("Download failed for %s: %s", ticker, e)
        return pd.Series(dtype="float64")

    if df is None or df.empty or "Close" not in df.columns:
        return pd.Series(dtype="float64")

    close = df["Close"]
    # Single-ticker downloads can come back with a MultiIndex column.
    if isinstance(close, pd.DataFrame):
        close = close.iloc[:, 0]
    close = close.dropna()
    close.index = pd.to_datetime(close.index).tz_localize(None).normalize()
    return close


def _fetch_frame(delivery_months, start: str) -> pd.DataFrame:
    """Fetch a set of contracts and return the tidy long DataFrame."""
    frames = []
    for dm in delivery_months:
        ticker = contract_ticker(dm.year, dm.month)
        close = _download_close(ticker, start)
        if close.empty:
            logger.info("No data for %s (%s)", ticker, dm.date())
            continue
        frames.append(pd.DataFrame({
            "observation_date": close.index,
            "delivery_month": dm,
            "price": close.values,
        }))
        logger.info("Fetched %s: %d obs", ticker, len(close))

    if not frames:
        return _empty_frame()

    out = pd.concat(frames, ignore_index=True)
    out["benchmark"] = BENCHMARK
    return _normalize(out)


def _empty_frame() -> pd.DataFrame:
    return pd.DataFrame({
        "observation_date": pd.Series(dtype="datetime64[ns]"),
        "delivery_month": pd.Series(dtype="datetime64[ns]"),
        "price": pd.Series(dtype="float64"),
        "benchmark": pd.Series(dtype="object"),
    })


def _normalize(df: pd.DataFrame) -> pd.DataFrame:
    """Types, de-dup (keep latest price per obs/contract), sort."""
    if df.empty:
        return _empty_frame()
    df = df.copy()
    df["observation_date"] = pd.to_datetime(df["observation_date"]).dt.normalize()
    df["delivery_month"] = pd.to_datetime(df["delivery_month"]).dt.normalize()
    df["price"] = pd.to_numeric(df["price"], errors="coerce")
    df = df.dropna(subset=["price"])
    # Only keep points where the contract has not yet gone to delivery.
    df = df[df["delivery_month"] >= df["observation_date"].map(_month_floor)]
    df = (
        df.sort_values("observation_date")
        .drop_duplicates(["observation_date", "delivery_month"], keep="last")
        .sort_values(["observation_date", "delivery_month"])
        .reset_index(drop=True)
    )
    return df


# ── Public: backfill / update / load ──────────────────────────────────

def backfill(history_days: int = 430, forward_months: int = CURVE_TENORS + 1) -> pd.DataFrame:
    """Reconstruct ~1 year of daily curves and (over)write the Parquet store.

    Fetches every monthly contract whose delivery month falls between
    ``history_days`` ago and ``forward_months`` ahead, so that for any
    observation date in the past year the full M1..M13 curve is available.
    """
    today = _today()
    start_dt = today - timedelta(days=history_days)
    first_delivery = _month_floor(start_dt)
    last_delivery = _add_months(_month_floor(today), forward_months)

    months = list(_contract_months(first_delivery, last_delivery))
    logger.info(
        "Backfill: %d contracts %s .. %s",
        len(months), months[0].date(), months[-1].date(),
    )
    df = _fetch_frame(months, start=start_dt.strftime("%Y-%m-%d"))
    _save(df)
    return df


def update() -> pd.DataFrame:
    """Fetch recent closes for the forward curve and merge into the store.

    Only the near ~30 days of the currently-listed forward contracts are
    pulled, then merged idempotently with existing history. Safe to run
    daily. If no store exists yet, this delegates to a full backfill.
    """
    path = data_path()
    if not path.exists():
        logger.info("No existing store — running full backfill instead.")
        return backfill()

    today = _today()
    first_delivery = _month_floor(today)
    last_delivery = _add_months(first_delivery, CURVE_TENORS + 1)
    months = list(_contract_months(first_delivery, last_delivery))

    start = (today - timedelta(days=30)).strftime("%Y-%m-%d")
    fresh = _fetch_frame(months, start=start)

    existing = load_curve_data()
    combined = _normalize(pd.concat([existing, fresh], ignore_index=True))
    _save(combined)
    logger.info(
        "Update merged %d fresh rows; store now %d rows through %s",
        len(fresh), len(combined),
        combined["observation_date"].max().date() if not combined.empty else "—",
    )
    return combined


def load_curve_data() -> pd.DataFrame:
    """Load the Parquet store (empty frame if it does not exist)."""
    path = data_path()
    if not path.exists():
        logger.warning("%s not found", path)
        return _empty_frame()
    df = pd.read_parquet(path)
    return _normalize(df)


def _save(df: pd.DataFrame) -> None:
    path = data_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path, index=False)
    logger.info("Saved %d rows to %s", len(df), path)


def _today() -> pd.Timestamp:
    return pd.Timestamp(datetime.now().date())


# ── Curve reconstruction (used by the tab) ─────────────────────────────

def curve_as_of(df: pd.DataFrame, target: pd.Timestamp, tenors: int = CURVE_TENORS) -> pd.DataFrame:
    """The forward curve on the trading day on/before ``target``.

    Returns a DataFrame with columns [tenor, delivery_month, price] for the
    nearest ``tenors`` contracts, plus attribute ``.obs_date`` via the
    returned frame's ``attrs``. Empty if no data on/before target.
    """
    if df.empty:
        return df
    obs_dates = df["observation_date"].drop_duplicates().sort_values()
    idx = obs_dates.searchsorted(pd.Timestamp(target), side="right") - 1
    if idx < 0:
        return df.iloc[0:0]
    obs = obs_dates.iloc[idx]

    snap = df[df["observation_date"] == obs].sort_values("delivery_month").copy()
    obs_month = _month_floor(obs)
    snap = snap[snap["delivery_month"] >= obs_month]
    snap["tenor"] = snap["delivery_month"].apply(
        lambda dm: (dm.year - obs_month.year) * 12 + (dm.month - obs_month.month)
    )
    snap = snap[snap["tenor"] <= tenors].reset_index(drop=True)
    snap.attrs["obs_date"] = obs
    return snap[["tenor", "delivery_month", "price"]]


def front_month_series(df: pd.DataFrame) -> pd.Series:
    """Front-of-curve (nearest tenor ≥ current month) price over time."""
    if df.empty:
        return pd.Series(dtype="float64")
    fronts = (
        df.sort_values(["observation_date", "delivery_month"])
        .groupby("observation_date", as_index=True)
        .first()["price"]
    )
    return fronts


# ── CLI ────────────────────────────────────────────────────────────────

def main() -> None:
    """Console entry: ``aemo-oil-update [--backfill]``."""
    import argparse

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    parser = argparse.ArgumentParser(description="Update WTI oil futures curve data")
    parser.add_argument(
        "--backfill", action="store_true",
        help="Full ~1-year historical rebuild (run once to seed the store).",
    )
    args = parser.parse_args()

    df = backfill() if args.backfill else update()
    if df.empty:
        logger.error("No data written — check network access to Yahoo Finance.")
        raise SystemExit(1)
    print(
        f"{BENCHMARK}: {len(df)} rows, "
        f"{df['observation_date'].min().date()} .. {df['observation_date'].max().date()}, "
        f"{df['delivery_month'].nunique()} contracts."
    )


if __name__ == "__main__":
    main()
