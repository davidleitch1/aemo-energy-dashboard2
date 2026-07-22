#!/usr/bin/env python3
"""
Update the oil futures curve data file used by the dashboard "Oil" tab.

Pulls the monthly futures strip (spot/front month out ~13 months) for WTI
(NYMEX CL) and Brent (ICE B) from Yahoo Finance, and appends today's full
curve to ``$AEMO_DATA_PATH/oil_futures.csv``.

File layout (mirrors futures.csv):
  - index column ``date``  : observation (settlement) date
  - one column per contract: ``<Benchmark> <YYYY-MM>``  e.g. ``WTI 2026-08``
  - cell value             : that contract's settlement/close on that date

Each run refetches ~2 years of daily history for every currently-listed
contract and merges it in, so re-running is idempotent and self-healing.
Newly-fetched values win on overlap; previously-stored values for contracts
Yahoo no longer serves (expired months) are preserved.

NOTE ON HISTORY: Yahoo only serves history for contracts that are still
listed. So a freshly-seeded file can reconstruct the *far* part of past
curves but not the near, already-expired part. The authoritative record of
the full "1 month ago" / "1 year ago" curves builds up as this script runs
regularly (e.g. daily via cron). Run it once to seed, then schedule it.

Usage:
    python scripts/update_oil_futures.py                 # WTI + Brent
    python scripts/update_oil_futures.py --benchmarks WTI
    python scripts/update_oil_futures.py --months 13 --range 2y
    AEMO_DATA_PATH=/path/to/data python scripts/update_oil_futures.py
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
from datetime import date, datetime, timezone
from pathlib import Path

import pandas as pd
import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger("update_oil_futures")

# Futures month codes: Jan..Dec
MONTH_CODES = "FGHJKMNQUVXZ"

# Benchmark -> Yahoo Finance monthly-contract symbol builder.
# WTI trades on NYMEX and Brent (the Yahoo "BZ" series) is quoted on the same
# venue tag, so both use the ``.NYM`` suffix, e.g. CLQ26.NYM / BZQ26.NYM.
BENCHMARKS = {
    "WTI": {"root": "CL", "suffix": ".NYM"},
    "Brent": {"root": "BZ", "suffix": ".NYM"},
}

DATA_DIR = Path(os.environ.get("AEMO_DATA_PATH", "/Users/davidleitch/aemo_production/data"))
OUTPUT_FILE = DATA_DIR / "oil_futures.csv"

YF_CHART = "https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"
HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; aemo-dashboard/1.0)"}


def _contract_months(start: date, n_months: int) -> list[tuple[int, int]]:
    """Return the next ``n_months`` (year, month) delivery periods from ``start``."""
    out = []
    y, m = start.year, start.month
    for _ in range(n_months):
        out.append((y, m))
        m += 1
        if m > 12:
            m = 1
            y += 1
    return out


def _yahoo_symbol(root: str, suffix: str, year: int, month: int) -> str:
    letter = MONTH_CODES[month - 1]
    return f"{root}{letter}{year % 100:02d}{suffix}"


def _fetch_contract(symbol: str, rng: str) -> pd.Series | None:
    """Fetch daily close history for a single contract. Returns a date-indexed Series."""
    url = YF_CHART.format(symbol=symbol)
    try:
        resp = requests.get(
            url, params={"range": rng, "interval": "1d"}, headers=HEADERS, timeout=30
        )
        resp.raise_for_status()
        payload = resp.json()
    except Exception as exc:  # noqa: BLE001 - network/JSON errors are all non-fatal per symbol
        logger.warning("  %s: fetch failed (%s)", symbol, exc)
        return None

    result = (payload.get("chart") or {}).get("result")
    if not result:
        err = (payload.get("chart") or {}).get("error")
        logger.warning("  %s: no data (%s)", symbol, err)
        return None

    res = result[0]
    timestamps = res.get("timestamp") or []
    quote = (res.get("indicators", {}).get("quote") or [{}])[0]
    closes = quote.get("close") or []
    if not timestamps or not closes:
        logger.warning("  %s: empty series", symbol)
        return None

    idx = [datetime.fromtimestamp(t, tz=timezone.utc).date() for t in timestamps]
    ser = pd.Series(closes, index=pd.to_datetime(idx), name=symbol)
    ser = ser[ser.notna()]
    return ser if not ser.empty else None


def build_curve_frame(benchmarks: list[str], n_months: int, rng: str) -> pd.DataFrame:
    """Fetch every contract for the requested benchmarks into a date x contract frame."""
    today = datetime.now(timezone.utc).date()
    columns: dict[str, pd.Series] = {}

    for bench in benchmarks:
        spec = BENCHMARKS[bench]
        logger.info("Fetching %s strip (%d months)...", bench, n_months)
        for (y, m) in _contract_months(today, n_months):
            symbol = _yahoo_symbol(spec["root"], spec["suffix"], y, m)
            ser = _fetch_contract(symbol, rng)
            if ser is None:
                continue
            col = f"{bench} {y:04d}-{m:02d}"
            columns[col] = ser
            logger.info("  %s -> %s (%d rows)", symbol, col, len(ser))

    if not columns:
        return pd.DataFrame()

    frame = pd.DataFrame(columns)
    frame.index.name = "date"
    return frame.sort_index().round(2)


def merge_and_write(new_frame: pd.DataFrame, output: Path) -> pd.DataFrame:
    """Merge freshly-fetched data with any existing file; newest values win."""
    if output.exists():
        existing = pd.read_csv(output, parse_dates=["date"]).set_index("date")
        # New values win on overlap; existing fills gaps (incl. expired-contract columns).
        merged = new_frame.combine_first(existing)
        # Union of columns, keep a stable sorted order.
        merged = merged.reindex(sorted(merged.columns), axis=1).sort_index()
    else:
        merged = new_frame.reindex(sorted(new_frame.columns), axis=1)

    output.parent.mkdir(parents=True, exist_ok=True)
    merged.round(2).to_csv(output)
    return merged


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--benchmarks", nargs="+", default=list(BENCHMARKS),
        choices=list(BENCHMARKS), help="Benchmarks to fetch (default: all)",
    )
    parser.add_argument(
        "--months", type=int, default=13,
        help="Number of forward contract months to fetch (default: 13 = spot + ~1yr)",
    )
    parser.add_argument(
        "--range", default="2y", dest="rng",
        help="Yahoo history range per contract (default: 2y)",
    )
    parser.add_argument(
        "--output", type=Path, default=OUTPUT_FILE,
        help=f"Output CSV path (default: {OUTPUT_FILE})",
    )
    args = parser.parse_args(argv)

    frame = build_curve_frame(args.benchmarks, args.months, args.rng)
    if frame.empty:
        logger.error("No contract data fetched - nothing written. Check network/symbols.")
        return 1

    merged = merge_and_write(frame, args.output)
    logger.info(
        "Wrote %s: %d rows x %d contracts (latest %s)",
        args.output, len(merged), merged.shape[1],
        merged.index.max().date() if len(merged) else "n/a",
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
