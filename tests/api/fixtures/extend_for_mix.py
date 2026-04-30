"""One-shot script that extends test.duckdb with multi-stamp 5-min data
spanning a 24-hour window before NOW, plus matching 30-min rooftop coverage.

This enables a regression test for the rooftop-densification fix in
/v1/generation/mix at 5-min resolution. The latest stamp/value remain the same
as extend_for_gauges.py, so gauge tests stay green.

Run after extend_for_gauges.py. Re-runnable: it preserves the latest 5-min
stamp's existing rows and only inserts earlier rows.
"""
from __future__ import annotations

import math
from datetime import datetime, timedelta
from pathlib import Path

import duckdb

FIXTURE = Path(__file__).parent / "test.duckdb"
REGIONS = ("NSW1", "QLD1", "VIC1", "SA1", "TAS1")
NOW = datetime(2026, 4, 29, 20, 0)
HOURS_BACK = 24

# Per-region 5-min generation mix at peak. Time-of-day shape applied below.
GEN_MIX = {
    "NSW1": {"Coal": 5800, "Wind": 1200, "Solar":   50, "Water":  900, "CCGT":   50, "Battery Storage":  400},
    "QLD1": {"Coal": 5200, "Wind":  600, "Solar":   30, "Water":  100, "CCGT":  300, "Battery Storage":  300},
    "VIC1": {"Coal": 4300, "Wind":  900, "Solar":   20, "Water":  500, "CCGT":   50, "Battery Storage":  350},
    "SA1":  {"Wind":  500, "Solar":   10, "CCGT":  140, "Battery Storage":   50},
    "TAS1": {"Wind":  150, "Water":  900, "CCGT":    0},
}

ROOF_PEAK = {"NSW1": 1800, "QLD1": 2200, "VIC1": 1100, "SA1": 600, "TAS1": 100}


def main() -> None:
    if not FIXTURE.exists():
        raise SystemExit(f"missing fixture {FIXTURE}; run extend_for_gauges.py first")
    conn = duckdb.connect(str(FIXTURE), read_only=False)
    try:
        # Preserve the latest stamp's rows (used by gauges tests). Drop only
        # older rows we may have added previously.
        conn.execute(
            "DELETE FROM generation_by_fuel_5min WHERE settlementdate < ?",
            [NOW],
        )

        # Add 5-min utility-scale generation for the prior 24 hours.
        gen_rows = []
        steps = HOURS_BACK * 12  # 5-min steps
        for i in range(steps):
            t = NOW - timedelta(minutes=5 * (steps - i))  # earliest first; excludes NOW
            hour_frac = (t.hour + t.minute / 60.0) / 24.0
            # Solar shape: peaks at noon, zero outside 0.25..0.75 of day
            sun = max(0.0, math.sin(math.pi * (hour_frac - 0.25) / 0.5)) if 0.25 <= hour_frac <= 0.75 else 0.0
            # Coal load-follow: lower at midday when rooftop+solar are high
            coal_factor = 1.0 - 0.45 * sun
            for reg, mix in GEN_MIX.items():
                for fuel, peak in mix.items():
                    if fuel == "Solar":
                        mw = peak * sun * 30.0  # utility solar scales with sun, much larger than the 50MW peak
                    elif fuel == "Coal":
                        mw = peak * coal_factor
                    else:
                        mw = peak * 0.95  # mild variation
                    gen_rows.append((t, fuel, reg, mw, 1, mw * 1.5))

        conn.executemany(
            "INSERT INTO generation_by_fuel_5min VALUES (?, ?, ?, ?, ?, ?)",
            gen_rows,
        )

        # Top up rooftop30 for the prior 24 hours if it doesn't already cover.
        # extend_for_gauges.py already wrote 30 days at 30-min cadence; this is
        # a no-op INSERT guarded by anti-join to be idempotent.
        roof_rows = []
        rsteps = HOURS_BACK * 2  # 30-min steps
        for i in range(rsteps + 1):
            t = NOW - timedelta(minutes=30 * (rsteps - i))
            hour_frac = (t.hour + t.minute / 60.0) / 24.0
            sun = max(0.0, math.sin(math.pi * (hour_frac - 0.25) / 0.5)) if 0.25 <= hour_frac <= 0.75 else 0.0
            for reg in REGIONS:
                roof_rows.append((t, reg, ROOF_PEAK[reg] * sun, "FINAL", "DAILY", "ARCHIVE"))

        # Idempotent: insert only rows that don't already exist.
        conn.executemany(
            """
            INSERT INTO rooftop30
            SELECT ?, ?, ?, ?, ?, ?
            WHERE NOT EXISTS (
                SELECT 1 FROM rooftop30
                WHERE settlementdate = ? AND regionid = ?
            )
            """,
            [(t, r, p, q, ty, sa, t, r) for (t, r, p, q, ty, sa) in roof_rows],
        )

        # Sanity print
        n_5min = conn.execute(
            "SELECT COUNT(DISTINCT settlementdate) FROM generation_by_fuel_5min"
        ).fetchone()[0]
        n_roof = conn.execute(
            "SELECT COUNT(DISTINCT settlementdate) FROM rooftop30"
        ).fetchone()[0]
        print(f"generation_by_fuel_5min: {n_5min} distinct stamps")
        print(f"rooftop30:               {n_roof} distinct stamps")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
