"""One-shot script that extends test.duckdb with the tables needed by
test_gauges.py. Adds 30 days of synthetic data so the all-time min/max
and 30-day rolling-max queries return non-trivial values.

Run once after edits to the gauge schema; commit the resulting
test.duckdb. Re-runnable: drops + recreates the new tables but
leaves prices5 untouched.
"""
from __future__ import annotations

import math
from datetime import datetime, timedelta
from pathlib import Path

import duckdb

FIXTURE = Path(__file__).parent / "test.duckdb"
REGIONS = ("NSW1", "QLD1", "VIC1", "SA1", "TAS1")
MAINLAND = ("NSW1", "QLD1", "VIC1", "SA1")
NOW = datetime(2026, 4, 29, 20, 0)
DAYS = 30


def main() -> None:
    if not FIXTURE.exists():
        raise SystemExit(f"missing fixture {FIXTURE}")
    conn = duckdb.connect(str(FIXTURE), read_only=False)
    try:
        for tbl in ("demand30", "rooftop30", "bdu5",
                    "generation_by_fuel_5min", "predispatch"):
            conn.execute(f"DROP TABLE IF EXISTS {tbl}")

        conn.execute("""
            CREATE TABLE demand30 (
                settlementdate TIMESTAMP_NS, regionid VARCHAR,
                demand DOUBLE, demand_less_snsg DOUBLE
            )
        """)
        conn.execute("""
            CREATE TABLE rooftop30 (
                settlementdate TIMESTAMP_NS, regionid VARCHAR,
                power DOUBLE, quality_indicator VARCHAR,
                type VARCHAR, source_archive VARCHAR
            )
        """)
        conn.execute("""
            CREATE TABLE bdu5 (
                settlementdate TIMESTAMP_NS, regionid VARCHAR,
                bdu_energy_storage DOUBLE,
                bdu_clearedmw_gen DOUBLE, bdu_clearedmw_load DOUBLE
            )
        """)
        conn.execute("""
            CREATE TABLE generation_by_fuel_5min (
                settlementdate TIMESTAMP_NS, fuel_type VARCHAR, region VARCHAR,
                total_generation_mw DOUBLE, unit_count BIGINT,
                total_capacity_mw DOUBLE
            )
        """)
        conn.execute("""
            CREATE TABLE predispatch (
                run_time TIMESTAMP, settlementdate TIMESTAMP_NS, regionid VARCHAR,
                price_forecast DOUBLE, demand_forecast DOUBLE,
                solar_forecast DOUBLE, wind_forecast DOUBLE
            )
        """)

        # 30 days of half-hourly data — enough for all-time + 30-day max queries.
        # Per-region demand has a daily sinusoid; rooftop peaks midday.
        base_demand = {"NSW1": 8000, "QLD1": 6500, "VIC1": 5500, "SA1": 1500, "TAS1": 1100}
        roof_peak   = {"NSW1": 1800, "QLD1": 2200, "VIC1": 1100, "SA1":  600, "TAS1":  100}
        battery_pk  = {"NSW1": 4000, "QLD1": 3000, "VIC1": 3500, "SA1":  1500}

        steps = DAYS * 48
        d_rows = []
        r_rows = []
        b_rows = []
        for i in range(steps):
            t = NOW - timedelta(minutes=30 * (steps - 1 - i))
            hour_frac = (t.hour + t.minute / 60.0) / 24.0
            day_factor = 0.7 + 0.3 * math.sin(2 * math.pi * hour_frac - math.pi / 2)  # min ~04:00
            sun = max(0.0, math.sin(math.pi * (hour_frac - 0.25) / 0.5)) if 0.25 <= hour_frac <= 0.75 else 0.0
            soc_phase = math.sin(2 * math.pi * (hour_frac - 0.25))  # charge midday, discharge eve
            for reg in REGIONS:
                d_rows.append((t, reg, base_demand[reg] * day_factor, base_demand[reg] * day_factor * 0.95))
                r_rows.append((t, reg, roof_peak[reg] * sun, "FINAL", "DAILY", "ARCHIVE"))
            for reg in MAINLAND:
                # SoC oscillates around mid-charge; at end (i=steps-1) we want about 25% of cap.
                soc = battery_pk[reg] * (0.55 + 0.35 * soc_phase)
                b_rows.append((t, reg, soc, 0.0, 0.0))
            # TAS bdu5 has nulls (matches production)
            b_rows.append((t, "TAS1", None, 0.0, 0.0))

        conn.executemany(
            "INSERT INTO demand30 VALUES (?, ?, ?, ?)", d_rows
        )
        conn.executemany(
            "INSERT INTO rooftop30 VALUES (?, ?, ?, ?, ?, ?)", r_rows
        )
        conn.executemany(
            "INSERT INTO bdu5 VALUES (?, ?, ?, ?, ?)", b_rows
        )

        # Generation by fuel — last hour only is enough; earlier 30 days unused
        # because gauges read latest-only.
        latest_gen_ts = NOW
        gen_rows = []
        # Per-region fuel mix (MW). Roughly realistic NEM evening peak.
        gen_mix = {
            "NSW1": {"Coal": 5800, "Wind": 1200, "Solar":   50, "Water":  900, "CCGT":   50, "Battery Storage":  400},
            "QLD1": {"Coal": 5200, "Wind":  600, "Solar":   30, "Water":  100, "CCGT":  300, "Battery Storage":  300},
            "VIC1": {"Coal": 4300, "Wind":  900, "Solar":   20, "Water":  500, "CCGT":   50, "Battery Storage":  350},
            "SA1":  {"Wind":  500, "Solar":   10, "CCGT":  140, "Battery Storage":   50},
            "TAS1": {"Wind":  150, "Water":  900, "CCGT":    0},
        }
        for reg, mix in gen_mix.items():
            for fuel, mw in mix.items():
                gen_rows.append((latest_gen_ts, fuel, reg, mw, 1, mw * 1.5))
        conn.executemany(
            "INSERT INTO generation_by_fuel_5min VALUES (?, ?, ?, ?, ?, ?)",
            gen_rows,
        )

        # Predispatch — single run_time, 24 forward intervals, demand_forecast > current.
        run_time = NOW
        pre_rows = []
        for h in range(24):
            ts = NOW + timedelta(minutes=30 * (h + 1))
            for reg in REGIONS:
                # Forecast peak ~ 5% above current at hour 18 (relative to NOW)
                bump = 1.0 + 0.05 * math.sin(2 * math.pi * (h / 24))
                pre_rows.append((run_time, ts, reg, 80.0, base_demand[reg] * bump, 0.0, 0.0))
        conn.executemany(
            "INSERT INTO predispatch VALUES (?, ?, ?, ?, ?, ?, ?)",
            pre_rows,
        )

        # Quick sanity print
        for tbl in ("demand30", "rooftop30", "bdu5",
                    "generation_by_fuel_5min", "predispatch"):
            n = conn.execute(f"SELECT COUNT(*) FROM {tbl}").fetchone()[0]
            print(f"{tbl}: {n} rows")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
