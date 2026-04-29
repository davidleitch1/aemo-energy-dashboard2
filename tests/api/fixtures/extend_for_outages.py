"""Extend the test fixture with duid_info table and a minimal ST-PASA
parquet so test_outages.py runs without production data.

Re-runnable; drops + recreates the duid_info table and overwrites
test-pasa parquet.
"""
from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path

import duckdb
import pandas as pd

FIXTURE = Path(__file__).parent / "test.duckdb"
PASA = Path(__file__).parent / "outages_stpasa.parquet"


def main() -> None:
    if not FIXTURE.exists():
        raise SystemExit(f"missing {FIXTURE}")

    conn = duckdb.connect(str(FIXTURE), read_only=False)
    try:
        conn.execute("DROP TABLE IF EXISTS duid_info")
        conn.execute("""
            CREATE TABLE duid_info (
                DUID VARCHAR,
                \"Site Name\" VARCHAR,
                Owner VARCHAR,
                Fuel VARCHAR,
                Region VARCHAR,
                \"Capacity(MW)\" DOUBLE,
                \"Storage(MWh)\" DOUBLE
            )
        """)
        rows = [
            ("TARONG#2",  "Tarong",         "Stanwell", "Coal", "QLD1", 350.0, 0.0),
            ("TARONG#3",  "Tarong",         "Stanwell", "Coal", "QLD1", 350.0, 0.0),
            ("CPP_3",     "Callide C",      "CS Energy", "Coal", "QLD1", 240.0, 0.0),
            ("TNPS1",     "Tarong North",   "Stanwell", "Coal", "QLD1", 200.0, 0.0),
            ("URANQ11",   "Uranquinty",     "Snowy",    "OCGT", "NSW1", 165.0, 0.0),
            ("MSTUART1",  "Mt Stuart",      "Origin",   "OCGT", "QLD1", 145.0, 0.0),
            ("MSTUART2",  "Mt Stuart",      "Origin",   "OCGT", "QLD1", 130.0, 0.0),
            ("MSTUART3",  "Mt Stuart",      "Origin",   "OCGT", "QLD1", 120.0, 0.0),
            ("TIB1",      "Tailem Bend",    "Vena",     "Battery Storage", "SA1",  235.0, 470.0),
            ("HPRG1",     "Hornsdale",      "Neoen",    "Battery Storage", "VIC1",  75.0, 150.0),
            # A control DUID with no outage so we exercise the join.
            ("BAYSW1",    "Bayswater",      "AGL",      "Coal", "NSW1", 660.0, 0.0),
        ]
        conn.executemany(
            "INSERT INTO duid_info VALUES (?, ?, ?, ?, ?, ?, ?)", rows
        )
        n = conn.execute("SELECT COUNT(*) FROM duid_info").fetchone()[0]
        print(f"duid_info: {n} rows")
    finally:
        conn.close()

    # ST-PASA parquet — use a fixed near-future window so the test always sees
    # this data inside its 48h horizon (pd.Timestamp.now() at test time).
    base_run = pd.Timestamp(datetime.now()).floor("H")
    intervals = [base_run + pd.Timedelta(hours=h) for h in range(1, 8)]

    # (DUID, max_avail, pasa_avail) — pasa_avail is the available output, max_avail the cap.
    duid_targets = [
        # Coal — visible totals roughly mimic the dashboard screenshot.
        ("TARONG#2", 350.0, 0.0),
        ("TARONG#3", 350.0, 50.0),
        ("CPP_3",    240.0, 0.0),
        ("TNPS1",    200.0, 50.0),
        # OCGT
        ("URANQ11",  165.0, 50.0),
        ("MSTUART1", 145.0, 0.0),
        ("MSTUART2", 130.0, 0.0),
        ("MSTUART3", 120.0, 0.0),
        # Battery Storage
        ("TIB1",     235.0, 0.0),
        ("HPRG1",     75.0, 0.0),
        # Bayswater fully available — should be excluded.
        ("BAYSW1",   660.0, 660.0),
    ]
    rows_p = []
    for ts in intervals:
        for duid, mx, pa in duid_targets:
            rows_p.append({
                "RUN_DATETIME": base_run,
                "DUID": duid,
                "INTERVAL_DATETIME": ts,
                "GENERATION_MAX_AVAILABILITY": mx,
                "GENERATION_PASA_AVAILABILITY": pa,
                "GENERATION_RECALL_PERIOD": None,
                "LOAD_MAX_AVAILABILITY": None,
                "LOAD_PASA_AVAILABILITY": None,
                "LOAD_RECALL_PERIOD": None,
                "LASTCHANGED": base_run,
                "source_file": "FIXTURE",
                "collected_at": base_run,
            })
    df = pd.DataFrame(rows_p)
    df.to_parquet(PASA, index=False)
    print(f"{PASA.name}: {len(df)} rows")


if __name__ == "__main__":
    main()
