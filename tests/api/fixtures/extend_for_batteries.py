"""Extend the test fixture with a scada30 table covering the two battery
DUIDs already added by extend_for_outages.py (TIB1 SA1, HPRG1 VIC1).

Re-runnable; drops + recreates only the scada30 table. Aligns to the
existing prices30 window so battery × price joins produce data.

Per-DUID dispatch pattern is deterministic so test_batteries_*.py can
assert on energies and ordering without reading random fixture prices.
"""
from __future__ import annotations

from datetime import timedelta
from pathlib import Path

import duckdb
import pandas as pd

FIXTURE = Path(__file__).parent / 'test.duckdb'


def main() -> None:
    if not FIXTURE.exists():
        raise SystemExit(f'missing {FIXTURE}')

    conn = duckdb.connect(str(FIXTURE), read_only=False)
    try:
        # Use the prices30 window so joins produce non-empty results.
        rng = conn.execute(
            'SELECT MIN(settlementdate), MAX(settlementdate) FROM prices30'
        ).fetchone()
        start, end = rng
        if start is None:
            raise SystemExit('prices30 is empty in the fixture')

        # Build 30-min slots across the full window (inclusive).
        slots = pd.date_range(start, end, freq='30min')

        # Deterministic charge/discharge profile relative to NEM hour-of-day.
        # TIB1 (SA1): big discharger 17:00-19:00, charges 11:00-13:00
        # HPRG1 (VIC1): smaller battery, similar shape shifted by an hour
        rows = []
        for ts in slots:
            h = ts.hour
            if 17 <= h <= 19:
                rows.append((ts, 'TIB1', 200.0))
                rows.append((ts, 'HPRG1', 60.0))
            elif 11 <= h <= 13:
                rows.append((ts, 'TIB1', -100.0))
                rows.append((ts, 'HPRG1', -45.0))
            else:
                rows.append((ts, 'TIB1', 0.0))
                rows.append((ts, 'HPRG1', 0.0))

        conn.execute('DROP TABLE IF EXISTS scada30')
        conn.execute("""
            CREATE TABLE scada30 (
                settlementdate TIMESTAMP_NS,
                duid VARCHAR,
                scadavalue DOUBLE
            )
        """)
        conn.executemany(
            'INSERT INTO scada30 VALUES (?, ?, ?)', rows
        )
        n = conn.execute('SELECT COUNT(*) FROM scada30').fetchone()[0]
        print(f'scada30: {n} rows across {len(slots)} 30-min slots')
    finally:
        conn.close()


if __name__ == '__main__':
    main()
