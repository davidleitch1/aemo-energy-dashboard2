#!/usr/bin/env python3
"""
Simple direct query to diagnose curtailment rate calculation
"""

import duckdb
import pandas as pd
from datetime import datetime, timedelta

print("=" * 80)
print("SIMPLE CURTAILMENT RATE TEST")
print("=" * 80)

# Connect to DuckDB
conn = duckdb.connect(':memory:')

# Data paths
curtailment_file = '/Volumes/davidleitch/aemo_production/data/curtailment5.parquet'
scada_file = '/Volumes/davidleitch/aemo_production/data/scada30.parquet'

# Date range
end_date = datetime(2025, 10, 15)
start_date = end_date - timedelta(days=7)

print(f"\nQuerying {start_date.date()} to {end_date.date()}")

# Simple query: Top curtailed units WITHOUT joining to SCADA
print("\n" + "=" * 80)
print("TEST 1: Curtailment totals (no SCADA join)")
print("=" * 80)

query1 = f"""
    SELECT
        DUID as duid,
        COUNT(*) as intervals,
        SUM(CURTAILMENT) / 12 as curtailed_mwh,
        AVG(AVAILABILITY) as avg_avail_mw,
        MAX(CURTAILMENT) as max_curt_mw
    FROM read_parquet('{curtailment_file}')
    WHERE SETTLEMENTDATE >= '{start_date.strftime('%Y-%m-%d %H:%M:%S')}'
      AND SETTLEMENTDATE <= '{end_date.strftime('%Y-%m-%d %H:%M:%S')}'
      AND CURTAILMENT > 0
    GROUP BY DUID
    ORDER BY curtailed_mwh DESC
    LIMIT 5
"""

result1 = conn.execute(query1).df()
print(result1.to_string(index=False))

# Now get SCADA data for these DUIDs
print("\n" + "=" * 80)
print("TEST 2: SCADA generation for top curtailed units")
print("=" * 80)

if not result1.empty:
    duids = result1['duid'].tolist()
    duid_list = "','".join(duids)

    query2 = f"""
        SELECT
            UPPER(duid) as duid,
            SUM(scadavalue) * 0.5 as actual_mwh
        FROM read_parquet('{scada_file}')
        WHERE settlementdate >= '{start_date.strftime('%Y-%m-%d %H:%M:%S')}'
          AND settlementdate <= '{end_date.strftime('%Y-%m-%d %H:%M:%S')}'
          AND UPPER(duid) IN ('{duid_list}')
        GROUP BY UPPER(duid)
    """

    result2 = conn.execute(query2).df()
    print(result2.to_string(index=False))

    # Merge and calculate rates
    print("\n" + "=" * 80)
    print("TEST 3: Calculated Curtailment Rates")
    print("=" * 80)

    merged = result1.merge(result2, on='duid', how='left')
    merged['actual_mwh'] = merged['actual_mwh'].fillna(0)
    merged['rate_pct'] = (merged['curtailed_mwh'] / (merged['curtailed_mwh'] + merged['actual_mwh'])) * 100

    print("\nDUID                Curtailed    Actual      Rate")
    print("-" * 60)
    for _, row in merged.iterrows():
        print(f"{row['duid']:<15} {row['curtailed_mwh']:>10,.0f} {row['actual_mwh']:>10,.0f}  {row['rate_pct']:>6.1f}%")

        # Diagnosis
        if row['actual_mwh'] < row['curtailed_mwh'] * 0.1:
            print(f"  ⚠️  WARNING: Actual generation very low - possible SCADA data issue")
        if row['rate_pct'] > 50:
            print(f"  ⚠️  Rate > 50% - unit was mostly curtailed")

print("\n" + "=" * 80)
