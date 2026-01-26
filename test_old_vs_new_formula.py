#!/usr/bin/env python3
"""
Compare OLD curtailment rate formula vs NEW formula
"""

import duckdb
from datetime import datetime, timedelta

print("=" * 80)
print("OLD FORMULA vs NEW FORMULA COMPARISON")
print("=" * 80)

conn = duckdb.connect(':memory:')

curtailment_file = '/Volumes/davidleitch/aemo_production/data/curtailment5.parquet'
scada_file = '/Volumes/davidleitch/aemo_production/data/scada30.parquet'

end_date = datetime(2025, 10, 15)
start_date = end_date - timedelta(days=7)

print(f"\nPeriod: {start_date.date()} to {end_date.date()}\n")

# OLD Formula: curtailment / availgen × 100
query_old = f"""
    SELECT
        DUID as duid,
        SUM(CURTAILMENT) / 12 as curtailed_mwh,
        SUM(CASE WHEN AVAILABILITY > 0 THEN AVAILABILITY ELSE 0 END) / 12 as total_avail_mwh,
        (SUM(CURTAILMENT) / NULLIF(SUM(CASE WHEN AVAILABILITY > 0 THEN AVAILABILITY ELSE 0 END), 0)) * 100 as rate_old
    FROM read_parquet('{curtailment_file}')
    WHERE SETTLEMENTDATE >= '{start_date.strftime('%Y-%m-%d %H:%M:%S')}'
      AND SETTLEMENTDATE <= '{end_date.strftime('%Y-%m-%d %H:%M:%S')}'
    GROUP BY DUID
    HAVING SUM(CURTAILMENT) > 0
    ORDER BY curtailed_mwh DESC
    LIMIT 5
"""

old_result = conn.execute(query_old).df()

# Get SCADA data for these DUIDs
duids = old_result['duid'].tolist()
duid_list = "','".join(duids)

query_scada = f"""
    SELECT
        UPPER(duid) as duid,
        SUM(scadavalue) * 0.5 as actual_mwh
    FROM read_parquet('{scada_file}')
    WHERE settlementdate >= '{start_date.strftime('%Y-%m-%d %H:%M:%S')}'
      AND settlementdate <= '{end_date.strftime('%Y-%m-%d %H:%M:%S')}'
      AND UPPER(duid) IN ('{duid_list}')
    GROUP BY UPPER(duid)
"""

scada_result = conn.execute(query_scada).df()

# Merge
merged = old_result.merge(scada_result, on='duid', how='left')
merged['actual_mwh'] = merged['actual_mwh'].fillna(0)

# NEW Formula: curtailment / (curtailment + actual) × 100
merged['rate_new'] = (merged['curtailed_mwh'] / (merged['curtailed_mwh'] + merged['actual_mwh'])) * 100

# Display comparison
print("=" * 100)
print("DUID            Curtailed   Avail Cap    Actual    OLD Rate  NEW Rate   Difference")
print("=" * 100)

for _, row in merged.iterrows():
    diff = row['rate_new'] - row['rate_old']
    print(f"{row['duid']:<12} {row['curtailed_mwh']:>10,.0f} {row['total_avail_mwh']:>10,.0f} {row['actual_mwh']:>10,.0f}   {row['rate_old']:>6.1f}%  {row['rate_new']:>6.1f}%   {diff:>+6.1f}%")

print("=" * 100)

print("\n" + "=" * 80)
print("FORMULA COMPARISON")
print("=" * 80)
print("\nOLD Formula (was in code before fix):")
print("  curtailment / available_capacity × 100")
print("  Meaning: What % of the unit's theoretical maximum was curtailed")
print("  Example: If 100 MW available, 20 MW curtailed → 20%")

print("\nNEW Formula (from documentation):")
print("  curtailment / (curtailment + actual) × 100")
print("  Meaning: What % of potential energy was lost to curtailment")
print("  Example: If 20 MW curtailed, 30 MW generated → 40%")

print("\nWhich is correct?")
print("  The documentation (curtailment.md and CLAUDE.md) specifies the NEW formula.")
print("  Regional summary uses NEW formula and shows 7.2% for NSW1.")
print("  But the old Top Units query was using the OLD formula.")
print("=" * 80)
