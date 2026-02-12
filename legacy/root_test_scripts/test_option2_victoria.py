#!/usr/bin/env python3
"""
Test Option 2: SCADA ≈ TOTALCLEARED compliance check
Applied to all Victorian wind and solar units across all available data
"""

import duckdb
import pandas as pd
from datetime import datetime

print("=" * 100)
print("OPTION 2 TEST: Victorian Wind & Solar Curtailment Analysis")
print("=" * 100)

conn = duckdb.connect(':memory:')

# Data paths
curtailment_file = '/Users/davidleitch/Library/Mobile Documents/com~apple~CloudDocs/snakeplay/AEMO_spot/aemo-energy-dashboard/data/curtailment5.parquet'
scada_file = '/Volumes/davidleitch/aemo_production/data/scada5.parquet'

# Get data coverage
coverage_query = f"""
    SELECT
        MIN(SETTLEMENTDATE) as start_date,
        MAX(SETTLEMENTDATE) as end_date,
        COUNT(DISTINCT DUID) as total_duids
    FROM read_parquet('{curtailment_file}')
"""

coverage = conn.execute(coverage_query).df()
print(f"\nData Coverage:")
print(f"  Period: {coverage['start_date'].iloc[0]} to {coverage['end_date'].iloc[0]}")
print(f"  Total Units: {coverage['total_duids'].iloc[0]}")

# Load wind/solar mapping to identify VIC1 units
import pickle
from pathlib import Path

ws_mapping_path = Path('/Volumes/davidleitch/aemo_production/aemo-energy-dashboard2/src/aemo_dashboard/curtailment/wind_solar_regions_complete.pkl')

if ws_mapping_path.exists():
    with open(ws_mapping_path, 'rb') as f:
        ws_mapping = pickle.load(f)

    # Filter to VIC1 only
    vic_units = {duid: info for duid, info in ws_mapping.items()
                 if info.get('region') == 'VIC1'}

    print(f"\nVictorian Units: {len(vic_units)}")
    vic_wind = {d: i for d, i in vic_units.items() if 'Wind' in i.get('fuel', '')}
    vic_solar = {d: i for d, i in vic_units.items() if 'Solar' in i.get('fuel', '')}
    print(f"  Wind: {len(vic_wind)}")
    print(f"  Solar: {len(vic_solar)}")
else:
    print("ERROR: Cannot find wind/solar mapping file")
    exit(1)

# Create merged view with curtailment + SCADA
print("\n" + "=" * 100)
print("Creating merged curtailment + SCADA view...")
print("=" * 100)

# Join curtailment with SCADA on timestamp and DUID
conn.execute(f"""
    CREATE OR REPLACE TABLE merged_data AS
    SELECT
        c.SETTLEMENTDATE as timestamp,
        c.DUID as duid,
        c.AVAILABILITY as availability,
        c.TOTALCLEARED as totalcleared,
        c.SEMIDISPATCHCAP as semidispatchcap,
        c.CURTAILMENT as curtailment_claimed,
        COALESCE(s.scadavalue, 0) as scada
    FROM read_parquet('{curtailment_file}') c
    LEFT JOIN read_parquet('{scada_file}') s
        ON c.SETTLEMENTDATE = s.settlementdate
        AND UPPER(c.DUID) = UPPER(s.duid)
    WHERE c.SEMIDISPATCHCAP = 1
""")

print("✓ Merged data created")

# Define Option 2 logic
# Count curtailment only when:
# 1. SCADA > threshold (proves wind/sun was present), AND
# 2. Unit obeyed dispatch (SCADA ≈ TOTALCLEARED within tolerance)

SCADA_THRESHOLD = 1.0  # MW - must be generating
COMPLIANCE_TOLERANCE = 0.20  # 20% - allow some variance

print("\nOption 2 Parameters:")
print(f"  SCADA threshold: {SCADA_THRESHOLD} MW")
print(f"  Compliance tolerance: {COMPLIANCE_TOLERANCE * 100}%")

# Calculate curtailment using both methods
query = f"""
    SELECT
        duid,
        COUNT(*) as total_intervals,

        -- CURRENT METHOD: All intervals where SEMIDISPATCHCAP=1
        COUNT(CASE WHEN curtailment_claimed > 0 THEN 1 END) as curtailed_intervals_current,
        SUM(curtailment_claimed) / 12 as curtailed_mwh_current,
        SUM(scada) * (5.0 / 60.0) as actual_mwh,

        -- OPTION 2: Only when SCADA > threshold AND unit obeyed dispatch
        COUNT(CASE
            WHEN scada > {SCADA_THRESHOLD} AND curtailment_claimed > 0 THEN 1
            ELSE NULL
        END) as curtailed_intervals_option2,

        SUM(CASE
            WHEN scada > {SCADA_THRESHOLD} THEN
                -- Check compliance
                CASE
                    WHEN totalcleared > 0 THEN
                        -- Unit should be generating
                        CASE WHEN ABS(scada - totalcleared) / NULLIF(totalcleared, 0) <= {COMPLIANCE_TOLERANCE}
                            THEN curtailment_claimed
                            ELSE 0
                        END
                    ELSE
                        -- Dispatch = 0: if generating, assume wind present but unit delayed shutdown
                        CASE WHEN scada < {SCADA_THRESHOLD} * 2
                            THEN curtailment_claimed  -- Small generation OK
                            ELSE 0  -- Large generation = non-compliance
                        END
                END
            ELSE 0
        END) / 12 as curtailed_mwh_option2

    FROM merged_data
    GROUP BY duid
    HAVING curtailed_intervals_current > 0
"""

print("\nRunning curtailment analysis...")
results = conn.execute(query).df()

print(f"✓ Analyzed {len(results)} units with curtailment\n")

# Add fuel type
results['fuel'] = results['duid'].map(lambda d: vic_units.get(d, {}).get('fuel', 'Unknown'))

# Filter to VIC1 only
results_vic = results[results['duid'].isin(vic_units.keys())].copy()

# Calculate rates
results_vic['rate_current'] = (results_vic['curtailed_mwh_current'] /
                                 (results_vic['curtailed_mwh_current'] + results_vic['actual_mwh'])) * 100

results_vic['rate_option2'] = (results_vic['curtailed_mwh_option2'] /
                                 (results_vic['curtailed_mwh_option2'] + results_vic['actual_mwh'])) * 100

# Sort by Option 2 curtailed MWh
results_vic = results_vic.sort_values('curtailed_mwh_option2', ascending=False)

# Separate wind and solar
wind_results = results_vic[results_vic['fuel'] == 'Wind'].copy()
solar_results = results_vic[results_vic['fuel'] == 'Solar'].copy()

# Display results
print("=" * 100)
print("VICTORIAN WIND UNITS - Top 10 by Option 2 Curtailment")
print("=" * 100)
print(f"{'DUID':<12} {'Intervals':<12} {'Actual':<12} {'Current':<12} {'Option 2':<12} {'Rate Curr':<10} {'Rate Opt2':<10}")
print(f"{'':12} {'Curtailed':<12} {'MWh':<12} {'Curt MWh':<12} {'Curt MWh':<12} {'%':<10} {'%':<10}")
print("-" * 100)

for _, row in wind_results.head(10).iterrows():
    print(f"{row['duid']:<12} {row['curtailed_intervals_option2']:>11,.0f} "
          f"{row['actual_mwh']:>11,.0f} {row['curtailed_mwh_current']:>11,.0f} "
          f"{row['curtailed_mwh_option2']:>11,.0f} {row['rate_current']:>9.1f} "
          f"{row['rate_option2']:>9.1f}")

print("\n" + "=" * 100)
print("VICTORIAN SOLAR UNITS - Top 10 by Option 2 Curtailment")
print("=" * 100)
print(f"{'DUID':<12} {'Intervals':<12} {'Actual':<12} {'Current':<12} {'Option 2':<12} {'Rate Curr':<10} {'Rate Opt2':<10}")
print(f"{'':12} {'Curtailed':<12} {'MWh':<12} {'Curt MWh':<12} {'Curt MWh':<12} {'%':<10} {'%':<10}")
print("-" * 100)

if len(solar_results) > 0:
    for _, row in solar_results.head(10).iterrows():
        print(f"{row['duid']:<12} {row['curtailed_intervals_option2']:>11,.0f} "
              f"{row['actual_mwh']:>11,.0f} {row['curtailed_mwh_current']:>11,.0f} "
              f"{row['curtailed_mwh_option2']:>11,.0f} {row['rate_current']:>9.1f} "
              f"{row['rate_option2']:>9.1f}")
else:
    print("No solar units with curtailment in Option 2 method")

# Summary statistics
print("\n" + "=" * 100)
print("SUMMARY STATISTICS")
print("=" * 100)

def print_stats(df, label):
    if len(df) == 0:
        print(f"\n{label}: No data")
        return

    print(f"\n{label} ({len(df)} units):")
    print("-" * 60)

    total_actual = df['actual_mwh'].sum()
    total_curt_current = df['curtailed_mwh_current'].sum()
    total_curt_option2 = df['curtailed_mwh_option2'].sum()

    rate_current = (total_curt_current / (total_curt_current + total_actual)) * 100
    rate_option2 = (total_curt_option2 / (total_curt_option2 + total_actual)) * 100

    print(f"  Total Actual Generation:       {total_actual:>12,.0f} MWh")
    print(f"  Total Curtailed (Current):     {total_curt_current:>12,.0f} MWh  ({rate_current:.1f}%)")
    print(f"  Total Curtailed (Option 2):    {total_curt_option2:>12,.0f} MWh  ({rate_option2:.1f}%)")
    print(f"  Reduction:                     {total_curt_current - total_curt_option2:>12,.0f} MWh  ({(1 - rate_option2/rate_current)*100:.0f}% lower)")

    print(f"\n  Average Rate (Current):        {df['rate_current'].mean():>12.1f}%")
    print(f"  Average Rate (Option 2):       {df['rate_option2'].mean():>12.1f}%")
    print(f"  Median Rate (Current):         {df['rate_current'].median():>12.1f}%")
    print(f"  Median Rate (Option 2):        {df['rate_option2'].median():>12.1f}%")

print_stats(wind_results, "WIND")
print_stats(solar_results, "SOLAR")
print_stats(results_vic, "ALL VICTORIAN UNITS")

# Test hypothesis: Solar has higher curtailment than wind
print("\n" + "=" * 100)
print("HYPOTHESIS TEST: Solar vs Wind Curtailment")
print("=" * 100)

if len(wind_results) > 0 and len(solar_results) > 0:
    wind_avg = wind_results['rate_option2'].mean()
    solar_avg = solar_results['rate_option2'].mean()

    print(f"\nAverage Curtailment Rate (Option 2):")
    print(f"  Wind:  {wind_avg:.1f}%")
    print(f"  Solar: {solar_avg:.1f}%")

    if solar_avg > wind_avg:
        print(f"\n✓ HYPOTHESIS CONFIRMED: Solar curtailment ({solar_avg:.1f}%) > Wind ({wind_avg:.1f}%)")
        print(f"  Difference: {solar_avg - wind_avg:.1f} percentage points")
    else:
        print(f"\n✗ HYPOTHESIS NOT CONFIRMED: Wind curtailment ({wind_avg:.1f}%) ≥ Solar ({solar_avg:.1f}%)")
else:
    print("\nInsufficient data for comparison")

# Check data quality
print("\n" + "=" * 100)
print("DATA QUALITY CHECK")
print("=" * 100)

intervals_kept = results_vic['curtailed_intervals_option2'].sum()
intervals_total = results_vic['curtailed_intervals_current'].sum()
retention_rate = (intervals_kept / intervals_total) * 100 if intervals_total > 0 else 0

print(f"\nCurtailment intervals:")
print(f"  Current method: {intervals_total:>10,.0f}")
print(f"  Option 2:       {intervals_kept:>10,.0f}")
print(f"  Retention rate: {retention_rate:>10.1f}%")
print(f"\nThis means Option 2 filtered out {100-retention_rate:.1f}% of curtailment intervals")
print("as unverified (unit not generating or not following dispatch)")

print("\n" + "=" * 100)
print("ANALYSIS COMPLETE")
print("=" * 100)
