#!/usr/bin/env python3
"""Check which batteries are missing scada data"""

import sys
sys.path.insert(0, 'src')

import duckdb
import pickle

# Initialize DuckDB
conn = duckdb.connect(':memory:')

# Load data
scada30_path = '/Volumes/davidleitch/aemo_production/data/scada30.parquet'
gen_info_path = '/Volumes/davidleitch/aemo_production/data/gen_info.pkl'

conn.execute(f"CREATE VIEW scada30 AS SELECT * FROM read_parquet('{scada30_path}')")

with open(gen_info_path, 'rb') as f:
    duid_df = pickle.load(f)
conn.register('duid_mapping', duid_df)

# Get all batteries from mapping
all_batteries = conn.execute("""
    SELECT DUID, "Site Name", Region, "Capacity(MW)", Fuel
    FROM duid_mapping
    WHERE Fuel = 'Battery Storage'
    ORDER BY Region, DUID
""").df()

print(f"Total batteries in mapping: {len(all_batteries)}")

# Get batteries with recent data
batteries_with_data = conn.execute("""
    SELECT DISTINCT d.DUID
    FROM scada30 s
    INNER JOIN duid_mapping d ON s.duid = d.DUID
    WHERE d.Fuel = 'Battery Storage'
      AND s.settlementdate >= CURRENT_DATE - INTERVAL '7 days'
""").df()

print(f"Batteries with data (last 7 days): {len(batteries_with_data)}")

# Find missing batteries
missing = all_batteries[~all_batteries['DUID'].isin(batteries_with_data['DUID'])]

print(f"\n{'='*80}")
print(f"MISSING BATTERIES ({len(missing)} units):")
print(f"{'='*80}")
print(missing[['DUID', 'Site Name', 'Region', 'Capacity(MW)']].to_string(index=False))

# Check total capacity
print(f"\n{'='*80}")
print(f"CAPACITY ANALYSIS:")
print(f"{'='*80}")
print(f"Total battery capacity (all): {all_batteries['Capacity(MW)'].sum():.1f} MW")
print(f"Missing battery capacity: {missing['Capacity(MW)'].sum():.1f} MW")
print(f"Active battery capacity: {all_batteries[all_batteries['DUID'].isin(batteries_with_data['DUID'])]['Capacity(MW)'].sum():.1f} MW")

# Check peak discharge by region (last 7 days)
print(f"\n{'='*80}")
print(f"PEAK BATTERY DISCHARGE BY REGION (last 7 days):")
print(f"{'='*80}")

regional_peaks = conn.execute("""
    SELECT
        d.Region,
        COUNT(DISTINCT d.DUID) as num_batteries,
        SUM(d."Capacity(MW)") as total_capacity_mw,
        MAX(region_total.total_discharge) as peak_discharge_mw,
        MAX(region_total.total_charge) as peak_charge_mw
    FROM duid_mapping d
    LEFT JOIN (
        SELECT
            d2.Region,
            s.settlementdate,
            SUM(CASE WHEN s.scadavalue > 0 THEN s.scadavalue ELSE 0 END) as total_discharge,
            SUM(CASE WHEN s.scadavalue < 0 THEN s.scadavalue ELSE 0 END) as total_charge
        FROM scada30 s
        INNER JOIN duid_mapping d2 ON s.duid = d2.DUID
        WHERE d2.Fuel = 'Battery Storage'
          AND s.settlementdate >= CURRENT_DATE - INTERVAL '7 days'
        GROUP BY d2.Region, s.settlementdate
    ) region_total ON d.Region = region_total.Region
    WHERE d.Fuel = 'Battery Storage'
    GROUP BY d.Region
    ORDER BY d.Region
""").df()

print(regional_peaks.to_string(index=False))

# Check if any batteries have suspiciously low max values despite high capacity
print(f"\n{'='*80}")
print(f"BATTERIES WITH LOW UTILIZATION (max < 10% of capacity):")
print(f"{'='*80}")

low_util = conn.execute("""
    SELECT
        d.DUID,
        d."Site Name",
        d.Region,
        d."Capacity(MW)" as capacity_mw,
        MAX(ABS(s.scadavalue)) as max_abs_mw,
        MAX(ABS(s.scadavalue)) / d."Capacity(MW)" * 100 as utilization_pct
    FROM scada30 s
    INNER JOIN duid_mapping d ON s.duid = d.DUID
    WHERE d.Fuel = 'Battery Storage'
      AND s.settlementdate >= CURRENT_DATE - INTERVAL '7 days'
    GROUP BY d.DUID, d."Site Name", d.Region, d."Capacity(MW)"
    HAVING MAX(ABS(s.scadavalue)) / d."Capacity(MW)" < 0.10
    ORDER BY d."Capacity(MW)" DESC
""").df()

print(low_util.to_string(index=False))
