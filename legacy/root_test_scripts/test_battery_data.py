#!/usr/bin/env python3
"""Test script to check battery data availability"""

import sys
sys.path.insert(0, 'src')

import duckdb
from aemo_dashboard.shared.config import config
import pickle

# Initialize DuckDB
conn = duckdb.connect(':memory:')

# Load parquet files - use volume paths for dev machine
scada30_path = '/Volumes/davidleitch/aemo_production/data/scada30.parquet'
gen_info_path = '/Volumes/davidleitch/aemo_production/data/gen_info.pkl'

print(f"Loading scada30 from: {scada30_path}")
print(f"Loading gen_info from: {gen_info_path}")

# Register scada data
conn.execute(f"CREATE VIEW scada30 AS SELECT * FROM read_parquet('{scada30_path}')")

# Load gen_info (pickle file)
with open(gen_info_path, 'rb') as f:
    duid_df = pickle.load(f)

print(f"\nDUID mapping loaded: {len(duid_df)} records")
print(f"Columns: {duid_df.columns.tolist()}")

# Register in DuckDB
conn.register('duid_mapping', duid_df)

# Check fuel types
print("\n" + "="*60)
print("All fuel types in DUID mapping:")
fuel_types = conn.execute("SELECT DISTINCT Fuel FROM duid_mapping ORDER BY Fuel").df()
print(fuel_types)

# Check for battery/BESS
print("\n" + "="*60)
print("Batteries/BESS in DUID mapping:")
batteries = conn.execute("""
    SELECT DUID, "Site Name", Fuel, Region, "Capacity(MW)"
    FROM duid_mapping
    WHERE UPPER(Fuel) LIKE '%BATT%' OR UPPER(Fuel) LIKE '%BESS%'
    ORDER BY Region, DUID
""").df()
print(f"Found {len(batteries)} battery units:")
print(batteries.to_string())

# Check if there's actual scada data for batteries
print("\n" + "="*60)
print("Checking scada30 data for batteries (last 7 days):")
battery_scada = conn.execute("""
    SELECT
        d.DUID,
        d."Site Name",
        d.Fuel,
        d.Region,
        COUNT(*) as record_count,
        MIN(s.settlementdate) as first_date,
        MAX(s.settlementdate) as last_date,
        AVG(s.scadavalue) as avg_mw,
        MIN(s.scadavalue) as min_mw,
        MAX(s.scadavalue) as max_mw
    FROM scada30 s
    INNER JOIN duid_mapping d ON s.duid = d.DUID
    WHERE (UPPER(d.Fuel) LIKE '%BATT%' OR UPPER(d.Fuel) LIKE '%BESS%')
      AND s.settlementdate >= CURRENT_DATE - INTERVAL '7 days'
    GROUP BY d.DUID, d."Site Name", d.Fuel, d.Region
    ORDER BY d.Region, d.DUID
""").df()

print(f"Found {len(battery_scada)} batteries with scada data in last 7 days:")
print(battery_scada.to_string())

# Check total battery MW by region
print("\n" + "="*60)
print("Total battery generation by region (last 7 days):")
regional = conn.execute("""
    SELECT
        d.Region,
        COUNT(DISTINCT d.DUID) as num_batteries,
        COUNT(*) as total_records,
        AVG(s.scadavalue) as avg_mw,
        SUM(CASE WHEN s.scadavalue > 0 THEN s.scadavalue ELSE 0 END) / COUNT(*) as avg_discharge_mw,
        SUM(CASE WHEN s.scadavalue < 0 THEN s.scadavalue ELSE 0 END) / COUNT(*) as avg_charge_mw
    FROM scada30 s
    INNER JOIN duid_mapping d ON s.duid = d.DUID
    WHERE (UPPER(d.Fuel) LIKE '%BATT%' OR UPPER(d.Fuel) LIKE '%BESS%')
      AND s.settlementdate >= CURRENT_DATE - INTERVAL '7 days'
    GROUP BY d.Region
    ORDER BY d.Region
""").df()

print(regional.to_string())
