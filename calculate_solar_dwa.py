#!/usr/bin/env python3
"""Calculate Solar Dispatch-Weighted Average price for SA1 FY2024-25"""

import pandas as pd
import duckdb
from pathlib import Path
import pickle

# Set up paths
data_dir = Path("/Volumes/davidleitch/aemo_production/data")
gen_30min = data_dir / "scada30.parquet"
prices_30min = data_dir / "prices30.parquet"
gen_info = data_dir / "gen_info.pkl"

# Load DUID mapping
print("Loading DUID mapping...")
with open(gen_info, "rb") as f:
    duid_mapping = pickle.load(f)

# Get Solar DUIDs in SA1
solar_duids_sa1 = duid_mapping[(duid_mapping["Fuel"] == "Solar") & (duid_mapping["Region"] == "SA1")]["DUID"].tolist()
print(f"Found {len(solar_duids_sa1)} Solar DUIDs in SA1")
if len(solar_duids_sa1) > 5:
    print(f"Solar DUIDs (first 5): {solar_duids_sa1[:5]}")
else:
    print(f"Solar DUIDs: {solar_duids_sa1}")

# Connect to DuckDB and query data
print("\nQuerying data from DuckDB...")
conn = duckdb.connect()

# Query for generation and price data
query = """
WITH solar_gen AS (
    SELECT 
        settlementdate,
        SUM(scadavalue) as total_solar_mw
    FROM read_parquet(?)
    WHERE settlementdate >= '2024-07-01'
      AND settlementdate <= '2025-06-30 23:30:00'
      AND duid = ANY(?)
    GROUP BY settlementdate
),
prices AS (
    SELECT 
        settlementdate,
        rrp
    FROM read_parquet(?)
    WHERE settlementdate >= '2024-07-01'
      AND settlementdate <= '2025-06-30 23:30:00'
      AND regionid = 'SA1'
)
SELECT 
    sg.settlementdate,
    sg.total_solar_mw,
    p.rrp,
    sg.total_solar_mw * p.rrp * 0.5 as revenue,
    sg.total_solar_mw * 0.5 as energy_mwh
FROM solar_gen sg
JOIN prices p ON sg.settlementdate = p.settlementdate
ORDER BY sg.settlementdate
"""

result = conn.execute(query, [str(gen_30min), solar_duids_sa1, str(prices_30min)]).df()

print(f"\nData loaded: {len(result)} records")
if len(result) > 0:
    print(f"Date range: {result['settlementdate'].min()} to {result['settlementdate'].max()}")

    # Calculate dispatch-weighted average
    total_revenue = result["revenue"].sum()
    total_energy = result["energy_mwh"].sum()
    dwa_price = total_revenue / total_energy if total_energy > 0 else 0

    print(f"\nSolar Dispatch-Weighted Average for SA1 (FY2024-25):")
    print(f"Total Solar Energy: {total_energy:,.0f} MWh")
    print(f"Total Solar Revenue: ${total_revenue:,.0f}")
    print(f"Dispatch-Weighted Average Price: ${dwa_price:.2f}/MWh")

    # Also calculate simple average for comparison
    simple_avg = result["rrp"].mean()
    print(f"\nFor comparison:")
    print(f"Simple Average Price (all periods): ${simple_avg:.2f}/MWh")
    print(f"Solar captures {(dwa_price/simple_avg)*100:.1f}% of average price")

    # Show some statistics
    print(f"\nAdditional statistics:")
    print(f"Max solar generation: {result['total_solar_mw'].max():.1f} MW")
    print(f"Average solar generation: {result['total_solar_mw'].mean():.1f} MW")
    print(f"Min price during solar generation: ${result[result['total_solar_mw'] > 0]['rrp'].min():.2f}/MWh")
    print(f"Max price during solar generation: ${result[result['total_solar_mw'] > 0]['rrp'].max():.2f}/MWh")
    
    # Show some sample data
    print(f"\nSample data (first 5 rows):")
    print(result[["settlementdate", "total_solar_mw", "rrp", "revenue", "energy_mwh"]].head())
else:
    print("No data found for the specified period")