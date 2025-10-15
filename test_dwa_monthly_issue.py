#!/usr/bin/env python3
"""Trace monthly DWA calculation issue - why does monthly show $97 instead of $55?"""

import pandas as pd
import duckdb
from pathlib import Path
import pickle

# Set up paths
data_dir = Path('/Volumes/davidleitch/aemo_production/data')

# Load DUID mapping
print("Loading DUID mapping...")
with open(data_dir / 'gen_info.pkl', 'rb') as f:
    duid_mapping = pickle.load(f)

# Get Solar DUIDs in SA1
solar_duids = duid_mapping[(duid_mapping['Fuel'] == 'Solar') & (duid_mapping['Region'] == 'SA1')]['DUID'].tolist()
print(f"Found {len(solar_duids)} Solar DUIDs in SA1")

# Connect to DuckDB
conn = duckdb.connect()

# First, get the CORRECT 30-minute DWA as baseline
print("\n=== BASELINE: 30-MINUTE DWA (CORRECT) ===")
query_30min = """
SELECT 
    g.settlementdate,
    SUM(g.scadavalue) as total_solar_mw,
    p.rrp
FROM read_parquet(?) g
JOIN read_parquet(?) p 
    ON g.settlementdate = p.settlementdate 
    AND p.regionid = 'SA1'
WHERE g.settlementdate >= '2024-07-01'
  AND g.settlementdate <= '2025-06-30 23:30:00'
  AND g.duid = ANY(?)
GROUP BY g.settlementdate, p.rrp
ORDER BY g.settlementdate
"""

result_30min = conn.execute(query_30min, [
    str(data_dir / 'scada30.parquet'),
    str(data_dir / 'prices30.parquet'),
    solar_duids
]).df()

print(f"30-min records: {len(result_30min)}")

# Calculate correct DWA
revenue_30min = (result_30min['total_solar_mw'] * result_30min['rrp'] * 0.5).sum()
energy_30min = (result_30min['total_solar_mw'] * 0.5).sum()
dwa_30min = revenue_30min / energy_30min if energy_30min > 0 else 0

print(f"Total Energy: {energy_30min:,.0f} MWh")
print(f"Total Revenue: ${revenue_30min:,.0f}")
print(f"DWA (30-min): ${dwa_30min:.2f}/MWh")

# Now simulate what the dashboard does with monthly frequency
print("\n=== SIMULATING DASHBOARD MONTHLY LOGIC ===")

# Step 1: Resample generation to monthly (SUM)
result_30min['month'] = pd.to_datetime(result_30min['settlementdate']).dt.to_period('M')
gen_monthly = result_30min.groupby('month').agg({
    'total_solar_mw': 'sum'  # Sum of MW values for the month
}).reset_index()

print(f"\nGeneration resampled to monthly:")
print(f"Records: {len(gen_monthly)}")
print(f"Total MW (summed): {gen_monthly['total_solar_mw'].sum():,.0f}")
print("First 3 months:")
print(gen_monthly.head(3))

# Step 2: Resample prices to monthly (MEAN)
price_monthly = result_30min.groupby('month').agg({
    'rrp': 'mean'  # Average price for the month
}).reset_index()

print(f"\nPrices resampled to monthly:")
print(f"Records: {len(price_monthly)}")
print("First 3 months:")
print(price_monthly.head(3))

# Step 3: Merge monthly data
merged_monthly = pd.merge(gen_monthly, price_monthly, on='month')

print(f"\nMerged monthly data:")
print(f"Records: {len(merged_monthly)}")
print("First 3 months:")
print(merged_monthly.head(3))

# Step 4: Calculate DWA (WRONG WAY - likely what dashboard is doing)
print("\n=== WRONG CALCULATION (Dashboard behavior) ===")

# Dashboard might be using hours_per_period incorrectly
# For monthly data, it might think each row represents one month
# But the MW values are already summed for all 30-min periods

# Wrong way 1: Treating summed MW as if it needs 0.5 hour conversion
revenue_wrong1 = (merged_monthly['total_solar_mw'] * merged_monthly['rrp'] * 0.5).sum()
energy_wrong1 = (merged_monthly['total_solar_mw'] * 0.5).sum()
dwa_wrong1 = revenue_wrong1 / energy_wrong1 if energy_wrong1 > 0 else 0
print(f"Wrong DWA (0.5 hours): ${dwa_wrong1:.2f}/MWh")

# Wrong way 2: Treating each month as 720 hours (30 days * 24 hours)
revenue_wrong2 = (merged_monthly['total_solar_mw'] * merged_monthly['rrp'] * 720).sum()
energy_wrong2 = (merged_monthly['total_solar_mw'] * 720).sum()
dwa_wrong2 = revenue_wrong2 / energy_wrong2 if energy_wrong2 > 0 else 0
print(f"Wrong DWA (720 hours): ${dwa_wrong2:.2f}/MWh")

# Wrong way 3: Not using any hours conversion
revenue_wrong3 = (merged_monthly['total_solar_mw'] * merged_monthly['rrp']).sum()
energy_wrong3 = merged_monthly['total_solar_mw'].sum()
dwa_wrong3 = revenue_wrong3 / energy_wrong3 if energy_wrong3 > 0 else 0
print(f"Wrong DWA (no hours): ${dwa_wrong3:.2f}/MWh")

print("\n=== CORRECT CALCULATION FOR MONTHLY ===")

# The CORRECT way: Calculate weighted average preserving the original calculation
# We need to track the original revenue and energy

# Go back to 30-min data and calculate monthly weighted averages properly
monthly_correct = result_30min.groupby('month').apply(
    lambda x: pd.Series({
        'total_generation_mwh': (x['total_solar_mw'] * 0.5).sum(),
        'total_revenue': (x['total_solar_mw'] * x['rrp'] * 0.5).sum(),
        'weighted_avg_price': (x['total_solar_mw'] * x['rrp']).sum() / x['total_solar_mw'].sum() if x['total_solar_mw'].sum() > 0 else 0,
        'periods': len(x)
    })
).reset_index()

print(f"Correctly aggregated monthly data:")
print(monthly_correct.head())

# Calculate DWA from monthly aggregated data
total_revenue = monthly_correct['total_revenue'].sum()
total_energy = monthly_correct['total_generation_mwh'].sum()
dwa_correct = total_revenue / total_energy if total_energy > 0 else 0

print(f"\nTotal Energy (MWh): {total_energy:,.0f}")
print(f"Total Revenue: ${total_revenue:,.0f}")
print(f"DWA (monthly correct): ${dwa_correct:.2f}/MWh")

print("\n=== SUMMARY ===")
print(f"30-min DWA (baseline): ${dwa_30min:.2f}/MWh")
print(f"Monthly DWA (correct): ${dwa_correct:.2f}/MWh")
print(f"Difference: ${abs(dwa_correct - dwa_30min):.2f}")
print(f"Match: {abs(dwa_correct - dwa_30min) < 0.01}")