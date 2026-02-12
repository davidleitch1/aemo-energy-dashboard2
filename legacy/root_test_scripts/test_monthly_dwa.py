#!/usr/bin/env python3
"""Test monthly aggregation for Solar DWA in SA1 FY25"""

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

# First, get 30-minute data and calculate correct DWA
print("\n=== 30-MINUTE CALCULATION (BASELINE) ===")
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

# Calculate 30-min DWA
revenue_30min = (result_30min['total_solar_mw'] * result_30min['rrp'] * 0.5).sum()
energy_30min = (result_30min['total_solar_mw'] * 0.5).sum()
dwa_30min = revenue_30min / energy_30min if energy_30min > 0 else 0

print(f"Records: {len(result_30min)}")
print(f"Total Energy: {energy_30min:,.0f} MWh")
print(f"Total Revenue: ${revenue_30min:,.0f}")
print(f"DWA: ${dwa_30min:.2f}/MWh")

# Now aggregate to monthly
print("\n=== MONTHLY AGGREGATION (CORRECT METHOD) ===")

# Resample to monthly - sum generation, weighted average for price
result_30min['month'] = pd.to_datetime(result_30min['settlementdate']).dt.to_period('M')
monthly_agg = result_30min.groupby('month').apply(
    lambda x: pd.Series({
        'total_generation_mwh': (x['total_solar_mw'] * 0.5).sum(),
        'total_revenue': (x['total_solar_mw'] * x['rrp'] * 0.5).sum(),
        'weighted_avg_price': (x['total_solar_mw'] * x['rrp']).sum() / x['total_solar_mw'].sum() if x['total_solar_mw'].sum() > 0 else 0,
        'periods': len(x)
    })
).reset_index()

print(f"\nMonthly aggregated data:")
print(monthly_agg[['month', 'total_generation_mwh', 'weighted_avg_price']].to_string())

# Calculate DWA from monthly data
total_revenue_monthly = monthly_agg['total_revenue'].sum()
total_energy_monthly = monthly_agg['total_generation_mwh'].sum()
dwa_monthly = total_revenue_monthly / total_energy_monthly if total_energy_monthly > 0 else 0

print(f"\nTotal Energy (monthly sum): {total_energy_monthly:,.0f} MWh")
print(f"Total Revenue (monthly sum): ${total_revenue_monthly:,.0f}")
print(f"DWA from monthly: ${dwa_monthly:.2f}/MWh")

# Now simulate what the dashboard might be doing wrong
print("\n=== POTENTIAL DASHBOARD ERROR (SIMPLE AVERAGE) ===")

# Wrong method: simple average of monthly averages
simple_avg_monthly = monthly_agg['weighted_avg_price'].mean()
print(f"Simple average of monthly prices: ${simple_avg_monthly:.2f}/MWh")

# Another wrong method: resample both to monthly first, then merge
print("\n=== ANOTHER POTENTIAL ERROR (RESAMPLE THEN MERGE) ===")

# Resample generation to monthly
gen_monthly = result_30min.groupby('month').agg({
    'total_solar_mw': 'sum'  # Sum of 30-min values
}).reset_index()

# Resample prices to monthly
price_monthly = result_30min.groupby('month').agg({
    'rrp': 'mean'  # Average price
}).reset_index()

# Merge monthly data
merged_monthly = pd.merge(gen_monthly, price_monthly, on='month')

# Calculate with wrong hours_per_period
# Dashboard might think monthly = 1 period = 720 hours (30 days)
revenue_wrong = (merged_monthly['total_solar_mw'] * merged_monthly['rrp'] * 720).sum()
energy_wrong = (merged_monthly['total_solar_mw'] * 720).sum()
dwa_wrong = revenue_wrong / energy_wrong if energy_wrong > 0 else 0

print(f"Wrong calculation with 720 hours/month: ${dwa_wrong:.2f}/MWh")

# Or it might use 0.5 hours still (wrong!)
revenue_wrong2 = (merged_monthly['total_solar_mw'] * merged_monthly['rrp'] * 0.5).sum()
energy_wrong2 = (merged_monthly['total_solar_mw'] * 0.5).sum()
dwa_wrong2 = revenue_wrong2 / energy_wrong2 if energy_wrong2 > 0 else 0

print(f"Wrong calculation with 0.5 hours: ${dwa_wrong2:.2f}/MWh")