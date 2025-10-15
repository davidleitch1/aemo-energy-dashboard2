#!/usr/bin/env python3
"""Test the proper fix for DWA calculation with aggregated frequencies"""

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

# Get 30-minute data
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

print(f"30-min baseline records: {len(result_30min)}")

# Calculate correct DWA at 30-min level
revenue_30min = (result_30min['total_solar_mw'] * result_30min['rrp'] * 0.5).sum()
energy_30min = (result_30min['total_solar_mw'] * 0.5).sum()
dwa_30min = revenue_30min / energy_30min if energy_30min > 0 else 0
print(f"Baseline DWA (30-min): ${dwa_30min:.2f}/MWh")

def calculate_dwa_for_frequency(data, freq_code):
    """
    Calculate DWA for different frequency aggregations
    This is the FIXED version that should be used in the dashboard
    """
    
    # Convert to datetime and set period
    data = data.copy()
    data['settlementdate'] = pd.to_datetime(data['settlementdate'])
    
    if freq_code in ['D', 'M', 'Q', 'Y']:
        # For aggregated frequencies, we need to properly weight the prices
        # We cannot use simple mean of RRP - must track revenue and energy
        
        # Group by the period
        if freq_code == 'D':
            data['period'] = data['settlementdate'].dt.date
        elif freq_code == 'M':
            data['period'] = data['settlementdate'].dt.to_period('M')
        elif freq_code == 'Q':
            data['period'] = data['settlementdate'].dt.to_period('Q')
        elif freq_code == 'Y':
            data['period'] = data['settlementdate'].dt.to_period('Y')
        
        # For each period, calculate total revenue and total energy
        period_stats = data.groupby('period').apply(
            lambda x: pd.Series({
                'total_mw': x['total_solar_mw'].sum(),
                'total_revenue': (x['total_solar_mw'] * x['rrp'] * 0.5).sum(),
                'total_energy_mwh': (x['total_solar_mw'] * 0.5).sum(),
                'weighted_avg_price': (x['total_solar_mw'] * x['rrp']).sum() / x['total_solar_mw'].sum() if x['total_solar_mw'].sum() > 0 else 0
            })
        ).reset_index()
        
        # Now we have period-aggregated data
        # Calculate DWA from the aggregated revenue and energy
        total_revenue = period_stats['total_revenue'].sum()
        total_energy = period_stats['total_energy_mwh'].sum()
        dwa = total_revenue / total_energy if total_energy > 0 else 0
        
        return dwa, period_stats
    
    else:
        # For non-aggregated frequencies (5min, 30min, hourly)
        # Simple calculation
        revenue = (data['total_solar_mw'] * data['rrp'] * 0.5).sum()
        energy = (data['total_solar_mw'] * 0.5).sum()
        dwa = revenue / energy if energy > 0 else 0
        return dwa, None

# Test different frequencies
print("\n=== TESTING FIXED DWA CALCULATION ===")

frequencies = ['D', 'M', 'Q', 'Y']
freq_names = {'D': 'Daily', 'M': 'Monthly', 'Q': 'Quarterly', 'Y': 'Yearly'}

for freq in frequencies:
    dwa, period_data = calculate_dwa_for_frequency(result_30min, freq)
    print(f"{freq_names[freq]} DWA: ${dwa:.2f}/MWh (diff from baseline: ${abs(dwa - dwa_30min):.2f})")
    if period_data is not None and freq == 'M':
        print(f"  Periods: {len(period_data)}")
        print("  First 3 months:")
        print(period_data[['period', 'total_mw', 'weighted_avg_price', 'total_energy_mwh', 'total_revenue']].head(3).to_string(index=False))

print("\n=== SUMMARY ===")
print("All frequencies should produce the same DWA (~$55.54/MWh)")
print("The key insight: For aggregated data, we must track total revenue and energy,")
print("not use simple average prices multiplied by summed generation.")