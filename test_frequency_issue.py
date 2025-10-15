#!/usr/bin/env python3
"""Demonstrate the frequency mismatch issue in DWA calculation"""

import pandas as pd
import numpy as np

# Create sample 30-minute data
dates = pd.date_range('2024-07-01', '2024-07-01 06:00:00', freq='30min')
print(f"Original 30-minute timestamps: {len(dates)} records")
for d in dates[:10]:
    print(f"  {d}")

# Sample generation data (30-min) - varies by time
generation_30min = pd.DataFrame({
    'SETTLEMENTDATE': dates,
    'SCADAVALUE': [0, 0, 0, 0, 0, 0, 0, 0,  # Night: 00:00-04:00
                   10, 20,  # Dawn: 04:00-05:00
                   30, 40,  # Morning: 05:00-06:00
                   50]      # 06:00
})

# Sample price data (30-min)
price_30min = pd.DataFrame({
    'SETTLEMENTDATE': dates,
    'RRP': [100, 110, 90, 95, 80, 85, 70, 75,  # Night prices
            60, 65,   # Dawn
            50, 55,   # Morning
            45]       # 06:00
})

print("\n30-minute data:")
print(pd.merge(generation_30min, price_30min, on='SETTLEMENTDATE'))

# Calculate correct DWA with 30-min data
merged_30min = pd.merge(generation_30min, price_30min, on='SETTLEMENTDATE')
revenue_30min = (merged_30min['SCADAVALUE'] * merged_30min['RRP'] * 0.5).sum()
energy_30min = (merged_30min['SCADAVALUE'] * 0.5).sum()
dwa_30min = revenue_30min / energy_30min if energy_30min > 0 else 0

print(f"\n30-minute DWA calculation:")
print(f"  Revenue: ${revenue_30min:.2f}")
print(f"  Energy: {energy_30min:.2f} MWh")
print(f"  DWA: ${dwa_30min:.2f}/MWh")

# Now simulate what happens with 1-hour frequency
# Resample price to 1-hour (averaging)
price_1hour = price_30min.set_index('SETTLEMENTDATE').resample('h')['RRP'].mean().reset_index()
print(f"\n1-hour resampled prices: {len(price_1hour)} records")
print(price_1hour)

# Generation stays at 30-min
# Try to merge 30-min generation with 1-hour prices
merged_incorrect = pd.merge(generation_30min, price_1hour, on='SETTLEMENTDATE', how='inner')
print(f"\nMerge result (INCORRECT): {len(merged_incorrect)} records (should be 13, got {len(merged_incorrect)})")
print(merged_incorrect)

# Calculate incorrect DWA (missing data!)
if len(merged_incorrect) > 0:
    # Note: hours_per_period would be detected as 1.0 now
    revenue_incorrect = (merged_incorrect['SCADAVALUE'] * merged_incorrect['RRP'] * 1.0).sum()
    energy_incorrect = (merged_incorrect['SCADAVALUE'] * 1.0).sum()
    dwa_incorrect = revenue_incorrect / energy_incorrect if energy_incorrect > 0 else 0
    
    print(f"\n1-hour INCORRECT DWA calculation:")
    print(f"  Revenue: ${revenue_incorrect:.2f}")
    print(f"  Energy: {energy_incorrect:.2f} MWh")
    print(f"  DWA: ${dwa_incorrect:.2f}/MWh")
    print(f"  Error: {((dwa_incorrect - dwa_30min) / dwa_30min * 100):.1f}%")
else:
    print("\nNo data matched in merge!")

# The CORRECT way: resample both generation and price to same frequency
generation_1hour = generation_30min.set_index('SETTLEMENTDATE').resample('h')['SCADAVALUE'].sum().reset_index()
merged_correct = pd.merge(generation_1hour, price_1hour, on='SETTLEMENTDATE', how='inner')

print(f"\n1-hour CORRECT approach:")
print(f"  Generation resampled to 1-hour (summed)")
print(f"  Prices resampled to 1-hour (averaged)")
print(f"  Merged: {len(merged_correct)} records")

revenue_correct = (merged_correct['SCADAVALUE'] * merged_correct['RRP'] * 1.0).sum()
energy_correct = (merged_correct['SCADAVALUE'] * 1.0).sum()
dwa_correct = revenue_correct / energy_correct if energy_correct > 0 else 0

print(f"\n1-hour CORRECT DWA calculation:")
print(f"  Revenue: ${revenue_correct:.2f}")
print(f"  Energy: {energy_correct:.2f} MWh")
print(f"  DWA: ${dwa_correct:.2f}/MWh")
print(f"  Matches 30-min DWA: {abs(dwa_correct - dwa_30min) < 0.01}")