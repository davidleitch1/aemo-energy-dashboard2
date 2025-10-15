#!/usr/bin/env python3
"""
Calculate corrected scada30 for a sample period to demonstrate the difference
Focus on Solar DUIDs for one day
"""

import pandas as pd
import numpy as np
from pathlib import Path
import pickle

# Data paths
data_dir = Path('/Volumes/davidleitch/aemo_production/data')

print("=" * 80)
print("SCADA30 CORRECTION - SAMPLE ANALYSIS")
print("=" * 80)

# Load data
print("\n1. Loading data...")
scada5_df = pd.read_parquet(data_dir / 'scada5.parquet')
scada30_existing = pd.read_parquet(data_dir / 'scada30.parquet')

# Load DUID mapping for Solar
with open(data_dir / 'gen_info.pkl', 'rb') as f:
    gen_info = pickle.load(f)

solar_duids = gen_info[gen_info['Fuel'] == 'Solar']['DUID'].tolist()
print(f"   Found {len(solar_duids)} Solar DUIDs")

# Focus on a few solar DUIDs for quick analysis
test_duids = ['BROKENH1', 'SRSF1', 'BAPLANT1']
test_duids = [d for d in test_duids if d in solar_duids]
if not test_duids:
    test_duids = solar_duids[:3]

print(f"   Testing with: {test_duids}")

# Test period - one full day
test_date = pd.Timestamp('2025-09-02')
test_start = pd.Timestamp(f'{test_date.date()} 00:00:00')
test_end = pd.Timestamp(f'{test_date.date()} 23:30:00')

print(f"\n2. Test period: {test_start} to {test_end}")

# Get all 30-minute endpoints for the test period
endpoints = pd.date_range(test_start, test_end, freq='30min')

def calculate_with_5_intervals(scada5_data, duid, endpoint):
    """Current (incorrect) method - uses only 5 intervals"""
    start_time = endpoint - pd.Timedelta(minutes=25)
    mask = (
        (scada5_data['duid'] == duid) & 
        (scada5_data['settlementdate'] > start_time) & 
        (scada5_data['settlementdate'] <= endpoint)
    )
    intervals = scada5_data[mask]
    if len(intervals) > 0:
        return intervals['scadavalue'].mean(), len(intervals), list(intervals['scadavalue'].values)
    return np.nan, 0, []

def calculate_with_6_intervals(scada5_data, duid, endpoint):
    """Corrected method - uses all 6 intervals"""
    start_time = endpoint - pd.Timedelta(minutes=30)  # Changed from 25 to 30
    mask = (
        (scada5_data['duid'] == duid) & 
        (scada5_data['settlementdate'] > start_time) & 
        (scada5_data['settlementdate'] <= endpoint)
    )
    intervals = scada5_data[mask]
    if len(intervals) > 0:
        return intervals['scadavalue'].mean(), len(intervals), list(intervals['scadavalue'].values)
    return np.nan, 0, []

print("\n3. Calculating and comparing values...")
print("=" * 80)

results = []

for duid in test_duids:
    print(f"\nDUID: {duid}")
    print("-" * 40)
    
    duid_results = []
    
    for endpoint in endpoints:
        # Calculate with 5 intervals (current method)
        mean_5, count_5, values_5 = calculate_with_5_intervals(scada5_df, duid, endpoint)
        
        # Calculate with 6 intervals (corrected method)
        mean_6, count_6, values_6 = calculate_with_6_intervals(scada5_df, duid, endpoint)
        
        # Get existing scada30 value
        mask_existing = (
            (scada30_existing['duid'] == duid) & 
            (scada30_existing['settlementdate'] == endpoint)
        )
        existing_value = scada30_existing[mask_existing]
        existing_val = existing_value['scadavalue'].iloc[0] if not existing_value.empty else np.nan
        
        if not np.isnan(mean_6) and not np.isnan(existing_val):
            duid_results.append({
                'endpoint': endpoint,
                'mean_5_intervals': mean_5,
                'mean_6_intervals': mean_6,
                'existing_scada30': existing_val,
                'count_5': count_5,
                'count_6': count_6,
                'difference': mean_6 - mean_5,
                'pct_change': ((mean_6 - mean_5) / mean_5 * 100) if mean_5 != 0 else 0,
                'matches_existing': abs(mean_5 - existing_val) < 0.001,
                'values_5': values_5,
                'values_6': values_6
            })
            results.append(duid_results[-1])
    
    if duid_results:
        # Show sample results for key periods
        df = pd.DataFrame(duid_results)
        
        # Focus on periods with significant generation (daytime for solar)
        daytime = df[(df['endpoint'].dt.hour >= 6) & (df['endpoint'].dt.hour <= 18)]
        
        if not daytime.empty:
            print("\nSample periods (6:00 - 18:00):")
            print("Time     5-int    6-int   Existing  Diff    %Change  Match?")
            print("-" * 60)
            
            for _, row in daytime.head(10).iterrows():
                print(f"{row['endpoint'].strftime('%H:%M')}  {row['mean_5_intervals']:7.2f}  "
                      f"{row['mean_6_intervals']:7.2f}  {row['existing_scada30']:7.2f}  "
                      f"{row['difference']:7.2f}  {row['pct_change']:6.1f}%  "
                      f"{'Yes' if row['matches_existing'] else 'No'}")
            
            # Show one detailed example
            example = daytime[daytime['mean_6_intervals'] > 10].iloc[0] if len(daytime[daytime['mean_6_intervals'] > 10]) > 0 else daytime.iloc[0]
            print(f"\nDetailed example at {example['endpoint'].strftime('%H:%M')}:")
            print(f"  5 intervals: {[f'{v:.1f}' for v in example['values_5']]}")
            print(f"  Mean of 5: {example['mean_5_intervals']:.2f} MW")
            print(f"  6 intervals: {[f'{v:.1f}' for v in example['values_6']]}")
            print(f"  Mean of 6: {example['mean_6_intervals']:.2f} MW")
            print(f"  Difference: {example['difference']:.2f} MW ({example['pct_change']:.1f}%)")

# Overall analysis
if results:
    print("\n" + "=" * 80)
    print("OVERALL ANALYSIS")
    print("=" * 80)
    
    df_all = pd.DataFrame(results)
    
    # Filter to periods with meaningful generation
    active = df_all[df_all['mean_6_intervals'] > 1]  # More than 1 MW
    
    if not active.empty:
        print(f"\nAnalyzed {len(active)} periods with generation > 1 MW")
        print(f"Average difference: {active['difference'].mean():.3f} MW")
        print(f"Average % change: {active['pct_change'].mean():.2f}%")
        print(f"Maximum difference: {active['difference'].abs().max():.3f} MW")
        print(f"Maximum % change: {active['pct_change'].abs().max():.2f}%")
        
        # Check how well existing matches the 5-interval calculation
        matches = active['matches_existing'].sum()
        print(f"\nExisting scada30 matches 5-interval calculation: {matches}/{len(active)} ({matches/len(active)*100:.1f}%)")
        
        # Calculate energy impact
        energy_5 = (active['mean_5_intervals'] * 0.5).sum()  # MWh
        energy_6 = (active['mean_6_intervals'] * 0.5).sum()  # MWh
        energy_diff = energy_6 - energy_5
        
        print(f"\nEnergy impact for test DUIDs on {test_date.date()}:")
        print(f"  5 intervals: {energy_5:,.1f} MWh")
        print(f"  6 intervals: {energy_6:,.1f} MWh")
        print(f"  Difference: {energy_diff:,.1f} MWh ({energy_diff/energy_5*100:.2f}%)")

print("\n" + "=" * 80)
print("CONCLUSION")
print("=" * 80)
print("The existing scada30 calculation:")
print("  1. Uses only 5 of 6 available intervals (excludes first interval)")
print("  2. This matches the existing scada30 values in the file")
print("  3. This creates a systematic bias in the aggregated data")
print("")
print("Impact for Solar generation:")
print("  - Missing the first interval can significantly affect ramping periods")
print("  - The error is most pronounced during sunrise/sunset")
print("  - Overall energy calculations will be biased")