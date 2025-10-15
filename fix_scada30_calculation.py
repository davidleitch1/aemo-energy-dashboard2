#!/usr/bin/env python3
"""
Correctly calculate scada30 from scada5 using all 6 intervals
and compare with existing (incorrect) scada30 values
"""

import pandas as pd
import numpy as np
from pathlib import Path
import pickle
from datetime import datetime
import time

# Data paths
data_dir = Path('/Volumes/davidleitch/aemo_production/data')

print("=" * 80)
print("FIXING SCADA30 CALCULATION")
print("=" * 80)

# Load scada5 data
print("\n1. Loading scada5 data...")
scada5_df = pd.read_parquet(data_dir / 'scada5.parquet')
print(f"   Loaded {len(scada5_df):,} records")
print(f"   Date range: {scada5_df['settlementdate'].min()} to {scada5_df['settlementdate'].max()}")

# Load existing scada30 for comparison
print("\n2. Loading existing scada30 data...")
scada30_existing = pd.read_parquet(data_dir / 'scada30.parquet')
print(f"   Loaded {len(scada30_existing):,} records")

# Get unique DUIDs from scada5
unique_duids = scada5_df['duid'].unique()
print(f"\n3. Found {len(unique_duids)} unique DUIDs in scada5")

# Define all 30-minute endpoints in the scada5 date range
start_date = scada5_df['settlementdate'].min()
end_date = scada5_df['settlementdate'].max()

# Round start to next 30-minute boundary
if start_date.minute not in [0, 30]:
    if start_date.minute < 30:
        start_date = start_date.replace(minute=30, second=0, microsecond=0)
    else:
        start_date = (start_date + pd.Timedelta(hours=1)).replace(minute=0, second=0, microsecond=0)

# Generate all 30-minute endpoints
endpoints = pd.date_range(start=start_date, end=end_date, freq='30min')
print(f"\n4. Processing {len(endpoints)} 30-minute endpoints")

def calculate_correct_30min(scada5_data, duid, endpoint):
    """
    Correctly calculate 30-minute value using ALL 6 intervals
    The correct window should include 6 intervals spanning full 30 minutes
    """
    # For endpoint at HH:00, include: HH-1:35, HH-1:40, HH-1:45, HH-1:50, HH-1:55, HH:00
    # For endpoint at HH:30, include: HH:05, HH:10, HH:15, HH:20, HH:25, HH:30
    
    # The correct window: > (endpoint - 30 minutes) AND <= endpoint
    start_time = endpoint - pd.Timedelta(minutes=30)
    
    # Get all intervals in the 30-minute window
    mask = (
        (scada5_data['duid'] == duid) & 
        (scada5_data['settlementdate'] > start_time) & 
        (scada5_data['settlementdate'] <= endpoint)
    )
    intervals = scada5_data[mask]
    
    if len(intervals) > 0:
        # Calculate mean of all available intervals
        return intervals['scadavalue'].mean(), len(intervals)
    else:
        return np.nan, 0

# Process in batches to manage memory
print("\n5. Calculating corrected scada30 values...")
batch_size = 100  # Process 100 endpoints at a time
all_results = []
total_processed = 0

start_time = time.time()

for batch_start in range(0, len(endpoints), batch_size):
    batch_end = min(batch_start + batch_size, len(endpoints))
    batch_endpoints = endpoints[batch_start:batch_end]
    
    # Show progress
    if batch_start % 1000 == 0:
        elapsed = time.time() - start_time
        rate = total_processed / elapsed if elapsed > 0 else 0
        remaining = (len(endpoints) - batch_start) / rate if rate > 0 else 0
        print(f"   Processing endpoints {batch_start+1}-{batch_end}/{len(endpoints)} "
              f"({batch_start/len(endpoints)*100:.1f}%) - "
              f"Est. remaining: {remaining/60:.1f} min")
    
    batch_results = []
    
    for endpoint in batch_endpoints:
        # Process each DUID for this endpoint
        for duid in unique_duids:
            value, num_intervals = calculate_correct_30min(scada5_df, duid, endpoint)
            
            if not np.isnan(value):
                batch_results.append({
                    'settlementdate': endpoint,
                    'duid': duid,
                    'scadavalue': value,
                    'num_intervals': num_intervals
                })
        
        total_processed += 1
    
    if batch_results:
        batch_df = pd.DataFrame(batch_results)
        all_results.append(batch_df)

print(f"\n   Total calculation time: {(time.time() - start_time)/60:.1f} minutes")

# Combine all results
print("\n6. Combining results...")
if all_results:
    scada30_fixed = pd.concat(all_results, ignore_index=True)
    scada30_fixed = scada30_fixed.drop_duplicates(subset=['settlementdate', 'duid'])
    scada30_fixed = scada30_fixed.sort_values(['settlementdate', 'duid'])
    
    # Drop the num_intervals column before saving
    scada30_fixed_save = scada30_fixed[['settlementdate', 'duid', 'scadavalue']]
    
    print(f"   Created {len(scada30_fixed):,} corrected scada30 records")
    
    # Save the corrected data
    output_file = data_dir / 'scada30_fixed.parquet'
    print(f"\n7. Saving corrected data to {output_file}")
    scada30_fixed_save.to_parquet(output_file, compression='snappy', index=False)
    print("   ✓ Saved successfully")
    
    # Analyze interval counts
    interval_counts = scada30_fixed['num_intervals'].value_counts().sort_index()
    print("\n8. Interval count analysis:")
    for num, count in interval_counts.items():
        print(f"   {num} intervals: {count:,} records ({count/len(scada30_fixed)*100:.1f}%)")
    
else:
    print("   ERROR: No results generated")
    scada30_fixed = pd.DataFrame()

# COMPARISON WITH EXISTING SCADA30
print("\n" + "=" * 80)
print("COMPARISON: FIXED vs EXISTING SCADA30")
print("=" * 80)

if not scada30_fixed.empty:
    # Load DUID mapping to identify solar generators
    print("\n1. Loading DUID mapping...")
    with open(data_dir / 'gen_info.pkl', 'rb') as f:
        gen_info = pickle.load(f)
    
    # Get solar DUIDs
    solar_duids = gen_info[gen_info['Fuel'] == 'Solar']['DUID'].tolist()
    print(f"   Found {len(solar_duids)} Solar DUIDs")
    
    # Choose a solar DUID with good data
    test_duid = None
    for duid in ['BROKENH1', 'SRSF1', 'BAPLANT1'] + solar_duids[:5]:
        if duid in scada30_fixed['duid'].values:
            test_duid = duid
            break
    
    if test_duid:
        print(f"\n2. Comparing values for Solar DUID: {test_duid}")
        
        # Get a sample time period for comparison
        test_date = pd.Timestamp('2025-09-02')
        test_start = pd.Timestamp(f'{test_date.date()} 06:00:00')  # Sunrise
        test_end = pd.Timestamp(f'{test_date.date()} 18:00:00')    # Sunset
        
        # Get fixed values
        mask_fixed = (
            (scada30_fixed['duid'] == test_duid) &
            (scada30_fixed['settlementdate'] >= test_start) &
            (scada30_fixed['settlementdate'] <= test_end)
        )
        fixed_data = scada30_fixed[mask_fixed].set_index('settlementdate')
        
        # Get existing values
        mask_existing = (
            (scada30_existing['duid'] == test_duid) &
            (scada30_existing['settlementdate'] >= test_start) &
            (scada30_existing['settlementdate'] <= test_end)
        )
        existing_data = scada30_existing[mask_existing].set_index('settlementdate')
        
        # Merge for comparison
        comparison = pd.DataFrame({
            'fixed': fixed_data['scadavalue'],
            'existing': existing_data['scadavalue'],
            'intervals': fixed_data['num_intervals']
        }).dropna()
        
        if not comparison.empty:
            comparison['difference'] = comparison['fixed'] - comparison['existing']
            comparison['pct_change'] = (comparison['difference'] / comparison['existing'] * 100).replace([np.inf, -np.inf], 0)
            
            print(f"\n3. Detailed comparison for {test_date.date()}:")
            print("-" * 60)
            print("Time         Fixed    Existing  Diff     %Change  Intervals")
            print("-" * 60)
            
            for timestamp, row in comparison.iterrows():
                print(f"{timestamp.strftime('%H:%M')}  {row['fixed']:8.2f} {row['existing']:8.2f} "
                      f"{row['difference']:8.2f} {row['pct_change']:7.1f}%  {int(row['intervals'])}")
            
            # Summary statistics
            print("\n4. Summary Statistics:")
            print("-" * 40)
            print(f"   Average fixed value: {comparison['fixed'].mean():.2f} MW")
            print(f"   Average existing value: {comparison['existing'].mean():.2f} MW")
            print(f"   Average difference: {comparison['difference'].mean():.2f} MW")
            print(f"   Average % change: {comparison['pct_change'].mean():.1f}%")
            print(f"   Max absolute difference: {comparison['difference'].abs().max():.2f} MW")
            print(f"   Max % change: {comparison['pct_change'].abs().max():.1f}%")
            
            # Check if differences are significant
            significant_diffs = comparison[comparison['difference'].abs() > 0.01]
            print(f"\n   Periods with significant difference (>0.01 MW): {len(significant_diffs)}/{len(comparison)}")
            
            # Calculate total energy difference for the day
            total_energy_fixed = (comparison['fixed'] * 0.5).sum()  # MWh
            total_energy_existing = (comparison['existing'] * 0.5).sum()  # MWh
            energy_difference = total_energy_fixed - total_energy_existing
            
            print(f"\n5. Energy Impact for {test_duid} on {test_date.date()}:")
            print("-" * 40)
            print(f"   Total energy (fixed): {total_energy_fixed:,.1f} MWh")
            print(f"   Total energy (existing): {total_energy_existing:,.1f} MWh")
            print(f"   Energy difference: {energy_difference:,.1f} MWh ({energy_difference/total_energy_existing*100:.1f}%)")
            
            print("\n6. CONCLUSION:")
            print("=" * 60)
            if comparison['difference'].abs().mean() > 0.01:
                print("⚠️ SIGNIFICANT DIFFERENCES FOUND")
                print(f"   The existing scada30 values are systematically biased")
                print(f"   by excluding the first interval of each 30-minute period.")
                print(f"   For {test_duid}, this creates an average error of {comparison['pct_change'].mean():.1f}%")
            else:
                print("✓ Values are very close (differences < 0.01 MW)")
                print("   This could happen if generation is very stable")
        else:
            print("   No overlapping data found for comparison")
    else:
        print("   Could not find suitable Solar DUID for comparison")
else:
    print("   No fixed data available for comparison")

print("\n" + "=" * 80)
print("SCRIPT COMPLETE")
print("Fixed scada30 data saved to: scada30_fixed.parquet")
print("=" * 80)