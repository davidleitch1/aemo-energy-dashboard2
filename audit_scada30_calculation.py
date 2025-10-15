#!/usr/bin/env python3
"""
Audit the scada30 calculation from scada5 data
Focus on Solar generation to verify aggregation is correct
"""

import pandas as pd
import numpy as np
from pathlib import Path
import pickle
from datetime import datetime, timedelta

# Data paths
data_dir = Path('/Volumes/davidleitch/aemo_production/data')

print("=" * 80)
print("SCADA30 CALCULATION AUDIT - SOLAR GENERATION")
print("=" * 80)

# Load DUID mapping to identify solar generators
print("\n1. Loading DUID mapping...")
with open(data_dir / 'gen_info.pkl', 'rb') as f:
    gen_info = pickle.load(f)

# Get all solar DUIDs
solar_duids = gen_info[gen_info['Fuel'] == 'Solar']['DUID'].tolist()
print(f"   Found {len(solar_duids)} Solar DUIDs")

# Let's focus on a few specific solar farms for testing
test_duids = ['SRSF1', 'BAPLANT1', 'BROKENH1']  # Sample solar farms
test_duids = [d for d in test_duids if d in solar_duids]
if not test_duids:
    test_duids = solar_duids[:3]  # Use first 3 if our samples don't exist
    
print(f"   Testing with DUIDs: {test_duids}")

# Load the data files
print("\n2. Loading data files...")
print("   Loading scada5.parquet...")
scada5_df = pd.read_parquet(data_dir / 'scada5.parquet')
print(f"   Loaded {len(scada5_df):,} records")

print("   Loading scada30.parquet...")
scada30_df = pd.read_parquet(data_dir / 'scada30.parquet')
print(f"   Loaded {len(scada30_df):,} records")

# Check date ranges
print("\n3. Data date ranges:")
print(f"   scada5:  {scada5_df['settlementdate'].min()} to {scada5_df['settlementdate'].max()}")
print(f"   scada30: {scada30_df['settlementdate'].min()} to {scada30_df['settlementdate'].max()}")

# Select a test period - let's use a recent day with good solar generation
test_date = pd.Timestamp('2025-09-02')  # Recent sunny day
test_start = pd.Timestamp(f'{test_date.date()} 09:00:00')  # Start of solar generation
test_end = pd.Timestamp(f'{test_date.date()} 15:00:00')    # Peak solar hours

print(f"\n4. Testing period: {test_start} to {test_end}")

# Function to replicate the calculation logic from the collector
def calculate_30min_from_5min(scada5_data, duid, end_time):
    """
    Replicate the collector's logic:
    - Take mean of available 5-minute intervals in 30-minute window
    """
    start_time = end_time - pd.Timedelta(minutes=25)
    
    # Get all intervals for this DUID in the 30-minute window
    mask = (
        (scada5_data['duid'] == duid) & 
        (scada5_data['settlementdate'] > start_time) & 
        (scada5_data['settlementdate'] <= end_time)
    )
    intervals = scada5_data[mask]
    
    if len(intervals) > 0:
        # Mean of available intervals (as per collector code)
        return intervals['scadavalue'].mean()
    else:
        return np.nan

# Test the calculation for each test DUID
print("\n5. Detailed comparison for test DUIDs:")
print("=" * 80)

mismatches = []
total_comparisons = 0
max_absolute_diff = 0
sum_absolute_diff = 0

for duid in test_duids:
    print(f"\nDUID: {duid}")
    print("-" * 40)
    
    # Get 30-minute endpoints in test period
    endpoints = pd.date_range(test_start, test_end, freq='30min')
    
    comparison_data = []
    
    for endpoint in endpoints:
        # Get actual scada30 value
        mask_30 = (
            (scada30_df['duid'] == duid) & 
            (scada30_df['settlementdate'] == endpoint)
        )
        actual_30 = scada30_df[mask_30]
        
        if not actual_30.empty:
            actual_value = actual_30['scadavalue'].iloc[0]
            
            # Calculate what it should be from 5-minute data
            calculated_value = calculate_30min_from_5min(scada5_df, duid, endpoint)
            
            if not pd.isna(calculated_value):
                # Count the 5-minute intervals available
                start_time = endpoint - pd.Timedelta(minutes=25)
                mask_5 = (
                    (scada5_df['duid'] == duid) & 
                    (scada5_df['settlementdate'] > start_time) & 
                    (scada5_df['settlementdate'] <= endpoint)
                )
                intervals = scada5_df[mask_5]
                num_intervals = len(intervals)
                
                # Calculate difference
                diff = actual_value - calculated_value
                abs_diff = abs(diff)
                
                total_comparisons += 1
                sum_absolute_diff += abs_diff
                if abs_diff > max_absolute_diff:
                    max_absolute_diff = abs_diff
                
                # Record comparison
                comparison_data.append({
                    'time': endpoint,
                    'actual': actual_value,
                    'calculated': calculated_value,
                    'difference': diff,
                    'abs_diff': abs_diff,
                    'intervals': num_intervals,
                    '5min_values': list(intervals['scadavalue'].values)
                })
                
                # Flag significant mismatches
                if abs_diff > 0.01:  # Tolerance of 0.01 MW
                    mismatches.append({
                        'duid': duid,
                        'time': endpoint,
                        'actual': actual_value,
                        'calculated': calculated_value,
                        'diff': diff,
                        'intervals': num_intervals
                    })
    
    # Show sample comparisons for this DUID
    if comparison_data:
        df = pd.DataFrame(comparison_data[:5])  # Show first 5 periods
        print(f"\nSample comparisons (first 5 periods):")
        print(df[['time', 'actual', 'calculated', 'abs_diff', 'intervals']].to_string(index=False))
        
        # Show the 5-minute values for the first period
        if comparison_data[0]['5min_values']:
            print(f"\n5-minute values for {comparison_data[0]['time']}:")
            print(f"  Values: {comparison_data[0]['5min_values']}")
            print(f"  Mean: {np.mean(comparison_data[0]['5min_values']):.3f}")

# Test the specific calculation method
print("\n6. Calculation Method Analysis:")
print("=" * 80)

# Pick a specific test case
test_duid = test_duids[0]
test_endpoint = pd.Timestamp(f'{test_date.date()} 12:00:00')  # Midday

print(f"\nDetailed analysis for {test_duid} at {test_endpoint}:")
start_time = test_endpoint - pd.Timedelta(minutes=25)

# Get the 5-minute data
mask_5 = (
    (scada5_df['duid'] == test_duid) & 
    (scada5_df['settlementdate'] > start_time) & 
    (scada5_df['settlementdate'] <= test_endpoint)
)
intervals_5min = scada5_df[mask_5].sort_values('settlementdate')

if not intervals_5min.empty:
    print(f"\n5-minute intervals ({len(intervals_5min)} values):")
    for _, row in intervals_5min.iterrows():
        print(f"  {row['settlementdate']}: {row['scadavalue']:.2f} MW")
    
    print(f"\nCalculation methods:")
    mean_value = intervals_5min['scadavalue'].mean()
    sum_value = intervals_5min['scadavalue'].sum()
    
    print(f"  Mean of intervals: {mean_value:.3f} MW")
    print(f"  Sum of intervals: {sum_value:.3f} MW")
    print(f"  Sum / 6 (if all present): {sum_value / 6:.3f} MW")
    print(f"  Sum / {len(intervals_5min)}: {sum_value / len(intervals_5min):.3f} MW")
    
    # Get actual scada30 value
    mask_30 = (
        (scada30_df['duid'] == test_duid) & 
        (scada30_df['settlementdate'] == test_endpoint)
    )
    actual_30 = scada30_df[mask_30]
    
    if not actual_30.empty:
        actual_value = actual_30['scadavalue'].iloc[0]
        print(f"\nActual scada30 value: {actual_value:.3f} MW")
        print(f"Matches mean method: {abs(actual_value - mean_value) < 0.001}")

# Summary statistics
print("\n7. Summary Statistics:")
print("=" * 80)
print(f"Total comparisons: {total_comparisons}")
print(f"Mismatches (diff > 0.01 MW): {len(mismatches)}")
if total_comparisons > 0:
    print(f"Average absolute difference: {sum_absolute_diff/total_comparisons:.6f} MW")
    print(f"Maximum absolute difference: {max_absolute_diff:.6f} MW")
    print(f"Match rate: {((total_comparisons - len(mismatches)) / total_comparisons * 100):.2f}%")

if mismatches:
    print(f"\n8. Significant Mismatches:")
    print("=" * 80)
    for m in mismatches[:5]:  # Show first 5
        print(f"  {m['duid']} at {m['time']}: Actual={m['actual']:.3f}, Calc={m['calculated']:.3f}, Diff={m['diff']:.3f} ({m['intervals']} intervals)")
else:
    print("\n✅ NO SIGNIFICANT MISMATCHES FOUND")

# Final conclusion
print("\n9. CONCLUSION:")
print("=" * 80)
print("The scada30 calculation uses the MEAN (average) of available 5-minute intervals")
print("within each 30-minute window. This is mathematically correct for aggregating")
print("power (MW) measurements across time intervals.")
print("")
print("Formula: scada30_value = MEAN(scada5_values in 30-min window)")
print("")
if len(mismatches) == 0:
    print("✅ The calculation appears to be CORRECT for Solar generation")
elif len(mismatches) < total_comparisons * 0.01:  # Less than 1% mismatches
    print("✅ The calculation is MOSTLY CORRECT with minor discrepancies")
else:
    print("⚠️ There are some discrepancies that may need investigation")