#!/usr/bin/env python3
"""
Thorough verification of the potential bug in scada30 calculation
Testing with multiple data samples and checking actual timestamps
"""

import pandas as pd
import numpy as np
from pathlib import Path
import pickle

# Data paths
data_dir = Path('/Volumes/davidleitch/aemo_production/data')

print("=" * 80)
print("THOROUGH VERIFICATION OF SCADA30 CALCULATION")
print("=" * 80)

# Load data
print("\n1. Loading data...")
scada5_df = pd.read_parquet(data_dir / 'scada5.parquet')
scada30_df = pd.read_parquet(data_dir / 'scada30.parquet')

# Load DUID mapping for variable sources (Wind and Solar)
with open(data_dir / 'gen_info.pkl', 'rb') as f:
    gen_info = pickle.load(f)

# Get Wind and Solar DUIDs for testing
wind_duids = gen_info[gen_info['Fuel'] == 'Wind']['DUID'].tolist()[:3]
solar_duids = gen_info[gen_info['Fuel'] == 'Solar']['DUID'].tolist()[:3]

print(f"Testing with Wind DUIDs: {wind_duids}")
print(f"Testing with Solar DUIDs: {solar_duids}")

print("\n2. UNDERSTANDING THE 5-MINUTE DATA STRUCTURE")
print("-" * 40)

# First, let's see what timestamps actually exist in the 5-minute data
sample_duid = solar_duids[0] if solar_duids else wind_duids[0]
sample_time = pd.Timestamp('2025-09-02 12:00:00')

# Get a wider window to see the pattern
wide_start = sample_time - pd.Timedelta(hours=1)
wide_end = sample_time + pd.Timedelta(hours=1)

mask = (
    (scada5_df['duid'] == sample_duid) & 
    (scada5_df['settlementdate'] >= wide_start) & 
    (scada5_df['settlementdate'] <= wide_end)
)
sample_data = scada5_df[mask].sort_values('settlementdate')

print(f"Sample 5-minute timestamps for {sample_duid} around {sample_time}:")
for _, row in sample_data.head(20).iterrows():
    print(f"  {row['settlementdate']}")

# Check the actual interval pattern
if len(sample_data) > 1:
    intervals = sample_data['settlementdate'].diff().dropna()
    unique_intervals = intervals.unique()
    print(f"\nTime differences between consecutive records:")
    for interval in unique_intervals:
        print(f"  {interval}")

print("\n3. TESTING THE 30-MINUTE WINDOW CALCULATION")
print("-" * 40)

def test_window_calculation(duid, endpoint):
    """Test different window calculations and compare with actual scada30"""
    
    # Method 1: Current code (> endpoint - 25 min)
    start_25 = endpoint - pd.Timedelta(minutes=25)
    mask_25 = (
        (scada5_df['duid'] == duid) & 
        (scada5_df['settlementdate'] > start_25) & 
        (scada5_df['settlementdate'] <= endpoint)
    )
    data_25 = scada5_df[mask_25].sort_values('settlementdate')
    
    # Method 2: Full 30 minutes (> endpoint - 30 min)
    start_30 = endpoint - pd.Timedelta(minutes=30)
    mask_30 = (
        (scada5_df['duid'] == duid) & 
        (scada5_df['settlementdate'] > start_30) & 
        (scada5_df['settlementdate'] <= endpoint)
    )
    data_30 = scada5_df[mask_30].sort_values('settlementdate')
    
    # Method 3: Inclusive start (>= endpoint - 25 min)
    mask_25_inclusive = (
        (scada5_df['duid'] == duid) & 
        (scada5_df['settlementdate'] >= start_25) & 
        (scada5_df['settlementdate'] <= endpoint)
    )
    data_25_inclusive = scada5_df[mask_25_inclusive].sort_values('settlementdate')
    
    # Get actual scada30 value
    mask_actual = (
        (scada30_df['duid'] == duid) & 
        (scada30_df['settlementdate'] == endpoint)
    )
    actual_30 = scada30_df[mask_actual]
    
    result = {
        'duid': duid,
        'endpoint': endpoint,
        'count_25min': len(data_25),
        'count_30min': len(data_30),
        'count_25min_inclusive': len(data_25_inclusive),
        'intervals_25min': list(data_25['settlementdate'].values),
        'values_25min': list(data_25['scadavalue'].values),
        'mean_25min': data_25['scadavalue'].mean() if len(data_25) > 0 else np.nan,
        'mean_30min': data_30['scadavalue'].mean() if len(data_30) > 0 else np.nan,
        'mean_25min_inclusive': data_25_inclusive['scadavalue'].mean() if len(data_25_inclusive) > 0 else np.nan,
        'actual_scada30': actual_30['scadavalue'].iloc[0] if not actual_30.empty else np.nan
    }
    
    return result

# Test multiple endpoints
test_endpoints = [
    pd.Timestamp('2025-09-02 06:30:00'),  # Early morning (sunrise for solar)
    pd.Timestamp('2025-09-02 12:00:00'),  # Midday
    pd.Timestamp('2025-09-02 12:30:00'),  # Midday + 30
    pd.Timestamp('2025-09-02 18:00:00'),  # Evening (sunset for solar)
]

print("\n4. DETAILED RESULTS FOR EACH TEST CASE")
print("-" * 40)

all_results = []

for duid in (solar_duids[:2] + wind_duids[:1]):
    for endpoint in test_endpoints:
        result = test_window_calculation(duid, endpoint)
        all_results.append(result)
        
        if not np.isnan(result['actual_scada30']):
            print(f"\n{duid} at {endpoint}:")
            print(f"  Intervals found:")
            print(f"    25-min window (>):  {result['count_25min']} intervals")
            print(f"    30-min window (>):  {result['count_30min']} intervals")
            print(f"    25-min window (>=): {result['count_25min_inclusive']} intervals")
            
            print(f"  Mean values:")
            print(f"    25-min (>):  {result['mean_25min']:.3f} MW")
            print(f"    30-min (>):  {result['mean_30min']:.3f} MW")
            print(f"    25-min (>=): {result['mean_25min_inclusive']:.3f} MW")
            print(f"    Actual scada30: {result['actual_scada30']:.3f} MW")
            
            # Which one matches?
            diff_25 = abs(result['mean_25min'] - result['actual_scada30'])
            diff_30 = abs(result['mean_30min'] - result['actual_scada30'])
            diff_25_inc = abs(result['mean_25min_inclusive'] - result['actual_scada30'])
            
            min_diff = min(diff_25, diff_30, diff_25_inc)
            if min_diff < 0.001:
                if diff_25 == min_diff:
                    print(f"  ✓ Matches 25-min window (>) - CURRENT CODE")
                elif diff_30 == min_diff:
                    print(f"  ✓ Matches 30-min window (>)")
                elif diff_25_inc == min_diff:
                    print(f"  ✓ Matches 25-min window (>=)")
            else:
                print(f"  ✗ No exact match (min diff: {min_diff:.6f})")
            
            # Show the actual intervals
            if result['count_25min'] <= 6:
                print(f"  Intervals in 25-min window:")
                for t, v in zip(result['intervals_25min'], result['values_25min']):
                    print(f"    {t}: {v:.2f} MW")

print("\n5. STATISTICAL SUMMARY")
print("-" * 40)

# Analyze all results
df_results = pd.DataFrame(all_results)
df_results = df_results[df_results['actual_scada30'].notna()]

if len(df_results) > 0:
    # Calculate which method matches best
    df_results['diff_25'] = abs(df_results['mean_25min'] - df_results['actual_scada30'])
    df_results['diff_30'] = abs(df_results['mean_30min'] - df_results['actual_scada30'])
    df_results['diff_25_inc'] = abs(df_results['mean_25min_inclusive'] - df_results['actual_scada30'])
    
    # Count matches (within 0.001 MW tolerance)
    tolerance = 0.001
    matches_25 = (df_results['diff_25'] < tolerance).sum()
    matches_30 = (df_results['diff_30'] < tolerance).sum()
    matches_25_inc = (df_results['diff_25_inc'] < tolerance).sum()
    
    print(f"Out of {len(df_results)} test cases:")
    print(f"  25-min window (>):  {matches_25} matches ({matches_25/len(df_results)*100:.1f}%)")
    print(f"  30-min window (>):  {matches_30} matches ({matches_30/len(df_results)*100:.1f}%)")
    print(f"  25-min window (>=): {matches_25_inc} matches ({matches_25_inc/len(df_results)*100:.1f}%)")
    
    # Check interval counts
    print(f"\nInterval counts:")
    print(f"  25-min window (>):  Mode = {df_results['count_25min'].mode().iloc[0] if not df_results['count_25min'].mode().empty else 'N/A'}")
    print(f"  30-min window (>):  Mode = {df_results['count_30min'].mode().iloc[0] if not df_results['count_30min'].mode().empty else 'N/A'}")
    print(f"  25-min window (>=): Mode = {df_results['count_25min_inclusive'].mode().iloc[0] if not df_results['count_25min_inclusive'].mode().empty else 'N/A'}")

print("\n6. CONCLUSION")
print("=" * 80)

if len(df_results) > 0:
    if matches_25 > matches_30 and matches_25 > matches_25_inc:
        print("✓ The current code (25-min window with >) appears to be CORRECT")
        print("  It matches the actual scada30 values in the majority of cases")
    elif matches_30 > matches_25:
        print("✗ BUG CONFIRMED: The code should use 30-min window")
    elif matches_25_inc > matches_25:
        print("✗ BUG CONFIRMED: The code should use >= instead of >")
    else:
        print("? INCONCLUSIVE: Cannot determine the correct method")
else:
    print("? No data to analyze")

print("\nNOTE: The 5-minute data may not always have exactly 6 intervals per 30 minutes")
print("due to data collection issues, but the calculation should use whatever is available.")