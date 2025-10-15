#!/usr/bin/env python3
"""
Quick comparison of 5 vs 6 interval calculation for a single Solar DUID
"""

import pandas as pd
import numpy as np
from pathlib import Path

# Data paths
data_dir = Path('/Volumes/davidleitch/aemo_production/data')

print("QUICK SCADA30 COMPARISON - 5 vs 6 INTERVALS")
print("=" * 60)

# Load data
scada5_df = pd.read_parquet(data_dir / 'scada5.parquet')
scada30_existing = pd.read_parquet(data_dir / 'scada30.parquet')

# Use a specific Solar DUID and time
test_duid = 'BROKENH1'  # Solar farm
test_endpoints = [
    pd.Timestamp('2025-09-02 07:00:00'),  # Early morning
    pd.Timestamp('2025-09-02 12:00:00'),  # Midday
    pd.Timestamp('2025-09-02 17:00:00'),  # Late afternoon
]

print(f"\nTesting DUID: {test_duid} (Solar)")
print("-" * 60)

for endpoint in test_endpoints:
    print(f"\n{endpoint} Analysis:")
    print("-" * 40)
    
    # Method 1: 5 intervals (current/incorrect)
    start_5 = endpoint - pd.Timedelta(minutes=25)
    mask_5 = (
        (scada5_df['duid'] == test_duid) & 
        (scada5_df['settlementdate'] > start_5) & 
        (scada5_df['settlementdate'] <= endpoint)
    )
    data_5 = scada5_df[mask_5].sort_values('settlementdate')
    
    # Method 2: 6 intervals (correct)
    start_6 = endpoint - pd.Timedelta(minutes=30)
    mask_6 = (
        (scada5_df['duid'] == test_duid) & 
        (scada5_df['settlementdate'] > start_6) & 
        (scada5_df['settlementdate'] <= endpoint)
    )
    data_6 = scada5_df[mask_6].sort_values('settlementdate')
    
    # Get existing scada30
    mask_existing = (
        (scada30_existing['duid'] == test_duid) & 
        (scada30_existing['settlementdate'] == endpoint)
    )
    existing = scada30_existing[mask_existing]
    
    # Show the data
    print("\n6-interval data (CORRECT):")
    for _, row in data_6.iterrows():
        in_5 = row['settlementdate'] in data_5['settlementdate'].values
        marker = "←" if not in_5 else " "
        print(f"  {row['settlementdate'].strftime('%H:%M')}: {row['scadavalue']:7.2f} MW {marker}")
    
    if len(data_6) > 0:
        mean_6 = data_6['scadavalue'].mean()
        print(f"  Mean (6 intervals): {mean_6:.2f} MW")
    
    if len(data_5) > 0:
        mean_5 = data_5['scadavalue'].mean()
        print(f"\n5-interval mean: {mean_5:.2f} MW")
    
    if not existing.empty:
        existing_val = existing['scadavalue'].iloc[0]
        print(f"Existing scada30: {existing_val:.2f} MW")
        
        if len(data_5) > 0:
            match_5 = abs(mean_5 - existing_val) < 0.001
            print(f"Existing matches 5-interval? {match_5}")
    
    if len(data_5) > 0 and len(data_6) > 0:
        diff = mean_6 - mean_5
        pct = (diff / mean_5 * 100) if mean_5 != 0 else 0
        print(f"\nDifference: {diff:.2f} MW ({pct:+.1f}%)")
        
        # Show the missing interval explicitly
        missing_intervals = data_6[~data_6['settlementdate'].isin(data_5['settlementdate'])]
        if not missing_intervals.empty:
            print(f"Missing interval value: {missing_intervals['scadavalue'].iloc[0]:.2f} MW")

print("\n" + "=" * 60)
print("SUMMARY:")
print("The existing scada30 uses only 5 intervals (missing the first)")
print("This creates a systematic bias in the aggregated data")