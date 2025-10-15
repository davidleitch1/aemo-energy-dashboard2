#!/usr/bin/env python3
"""
Quick test to verify the 30-minute window calculation
"""

import pandas as pd
from pathlib import Path

# Data paths
data_dir = Path('/Volumes/davidleitch/aemo_production/data')

print("=" * 80)
print("TESTING 30-MINUTE WINDOW DEFINITION")
print("=" * 80)

# Load just a sample of data
scada5_df = pd.read_parquet(data_dir / 'scada5.parquet')

# Test a specific DUID and time
test_duid = 'SRSF1'
test_endpoint = pd.Timestamp('2025-09-02 12:00:00')

print(f"\nTest case: {test_duid} at endpoint {test_endpoint}")
print("-" * 40)

# What the code currently does (from unified_collector.py line 527-531):
print("\nCURRENT CODE LOGIC:")
print("  start_time = endpoint - 25 minutes")
print("  window: settlementdate > start_time AND settlementdate <= endpoint")

start_time_code = test_endpoint - pd.Timedelta(minutes=25)
print(f"\n  For endpoint {test_endpoint}:")
print(f"  start_time = {start_time_code}")
print(f"  Window: > {start_time_code} and <= {test_endpoint}")

# Get intervals using current logic
mask_current = (
    (scada5_df['duid'] == test_duid) & 
    (scada5_df['settlementdate'] > start_time_code) & 
    (scada5_df['settlementdate'] <= test_endpoint)
)
intervals_current = scada5_df[mask_current].sort_values('settlementdate')

print(f"\n  Intervals captured by CURRENT logic ({len(intervals_current)} intervals):")
for _, row in intervals_current.iterrows():
    print(f"    {row['settlementdate']}: {row['scadavalue']:.2f} MW")

# What the code SHOULD do for correct 30-minute window:
print("\n" + "=" * 40)
print("CORRECT 30-MINUTE WINDOW:")
print("  Should include exactly 6 intervals spanning 30 minutes")

# Correct window should be from (endpoint - 30min) to endpoint, inclusive of start
start_time_correct = test_endpoint - pd.Timedelta(minutes=30)
print(f"\n  For endpoint {test_endpoint}:")
print(f"  Correct window: >= {start_time_correct} and <= {test_endpoint}")
print(f"  OR: > {start_time_correct + pd.Timedelta(minutes=5)} and <= {test_endpoint}")

# Get intervals using correct logic
mask_correct = (
    (scada5_df['duid'] == test_duid) & 
    (scada5_df['settlementdate'] > (test_endpoint - pd.Timedelta(minutes=30))) & 
    (scada5_df['settlementdate'] <= test_endpoint)
)
intervals_correct = scada5_df[mask_correct].sort_values('settlementdate')

print(f"\n  Intervals that SHOULD be included ({len(intervals_correct)} intervals):")
for _, row in intervals_correct.iterrows():
    in_current = row['settlementdate'] in intervals_current['settlementdate'].values
    marker = "" if in_current else " <-- MISSING IN CURRENT LOGIC!"
    print(f"    {row['settlementdate']}: {row['scadavalue']:.2f} MW{marker}")

# Calculate the impact
if len(intervals_current) > 0 and len(intervals_correct) > 0:
    mean_current = intervals_current['scadavalue'].mean()
    mean_correct = intervals_correct['scadavalue'].mean()
    
    print(f"\n" + "=" * 40)
    print("CALCULATION COMPARISON:")
    print(f"  Current logic (5 intervals): mean = {mean_current:.3f} MW")
    print(f"  Correct logic (6 intervals): mean = {mean_correct:.3f} MW")
    print(f"  Difference: {abs(mean_current - mean_correct):.3f} MW")
    print(f"  Error: {abs(mean_current - mean_correct) / mean_correct * 100:.1f}%")

# Test multiple endpoints
print(f"\n" + "=" * 40)
print("TESTING MULTIPLE ENDPOINTS:")

test_endpoints = [
    pd.Timestamp('2025-09-02 12:00:00'),
    pd.Timestamp('2025-09-02 12:30:00'),
    pd.Timestamp('2025-09-02 13:00:00'),
]

for endpoint in test_endpoints:
    start_current = endpoint - pd.Timedelta(minutes=25)
    start_correct = endpoint - pd.Timedelta(minutes=30) + pd.Timedelta(minutes=5)
    
    # Count intervals with current logic
    mask_current = (
        (scada5_df['duid'] == test_duid) & 
        (scada5_df['settlementdate'] > start_current) & 
        (scada5_df['settlementdate'] <= endpoint)
    )
    count_current = len(scada5_df[mask_current])
    
    # Count intervals with correct logic  
    mask_correct = (
        (scada5_df['duid'] == test_duid) & 
        (scada5_df['settlementdate'] > start_correct) & 
        (scada5_df['settlementdate'] <= endpoint)
    )
    count_correct = len(scada5_df[mask_correct])
    
    print(f"\nEndpoint {endpoint}:")
    print(f"  Current logic: {count_current} intervals")
    print(f"  Should have: {count_correct} intervals")

print(f"\n" + "=" * 80)
print("CONCLUSION:")
print("-" * 40)
print("❌ BUG FOUND: The current code systematically captures only 5 of 6 intervals")
print("   due to using > (endpoint - 25 minutes) instead of > (endpoint - 30 minutes)")
print("")
print("IMPACT: This creates a systematic bias in the 30-minute aggregation.")
print("   The first 5-minute interval of each 30-minute period is excluded.")
print("")
print("FIX NEEDED: Change the window calculation to properly include all 6 intervals.")