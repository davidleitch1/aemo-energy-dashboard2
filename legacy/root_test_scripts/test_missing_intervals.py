#!/usr/bin/env python3
"""
Test how the scada30 calculation handles missing 5-minute intervals
Critical question: Does it correctly handle missing data or does it introduce bias?
"""

import pandas as pd
import numpy as np
from pathlib import Path
import pickle

# Data paths
data_dir = Path('/Volumes/davidleitch/aemo_production/data')

print("=" * 80)
print("TESTING SCADA30 CALCULATION WITH MISSING INTERVALS")
print("=" * 80)

# Load data
print("\n1. Loading data files...")
scada5_df = pd.read_parquet(data_dir / 'scada5.parquet')
scada30_df = pd.read_parquet(data_dir / 'scada30.parquet')

print("\n2. THEORETICAL PROBLEM:")
print("-" * 40)
print("If we have 5 intervals of 10 MW each:")
print("  Intervals: [10, 10, 10, 10, 10, missing]")
print("  Mean of available (5 intervals): 50/5 = 10 MW")
print("  True mean (if missing was 0): 50/6 = 8.33 MW")
print("  True mean (if missing was 10): 60/6 = 10 MW")
print("\nThe question: What does the missing interval represent?")
print("  - If equipment was offline (0 MW) → mean of available gives WRONG answer")
print("  - If data collection failed but equipment was running → mean of available is CORRECT")

print("\n3. FINDING CASES WITH MISSING INTERVALS:")
print("-" * 40)

# Let's find actual cases where we have < 6 intervals
# Pick a test period
test_date = pd.Timestamp('2025-09-02')
test_start = pd.Timestamp(f'{test_date.date()} 00:00:00')
test_end = pd.Timestamp(f'{test_date.date()} 23:30:00')

# Get all 30-minute endpoints
endpoints = pd.date_range(test_start, test_end, freq='30min')

# Load DUID mapping for Solar
with open(data_dir / 'gen_info.pkl', 'rb') as f:
    gen_info = pickle.load(f)
solar_duids = gen_info[gen_info['Fuel'] == 'Solar']['DUID'].tolist()

print(f"Testing with {len(solar_duids)} Solar DUIDs")
print(f"Test period: {test_start} to {test_end}")

# Find cases with missing intervals
missing_cases = []
complete_cases = []

for endpoint in endpoints[:10]:  # Check first 10 endpoints for efficiency
    start_time = endpoint - pd.Timedelta(minutes=25)
    
    for duid in solar_duids[:20]:  # Check first 20 solar DUIDs
        # Count 5-minute intervals
        mask_5min = (
            (scada5_df['duid'] == duid) & 
            (scada5_df['settlementdate'] > start_time) & 
            (scada5_df['settlementdate'] <= endpoint)
        )
        intervals = scada5_df[mask_5min]
        num_intervals = len(intervals)
        
        # Get 30-minute value
        mask_30min = (
            (scada30_df['duid'] == duid) & 
            (scada30_df['settlementdate'] == endpoint)
        )
        value_30min = scada30_df[mask_30min]
        
        if not value_30min.empty:
            if num_intervals < 6:
                missing_cases.append({
                    'duid': duid,
                    'endpoint': endpoint,
                    'num_intervals': num_intervals,
                    'scada30_value': value_30min['scadavalue'].iloc[0],
                    'intervals_data': intervals
                })
            elif num_intervals == 6:
                complete_cases.append({
                    'duid': duid,
                    'endpoint': endpoint,
                    'num_intervals': num_intervals,
                    'scada30_value': value_30min['scadavalue'].iloc[0],
                    'intervals_data': intervals
                })

print(f"\nFound {len(missing_cases)} cases with < 6 intervals")
print(f"Found {len(complete_cases)} cases with 6 intervals")

if missing_cases:
    print("\n4. DETAILED ANALYSIS OF MISSING INTERVAL CASES:")
    print("-" * 40)
    
    # Analyze first few cases
    for case in missing_cases[:5]:
        print(f"\nCase: {case['duid']} at {case['endpoint']}")
        print(f"  Number of intervals: {case['num_intervals']}")
        
        intervals = case['intervals_data']
        if not intervals.empty:
            values = intervals['scadavalue'].values
            times = intervals['settlementdate'].values
            
            print(f"  5-min values: {[f'{v:.2f}' for v in values]}")
            print(f"  5-min times: {[str(t) for t in times]}")
            print(f"  Sum of 5-min values: {sum(values):.2f} MW")
            print(f"  Mean of available: {np.mean(values):.2f} MW")
            print(f"  If missing was 0: mean would be {sum(values)/6:.2f} MW")
            print(f"  Actual scada30: {case['scada30_value']:.2f} MW")
            
            # Check which calculation matches
            mean_available = np.mean(values)
            mean_with_zero = sum(values) / 6
            
            diff_available = abs(case['scada30_value'] - mean_available)
            diff_with_zero = abs(case['scada30_value'] - mean_with_zero)
            
            if diff_available < 0.001:
                print(f"  ✓ Matches mean of available intervals")
            elif diff_with_zero < 0.001:
                print(f"  ✓ Matches mean assuming missing = 0")
            else:
                print(f"  ? Doesn't match either calculation")

print("\n5. CHECKING PATTERN OF MISSING INTERVALS:")
print("-" * 40)

# Check WHEN intervals are missing
if missing_cases:
    missing_times = []
    for case in missing_cases:
        intervals = case['intervals_data']
        if not intervals.empty:
            # Find which interval times are present
            present_times = set(intervals['settlementdate'].dt.minute.values)
            endpoint_minute = case['endpoint'].minute
            
            # Expected times (working backwards from endpoint)
            if endpoint_minute == 0:
                expected = [40, 45, 50, 55, 0, 5]  # Note: 5 is from next hour
            else:  # endpoint_minute == 30
                expected = [10, 15, 20, 25, 30, 35]  # Note: 35 is overflow
            
            # Which are missing?
            for exp_min in expected[:5]:  # Only check first 5 (the 6th is problematic)
                if exp_min not in present_times:
                    missing_times.append(exp_min)
    
    if missing_times:
        from collections import Counter
        missing_counter = Counter(missing_times)
        print("Minutes most often missing:")
        for minute, count in missing_counter.most_common(5):
            print(f"  :{minute:02d} - {count} times")

print("\n6. INVESTIGATION OF DATA AVAILABILITY:")
print("-" * 40)

# Check the actual structure - are we counting intervals correctly?
test_endpoint = pd.Timestamp('2025-09-02 12:00:00')
test_start = test_endpoint - pd.Timedelta(minutes=25)
test_duid = solar_duids[0]

print(f"\nDetailed check for {test_duid} at {test_endpoint}:")
print(f"Looking for intervals > {test_start} and <= {test_endpoint}")

# Get ALL 5-minute data for this DUID around this time
wide_start = test_endpoint - pd.Timedelta(minutes=35)
wide_end = test_endpoint + pd.Timedelta(minutes=10)

mask_wide = (
    (scada5_df['duid'] == test_duid) & 
    (scada5_df['settlementdate'] >= wide_start) & 
    (scada5_df['settlementdate'] <= wide_end)
)
wide_intervals = scada5_df[mask_wide].sort_values('settlementdate')

print(f"\nAll 5-minute intervals around this period:")
for _, row in wide_intervals.iterrows():
    in_window = (row['settlementdate'] > test_start) and (row['settlementdate'] <= test_endpoint)
    marker = " <-- IN WINDOW" if in_window else ""
    print(f"  {row['settlementdate']}: {row['scadavalue']:.2f} MW{marker}")

# Count how many should be in window
in_window_count = len(wide_intervals[
    (wide_intervals['settlementdate'] > test_start) & 
    (wide_intervals['settlementdate'] <= test_endpoint)
])
print(f"\nIntervals in window: {in_window_count}")

print("\n7. CRITICAL FINDING:")
print("=" * 80)

# The key question: Is the "missing" 6th interval actually at a different timestamp?
print("The 30-minute window for endpoint at HH:00 should include:")
print("  HH-1:35, HH-1:40, HH-1:45, HH-1:50, HH-1:55, HH:00")
print("")
print("The 30-minute window for endpoint at HH:30 should include:")  
print("  HH:05, HH:10, HH:15, HH:20, HH:25, HH:30")
print("")
print("BUT the code uses: settlementdate > (endpoint - 25min) AND settlementdate <= endpoint")
print("This means:")
print("  - For HH:00 endpoint: gets HH-1:40, HH-1:45, HH-1:50, HH-1:55, HH:00 (5 intervals)")
print("  - For HH:30 endpoint: gets HH:10, HH:15, HH:20, HH:25, HH:30 (5 intervals)")
print("")
print("The PROBLEM: The code is only capturing 5 of the 6 intervals!")
print("It's missing the first interval of each 30-minute period.")
print("")
print("This means the calculation is BIASED - it's not truly representing the full 30 minutes.")