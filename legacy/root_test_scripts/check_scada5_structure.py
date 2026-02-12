#!/usr/bin/env python3
"""
Check the actual structure and timing of scada5 data to understand
if we're missing intervals in the 30-minute calculation
"""

import pandas as pd
from pathlib import Path

# Data paths
data_dir = Path('/Volumes/davidleitch/aemo_production/data')

print("=" * 80)
print("INVESTIGATING SCADA5 DATA STRUCTURE")
print("=" * 80)

# Load scada5 data
scada5_df = pd.read_parquet(data_dir / 'scada5.parquet')

print("\n1. SCADA5 DATA RANGE")
print("-" * 40)
print(f"First timestamp: {scada5_df['settlementdate'].min()}")
print(f"Last timestamp: {scada5_df['settlementdate'].max()}")
print(f"Total records: {len(scada5_df):,}")

# Get unique timestamps to understand the pattern
print("\n2. TIMESTAMP PATTERN ANALYSIS")
print("-" * 40)

# Look at first 50 unique timestamps
unique_times = sorted(scada5_df['settlementdate'].unique())
print(f"Total unique timestamps: {len(unique_times)}")
print("\nFirst 20 timestamps:")
for t in unique_times[:20]:
    print(f"  {t}")

# Check the interval between consecutive timestamps
print("\n3. INTERVAL ANALYSIS")
print("-" * 40)
time_diffs = pd.Series(unique_times).diff().dropna()
unique_intervals = time_diffs.value_counts()
print("Time intervals between consecutive timestamps:")
for interval, count in unique_intervals.items():
    print(f"  {interval}: {count} occurrences")

# Focus on a specific DUID to see the pattern clearly
test_duid = 'ARWF1'
print(f"\n4. DETAILED PATTERN FOR {test_duid}")
print("-" * 40)

# Get data for this DUID for a full hour
test_start = pd.Timestamp('2025-09-02 11:00:00')
test_end = pd.Timestamp('2025-09-02 12:30:00')

mask = (
    (scada5_df['duid'] == test_duid) &
    (scada5_df['settlementdate'] >= test_start) &
    (scada5_df['settlementdate'] <= test_end)
)
test_data = scada5_df[mask].sort_values('settlementdate')

print(f"Timestamps for {test_duid} from {test_start} to {test_end}:")
for _, row in test_data.iterrows():
    print(f"  {row['settlementdate']}: {row['scadavalue']:.2f} MW")

# Count intervals per 30-minute period
print(f"\n5. INTERVALS PER 30-MINUTE PERIOD")
print("-" * 40)

# Check multiple 30-minute periods
endpoints = [
    pd.Timestamp('2025-09-02 06:00:00'),
    pd.Timestamp('2025-09-02 06:30:00'),
    pd.Timestamp('2025-09-02 12:00:00'),
    pd.Timestamp('2025-09-02 12:30:00'),
    pd.Timestamp('2025-09-02 18:00:00'),
    pd.Timestamp('2025-09-02 18:30:00'),
]

for endpoint in endpoints:
    # Count how many 5-minute intervals exist in the full 30-minute window
    window_start = endpoint - pd.Timedelta(minutes=30)
    
    # Method 1: Inclusive of start (>= window_start)
    mask1 = (
        (scada5_df['duid'] == test_duid) &
        (scada5_df['settlementdate'] > window_start) &
        (scada5_df['settlementdate'] <= endpoint)
    )
    count1 = len(scada5_df[mask1])
    
    # Method 2: What the code currently does (> endpoint - 25min)
    current_start = endpoint - pd.Timedelta(minutes=25)
    mask2 = (
        (scada5_df['duid'] == test_duid) &
        (scada5_df['settlementdate'] > current_start) &
        (scada5_df['settlementdate'] <= endpoint)
    )
    count2 = len(scada5_df[mask2])
    
    # Get the actual intervals to see what's there
    actual_intervals = scada5_df[mask1]['settlementdate'].tolist()
    
    print(f"\n{endpoint}:")
    print(f"  Full 30-min window (>{window_start}): {count1} intervals")
    print(f"  Current code (>{current_start}): {count2} intervals")
    if actual_intervals:
        print(f"  Actual timestamps: {[str(t) for t in sorted(actual_intervals)]}")

print(f"\n6. THE KEY QUESTION")
print("=" * 80)
print("Are we systematically missing data that exists in scada5?")
print("")

# Check if the 'missing' 6th interval actually exists
test_endpoint = pd.Timestamp('2025-09-02 12:00:00')
expected_first = test_endpoint - pd.Timedelta(minutes=30)  # 11:30:00
expected_second = test_endpoint - pd.Timedelta(minutes=25) # 11:35:00

# Check if these specific timestamps exist
mask_first = (
    (scada5_df['duid'] == test_duid) &
    (scada5_df['settlementdate'] == expected_first)
)
mask_second = (
    (scada5_df['duid'] == test_duid) &
    (scada5_df['settlementdate'] == expected_second)
)

exists_first = len(scada5_df[mask_first]) > 0
exists_second = len(scada5_df[mask_second]) > 0

print(f"For endpoint {test_endpoint}:")
print(f"  Does {expected_first} exist in scada5? {exists_first}")
print(f"  Does {expected_second} exist in scada5? {exists_second}")

if exists_first:
    value_first = scada5_df[mask_first]['scadavalue'].iloc[0]
    print(f"    Value at {expected_first}: {value_first:.2f} MW")
if exists_second:
    value_second = scada5_df[mask_second]['scadavalue'].iloc[0]
    print(f"    Value at {expected_second}: {value_second:.2f} MW")

print("\nCONCLUSION:")
if exists_first and not exists_second:
    print("ERROR: We have 11:30 but not 11:35 - data gap in scada5")
elif exists_second and not exists_first:
    print("OK: We have data starting from 11:35 - this is the actual data pattern")
elif exists_first and exists_second:
    print("PROBLEM: Both timestamps exist, so we should be using 6 intervals!")
else:
    print("DATA ISSUE: Neither timestamp exists")

# Final check: How many 5-minute periods SHOULD be in 30 minutes?
print("\n7. THEORETICAL VS ACTUAL")
print("-" * 40)
print("In theory: 30 minutes ÷ 5 minutes = 6 intervals")
print("In practice: We're seeing 5 intervals in the data")
print("")
print("Possible explanations:")
print("1. AEMO provides data at :05, :10, :15, :20, :25, :30, :35, :40, :45, :50, :55, :00")
print("2. The 30-min window from :30:01 to :00:00 would include :35, :40, :45, :50, :55, :00 (6 intervals)")
print("3. But if data starts at a different offset, we might only have 5")
print("4. OR there's a systematic issue with how the data is being collected/stored")