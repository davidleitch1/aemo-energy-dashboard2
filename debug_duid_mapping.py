#!/usr/bin/env python3
"""
Debug DUID mapping structure to find the issue
"""

import pandas as pd
import pickle
from datetime import datetime, timedelta

# Load DUID mapping directly
mapping_file = "/Users/davidleitch/Library/Mobile Documents/com~apple~CloudDocs/snakeplay/AEMO_spot/aemo-energy-dashboard/data/gen_info.pkl"

with open(mapping_file, 'rb') as f:
    duid_mapping = pickle.load(f)

print("🔍 DUID Mapping Structure Analysis")
print("=" * 40)

print(f"Type: {type(duid_mapping)}")
print(f"Length: {len(duid_mapping)}")

if isinstance(duid_mapping, pd.DataFrame):
    print("DataFrame structure:")
    print(f"  Shape: {duid_mapping.shape}")
    print(f"  Columns: {list(duid_mapping.columns)}")
    print(f"  Index: {list(duid_mapping.index[:5])}")
    print("\nFirst few rows:")
    print(duid_mapping.head())
    
    # Look for actual DUIDs
    if 'DUID' in duid_mapping.columns:
        print(f"\nDUID column values (first 10): {duid_mapping['DUID'].head(10).tolist()}")
        valid_duid = duid_mapping['DUID'].iloc[0]
    else:
        print("\nNo DUID column found, checking index...")
        # Maybe DUIDs are in the index
        valid_duid = duid_mapping.index[0]
else:
    print("Dictionary structure:")
    print(f"  Keys: {list(duid_mapping.keys())[:10]}")
    print(f"  Sample entry: {list(duid_mapping.items())[0]}")
    valid_duid = list(duid_mapping.keys())[0]

print(f"\nValid DUID for testing: {valid_duid}")

# Now test with generation data to see if this DUID exists
print("\n" + "=" * 40)
print("🔍 Checking DUID in Generation Data")
print("=" * 40)

gen_file = "/Users/davidleitch/Library/Mobile Documents/com~apple~CloudDocs/snakeplay/AEMO_spot/aemo-data-updater/data 2/scada5.parquet"
gen_data = pd.read_parquet(gen_file)

# Filter to recent period
end_date = datetime.now()
start_date = end_date - timedelta(days=1)

gen_filtered = gen_data[
    (gen_data['settlementdate'] >= start_date) & 
    (gen_data['settlementdate'] <= end_date)
]

print(f"Generation data for last 24h: {len(gen_filtered):,} records")
print(f"Unique DUIDs: {gen_filtered['duid'].nunique()}")
print(f"Sample DUIDs: {gen_filtered['duid'].unique()[:10].tolist()}")

# Check if our test DUID is in the data
if valid_duid in gen_filtered['duid'].values:
    print(f"✅ DUID {valid_duid} found in generation data")
    duid_data = gen_filtered[gen_filtered['duid'] == valid_duid]
    print(f"   Records for this DUID: {len(duid_data)}")
else:
    print(f"❌ DUID {valid_duid} NOT found in generation data")
    print("   This explains why station analysis shows no data!")
    
    # Find a DUID that IS in the recent generation data
    available_duid = gen_filtered['duid'].iloc[0]
    print(f"   Using available DUID instead: {available_duid}")
    
    # Check if this DUID is in the mapping
    if isinstance(duid_mapping, pd.DataFrame):
        if 'DUID' in duid_mapping.columns:
            has_mapping = available_duid in duid_mapping['DUID'].values
        else:
            has_mapping = available_duid in duid_mapping.index
    else:
        has_mapping = available_duid in duid_mapping
    
    print(f"   Available DUID {available_duid} in mapping: {has_mapping}")

print("\n" + "=" * 40)
print("💡 Root Cause Analysis")
print("=" * 40)
print("The issue is likely that:")
print("1. The station analysis UI is trying to use DUIDs from the mapping")
print("2. But those DUIDs don't have recent data in the generation files")
print("3. OR the mapping structure is not being parsed correctly")
print("4. This causes filter_station_data to return False, showing no data")