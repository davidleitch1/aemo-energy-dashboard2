#!/usr/bin/env python3
"""Analyze battery storage data for South Australia to diagnose display issue"""

import sys
sys.path.insert(0, 'src')

import pandas as pd
from datetime import datetime, timedelta
from aemo_dashboard.shared import adapter_selector
from aemo_dashboard.generation.generation_query_manager import GenerationQueryManager

print("=== BATTERY STORAGE ANALYSIS FOR SA ===\n")

# Time periods
now = datetime.now()
end_time = now
start_time = now - timedelta(hours=24)

print(f"Analysis period: {start_time} to {end_time}")

# Load generation data for SA
gen_manager = GenerationQueryManager()

# Get generation data by fuel type for SA
print("\n1. Loading generation data for SA...")
gen_data = gen_manager.query_generation_by_fuel(
    start_date=start_time,
    end_date=end_time,
    region='SA1',
    resolution='5min'
)

if gen_data.empty:
    print("No generation data found!")
else:
    print(f"Generation data shape: {gen_data.shape}")
    print(f"Columns: {list(gen_data.columns)}")
    
    # Check if Battery Storage exists
    if 'fuel_type' in gen_data.columns:
        battery_data = gen_data[gen_data['fuel_type'] == 'Battery Storage']
        
        if not battery_data.empty:
            print(f"\n2. Battery Storage data found: {len(battery_data)} records")
            
            # Get generation values
            gen_values = battery_data['total_generation_mw'].values
            
            print(f"\nBattery Statistics:")
            print(f"  Min value: {gen_values.min():.2f} MW")
            print(f"  Max value: {gen_values.max():.2f} MW")
            print(f"  Mean value: {gen_values.mean():.2f} MW")
            
            # Count positive vs negative
            positive_count = (gen_values > 0).sum()
            negative_count = (gen_values < 0).sum()
            zero_count = (gen_values == 0).sum()
            
            print(f"\nValue Distribution:")
            print(f"  Positive (discharging): {positive_count} records ({positive_count/len(battery_data)*100:.1f}%)")
            print(f"  Negative (charging): {negative_count} records ({negative_count/len(battery_data)*100:.1f}%)")
            print(f"  Zero: {zero_count} records ({zero_count/len(battery_data)*100:.1f}%)")
            
            # Show sample of negative values (charging)
            if negative_count > 0:
                print(f"\nSample of charging periods (negative values):")
                charging_data = battery_data[battery_data['total_generation_mw'] < 0].head(10)
                for idx, row in charging_data.iterrows():
                    print(f"  {row['settlementdate']}: {row['total_generation_mw']:.2f} MW")
            
            # Show most recent 10 values
            print(f"\nMost recent 10 battery values:")
            recent_data = battery_data.tail(10)
            for idx, row in recent_data.iterrows():
                status = "charging" if row['total_generation_mw'] < 0 else "discharging" if row['total_generation_mw'] > 0 else "idle"
                print(f"  {row['settlementdate']}: {row['total_generation_mw']:.2f} MW ({status})")
                
        else:
            print("\n2. No Battery Storage data found for SA!")
    else:
        print("\nError: 'fuel_type' column not found in data")

# Now let's check the raw generation data to see all DUIDs
print("\n\n3. Checking raw generation data for battery DUIDs...")
raw_gen = adapter_selector.load_generation_data(
    start_date=start_time,
    end_date=end_time,
    resolution='5min'
)

if not raw_gen.empty:
    print(f"Raw generation data shape: {raw_gen.shape}")
    
    # Look for battery-related DUIDs
    print("\nSearching for battery-related DUIDs...")
    battery_duids = []
    for duid in raw_gen['duid'].unique():
        if 'BATT' in duid or 'BBS' in duid or 'HPRG' in duid or 'DALNTH' in duid:
            battery_duids.append(duid)
    
    if battery_duids:
        print(f"Found {len(battery_duids)} battery DUIDs: {battery_duids}")
        
        # Check each battery DUID
        for duid in battery_duids:
            duid_data = raw_gen[raw_gen['duid'] == duid]
            values = duid_data['scadavalue'].values
            print(f"\n  {duid}:")
            print(f"    Records: {len(duid_data)}")
            print(f"    Min: {values.min():.2f} MW")
            print(f"    Max: {values.max():.2f} MW")
            print(f"    Negative values: {(values < 0).sum()}")
    else:
        print("No battery DUIDs found in raw data")

print("\n=== END OF ANALYSIS ===")