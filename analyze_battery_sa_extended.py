#!/usr/bin/env python3
"""Extended analysis of battery storage data for SA - check 7 days"""

import sys
sys.path.insert(0, 'src')

import pandas as pd
from datetime import datetime, timedelta
from aemo_dashboard.shared import adapter_selector
from aemo_dashboard.generation.generation_query_manager import GenerationQueryManager

print("=== EXTENDED BATTERY STORAGE ANALYSIS FOR SA (7 DAYS) ===\n")

# Time periods - 7 days
now = datetime.now()
end_time = now
start_time = now - timedelta(days=7)

print(f"Analysis period: {start_time} to {end_time}")

# Load generation data for SA
gen_manager = GenerationQueryManager()

# Get generation data by fuel type for SA
print("\n1. Loading 7 days of generation data for SA...")
gen_data = gen_manager.query_generation_by_fuel(
    start_date=start_time,
    end_date=end_time,
    region='SA1',
    resolution='30min'  # Use 30min for 7 days
)

if not gen_data.empty and 'fuel_type' in gen_data.columns:
    battery_data = gen_data[gen_data['fuel_type'] == 'Battery Storage']
    
    if not battery_data.empty:
        print(f"\nBattery Storage data found: {len(battery_data)} records")
        
        # Get generation values
        gen_values = battery_data['total_generation_mw'].values
        
        print(f"\nBattery Statistics (7 days):")
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
        
        # If there are negative values, show when they occur
        if negative_count > 0:
            print(f"\nCharging periods found!")
            charging_data = battery_data[battery_data['total_generation_mw'] < 0]
            print(f"First charging: {charging_data.iloc[0]['settlementdate']}")
            print(f"Last charging: {charging_data.iloc[-1]['settlementdate']}")
            
            # Show distribution by day
            charging_data['date'] = pd.to_datetime(charging_data['settlementdate']).dt.date
            daily_charging = charging_data.groupby('date').size()
            print(f"\nCharging periods by day:")
            for date, count in daily_charging.items():
                print(f"  {date}: {count} periods")

# Now check all regions to see if ANY battery shows charging
print("\n\n2. Checking all regions for battery charging...")
all_regions = ['NSW1', 'QLD1', 'SA1', 'TAS1', 'VIC1']

for region in all_regions:
    gen_data = gen_manager.query_generation_by_fuel(
        start_date=now - timedelta(hours=24),
        end_date=now,
        region=region,
        resolution='5min'
    )
    
    if not gen_data.empty and 'fuel_type' in gen_data.columns:
        battery_data = gen_data[gen_data['fuel_type'] == 'Battery Storage']
        if not battery_data.empty:
            gen_values = battery_data['total_generation_mw'].values
            negative_count = (gen_values < 0).sum()
            min_val = gen_values.min()
            print(f"  {region}: {negative_count} charging periods, min value: {min_val:.2f} MW")
        else:
            print(f"  {region}: No battery data")

# Check raw SCADA data for specific time when we expect charging
print("\n\n3. Checking raw SCADA data for overnight period (typically charging time)...")
overnight_start = datetime.now().replace(hour=2, minute=0, second=0, microsecond=0)
overnight_end = overnight_start + timedelta(hours=4)

raw_gen = adapter_selector.load_generation_data(
    start_date=overnight_start,
    end_date=overnight_end,
    resolution='5min'
)

if not raw_gen.empty:
    # Look for all battery DUIDs across all regions
    battery_keywords = ['BATT', 'BBS', 'HPRG', 'DALNTH', 'BESS', 'GANNBG', 'WGWF', 'BULLAG']
    battery_duids = []
    
    for duid in raw_gen['duid'].unique():
        if any(keyword in duid for keyword in battery_keywords):
            battery_duids.append(duid)
    
    if battery_duids:
        print(f"\nFound {len(battery_duids)} battery DUIDs across all regions")
        print(f"Checking overnight period: {overnight_start} to {overnight_end}")
        
        # Check for any negative values
        for duid in sorted(battery_duids):
            duid_data = raw_gen[raw_gen['duid'] == duid]
            if not duid_data.empty:
                values = duid_data['scadavalue'].values
                neg_count = (values < 0).sum()
                min_val = values.min()
                if neg_count > 0 or min_val < 0:
                    print(f"  {duid}: {neg_count} negative values, min: {min_val:.2f} MW *** CHARGING FOUND ***")
                else:
                    print(f"  {duid}: No charging (min: {min_val:.2f} MW)")

print("\n=== END OF ANALYSIS ===")