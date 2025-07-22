#!/usr/bin/env python3
"""
Debug VRE patterns to understand why they don't match the reference.
"""
import os
import sys
from pathlib import Path
from datetime import datetime

# Set environment variable BEFORE any imports
os.environ['USE_DUCKDB'] = 'true'

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / 'src'))

import pandas as pd
from aemo_dashboard.generation.generation_query_manager import GenerationQueryManager
from aemo_dashboard.shared.config import Config

def debug_vre_patterns():
    """Debug VRE patterns."""
    print("Debugging VRE patterns...")
    
    # Initialize
    query_manager = GenerationQueryManager()
    config = Config()
    
    # Check a specific period - let's look at day 50 of 2025 where we see the dramatic dip
    # Day 50 = Feb 19
    test_date = datetime(2025, 2, 19)
    
    # Get a week of data around this date
    start_date = datetime(2025, 2, 15)
    end_date = datetime(2025, 2, 25)
    
    print(f"\nChecking data around day 50 of 2025 (Feb 19)...")
    
    # Get generation data
    gen_data = query_manager.query_generation_by_fuel(
        start_date=start_date,
        end_date=end_date,
        region='NEM',
        resolution='30min'
    )
    
    # Filter for VRE
    vre_data = gen_data[gen_data['fuel_type'].isin(['Wind', 'Solar'])].copy()
    vre_data['date'] = pd.to_datetime(vre_data['settlementdate']).dt.date
    
    # Daily totals by fuel type
    daily_by_fuel = vre_data.groupby(['date', 'fuel_type'])['total_generation_mw'].mean()
    
    print("\nDaily averages by fuel type:")
    print(daily_by_fuel)
    
    # Load rooftop data directly
    rooftop_file = config.rooftop_solar_file
    print(f"\nLoading rooftop from: {rooftop_file}")
    
    if rooftop_file and Path(rooftop_file).exists():
        rooftop_df = pd.read_parquet(rooftop_file)
        print(f"Rooftop columns: {rooftop_df.columns.tolist()}")
        
        # Filter dates
        rooftop_df['settlementdate'] = pd.to_datetime(rooftop_df['settlementdate'])
        rooftop_filtered = rooftop_df[(rooftop_df['settlementdate'] >= start_date) & 
                                      (rooftop_df['settlementdate'] <= end_date)]
        
        print(f"Rooftop data shape for period: {rooftop_filtered.shape}")
        
        # Check if it's long or wide format
        if 'regionid' in rooftop_filtered.columns:
            print("Rooftop is in LONG format")
            # Sum by timestamp
            rooftop_by_time = rooftop_filtered.groupby('settlementdate')['power'].sum()
        else:
            print("Rooftop is in WIDE format")
            # Sum across regions
            region_cols = [col for col in rooftop_filtered.columns if col != 'settlementdate']
            rooftop_by_time = rooftop_filtered.set_index('settlementdate')[region_cols].sum(axis=1)
        
        # Daily average
        rooftop_daily = rooftop_by_time.resample('D').mean()
        print("\nRooftop daily averages:")
        print(rooftop_daily)
    
    # Now check full year patterns
    print("\n\nChecking full year 2025 pattern...")
    
    # Get all 2025 data
    gen_2025 = query_manager.query_generation_by_fuel(
        start_date=datetime(2025, 1, 1),
        end_date=datetime(2025, 7, 19),  # Current date
        region='NEM',
        resolution='30min'
    )
    
    vre_2025 = gen_2025[gen_2025['fuel_type'].isin(['Wind', 'Solar'])].copy()
    vre_2025['date'] = pd.to_datetime(vre_2025['settlementdate'])
    vre_2025['dayofyear'] = vre_2025['date'].dt.dayofyear
    
    # Daily averages
    daily_2025 = vre_2025.groupby('dayofyear')['total_generation_mw'].mean()
    
    print(f"\n2025 VRE (Wind+Solar) pattern:")
    print(f"Days 1-10: {daily_2025[1:11].mean():.0f} MW")
    print(f"Days 40-50: {daily_2025[40:51].mean():.0f} MW")
    print(f"Days 190-200: {daily_2025[190:201].mean():.0f} MW")
    
    # Check for data gaps
    expected_days = set(range(1, 201))
    actual_days = set(daily_2025.index)
    missing_days = expected_days - actual_days
    
    if missing_days:
        print(f"\nMISSING DAYS in 2025: {sorted(missing_days)}")

if __name__ == "__main__":
    debug_vre_patterns()