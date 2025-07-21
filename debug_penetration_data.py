#!/usr/bin/env python3
"""
Debug script for penetration tab data.
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
from aemo_dashboard.shared.smoothing import apply_ewm_smoothing

def debug_data():
    """Debug the data flow for penetration tab."""
    print("Debugging Penetration Tab Data...")
    
    # Initialize query manager
    query_manager = GenerationQueryManager()
    
    # Test for 2024 data
    start_date = datetime(2024, 1, 1)
    end_date = datetime(2024, 12, 31, 23, 59, 59)
    
    print(f"\nQuerying data from {start_date} to {end_date}")
    
    # Query data
    data = query_manager.query_generation_by_fuel(
        start_date=start_date,
        end_date=end_date,
        region='NEM',
        resolution='30min'
    )
    
    print(f"\nData shape: {data.shape}")
    print(f"Columns: {data.columns.tolist()}")
    print(f"\nFirst few rows:")
    print(data.head())
    
    # Check fuel types
    print(f"\nUnique fuel types: {data['fuel_type'].unique()}")
    
    # Filter for VRE
    vre_fuels = ['Wind', 'Solar', 'Rooftop']
    vre_data = data[data['fuel_type'].isin(vre_fuels)]
    
    print(f"\nVRE data shape: {vre_data.shape}")
    print(f"VRE fuel types: {vre_data['fuel_type'].unique()}")
    
    # Check data by fuel type
    for fuel in vre_fuels:
        fuel_data = vre_data[vre_data['fuel_type'] == fuel]
        if not fuel_data.empty:
            print(f"\n{fuel}: {len(fuel_data)} records")
            print(f"  Generation range: {fuel_data['total_generation_mw'].min():.2f} - {fuel_data['total_generation_mw'].max():.2f} MW")
        else:
            print(f"\n{fuel}: NO DATA")
    
    # Test aggregation
    print("\n\nTesting aggregation...")
    vre_data = vre_data.copy()
    vre_data['date'] = pd.to_datetime(vre_data['settlementdate'])
    vre_data['year'] = vre_data['date'].dt.year
    vre_data['dayofyear'] = vre_data['date'].dt.dayofyear
    
    # Sum across fuel types for each timestamp
    daily_sum = vre_data.groupby(['settlementdate', 'year', 'dayofyear'])['total_generation_mw'].sum().reset_index()
    print(f"\nDaily sum shape: {daily_sum.shape}")
    print(f"First few rows of daily sum:")
    print(daily_sum.head())
    
    # Average by day of year
    daily_avg = daily_sum.groupby(['year', 'dayofyear'])['total_generation_mw'].mean().reset_index()
    print(f"\nDaily average shape: {daily_avg.shape}")
    print(f"First few rows of daily average:")
    print(daily_avg.head())
    
    # Convert to TWh
    daily_avg['twh_annualised'] = daily_avg['total_generation_mw'] * 24 * 365 / 1_000_000
    print(f"\nTWh range: {daily_avg['twh_annualised'].min():.2f} - {daily_avg['twh_annualised'].max():.2f}")
    
    # Test smoothing
    daily_avg['twh_smoothed'] = apply_ewm_smoothing(daily_avg['twh_annualised'], span=30)
    print(f"Smoothed TWh range: {daily_avg['twh_smoothed'].min():.2f} - {daily_avg['twh_smoothed'].max():.2f}")
    
    print("\nDebug complete!")

if __name__ == "__main__":
    debug_data()