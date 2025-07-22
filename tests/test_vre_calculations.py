#!/usr/bin/env python3
"""
Test VRE calculations to ensure correct values.
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
from aemo_dashboard.shared.rooftop_adapter import load_rooftop_data

def test_vre_totals():
    """Test VRE totals calculation."""
    print("Testing VRE totals for a sample day...")
    
    # Initialize query manager
    query_manager = GenerationQueryManager()
    
    # Get data for a specific day
    test_date = datetime(2024, 6, 1)
    end_date = datetime(2024, 6, 1, 23, 59, 59)
    
    # Get generation data
    gen_data = query_manager.query_generation_by_fuel(
        start_date=test_date,
        end_date=end_date,
        region='NEM',
        resolution='30min'
    )
    
    # Get rooftop data
    rooftop_data = load_rooftop_data(
        start_date=test_date,
        end_date=end_date
    )
    
    print(f"\nGeneration data shape: {gen_data.shape}")
    print(f"Rooftop data shape: {rooftop_data.shape}")
    
    # Filter for VRE in generation
    vre_gen = gen_data[gen_data['fuel_type'].isin(['Wind', 'Solar'])]
    
    # Calculate averages
    wind_avg = vre_gen[vre_gen['fuel_type'] == 'Wind']['total_generation_mw'].mean()
    solar_avg = vre_gen[vre_gen['fuel_type'] == 'Solar']['total_generation_mw'].mean()
    
    # Sum rooftop across regions
    region_cols = [col for col in rooftop_data.columns if col != 'settlementdate']
    rooftop_avg = rooftop_data[region_cols].sum(axis=1).mean()
    
    print(f"\nDaily averages:")
    print(f"  Wind: {wind_avg:.0f} MW")
    print(f"  Solar: {solar_avg:.0f} MW")
    print(f"  Rooftop: {rooftop_avg:.0f} MW")
    print(f"  Total VRE: {wind_avg + solar_avg + rooftop_avg:.0f} MW")
    
    # Calculate annualised
    total_vre = wind_avg + solar_avg + rooftop_avg
    annualised = total_vre * 24 * 365 / 1_000_000
    
    print(f"\nAnnualised VRE: {annualised:.1f} TWh")
    
    # Test full year
    print("\n\nTesting full year 2024...")
    start_2024 = datetime(2024, 1, 1)
    end_2024 = datetime(2024, 12, 31, 23, 59, 59)
    
    gen_2024 = query_manager.query_generation_by_fuel(
        start_date=start_2024,
        end_date=end_2024,
        region='NEM',
        resolution='30min'
    )
    
    # Calculate daily averages for the year
    vre_2024 = gen_2024[gen_2024['fuel_type'].isin(['Wind', 'Solar'])].copy()
    vre_2024['date'] = pd.to_datetime(vre_2024['settlementdate']).dt.date
    
    # Daily totals
    daily_totals = vre_2024.groupby(['date', 'fuel_type'])['total_generation_mw'].mean().reset_index()
    
    # Average by fuel type
    avg_by_fuel = daily_totals.groupby('fuel_type')['total_generation_mw'].mean()
    
    print(f"\n2024 Average generation:")
    for fuel, avg in avg_by_fuel.items():
        print(f"  {fuel}: {avg:.0f} MW ({avg * 24 * 365 / 1_000_000:.1f} TWh annualised)")
    
    print(f"\nTotal (without rooftop): {avg_by_fuel.sum():.0f} MW ({avg_by_fuel.sum() * 24 * 365 / 1_000_000:.1f} TWh annualised)")
    print("\nNote: Rooftop would add approximately 20-30 TWh to this total")

if __name__ == "__main__":
    test_vre_totals()