#!/usr/bin/env python3
"""
Test the fixed penetration tab data loading.
"""
import os
import sys
from pathlib import Path

# Set environment variable BEFORE any imports
os.environ['USE_DUCKDB'] = 'true'

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / 'src'))

from aemo_dashboard.penetration import PenetrationTab

def test_data_loading():
    """Test the data loading with rooftop included."""
    print("Testing Penetration Tab data loading with rooftop...")
    
    # Create tab instance
    tab = PenetrationTab()
    
    # Get data for testing
    years = [2024]
    data = tab._get_generation_data(years)
    
    print(f"\nTotal data shape: {data.shape}")
    print(f"Fuel types: {sorted(data['fuel_type'].unique())}")
    
    # Check VRE components
    for fuel in ['Wind', 'Solar', 'Rooftop']:
        fuel_data = data[data['fuel_type'] == fuel]
        if not fuel_data.empty:
            print(f"\n{fuel}: {len(fuel_data)} records")
            print(f"  Generation range: {fuel_data['total_generation_mw'].min():.2f} - {fuel_data['total_generation_mw'].max():.2f} MW")
        else:
            print(f"\n{fuel}: NO DATA")
    
    # Test the chart creation
    print("\nTesting chart creation...")
    try:
        chart = tab._create_vre_production_chart()
        print("✓ Chart created successfully")
    except Exception as e:
        print(f"✗ Chart creation failed: {e}")

if __name__ == "__main__":
    test_data_loading()