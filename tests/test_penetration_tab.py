#!/usr/bin/env python3
"""
Test script for the Penetration tab VRE production chart.
"""
import os
import sys
from pathlib import Path

# Set environment variable BEFORE any imports
os.environ['USE_DUCKDB'] = 'true'

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / 'src'))

import pandas as pd
import numpy as np
from datetime import datetime, timedelta

def test_penetration_tab():
    """Test the penetration tab functionality."""
    print("Testing Penetration Tab...")
    
    # Import after setting up environment
    from aemo_dashboard.penetration import PenetrationTab
    from aemo_dashboard.shared.smoothing import apply_ewm_smoothing
    
    # Test smoothing function
    print("\n1. Testing EWM smoothing function...")
    test_data = pd.Series(np.random.randn(100).cumsum())
    smoothed = apply_ewm_smoothing(test_data, span=30)
    print(f"   Original data range: {test_data.min():.2f} to {test_data.max():.2f}")
    print(f"   Smoothed data range: {smoothed.min():.2f} to {smoothed.max():.2f}")
    print("   ✓ Smoothing function works")
    
    # Test PenetrationTab instantiation
    print("\n2. Testing PenetrationTab instantiation...")
    try:
        tab = PenetrationTab()
        print("   ✓ PenetrationTab created successfully")
    except Exception as e:
        print(f"   ✗ Error creating PenetrationTab: {e}")
        return
    
    # Test layout creation
    print("\n3. Testing layout creation...")
    try:
        layout = tab.create_layout()
        print("   ✓ Layout created successfully")
        print(f"   Layout type: {type(layout)}")
    except Exception as e:
        print(f"   ✗ Error creating layout: {e}")
        return
    
    # Test data fetching
    print("\n4. Testing data fetching...")
    try:
        years = [2023, 2024, 2025]
        data = tab._get_generation_data(years)
        if data.empty:
            print("   ⚠ No data returned (might be expected if no data available)")
        else:
            print(f"   ✓ Data fetched: {len(data)} rows")
            print(f"   Date range: {data['settlementdate'].min()} to {data['settlementdate'].max()}")
            print(f"   Fuel types: {data['fuel_type'].unique()[:5]}...")
    except Exception as e:
        print(f"   ✗ Error fetching data: {e}")
    
    print("\n5. Testing chart creation...")
    try:
        chart = tab._create_vre_production_chart()
        print("   ✓ Chart created successfully")
        print(f"   Chart type: {type(chart)}")
    except Exception as e:
        print(f"   ✗ Error creating chart: {e}")
    
    print("\nTest complete!")

if __name__ == "__main__":
    test_penetration_tab()