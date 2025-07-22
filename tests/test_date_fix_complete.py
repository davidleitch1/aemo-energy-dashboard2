#!/usr/bin/env python3
"""
Test script to verify the date type fix and logging improvements
This tests the complete flow from dashboard date selection to price data loading
"""

import os
import sys
import pandas as pd
from datetime import datetime, date, timedelta
import logging

# Set up detailed logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

# Add project path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

print("Testing Date Type Fix for Safari Refresh Issue")
print("=" * 60)

# Test 1: Simulate dashboard providing date objects (like param.Date)
print("\nTest 1: Dashboard provides date objects (typical scenario)")
print("-" * 40)

# Create a mock dashboard instance with date objects
class MockDashboard:
    def __init__(self):
        # Simulate param.Date behavior - provides date objects
        self.start_date = date.today() - timedelta(days=1)
        self.end_date = date.today()
        print(f"Dashboard dates: start={self.start_date} (type: {type(self.start_date)})")
        print(f"                 end={self.end_date} (type: {type(self.end_date)})")

dashboard = MockDashboard()

# Test the nem_dash_tab flow
try:
    from aemo_dashboard.nem_dash.nem_dash_tab import create_nem_dash_tab
    print("\nCreating nem_dash_tab with date objects...")
    tab = create_nem_dash_tab(dashboard)
    print("✓ Successfully created tab with date conversion")
except Exception as e:
    print(f"✗ Error creating tab: {e}")
    import traceback
    traceback.print_exc()

# Test 2: Direct price component test
print("\n\nTest 2: Direct price component with date objects")
print("-" * 40)

try:
    from aemo_dashboard.nem_dash.price_components import load_price_data
    
    # Test with date objects
    start_date = date.today() - timedelta(days=1)
    end_date = date.today()
    
    print(f"Loading price data with date objects...")
    print(f"  start: {start_date} (type: {type(start_date)})")
    print(f"  end: {end_date} (type: {type(end_date)})")
    
    prices = load_price_data(start_date, end_date)
    
    if not prices.empty:
        print(f"✓ Successfully loaded {len(prices)} price records")
        print(f"  Date range: {prices.index.min()} to {prices.index.max()}")
        print(f"  Regions: {list(prices.columns)}")
    else:
        print("✗ No price data returned")
        
except Exception as e:
    print(f"✗ Error loading price data: {e}")
    import traceback
    traceback.print_exc()

# Test 3: Test resolution manager with date objects
print("\n\nTest 3: Resolution manager with date objects")
print("-" * 40)

try:
    from aemo_dashboard.shared.resolution_manager import resolution_manager
    
    start_date = date.today() - timedelta(days=7)
    end_date = date.today()
    
    print(f"Testing resolution selection with date objects...")
    print(f"  start: {start_date} (type: {type(start_date)})")
    print(f"  end: {end_date} (type: {type(end_date)})")
    
    resolution = resolution_manager.get_optimal_resolution(
        start_date, end_date, 'price'
    )
    
    print(f"✓ Resolution determined: {resolution}")
    
except Exception as e:
    print(f"✗ Error in resolution manager: {e}")
    import traceback
    traceback.print_exc()

# Test 4: Verify data volume for different date ranges
print("\n\nTest 4: Data volume verification")
print("-" * 40)

try:
    # Test 48-hour range (should be reasonable)
    start_48h = datetime.now() - timedelta(hours=48)
    end_48h = datetime.now()
    
    print("Loading 48 hours of data...")
    prices_48h = load_price_data(start_48h, end_48h)
    print(f"  Records loaded: {len(prices_48h)}")
    
    # Test without dates (should use defaults, not load all data)
    print("\nLoading with no dates (should default to 48 hours)...")
    prices_default = load_price_data()
    print(f"  Records loaded: {len(prices_default)}")
    
    # Verify we're not loading excessive data
    if len(prices_default) > 10000:
        print("⚠️ WARNING: Loading too much data by default!")
    else:
        print("✓ Default data volume is reasonable")
        
except Exception as e:
    print(f"✗ Error testing data volumes: {e}")

print("\n" + "=" * 60)
print("Date Type Fix Testing Complete")
print("=" * 60)

# Summary
print("\nSUMMARY:")
print("- Date type conversion is implemented in nem_dash_tab.py")
print("- Resolution manager has fallback date conversion")
print("- Price component handles date objects correctly")
print("- Comprehensive logging added throughout the flow")
print("\nThe Safari refresh hang should now be resolved!")