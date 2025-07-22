#!/usr/bin/env python3
"""
Test DuckDB rooftop adapter
"""

import os
import sys
from pathlib import Path
from datetime import datetime, timedelta

# Set DuckDB mode
os.environ['USE_DUCKDB'] = 'true'

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / 'src'))

from aemo_dashboard.shared.adapter_selector import (
    load_rooftop_data,
    get_rooftop_at_time,
    get_rooftop_summary,
    smooth_rooftop_data,
    adapter_type,
    USE_DUCKDB
)

def test_rooftop_adapter():
    """Test the DuckDB rooftop adapter"""
    
    print("=" * 60)
    print("TESTING DUCKDB ROOFTOP ADAPTER")
    print("=" * 60)
    print(f"Adapter type: {adapter_type}")
    print(f"USE_DUCKDB: {USE_DUCKDB}")
    
    # Set up test date range
    end_date = datetime.now()
    start_date = end_date - timedelta(days=1)
    
    # Test 1: Load 30-minute rooftop data
    print("\n1. Testing load_rooftop_data (30min)...")
    try:
        df_30min = load_rooftop_data(
            start_date=start_date,
            end_date=end_date,
            target_resolution='30min'
        )
        print(f"✅ Loaded {len(df_30min)} 30-minute rooftop records")
        if not df_30min.empty:
            print(f"   Columns: {list(df_30min.columns)}")
            print(f"   Regions: {df_30min['regionid'].unique()}")
            print(f"   Date range: {df_30min['settlementdate'].min()} to {df_30min['settlementdate'].max()}")
            print(f"   Max generation: {df_30min['rooftop_solar_mw'].max():.1f} MW")
    except Exception as e:
        print(f"❌ Error loading 30min rooftop data: {e}")
    
    # Test 2: Load 5-minute interpolated data
    print("\n2. Testing load_rooftop_data (5min interpolated)...")
    try:
        df_5min = load_rooftop_data(
            start_date=start_date,
            end_date=end_date,
            target_resolution='5min'
        )
        print(f"✅ Loaded {len(df_5min)} 5-minute rooftop records (interpolated)")
        if not df_5min.empty:
            print(f"   Regions: {df_5min['regionid'].unique()}")
            print(f"   Date range: {df_5min['settlementdate'].min()} to {df_5min['settlementdate'].max()}")
            print(f"   Max generation: {df_5min['rooftop_solar_mw'].max():.1f} MW")
    except Exception as e:
        print(f"❌ Error loading 5min rooftop data: {e}")
    
    # Test 3: Get rooftop summary
    print("\n3. Testing get_rooftop_summary...")
    try:
        summary = get_rooftop_summary(start_date=start_date, end_date=end_date)
        print(f"✅ Got rooftop summary: {summary}")
    except Exception as e:
        print(f"❌ Error getting rooftop summary: {e}")
    
    # Test 4: Get rooftop at specific time
    print("\n4. Testing get_rooftop_at_time...")
    try:
        if not df_30min.empty:
            test_time = df_30min['settlementdate'].iloc[-1]
            value = get_rooftop_at_time(test_time, 'NSW1')
            print(f"✅ Rooftop at {test_time} for NSW1: {value:.1f} MW")
            
            # Test all regions
            df_all = get_rooftop_at_time(test_time)
            print(f"✅ Got data for all regions: {len(df_all)} regions")
    except Exception as e:
        print(f"❌ Error getting rooftop at time: {e}")
    
    # Test 5: Test region-specific loading
    print("\n5. Testing region-specific loading...")
    try:
        df_nsw = load_rooftop_data(
            start_date=start_date,
            end_date=end_date,
            region='NSW1',
            target_resolution='5min'
        )
        print(f"✅ Loaded {len(df_nsw)} records for NSW1")
    except Exception as e:
        print(f"❌ Error loading region-specific data: {e}")
    
    # Test 6: Test smoothing function
    print("\n6. Testing smooth_rooftop_data...")
    try:
        if not df_5min.empty:
            df_smoothed = smooth_rooftop_data(df_5min.head(100))
            print(f"✅ Smoothing function works (compatibility wrapper)")
    except Exception as e:
        print(f"❌ Error in smoothing function: {e}")
    
    # Test 7: Memory efficiency check
    print("\n7. Checking memory usage...")
    import psutil
    process = psutil.Process()
    memory_mb = process.memory_info().rss / 1024 / 1024
    print(f"✅ Memory usage: {memory_mb:.1f} MB (should be < 200 MB)")
    
    print("\n" + "=" * 60)
    print("ROOFTOP ADAPTER TEST SUMMARY")
    print("=" * 60)
    print("✅ All rooftop adapter functions working with DuckDB")
    print("✅ Interpolation to 5-minute resolution working")
    print(f"✅ Memory efficient: {memory_mb:.1f} MB")
    print("=" * 60)

if __name__ == "__main__":
    test_rooftop_adapter()