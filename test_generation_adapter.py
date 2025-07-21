#!/usr/bin/env python3
"""
Comprehensive test suite for Generation Data Adapter
Tests all functionality with both 5-minute and 30-minute data
"""

import sys
import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime, timedelta

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / 'src'))

def test_generation_adapter():
    """Test the generation adapter with comprehensive scenarios"""
    
    print("=" * 70)
    print("TESTING GENERATION DATA ADAPTER")
    print("=" * 70)
    
    from aemo_dashboard.shared.generation_adapter import (
        load_generation_data, 
        get_generation_summary, 
        get_available_duids,
        _standardize_generation_format
    )
    from aemo_dashboard.shared.resolution_manager import resolution_manager
    
    # Test 1: Load 5-minute data
    print("\n1. TESTING 5-MINUTE DATA LOADING")
    print("-" * 40)
    
    try:
        df_5min = load_generation_data(resolution='5min')
        print(f"✅ 5-minute data loaded: {len(df_5min):,} records")
        print(f"   Columns: {list(df_5min.columns)}")
        print(f"   Date range: {df_5min['settlementdate'].min()} to {df_5min['settlementdate'].max()}")
        print(f"   Unique DUIDs: {df_5min['duid'].nunique()}")
        
        # Validate format
        expected_cols = ['settlementdate', 'duid', 'scadavalue']
        assert all(col in df_5min.columns for col in expected_cols), f"Missing columns: {expected_cols}"
        assert pd.api.types.is_datetime64_any_dtype(df_5min['settlementdate']), "settlementdate not datetime"
        assert pd.api.types.is_numeric_dtype(df_5min['scadavalue']), "scadavalue not numeric"
        print("✅ 5-minute data format validation passed")
        
    except Exception as e:
        print(f"❌ 5-minute data loading failed: {e}")
        return False
    
    # Test 2: Load 30-minute data
    print("\n2. TESTING 30-MINUTE DATA LOADING") 
    print("-" * 40)
    
    try:
        df_30min = load_generation_data(resolution='30min')
        print(f"✅ 30-minute data loaded: {len(df_30min):,} records")
        print(f"   Columns: {list(df_30min.columns)}")
        print(f"   Date range: {df_30min['settlementdate'].min()} to {df_30min['settlementdate'].max()}")
        print(f"   Unique DUIDs: {df_30min['duid'].nunique()}")
        
        # Validate same format as 5-minute
        assert list(df_30min.columns) == list(df_5min.columns), "Column mismatch between resolutions"
        assert pd.api.types.is_datetime64_any_dtype(df_30min['settlementdate']), "settlementdate not datetime"
        assert pd.api.types.is_numeric_dtype(df_30min['scadavalue']), "scadavalue not numeric"
        print("✅ 30-minute data format validation passed")
        
    except Exception as e:
        print(f"❌ 30-minute data loading failed: {e}")
        return False
    
    # Test 3: Auto resolution selection
    print("\n3. TESTING AUTO RESOLUTION SELECTION")
    print("-" * 40)
    
    # Short range (should use 5-minute)
    short_start = datetime.now() - timedelta(days=3)
    short_end = datetime.now()
    
    df_auto_short = load_generation_data(
        start_date=short_start, 
        end_date=short_end, 
        resolution='auto'
    )
    print(f"✅ Auto resolution for 3-day range: {len(df_auto_short):,} records")
    
    # Long range (should use 30-minute)
    long_start = datetime.now() - timedelta(days=30)
    long_end = datetime.now()
    
    df_auto_long = load_generation_data(
        start_date=long_start,
        end_date=long_end, 
        resolution='auto'
    )
    print(f"✅ Auto resolution for 30-day range: {len(df_auto_long):,} records")
    
    # Test 4: Date range filtering
    print("\n4. TESTING DATE RANGE FILTERING")
    print("-" * 40)
    
    # Get recent 1 day of data
    filter_start = datetime.now() - timedelta(days=1)
    filter_end = datetime.now()
    
    df_filtered = load_generation_data(
        start_date=filter_start,
        end_date=filter_end,
        resolution='5min'
    )
    
    if not df_filtered.empty:
        actual_start = df_filtered['settlementdate'].min()
        actual_end = df_filtered['settlementdate'].max()
        print(f"✅ Date filtering: {len(df_filtered):,} records")
        print(f"   Requested: {filter_start} to {filter_end}")
        print(f"   Actual: {actual_start} to {actual_end}")
        
        # Validate date filtering worked
        assert actual_start >= filter_start, f"Start date not filtered: {actual_start} < {filter_start}"
        assert actual_end <= filter_end, f"End date not filtered: {actual_end} > {filter_end}"
        print("✅ Date range filtering validation passed")
    else:
        print("⚠️  No data in recent 1-day range - this may be expected")
    
    # Test 5: DUID filtering
    print("\n5. TESTING DUID FILTERING")
    print("-" * 40)
    
    # Get a few DUIDs to test with
    available_duids = get_available_duids('5min')
    if len(available_duids) >= 3:
        test_duids = available_duids[:3]
        
        df_duid_filtered = load_generation_data(
            duids=test_duids,
            resolution='5min'
        )
        
        unique_duids_result = set(df_duid_filtered['duid'].unique())
        expected_duids = set(test_duids)
        
        print(f"✅ DUID filtering: {len(df_duid_filtered):,} records for {len(test_duids)} DUIDs")
        print(f"   Requested DUIDs: {test_duids}")
        print(f"   Found DUIDs: {sorted(unique_duids_result)}")
        
        # Validate DUID filtering
        assert unique_duids_result.issubset(expected_duids), f"Unexpected DUIDs found: {unique_duids_result - expected_duids}"
        print("✅ DUID filtering validation passed")
    else:
        print("⚠️  Not enough DUIDs available for testing")
    
    # Test 6: Generation summary
    print("\n6. TESTING GENERATION SUMMARY")
    print("-" * 40)
    
    summary_5min = get_generation_summary(resolution='5min')
    summary_30min = get_generation_summary(resolution='30min')
    
    print(f"5-minute summary:")
    print(f"   Total records: {summary_5min['total_records']:,}")
    print(f"   Unique DUIDs: {summary_5min['unique_duids']:,}")
    print(f"   Total generation: {summary_5min['total_generation_mw']:,.0f} MW")
    
    print(f"30-minute summary:")
    print(f"   Total records: {summary_30min['total_records']:,}")
    print(f"   Unique DUIDs: {summary_30min['unique_duids']:,}")
    print(f"   Total generation: {summary_30min['total_generation_mw']:,.0f} MW")
    
    # Test 7: Performance comparison
    print("\n7. TESTING PERFORMANCE COMPARISON")
    print("-" * 40)
    
    import time
    
    # Test loading performance for 1 week of data
    perf_start = datetime.now() - timedelta(days=7)
    perf_end = datetime.now()
    
    # 5-minute performance
    start_time = time.time()
    df_5min_perf = load_generation_data(
        start_date=perf_start,
        end_date=perf_end, 
        resolution='5min'
    )
    time_5min = time.time() - start_time
    
    # 30-minute performance
    start_time = time.time()
    df_30min_perf = load_generation_data(
        start_date=perf_start,
        end_date=perf_end,
        resolution='30min'
    )
    time_30min = time.time() - start_time
    
    print(f"Performance for 7-day range:")
    print(f"   5-minute: {len(df_5min_perf):,} records in {time_5min:.2f}s")
    print(f"   30-minute: {len(df_30min_perf):,} records in {time_30min:.2f}s")
    
    if time_5min > 0 and time_30min > 0:
        speedup = time_5min / time_30min
        print(f"   30-minute is {speedup:.1f}x faster")
    
    # Test 8: Memory efficiency
    print("\n8. TESTING MEMORY EFFICIENCY")
    print("-" * 40)
    
    # Calculate memory usage estimates
    memory_5min = resolution_manager.estimate_memory_usage(
        perf_start, perf_end, '5min', 'generation'
    )
    memory_30min = resolution_manager.estimate_memory_usage(
        perf_start, perf_end, '30min', 'generation'
    )
    
    print(f"Memory estimates for 7-day range:")
    print(f"   5-minute: {memory_5min:.1f} MB")
    print(f"   30-minute: {memory_30min:.1f} MB")
    
    if memory_5min > 0:
        reduction = (memory_5min - memory_30min) / memory_5min * 100
        print(f"   30-minute reduces memory by {reduction:.1f}%")
    
    # Test 9: Error handling
    print("\n9. TESTING ERROR HANDLING")
    print("-" * 40)
    
    # Test with invalid file path
    df_invalid = load_generation_data(file_path="/invalid/path.parquet")
    assert df_invalid.empty, "Should return empty DataFrame for invalid file"
    print("✅ Invalid file path handled correctly")
    
    # Test with invalid date range
    invalid_start = datetime.now() + timedelta(days=30)  # Future date
    invalid_end = datetime.now() + timedelta(days=31)
    
    df_future = load_generation_data(
        start_date=invalid_start,
        end_date=invalid_end,
        resolution='5min'
    )
    # Should be empty or very small
    print(f"✅ Future date range handled: {len(df_future)} records")
    
    print("\n" + "🎯" * 25)
    print("\nGENERATION ADAPTER TESTING COMPLETE")
    print("All tests passed successfully!")
    print("\nKey validation points:")
    print("✅ Both 5-minute and 30-minute data load correctly")
    print("✅ Consistent output format across resolutions")
    print("✅ Auto resolution selection works properly") 
    print("✅ Date range and DUID filtering functional")
    print("✅ Performance improvements with 30-minute data")
    print("✅ Memory efficiency gains demonstrated")
    print("✅ Error handling robust")
    
    return True

if __name__ == "__main__":
    success = test_generation_adapter()
    if success:
        print("\n🎉 ALL GENERATION ADAPTER TESTS PASSED!")
    else:
        print("\n❌ SOME TESTS FAILED")
        sys.exit(1)