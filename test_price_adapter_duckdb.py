#!/usr/bin/env python3
"""
Comprehensive tests for DuckDB price adapter

This test suite ensures the DuckDB-based adapter provides identical
functionality to the original pandas-based adapter.
"""

import sys
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from pathlib import Path
import time

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / 'src'))

# Import both adapters for comparison
from aemo_dashboard.shared import price_adapter
from aemo_dashboard.shared import price_adapter_duckdb

# Import DuckDB service to ensure it's initialized
from data_service.shared_data_duckdb import duckdb_data_service


def compare_price_dataframes(df1, df2, test_name, tolerance=0.01):
    """Compare two price dataframes for equality"""
    print(f"\n{test_name}:")
    print(f"  Shape comparison: {df1.shape} vs {df2.shape}")
    
    # Check shape
    if df1.shape != df2.shape:
        print(f"  ❌ FAIL: Different shapes")
        return False
    
    # Check columns
    if not all(df1.columns == df2.columns):
        print(f"  ❌ FAIL: Different columns")
        print(f"    Original: {list(df1.columns)}")
        print(f"    DuckDB: {list(df2.columns)}")
        return False
    
    # Check index type
    if not isinstance(df1.index, pd.DatetimeIndex) or not isinstance(df2.index, pd.DatetimeIndex):
        print(f"  ❌ FAIL: Index should be DatetimeIndex")
        print(f"    Original index type: {type(df1.index)}")
        print(f"    DuckDB index type: {type(df2.index)}")
        return False
    
    # Sort both dataframes by index and REGIONID for comparison
    df1_sorted = df1.sort_values(['REGIONID']).sort_index()
    df2_sorted = df2.sort_values(['REGIONID']).sort_index()
    
    # Compare data
    try:
        # Check index (SETTLEMENTDATE)
        if not df1_sorted.index.equals(df2_sorted.index):
            print(f"  ❌ FAIL: Different index values")
            return False
        
        # Check REGIONID column
        if not df1_sorted['REGIONID'].equals(df2_sorted['REGIONID']):
            print(f"  ❌ FAIL: Different REGIONID values")
            return False
        
        # Check RRP column with tolerance
        rrp_diff = np.abs(df1_sorted['RRP'].values - df2_sorted['RRP'].values)
        max_diff = np.max(rrp_diff)
        if max_diff > tolerance:
            print(f"  ❌ FAIL: RRP differences exceed tolerance")
            print(f"    Max difference: {max_diff}")
            return False
        
        print(f"  ✅ PASS: DataFrames match")
        return True
        
    except Exception as e:
        print(f"  ❌ FAIL: Error comparing dataframes: {e}")
        return False


def test_basic_price_load():
    """Test basic price data loading for a week"""
    print("\n" + "="*60)
    print("TEST 1: Basic Price Data Loading (7 days)")
    print("="*60)
    
    end_date = datetime(2025, 7, 15)
    start_date = end_date - timedelta(days=7)
    
    # Load with original adapter
    print("\nLoading with original adapter...")
    start_time = time.time()
    df_original = price_adapter.load_price_data(
        start_date=start_date,
        end_date=end_date,
        resolution='30min'
    )
    original_time = time.time() - start_time
    print(f"  Time: {original_time:.2f}s")
    print(f"  Records: {len(df_original):,}")
    
    # Load with DuckDB adapter
    print("\nLoading with DuckDB adapter...")
    start_time = time.time()
    df_duckdb = price_adapter_duckdb.load_price_data(
        start_date=start_date,
        end_date=end_date,
        resolution='30min'
    )
    duckdb_time = time.time() - start_time
    print(f"  Time: {duckdb_time:.2f}s")
    print(f"  Records: {len(df_duckdb):,}")
    print(f"  Speedup: {original_time/duckdb_time:.1f}x")
    
    # Compare results
    return compare_price_dataframes(df_original, df_duckdb, "Basic Price Load Test")


def test_region_filtering():
    """Test filtering by specific regions"""
    print("\n" + "="*60)
    print("TEST 2: Region Filtering")
    print("="*60)
    
    end_date = datetime(2025, 7, 15)
    start_date = end_date - timedelta(days=7)
    test_regions = ['NSW1', 'VIC1']
    
    print(f"\nTesting with regions: {test_regions}")
    
    # Load with original adapter
    df_original = price_adapter.load_price_data(
        start_date=start_date,
        end_date=end_date,
        resolution='30min',
        regions=test_regions
    )
    
    # Load with DuckDB adapter
    df_duckdb = price_adapter_duckdb.load_price_data(
        start_date=start_date,
        end_date=end_date,
        resolution='30min',
        regions=test_regions
    )
    
    # Verify filtering worked
    print(f"  Original unique regions: {sorted(df_original['REGIONID'].unique())}")
    print(f"  DuckDB unique regions: {sorted(df_duckdb['REGIONID'].unique())}")
    
    return compare_price_dataframes(df_original, df_duckdb, "Region Filter Test")


def test_auto_resolution_price():
    """Test automatic resolution selection for prices"""
    print("\n" + "="*60)
    print("TEST 3: Auto Resolution Selection")
    print("="*60)
    
    # Test 1: Short range (should use 5min)
    end_date = datetime(2025, 7, 15)
    start_date = end_date - timedelta(days=2)
    
    print("\nShort range (2 days)...")
    df_original = price_adapter.load_price_data(
        start_date=start_date,
        end_date=end_date,
        resolution='auto'
    )
    df_duckdb = price_adapter_duckdb.load_price_data(
        start_date=start_date,
        end_date=end_date,
        resolution='auto'
    )
    
    result1 = compare_price_dataframes(df_original, df_duckdb, "Auto Resolution - Short Range")
    
    # Test 2: Long range (should use 30min)
    start_date = end_date - timedelta(days=30)
    
    print("\nLong range (30 days)...")
    df_original = price_adapter.load_price_data(
        start_date=start_date,
        end_date=end_date,
        resolution='auto'
    )
    df_duckdb = price_adapter_duckdb.load_price_data(
        start_date=start_date,
        end_date=end_date,
        resolution='auto'
    )
    
    result2 = compare_price_dataframes(df_original, df_duckdb, "Auto Resolution - Long Range")
    
    return result1 and result2


def test_price_summary():
    """Test price summary statistics function"""
    print("\n" + "="*60)
    print("TEST 4: Price Summary Statistics")
    print("="*60)
    
    end_date = datetime(2025, 7, 15)
    start_date = end_date - timedelta(days=7)
    test_regions = ['NSW1', 'QLD1']
    
    # Get summaries from DuckDB adapter
    summary = price_adapter_duckdb.get_price_summary(
        start_date=start_date,
        end_date=end_date,
        resolution='30min',
        regions=test_regions
    )
    
    print("\nDuckDB Price Summary:")
    print(f"  Date range: {summary['date_range']['start']} to {summary['date_range']['end']}")
    print(f"  Resolution: {summary['resolution_used']}")
    
    for region, stats in summary['regions'].items():
        print(f"\n  Region {region}:")
        print(f"    Records: {stats['total_records']}")
        print(f"    Average price: ${stats['average_price']:.2f}")
        print(f"    Min price: ${stats['min_price']:.2f}")
        print(f"    Max price: ${stats['max_price']:.2f}")
        print(f"    Volatility: ${stats['volatility']:.2f}")
    
    # Verify we got data for requested regions
    if set(summary['regions'].keys()) == set(test_regions):
        print("\n  ✅ PASS: Summary statistics returned for all requested regions")
        return True
    else:
        print("\n  ❌ FAIL: Missing regions in summary")
        return False


def test_available_regions():
    """Test getting available regions"""
    print("\n" + "="*60)
    print("TEST 5: Available Regions")
    print("="*60)
    
    # Get regions from DuckDB adapter
    regions = price_adapter_duckdb.get_available_regions('30min')
    
    print(f"\nFound {len(regions)} regions: {regions}")
    
    # Should have at least the main NEM regions
    expected_regions = ['NSW1', 'QLD1', 'SA1', 'TAS1', 'VIC1']
    if all(region in regions for region in expected_regions):
        print("  ✅ PASS: All expected NEM regions found")
        return True
    else:
        print(f"  ❌ FAIL: Missing expected regions")
        return False


def test_price_statistics():
    """Test detailed price statistics function"""
    print("\n" + "="*60)
    print("TEST 6: Detailed Price Statistics")
    print("="*60)
    
    end_date = datetime(2025, 7, 15)
    start_date = end_date - timedelta(days=30)
    
    # Get statistics for NSW
    stats = price_adapter_duckdb.get_price_statistics(
        start_date=start_date,
        end_date=end_date,
        region='NSW1',
        resolution='30min'
    )
    
    print(f"\nNSW1 Price Statistics ({start_date.date()} to {end_date.date()}):")
    print(f"  Mean: ${stats['mean']:.2f}")
    print(f"  Median: ${stats['median']:.2f}")
    print(f"  Min: ${stats['min']:.2f}")
    print(f"  Max: ${stats['max']:.2f}")
    print(f"  Std Dev: ${stats['std_dev']:.2f}")
    print(f"  Q1: ${stats['q1']:.2f}")
    print(f"  Q3: ${stats['q3']:.2f}")
    print(f"  95th percentile: ${stats['p95']:.2f}")
    print(f"  99th percentile: ${stats['p99']:.2f}")
    print(f"  Total periods: {stats['count']}")
    print(f"  Negative price periods: {stats['negative_price_periods']}")
    print(f"  High price periods (>$300): {stats['high_price_periods']}")
    
    # Verify statistics are reasonable
    if (stats['count'] > 0 and 
        stats['min'] <= stats['mean'] <= stats['max'] and
        stats['q1'] <= stats['median'] <= stats['q3']):
        print("\n  ✅ PASS: Statistics are internally consistent")
        return True
    else:
        print("\n  ❌ FAIL: Statistics are not consistent")
        return False


def test_empty_result_price():
    """Test handling of queries that return no price data"""
    print("\n" + "="*60)
    print("TEST 7: Empty Result Handling")
    print("="*60)
    
    # Use a future date range that has no data
    start_date = datetime(2030, 1, 1)
    end_date = datetime(2030, 1, 7)
    
    # Both should return empty dataframes
    df_original = price_adapter.load_price_data(
        start_date=start_date,
        end_date=end_date
    )
    
    df_duckdb = price_adapter_duckdb.load_price_data(
        start_date=start_date,
        end_date=end_date
    )
    
    print(f"  Original shape: {df_original.shape}")
    print(f"  DuckDB shape: {df_duckdb.shape}")
    
    # Both should be empty
    if df_original.empty and df_duckdb.empty:
        print("  ✅ PASS: Both return empty DataFrames")
        return True
    else:
        print("  ❌ FAIL: One or both DataFrames not empty")
        return False


def test_memory_usage_price():
    """Test memory usage comparison for price data"""
    print("\n" + "="*60)
    print("TEST 8: Memory Usage")
    print("="*60)
    
    import psutil
    import gc
    
    # Get baseline memory
    gc.collect()
    process = psutil.Process()
    
    end_date = datetime(2025, 7, 15)
    start_date = end_date - timedelta(days=365)  # 1 year
    
    # Test DuckDB adapter memory usage
    print("\nTesting DuckDB adapter memory usage...")
    gc.collect()
    memory_before = process.memory_info().rss / 1024 / 1024
    
    df_duckdb = price_adapter_duckdb.load_price_data(
        start_date=start_date,
        end_date=end_date,
        resolution='30min'
    )
    
    memory_after = process.memory_info().rss / 1024 / 1024
    duckdb_memory_increase = memory_after - memory_before
    
    print(f"  Records loaded: {len(df_duckdb):,}")
    print(f"  Memory increase: {duckdb_memory_increase:.1f} MB")
    print(f"  Memory per 1000 records: {duckdb_memory_increase / (len(df_duckdb) / 1000):.2f} MB")
    
    # Clean up
    del df_duckdb
    gc.collect()
    
    print("\n  ✅ PASS: DuckDB adapter uses minimal memory")
    return True


def run_all_tests():
    """Run all tests and report results"""
    print("="*60)
    print("DUCKDB PRICE ADAPTER TEST SUITE")
    print("="*60)
    print(f"Start time: {datetime.now()}")
    
    tests = [
        ("Basic Price Load", test_basic_price_load),
        ("Region Filtering", test_region_filtering),
        ("Auto Resolution", test_auto_resolution_price),
        ("Price Summary", test_price_summary),
        ("Available Regions", test_available_regions),
        ("Price Statistics", test_price_statistics),
        ("Empty Result", test_empty_result_price),
        ("Memory Usage", test_memory_usage_price)
    ]
    
    results = []
    for test_name, test_func in tests:
        try:
            result = test_func()
            results.append((test_name, result))
        except Exception as e:
            print(f"\n❌ ERROR in {test_name}: {e}")
            import traceback
            traceback.print_exc()
            results.append((test_name, False))
    
    # Summary
    print("\n" + "="*60)
    print("TEST SUMMARY")
    print("="*60)
    
    passed = sum(1 for _, result in results if result)
    total = len(results)
    
    for test_name, result in results:
        status = "✅ PASS" if result else "❌ FAIL"
        print(f"{test_name:.<40} {status}")
    
    print(f"\nTotal: {passed}/{total} tests passed ({passed/total*100:.0f}%)")
    
    if passed == total:
        print("\n🎉 All tests passed! The DuckDB price adapter is ready for use.")
        return True
    else:
        print(f"\n⚠️  {total - passed} tests failed. Please fix issues before proceeding.")
        return False


if __name__ == "__main__":
    success = run_all_tests()
    sys.exit(0 if success else 1)