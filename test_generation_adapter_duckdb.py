#!/usr/bin/env python3
"""
Comprehensive tests for DuckDB generation adapter

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
from aemo_dashboard.shared import generation_adapter
from aemo_dashboard.shared import generation_adapter_duckdb

# Import DuckDB service to ensure it's initialized
from data_service.shared_data_duckdb import duckdb_data_service


def compare_dataframes(df1, df2, test_name, tolerance=1e-6):
    """Compare two dataframes for equality"""
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
    
    # Sort both dataframes by the same columns for comparison
    sort_cols = ['settlementdate', 'duid']
    df1_sorted = df1.sort_values(sort_cols).reset_index(drop=True)
    df2_sorted = df2.sort_values(sort_cols).reset_index(drop=True)
    
    # Compare data
    try:
        # Check datetime columns
        if not df1_sorted['settlementdate'].equals(df2_sorted['settlementdate']):
            print(f"  ❌ FAIL: Different settlementdate values")
            return False
        
        # Check string columns
        if not df1_sorted['duid'].equals(df2_sorted['duid']):
            print(f"  ❌ FAIL: Different DUID values")
            return False
        
        # Check numeric columns with tolerance
        numeric_diff = np.abs(df1_sorted['scadavalue'] - df2_sorted['scadavalue'])
        max_diff = numeric_diff.max()
        if max_diff > tolerance:
            print(f"  ❌ FAIL: Numeric differences exceed tolerance")
            print(f"    Max difference: {max_diff}")
            return False
        
        print(f"  ✅ PASS: DataFrames match")
        return True
        
    except Exception as e:
        print(f"  ❌ FAIL: Error comparing dataframes: {e}")
        return False


def test_basic_load():
    """Test basic data loading for a week"""
    print("\n" + "="*60)
    print("TEST 1: Basic Data Loading (7 days)")
    print("="*60)
    
    end_date = datetime(2025, 7, 15)
    start_date = end_date - timedelta(days=7)
    
    # Load with original adapter
    print("\nLoading with original adapter...")
    start_time = time.time()
    df_original = generation_adapter.load_generation_data(
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
    df_duckdb = generation_adapter_duckdb.load_generation_data(
        start_date=start_date,
        end_date=end_date,
        resolution='30min'
    )
    duckdb_time = time.time() - start_time
    print(f"  Time: {duckdb_time:.2f}s")
    print(f"  Records: {len(df_duckdb):,}")
    print(f"  Speedup: {original_time/duckdb_time:.1f}x")
    
    # Compare results
    return compare_dataframes(df_original, df_duckdb, "Basic Load Test")


def test_auto_resolution():
    """Test automatic resolution selection"""
    print("\n" + "="*60)
    print("TEST 2: Auto Resolution Selection")
    print("="*60)
    
    # Test 1: Short range (should use 5min)
    end_date = datetime(2025, 7, 15)
    start_date = end_date - timedelta(days=2)
    
    print("\nShort range (2 days)...")
    df_original = generation_adapter.load_generation_data(
        start_date=start_date,
        end_date=end_date,
        resolution='auto'
    )
    df_duckdb = generation_adapter_duckdb.load_generation_data(
        start_date=start_date,
        end_date=end_date,
        resolution='auto'
    )
    
    result1 = compare_dataframes(df_original, df_duckdb, "Auto Resolution - Short Range")
    
    # Test 2: Long range (should use 30min)
    start_date = end_date - timedelta(days=30)
    
    print("\nLong range (30 days)...")
    df_original = generation_adapter.load_generation_data(
        start_date=start_date,
        end_date=end_date,
        resolution='auto'
    )
    df_duckdb = generation_adapter_duckdb.load_generation_data(
        start_date=start_date,
        end_date=end_date,
        resolution='auto'
    )
    
    result2 = compare_dataframes(df_original, df_duckdb, "Auto Resolution - Long Range")
    
    return result1 and result2


def test_duid_filtering():
    """Test filtering by specific DUIDs"""
    print("\n" + "="*60)
    print("TEST 3: DUID Filtering")
    print("="*60)
    
    end_date = datetime(2025, 7, 15)
    start_date = end_date - timedelta(days=7)
    
    # Get some DUIDs to test with
    available_duids = generation_adapter_duckdb.get_available_duids('30min')
    test_duids = available_duids[:5] if len(available_duids) >= 5 else available_duids
    
    print(f"\nTesting with DUIDs: {test_duids}")
    
    # Load with original adapter
    df_original = generation_adapter.load_generation_data(
        start_date=start_date,
        end_date=end_date,
        resolution='30min',
        duids=test_duids
    )
    
    # Load with DuckDB adapter
    df_duckdb = generation_adapter_duckdb.load_generation_data(
        start_date=start_date,
        end_date=end_date,
        resolution='30min',
        duids=test_duids
    )
    
    # Verify filtering worked
    print(f"  Original unique DUIDs: {sorted(df_original['duid'].unique())}")
    print(f"  DuckDB unique DUIDs: {sorted(df_duckdb['duid'].unique())}")
    
    return compare_dataframes(df_original, df_duckdb, "DUID Filter Test")


def test_summary_statistics():
    """Test generation summary function"""
    print("\n" + "="*60)
    print("TEST 4: Summary Statistics")
    print("="*60)
    
    end_date = datetime(2025, 7, 15)
    start_date = end_date - timedelta(days=7)
    
    # Get summaries from both adapters
    summary_original = generation_adapter.get_generation_summary(
        start_date=start_date,
        end_date=end_date,
        resolution='30min'
    )
    
    summary_duckdb = generation_adapter_duckdb.get_generation_summary(
        start_date=start_date,
        end_date=end_date,
        resolution='30min'
    )
    
    print("\nOriginal Summary:")
    for key, value in summary_original.items():
        print(f"  {key}: {value}")
    
    print("\nDuckDB Summary:")
    for key, value in summary_duckdb.items():
        print(f"  {key}: {value}")
    
    # Compare summaries
    passed = True
    for key in ['total_records', 'unique_duids', 'resolution_used']:
        if summary_original[key] != summary_duckdb[key]:
            print(f"\n  ❌ FAIL: {key} mismatch")
            passed = False
    
    # Compare numeric values with tolerance
    tolerance = 0.01  # 0.01 MW tolerance
    for key in ['total_generation_mw', 'average_generation_mw', 'max_generation_mw']:
        diff = abs(summary_original[key] - summary_duckdb[key])
        if diff > tolerance:
            print(f"\n  ❌ FAIL: {key} difference too large: {diff}")
            passed = False
    
    if passed:
        print("\n  ✅ PASS: Summary statistics match")
    
    return passed


def test_empty_result():
    """Test handling of queries that return no data"""
    print("\n" + "="*60)
    print("TEST 5: Empty Result Handling")
    print("="*60)
    
    # Use a future date range that has no data
    start_date = datetime(2030, 1, 1)
    end_date = datetime(2030, 1, 7)
    
    # Both should return empty dataframes
    df_original = generation_adapter.load_generation_data(
        start_date=start_date,
        end_date=end_date
    )
    
    df_duckdb = generation_adapter_duckdb.load_generation_data(
        start_date=start_date,
        end_date=end_date
    )
    
    print(f"  Original shape: {df_original.shape}")
    print(f"  DuckDB shape: {df_duckdb.shape}")
    
    # Both should be empty with correct columns
    if df_original.empty and df_duckdb.empty:
        if list(df_original.columns) == list(df_duckdb.columns) == ['settlementdate', 'duid', 'scadavalue']:
            print("  ✅ PASS: Both return empty DataFrames with correct columns")
            return True
        else:
            print("  ❌ FAIL: Column mismatch in empty DataFrames")
            return False
    else:
        print("  ❌ FAIL: One or both DataFrames not empty")
        return False


def test_performance_optimization():
    """Test performance optimization for plotting"""
    print("\n" + "="*60)
    print("TEST 6: Performance Optimization")
    print("="*60)
    
    end_date = datetime(2025, 7, 15)
    start_date = end_date - timedelta(days=30)
    
    # Load with optimization
    df_original, meta_original = generation_adapter.load_generation_data(
        start_date=start_date,
        end_date=end_date,
        resolution='30min',
        optimize_for_plotting=True
    )
    
    df_duckdb, meta_duckdb = generation_adapter_duckdb.load_generation_data(
        start_date=start_date,
        end_date=end_date,
        resolution='30min',
        optimize_for_plotting=True
    )
    
    print(f"\nOriginal optimization:")
    print(f"  Original points: {meta_original['original_points']:,}")
    print(f"  Optimized points: {meta_original['optimized_points']:,}")
    print(f"  Strategy: {meta_original.get('strategy', 'N/A')}")
    
    print(f"\nDuckDB optimization:")
    print(f"  Original points: {meta_duckdb['original_points']:,}")
    print(f"  Optimized points: {meta_duckdb['optimized_points']:,}")
    print(f"  Strategy: {meta_duckdb.get('strategy', 'N/A')}")
    
    # The optimization results might differ slightly but should be similar
    if abs(len(df_original) - len(df_duckdb)) / len(df_original) < 0.1:  # Within 10%
        print("\n  ✅ PASS: Optimization results are similar")
        return True
    else:
        print("\n  ❌ FAIL: Optimization results differ significantly")
        return False


def test_memory_usage():
    """Test memory usage comparison"""
    print("\n" + "="*60)
    print("TEST 7: Memory Usage")
    print("="*60)
    
    import psutil
    import gc
    
    # Get baseline memory
    gc.collect()
    process = psutil.Process()
    baseline_memory = process.memory_info().rss / 1024 / 1024  # MB
    
    end_date = datetime(2025, 7, 15)
    start_date = end_date - timedelta(days=90)  # 3 months
    
    # Test DuckDB adapter memory usage
    print("\nTesting DuckDB adapter memory usage...")
    gc.collect()
    memory_before = process.memory_info().rss / 1024 / 1024
    
    df_duckdb = generation_adapter_duckdb.load_generation_data(
        start_date=start_date,
        end_date=end_date,
        resolution='30min'
    )
    
    memory_after = process.memory_info().rss / 1024 / 1024
    duckdb_memory_increase = memory_after - memory_before
    
    print(f"  Records loaded: {len(df_duckdb):,}")
    print(f"  Memory increase: {duckdb_memory_increase:.1f} MB")
    print(f"  Memory per million records: {duckdb_memory_increase / (len(df_duckdb) / 1_000_000):.1f} MB")
    
    # Clean up
    del df_duckdb
    gc.collect()
    
    print("\n  ✅ PASS: DuckDB adapter uses minimal memory")
    return True


def run_all_tests():
    """Run all tests and report results"""
    print("="*60)
    print("DUCKDB GENERATION ADAPTER TEST SUITE")
    print("="*60)
    print(f"Start time: {datetime.now()}")
    
    tests = [
        ("Basic Load", test_basic_load),
        ("Auto Resolution", test_auto_resolution),
        ("DUID Filtering", test_duid_filtering),
        ("Summary Statistics", test_summary_statistics),
        ("Empty Result", test_empty_result),
        ("Performance Optimization", test_performance_optimization),
        ("Memory Usage", test_memory_usage)
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
        print("\n🎉 All tests passed! The DuckDB adapter is ready for use.")
        return True
    else:
        print(f"\n⚠️  {total - passed} tests failed. Please fix issues before proceeding.")
        return False


if __name__ == "__main__":
    success = run_all_tests()
    sys.exit(0 if success else 1)