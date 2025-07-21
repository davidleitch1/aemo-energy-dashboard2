#!/usr/bin/env python3
"""
Test the GenerationQueryManager
"""

import os
import sys
import time
import psutil
from pathlib import Path
from datetime import datetime, timedelta

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / 'src'))

# Set DuckDB mode
os.environ['USE_DUCKDB'] = 'true'

from aemo_dashboard.generation.generation_query_manager import GenerationQueryManager
from aemo_dashboard.shared.logging_config import setup_logging

# Set up logging
setup_logging()


def get_memory_usage():
    """Get current memory usage in MB"""
    process = psutil.Process()
    return process.memory_info().rss / 1024 / 1024


def test_generation_query_manager():
    """Test the GenerationQueryManager with various scenarios"""
    print("="*60)
    print("TESTING GENERATION QUERY MANAGER")
    print("="*60)
    
    # Track memory
    start_memory = get_memory_usage()
    print(f"\nInitial memory: {start_memory:.1f} MB")
    
    # Create manager
    print("\n1. Creating GenerationQueryManager...")
    t1 = time.time()
    manager = GenerationQueryManager()
    t1_duration = time.time() - t1
    print(f"✓ Manager created in {t1_duration:.2f}s")
    
    current_memory = get_memory_usage()
    print(f"✓ Memory after creation: {current_memory:.1f} MB (Δ{current_memory-start_memory:+.1f} MB)")
    
    # Test different date ranges
    end_date = datetime.now()
    test_scenarios = [
        ("24 hours", timedelta(days=1), "NSW1"),
        ("7 days", timedelta(days=7), "NSW1"),
        ("30 days", timedelta(days=30), "QLD1"),
        ("1 year", timedelta(days=365), "VIC1"),
        ("All NEM - 30 days", timedelta(days=30), "NEM"),
        ("All NEM - 1 year", timedelta(days=365), "NEM"),
    ]
    
    for scenario_name, time_delta, region in test_scenarios:
        print(f"\n2. Testing {scenario_name} for {region}...")
        start_date = end_date - time_delta
        
        # First query (not cached)
        t2 = time.time()
        data = manager.query_generation_by_fuel(start_date, end_date, region)
        t2_duration = time.time() - t2
        
        if not data.empty:
            print(f"✓ Query completed in {t2_duration:.2f}s")
            print(f"✓ Records: {len(data):,}")
            print(f"✓ Fuel types: {len(data['fuel_type'].unique())}")
            print(f"✓ Date range in data: {data['settlementdate'].min()} to {data['settlementdate'].max()}")
            
            # Show sample data
            fuel_summary = data.groupby('fuel_type')['total_generation_mw'].agg(['mean', 'sum', 'count'])
            print("\nTop 5 fuel types by average generation:")
            print(fuel_summary.sort_values('mean', ascending=False).head())
            
            # Memory check
            current_memory = get_memory_usage()
            print(f"\nMemory usage: {current_memory:.1f} MB (Δ{current_memory-start_memory:+.1f} MB)")
        else:
            print(f"✗ No data returned")
        
        # Test cache (second query)
        print(f"\nTesting cache for {scenario_name}...")
        t3 = time.time()
        data_cached = manager.query_generation_by_fuel(start_date, end_date, region)
        t3_duration = time.time() - t3
        
        print(f"✓ Cached query completed in {t3_duration:.3f}s (vs {t2_duration:.3f}s original)")
        print(f"✓ Speed improvement: {t2_duration/t3_duration:.1f}x faster")
    
    # Test capacity utilization
    print("\n3. Testing capacity utilization query...")
    t4 = time.time()
    util_data = manager.query_capacity_utilization(
        end_date - timedelta(days=1),
        end_date,
        'NSW1'
    )
    t4_duration = time.time() - t4
    
    if not util_data.empty:
        print(f"✓ Utilization query completed in {t4_duration:.2f}s")
        print(f"✓ Records: {len(util_data):,}")
        
        # Show utilization summary
        util_summary = util_data.groupby('fuel_type')['utilization_pct'].agg(['mean', 'max', 'min'])
        print("\nCapacity utilization by fuel type:")
        print(util_summary.sort_values('mean', ascending=False))
    
    # Test fuel capacities
    print("\n4. Testing fuel capacities query...")
    t5 = time.time()
    capacities = manager.query_fuel_capacities('NEM')
    t5_duration = time.time() - t5
    
    print(f"✓ Capacities query completed in {t5_duration:.2f}s")
    print(f"✓ Fuel types: {len(capacities)}")
    print("\nTop 5 fuel types by capacity:")
    sorted_capacities = sorted(capacities.items(), key=lambda x: x[1], reverse=True)[:5]
    for fuel, capacity in sorted_capacities:
        print(f"  {fuel}: {capacity:,.0f} MW")
    
    # Show cache statistics
    print("\n5. Cache Statistics:")
    stats = manager.get_statistics()
    for key, value in stats.items():
        print(f"  {key}: {value}")
    
    # Final memory check
    final_memory = get_memory_usage()
    print(f"\n6. Final memory usage: {final_memory:.1f} MB")
    print(f"   Total memory increase: {final_memory-start_memory:.1f} MB")
    
    if final_memory - start_memory < 500:
        print("   ✅ Memory usage is excellent!")
    else:
        print("   ⚠️  Memory usage is higher than expected")
    
    print("\n" + "="*60)
    print("TESTING COMPLETE")
    print("="*60)


if __name__ == "__main__":
    test_generation_query_manager()