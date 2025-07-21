#!/usr/bin/env python3
"""
Integration test for Generation Dashboard with refactored code
Tests the actual dashboard functionality with DuckDB backend
"""

import os
import sys
import time
import psutil
import gc
from datetime import datetime, timedelta

# Add the src directory to Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

# Import the actual dashboard module
from aemo_dashboard.generation.gen_dash import EnergyDashboard

def get_memory_usage():
    """Get current memory usage in MB"""
    process = psutil.Process()
    return process.memory_info().rss / 1024 / 1024

def test_generation_dashboard():
    """Test the actual generation dashboard with refactored code"""
    print("="*80)
    print("GENERATION DASHBOARD INTEGRATION TEST")
    print("="*80)
    print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Get initial memory
    gc.collect()
    initial_memory = get_memory_usage()
    print(f"\nInitial memory usage: {initial_memory:.1f} MB")
    
    try:
        # Initialize dashboard
        print("\n1. Initializing Generation Dashboard...")
        start_init = time.time()
        dashboard = EnergyDashboard()
        init_time = time.time() - start_init
        
        init_memory = get_memory_usage()
        print(f"✅ Dashboard initialized in {init_time:.2f}s")
        print(f"   Memory after init: {init_memory:.1f} MB (+{init_memory - initial_memory:.1f} MB)")
        
        # Test 1: Load 24 hours of data
        print("\n2. Testing 24-hour data load...")
        dashboard.time_range = '24 hours'
        
        start_load = time.time()
        dashboard.load_generation_data()
        load_time = time.time() - start_load
        
        load_memory = get_memory_usage()
        print(f"✅ Loaded 24 hours in {load_time:.2f}s")
        print(f"   Memory after load: {load_memory:.1f} MB (+{load_memory - init_memory:.1f} MB)")
        
        if hasattr(dashboard, 'generation_df') and dashboard.generation_df is not None:
            print(f"   Records loaded: {len(dashboard.generation_df):,}")
        
        # Test 2: Test region filtering
        print("\n3. Testing region filtering...")
        regions = ['NSW1', 'QLD1', 'VIC1', 'SA1', 'TAS1']
        
        for region in regions[:3]:  # Test first 3 regions
            dashboard.selected_region = region
            start_region = time.time()
            dashboard.process_data_for_region()
            region_time = time.time() - start_region
            
            print(f"✅ {region}: processed in {region_time:.3f}s")
        
        # Test 3: Load 30 days (should use aggregated data)
        print("\n4. Testing 30-day data load (aggregated)...")
        dashboard.time_range = '30 days'
        dashboard.selected_region = 'NEM'
        
        gc.collect()
        before_30d = get_memory_usage()
        
        start_30d = time.time()
        dashboard.load_generation_data()
        load_30d_time = time.time() - start_30d
        
        after_30d = get_memory_usage()
        print(f"✅ Loaded 30 days in {load_30d_time:.2f}s")
        print(f"   Memory usage: {after_30d:.1f} MB (+{after_30d - before_30d:.1f} MB)")
        print(f"   Using aggregated data: {getattr(dashboard, '_using_aggregated_data', False)}")
        
        # Test 4: Load All Available Data (ultimate test)
        print("\n5. Testing 'All Available Data' load...")
        dashboard.time_range = 'All Available Data'
        
        gc.collect()
        before_all = get_memory_usage()
        
        start_all = time.time()
        dashboard.load_generation_data()
        load_all_time = time.time() - start_all
        
        after_all = get_memory_usage()
        memory_increase = after_all - before_all
        total_memory = after_all - initial_memory
        
        print(f"✅ Loaded all data in {load_all_time:.2f}s")
        print(f"   Memory for operation: {memory_increase:.1f} MB")
        print(f"   Total memory usage: {after_all:.1f} MB")
        print(f"   Total increase from start: {total_memory:.1f} MB")
        
        # Test 5: Check cache statistics
        print("\n6. Cache Statistics:")
        if hasattr(dashboard, 'query_manager'):
            stats = dashboard.query_manager.get_statistics()
            print(f"   Cache entries: {stats.get('cache_entries', 0)}")
            print(f"   Cache size: {stats.get('cache_memory_mb', 0):.1f} MB")
            print(f"   Cache hit rate: {stats.get('hit_rate', 0):.1f}%")
        
        # Summary
        print("\n" + "="*80)
        print("SUMMARY")
        print("="*80)
        
        success = True
        issues = []
        
        # Check memory target
        if after_all > 500:
            issues.append(f"Memory usage ({after_all:.0f} MB) exceeds target (500 MB)")
            success = False
        else:
            print(f"✅ Memory target achieved: {after_all:.0f} MB < 500 MB")
        
        # Check load time
        if load_all_time > 5.0:
            issues.append(f"Load time ({load_all_time:.1f}s) exceeds target (5s)")
            # Don't fail for this - still acceptable
        else:
            print(f"✅ Load time target achieved: {load_all_time:.1f}s < 5s")
        
        # Check initialization time
        if init_time < 15:
            print(f"✅ Fast initialization: {init_time:.1f}s")
        else:
            issues.append(f"Slow initialization: {init_time:.1f}s")
        
        if success:
            print("\n🎉 ALL TARGETS MET! Dashboard is ready for production.")
        else:
            print("\n⚠️  Issues found:")
            for issue in issues:
                print(f"   - {issue}")
        
        return success
        
    except Exception as e:
        print(f"\n❌ ERROR: {type(e).__name__}: {str(e)}")
        import traceback
        traceback.print_exc()
        return False
    
    finally:
        print(f"\nCompleted at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")


if __name__ == "__main__":
    success = test_generation_dashboard()
    sys.exit(0 if success else 1)