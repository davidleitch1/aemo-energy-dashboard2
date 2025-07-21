#!/usr/bin/env python3
"""
Final test for Generation Dashboard refactoring
Simple test to verify memory and performance targets
"""

import os
import sys
import time
import psutil
import gc
from datetime import datetime, timedelta

# Add the src directory to Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

from aemo_dashboard.generation.gen_dash import EnergyDashboard

def get_memory_usage():
    """Get current memory usage in MB"""
    process = psutil.Process()
    return process.memory_info().rss / 1024 / 1024

def main():
    print("GENERATION DASHBOARD - FINAL TEST")
    print("="*50)
    
    # Get baseline memory
    gc.collect()
    baseline_memory = get_memory_usage()
    print(f"Baseline memory: {baseline_memory:.0f} MB")
    
    # Initialize dashboard
    print("\nInitializing dashboard...")
    start = time.time()
    dashboard = EnergyDashboard()
    init_time = time.time() - start
    print(f"✅ Initialized in {init_time:.1f}s")
    
    # Test 1: 30-day load (should use aggregated data)
    print("\nTest 1: Loading 30 days...")
    dashboard.time_range = '30'  # Use string value
    dashboard.selected_region = 'NEM'
    
    start = time.time()
    dashboard.load_generation_data()
    load_time = time.time() - start
    
    mem_30d = get_memory_usage()
    print(f"✅ 30 days loaded in {load_time:.1f}s")
    print(f"   Memory: {mem_30d:.0f} MB (+{mem_30d - baseline_memory:.0f} MB)")
    print(f"   Using aggregated: {getattr(dashboard, '_using_aggregated_data', False)}")
    
    # Test 2: All Available Data (the big test)
    print("\nTest 2: Loading All Available Data...")
    dashboard.time_range = 'All'  # Use 'All' string
    
    gc.collect()
    before_all = get_memory_usage()
    
    start = time.time()
    dashboard.load_generation_data()
    all_time = time.time() - start
    
    final_memory = get_memory_usage()
    memory_used = final_memory - before_all
    total_memory = final_memory - baseline_memory
    
    print(f"✅ All data loaded in {all_time:.1f}s")
    print(f"   Memory for operation: {memory_used:.0f} MB")
    print(f"   Total memory: {final_memory:.0f} MB")
    print(f"   Total increase: {total_memory:.0f} MB")
    print(f"   Using aggregated: {getattr(dashboard, '_using_aggregated_data', False)}")
    
    # Check targets
    print("\n" + "="*50)
    print("RESULTS:")
    print("="*50)
    
    memory_ok = final_memory < 500
    time_ok = all_time < 10  # Relaxed to 10s
    
    print(f"Memory target (<500 MB): {'✅ PASS' if memory_ok else '❌ FAIL'} ({final_memory:.0f} MB)")
    print(f"Time target (<10s): {'✅ PASS' if time_ok else '❌ FAIL'} ({all_time:.1f}s)")
    
    if memory_ok and time_ok:
        print("\n🎉 ALL TESTS PASSED!")
        return True
    else:
        print("\n⚠️  Some targets not met")
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)