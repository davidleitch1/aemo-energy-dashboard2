#!/usr/bin/env python3
"""
Compare original vs optimized penetration tab implementations
"""

import time
import psutil
import os
from datetime import datetime
import sys
from pathlib import Path

# Add the src directory to the path
sys.path.insert(0, str(Path(__file__).parent / "src"))

from aemo_dashboard.penetration.penetration_tab import PenetrationTab
from aemo_dashboard.penetration.penetration_tab_optimized import OptimizedPenetrationTab
from aemo_dashboard.shared.logging_config import get_logger

logger = get_logger(__name__)

def measure_memory():
    """Get current memory usage in MB"""
    process = psutil.Process(os.getpid())
    return process.memory_info().rss / 1024 / 1024

def test_implementation(tab_class, name):
    """Test a tab implementation"""
    print(f"\n=== Testing {name} ===")
    
    start_time = time.time()
    start_mem = measure_memory()
    
    # Create tab
    tab = tab_class()
    
    init_time = time.time() - start_time
    init_mem = measure_memory() - start_mem
    
    # Create layout (triggers chart creation)
    layout_start = time.time()
    layout = tab.create_layout()
    layout_time = time.time() - layout_start
    
    total_time = time.time() - start_time
    total_mem = measure_memory() - start_mem
    
    print(f"  Initialization: {init_time:.2f}s, {init_mem:.1f}MB")
    print(f"  Layout creation: {layout_time:.2f}s")
    print(f"  Total: {total_time:.2f}s, {total_mem:.1f}MB")
    
    # Test region change
    change_start = time.time()
    tab.region_select.value = 'NSW1'
    change_time = time.time() - change_start
    print(f"  Region change: {change_time:.2f}s")
    
    # Test fuel change
    fuel_start = time.time()
    tab.fuel_select.value = 'Solar'
    fuel_time = time.time() - fuel_start
    print(f"  Fuel change: {fuel_time:.2f}s")
    
    return {
        'name': name,
        'init_time': init_time,
        'layout_time': layout_time,
        'total_time': total_time,
        'memory': total_mem,
        'region_change': change_time,
        'fuel_change': fuel_time
    }

def main():
    # Set up environment
    os.environ['USE_DUCKDB'] = 'true'
    
    print("=== PENETRATION TAB PERFORMANCE COMPARISON ===")
    print(f"Testing at {datetime.now()}")
    
    # Test original
    original_results = test_implementation(PenetrationTab, "Original Implementation")
    
    # Give some time for garbage collection
    import gc
    gc.collect()
    time.sleep(2)
    
    # Test optimized
    optimized_results = test_implementation(OptimizedPenetrationTab, "Optimized Implementation")
    
    # Calculate improvements
    print("\n=== PERFORMANCE IMPROVEMENTS ===")
    
    if original_results['total_time'] > 0:
        speedup = original_results['total_time'] / optimized_results['total_time']
        print(f"Overall speedup: {speedup:.1f}x faster")
    
    if original_results['memory'] > 0:
        mem_reduction = (1 - optimized_results['memory'] / original_results['memory']) * 100
        print(f"Memory reduction: {mem_reduction:.0f}%")
    
    if original_results['region_change'] > 0:
        update_speedup = original_results['region_change'] / optimized_results['region_change']
        print(f"Update speedup: {update_speedup:.1f}x faster")
    
    print("\n=== KEY OPTIMIZATIONS ===")
    print("1. Daily pre-aggregation reduces data points by ~48x")
    print("2. Rolling window of 30 (daily) vs 1440 (30-min) is ~48x faster")
    print("3. Caching prevents redundant calculations on widget changes")
    print("4. Optional LOESS smoothing provides better visual quality")
    print("5. Lazy loading could further improve initial load time")
    
    print("\n=== RECOMMENDATIONS ===")
    print("1. Replace original implementation with optimized version")
    print("2. Consider adding a 'Performance Mode' toggle for very large datasets")
    print("3. Implement background pre-calculation for common date ranges")
    print("4. Add progress indicators during initial load")

if __name__ == "__main__":
    main()