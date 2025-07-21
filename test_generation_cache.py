#!/usr/bin/env python3
"""
Test script for generation dashboard caching
Measures performance improvement with pn.cache
"""

import time
import os
import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / 'src'))

# Enable caching
os.environ['ENABLE_PN_CACHE'] = 'true'
os.environ['USE_DUCKDB'] = 'true'

def test_cache_performance():
    """Test the cache performance of generation dashboard"""
    print("Testing Generation Dashboard Cache Performance")
    print("=" * 60)
    
    # Import after setting environment
    from aemo_dashboard.generation.gen_dash import EnergyDashboard
    
    # Create dashboard instance
    print("Creating dashboard instance...")
    dashboard = EnergyDashboard()
    
    # Set to a specific region and time range
    dashboard.region = 'NSW1'
    dashboard.time_range = '1'  # Last 24 hours
    
    print(f"Testing with region={dashboard.region}, time_range={dashboard.time_range}")
    print()
    
    # First plot creation (cache miss)
    print("First plot creation (cache miss expected)...")
    start_time = time.time()
    plot1 = dashboard.create_plot()
    time1 = time.time() - start_time
    print(f"  Time: {time1:.2f} seconds")
    print(f"  Cache stats: {dashboard.get_cache_stats_display()}")
    print()
    
    # Second plot creation (cache hit)
    print("Second plot creation (cache hit expected)...")
    start_time = time.time()
    plot2 = dashboard.create_plot()
    time2 = time.time() - start_time
    print(f"  Time: {time2:.2f} seconds")
    print(f"  Cache stats: {dashboard.get_cache_stats_display()}")
    print()
    
    # Performance improvement
    if time2 < time1:
        speedup = time1 / time2
        print(f"Performance improvement: {speedup:.1f}x faster!")
        print(f"Time saved: {time1 - time2:.2f} seconds")
    else:
        print("No performance improvement detected")
    
    print()
    
    # Test with different region (cache miss)
    print("Testing different region (QLD1)...")
    dashboard.region = 'QLD1'
    start_time = time.time()
    plot3 = dashboard.create_plot()
    time3 = time.time() - start_time
    print(f"  Time: {time3:.2f} seconds")
    print(f"  Cache stats: {dashboard.get_cache_stats_display()}")
    print()
    
    # Test same region again (cache hit)
    print("Testing QLD1 again (cache hit expected)...")
    start_time = time.time()
    plot4 = dashboard.create_plot()
    time4 = time.time() - start_time
    print(f"  Time: {time4:.2f} seconds")
    print(f"  Cache stats: {dashboard.get_cache_stats_display()}")
    print()
    
    # Summary
    print("=" * 60)
    print("Summary:")
    print(f"  Average first load: {(time1 + time3) / 2:.2f} seconds")
    print(f"  Average cached load: {(time2 + time4) / 2:.2f} seconds")
    print(f"  Final cache stats: {dashboard.get_cache_stats_display()}")
    
    # Test with cache disabled
    print("\nTesting with cache disabled...")
    os.environ['ENABLE_PN_CACHE'] = 'false'
    
    # Need to reload the module
    import importlib
    import aemo_dashboard.generation.gen_dash
    importlib.reload(aemo_dashboard.generation.gen_dash)
    
    dashboard_no_cache = aemo_dashboard.generation.gen_dash.EnergyDashboard()
    dashboard_no_cache.region = 'NSW1'
    dashboard_no_cache.time_range = '1'
    
    start_time = time.time()
    plot5 = dashboard_no_cache.create_plot()
    time5 = time.time() - start_time
    print(f"  Time without cache: {time5:.2f} seconds")
    print(f"  Cache stats: {dashboard_no_cache.get_cache_stats_display()}")

if __name__ == "__main__":
    test_cache_performance()