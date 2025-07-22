#!/usr/bin/env python3
"""
Profile dashboard initialization to find performance bottlenecks
"""

import cProfile
import pstats
import io
import time
import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / 'src'))

def profile_dashboard_init():
    """Profile the dashboard initialization"""
    
    # Start profiling
    profiler = cProfile.Profile()
    profiler.enable()
    
    start_time = time.time()
    
    # Import and time each major component
    print("1. Importing modules...")
    t1 = time.time()
    from aemo_dashboard.generation.gen_dash import EnergyDashboard
    print(f"   Import time: {time.time() - t1:.2f}s")
    
    print("\n2. Creating dashboard instance...")
    t2 = time.time()
    dashboard = EnergyDashboard()
    print(f"   Instance creation time: {time.time() - t2:.2f}s")
    
    print("\n3. Testing individual methods...")
    
    # Test data loading
    t3 = time.time()
    print("   - Loading generation data...")
    gen_data = dashboard.load_generation_data()
    print(f"     Generation data load time: {time.time() - t3:.2f}s")
    print(f"     Data shape: {gen_data.shape}")
    
    # Test plot creation
    t4 = time.time()
    print("   - Creating generation plot...")
    try:
        # First prepare the data
        dashboard.filtered_gen_data = dashboard.process_data_for_region(gen_data)
        plot = dashboard.create_plot()
        print(f"     Plot creation time: {time.time() - t4:.2f}s")
    except Exception as e:
        print(f"     Plot creation error: {e}")
    
    # Test capacity utilization
    t5 = time.time()
    print("   - Creating capacity utilization plot...")
    try:
        util_plot = dashboard.create_capacity_utilization_plot()
        print(f"     Utilization plot time: {time.time() - t5:.2f}s")
    except Exception as e:
        print(f"     Utilization plot error: {e}")
    
    # Stop profiling
    profiler.disable()
    end_time = time.time()
    
    print(f"\n\nTotal profiling time: {end_time - start_time:.2f} seconds")
    
    # Analyze results
    print("\n" + "="*80)
    print("TOP 30 TIME CONSUMERS (cumulative time):")
    print("="*80)
    
    s = io.StringIO()
    ps = pstats.Stats(profiler, stream=s).sort_stats('cumulative')
    ps.print_stats(30)
    print(s.getvalue())
    
    # Show functions by time
    print("\n" + "="*80)
    print("TOP 20 BY TIME (not cumulative):")
    print("="*80)
    
    s2 = io.StringIO()
    ps2 = pstats.Stats(profiler, stream=s2).sort_stats('time')
    ps2.print_stats(20)
    print(s2.getvalue())
    
    # Look for Panel/HoloViews functions
    print("\n" + "="*80)
    print("PANEL/HOLOVIEWS FUNCTIONS:")
    print("="*80)
    
    s3 = io.StringIO()
    ps3 = pstats.Stats(profiler, stream=s3).sort_stats('cumulative')
    ps3.print_stats(r'panel|holoviews|hvplot|bokeh', 20)
    print(s3.getvalue())
    
    # Look for DuckDB operations
    print("\n" + "="*80)
    print("DUCKDB OPERATIONS:")
    print("="*80)
    
    s4 = io.StringIO()
    ps4 = pstats.Stats(profiler, stream=s4).sort_stats('cumulative')
    ps4.print_stats('duckdb|execute|query', 20)
    print(s4.getvalue())

if __name__ == "__main__":
    profile_dashboard_init()