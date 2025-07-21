#!/usr/bin/env python3
"""
Profile dashboard startup to find the real performance bottleneck
"""

import cProfile
import pstats
import io
import time
import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / 'src'))

def profile_dashboard_startup():
    """Profile the dashboard initialization to find bottlenecks"""
    
    # Start profiling
    profiler = cProfile.Profile()
    profiler.enable()
    
    start_time = time.time()
    
    # Import and initialize dashboard
    print("Starting dashboard profiling...")
    from aemo_dashboard.generation.gen_dash import EnergyDashboard, create_app
    
    print("Creating app factory...")
    app_factory = create_app()
    
    print("Creating dashboard instance...")
    # Call the factory to create a dashboard instance
    dashboard_instance = app_factory()
    
    # Stop profiling
    profiler.disable()
    end_time = time.time()
    
    print(f"\nTotal time: {end_time - start_time:.2f} seconds")
    
    # Create string buffer for stats
    s = io.StringIO()
    ps = pstats.Stats(profiler, stream=s).sort_stats('cumulative')
    
    # Print top 30 time-consuming functions
    ps.print_stats(30)
    
    # Also print callers of the top functions
    print("\n" + "="*80)
    print("TOP TIME CONSUMERS:")
    print("="*80)
    print(s.getvalue())
    
    # Find specific bottlenecks
    s2 = io.StringIO()
    ps2 = pstats.Stats(profiler, stream=s2).sort_stats('time')
    ps2.print_stats('create_', 20)  # Functions with 'create_' in name
    
    print("\n" + "="*80)
    print("CREATE FUNCTIONS:")
    print("="*80)
    print(s2.getvalue())
    
    # Panel-specific functions
    s3 = io.StringIO()
    ps3 = pstats.Stats(profiler, stream=s3).sort_stats('cumulative')
    ps3.print_stats(r'panel|pn\.', 20)
    
    print("\n" + "="*80)
    print("PANEL FUNCTIONS:")
    print("="*80)
    print(s3.getvalue())

if __name__ == "__main__":
    profile_dashboard_startup()