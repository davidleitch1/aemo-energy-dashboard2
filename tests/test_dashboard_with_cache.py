#!/usr/bin/env python3
"""
Test the actual dashboard startup with caching enabled
"""

import time
import os
import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / 'src'))

# Enable caching and DuckDB
os.environ['ENABLE_PN_CACHE'] = 'true'
os.environ['USE_DUCKDB'] = 'true'
os.environ['DASHBOARD_PORT'] = '5009'  # Different port to avoid conflicts

def test_dashboard_startup():
    """Test dashboard startup time with caching"""
    print("Testing Dashboard Startup with Panel Caching")
    print("=" * 60)
    
    # First startup (cache cold)
    print("\nFirst Dashboard Startup (cold cache)...")
    start_time = time.time()
    
    # Import and create dashboard
    from aemo_dashboard.generation.gen_dash import main
    
    # Create the app but don't serve it
    import panel as pn
    
    # Measure time to create the dashboard app
    app_start = time.time()
    dashboard = None
    
    # Import the EnergyDashboard class
    from aemo_dashboard.generation.gen_dash import EnergyDashboard
    dashboard = EnergyDashboard()
    
    # Create the tabbed interface (this is where the expensive operations happen)
    print("Creating dashboard tabs...")
    tabs_start = time.time()
    
    # Create each tab
    from aemo_dashboard.nem_dash.nem_dash_tab import create_nem_dash_tab_with_updates
    from aemo_dashboard.analysis.price_analysis_ui import create_price_analysis_tab
    from aemo_dashboard.station.station_analysis_ui import create_station_analysis_tab
    
    nem_tab = create_nem_dash_tab_with_updates(
        update_period_ms=270000,
        time_range='1',
        region='NSW1'
    )
    
    tabs = pn.Tabs(
        ("Today", nem_tab),
        ("Generation mix", pn.pane.HTML("Loading...")),
        ("Pivot table", pn.pane.HTML("Loading...")),
        ("Station Analysis", pn.pane.HTML("Loading...")),
        ("Penetration", pn.pane.HTML("Loading..."))
    )
    
    tabs_time = time.time() - tabs_start
    total_time = time.time() - start_time
    
    print(f"\nStartup times:")
    print(f"  Tab creation: {tabs_time:.2f}s")
    print(f"  Total time: {total_time:.2f}s")
    
    # Now test Generation tab creation specifically
    print("\nTesting Generation tab creation...")
    gen_start = time.time()
    
    # Create generation plot
    gen_plot = dashboard.create_plot()
    gen_time = time.time() - gen_start
    
    print(f"  Generation plot creation: {gen_time:.2f}s")
    print(f"  Cache stats: {dashboard.get_cache_stats_display()}")
    
    # Create it again to test cache
    print("\nTesting cached generation plot...")
    gen_start2 = time.time()
    gen_plot2 = dashboard.create_plot()
    gen_time2 = time.time() - gen_start2
    
    print(f"  Cached generation plot: {gen_time2:.2f}s")
    print(f"  Cache stats: {dashboard.get_cache_stats_display()}")
    
    if gen_time2 < gen_time:
        speedup = gen_time / gen_time2
        print(f"  Speedup: {speedup:.1f}x")
    
    # Test the full generation tab with all plots
    print("\nTesting full Generation tab creation...")
    full_gen_start = time.time()
    
    # This would create all the plots
    gen_view = dashboard.panel()
    
    full_gen_time = time.time() - full_gen_start
    print(f"  Full generation tab: {full_gen_time:.2f}s")

if __name__ == "__main__":
    test_dashboard_startup()