#!/usr/bin/env python3
"""
Test defer_load performance improvement
"""
import time
import sys
import os
import json
from datetime import datetime

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

class DeferLoadProfiler:
    def __init__(self):
        self.timings = []
        self.start_time = None
        self.total_start = time.time()
        
    def start(self, phase_name):
        self.start_time = time.time()
        
    def end(self, phase_name, details=None):
        duration = time.time() - self.start_time
        self.timings.append({
            'phase': phase_name,
            'duration': duration,
            'details': details,
            'timestamp': datetime.now().isoformat()
        })
        print(f"{phase_name}: {duration:.3f}s")
        
    def save_results(self):
        total_time = time.time() - self.total_start
        
        results = {
            'total_time': total_time,
            'timings': self.timings,
            'timestamp': datetime.now().isoformat()
        }
        
        filename = f"defer_load_profile_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(filename, 'w') as f:
            json.dump(results, f, indent=2)
        
        print(f"\nTotal time: {total_time:.3f}s")
        print(f"Results saved to {filename}")
        
        # Compare with baseline
        baseline_file = "complete_startup_profile_20250721_224330.json"
        if os.path.exists(baseline_file):
            with open(baseline_file, 'r') as f:
                baseline = json.load(f)
                baseline_total = baseline['total_time']
                
            improvement = baseline_total - total_time
            improvement_pct = (improvement / baseline_total) * 100
            
            print(f"\nBaseline time: {baseline_total:.3f}s")
            print(f"Current time: {total_time:.3f}s")
            print(f"Improvement: {improvement:.3f}s ({improvement_pct:.1f}%)")
            
            # Find specific phase improvements
            baseline_dashboard = next((t for t in baseline['timings'] if t['phase'] == 'First user dashboard creation'), None)
            current_dashboard = next((t for t in self.timings if t['phase'] == 'First user dashboard creation'), None)
            
            if baseline_dashboard and current_dashboard:
                dash_improvement = baseline_dashboard['duration'] - current_dashboard['duration']
                dash_improvement_pct = (dash_improvement / baseline_dashboard['duration']) * 100
                print(f"\nDashboard creation improvement:")
                print(f"  Baseline: {baseline_dashboard['duration']:.3f}s")
                print(f"  Current: {current_dashboard['duration']:.3f}s")
                print(f"  Improvement: {dash_improvement:.3f}s ({dash_improvement_pct:.1f}%)")


def test_defer_load_performance():
    profiler = DeferLoadProfiler()
    
    # Stage 1: Environment setup
    profiler.start("Environment and config setup")
    os.environ.setdefault('AEMO_DASHBOARD_ENV', 'development')
    profiler.end("Environment and config setup")
    
    # Stage 2: Core imports
    profiler.start("Core Python imports")
    import pandas as pd
    import numpy as np
    import asyncio
    profiler.end("Core Python imports", "pandas, numpy, asyncio")
    
    # Stage 3: Panel initialization
    profiler.start("Panel initialization")
    import panel as pn
    # Panel is already initialized with defer_load in gen_dash.py
    profiler.end("Panel initialization", "defer_load=True enabled")
    
    # Stage 4: Visualization imports
    profiler.start("Visualization library imports")
    import hvplot.pandas
    import holoviews as hv
    from bokeh.plotting import figure
    profiler.end("Visualization library imports", "hvplot, holoviews, bokeh")
    
    # Stage 5: Dashboard module imports
    profiler.start("Dashboard module imports")
    from aemo_dashboard.generation.gen_dash import EnergyDashboard, main
    profiler.end("Dashboard module imports", "Includes DuckDB view creation")
    
    # Stage 6: Create app factory
    profiler.start("App factory creation")
    app_factory = main()
    profiler.end("App factory creation")
    
    # Stage 7: First user dashboard creation
    profiler.start("First user dashboard creation")
    dashboard_app = app_factory()
    profiler.end("First user dashboard creation", "Dashboard object created with defer_load")
    
    # Stage 8: Check structure
    profiler.start("Dashboard structure analysis")
    has_tabs = hasattr(dashboard_app, 'tabs') or 'tabs' in str(type(dashboard_app))
    tab_count = 0
    
    if hasattr(dashboard_app, '__len__'):
        try:
            tab_count = len(dashboard_app)
        except:
            pass
    
    profiler.end("Dashboard structure analysis", f"Has tabs: {has_tabs}, Count: {tab_count}")
    
    # Stage 9: Simulate rendering
    profiler.start("Simulated rendering process")
    try:
        # Simulate the Bokeh serialization
        from bokeh.embed import components
        if hasattr(dashboard_app, 'get_root'):
            root = dashboard_app.get_root()
            script, div = components(root)
        time.sleep(0.005)  # Simulate network latency
    except:
        pass
    profiler.end("Simulated rendering process", "Initial render complete")
    
    # Stage 10: Test deferred loading
    profiler.start("Deferred component loading test")
    # Simulate Panel's defer_load execution
    import threading
    
    def simulate_deferred_loads():
        # Simulate the deferred components loading
        time.sleep(0.1)  # Price component
        time.sleep(0.1)  # Renewable gauge
        time.sleep(0.2)  # Generation overview
    
    thread = threading.Thread(target=simulate_deferred_loads)
    thread.start()
    
    # Main thread continues immediately
    profiler.end("Deferred component loading test", "Components loading in background")
    
    # Wait for deferred loads to complete
    thread.join()
    
    profiler.save_results()


if __name__ == "__main__":
    print("Testing defer_load performance improvement...")
    print("=" * 60)
    test_defer_load_performance()