#!/usr/bin/env python3
"""
Profile the COMPLETE dashboard startup sequence including rendering
This simulates what happens when a user opens the dashboard URL
"""

import time
import sys
import os
import threading
import queue
from pathlib import Path
from datetime import datetime
import json
import asyncio
import panel as pn

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / 'src'))

class CompleteDashboardProfiler:
    def __init__(self):
        self.timings = []
        self.start_time = None
        self.total_start = time.time()
        
    def start(self, phase_name):
        """Start timing a phase"""
        self.start_time = time.time()
        return self.start_time
        
    def end(self, phase_name, details=None):
        """End timing a phase and record it"""
        if self.start_time is None:
            return
        
        end_time = time.time()
        duration = end_time - self.start_time
        elapsed = end_time - self.total_start
        
        self.timings.append({
            'phase': phase_name,
            'duration': duration,
            'elapsed': elapsed,
            'details': details,
            'timestamp': datetime.now().isoformat()
        })
        
        # Print real-time feedback
        print(f"[{elapsed:6.2f}s] ✓ {phase_name}: {duration:.3f}s" + (f" - {details}" if details else ""))
        
        self.start_time = None
        return duration
    
    def report(self):
        """Generate a comprehensive report"""
        total_time = time.time() - self.total_start
        
        print("\n" + "="*80)
        print("COMPLETE DASHBOARD STARTUP TIMING REPORT")
        print("="*80)
        print(f"Total startup time: {total_time:.2f} seconds")
        print(f"Timestamp: {datetime.now().isoformat()}")
        print("="*80 + "\n")
        
        # Timeline view
        print("STARTUP TIMELINE:")
        print("-"*80)
        print(f"{'Time':>8} {'Duration':>10} {'Phase':<50}")
        print("-"*80)
        for timing in self.timings:
            print(f"{timing['elapsed']:8.2f}s {timing['duration']:9.3f}s  {timing['phase']:<50}")
            if timing['details']:
                print(f"{'':>19} └─ {timing['details']}")
        
        # Save detailed report
        report_file = f"complete_startup_profile_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(report_file, 'w') as f:
            json.dump({
                'total_time': total_time,
                'timings': self.timings,
                'timestamp': datetime.now().isoformat()
            }, f, indent=2)
        print(f"\n✅ Detailed report saved to: {report_file}")

def profile_complete_startup():
    """Profile the complete dashboard startup including rendering"""
    
    profiler = CompleteDashboardProfiler()
    
    print("Starting COMPLETE dashboard startup profiling...")
    print("This simulates what happens when a user opens the dashboard URL")
    print("="*80)
    
    # Phase 1: Environment setup
    profiler.start("Environment and config setup")
    from dotenv import load_dotenv
    env_file = Path('.env')
    if env_file.exists():
        load_dotenv(env_file)
    os.environ['ENABLE_PN_CACHE'] = 'true'  # Ensure caching is on
    profiler.end("Environment and config setup")
    
    # Phase 2: Core imports
    profiler.start("Core Python imports")
    import pandas as pd
    import numpy as np
    import asyncio
    profiler.end("Core Python imports", "pandas, numpy, asyncio")
    
    # Phase 3: Panel setup
    profiler.start("Panel initialization")
    pn.extension('tabulator')
    pn.config.theme = 'dark'
    profiler.end("Panel initialization", "dark theme, tabulator extension")
    
    # Phase 4: Visualization imports
    profiler.start("Visualization library imports")
    import hvplot.pandas
    import holoviews as hv
    hv.extension('bokeh')
    profiler.end("Visualization library imports", "hvplot, holoviews, bokeh")
    
    # Phase 5: Dashboard imports (this triggers DuckDB views)
    profiler.start("Dashboard module imports")
    from aemo_dashboard.generation.gen_dash import create_app
    from aemo_dashboard.shared.logging_config import setup_logging
    profiler.end("Dashboard module imports", "Includes DuckDB view creation")
    
    # Phase 6: Create app factory
    profiler.start("App factory creation")
    app_factory = create_app()
    profiler.end("App factory creation")
    
    # Phase 7: Simulate first user connection
    profiler.start("First user dashboard creation")
    
    # Create a mock request context
    class MockRequest:
        headers = {}
        cookies = {}
    
    class MockSession:
        def __init__(self):
            self.id = "test-session"
            self._data = {}
    
    # Mock Panel state
    class MockState:
        def __init__(self):
            self.curdoc = None
            self.cache = {}
            self.session_info = {'total': 0}
            self._scheduled = []
            
        def add_periodic_callback(self, callback, period, count=None):
            # Execute callback immediately for testing
            if count == 1:
                try:
                    callback()
                except Exception as e:
                    print(f"Callback error: {e}")
            self._scheduled.append((callback, period, count))
            return None
    
    # Create dashboard instance with mocked state
    original_state = pn.state
    mock_state = MockState()
    pn.state = mock_state
    
    try:
        dashboard_instance = app_factory()
        profiler.end("First user dashboard creation", "Dashboard object created")
    except Exception as e:
        profiler.end("First user dashboard creation", f"Error: {str(e)}")
        dashboard_instance = None
    finally:
        pn.state = original_state
    
    # Phase 8: Analyze dashboard structure
    if dashboard_instance:
        profiler.start("Dashboard structure analysis")
        
        # Find the tabs component
        tabs_found = False
        tab_count = 0
        tab_names = []
        
        if hasattr(dashboard_instance, 'objects'):
            for obj in dashboard_instance.objects:
                if hasattr(obj, '_names'):
                    tabs_found = True
                    tab_count = len(obj)
                    tab_names = list(obj._names) if hasattr(obj, '_names') else []
                    break
        
        details = f"Tabs: {tab_count}, Names: {tab_names}" if tabs_found else "No tabs found"
        profiler.end("Dashboard structure analysis", details)
    
    # Phase 9: Simulate tab rendering (measure each tab initialization)
    if dashboard_instance and tabs_found:
        profiler.start("Tab initialization timing")
        
        # Find tabs component again
        tabs_component = None
        for obj in dashboard_instance.objects:
            if hasattr(obj, '_names'):
                tabs_component = obj
                break
        
        if tabs_component:
            # Check each tab's state
            tab_details = []
            for i, tab_name in enumerate(tab_names):
                tab_content = tabs_component[i]
                content_type = type(tab_content).__name__
                
                # Check if it's a loading indicator
                is_loading = 'HTML' in content_type and 'loading' in str(tab_content).lower()
                tab_details.append(f"{tab_name}: {'loading' if is_loading else 'ready'}")
            
            profiler.end("Tab initialization timing", ", ".join(tab_details))
        else:
            profiler.end("Tab initialization timing", "Could not access tabs")
    
    # Phase 10: Simulate rendering process
    profiler.start("Simulated rendering process")
    
    # This is where Panel would normally:
    # 1. Set up WebSocket connections
    # 2. Serialize the dashboard to Bokeh models
    # 3. Send JavaScript/CSS resources to browser
    # 4. Render the dashboard in the browser
    
    # We can't fully simulate this without running a server,
    # but we can measure the serialization time
    try:
        if dashboard_instance:
            # Get the Bokeh model (this triggers serialization)
            from panel.io.server import get_server
            from bokeh.document import Document
            
            doc = Document()
            if hasattr(dashboard_instance, 'server_doc'):
                dashboard_instance.server_doc(doc)
            
            profiler.end("Simulated rendering process", "Bokeh serialization complete")
        else:
            profiler.end("Simulated rendering process", "No dashboard to render")
    except Exception as e:
        profiler.end("Simulated rendering process", f"Serialization error: {str(e)}")
    
    # Phase 11: Test actual data loading for Today tab
    profiler.start("Today tab data loading test")
    try:
        from aemo_dashboard.generation.gen_dash import EnergyDashboard
        test_dash = EnergyDashboard()
        
        # Time individual data operations
        ops_timing = []
        
        # Load generation data
        start = time.time()
        gen_data = test_dash.load_generation_data()
        ops_timing.append(f"gen:{time.time()-start:.2f}s")
        
        # Process for region
        start = time.time()
        if gen_data is not None:
            processed = test_dash.process_data_for_region(gen_data)
            ops_timing.append(f"process:{time.time()-start:.2f}s")
        
        # Create plot
        start = time.time()
        if hasattr(test_dash, 'create_plot'):
            plot = test_dash.create_plot()
            ops_timing.append(f"plot:{time.time()-start:.2f}s")
        
        profiler.end("Today tab data loading test", ", ".join(ops_timing))
    except Exception as e:
        profiler.end("Today tab data loading test", f"Error: {str(e)}")
    
    # Generate the final report
    profiler.report()
    
    # Additional analysis
    print("\n" + "="*80)
    print("PERFORMANCE ANALYSIS:")
    print("="*80)
    
    # Calculate phase categories
    import_time = sum(t['duration'] for t in profiler.timings 
                     if 'import' in t['phase'].lower() or 'initialization' in t['phase'].lower())
    creation_time = sum(t['duration'] for t in profiler.timings 
                       if 'creation' in t['phase'].lower() or 'factory' in t['phase'].lower())
    data_time = sum(t['duration'] for t in profiler.timings 
                   if 'data' in t['phase'].lower())
    
    total = sum(t['duration'] for t in profiler.timings)
    
    print(f"\nTime by category:")
    print(f"- Imports & Init: {import_time:.2f}s ({import_time/total*100:.1f}%)")
    print(f"- Dashboard Creation: {creation_time:.2f}s ({creation_time/total*100:.1f}%)")
    print(f"- Data Operations: {data_time:.2f}s ({data_time/total*100:.1f}%)")
    print(f"- Other: {total - import_time - creation_time - data_time:.2f}s")
    
    print("\n" + "="*80)
    print("MISSING FROM PROFILE:")
    print("="*80)
    print("- WebSocket connection setup")
    print("- Browser resource loading (JS/CSS)")
    print("- Actual DOM rendering")
    print("- Network latency")
    print("- These typically add 3-5 seconds to user-perceived load time")
    
    return profiler

def main():
    """Run the complete dashboard profiling"""
    
    # Check required files
    from aemo_dashboard.shared.config import config
    
    if not os.path.exists(config.gen_info_file):
        print(f"Error: {config.gen_info_file} not found")
        return
    
    if not os.path.exists(config.gen_output_file):
        print(f"Error: {config.gen_output_file} not found")
        return
    
    # Run profiling
    profiler = profile_complete_startup()
    
    print("\n✅ Complete profiling finished!")
    print("\nNOTE: To measure the FULL user experience (including browser rendering),")
    print("you would need to use browser developer tools while loading the dashboard.")

if __name__ == "__main__":
    main()