#!/usr/bin/env python3
"""
Profile the FULL dashboard startup to understand where time is spent
This creates a detailed breakdown of the startup process
"""

import time
import sys
import os
from pathlib import Path
from datetime import datetime
import json

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / 'src'))

class StartupProfiler:
    def __init__(self):
        self.timings = []
        self.start_time = None
        
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
        
        self.timings.append({
            'phase': phase_name,
            'duration': duration,
            'details': details,
            'timestamp': datetime.now().isoformat()
        })
        
        # Print real-time feedback
        print(f"✓ {phase_name}: {duration:.3f}s" + (f" - {details}" if details else ""))
        
        self.start_time = None
        return duration
    
    def report(self):
        """Generate a comprehensive report"""
        total_time = sum(t['duration'] for t in self.timings)
        
        print("\n" + "="*80)
        print("DASHBOARD STARTUP TIMING REPORT")
        print("="*80)
        print(f"Total startup time: {total_time:.2f} seconds")
        print(f"Timestamp: {datetime.now().isoformat()}")
        print("="*80 + "\n")
        
        # Sort by duration
        sorted_timings = sorted(self.timings, key=lambda x: x['duration'], reverse=True)
        
        print("BREAKDOWN BY DURATION:")
        print("-"*80)
        for timing in sorted_timings:
            percentage = (timing['duration'] / total_time) * 100
            print(f"{timing['phase']:50} {timing['duration']:6.3f}s ({percentage:4.1f}%)")
            if timing['details']:
                print(f"{'':50} Details: {timing['details']}")
        
        print("\n" + "="*80)
        print("STARTUP SEQUENCE (chronological):")
        print("-"*80)
        cumulative = 0
        for i, timing in enumerate(self.timings, 1):
            cumulative += timing['duration']
            print(f"{i:2}. {timing['phase']:45} {timing['duration']:6.3f}s (cumulative: {cumulative:6.3f}s)")
        
        # Save to file
        report_file = f"startup_profile_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(report_file, 'w') as f:
            json.dump({
                'total_time': total_time,
                'timings': self.timings,
                'timestamp': datetime.now().isoformat()
            }, f, indent=2)
        print(f"\n✅ Full report saved to: {report_file}")

def profile_full_startup():
    """Profile the complete dashboard startup process"""
    
    profiler = StartupProfiler()
    
    print("Starting comprehensive dashboard startup profiling...")
    print("="*80)
    
    # Phase 1: Python and environment setup
    profiler.start("Python initialization")
    profiler.end("Python initialization", "Script startup and path setup")
    
    # Phase 2: Load environment variables
    profiler.start("Environment setup")
    from dotenv import load_dotenv
    env_file = Path('.env')
    if env_file.exists():
        load_dotenv(env_file)
    profiler.end("Environment setup", f".env file {'loaded' if env_file.exists() else 'not found'}")
    
    # Phase 3: Import standard libraries
    profiler.start("Standard library imports")
    import asyncio
    import pandas as pd
    import numpy as np
    profiler.end("Standard library imports", "pandas, numpy, asyncio")
    
    # Phase 4: Import Panel and extensions
    profiler.start("Panel import and setup")
    import panel as pn
    pn.extension('tabulator')
    pn.config.theme = 'dark'
    profiler.end("Panel import and setup", "Panel with tabulator extension")
    
    # Phase 5: Import hvplot/holoviews
    profiler.start("HvPlot/HoloViews import")
    import hvplot.pandas
    import holoviews as hv
    from holoviews import opts
    hv.extension('bokeh')
    profiler.end("HvPlot/HoloViews import", "hvplot, holoviews with bokeh")
    
    # Phase 6: Import dashboard modules
    profiler.start("Dashboard module imports")
    from aemo_dashboard.generation.gen_dash import EnergyDashboard, create_app
    from aemo_dashboard.shared.logging_config import setup_logging
    profiler.end("Dashboard module imports", "EnergyDashboard and dependencies")
    
    # Phase 7: DuckDB setup
    profiler.start("DuckDB initialization")
    from data_service.shared_data_duckdb import duckdb_data_service
    # This triggers view creation
    view_count = len(duckdb_data_service._views_created) if hasattr(duckdb_data_service, '_views_created') else 'Unknown'
    profiler.end("DuckDB initialization", f"Views created: {view_count}")
    
    # Phase 8: Create app factory
    profiler.start("App factory creation")
    app_factory = create_app()
    profiler.end("App factory creation", "create_app() function")
    
    # Phase 9: Create dashboard instance (simulating first user)
    profiler.start("Dashboard instance creation")
    dashboard_instance = None
    try:
        # This is what happens when first user connects
        class MockState:
            """Mock Panel state for testing"""
            cache = {}
            def add_periodic_callback(self, *args, **kwargs):
                # Mock the periodic callback
                pass
        
        # Temporarily mock pn.state for testing
        original_state = pn.state
        pn.state = MockState()
        
        dashboard_instance = app_factory()
        
        # Restore original state
        pn.state = original_state
        
        profiler.end("Dashboard instance creation", "First user dashboard created")
    except Exception as e:
        profiler.end("Dashboard instance creation", f"Error: {str(e)}")
    
    # Phase 10: Individual component timing (if dashboard created successfully)
    if hasattr(dashboard_instance, 'param') and dashboard_instance is not None:
        # Extract the actual dashboard from the Column wrapper
        if hasattr(dashboard_instance, 'objects') and len(dashboard_instance.objects) > 0:
            # Find the tabs component
            tabs_component = None
            for obj in dashboard_instance.objects:
                if hasattr(obj, '_names') and 'Today' in getattr(obj, '_names', []):
                    tabs_component = obj
                    break
            
            if tabs_component:
                profiler.start("Today tab inspection")
                today_tab = tabs_component[0] if len(tabs_component) > 0 else None
                tab_type = type(today_tab).__name__ if today_tab else "Unknown"
                profiler.end("Today tab inspection", f"Tab type: {tab_type}")
    
    # Phase 11: Data loading simulation
    profiler.start("Initial data loading test")
    try:
        test_dashboard = EnergyDashboard()
        
        # Test generation data load
        start = time.time()
        gen_data = test_dashboard.load_generation_data()
        gen_time = time.time() - start
        
        # Test rooftop data load
        start = time.time()
        rooftop_data = test_dashboard.rooftop_solar_data
        rooftop_time = time.time() - start
        
        details = f"Generation: {gen_time:.3f}s, Rooftop: {rooftop_time:.3f}s"
        profiler.end("Initial data loading test", details)
    except Exception as e:
        profiler.end("Initial data loading test", f"Error: {str(e)}")
    
    # Phase 12: Widget creation timing
    profiler.start("Panel widget creation test")
    try:
        # Test creating individual widgets
        start = time.time()
        test_select = pn.widgets.Select(
            name='Region',
            value='NEM',
            options=['NEM', 'NSW1', 'QLD1', 'SA1', 'TAS1', 'VIC1']
        )
        select_time = time.time() - start
        
        start = time.time()
        test_radio = pn.widgets.RadioButtonGroup(
            name='Time Range',
            value='24 Hours',
            options=['24 Hours', '7 Days', '30 Days', 'All Data']
        )
        radio_time = time.time() - start
        
        details = f"Select: {select_time:.3f}s, Radio: {radio_time:.3f}s"
        profiler.end("Panel widget creation test", details)
    except Exception as e:
        profiler.end("Panel widget creation test", f"Error: {str(e)}")
    
    # Generate the report
    profiler.report()
    
    return profiler

def main():
    """Run the full startup profiling"""
    
    # Check if required files exist
    from aemo_dashboard.shared.config import config
    
    GEN_INFO_FILE = config.gen_info_file
    GEN_OUTPUT_FILE = config.gen_output_file
    
    if not os.path.exists(GEN_INFO_FILE):
        print(f"Error: {GEN_INFO_FILE} not found")
        return
    
    if not os.path.exists(GEN_OUTPUT_FILE):
        print(f"Error: {GEN_OUTPUT_FILE} not found")
        return
    
    # Run the profiling
    profiler = profile_full_startup()
    
    # Additional analysis
    print("\n" + "="*80)
    print("KEY FINDINGS:")
    print("="*80)
    
    # Find slowest phases
    slowest = sorted(profiler.timings, key=lambda x: x['duration'], reverse=True)[:5]
    print("\nTop 5 slowest phases:")
    for i, phase in enumerate(slowest, 1):
        print(f"{i}. {phase['phase']}: {phase['duration']:.3f}s")
    
    # Calculate categories
    import_time = sum(t['duration'] for t in profiler.timings if 'import' in t['phase'].lower())
    data_time = sum(t['duration'] for t in profiler.timings if 'data' in t['phase'].lower() or 'DuckDB' in t['phase'])
    ui_time = sum(t['duration'] for t in profiler.timings if 'widget' in t['phase'].lower() or 'tab' in t['phase'].lower() or 'instance' in t['phase'].lower())
    
    total = sum(t['duration'] for t in profiler.timings)
    
    print(f"\nTime by category:")
    print(f"- Imports: {import_time:.2f}s ({import_time/total*100:.1f}%)")
    print(f"- Data setup: {data_time:.2f}s ({data_time/total*100:.1f}%)")
    print(f"- UI creation: {ui_time:.2f}s ({ui_time/total*100:.1f}%)")
    print(f"- Other: {total - import_time - data_time - ui_time:.2f}s")
    
    print("\n✅ Profiling complete!")

if __name__ == "__main__":
    main()