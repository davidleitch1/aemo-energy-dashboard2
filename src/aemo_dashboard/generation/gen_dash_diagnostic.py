"""
Diagnostic wrapper for dashboard initialization with detailed logging
"""

import time
import functools
import os
from datetime import datetime

# Enable debug mode if environment variable is set
DEBUG_MODE = os.getenv('DASHBOARD_DEBUG', 'false').lower() == 'true'

def log_timing(func):
    """Decorator to log function execution time"""
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        if not DEBUG_MODE:
            return func(*args, **kwargs)
            
        start_time = time.time()
        func_name = f"{func.__module__}.{func.__name__}"
        print(f"[{datetime.now().strftime('%H:%M:%S.%f')[:-3]}] Starting: {func_name}")
        
        try:
            result = func(*args, **kwargs)
            duration = time.time() - start_time
            print(f"[{datetime.now().strftime('%H:%M:%S.%f')[:-3]}] Completed: {func_name} ({duration:.3f}s)")
            return result
        except Exception as e:
            duration = time.time() - start_time
            print(f"[{datetime.now().strftime('%H:%M:%S.%f')[:-3]}] FAILED: {func_name} ({duration:.3f}s) - {str(e)}")
            raise
    
    return wrapper

def patch_dashboard_for_diagnostics():
    """Patch the dashboard classes with diagnostic logging"""
    if not DEBUG_MODE:
        return
        
    print(f"[{datetime.now().strftime('%H:%M:%S.%f')[:-3]}] Diagnostic mode enabled - patching dashboard methods")
    
    # Import the modules to patch
    from . import gen_dash
    from ..shared import duckdb_views
    from . import generation_query_manager
    
    # Patch EnergyDashboard methods
    gen_dash.EnergyDashboard.__init__ = log_timing(gen_dash.EnergyDashboard.__init__)
    gen_dash.EnergyDashboard.load_reference_data = log_timing(gen_dash.EnergyDashboard.load_reference_data)
    gen_dash.EnergyDashboard._initialize_panes = log_timing(gen_dash.EnergyDashboard._initialize_panes)
    gen_dash.EnergyDashboard.create_plot = log_timing(gen_dash.EnergyDashboard.create_plot)
    gen_dash.EnergyDashboard.create_utilization_plot = log_timing(gen_dash.EnergyDashboard.create_utilization_plot)
    gen_dash.EnergyDashboard.create_transmission_plot = log_timing(gen_dash.EnergyDashboard.create_transmission_plot)
    gen_dash.EnergyDashboard.create_dashboard = log_timing(gen_dash.EnergyDashboard.create_dashboard)
    
    # Patch GenerationQueryManager
    generation_query_manager.GenerationQueryManager.__init__ = log_timing(generation_query_manager.GenerationQueryManager.__init__)
    
    # Patch DuckDB view creation
    duckdb_views.DuckDBViewManager.create_all_views = log_timing(duckdb_views.DuckDBViewManager.create_all_views)
    duckdb_views.DuckDBViewManager._create_integration_views = log_timing(duckdb_views.DuckDBViewManager._create_integration_views)
    duckdb_views.DuckDBViewManager._create_aggregation_views = log_timing(duckdb_views.DuckDBViewManager._create_aggregation_views)
    duckdb_views.DuckDBViewManager._create_helper_views = log_timing(duckdb_views.DuckDBViewManager._create_helper_views)
    duckdb_views.DuckDBViewManager._create_materialized_views = log_timing(duckdb_views.DuckDBViewManager._create_materialized_views)
    
    # Patch the initialization callback
    original_initialize = None
    
    def patched_create_app():
        """Patched version of create_app with diagnostic logging"""
        print(f"[{datetime.now().strftime('%H:%M:%S.%f')[:-3]}] Starting create_app")
        
        original_create_app = gen_dash.create_app.__wrapped__ if hasattr(gen_dash.create_app, '__wrapped__') else gen_dash.create_app
        
        def _create_dashboard():
            print(f"[{datetime.now().strftime('%H:%M:%S.%f')[:-3]}] Creating dashboard factory function")
            
            # Import Panel
            import panel as pn
            
            # Create loading screen
            loading_screen = pn.Column(
                pn.pane.HTML(
                    """
                    <div style='text-align: center; padding: 100px;'>
                        <h1 style='color: #008B8B;'>NEM Analysis Dashboard</h1>
                        <div style='margin: 50px auto;'>
                            <div class="spinner" style="margin: 0 auto;"></div>
                            <p style='margin-top: 20px; font-size: 18px; color: #666;'>
                                Initializing dashboard components...
                            </p>
                        </div>
                    </div>
                    <style>
                        .spinner {
                            width: 60px;
                            height: 60px;
                            border: 6px solid #f3f3f3;
                            border-top: 6px solid #008B8B;
                            border-radius: 50%;
                            animation: spin 1s linear infinite;
                        }
                        @keyframes spin {
                            0% { transform: rotate(0deg); }
                            100% { transform: rotate(360deg); }
                        }
                    </style>
                    """,
                    sizing_mode='stretch_width',
                    min_height=600
                )
            )
            
            dashboard_container = pn.Column(loading_screen, sizing_mode='stretch_width')
            
            def initialize_dashboard():
                print(f"[{datetime.now().strftime('%H:%M:%S.%f')[:-3]}] Initialize callback triggered")
                try:
                    # Create dashboard instance
                    print(f"[{datetime.now().strftime('%H:%M:%S.%f')[:-3]}] Creating EnergyDashboard instance...")
                    dashboard = gen_dash.EnergyDashboard()
                    
                    # Create the app
                    print(f"[{datetime.now().strftime('%H:%M:%S.%f')[:-3]}] Creating dashboard app...")
                    app = dashboard.create_dashboard()
                    
                    # Replace loading screen
                    print(f"[{datetime.now().strftime('%H:%M:%S.%f')[:-3]}] Replacing loading screen...")
                    dashboard_container.clear()
                    dashboard_container.append(app)
                    
                    # Start auto-update
                    def start_dashboard_updates():
                        print(f"[{datetime.now().strftime('%H:%M:%S.%f')[:-3]}] Starting auto-update...")
                        try:
                            dashboard.start_auto_update()
                        except Exception as e:
                            print(f"[{datetime.now().strftime('%H:%M:%S.%f')[:-3]}] Error starting auto-update: {e}")
                    
                    pn.state.onload(start_dashboard_updates)
                    print(f"[{datetime.now().strftime('%H:%M:%S.%f')[:-3]}] Dashboard initialization complete")
                    
                except Exception as e:
                    print(f"[{datetime.now().strftime('%H:%M:%S.%f')[:-3]}] Dashboard initialization FAILED: {e}")
                    import traceback
                    traceback.print_exc()
                    dashboard_container.clear()
                    dashboard_container.append(
                        pn.pane.HTML(f"<h1>Application Error: {str(e)}</h1>")
                    )
            
            # Schedule initialization
            print(f"[{datetime.now().strftime('%H:%M:%S.%f')[:-3]}] Scheduling dashboard initialization callback...")
            pn.state.add_periodic_callback(initialize_dashboard, period=100, count=1)
            
            return dashboard_container
        
        return _create_dashboard
    
    # Replace the create_app function
    gen_dash.create_app = patched_create_app
    
    print(f"[{datetime.now().strftime('%H:%M:%S.%f')[:-3]}] Diagnostic patching complete")