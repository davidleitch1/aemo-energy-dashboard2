#!/usr/bin/env python3
"""
Run the AEMO dashboard with immediate initialization (no loading screen)
This avoids the callback timing issues
"""

import os
import sys
from pathlib import Path

# Set environment variable BEFORE any imports
os.environ['USE_DUCKDB'] = 'true'

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / 'src'))

# Monkey patch the create_app function to initialize immediately
def patch_immediate_init():
    """Patch dashboard to initialize immediately without loading screen"""
    from aemo_dashboard.generation import gen_dash
    
    def create_app_immediate():
        """Create app with immediate initialization"""
        def _create_dashboard():
            """Create dashboard without loading screen"""
            try:
                # Create dashboard instance immediately
                dashboard = gen_dash.EnergyDashboard()
                app = dashboard.create_dashboard()
                
                # Start auto-update
                import panel as pn
                if hasattr(pn.state, 'onload'):
                    pn.state.onload(lambda: dashboard.start_auto_update())
                
                return app
                
            except Exception as e:
                import panel as pn
                import traceback
                error_msg = f"Dashboard initialization error:\n{str(e)}\n\n{traceback.format_exc()}"
                return pn.pane.HTML(
                    f"<pre style='color: red; padding: 20px;'>{error_msg}</pre>",
                    sizing_mode='stretch_width'
                )
        
        return _create_dashboard
    
    # Replace the create_app function
    gen_dash.create_app = create_app_immediate

# Apply the patch
patch_immediate_init()

# Import and run the dashboard
from aemo_dashboard.generation.gen_dash import main

if __name__ == "__main__":
    print("Starting AEMO Dashboard with immediate initialization...")
    print("This version skips the loading screen to avoid timing issues")
    print("\nDashboard will be available at http://localhost:5008")
    print("Press Ctrl+C to stop\n")
    
    try:
        main()
    except KeyboardInterrupt:
        print("\nDashboard stopped by user")
    except Exception as e:
        print(f"\nError starting dashboard: {e}")
        import traceback
        traceback.print_exc()