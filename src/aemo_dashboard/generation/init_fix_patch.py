"""
Patch to fix dashboard initialization hang
"""

import logging

logger = logging.getLogger(__name__)

def patch_dashboard_initialization():
    """Patch the dashboard initialization to fix the loading screen hang"""
    
    # Import the gen_dash module
    from . import gen_dash
    
    # Store original create_app
    original_create_app = gen_dash.create_app
    
    def create_app_fixed():
        """Fixed version of create_app"""
        def _create_dashboard():
            """Fixed dashboard creation"""
            import panel as pn
            
            # Create the dashboard immediately without loading screen
            try:
                logger.info("Creating dashboard instance (fixed version)...")
                
                # Create dashboard
                dashboard = gen_dash.EnergyDashboard()
                app = dashboard.create_dashboard()
                
                # Wrap in a column to ensure proper sizing
                container = pn.Column(
                    app,
                    sizing_mode='stretch_width'
                )
                
                # Schedule auto-update start
                def start_updates():
                    try:
                        dashboard.start_auto_update()
                        logger.info("Auto-update started successfully")
                    except Exception as e:
                        logger.error(f"Failed to start auto-update: {e}")
                
                # Use onload if available, otherwise start immediately
                if hasattr(pn.state, 'onload'):
                    pn.state.onload(start_updates)
                else:
                    # In non-server context, just return without auto-update
                    logger.info("Non-server context detected, skipping auto-update")
                
                return container
                
            except Exception as e:
                logger.error(f"Dashboard creation failed: {e}")
                import traceback
                error_details = traceback.format_exc()
                
                # Return error display
                return pn.Column(
                    pn.pane.HTML(
                        f"""
                        <div style='padding: 20px; color: red;'>
                            <h2>Dashboard Initialization Error</h2>
                            <pre>{str(e)}</pre>
                            <details>
                                <summary>Full traceback</summary>
                                <pre>{error_details}</pre>
                            </details>
                        </div>
                        """,
                        sizing_mode='stretch_width'
                    )
                )
        
        return _create_dashboard
    
    # Replace the create_app function
    gen_dash.create_app = create_app_fixed
    logger.info("Dashboard initialization patched successfully")