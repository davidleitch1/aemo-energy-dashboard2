"""
Fixed version of dashboard initialization that properly handles the loading screen
"""

def create_app_fixed():
    """Create the Panel application with fixed initialization"""
    def _create_dashboard():
        """Factory function to create a new dashboard instance per session"""
        import panel as pn
        
        # Create container that will hold everything
        dashboard_container = pn.Column(sizing_mode='stretch_width')
        
        # Create loading screen
        loading_html = """
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
        """
        
        loading_pane = pn.pane.HTML(
            loading_html,
            sizing_mode='stretch_width',
            min_height=600
        )
        
        # Add loading screen initially
        dashboard_container.append(loading_pane)
        
        # Function to initialize dashboard
        def initialize_dashboard():
            try:
                print("Starting dashboard initialization...")
                
                # Import here to avoid circular imports
                from . import gen_dash
                
                # Create dashboard instance
                dashboard = gen_dash.EnergyDashboard()
                
                # Create the app
                app = dashboard.create_dashboard()
                
                # Replace loading screen with actual dashboard
                dashboard_container.clear()
                dashboard_container.append(app)
                
                print("Dashboard initialization complete")
                
                # Start auto-update after dashboard is loaded
                if hasattr(pn.state, 'curdoc') and pn.state.curdoc:
                    # We're in a server context
                    try:
                        dashboard.start_auto_update()
                    except Exception as e:
                        print(f"Error starting auto-update: {e}")
                
            except Exception as e:
                print(f"Error creating dashboard: {e}")
                import traceback
                traceback.print_exc()
                
                dashboard_container.clear()
                dashboard_container.append(
                    pn.pane.HTML(
                        f"<h1>Application Error</h1><pre>{str(e)}</pre>",
                        style={'color': 'red', 'padding': '20px'}
                    )
                )
        
        # Use pn.state.onload for server context
        if hasattr(pn.state, 'onload'):
            # Server context - use onload
            pn.state.onload(initialize_dashboard)
        else:
            # Non-server context - initialize immediately
            initialize_dashboard()
        
        return dashboard_container
    
    return _create_dashboard