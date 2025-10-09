"""
Optimized NEM Dashboard Tab - Loads minimal data initially
"""

import panel as pn
import pandas as pd
from datetime import datetime, timedelta
import threading

from ..shared.logging_config import get_logger
from .renewable_gauge import create_renewable_gauge_component
from .generation_overview import create_generation_overview_component

logger = get_logger(__name__)

# Custom CSS for the NEM dash
CUSTOM_CSS = """
.nem-dash-container {
    padding: 10px;
}
.loading-message {
    text-align: center;
    padding: 20px;
    color: #666;
}
"""


def create_nem_dash_tab_optimized(dashboard_instance=None):
    """
    Create optimized NEM dash tab with progressive loading
    """
    try:
        logger.info("Creating optimized Nem-dash tab")
        
        # Create placeholders for components
        price_placeholder = pn.pane.HTML(
            '<div class="loading-message">Loading price data...</div>',
            width=450, height=300
        )
        
        gauge_placeholder = pn.pane.HTML(
            '<div class="loading-message">Loading renewable status...</div>',
            width=400, height=350
        )
        
        generation_placeholder = pn.pane.HTML(
            '<div class="loading-message">Loading generation overview...</div>',
            width=800, height=400
        )
        
        # Create initial layout with placeholders
        layout = pn.Column(
            pn.Row(
                generation_placeholder,
                price_placeholder,
                sizing_mode='stretch_width',
                margin=(5, 5)
            ),
            pn.Row(
                gauge_placeholder,
                sizing_mode='stretch_width',
                margin=(5, 5)
            ),
            sizing_mode='stretch_width',
            margin=(10, 10),
            name="Nem-dash",
            stylesheets=[CUSTOM_CSS]
        )
        
        # Load components progressively
        def load_price_section():
            try:
                # Create price display with only last hour of data
                from .price_components import create_price_section
                price_section = create_price_section()
                layout[0][1] = price_section
            except Exception as e:
                logger.error(f"Error loading price section: {e}")
                layout[0][1] = pn.pane.HTML(f"<div>Error loading prices: {e}</div>")
        
        def load_renewable_gauge():
            try:
                gauge = create_renewable_gauge_component(dashboard_instance)
                layout[1][0] = gauge
            except Exception as e:
                logger.error(f"Error loading renewable gauge: {e}")
                layout[1][0] = pn.pane.HTML(f"<div>Error loading gauge: {e}</div>")
        
        def load_generation_overview():
            try:
                # Load with minimal data (last 2 hours only)
                overview = create_generation_overview_minimal(dashboard_instance)
                layout[0][0] = overview
            except Exception as e:
                logger.error(f"Error loading generation overview: {e}")
                layout[0][0] = pn.pane.HTML(f"<div>Error loading generation: {e}</div>")
        
        # Start loading components in order of importance
        threading.Thread(target=load_price_section, daemon=True).start()
        threading.Timer(0.2, load_renewable_gauge).start()
        threading.Timer(0.4, load_generation_overview).start()
        
        logger.info("Optimized Nem-dash tab created with progressive loading")
        return layout
        
    except Exception as e:
        logger.error(f"Error creating optimized Nem-dash tab: {e}")
        return pn.pane.HTML(f"<div>Error: {e}</div>")


def create_generation_overview_minimal(dashboard_instance=None):
    """Create generation overview with minimal initial data"""
    try:
        from ..shared import adapter_selector
        
        # Load only last 2 hours of data initially
        # Note: 2-hour window accounts for QLD/NSW timezone offset during DST
        end_date = datetime.now()
        start_date = end_date - timedelta(hours=2)

        # Use query manager if available
        if dashboard_instance and hasattr(dashboard_instance, 'query_manager'):
            from .nem_dash_query_manager import NEMDashQueryManager
            query_manager = NEMDashQueryManager()

            # Get minimal generation data
            gen_data = query_manager.get_generation_overview(hours=2)

            if gen_data.empty:
                # Fallback: Try 4-hour window if 2-hour window returns no data
                logger.warning("No data in 2hr window, trying 4hr fallback")
                gen_data = query_manager.get_generation_overview(hours=4)

            if gen_data.empty:
                return pn.pane.HTML(
                    '<div style="text-align:center; padding:50px;">'
                    'No generation data available</div>'
                )
            
            # Create simple bar chart
            import hvplot.pandas
            
            # Get latest values by fuel type
            latest_data = gen_data.groupby('fuel_type')['generation_mw'].last().sort_values(ascending=True)
            
            plot = latest_data.hvplot.barh(
                title='Current Generation by Fuel Type',
                xlabel='Generation (MW)',
                ylabel='Fuel Type',
                width=800,
                height=400,
                color='darkblue',
                hover_cols=['generation_mw']
            )
            
            return pn.pane.HoloViews(plot, sizing_mode='stretch_width')
            
        else:
            # Fallback to simple display
            return pn.pane.HTML(
                '<div style="text-align:center; padding:50px;">'
                'Generation overview loading...</div>'
            )
            
    except Exception as e:
        logger.error(f"Error creating minimal generation overview: {e}")
        return pn.pane.HTML(f"<div>Error: {e}</div>")


def create_nem_dash_tab_with_updates_optimized(dashboard_instance=None, auto_update=True):
    """
    Create optimized NEM dash tab with auto-update functionality
    """
    try:
        # Create the optimized tab
        tab = create_nem_dash_tab_optimized(dashboard_instance)
        
        if auto_update:
            # Set up periodic updates (less frequent initially)
            def update_components():
                try:
                    # Update logic here - but only after initial load
                    pass
                except Exception as e:
                    logger.error(f"Error updating NEM dash: {e}")
            
            # Start updates after 30 seconds to let initial load complete
            if dashboard_instance:
                threading.Timer(30.0, lambda: setattr(
                    dashboard_instance, 
                    '_nem_dash_updater', 
                    pn.state.add_periodic_callback(update_components, period=300000)
                )).start()
        
        return tab
        
    except Exception as e:
        logger.error(f"Error creating NEM dash with updates: {e}")
        return create_nem_dash_tab_optimized(dashboard_instance)