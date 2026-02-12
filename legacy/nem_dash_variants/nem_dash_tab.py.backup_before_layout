"""
Nem-dash Tab - Primary Dashboard View
Combines price section, renewable energy gauge, and 24-hour generation overview
"""

import panel as pn
from ..shared.logging_config import get_logger
from .price_components import create_price_section
from .renewable_gauge import create_renewable_gauge_component
from .generation_overview import create_generation_overview_component

# Custom CSS to remove chart borders
CUSTOM_CSS = """
.chart-no-border .bk-root {
    border: none !important;
    outline: none !important;
}

.chart-no-border .bk {
    border: none !important;
    outline: none !important;
}

.chart-no-border {
    border: none !important;
    outline: none !important;
}
"""

logger = get_logger(__name__)


def create_nem_dash_tab(dashboard_instance=None):
    """
    Create the complete Nem-dash tab layout
    
    Args:
        dashboard_instance: Reference to main dashboard for data sharing
        
    Returns:
        Panel component containing the complete Nem-dash layout
    """
    try:
        logger.info("Creating Nem-dash tab components")
        
        # Get date range from dashboard instance if available
        start_date = getattr(dashboard_instance, 'start_date', None) if dashboard_instance else None
        end_date = getattr(dashboard_instance, 'end_date', None) if dashboard_instance else None
        
        # Add detailed logging about date types
        logger.info(f"Raw date values from dashboard - start: {start_date} (type: {type(start_date)}), end: {end_date} (type: {type(end_date)})")
        
        # Convert date objects to datetime objects for compatibility
        if start_date is not None:
            from datetime import datetime
            if not hasattr(start_date, 'hour'):  # It's a date object, not datetime
                start_date = datetime.combine(start_date, datetime.min.time())
                logger.info(f"Converted start_date to datetime: {start_date}")
        
        if end_date is not None:
            from datetime import datetime
            if not hasattr(end_date, 'hour'):  # It's a date object, not datetime
                end_date = datetime.combine(end_date, datetime.max.time())
                logger.info(f"Converted end_date to datetime: {end_date}")
        
        logger.info(f"Creating price section with date range: {start_date} to {end_date}")
        
        # Create individual components
        price_section = create_price_section(start_date, end_date)
        renewable_gauge = create_renewable_gauge_component(dashboard_instance)
        generation_overview = create_generation_overview_component(dashboard_instance)
        
        # Create the layout with 2x2 grid appearance:
        # Top row: Price section (left) and Renewable gauge (right)
        # Bottom row: Empty space (left) and Generation chart (right, spanning wider)
        layout = pn.Column(
            # Top row: Price section and gauge side by side
            pn.Row(
                price_section,          # ~550px width (left)
                renewable_gauge,        # ~400px width (right)
                sizing_mode='stretch_width',
                margin=(5, 5)
            ),
            # Bottom row: Empty left, Generation chart on right
            pn.Row(
                pn.Spacer(width=550),   # Empty space matching price section width
                generation_overview,    # ~800px width, 400px height
                sizing_mode='stretch_width',
                margin=(5, 5)
            ),
            sizing_mode='stretch_width',
            margin=(10, 10),
            name="Nem-dash",  # Tab name
            stylesheets=[CUSTOM_CSS]  # Apply custom CSS
        )
        
        logger.info("Nem-dash tab created successfully")
        return layout
        
    except Exception as e:
        logger.error(f"Error creating Nem-dash tab: {e}")
        # Return error fallback
        return pn.Column(
            pn.pane.HTML(
                f"<div style='padding:20px;text-align:center;'>"
                f"<h2>Error Loading Nem-dash</h2>"
                f"<p>Error: {e}</p>"
                f"<p>Please check the logs and try refreshing.</p>"
                f"</div>"
            ),
            name="Nem-dash",
            sizing_mode='stretch_width',
            height=600
        )


def create_nem_dash_tab_with_updates(dashboard_instance=None, auto_update=True):
    """
    Create Nem-dash tab with auto-update functionality
    
    Args:
        dashboard_instance: Reference to main dashboard for data sharing
        auto_update: Whether to enable auto-updates (default True)
        
    Returns:
        Panel component with auto-update capability
    """
    try:
        # Create the basic tab
        tab = create_nem_dash_tab(dashboard_instance)
        
        if auto_update:
            def update_all_components():
                """Update all components in the tab"""
                try:
                    logger.info("Updating Nem-dash tab components")
                    
                    # Get the current components with 2x2 grid layout
                    top_row = tab[0]      # Row with price section and gauge
                    bottom_row = tab[1]   # Row with spacer and generation chart
                    
                    # Get current date range from dashboard
                    start_date = getattr(dashboard_instance, 'start_date', None) if dashboard_instance else None
                    end_date = getattr(dashboard_instance, 'end_date', None) if dashboard_instance else None
                    
                    logger.info(f"Update: Raw dates - start: {start_date} (type: {type(start_date)}), end: {end_date} (type: {type(end_date)})")
                    
                    # Convert date objects to datetime objects for compatibility
                    if start_date is not None:
                        from datetime import datetime
                        if not hasattr(start_date, 'hour'):  # It's a date object, not datetime
                            start_date = datetime.combine(start_date, datetime.min.time())
                            logger.info(f"Update: Converted start_date to datetime: {start_date}")
                    
                    if end_date is not None:
                        from datetime import datetime
                        if not hasattr(end_date, 'hour'):  # It's a date object, not datetime
                            end_date = datetime.combine(end_date, datetime.max.time())
                            logger.info(f"Update: Converted end_date to datetime: {end_date}")
                    
                    # Update price section (index 0) with date filtering
                    new_price_section = create_price_section(start_date, end_date)
                    top_row[0] = new_price_section
                    
                    # Update renewable gauge (index 1)
                    new_gauge = create_renewable_gauge_component(dashboard_instance)
                    top_row[1] = new_gauge
                    
                    # Update generation overview (index 1 of bottom row, after spacer)
                    new_overview = create_generation_overview_component(dashboard_instance)
                    bottom_row[1] = new_overview
                    
                    logger.info("Nem-dash tab components updated successfully")
                    
                except Exception as e:
                    logger.error(f"Error updating Nem-dash components: {e}")
            
            # Set up periodic updates (every 4.5 minutes = 270000ms)
            pn.state.add_periodic_callback(update_all_components, 270000)
            logger.info("Auto-update enabled for Nem-dash tab (4.5 minute intervals)")
        
        return tab
        
    except Exception as e:
        logger.error(f"Error creating Nem-dash tab with updates: {e}")
        return create_nem_dash_tab(dashboard_instance)


if __name__ == "__main__":
    # Test the complete Nem-dash tab
    pn.extension(['bokeh', 'plotly'])
    
    # Create test tab
    tab = create_nem_dash_tab()
    
    # Create a simple app to test
    template = pn.template.MaterialTemplate(
        title="Nem-dash Test",
        sidebar=[],
        main=[tab]
    )
    
    template.show(port=5555)