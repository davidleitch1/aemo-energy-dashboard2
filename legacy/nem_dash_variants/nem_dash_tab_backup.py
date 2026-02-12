"""
Nem-dash Tab - Primary Dashboard View
Combines price section, renewable energy gauge, and 24-hour generation overview
"""

import panel as pn
from ..shared.logging_config import get_logger
from .price_components import create_price_section, create_price_chart_component, create_price_table_component, PriceDisplay
from .renewable_gauge import create_renewable_gauge_component
from .generation_overview import create_generation_overview_component
from .daily_summary import create_daily_summary_component

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
        
        # FIX: Convert both date AND datetime objects to proper day boundaries
        # This fixes the midnight bug where datetime.now() was passed instead of date objects
        if start_date is not None:
            from datetime import datetime
            if not hasattr(start_date, 'hour'):  # It's a date object, not datetime
                start_date = datetime.combine(start_date, datetime.min.time())
                logger.info(f"Converted start_date from date to datetime: {start_date}")
            else:
                # It's a datetime - convert to start of day
                start_date = datetime.combine(start_date.date(), datetime.min.time())
                logger.info(f"Reset start_date to start of day: {start_date}")
        
        if end_date is not None:
            from datetime import datetime
            if not hasattr(end_date, 'hour'):  # It's a date object, not datetime
                end_date = datetime.combine(end_date, datetime.max.time())
                logger.info(f"Converted end_date from date to datetime: {end_date}")
            else:
                # It's a datetime - convert to end of day
                end_date = datetime.combine(end_date.date(), datetime.max.time())
                logger.info(f"Reset end_date to end of day: {end_date}")
        
        logger.info(f"Creating price components with date range: {start_date} to {end_date}")
        
        # Create individual components for clean 2x3 grid layout
        price_chart = create_price_chart_component(start_date, end_date)
        price_table = create_price_table_component(start_date, end_date)
        renewable_gauge = create_renewable_gauge_component(dashboard_instance)
        generation_overview = create_generation_overview_component(dashboard_instance)
        daily_summary = create_daily_summary_component()
        
        # Create the layout with 2x3 grid
        # Top row: Price chart and Generation chart side by side
        # Bottom row: Price table, Renewable gauge, and Daily summary
        layout = pn.Column(
            # Top row: Two main charts side by side
            pn.Row(
                price_chart,            # Price chart (left)
                generation_overview,    # Generation chart (right)
                sizing_mode='stretch_width',
                margin=(5, 5)
            ),
            # Bottom row: Price table, gauge, and daily summary
            pn.Row(
                price_table,            # Price table (left)
                renewable_gauge,        # Renewable gauge (middle)
                daily_summary,          # Daily summary (right)
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
    Create Nem-dash tab with auto-update functionality using PriceDisplay pattern
    
    Args:
        dashboard_instance: Reference to main dashboard for data sharing
        auto_update: Whether to enable auto-updates (default True)
        
    Returns:
        Panel component with auto-update capability
    """
    try:
        logger.info("Creating Nem-dash tab with PriceDisplay pattern")
        
        # Get initial date range from dashboard instance if available
        start_date = getattr(dashboard_instance, 'start_date', None) if dashboard_instance else None
        end_date = getattr(dashboard_instance, 'end_date', None) if dashboard_instance else None
        
        # FIX: Convert both date AND datetime objects to proper day boundaries
        # This fixes the midnight bug where datetime.now() was passed instead of date objects
        if start_date is not None:
            from datetime import datetime
            if not hasattr(start_date, 'hour'):  # It's a date object, not datetime
                start_date = datetime.combine(start_date, datetime.min.time())
                logger.info(f"Initial: Converted start_date from date to datetime: {start_date}")
            else:
                # It's a datetime - convert to start of day
                start_date = datetime.combine(start_date.date(), datetime.min.time())
                logger.info(f"Initial: Reset start_date to start of day: {start_date}")
        
        if end_date is not None:
            from datetime import datetime
            if not hasattr(end_date, 'hour'):  # It's a date object, not datetime
                end_date = datetime.combine(end_date, datetime.max.time())
                logger.info(f"Initial: Converted end_date from date to datetime: {end_date}")
            else:
                # It's a datetime - convert to end of day
                end_date = datetime.combine(end_date.date(), datetime.max.time())
                logger.info(f"Initial: Reset end_date to end of day: {end_date}")
        
        # FIX: Create PriceDisplay instance with persistent panes
        price_display = PriceDisplay()
        
        # Initialize with current date range
        price_display.update(start_date, end_date)
        
        # Get persistent panes from PriceDisplay
        price_chart = price_display.get_chart()
        price_table = price_display.get_table()
        
        # Create other components (these will still use the old pattern for now)
        renewable_gauge = create_renewable_gauge_component(dashboard_instance)
        generation_overview = create_generation_overview_component(dashboard_instance)
        daily_summary = create_daily_summary_component()
        
        # Create the layout with 2x3 grid
        # Top row: Price chart and Generation chart side by side
        # Bottom row: Price table, Renewable gauge, and Daily summary
        tab = pn.Column(
            # Top row: Two main charts side by side
            pn.Row(
                price_chart,            # Persistent price chart pane
                generation_overview,    # Generation chart (will be updated later)
                sizing_mode='stretch_width',
                margin=(5, 5),
                name='top_row'
            ),
            # Bottom row: Price table, gauge, and daily summary
            pn.Row(
                price_table,            # Persistent price table pane
                renewable_gauge,        # Renewable gauge (will be updated later)
                daily_summary,          # Daily summary (will be updated later)
                sizing_mode='stretch_width',
                margin=(5, 5),
                name='bottom_row'
            ),
            sizing_mode='stretch_width',
            margin=(10, 10),
            name="Nem-dash",  # Tab name
            stylesheets=[CUSTOM_CSS]  # Apply custom CSS
        )
        
        if auto_update:
            def update_all_components():
                """Update all components in the tab"""
                try:
                    logger.info("Updating Nem-dash tab components with PriceDisplay")
                    
                    # Get the current row references
                    top_row = tab[0]
                    bottom_row = tab[1]
                    
                    # FIX for midnight rollover: First refresh dashboard dates if using preset time ranges
                    date_range_changed = False
                    if dashboard_instance and hasattr(dashboard_instance, 'time_range'):
                        time_range = getattr(dashboard_instance, 'time_range', None)
                        if time_range in ['1', '7', '30']:
                            # Store old date range
                            old_start_date = getattr(dashboard_instance, 'start_date', None)
                            old_end_date = getattr(dashboard_instance, 'end_date', None)
                            old_range = (old_start_date, old_end_date)
                            
                            # Refresh the dashboard's date range to current values
                            if hasattr(dashboard_instance, '_update_date_range_from_preset'):
                                dashboard_instance._update_date_range_from_preset()
                                new_start_date = getattr(dashboard_instance, 'start_date', None)
                                new_end_date = getattr(dashboard_instance, 'end_date', None)
                                new_range = (new_start_date, new_end_date)
                                
                                if old_range != new_range:
                                    date_range_changed = True
                                    logger.info(f"NEM dash: Date RANGE changed from {old_range} to {new_range}")
                                    
                                    # Force component refresh when date range changes
                                    if hasattr(dashboard_instance, '_force_component_refresh'):
                                        dashboard_instance._force_component_refresh()
                                elif old_end_date != new_end_date:
                                    logger.info(f"NEM dash: Date rollover detected, updated end_date from {old_end_date} to {new_end_date}")
                    
                    # Get current date range from dashboard (now refreshed!)
                    start_date = getattr(dashboard_instance, 'start_date', None) if dashboard_instance else None
                    end_date = getattr(dashboard_instance, 'end_date', None) if dashboard_instance else None
                    
                    logger.info(f"Update: Raw dates - start: {start_date} (type: {type(start_date)}), end: {end_date} (type: {type(end_date)})")
                    
                    # FIX: Convert both date AND datetime objects to proper day boundaries
                    # This fixes the midnight bug where datetime.now() was passed instead of date objects
                    if start_date is not None:
                        from datetime import datetime
                        if not hasattr(start_date, 'hour'):  # It's a date object, not datetime
                            start_date = datetime.combine(start_date, datetime.min.time())
                            logger.info(f"Update: Converted start_date from date to datetime: {start_date}")
                        else:
                            # It's a datetime - convert to start of day
                            start_date = datetime.combine(start_date.date(), datetime.min.time())
                            logger.info(f"Update: Reset start_date to start of day: {start_date}")
                    
                    if end_date is not None:
                        from datetime import datetime
                        if not hasattr(end_date, 'hour'):  # It's a date object, not datetime
                            end_date = datetime.combine(end_date, datetime.max.time())
                            logger.info(f"Update: Converted end_date from date to datetime: {end_date}")
                        else:
                            # It's a datetime - convert to end of day
                            end_date = datetime.combine(end_date.date(), datetime.max.time())
                            logger.info(f"Update: Reset end_date to end of day: {end_date}")
                    
                    # CRITICAL FIX: Update PriceDisplay via object properties (not replacement!)
                    price_display.update(start_date, end_date)
                    logger.info("PriceDisplay updated successfully via object properties")
                    
                    # For other components, still need to replace (will be fixed in future)
                    new_overview = create_generation_overview_component(dashboard_instance)
                    new_gauge = create_renewable_gauge_component(dashboard_instance)
                    new_daily_summary = create_daily_summary_component()
                    
                    # Update only the non-price components in the rows
                    # Price components are already updated via PriceDisplay.update()
                    top_row[1] = new_overview  # Only replace generation overview
                    
                    bottom_row[1] = new_gauge  # Replace gauge
                    bottom_row[2] = new_daily_summary  # Replace daily summary
                    
                    logger.info("Nem-dash tab components updated successfully")
                    
                except Exception as e:
                    logger.error(f"Error updating Nem-dash components: {e}")
            
            # Set up periodic updates (every 4.5 minutes = 270000ms)
            pn.state.add_periodic_callback(update_all_components, 270000)
            logger.info("Auto-update enabled for Nem-dash tab (4.5 minute intervals)")
        
        logger.info("Nem-dash tab with PriceDisplay created successfully")
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