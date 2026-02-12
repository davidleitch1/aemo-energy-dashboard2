#!/usr/bin/env python3
"""
Fast-loading version of the Energy Dashboard with optimized startup
"""
import os
import time
import threading
from datetime import datetime, timedelta
from typing import Optional, Dict, Any

import panel as pn
import param

# Defer heavy imports
pandas = None
hvplot = None
numpy = None

def lazy_import_pandas():
    """Import pandas only when needed"""
    global pandas
    if pandas is None:
        import pandas as pd
        pandas = pd
    return pandas

def lazy_import_hvplot():
    """Import hvplot only when needed"""
    global hvplot
    if hvplot is None:
        import hvplot.pandas
        hvplot = hvplot.pandas
    return hvplot

def lazy_import_numpy():
    """Import numpy only when needed"""
    global numpy
    if numpy is None:
        import numpy as np
        numpy = np
    return numpy

# Import minimal requirements
from ..shared.logging_config import get_logger
from ..shared.config import Config

logger = get_logger(__name__)

class FastEnergyDashboard(param.Parameterized):
    """Optimized dashboard with fast startup"""
    
    # Time range parameters
    time_range = param.Selector(
        default="Last 7 Days",
        objects=["Last 24 Hours", "Last 7 Days", "Last 30 Days", "Last 90 Days", 
                "Last 6 Months", "Last Year", "All Available Data", "Custom Range"],
        doc="Select time range for analysis"
    )
    
    start_date = param.Date(
        default=None,
        doc="Start date for custom range"
    )
    
    end_date = param.Date(
        default=None,
        doc="End date for custom range"  
    )
    
    auto_update = param.Boolean(
        default=False,
        doc="Enable auto-update every 5 minutes"
    )
    
    def __init__(self, **params):
        super().__init__(**params)
        self.config = Config()
        
        # Defer heavy initialization
        self._tabs_content = {}
        self._current_tab = None
        self._query_managers = {}
        self._initialized_tabs = set()
        
        # Lightweight UI elements
        self.status_text = pn.pane.Markdown("", width=300)
        self.last_update_text = pn.pane.Markdown("Dashboard starting...", width=300)
        
        # Create the main layout immediately
        self.main_tabs = pn.Tabs(
            ("NEM Dashboard", pn.pane.Markdown("Loading...", height=600)),
            ("Generation", pn.pane.Markdown("Loading...", height=600)),
            ("Price Analysis", pn.pane.Markdown("Loading...", height=600)),
            ("Station Analysis", pn.pane.Markdown("Loading...", height=600)),
            active=0,
            dynamic=True
        )
        
        # Set up tab change callback
        self.main_tabs.param.watch(self._on_tab_change, 'active')
        
        # Load first tab in background
        threading.Thread(target=self._load_initial_tab, daemon=True).start()
    
    def _load_initial_tab(self):
        """Load the first tab in background"""
        time.sleep(0.1)  # Let UI render first
        self._load_tab_content(0)
    
    def _on_tab_change(self, event):
        """Handle tab changes with lazy loading"""
        tab_index = event.new
        if tab_index not in self._initialized_tabs:
            # Show loading message immediately
            tab_names = ["NEM Dashboard", "Generation", "Price Analysis", "Station Analysis"]
            self.status_text.object = f"Loading {tab_names[tab_index]}..."
            
            # Load content in background
            threading.Thread(
                target=self._load_tab_content,
                args=(tab_index,),
                daemon=True
            ).start()
    
    def _load_tab_content(self, tab_index: int):
        """Load tab content on demand"""
        if tab_index in self._initialized_tabs:
            return
            
        try:
            if tab_index == 0:  # NEM Dashboard
                content = self._create_nem_dashboard()
            elif tab_index == 1:  # Generation
                content = self._create_generation_tab()
            elif tab_index == 2:  # Price Analysis
                content = self._create_price_analysis_tab()
            elif tab_index == 3:  # Station Analysis
                content = self._create_station_analysis_tab()
            else:
                content = pn.pane.Markdown("Unknown tab")
            
            # Update the tab content
            self.main_tabs[tab_index] = content
            self._initialized_tabs.add(tab_index)
            self.status_text.object = ""
            
        except Exception as e:
            logger.error(f"Error loading tab {tab_index}: {e}")
            self.main_tabs[tab_index] = pn.pane.Markdown(f"Error loading tab: {str(e)}")
    
    def _create_nem_dashboard(self):
        """Create NEM dashboard with deferred imports"""
        # Import only when needed
        from ..nem_dash.nem_dash_tab_lightweight import create_nem_dash_tab
        
        # Get or create fast query manager
        if 'nem' not in self._query_managers:
            from ..shared.hybrid_query_manager_fast import FastHybridQueryManager
            from ..nem_dash.nem_dash_query_manager import NEMDashQueryManager
            # Use fast hybrid manager as base
            self._query_managers['hybrid'] = FastHybridQueryManager()
            self._query_managers['nem'] = NEMDashQueryManager()
            # Share the fast connection
            self._query_managers['nem'].query_manager = self._query_managers['hybrid']
        
        return create_nem_dash_tab(self._query_managers['nem'])
    
    def _create_generation_tab(self):
        """Create generation tab with deferred imports"""
        # Lazy import pandas for this tab
        pd = lazy_import_pandas()
        
        # Import generation components
        from ..generation.components.generation_by_fuel import GenerationByFuelChart
        from ..generation.generation_query_manager import GenerationQueryManager
        
        # Get or create query manager
        if 'generation' not in self._query_managers:
            self._query_managers['generation'] = GenerationQueryManager()
        
        # Create simple layout for now
        chart = GenerationByFuelChart(query_manager=self._query_managers['generation'])
        
        return pn.Column(
            "# Generation Analysis",
            chart.panel,
            sizing_mode='stretch_both'
        )
    
    def _create_price_analysis_tab(self):
        """Create price analysis tab"""
        return pn.pane.Markdown(
            "# Price Analysis\n\nPrice analysis components will load here.",
            height=600
        )
    
    def _create_station_analysis_tab(self):
        """Create station analysis tab"""
        return pn.pane.Markdown(
            "# Station Analysis\n\nStation analysis components will load here.",
            height=600
        )
    
    def _create_time_controls(self):
        """Create time range controls"""
        time_controls = pn.Row(
            pn.pane.Markdown("**Time Range:**", width=100),
            pn.widgets.Select.from_param(self.param.time_range, width=200),
            pn.pane.Markdown("**Auto Update:**", width=100),
            pn.widgets.Checkbox.from_param(self.param.auto_update, width=50),
            self.last_update_text
        )
        
        # Custom date range controls
        custom_controls = pn.Row(
            pn.pane.Markdown("**Custom Range:**", width=100),
            pn.widgets.DatePicker.from_param(self.param.start_date, width=150),
            pn.pane.Markdown("to", width=30),
            pn.widgets.DatePicker.from_param(self.param.end_date, width=150),
            visible=False
        )
        
        # Show/hide custom controls based on selection
        def toggle_custom_controls(event):
            custom_controls.visible = (event.new == "Custom Range")
        
        self.param.watch(toggle_custom_controls, 'time_range')
        
        return pn.Column(time_controls, custom_controls)
    
    def view(self):
        """Create the main dashboard view"""
        template = pn.template.MaterialTemplate(
            title="AEMO Energy Dashboard (Fast)",
            sidebar=[
                pn.pane.Markdown("## Dashboard Controls"),
                self._create_time_controls(),
                pn.layout.Divider(),
                self.status_text
            ]
        )
        
        template.main.append(self.main_tabs)
        return template


def main():
    """Main entry point with fast startup"""
    start_time = time.time()
    
    # Create dashboard instance
    dashboard = FastEnergyDashboard()
    
    # Get the template
    template = dashboard.view()
    
    # Configure server with minimal options
    logger.info(f"Dashboard initialized in {time.time() - start_time:.2f}s")
    
    # Serve the dashboard
    template.servable()
    
    # Only show if running directly
    if __name__.startswith('bokeh_app'):
        # Running in server
        pass
    else:
        # Running as script
        template.show(
            port=5006,
            threaded=True,
            verbose=False,
            open=False
        )


if __name__ == "__main__":
    main()