#!/usr/bin/env python3
"""
Optimized Energy Generation Dashboard with Lazy Tab Loading
Reduces startup time by loading tabs on-demand and eliminating duplicate initializations.
"""

import pandas as pd
import numpy as np
import panel as pn
import param
import holoviews as hv
import hvplot.pandas
import asyncio
import os
from datetime import datetime, timedelta
import pickle
from pathlib import Path
import json
import sys
from bokeh.models import DatetimeTickFormatter
from dotenv import load_dotenv
import time
import threading
from functools import lru_cache

from ..shared.config import config
from ..shared.logging_config import setup_logging, get_logger
from ..shared.email_alerts import EmailAlertManager
from ..analysis.price_analysis_ui import create_price_analysis_tab
from ..station.station_analysis_ui import create_station_analysis_tab
from ..nem_dash.nem_dash_tab import create_nem_dash_tab_with_updates
from ..nem_dash.nem_dash_tab_optimized import create_nem_dash_tab_with_updates_optimized
from .generation_query_manager import GenerationQueryManager

# Copy all the imports and configuration from the original file
from .gen_dash import (
    EnergyDashboard as OriginalEnergyDashboard,
    create_sample_env_file,
    GEN_INFO_FILE,
    GEN_OUTPUT_FILE
)

# Set up logging
setup_logging()
logger = get_logger(__name__)

# Configure Panel and HoloViews BEFORE extension loading
pn.config.theme = 'dark'
pn.extension('tabulator', 'plotly', template='material')

# Custom CSS to ensure x-axis labels are visible and style header
pn.config.raw_css.append("""
.bk-axis-label {
    font-size: 12px !important;
}
.bk-tick-label {
    font-size: 11px !important;
}
/* Header background styling */
.header-container {
    background-color: #008B8B;
    padding: 10px 0;
    margin: -10px -10px 10px -10px;
    border-radius: 4px 4px 0 0;
}
/* Loading spinner styling */
.loading-container {
    display: flex;
    align-items: center;
    justify-content: center;
    height: 400px;
    flex-direction: column;
}
.loading-spinner {
    font-size: 24px;
    color: #008B8B;
    margin-bottom: 20px;
}
""")
hv.extension('bokeh')


class SharedQueryManagers:
    """Singleton class to share query managers across tabs"""
    _instance = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._initialized = False
        return cls._instance
    
    def __init__(self):
        if not self._initialized:
            logger.info("Initializing shared query managers...")
            self.generation_manager = GenerationQueryManager()
            self._initialized = True
            logger.info("Shared query managers initialized")
    
    @property
    def hybrid_manager(self):
        # Lazy initialization for hybrid manager
        if not hasattr(self, '_hybrid_manager'):
            from ..shared.hybrid_query_manager import HybridQueryManager
            self._hybrid_manager = HybridQueryManager(cache_size_mb=100, cache_ttl=300)
        return self._hybrid_manager


class OptimizedEnergyDashboard(OriginalEnergyDashboard):
    """Optimized dashboard with lazy tab loading and shared managers"""
    
    def __init__(self, **params):
        """Initialize dashboard with minimal startup work"""
        start_time = time.time()
        
        # Initialize parent class but skip heavy operations
        super().__init__(**params)
        
        # Use shared query managers
        self.shared_managers = SharedQueryManagers()
        self.query_manager = self.shared_managers.generation_manager
        
        # Track which tabs have been initialized
        self._tab_initialized = {
            'nem_dash': False,
            'generation': False,
            'price_analysis': False,
            'station_analysis': False
        }
        
        # Store tab content for caching
        self._tab_content = {}
        
        # Loading indicator
        self.loading_indicator = pn.indicators.LoadingSpinner(
            value=False,
            size=100,
            name="Loading...",
            align='center'
        )
        
        init_time = time.time() - start_time
        logger.info(f"Optimized dashboard initialized in {init_time:.2f} seconds")
    
    def _create_loading_placeholder(self, tab_name):
        """Create a loading placeholder for a tab"""
        return pn.Column(
            pn.pane.HTML(
                f'<div class="loading-container">'
                f'<div class="loading-spinner">⌛</div>'
                f'<div>Loading {tab_name}...</div>'
                f'</div>'
            ),
            sizing_mode='stretch_width',
            height=400
        )
    
    def _load_tab_content(self, tab_name, tab_index):
        """Load tab content on-demand"""
        try:
            # Check if already loaded
            if self._tab_initialized.get(tab_name, False):
                return self._tab_content.get(tab_name)
            
            logger.info(f"Loading {tab_name} tab content...")
            start_time = time.time()
            
            # Create content based on tab
            if tab_name == 'nem_dash':
                content = create_nem_dash_tab_with_updates_optimized(
                    dashboard_instance=self, 
                    auto_update=True
                )
            elif tab_name == 'generation':
                content = self._create_generation_tab()
            elif tab_name == 'price_analysis':
                content = create_price_analysis_tab()
            elif tab_name == 'station_analysis':
                content = create_station_analysis_tab()
            else:
                content = pn.pane.Markdown(f"Unknown tab: {tab_name}")
            
            # Cache the content
            self._tab_content[tab_name] = content
            self._tab_initialized[tab_name] = True
            
            load_time = time.time() - start_time
            logger.info(f"{tab_name} tab loaded in {load_time:.2f} seconds")
            
            return content
            
        except Exception as e:
            logger.error(f"Error loading {tab_name} tab: {e}")
            return pn.pane.Markdown(f"**Error loading {tab_name}:** {e}")
    
    def create_dashboard(self):
        """Create dashboard with lazy tab loading"""
        try:
            start_time = time.time()
            logger.info("Creating optimized dashboard...")
            
            # Create header (minimal work)
            self.header_section = pn.pane.HTML(
                '''
                <div class="header-container">
                    <h1 style="color: white; text-align: center; margin: 10px 0;">
                        Australian Energy Market Dashboard
                    </h1>
                    <p style="color: white; text-align: center; margin: 5px 0; font-size: 14px;">
                        Real-time visualization of generation, prices, and market dynamics
                    </p>
                </div>
                ''',
                sizing_mode='stretch_width'
            )
            
            # Create tabs with placeholders
            self.tabs = pn.Tabs(
                ("Nem-dash", self._create_loading_placeholder("NEM Dashboard")),
                ("Generation by Fuel", self._create_loading_placeholder("Generation")),
                ("Average Price Analysis", self._create_loading_placeholder("Price Analysis")),
                ("Station Analysis", self._create_loading_placeholder("Station Analysis")),
                dynamic=True,
                closable=False,
                sizing_mode='stretch_width'
            )
            
            # Set up tab change handler
            self.tabs.param.watch(self._on_tab_change, 'active')
            
            # Complete dashboard layout
            dashboard = pn.Column(
                self.header_section,
                self.tabs,
                sizing_mode='stretch_width'
            )
            
            # Load only the first tab immediately with minimal content
            def load_initial_tab():
                try:
                    # Create a super minimal initial tab
                    initial_content = pn.Column(
                        pn.pane.HTML(
                            '<div style="text-align: center; padding: 50px;">'
                            '<h2>Welcome to AEMO Energy Dashboard</h2>'
                            '<p>Loading market data...</p>'
                            '</div>'
                        ),
                        sizing_mode='stretch_width'
                    )
                    self.tabs[0] = ("Nem-dash", initial_content)
                    
                    # Then load the real content in background
                    def load_real_content():
                        content = self._load_tab_content('nem_dash', 0)
                        self.tabs[0] = ("Nem-dash", content)
                    
                    # Small delay to let UI render first
                    threading.Timer(0.1, load_real_content).start()
                        
                except Exception as e:
                    logger.error(f"Error loading initial tab: {e}")
            
            # Don't wait for onload, do it immediately
            load_initial_tab()
            
            create_time = time.time() - start_time
            logger.info(f"Dashboard created in {create_time:.2f} seconds")
            
            return dashboard
            
        except Exception as e:
            logger.error(f"Error creating optimized dashboard: {e}")
            return pn.pane.HTML(f"<h1>Error creating dashboard: {str(e)}</h1>")
    
    def _on_tab_change(self, event):
        """Handle tab change events"""
        tab_index = event.new
        tab_names = ['nem_dash', 'generation', 'price_analysis', 'station_analysis']
        
        if 0 <= tab_index < len(tab_names):
            tab_name = tab_names[tab_index]
            
            # Check if tab needs loading
            if not self._tab_initialized.get(tab_name, False):
                # Load content in background to keep UI responsive
                def load_tab():
                    content = self._load_tab_content(tab_name, tab_index)
                    # Update tab content in main thread
                    if tab_index == 0:
                        self.tabs[0] = ("Nem-dash", content)
                    elif tab_index == 1:
                        self.tabs[1] = ("Generation by Fuel", content)
                    elif tab_index == 2:
                        self.tabs[2] = ("Average Price Analysis", content)
                    elif tab_index == 3:
                        self.tabs[3] = ("Station Analysis", content)
                
                # Run in thread to avoid blocking
                threading.Thread(target=load_tab, daemon=True).start()
    
    def load_generation_data(self, start_date, end_date):
        """Optimized generation data loading"""
        # Only load data when actually needed
        if not hasattr(self, '_generation_data_cache'):
            self._generation_data_cache = {}
        
        cache_key = f"{start_date}_{end_date}"
        if cache_key in self._generation_data_cache:
            return self._generation_data_cache[cache_key]
        
        # Load data
        result = super().load_generation_data(start_date, end_date)
        
        # Cache for 5 minutes
        self._generation_data_cache[cache_key] = result
        
        # Clean old cache entries
        if len(self._generation_data_cache) > 10:
            # Remove oldest entries
            keys = list(self._generation_data_cache.keys())
            for key in keys[:5]:
                del self._generation_data_cache[key]
        
        return result


def create_optimized_app():
    """Create the optimized Panel application"""
    def _create_dashboard():
        """Factory function to create a new dashboard instance per session"""
        try:
            # Create optimized dashboard instance
            dashboard = OptimizedEnergyDashboard()
            
            # Create the app
            app = dashboard.create_dashboard()
            
            # Start auto-update for this session
            def start_dashboard_updates():
                try:
                    dashboard.start_auto_update()
                except Exception as e:
                    logger.error(f"Error starting dashboard updates: {e}")
            
            # Hook into Panel's server startup
            pn.state.onload(start_dashboard_updates)
            
            return app
            
        except Exception as e:
            logger.error(f"Error creating optimized app: {e}")
            return pn.pane.HTML(f"<h1>Application Error: {str(e)}</h1>")
    
    return _create_dashboard


def main():
    """Run the optimized dashboard"""
    logger.info("Starting optimized AEMO Dashboard server...")
    
    # Create and serve the app
    app = create_optimized_app()
    
    # Configure server
    pn.serve(
        app,
        port=5006,
        allow_websocket_origin=["*"],
        show=False,
        title="AEMO Energy Dashboard (Optimized)",
        autoreload=False
    )


if __name__ == "__main__":
    # Check for command line arguments
    if len(sys.argv) > 1:
        if sys.argv[1] == '--help':
            print("Optimized AEMO Energy Dashboard")
            print("Usage: python gen_dash_optimized.py")
            sys.exit(0)
    
    main()