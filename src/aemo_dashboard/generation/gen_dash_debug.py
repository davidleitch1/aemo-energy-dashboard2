#!/usr/bin/env python3
"""
Debug version of gen_dash.py with extensive logging for refresh issues
"""
import logging
import time
import sys
import os

# Set up detailed logging before any imports
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s.%(msecs)03d - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

# Create specific loggers
dash_logger = logging.getLogger('dashboard')
ws_logger = logging.getLogger('websocket')
component_logger = logging.getLogger('components')
refresh_logger = logging.getLogger('refresh')

# Set all to DEBUG
for logger in [dash_logger, ws_logger, component_logger, refresh_logger]:
    logger.setLevel(logging.DEBUG)

# Log imports
dash_logger.info("="*60)
dash_logger.info("DASHBOARD DEBUG MODE - Starting imports")
dash_logger.info("="*60)

# Add timing for imports
import_start = time.time()

# Copy imports from original gen_dash.py
from pathlib import Path
import pandas as pd
import panel as pn
import numpy as np
from datetime import datetime, timedelta
import pickle
import warnings
import asyncio
import threading
from collections import defaultdict

# Import dashboard components
from ..shared.config import config
from ..shared.logging_config import get_logger
from ..shared.file_access import DirectFileAccess
from ..shared.resolution_manager import ResolutionManager
from ..shared.adapter_selector import get_generation_adapter, get_price_adapter, get_transmission_adapter, get_rooftop_adapter
from ..shared.hybrid_query_manager import HybridQueryManager
from ..nem_dash.nem_dash_tab import create_nem_dash_tab
from ..analysis.price_analysis_ui import create_price_analysis_tab
from ..station.station_analysis_ui import create_station_analysis_ui
from .generation_query_manager import GenerationQueryManager

import_time = time.time() - import_start
dash_logger.info(f"Imports completed in {import_time:.2f}s")

# Original logger
logger = get_logger(__name__)

# Wrap Panel extension to log WebSocket events
original_extension = pn.extension

def logged_extension(*args, **kwargs):
    """Wrapped Panel extension with logging"""
    ws_logger.info(f"Panel extension called with args={args}, kwargs={kwargs}")
    result = original_extension(*args, **kwargs)
    
    # Add WebSocket lifecycle logging
    if hasattr(pn.state, 'on_session_created'):
        def log_session_created(session_context):
            ws_logger.info(f"WebSocket session created: {session_context.id}")
            refresh_logger.info("NEW SESSION STARTED")
        
        pn.state.on_session_created(log_session_created)
    
    if hasattr(pn.state, 'on_session_destroyed'):
        def log_session_destroyed(session_context):
            ws_logger.info(f"WebSocket session destroyed: {session_context.id}")
            refresh_logger.info("SESSION DESTROYED")
        
        pn.state.on_session_destroyed(log_session_destroyed)
    
    return result

pn.extension = logged_extension

# Copy the EnergyDashboard class with added logging
class EnergyDashboard:
    """Energy dashboard with comprehensive logging for debugging"""
    
    def __init__(self):
        """Initialize the dashboard with logging"""
        dash_logger.info("EnergyDashboard.__init__ started")
        self.start_time = time.time()
        
        # Basic setup
        self.file_access = DirectFileAccess()
        self.resolution_manager = ResolutionManager(self.file_access)
        self.query_manager = HybridQueryManager()
        self.generation_query_manager = GenerationQueryManager()
        
        # Initialize attributes
        self.duid_fuel = None
        self.fuel_colors = {
            'Coal': '#4A4A4A',
            'CCGT': '#FF6B6B',
            'OCGT': '#FFA500',
            'Gas other': '#FFD700',
            'Water': '#4E79A7',
            'Wind': '#59A14F',
            'Solar': '#EDC948',
            'Rooftop Solar': '#F28E2B',
            'Biomass': '#E15759',
            'Battery Storage': '#AF7AA1',
            'Other': '#C0C0C0'
        }
        
        # Load DUID mapping
        self.load_duid_mapping()
        
        # Initialize data with logging
        dash_logger.info("Initializing data...")
        self.initialize_data()
        
        dash_logger.info(f"EnergyDashboard.__init__ completed in {time.time() - self.start_time:.2f}s")
    
    def load_duid_mapping(self):
        """Load DUID mapping with logging"""
        component_logger.info("Loading DUID mapping...")
        gen_info_path = Path(os.environ.get('GEN_INFO_FILE', 
                                           '/Users/davidleitch/Library/Mobile Documents/com~apple~CloudDocs/snakeplay/AEMO_spot/gen_info.pkl'))
        try:
            with open(gen_info_path, 'rb') as f:
                gen_info = pickle.load(f)
            self.duid_fuel = gen_info[['DUID', 'FUEL']].copy()
            component_logger.info(f"Loaded {len(self.duid_fuel)} DUID mappings")
        except Exception as e:
            component_logger.error(f"Failed to load DUID mapping: {e}")
            self.duid_fuel = pd.DataFrame(columns=['DUID', 'FUEL'])
    
    def initialize_data(self):
        """Initialize data with default 2-day range"""
        component_logger.info("Initialize data called")
        self.end_date = pd.Timestamp.now()
        self.start_date = self.end_date - pd.Timedelta(days=2)
        self.selected_region = "NEM"
        self.load_data()
    
    def load_data(self):
        """Load data with logging"""
        load_start = time.time()
        component_logger.info(f"Loading data for {self.start_date} to {self.end_date}")
        
        # Load generation data
        self.load_generation_data(self.start_date, self.end_date, self.selected_region)
        
        component_logger.info(f"Data loaded in {time.time() - load_start:.2f}s")
    
    def load_generation_data(self, start_date, end_date, region):
        """Load generation data with logging"""
        component_logger.info(f"Loading generation data: {region}, {start_date} to {end_date}")
        
        # For short ranges, use raw DUID data
        days_diff = (end_date - start_date).total_seconds() / 86400
        if days_diff <= 30:
            component_logger.info(f"Using raw DUID data for {days_diff:.1f} day range")
            generation_adapter = get_generation_adapter()
            gen_data = generation_adapter.load_generation_data(
                start_date=start_date,
                end_date=end_date,
                resolution='auto'
            )
            
            if gen_data.empty:
                component_logger.warning("No generation data loaded")
                self.generation_data = pd.DataFrame()
                return
            
            # Process data
            self.generation_data = self.process_generation_data(gen_data)
        else:
            component_logger.info(f"Using query manager for {days_diff:.1f} day range")
            # Use query manager for longer ranges
            # (implementation details omitted for brevity)
    
    def process_generation_data(self, gen_data):
        """Process generation data"""
        component_logger.info(f"Processing {len(gen_data)} generation records")
        # (processing implementation)
        return gen_data
    
    def create_dashboard(self):
        """Create the dashboard with extensive logging"""
        dash_logger.info("Creating dashboard UI...")
        create_start = time.time()
        
        # Log Panel state
        ws_logger.info(f"Panel state before dashboard creation: curdoc={hasattr(pn.state, 'curdoc')}")
        
        # Create tabs with logging
        component_logger.info("Creating Today tab...")
        today_tab = self.create_today_tab()
        
        component_logger.info("Creating Generation tab...")
        generation_tab = pn.Column(name="Generation")
        
        component_logger.info("Creating Price Analysis tab...")
        price_analysis_tab = pn.Column(name="Price Analysis")
        
        component_logger.info("Creating Station Analysis tab...")
        station_analysis_tab = pn.Column(name="Station Analysis")
        
        # Create tabs
        tabs = pn.Tabs(
            ('Today', today_tab),
            ('Generation', generation_tab),
            ('Price Analysis', price_analysis_tab),
            ('Station Analysis', station_analysis_tab),
            dynamic=True
        )
        
        # Create template
        template = pn.template.MaterialTemplate(
            title="Energy Dashboard (DEBUG MODE)",
            header_background='#00796B',
        )
        
        template.main.append(tabs)
        
        dash_logger.info(f"Dashboard created in {time.time() - create_start:.2f}s")
        
        return template
    
    def create_today_tab(self):
        """Create Today tab with component logging"""
        component_logger.info("Creating Today tab components...")
        
        # Create components with individual logging
        component_logger.info("Creating price component...")
        price_component = self.create_price_component()
        
        component_logger.info("Creating renewable gauge...")
        renewable_gauge = self.create_renewable_gauge()
        
        component_logger.info("Creating generation overview...")
        generation_overview = self.create_generation_overview()
        
        # Layout
        today_layout = pn.Row(
            pn.Column(price_component),
            pn.Column(renewable_gauge, generation_overview)
        )
        
        component_logger.info("Today tab created successfully")
        return today_layout
    
    def create_price_component(self):
        """Create price component with logging"""
        refresh_logger.debug("create_price_component called")
        return pn.pane.Markdown("# Price Component\n(Placeholder for debugging)")
    
    def create_renewable_gauge(self):
        """Create renewable gauge with logging"""
        refresh_logger.debug("create_renewable_gauge called")
        return pn.pane.Markdown("# Renewable Gauge\n(Placeholder for debugging)")
    
    def create_generation_overview(self):
        """Create generation overview with logging"""
        refresh_logger.debug("create_generation_overview called")
        return pn.pane.Markdown("# Generation Overview\n(Placeholder for debugging)")


def main():
    """Main entry point with logging"""
    dash_logger.info("="*60)
    dash_logger.info("STARTING ENERGY DASHBOARD (DEBUG MODE)")
    dash_logger.info("="*60)
    
    # Configure Panel with logging
    ws_logger.info("Configuring Panel extension...")
    pn.extension('tabulator', 'plotly', template='material')
    
    # Log environment
    dash_logger.info(f"Python version: {sys.version}")
    dash_logger.info(f"Panel version: {pn.__version__}")
    dash_logger.info(f"Working directory: {os.getcwd()}")
    
    # Create dashboard
    dash_logger.info("Creating dashboard instance...")
    dashboard = EnergyDashboard()
    
    # Create UI
    dash_logger.info("Creating dashboard UI...")
    app = dashboard.create_dashboard()
    
    # Serve with logging
    port = 5013
    dash_logger.info(f"Starting server on port {port}...")
    dash_logger.info("="*60)
    dash_logger.info("IMPORTANT: Test Safari refresh and watch the logs")
    dash_logger.info("Look for:")
    dash_logger.info("  - SESSION DESTROYED messages")
    dash_logger.info("  - NEW SESSION STARTED messages")
    dash_logger.info("  - Any errors or hanging between these")
    dash_logger.info("="*60)
    
    app.show(port=port)


if __name__ == "__main__":
    main()