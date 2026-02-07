#!/usr/bin/env python3
"""
Real-time Energy Generation Dashboard with HvPlot
Shows interactive generation by fuel type with scrollable time window.
Supports both server and Pyodide (serverless) deployment.
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
import time
from bokeh.models import DatetimeTickFormatter
from dotenv import load_dotenv
import plotly.graph_objects as go
from plotly.subplots import make_subplots

from ..shared.config import config
from ..shared.logging_config import setup_logging, get_logger
from ..shared.email_alerts import EmailAlertManager
from ..analysis.price_analysis_ui import create_price_analysis_tab
from ..station.station_analysis_ui import create_station_analysis_tab
from ..nem_dash.nem_dash_tab import create_nem_dash_tab_with_updates
from ..curtailment import create_curtailment_tab
from .generation_query_manager import GenerationQueryManager
from ..shared.flexoki_theme import (
    FLEXOKI_PAPER,
    FLEXOKI_BLACK,
    FLEXOKI_BASE,
    FLEXOKI_ACCENT,
)

# Set up logging
setup_logging()
logger = get_logger(__name__)

# Configure Panel and HoloViews BEFORE extension loading
pn.config.theme = 'default'  # Use default (light) theme for Flexoki
pn.extension('tabulator', 'plotly', template='material')

# =============================================================================
# Cache Configuration
# =============================================================================
ENABLE_PN_CACHE = os.getenv('ENABLE_PN_CACHE', 'true').lower() == 'true'
logger.info(f"Panel caching: {'enabled' if ENABLE_PN_CACHE else 'disabled'}")

# Cache statistics
_cache_stats = {'hits': 0, 'misses': 0, 'errors': 0}

# =============================================================================
# Cached Plot Creation Functions
# =============================================================================


def _flexoki_background_hook(plot, element):
    """Standalone hook function to set Flexoki backgrounds for cached plots.

    Sets both plot area and legend backgrounds to FLEXOKI_PAPER (#FFFCF0).
    """
    try:
        p = plot.state
        # Set the main plot area background
        p.background_fill_color = FLEXOKI_PAPER
        # Set the border/outer area (where legend sits) - THIS IS THE KEY FIX
        p.border_fill_color = FLEXOKI_PAPER
        # Set outline color
        p.outline_line_color = FLEXOKI_BASE[150]
        if hasattr(p, 'legend') and p.legend:
            for legend in p.legend:
                legend.background_fill_color = FLEXOKI_PAPER
                legend.border_line_color = FLEXOKI_BASE[150]
                legend.border_line_alpha = 1.0
    except Exception as e:
        logger.debug(f"Could not set Flexoki backgrounds: {e}")


@pn.cache(max_items=20, policy='LRU', ttl=300, to_disk=False)
def create_generation_plot_cached(
    plot_data_json: str,
    fuel_types_str: str,
    fuel_colors_json: str,
    region: str,
    time_range: str,
    width: int,
    height: int
):
    """
    Cached generation plot creation.
    This is the expensive operation that takes 14+ seconds.
    
    Note: We serialize DataFrames to JSON to make them hashable for caching.
    """
    global _cache_stats
    _cache_stats['misses'] += 1
    
    start_time = time.time()
    logger.info(f"Creating new plot for {region} - {time_range} (cache miss)")
    
    # Deserialize inputs
    plot_data = pd.read_json(plot_data_json)
    plot_data['settlementdate'] = pd.to_datetime(plot_data['settlementdate'])
    fuel_types = json.loads(fuel_types_str)
    fuel_colors = json.loads(fuel_colors_json)
    
    # Create the plot (expensive operation)
    area_plot = plot_data.hvplot.area(
        x='settlementdate',
        y=fuel_types,
        stacked=True,
        width=width,
        height=height,
        ylabel='Generation (MW)',
        xlabel='',
        grid=True,
        legend='right',
        bgcolor=FLEXOKI_PAPER,
        color=[fuel_colors.get(fuel, '#6272a4') for fuel in fuel_types],
        alpha=0.8,
        hover=True,
        hover_tooltips=[('Fuel Type', '$name')],
        title=f'Generation by Fuel Type - {region} ({time_range}) | data:AEMO, design ITK'
    )
    
    area_plot = area_plot.opts(
        show_grid=False,
        bgcolor=FLEXOKI_PAPER,
        xaxis='bottom',  # Show x-axis for linking
        xlabel='Time',
        hooks=[_flexoki_background_hook]
    )

    creation_time = time.time() - start_time
    logger.info(f"Plot creation took {creation_time:.2f}s")
    
    return area_plot

# Custom CSS to ensure x-axis labels are visible and style header
pn.config.raw_css.append("""
/* Global page background - Flexoki Paper */
body, html {
    background-color: #FFFCF0 !important;
}

/* Panel main container background */
.bk-root, .bk, .pn-main, .main, #app {
    background-color: #FFFCF0 !important;
}

/* Bokeh plot backgrounds - force cream color */
.bk-canvas-wrapper, .bk-canvas-overlays {
    background-color: #FFFCF0 !important;
}

/* Bokeh plot wrapper and figure backgrounds */
.bk-Figure, .bk-plot-wrapper, .bk-plot-layout {
    background-color: #FFFCF0 !important;
}

/* Bokeh legend backgrounds */
.bk-Legend {
    background-color: #FFFCF0 !important;
    border-color: #B7B5AC !important;
}

/* Axis labels styling */
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

/* Panel Select widget light theme styling */
.bk-input select,
select.bk-input {
    background-color: #FFFCF0 !important;
    color: #100F0F !important;
    border: 1px solid #6F6E69 !important;
}

.bk-input select option,
select.bk-input option {
    background-color: #FFFCF0 !important;
    color: #100F0F !important;
}

.bk-input select option:checked,
select.bk-input option:checked {
    background-color: #E6E4D9 !important;
    color: #100F0F !important;
}

/* Bokeh legend background - Flexoki Paper */
.bk-legend {
    background-color: #FFFCF0 !important;
    border-color: #B7B5AC !important;
}

/* Bokeh plot area background */
.bk-plot-wrapper {
    background-color: #FFFCF0 !important;
}
""")
hv.extension('bokeh')

# Logging is set up in imports

# Configure Flexoki Light theme colors
FLEXOKI_COLORS = {
    'bg': FLEXOKI_PAPER,           # Background (#FFFCF0)
    'current': FLEXOKI_BASE[100],  # Current Line (#E6E4D9)
    'fg': FLEXOKI_BLACK,           # Foreground (#100F0F)
    'comment': FLEXOKI_BASE[600],  # Comment (#6F6E69)
    'cyan': FLEXOKI_ACCENT['cyan'],
    'green': FLEXOKI_ACCENT['green'],
    'orange': FLEXOKI_ACCENT['orange'],
    'pink': FLEXOKI_ACCENT['magenta'],
    'purple': FLEXOKI_ACCENT['purple'],
    'red': FLEXOKI_ACCENT['red'],
    'yellow': FLEXOKI_ACCENT['yellow']
}
# Alias for backwards compatibility
DRACULA_COLORS = FLEXOKI_COLORS

def _text_background_hook(plot, element):
    """Hook to set background colors for Text/placeholder plots."""
    try:
        p = plot.state
        p.background_fill_color = FLEXOKI_PAPER
        p.border_fill_color = FLEXOKI_PAPER
        p.outline_line_color = FLEXOKI_BASE[150]
    except Exception:
        pass

def create_themed_placeholder(text, width=None, height=None):
    """Create a placeholder text element with Flexoki theme backgrounds."""
    opts = dict(
        xlim=(0, 1), ylim=(0, 1),
        bgcolor=FLEXOKI_PAPER,
        color=FLEXOKI_BLACK,
        fontsize=14,
        hooks=[_text_background_hook]
    )
    if width:
        opts['width'] = width
    if height:
        opts['height'] = height
    return hv.Text(0.5, 0.5, text).opts(**opts)

hv.opts.defaults(
    hv.opts.Area(
        width=1200,  # Use larger fixed width
        height=500,
        alpha=0.8,
        show_grid=True,
        gridstyle={'grid_line_color': FLEXOKI_BASE[100], 'grid_line_alpha': 0.3},
        bgcolor=FLEXOKI_PAPER,
        toolbar='above'
    ),
    hv.opts.Bars(
        bgcolor=FLEXOKI_PAPER,
        show_grid=True,
        gridstyle={'grid_line_color': FLEXOKI_BASE[100], 'grid_line_alpha': 0.3},
        toolbar='above'
    ),
    hv.opts.Curve(
        bgcolor=FLEXOKI_PAPER,
        show_grid=True,
        gridstyle={'grid_line_color': FLEXOKI_BASE[100], 'grid_line_alpha': 0.3},
        toolbar='above'
    ),
    hv.opts.Overlay(
        bgcolor=FLEXOKI_PAPER,
        show_grid=True,
        gridstyle={'grid_line_color': FLEXOKI_BASE[100], 'grid_line_alpha': 0.3},
        toolbar='above'
    ),
    hv.opts.Text(
        bgcolor=FLEXOKI_PAPER,
        color=FLEXOKI_BLACK
    )
)

# File paths from shared config
GEN_INFO_FILE = config.gen_info_file
GEN_OUTPUT_FILE = config.gen_output_file

# Add this function anywhere in your gen_dash.py file
def create_sample_env_file():
    """Create a sample .env file with all available options"""
    
    sample_content = """# Energy Dashboard Configuration
# Copy this file to .env and fill in your values
# DO NOT COMMIT .env TO GIT - ADD TO .gitignore

# ===== EMAIL ALERT CONFIGURATION =====
# iCloud Mail settings
ALERT_EMAIL=your-email@icloud.com
ALERT_PASSWORD=your-app-specific-password
RECIPIENT_EMAIL=your-email@icloud.com

# Email server settings (iCloud defaults)
SMTP_SERVER=smtp.mail.me.com
SMTP_PORT=587

# Gmail alternative (uncomment if using Gmail)
# SMTP_SERVER=smtp.gmail.com
# SMTP_PORT=587

# ===== ALERT BEHAVIOR =====
ENABLE_EMAIL_ALERTS=true
ALERT_COOLDOWN_HOURS=24
AUTO_ADD_TO_EXCEPTIONS=true

# ===== DASHBOARD SETTINGS =====
DEFAULT_REGION=NEM
UPDATE_INTERVAL_MINUTES=4.5

# ===== FILE PATHS (optional overrides) =====
# BASE_PATH=/Users/davidleitch/Library/Mobile Documents/com~apple~CloudDocs/snakeplay/AEMO_spot
# GEN_INFO_FILE=/custom/path/to/gen_info.pkl
# GEN_OUTPUT_FILE=/custom/path/to/gen_output.parquet

# ===== LOGGING =====
LOG_LEVEL=INFO
LOG_FILE=dashboard.log
"""
    
    env_sample_file = Path('.env.sample')
    with open(env_sample_file, 'w') as f:
        f.write(sample_content)
    
    print(f"✅ Created sample configuration file: {env_sample_file}")
    print("📝 Next steps:")
    print("   1. Copy .env.sample to .env")
    print("   2. Edit .env with your iCloud email settings")
    print("   3. Get an iCloud App-Specific Password from appleid.apple.com")

    from bokeh.models import DatetimeTickFormatter

def apply_datetime_formatter(plot, element):
    """
    A Bokeh hook that forces the X axis to stay as real datetimes.
    """
    plot.handles['xaxis'].formatter = DatetimeTickFormatter(
        hours="%H:%M", days="%H:%M", months="%b %d", years="%Y"
    )

from bokeh.models import PrintfTickFormatter

def apply_numeric_yaxis_formatter(plot, element):
    """
    Forces the LEFT (generation) axis to use integer formatting.
    """
    plot.handles['yaxis'].formatter = PrintfTickFormatter(format="%.0f")

class EnergyDashboard(param.Parameterized):
    """
    Real-time energy generation dashboard with HvPlot
    """
    
    # Parameters for user controls
    region = param.Selector(
        default='NEM',
        objects=['NEM', 'NSW1', 'QLD1', 'SA1', 'TAS1', 'VIC1'],
        doc="Select region to display"
    )
    
    time_range = param.Selector(
        default='1',
        objects=['1', '7', '30', '90', '365', 'All'],
        doc="Select time range to display"
    )
    
    start_date = param.Date(
        default=datetime.now().date() - timedelta(days=1),
        doc="Start date for custom range"
    )
    
    end_date = param.Date(
        default=datetime.now().date(),
        doc="End date for custom range"
    )
    
    
    def __init__(self, **params):
        super().__init__(**params)
        self.gen_info_df = None
        self.gen_output_df = None
        self.transmission_df = None  # Add transmission data
        self.rooftop_df = None  # Add rooftop solar data
        self.duid_to_fuel = {}
        self.duid_to_region = {}
        self.last_update = None
        self.update_task = None
        # Hours will be determined dynamically based on time_range selection
        self._plot_objects = {}  # Cache for plot objects
        
        # Initialize query manager for optimized data loading
        self.query_manager = GenerationQueryManager()
        self._using_aggregated_data = False  # Flag to track if using pre-aggregated data
        
        # Initialize email alert manager
        self.email_manager = EmailAlertManager()
        
        # Load initial data
        self.load_reference_data()
        
        # Create the initial plot panes with proper initialization
        self.plot_pane = None
        self.utilization_pane = None
        self.transmission_pane = None
        self.generation_tod_pane = None
        self.summary_table_pane = None  # Add summary table pane
        self.main_content = None
        self.header_section = None
        # Track unknown DUIDs for session reporting
        self.session_unknown_duids = set()
        # Initialize panes
        self._initialize_panes()
        
    def _initialize_panes(self):
        """Initialize plot panes with proper document handling"""
        try:
            # Create fresh plots for initialization
            gen_plot = self.create_plot()  # Now returns pn.Column with Plotly chart + stats table
            util_plot = self.create_utilization_plot()
            transmission_plot = self.create_transmission_plot()

            # Plotly generation chart (create_plot returns pn.Column)
            self.plot_pane = pn.Column(
                gen_plot,
                sizing_mode='stretch_width',
                margin=(5, 5)
            )

            self.utilization_pane = pn.pane.HoloViews(
                util_plot,
                sizing_mode='stretch_width',
                height=500,
                margin=(5, 5),
                linked_axes=False  # Prevent UFuncTypeError when switching tabs
            )

            self.transmission_pane = pn.pane.HoloViews(
                transmission_plot,
                sizing_mode='stretch_width',
                height=400,
                margin=(5, 5),
                linked_axes=False  # Prevent UFuncTypeError when switching tabs
            )

            # Create generation TOD plot
            tod_plot = self.create_generation_tod_plot()
            self.generation_tod_pane = pn.pane.HoloViews(
                tod_plot,
                sizing_mode='stretch_width',
                height=700,
                margin=(5, 5),
                linked_axes=False
            )

            # Summary table is now included in plot_pane via create_plot()
            # Keep summary_table_pane for backward compatibility but it won't be displayed
            summary_table = self.create_generation_summary_table()
            self.summary_table_pane = pn.Column(
                summary_table,
                sizing_mode='stretch_width',
                margin=(10, 5)
            )

            # Set initial visibility
            self.plot_pane.visible = True
            self.utilization_pane.visible = True
            self.transmission_pane.visible = True
            self.generation_tod_pane.visible = True

        except Exception as e:
            logger.error(f"Error initializing panes: {e}")
            # Create fallback empty panes
            self.plot_pane = pn.pane.HTML("Loading generation chart...", height=600)
            self.utilization_pane = pn.pane.HTML("Loading utilization chart...", height=500)
            self.transmission_pane = pn.pane.HTML("Loading transmission chart...", height=400)
            self.generation_tod_pane = pn.pane.HTML("Loading time of day chart...", height=700)
        
    def load_reference_data(self):
        """Load DUID to fuel/region mapping from gen_info.pkl"""
        try:
            if os.path.exists(GEN_INFO_FILE):
                with open(GEN_INFO_FILE, 'rb') as f:
                    self.gen_info_df = pickle.load(f)
                
                # Create mapping dictionaries
                self.duid_to_fuel = dict(zip(self.gen_info_df['DUID'], self.gen_info_df['Fuel']))
                self.duid_to_region = dict(zip(self.gen_info_df['DUID'], self.gen_info_df['Region']))
                
                logger.info(f"Loaded {len(self.gen_info_df)} DUID mappings")
                logger.info(f"Fuel types: {self.gen_info_df['Fuel'].unique()}")
                
            else:
                logger.error(f"gen_info.pkl not found at {GEN_INFO_FILE}")
                
        except Exception as e:
            logger.error(f"Error loading gen_info.pkl: {e}")
    
    def load_duid_exception_list(self):
        """Load the list of DUIDs to ignore for email alerts"""
        exception_file = config.data_dir / "duid_exceptions.json"
        try:
            if exception_file.exists():
                with open(exception_file, 'r') as f:
                    data = json.load(f)
                    # Return as a set for fast lookup
                    return set(data.get('exception_duids', []))
            else:
                return set()
        except Exception as e:
            logger.error(f"Error loading DUID exception list: {e}")
            return set()
    
    def save_duid_exception_list(self, exception_duids):
        """Save the list of DUIDs to ignore for email alerts"""
        exception_file = config.data_dir / "duid_exceptions.json"
        try:
            # Convert set to list for JSON serialization
            data = {
                'exception_duids': sorted(list(exception_duids)),
                'last_updated': datetime.now().isoformat(),
                'note': 'DUIDs in this list will not trigger email alerts'
            }
            with open(exception_file, 'w') as f:
                json.dump(data, f, indent=2)
            logger.info(f"Saved {len(exception_duids)} DUIDs to exception list")
        except Exception as e:
            logger.error(f"Error saving DUID exception list: {e}")
    
    def add_duids_to_exception_list(self, duids_to_add):
        """Add DUIDs to the exception list"""
        current_exceptions = self.load_duid_exception_list()
        current_exceptions.update(duids_to_add)
        self.save_duid_exception_list(current_exceptions)
        logger.info(f"Added {len(duids_to_add)} DUIDs to exception list")
    
    def handle_unknown_duids(self, unknown_duids, df):
        """Handle unknown DUIDs - log and potentially send alerts"""
        
        # Load exception list
        exception_duids = self.load_duid_exception_list()
        
        # Filter out DUIDs that are in the exception list
        new_unknown_duids = unknown_duids - exception_duids
        known_exception_duids = unknown_duids & exception_duids
        
        # Always log the issue
        logger.warning(f"🚨 Found {len(unknown_duids)} unknown DUIDs not in gen_info.pkl:")
        logger.warning(f"   - {len(new_unknown_duids)} are NEW (will trigger email if enabled)")
        logger.warning(f"   - {len(known_exception_duids)} are in exception list (no email)")
        
        # PERFORMANCE FIX: Get latest records for all unknown DUIDs in single operation
        if unknown_duids:
            # Filter to only unknown DUIDs and get most recent record for each
            unknown_df = df[df['duid'].isin(unknown_duids)]
            if not unknown_df.empty:
                # Get latest record for each DUID efficiently
                latest_records = unknown_df.groupby('duid').tail(1).set_index('duid')
                
                for duid in sorted(unknown_duids):
                    if duid in latest_records.index:
                        sample = latest_records.loc[duid]
                        exception_flag = " [EXCEPTION LIST]" if duid in exception_duids else " [NEW]"
                        logger.warning(f"  - {duid}: {sample['scadavalue']:.1f} MW at {sample['settlementdate']}{exception_flag}")
                    else:
                        logger.warning(f"  - {duid}: No data found")
            else:
                for duid in sorted(unknown_duids):
                    logger.warning(f"  - {duid}: No data found")
        
        # Only send email for new unknown DUIDs not in exception list
        if new_unknown_duids:
            # Check if email alerts are enabled
            if os.getenv('ENABLE_EMAIL_ALERTS', 'true').lower() == 'true':
                if self.should_send_email_alert(new_unknown_duids):
                    self.send_unknown_duid_email(new_unknown_duids, df)
                    
                    # After sending email, optionally add these to exception list
                    # to prevent repeated emails about the same DUIDs
                    if os.getenv('AUTO_ADD_TO_EXCEPTIONS', 'true').lower() == 'true':
                        self.add_duids_to_exception_list(new_unknown_duids)
                        logger.info("Auto-added alerted DUIDs to exception list")
            else:
                logger.info(f"Email alerts disabled - would have alerted about {len(new_unknown_duids)} new DUIDs")

    def should_send_email_alert(self, unknown_duids):
        """Check if we should send an email alert (rate limiting)"""
        # Load cache of previously alerted DUIDs
        cache_file = config.data_dir / "unknown_duids_alerts.json"
        alert_cache = {}
        
        try:
            if cache_file.exists():
                with open(cache_file, 'r') as f:
                    alert_cache = json.load(f)
        except Exception as e:
            logger.error(f"Error loading alert cache: {e}")
        
        # Check if any DUID needs alerting (hasn't been alerted in last 24 hours)
        now = datetime.now()
        duids_needing_alert = []
        
        for duid in unknown_duids:
            if duid not in alert_cache:
                duids_needing_alert.append(duid)
            else:
                last_alert = datetime.fromisoformat(alert_cache[duid])
                if (now - last_alert).total_seconds() > 24 * 3600:  # 24 hours
                    duids_needing_alert.append(duid)
        
        if duids_needing_alert:
            # Update cache for DUIDs we're about to alert
            for duid in duids_needing_alert:
                alert_cache[duid] = now.isoformat()
            
            # Save updated cache
            try:
                with open(cache_file, 'w') as f:
                    json.dump(alert_cache, f, indent=2, default=str)
            except Exception as e:
                logger.error(f"Error saving alert cache: {e}")
            
            return True
        
        return False

    def send_unknown_duid_email(self, unknown_duids, df):
        """Send email alert about unknown DUIDs"""
        try:
            # Email configuration - use environment variables
            sender_email = os.getenv('ALERT_EMAIL')
            sender_password = os.getenv('ALERT_PASSWORD') 
            recipient_email = os.getenv('RECIPIENT_EMAIL', sender_email)
            smtp_server = os.getenv('SMTP_SERVER', 'smtp.mail.me.com')  # Default to iCloud
            smtp_port = int(os.getenv('SMTP_PORT', '587'))
            
            if not all([sender_email, sender_password]):
                logger.error("Email credentials not configured. Set ALERT_EMAIL and ALERT_PASSWORD environment variables.")
                return
            
            # Create email
            msg = MIMEMultipart()
            msg['From'] = sender_email
            msg['To'] = recipient_email
            msg['Subject'] = f"⚠️ Unknown DUIDs in Energy Dashboard - {datetime.now().strftime('%Y-%m-%d %H:%M')}"
            
            # Create email body
            body = self.create_alert_email_body(unknown_duids, df)
            msg.attach(MIMEText(body, 'html'))
            
            # Send email using configured SMTP server
            with smtplib.SMTP(smtp_server, smtp_port) as server:
                server.starttls()
                server.login(sender_email, sender_password)
                server.send_message(msg)
            
            logger.info(f"✅ Email alert sent for {len(unknown_duids)} unknown DUIDs via {smtp_server}")
            
        except Exception as e:
            logger.error(f"❌ Failed to send email alert: {e}")

    def create_alert_email_body(self, unknown_duids, df):
        """Create HTML email body"""
        # Get sample data for each unknown DUID
        duid_samples = []
        for duid in sorted(unknown_duids)[:10]:  # Limit to 10 for email size
            duid_data = df[df['duid'] == duid]
            if not duid_data.empty:
                sample = duid_data.iloc[-1]
                duid_samples.append({
                    'duid': duid,
                    'power': sample['scadavalue'],
                    'time': sample['settlementdate'],
                    'records': len(duid_data)
                })
        
        # Build HTML
        samples_html = ""
        for sample in duid_samples:
            samples_html += f"""
            <tr>
                <td>{sample['duid']}</td>
                <td>{sample['power']:.1f} MW</td>
                <td>{sample['time']}</td>
                <td>{sample['records']}</td>
            </tr>
            """
        
        body = f"""
        <html>
        <body>
            <h2>🚨 Unknown DUIDs Detected</h2>
            <p><strong>Time:</strong> {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
            <p>Found <strong>{len(unknown_duids)} unknown DUID(s)</strong> not in gen_info.pkl</p>
            
            <h3>Sample Data:</h3>
            <table border="1" style="border-collapse: collapse;">
            <tr>
                <th>DUID</th>
                <th>Latest Power</th>
                <th>Latest Time</th>
                <th>Records (24h)</th>
            </tr>
            {samples_html}
            </table>
            
            <h3>Action Required:</h3>
            <ul>
                <li>Update gen_info.pkl with new DUID information</li>
                <li>Check AEMO data sources for DUID details</li>
                <li>Verify these are legitimate new generation units</li>
            </ul>
            
            <p><em>All unknown DUIDs: {', '.join(sorted(unknown_duids))}</em></p>
        </body>
        </html>
        """
        return body
    
    def load_generation_data(self):
        """Load generation data using optimized query manager for long date ranges"""
        try:
            # Calculate time window based on selected time range
            start_time, end_time = self._get_effective_date_range()
            
            # Calculate days span to determine loading strategy
            days_span = (end_time - start_time).total_seconds() / (24 * 3600) if start_time and end_time else 0
            
            # For long date ranges, use pre-aggregated data from query manager
            if days_span > 30:  # Use aggregated data for ranges > 30 days
                logger.info(f"Using pre-aggregated data for {days_span:.0f} day range")
                
                # Query aggregated data by fuel type
                df = self.query_manager.query_generation_by_fuel(
                    start_date=start_time,
                    end_date=end_time,
                    region='NEM'  # Load all regions, filter later
                )
                
                if df.empty:
                    logger.warning("No generation data returned from query manager")
                    self.gen_output_df = pd.DataFrame()
                    return
                
                # Create synthetic structure to match existing code expectations
                # Add synthetic DUID column (fuel_type + _AGG)
                df['duid'] = df['fuel_type'] + '_AGG'
                df['scadavalue'] = df['total_generation_mw']
                df['fuel'] = df['fuel_type']
                
                # For NEM-wide data, set region to 'NEM' (will be filtered later in process_data_for_region)
                df['region'] = 'NEM'
                
                # Set flag to indicate we're using aggregated data
                self._using_aggregated_data = True
                
                logger.info(f"Loaded {len(df):,} pre-aggregated records by fuel type")
                
            else:
                # For short date ranges, use existing raw data approach
                logger.info(f"Using raw DUID data for {days_span:.0f} day range")
                from ..shared.adapter_selector import load_generation_data
                
                df = load_generation_data(
                    start_date=start_time,
                    end_date=end_time,
                    resolution='auto'
                )
                
                if df.empty:
                    logger.warning("No generation data returned from adapter")
                    self.gen_output_df = pd.DataFrame()
                    return
                
                # Check for unknown DUIDs
                all_duids_in_data = set(df['duid'].unique())
                known_duids = set(self.duid_to_fuel.keys())
                unknown_duids = all_duids_in_data - known_duids
                
                if unknown_duids:
                    self.handle_unknown_duids(unknown_duids, df)
                
                # Add fuel and region information
                df['fuel'] = df['duid'].map(self.duid_to_fuel)
                df['region'] = df['duid'].map(self.duid_to_region)
                
                # Log dropped records
                original_count = len(df)
                df = df.dropna(subset=['fuel', 'region'])
                dropped_count = original_count - len(df)
                
                if dropped_count > 0:
                    logger.warning(f"Dropped {dropped_count} records ({dropped_count/original_count*100:.1f}%) due to unknown DUIDs")
                
                # Set flag to indicate we're using raw data
                self._using_aggregated_data = False
                
                logger.info(f"Loaded {len(df)} raw generation records")
            
            self.gen_output_df = df
                
        except Exception as e:
            logger.error(f"Error loading generation data: {e}")
            self.gen_output_df = pd.DataFrame()
            self._using_aggregated_data = False

    def load_price_data(self):
        """Load and process price data using enhanced adapter with auto resolution"""
        try:
            # Use the enhanced price adapter with time filtering and auto resolution
            from ..shared.price_adapter import load_price_data
            
            # Calculate time window based on selected time range
            start_time, end_time = self._get_effective_date_range()
            
            df = load_price_data(
                start_date=start_time,
                end_date=end_time,
                resolution='auto'  # Automatically chooses best resolution
            )
            
            # Debug: Check the structure
            logger.info(f"Price data columns: {df.columns.tolist()}")
            logger.info(f"Price data index: {df.index.name}")
            logger.info(f"Price data shape: {df.shape}")
            logger.info(f"Price data dtypes:\n{df.dtypes}")
            
            # Check if SETTLEMENTDATE is the index
            if df.index.name == 'SETTLEMENTDATE':
                # Reset index to make SETTLEMENTDATE a regular column
                df = df.reset_index()
                logger.info("Reset index - SETTLEMENTDATE is now a column")
            
            # Now check columns again
            logger.info(f"Columns after reset_index: {df.columns.tolist()}")
            
            # Verify we have the required columns
            required_cols = ['SETTLEMENTDATE', 'RRP', 'REGIONID']
            missing_cols = [col for col in required_cols if col not in df.columns]
            if missing_cols:
                logger.error(f"Missing required columns: {missing_cols}")
                return pd.DataFrame()
            
            # Convert settlement date if it's not already datetime
            if not pd.api.types.is_datetime64_any_dtype(df['SETTLEMENTDATE']):
                df['SETTLEMENTDATE'] = pd.to_datetime(df['SETTLEMENTDATE'])
            
            # Apply time range filtering based on user selection
            start_datetime, end_datetime = self._get_effective_date_range()
            if start_datetime is not None:
                df = df[(df['SETTLEMENTDATE'] >= start_datetime) & (df['SETTLEMENTDATE'] <= end_datetime)]
                logger.info(f"Price data filtered to {start_datetime.date()} - {end_datetime.date()}")
            else:
                # For "All Data", use a reasonable fallback (last 3 months to avoid performance issues)
                fallback_start = datetime.now() - timedelta(days=90)
                df = df[df['SETTLEMENTDATE'] >= fallback_start]
                logger.info(f"Using fallback time filter for 'All Data': last 90 days")
            
            logger.info(f"Price data shape after time filtering: {df.shape}")
            
            # Filter by region
            if self.region != 'NEM':
                df = df[df['REGIONID'] == self.region]
            else:
                # For NEM, use NSW1 as representative (or you could average all regions)
                df = df[df['REGIONID'] == 'NSW1']
            
            logger.info(f"Price data shape after region filtering: {df.shape}")
            logger.info(f"Available regions in data: {df['REGIONID'].unique()}")
            
            # Ensure data is sorted by time
            df = df.sort_values('SETTLEMENTDATE')
            
            # Handle missing data by interpolating
            if not df.empty:
                # Create a clean dataframe with standardized column names
                clean_df = pd.DataFrame({
                    'settlementdate': df['SETTLEMENTDATE'],
                    'RRP': df['RRP']
                })
                
                # Set time as index for easier resampling/interpolation
                clean_df.set_index('settlementdate', inplace=True)
                
                # Always resample to 5-minute intervals and interpolate missing values
                clean_df = clean_df.resample('5min').mean()
                clean_df['RRP'] = clean_df['RRP'].interpolate(method='linear')
                
                # Reset index to get settlementdate back as column
                clean_df = clean_df.reset_index()
                
                logger.info(f"Loaded {len(clean_df)} price records for {self.time_range}")
                if not clean_df.empty:
                    logger.info(f"Price range: ${clean_df['RRP'].min():.2f} to ${clean_df['RRP'].max():.2f}")
                    logger.info(f"Time range: {clean_df['settlementdate'].min()} to {clean_df['settlementdate'].max()}")
                
                return clean_df
                
            else:
                logger.warning("No price data found for the specified time window and region")
                return pd.DataFrame()
            
        except Exception as e:
            logger.error(f"Error loading price data: {e}")
            import traceback
            traceback.print_exc()
            return pd.DataFrame()

    def load_transmission_data(self):
        """Load and process transmission flow data using enhanced adapter"""
        try:
            from ..shared.transmission_adapter import load_transmission_data
            
            # Calculate time window based on selected time range
            start_datetime, end_datetime = self._get_effective_date_range()
            
            # Load transmission data using enhanced adapter with auto resolution and performance optimization
            # Check if this is a long date range that needs optimization
            days_span = (end_datetime - start_datetime).total_seconds() / (24 * 3600) if start_datetime and end_datetime else 0
            needs_optimization = days_span > 90  # Optimize for ranges > 3 months
            
            if needs_optimization:
                logger.info(f"Long date range detected ({days_span:.1f} days), applying performance optimization")
                df, metadata = load_transmission_data(
                    start_date=start_datetime,
                    end_date=end_datetime,
                    resolution='auto',
                    optimize_for_plotting=True,
                    plot_type='transmission'
                )
                logger.info(f"Transmission optimization: {metadata.get('description', 'Unknown')}")
                logger.info(f"Reduction: {metadata.get('reduction_ratio', 0):.1%} of original data")
            else:
                df = load_transmission_data(
                    start_date=start_datetime,
                    end_date=end_datetime,
                    resolution='auto'
                )
            logger.info(f"Loaded transmission data using enhanced adapter: {df.shape}")
            
            if df.empty:
                self.transmission_df = pd.DataFrame()
                return
            
            # Store transmission data
            self.transmission_df = df
            logger.info(f"Loaded {len(df)} transmission records using enhanced adapter with auto resolution")
            logger.info(f"Transmission date range: {df['settlementdate'].min()} to {df['settlementdate'].max()}")
            logger.info(f"Available interconnectors: {df['interconnectorid'].unique()}")
            
        except Exception as e:
            logger.error(f"Error loading transmission data: {e}")
            self.transmission_df = pd.DataFrame()


    def load_rooftop_solar_data(self):
        """Load and process rooftop solar data using the enhanced rooftop adapter"""
        try:
            from ..shared.rooftop_adapter import load_rooftop_data
            
            # Calculate time window based on selected time range
            start_datetime, end_datetime = self._get_effective_date_range()
            
            # Load data using the enhanced adapter with time filtering
            df = load_rooftop_data(
                start_date=start_datetime,
                end_date=end_datetime
            )
            logger.info(f"Loaded rooftop solar data using enhanced adapter: {df.shape}")
            
            if df.empty:
                self.rooftop_df = pd.DataFrame()
                return
            
            logger.info(f"Rooftop solar date range: {df['settlementdate'].min()} to {df['settlementdate'].max()}")
            
            # Store rooftop solar data
            self.rooftop_df = df
            logger.info(f"Loaded {len(df)} rooftop solar records for {self.time_range}")
            logger.info(f"Available regions: {[col for col in df.columns if col != 'settlementdate']}")
            
        except Exception as e:
            logger.error(f"Error loading rooftop solar data: {e}")
            self.rooftop_df = pd.DataFrame()

    def calculate_regional_transmission_flows(self):
        """Calculate net transmission flows for the selected region"""
        if self.transmission_df is None or self.transmission_df.empty or self.region == 'NEM':
            return pd.DataFrame(), pd.DataFrame()
        
        try:
            df = self.transmission_df.copy()
            
            # Define interconnector mapping for each region
            interconnector_mapping = {
                'NSW1': {
                    'NSW1-QLD1': 'from_nsw',      # Positive = export to QLD
                    'VIC1-NSW1': 'to_nsw',        # Positive = import from VIC  
                    'N-Q-MNSP1': 'from_nsw'       # DirectLink: Positive = export to QLD
                },
                'QLD1': {
                    'NSW1-QLD1': 'to_qld',        # Positive = import from NSW
                    'N-Q-MNSP1': 'to_qld'         # DirectLink: Positive = import from NSW
                },
                'VIC1': {
                    'VIC1-NSW1': 'from_vic',      # Positive = export to NSW
                    'V-SA': 'from_vic',           # Positive = export to SA
                    'V-S-MNSP1': 'from_vic',      # Murraylink: Positive = export to SA
                    'T-V-MNSP1': 'to_vic'         # Basslink: Positive = import from TAS
                },
                'SA1': {
                    'V-SA': 'to_sa',              # Positive = import from VIC
                    'V-S-MNSP1': 'to_sa'          # Murraylink: Positive = import from VIC
                },
                'TAS1': {
                    'T-V-MNSP1': 'from_tas'       # Basslink: Positive = export to VIC
                }
            }
            
            region_interconnectors = interconnector_mapping.get(self.region, {})
            if not region_interconnectors:
                logger.warning(f"No interconnectors defined for region {self.region}")
                return pd.DataFrame(), pd.DataFrame()
            
            # Filter transmission data for this region's interconnectors
            region_transmission = df[df['interconnectorid'].isin(region_interconnectors.keys())].copy()
            
            if region_transmission.empty:
                logger.warning(f"No transmission data found for {self.region}")
                return pd.DataFrame(), pd.DataFrame()
            
            # Apply flow direction corrections based on region perspective
            def correct_flow_direction(row):
                interconnector = row['interconnectorid']
                flow_type = region_interconnectors[interconnector]
                flow = row['meteredmwflow']
                
                # Correct sign based on region perspective
                if flow_type.startswith('to_'):
                    # This interconnector brings power TO our region (import)
                    return flow  # Positive = import
                else:
                    # This interconnector takes power FROM our region (export)  
                    return -flow  # Negative = export
            
            region_transmission['regional_flow'] = region_transmission.apply(correct_flow_direction, axis=1)
            
            # Aggregate net flows by time (sum all interconnectors for this region)
            net_flows = region_transmission.groupby('settlementdate')['regional_flow'].sum().reset_index()
            net_flows.columns = ['settlementdate', 'net_transmission_mw']
            
            # Create individual line data for the third chart
            line_data = region_transmission.pivot(
                index='settlementdate', 
                columns='interconnectorid', 
                values='regional_flow'
            ).fillna(0).reset_index()
            
            logger.info(f"Calculated transmission flows for {self.region}: "
                       f"{len(net_flows)} time points, "
                       f"{len(region_interconnectors)} interconnectors")
            
            return net_flows, line_data
            
        except Exception as e:
            logger.error(f"Error calculating transmission flows: {e}")
            return pd.DataFrame(), pd.DataFrame()

    
    def process_data_for_region(self):
        """Process generation data for selected region and add transmission flows"""
        if self.gen_output_df is None or self.gen_output_df.empty:
            return pd.DataFrame()
        
        # Check if we're using pre-aggregated data
        if self._using_aggregated_data:
            # Data is already aggregated by fuel type
            if self.region != 'NEM':
                # Need to re-query for specific region
                start_datetime, end_datetime = self._get_effective_date_range()
                df = self.query_manager.query_generation_by_fuel(
                    start_date=start_datetime,
                    end_date=end_datetime,
                    region=self.region
                )
                
                if df.empty:
                    logger.warning(f"No aggregated data for region {self.region}")
                    return pd.DataFrame()
                
                # Ensure datetime index
                df['settlementdate'] = pd.to_datetime(df['settlementdate'])
                
                # Pivot directly (data is already aggregated)
                pivot_df = df.pivot(
                    index='settlementdate',
                    columns='fuel_type',
                    values='total_generation_mw'
                ).fillna(0)
                
                logger.info(f"Using pre-aggregated data for {self.region}: {len(pivot_df)} time periods")
            else:
                # For NEM, use the already loaded data
                df = self.gen_output_df.copy()
                df['settlementdate'] = pd.to_datetime(df['settlementdate'])
                
                # Pivot the aggregated data
                pivot_df = df.pivot(
                    index='settlementdate',
                    columns='fuel_type',
                    values='total_generation_mw'
                ).fillna(0)
                
                logger.info(f"Using pre-aggregated NEM data: {len(pivot_df)} time periods")
        else:
            # Using raw DUID data - process as before
            df = self.gen_output_df.copy()
            
            # Filter by region
            if self.region != 'NEM':
                df = df[df['region'] == self.region]
            
            # Group by time and fuel type
            df['settlementdate'] = pd.to_datetime(df['settlementdate'])
            
            # Apply time range filtering
            start_datetime, end_datetime = self._get_effective_date_range()
            if start_datetime is not None:
                df = df[(df['settlementdate'] >= start_datetime) & (df['settlementdate'] <= end_datetime)]
                logger.info(f"Filtered generation data to {start_datetime.date()} - {end_datetime.date()}: {len(df)} records")
            
            # Always use 5-minute intervals without resampling
            result = df.groupby([
                pd.Grouper(key='settlementdate', freq='5min'),
                'fuel'
            ])['scadavalue'].sum().reset_index()
            
            # Pivot to get fuel types as columns
            pivot_df = result.pivot(index='settlementdate', columns='fuel', values='scadavalue')
            pivot_df = pivot_df.fillna(0)
        
        # Add transmission flows if available and not NEM region
        if self.region != 'NEM':
            try:
                # Load transmission data if not already loaded
                if self.transmission_df is None:
                    self.load_transmission_data()
                
                # Calculate transmission flows for this region
                net_flows, _ = self.calculate_regional_transmission_flows()
                
                if not net_flows.empty:
                    # Convert to same time index as generation data
                    net_flows['settlementdate'] = pd.to_datetime(net_flows['settlementdate'])
                    net_flows.set_index('settlementdate', inplace=True)
                    
                    # Align with generation data timeframe
                    common_index = pivot_df.index.intersection(net_flows.index)
                    if len(common_index) > 0:
                        # Split transmission into imports (positive) and exports (negative)
                        # Ensure we're getting the right column and it's numeric
                        transmission_series = net_flows.reindex(pivot_df.index, fill_value=0)['net_transmission_mw']
                        # Convert to float to ensure numeric type
                        transmission_values = pd.to_numeric(transmission_series, errors='coerce').fillna(0)
                        
                        # Add transmission imports (positive values only) - goes to top of stack
                        pivot_df['Transmission Flow'] = pd.Series(
                            np.where(transmission_values.values > 0, transmission_values.values, 0),
                            index=transmission_values.index
                        )
                        
                        # Add transmission exports (negative values only) - goes below battery
                        pivot_df['Transmission Exports'] = pd.Series(
                            np.where(transmission_values.values < 0, transmission_values.values, 0),
                            index=transmission_values.index
                        )
                        
                        logger.info(f"Added transmission flows: Imports max {pivot_df['Transmission Flow'].max():.1f}MW, "
                                   f"Exports min {pivot_df['Transmission Exports'].min():.1f}MW")
                    else:
                        logger.warning("No overlapping time data between generation and transmission")
                        
            except Exception as e:
                logger.error(f"Error adding transmission flows: {e}")
        
        # Add rooftop solar data if available
        try:
            # Load rooftop solar data if not already loaded
            if self.rooftop_df is None:
                self.load_rooftop_solar_data()
            
            if not self.rooftop_df.empty:
                rooftop_df = self.rooftop_df.copy()
                rooftop_df['settlementdate'] = pd.to_datetime(rooftop_df['settlementdate'])
                rooftop_df.set_index('settlementdate', inplace=True)
                
                # No resampling needed - rooftop solar is already in MW values
                
                if self.region == 'NEM':
                    # For NEM view, sum all main regions (ending in '1')
                    main_regions = ['NSW1', 'QLD1', 'SA1', 'TAS1', 'VIC1']
                    available_regions = [r for r in main_regions if r in rooftop_df.columns]
                    
                    if available_regions:
                        # Sum rooftop solar across all regions
                        total_rooftop = rooftop_df[available_regions].sum(axis=1)
                        rooftop_values = total_rooftop.reindex(pivot_df.index)
                        
                        # Forward-fill missing values at the end (up to 2 hours)
                        # This handles the case where rooftop data is less recent than generation data
                        rooftop_values = rooftop_values.fillna(method='ffill', limit=24)  # 24 * 5min = 2 hours
                        
                        # Apply gentle decay for extended forward-fill periods
                        last_valid_idx = rooftop_values.last_valid_index()
                        if last_valid_idx is not None and last_valid_idx < rooftop_values.index[-1]:
                            # Calculate how many periods we're forward-filling
                            fill_start_pos = rooftop_values.index.get_loc(last_valid_idx) + 1
                            fill_periods = len(rooftop_values) - fill_start_pos
                            
                            if fill_periods > 0:
                                # Apply exponential decay for realism (solar decreases over time)
                                last_value = rooftop_values.iloc[fill_start_pos - 1]
                                decay_rate = 0.98  # 2% decay per 5-minute period
                                for i in range(fill_periods):
                                    rooftop_values.iloc[fill_start_pos + i] = last_value * (decay_rate ** (i + 1))
                        
                        # Fill any remaining NaN with 0
                        rooftop_values = rooftop_values.fillna(0)
                        pivot_df['Rooftop Solar'] = rooftop_values
                        
                        logger.info(f"Added rooftop solar (NEM total): max {pivot_df['Rooftop Solar'].max():.1f}MW, "
                                   f"avg {pivot_df['Rooftop Solar'].mean():.1f}MW")
                        
                elif self.region.endswith('1') and self.region in rooftop_df.columns:
                    # For individual regions
                    rooftop_values = rooftop_df.reindex(pivot_df.index)[self.region]
                    
                    # Forward-fill missing values at the end (up to 2 hours)
                    rooftop_values = rooftop_values.fillna(method='ffill', limit=24)
                    
                    # Apply gentle decay for extended forward-fill periods
                    last_valid_idx = rooftop_values.last_valid_index()
                    if last_valid_idx is not None and last_valid_idx < rooftop_values.index[-1]:
                        fill_start_pos = rooftop_values.index.get_loc(last_valid_idx) + 1
                        fill_periods = len(rooftop_values) - fill_start_pos
                        
                        if fill_periods > 0:
                            last_value = rooftop_values.iloc[fill_start_pos - 1]
                            decay_rate = 0.98  # 2% decay per 5-minute period
                            for i in range(fill_periods):
                                rooftop_values.iloc[fill_start_pos + i] = last_value * (decay_rate ** (i + 1))
                    
                    # Fill any remaining NaN with 0
                    rooftop_values = rooftop_values.fillna(0)
                    pivot_df['Rooftop Solar'] = rooftop_values
                    
                    logger.info(f"Added rooftop solar: max {pivot_df['Rooftop Solar'].max():.1f}MW, "
                               f"avg {pivot_df['Rooftop Solar'].mean():.1f}MW")
                               
                elif not self.region.endswith('1'):
                    logger.info(f"Rooftop solar data not available for sub-region {self.region} (only available for main regions ending in '1')")
                        
        except Exception as e:
            logger.error(f"Error adding rooftop solar data: {e}")
        
        # Define preferred fuel order with transmission at correct positions
        preferred_order = [
            'Transmission Flow',     # NEW: At top of stack (positive values)
            'Solar', 
            'Rooftop Solar',         # NEW: After regular solar
            'Wind', 
            'Other', 
            'Coal', 
            'CCGT', 
            'Gas other', 
            'OCGT', 
            'Water',
            'Battery Storage',       # Above zero line (can be negative for charging)
            'Transmission Exports'   # NEW: Below battery (negative values)
        ]
        
        # Reorder columns based on preferred order, only including columns that exist
        available_fuels = [fuel for fuel in preferred_order if fuel in pivot_df.columns]
        
        # Add any remaining fuels not in the preferred order
        remaining_fuels = [col for col in pivot_df.columns if col not in available_fuels]
        final_order = available_fuels + remaining_fuels
        
        # Reorder the dataframe
        pivot_df = pivot_df[final_order]

        return pivot_df

    def calculate_generation_summary(self, data_df):
        """
        Calculate generation totals and percentages by fuel type from processed data.

        Args:
            data_df: DataFrame with datetime index and fuel types as columns (MW values)

        Returns:
            DataFrame with columns: fuel_type, total_gwh, percentage
        """
        if data_df.empty:
            return pd.DataFrame(columns=['fuel_type', 'total_gwh', 'percentage'])

        # Exclude storage and transmission from total generation
        # These columns should not be counted as actual generation
        excluded_columns = ['Battery Storage', 'Transmission Flow', 'Transmission Exports']

        # Filter to only actual generation fuel types
        generation_cols = [col for col in data_df.columns if col not in excluded_columns]

        # Calculate totals for each fuel type
        # Data is in MW at 5-minute intervals
        # To convert to GWh: (MW * hours) / 1000
        # 5 minutes = 5/60 hours = 0.08333 hours
        interval_hours = 5 / 60

        totals = {}
        for fuel in generation_cols:
            # Sum MW over all intervals and convert to GWh
            total_mwh = data_df[fuel].sum() * interval_hours
            total_gwh = total_mwh / 1000
            totals[fuel] = total_gwh

        # Calculate total generation (excludes storage and transmission)
        total_generation_gwh = sum(totals.values())

        # Create summary DataFrame
        summary_data = []
        for fuel, gwh in totals.items():
            percentage = (gwh / total_generation_gwh * 100) if total_generation_gwh > 0 else 0
            summary_data.append({
                'fuel_type': fuel,
                'total_gwh': gwh,
                'percentage': percentage
            })

        summary_df = pd.DataFrame(summary_data)

        # Sort by total_gwh descending
        summary_df = summary_df.sort_values('total_gwh', ascending=False)

        logger.info(f"Generation summary calculated: {len(summary_df)} fuel types, {total_generation_gwh:.1f} GWh total")

        return summary_df

    def calculate_pcp_date_range(self):
        """
        Calculate the Previous Corresponding Period (PCP) date range.
        PCP is the same date range shifted back exactly 12 months (365 days).

        Returns:
            Tuple of (pcp_start_date, pcp_end_date) as datetime objects, or (None, None) if:
            - Custom range is >= 365 days (would overlap with current period)
            - Time range is 'All' (no meaningful PCP)
        """
        # Don't show PCP for 'All' data
        if self.time_range == 'All':
            logger.info("PCP not available for 'All' time range")
            return None, None

        # Get current period dates
        start_datetime, end_datetime = self._get_effective_date_range()

        # Calculate the duration of the selected period
        period_duration = end_datetime - start_datetime

        # If custom range >= 365 days, don't show PCP (would overlap)
        if period_duration.days >= 365:
            logger.info(f"PCP not shown: period duration ({period_duration.days} days) >= 365 days")
            return None, None

        # Calculate PCP dates (exactly 365 days prior)
        pcp_start = start_datetime - timedelta(days=365)
        pcp_end = end_datetime - timedelta(days=365)

        logger.info(f"PCP date range: {pcp_start.date()} to {pcp_end.date()}")

        return pcp_start, pcp_end

    def get_pcp_generation_data(self):
        """
        Query and process generation data for the Previous Corresponding Period (PCP).

        Returns:
            DataFrame with same structure as process_data_for_region() but for PCP dates,
            or empty DataFrame if PCP is not available
        """
        # Get PCP date range
        pcp_start, pcp_end = self.calculate_pcp_date_range()

        if pcp_start is None or pcp_end is None:
            return pd.DataFrame()

        try:
            # Calculate days span to determine loading strategy (same logic as load_generation_data)
            days_span = (pcp_end - pcp_start).total_seconds() / (24 * 3600)

            # Query PCP generation data
            if days_span > 30:
                # Use pre-aggregated data
                logger.info(f"Querying PCP pre-aggregated data for {days_span:.0f} day range")
                df = self.query_manager.query_generation_by_fuel(
                    start_date=pcp_start,
                    end_date=pcp_end,
                    region=self.region if self.region != 'NEM' else 'NEM'
                )

                if df.empty:
                    logger.warning("No PCP generation data available")
                    return pd.DataFrame()

                # Ensure datetime index
                df['settlementdate'] = pd.to_datetime(df['settlementdate'])

                # Pivot to get fuel types as columns
                pcp_df = df.pivot(
                    index='settlementdate',
                    columns='fuel_type',
                    values='total_generation_mw'
                ).fillna(0)

            else:
                # Use raw DUID data
                logger.info(f"Querying PCP raw data for {days_span:.0f} day range")
                from ..shared.adapter_selector import load_generation_data

                df = load_generation_data(
                    start_date=pcp_start,
                    end_date=pcp_end,
                    resolution='auto'
                )

                if df.empty:
                    logger.warning("No PCP generation data available")
                    return pd.DataFrame()

                # Add fuel and region information
                df['fuel'] = df['duid'].map(self.duid_to_fuel)
                df['region'] = df['duid'].map(self.duid_to_region)
                df = df.dropna(subset=['fuel', 'region'])

                # Filter by region
                if self.region != 'NEM':
                    df = df[df['region'] == self.region]

                # Group by time and fuel type
                df['settlementdate'] = pd.to_datetime(df['settlementdate'])
                result = df.groupby([
                    pd.Grouper(key='settlementdate', freq='5min'),
                    'fuel'
                ])['scadavalue'].sum().reset_index()

                # Pivot to get fuel types as columns
                pcp_df = result.pivot(index='settlementdate', columns='fuel', values='scadavalue')
                pcp_df = pcp_df.fillna(0)

            # Add rooftop solar if available (same logic as process_data_for_region)
            try:
                from ..shared.rooftop_adapter import load_rooftop_data
                rooftop_df = load_rooftop_data(
                    start_date=pcp_start,
                    end_date=pcp_end
                )

                if not rooftop_df.empty:
                    rooftop_df['settlementdate'] = pd.to_datetime(rooftop_df['settlementdate'])
                    rooftop_df.set_index('settlementdate', inplace=True)

                    if self.region == 'NEM':
                        main_regions = ['NSW1', 'QLD1', 'SA1', 'TAS1', 'VIC1']
                        available_regions = [r for r in main_regions if r in rooftop_df.columns]
                        if available_regions:
                            total_rooftop = rooftop_df[available_regions].sum(axis=1)
                            pcp_df['Rooftop Solar'] = total_rooftop.reindex(pcp_df.index).fillna(0)
                    elif self.region.endswith('1') and self.region in rooftop_df.columns:
                        pcp_df['Rooftop Solar'] = rooftop_df[self.region].reindex(pcp_df.index).fillna(0)

                    logger.info(f"Added PCP rooftop solar data")
            except Exception as e:
                logger.warning(f"Could not load PCP rooftop solar: {e}")

            logger.info(f"Loaded PCP generation data: {pcp_df.shape}")
            return pcp_df

        except Exception as e:
            logger.error(f"Error loading PCP generation data: {e}")
            return pd.DataFrame()

    def create_generation_summary_table(self):
        """
        Create a table showing generation totals and percentages by fuel type,
        comparing current period with Previous Corresponding Period (PCP).

        Returns:
            Panel Tabulator widget with the summary table
        """
        try:
            # Get current period data
            current_data = self.process_data_for_region()
            if current_data.empty:
                return pn.pane.Markdown("*No generation data available for selected period*")

            # Calculate current period summary
            current_summary = self.calculate_generation_summary(current_data)
            if current_summary.empty:
                return pn.pane.Markdown("*Unable to calculate generation summary*")

            # Get PCP data if available
            pcp_data = self.get_pcp_generation_data()
            show_pcp = not pcp_data.empty

            if show_pcp:
                # Calculate PCP summary
                pcp_summary = self.calculate_generation_summary(pcp_data)

                # Merge current and PCP summaries
                merged = current_summary.merge(
                    pcp_summary,
                    on='fuel_type',
                    how='outer',
                    suffixes=('_current', '_pcp')
                ).fillna(0)

                # Calculate change (absolute difference in GWh)
                merged['change_gwh'] = merged['total_gwh_current'] - merged['total_gwh_pcp']
                merged['change_pct_points'] = merged['percentage_current'] - merged['percentage_pcp']

                # Sort by current generation descending (most important first)
                merged = merged.sort_values('total_gwh_current', ascending=False)

                # Calculate totals for summary row
                total_current_gwh = merged['total_gwh_current'].sum()
                total_pcp_gwh = merged['total_gwh_pcp'].sum()
                total_change_gwh = total_current_gwh - total_pcp_gwh

                # Format the table data (0 decimal places)
                table_data = []
                for _, row in merged.iterrows():
                    table_data.append({
                        'Fuel Type': row['fuel_type'],
                        'Current (GWh)': int(round(row['total_gwh_current'])),
                        'Current %': int(round(row['percentage_current'])),
                        'PCP (GWh)': int(round(row['total_gwh_pcp'])),
                        'PCP %': int(round(row['percentage_pcp'])),
                        'Change (GWh)': int(round(row['change_gwh'])),
                        'Change (% pts)': int(round(row['change_pct_points']))
                    })

                # Add totals row
                table_data.append({
                    'Fuel Type': 'TOTAL',
                    'Current (GWh)': int(round(total_current_gwh)),
                    'Current %': 100,
                    'PCP (GWh)': int(round(total_pcp_gwh)),
                    'PCP %': 100,
                    'Change (GWh)': int(round(total_change_gwh)),
                    'Change (% pts)': 0
                })

            else:
                # No PCP data - show current period only
                # Sort by total generation descending
                current_summary = current_summary.sort_values('total_gwh', ascending=False)

                # Calculate total
                total_gwh = current_summary['total_gwh'].sum()

                table_data = []
                for _, row in current_summary.iterrows():
                    table_data.append({
                        'Fuel Type': row['fuel_type'],
                        'Total (GWh)': int(round(row['total_gwh'])),
                        'Percentage': int(round(row['percentage']))
                    })

                # Add totals row
                table_data.append({
                    'Fuel Type': 'TOTAL',
                    'Total (GWh)': int(round(total_gwh)),
                    'Percentage': 100
                })

            # Create Tabulator widget with right-aligned numeric columns
            table_df = pd.DataFrame(table_data)

            # Configure column alignment - numeric columns right-aligned
            if show_pcp:
                formatters = {
                    'Fuel Type': {'type': 'plaintext'},
                    'Current (GWh)': {'type': 'money', 'decimal': '', 'thousand': ',', 'symbol': '', 'precision': 0},
                    'Current %': {'type': 'money', 'decimal': '', 'thousand': ',', 'symbol': '', 'precision': 0},
                    'PCP (GWh)': {'type': 'money', 'decimal': '', 'thousand': ',', 'symbol': '', 'precision': 0},
                    'PCP %': {'type': 'money', 'decimal': '', 'thousand': ',', 'symbol': '', 'precision': 0},
                    'Change (GWh)': {'type': 'money', 'decimal': '', 'thousand': ',', 'symbol': '', 'precision': 0},
                    'Change (% pts)': {'type': 'money', 'decimal': '', 'thousand': ',', 'symbol': '', 'precision': 0}
                }
            else:
                formatters = {
                    'Fuel Type': {'type': 'plaintext'},
                    'Total (GWh)': {'type': 'money', 'decimal': '', 'thousand': ',', 'symbol': '', 'precision': 0},
                    'Percentage': {'type': 'money', 'decimal': '', 'thousand': ',', 'symbol': '', 'precision': 0}
                }

            table = pn.widgets.Tabulator(
                table_df,
                layout='fit_data_table',
                sizing_mode='stretch_width',
                theme='fast',
                show_index=False,
                disabled=True,  # Read-only
                formatters=formatters,
                stylesheets=[f"""
                    .tabulator {{
                        background-color: {FLEXOKI_PAPER};
                        color: {FLEXOKI_BLACK};
                    }}
                    .tabulator-header {{
                        background-color: {FLEXOKI_BASE[100]};
                        color: {FLEXOKI_BLACK};
                        font-weight: bold;
                    }}
                    .tabulator-row {{
                        background-color: {FLEXOKI_PAPER};
                        color: {FLEXOKI_BLACK};
                    }}
                    .tabulator-row:nth-child(even) {{
                        background-color: {FLEXOKI_BASE[50]};
                    }}
                    .tabulator-row:hover {{
                        background-color: {FLEXOKI_BASE[100]};
                    }}
                """],
                configuration={
                    'columnDefaults': {
                        'headerSort': False  # Disable sorting to preserve descending order
                    }
                }
            )

            # Get date range display for title
            time_range_display = self._get_time_range_display()
            region_display = self.region

            if show_pcp:
                pcp_start, pcp_end = self.calculate_pcp_date_range()
                pcp_display = f"{pcp_start.strftime('%Y-%m-%d')} to {pcp_end.strftime('%Y-%m-%d')}"
                title = pn.pane.Markdown(
                    f"### Generation Summary: {region_display} - {time_range_display} vs PCP ({pcp_display})\n"
                    f"*Data: AEMO*"
                )
            else:
                title = pn.pane.Markdown(
                    f"### Generation Summary: {region_display} - {time_range_display}\n"
                    f"*Data: AEMO*"
                )

            return pn.Column(title, table, sizing_mode='stretch_width')

        except Exception as e:
            logger.error(f"Error creating generation summary table: {e}")
            import traceback
            traceback.print_exc()
            return pn.pane.Markdown(f"**Error creating summary table:** {e}")

    def calculate_capacity_utilization(self):
        """Calculate capacity utilization by fuel type for selected region"""
        # Check if we can use pre-computed capacity utilization from DuckDB
        if self._using_aggregated_data:
            try:
                start_datetime, end_datetime = self._get_effective_date_range()
                
                # Query capacity utilization directly from DuckDB view
                util_data = self.query_manager.query_capacity_utilization(
                    start_date=start_datetime,
                    end_date=end_datetime,
                    region=self.region
                )
                
                if not util_data.empty:
                    # Ensure datetime index
                    util_data['settlementdate'] = pd.to_datetime(util_data['settlementdate'])
                    
                    # Pivot to get fuel types as columns
                    pivot_df = util_data.pivot(
                        index='settlementdate',
                        columns='fuel_type',
                        values='utilization_pct'
                    ).fillna(0)
                    
                    # Ensure all values are between 0 and 100
                    pivot_df = pivot_df.clip(lower=0, upper=100)
                    
                    logger.info(f"Using pre-computed capacity utilization: {len(pivot_df)} time periods")
                    return pivot_df
                    
            except Exception as e:
                logger.warning(f"Error loading pre-computed utilization, falling back to calculation: {e}")
        
        # Fallback to original calculation method for raw data
        if self.gen_output_df is None or self.gen_output_df.empty:
            return pd.DataFrame()
        
        df = self.gen_output_df.copy()
        
        # Filter by region
        if self.region != 'NEM':
            df = df[df['region'] == self.region]
        
        # Group generation by time and fuel type
        df['settlementdate'] = pd.to_datetime(df['settlementdate'])
        
        # Apply time range filtering
        start_datetime, end_datetime = self._get_effective_date_range()
        if start_datetime is not None:
            df = df[(df['settlementdate'] >= start_datetime) & (df['settlementdate'] <= end_datetime)]
        
        # Always aggregate generation by 5-minute intervals and fuel type
        generation = df.groupby([
            pd.Grouper(key='settlementdate', freq='5min'),
            'fuel'
        ])['scadavalue'].sum().reset_index()
        
        # Get capacity data by fuel type for the region
        capacity_df = self.gen_info_df.copy()
        if self.region != 'NEM':
            capacity_df = capacity_df[capacity_df['Region'] == self.region]
        
        # Clean and convert capacity data - handle string ranges and non-numeric values
        def clean_capacity(capacity):
            if pd.isna(capacity):
                return 0
            if isinstance(capacity, str):
                # Handle range strings like "23.44 - 27.60"
                if ' - ' in capacity:
                    try:
                        # Take the average of the range
                        parts = capacity.split(' - ')
                        return (float(parts[0]) + float(parts[1])) / 2
                    except ValueError:
                        return 0
                else:
                    try:
                        return float(capacity)
                    except ValueError:
                        return 0
            try:
                return float(capacity)
            except (ValueError, TypeError):
                return 0
        
        capacity_df['Clean_Capacity'] = capacity_df['Capacity(MW)'].apply(clean_capacity)
        
        # Sum capacity by fuel type using the cleaned capacity
        fuel_capacity = capacity_df.groupby('Fuel')['Clean_Capacity'].sum()
        
        # Debug: Log capacity data for troubleshooting
        logger.info(f"Fuel capacities for {self.region}: {fuel_capacity.to_dict()}")
        
        # Calculate utilization for each time period and fuel
        utilization_data = []
        for _, row in generation.iterrows():
            fuel = row['fuel']
            if fuel in fuel_capacity.index and fuel_capacity[fuel] > 0:
                generation_mw = row['scadavalue']
                capacity_mw = fuel_capacity[fuel]
                utilization = (generation_mw / capacity_mw) * 100
                
                # Debug logging for first few calculations
                if len(utilization_data) < 5:
                    logger.info(f"Fuel: {fuel}, Generation: {generation_mw:.2f} MW, Capacity: {capacity_mw:.2f} MW, Utilization: {utilization:.2f}%")
                
                # Cap at 100% to handle any data anomalies and negative values
                utilization = max(0, min(utilization, 100))
                utilization_data.append({
                    'settlementdate': row['settlementdate'],
                    'fuel': fuel,
                    'utilization': utilization
                })
        
        if not utilization_data:
            logger.warning("No utilization data calculated")
            return pd.DataFrame()
        
        utilization_df = pd.DataFrame(utilization_data)
        
        # Debug: Check the raw utilization values
        logger.info(f"Sample utilization values: {utilization_df['utilization'].head().tolist()}")
        
        # Pivot to get fuel types as columns
        pivot_df = utilization_df.pivot(index='settlementdate', columns='fuel', values='utilization')
        pivot_df = pivot_df.fillna(0)
        
        # Additional safety: ensure all values are between 0 and 100
        pivot_df = pivot_df.clip(lower=0, upper=100)
        
        # Debug: Check final pivot values
        logger.info(f"Final pivot data shape: {pivot_df.shape}")
        logger.info(f"Final pivot max values: {pivot_df.max().to_dict()}")
        
        return pivot_df
    
    def get_fuel_colors(self):
        """Define colors for different fuel types - Flexoki theme"""
        fuel_colors = {
            'Coal': '#6B3A10',        # Brown - distinctive for coal
            'Gas': FLEXOKI_ACCENT['orange'],  # Combined CCGT + OCGT + Gas other
            'CCGT': '#8A2E0D',        # Kept for backward compatibility
            'OCGT': '#E05830',        # Kept for backward compatibility
            'Gas other': FLEXOKI_ACCENT['orange'],  # Kept for backward compatibility
            'Solar': '#D4A000',       # Gold/bright yellow - sunny color
            'Rooftop Solar': '#E8C547',  # Lighter yellow - distributed solar
            'Wind': FLEXOKI_ACCENT['green'],  # Green - wind/renewable
            'Hydro': FLEXOKI_ACCENT['cyan'],  # Cyan - hydro (was 'Water')
            'Water': FLEXOKI_ACCENT['cyan'],  # Kept for backward compatibility
            'Battery': FLEXOKI_ACCENT['purple'],  # Purple - technology (was 'Battery Storage')
            'Battery Storage': FLEXOKI_ACCENT['purple'],  # Kept for backward compatibility
            'Biomass': '#4A7C23',     # Dark green - organic
            'Other': FLEXOKI_BASE[600],  # Gray - catch-all category
            'Transmission': FLEXOKI_ACCENT['magenta'],  # Magenta - combined imports/exports
            'Transmission Flow': FLEXOKI_ACCENT['magenta'],  # Kept for backward compatibility
            'Transmission Imports': FLEXOKI_ACCENT['magenta'],  # Kept for backward compatibility
            'Transmission Exports': FLEXOKI_ACCENT['magenta']   # Kept for backward compatibility
        }
        return fuel_colors

    def normalize_fuel_types(self, df):
        """Normalize fuel type column names to standard names for Plotly charts"""
        rename_map = {
            'Water': 'Hydro',
            'Battery Storage': 'Battery',
            'CCGT': 'Gas',
            'OCGT': 'Gas',
            'Gas other': 'Gas',
            'Transmission Exports': 'Transmission',
            'Transmission Imports': 'Transmission',
        }
        # Rename columns that exist
        df = df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns})
        # Combine duplicate columns (sum them)
        df = df.groupby(level=0, axis=1).sum()
        return df

    def calculate_generation_statistics(self, gen_data, price_data, trans_data):
        """Calculate volume share, VWAP by fuel at 30-min resolution for Plotly stats table"""
        if gen_data.empty or price_data.empty:
            return None, None

        # Resample to 30-minute intervals
        gen_30min = gen_data.resample('30min').mean()

        # Handle price data format (may have RRP or rrp column)
        price_col = 'RRP' if 'RRP' in price_data.columns else 'rrp'
        price_series = price_data.set_index('settlementdate')[price_col]
        price_30min = price_series.resample('30min').mean()
        price_aligned = price_30min.reindex(gen_30min.index, method='nearest')

        # Calculate MWh (MW * 0.5 hours per interval)
        gen_mwh = gen_30min * 0.5

        # Add transmission imports to the mix
        if trans_data is not None and isinstance(trans_data, pd.DataFrame) and not trans_data.empty:
            if 'net_transmission_mw' in trans_data.columns:
                trans_indexed = trans_data.set_index('settlementdate')['net_transmission_mw']
                trans_30min = trans_indexed.resample('30min').mean()
            elif 'net_flow' in trans_data.columns:
                trans_indexed = trans_data.set_index('settlementdate')['net_flow']
                trans_30min = trans_indexed.resample('30min').mean()
            elif isinstance(trans_data, pd.Series):
                trans_30min = trans_data.resample('30min').mean()
            else:
                trans_30min = None

            if trans_30min is not None:
                trans_aligned = trans_30min.reindex(gen_30min.index, fill_value=0)
                gen_30min['Transmission'] = trans_aligned.clip(lower=0)
                gen_mwh['Transmission'] = trans_aligned.clip(lower=0) * 0.5

        # Total generation (positive values only)
        total_mwh_per_interval = gen_mwh.clip(lower=0).sum(axis=1)
        total_mwh = total_mwh_per_interval.sum()

        # Track battery charging and transmission exports separately
        battery_charge_mwh = None
        battery_charge_avg_mw = None
        battery_charge_vwap = None
        if 'Battery' in gen_30min.columns:
            battery_charging = gen_30min['Battery'].clip(upper=0).abs()  # Negative values as positive
            battery_charge_mwh = (battery_charging * 0.5).sum()
            battery_charge_avg_mw = battery_charging.mean()
            if battery_charge_mwh > 0:
                battery_charge_vwap = (price_aligned * battery_charging * 0.5).sum() / battery_charge_mwh

        trans_export_mwh = None
        trans_export_avg_mw = None
        trans_export_vwap = None
        if trans_data is not None and isinstance(trans_data, pd.DataFrame) and not trans_data.empty:
            if 'net_transmission_mw' in trans_data.columns:
                trans_indexed = trans_data.set_index('settlementdate')['net_transmission_mw']
            elif 'net_flow' in trans_data.columns:
                trans_indexed = trans_data.set_index('settlementdate')['net_flow']
            else:
                trans_indexed = None

            if trans_indexed is not None:
                trans_30min_full = trans_indexed.resample('30min').mean()
                trans_aligned_full = trans_30min_full.reindex(gen_30min.index, fill_value=0)
                trans_exports = trans_aligned_full.clip(upper=0).abs()  # Negative values as positive
                trans_export_mwh = (trans_exports * 0.5).sum()
                trans_export_avg_mw = trans_exports.mean()
                if trans_export_mwh > 0:
                    trans_export_vwap = (price_aligned * trans_exports * 0.5).sum() / trans_export_mwh

        # Core fuels always shown, minor fuels need 1% threshold
        CORE_FUELS = {'Coal', 'Gas', 'Solar', 'Wind', 'Hydro', 'Battery'}
        SIGNIFICANCE_THRESHOLD = 0.01

        # Merit order for display
        merit_order = ['Coal', 'Wind', 'Solar', 'Rooftop Solar', 'Battery', 'Hydro', 'Gas', 'Transmission']

        stats = []
        for fuel in merit_order:
            if fuel not in gen_mwh.columns:
                continue

            fuel_mwh = gen_mwh[fuel].clip(lower=0)
            fuel_total_mwh = fuel_mwh.sum()
            fuel_avg_mw = gen_30min[fuel].clip(lower=0).mean()

            if fuel_total_mwh <= 0:
                continue

            # Skip insignificant minor fuels
            if fuel not in CORE_FUELS and fuel != 'Transmission':
                if total_mwh > 0 and fuel_total_mwh / total_mwh < SIGNIFICANCE_THRESHOLD:
                    continue

            share = fuel_total_mwh / total_mwh * 100 if total_mwh > 0 else 0
            vwap = (price_aligned * fuel_mwh).sum() / fuel_total_mwh if fuel_total_mwh > 0 else 0

            stats.append({
                'Fuel': fuel,
                'Avg GW': fuel_avg_mw / 1000,
                'Volume GWh': fuel_total_mwh / 1000,
                'Share': share,
                'VWAP': vwap
            })

        # Summary metrics
        avg_total_gw = gen_30min.clip(lower=0).sum(axis=1).mean() / 1000
        lwap = (price_aligned * total_mwh_per_interval).sum() / total_mwh if total_mwh > 0 else 0
        flat_price = price_aligned.mean()

        # Calculate net demand = total - battery charging - transmission exports
        net_demand_mwh = total_mwh
        if battery_charge_mwh:
            net_demand_mwh -= battery_charge_mwh
        if trans_export_mwh:
            net_demand_mwh -= trans_export_mwh

        summary = {
            'avg_total_gw': avg_total_gw,
            'total_gwh': total_mwh / 1000,
            'lwap': lwap,
            'flat_price': flat_price,
            'battery_charge_gwh': battery_charge_mwh / 1000 if battery_charge_mwh else None,
            'battery_charge_avg_gw': battery_charge_avg_mw / 1000 if battery_charge_avg_mw else None,
            'battery_charge_vwap': battery_charge_vwap,
            'trans_export_gwh': trans_export_mwh / 1000 if trans_export_mwh else None,
            'trans_export_avg_gw': trans_export_avg_mw / 1000 if trans_export_avg_mw else None,
            'trans_export_vwap': trans_export_vwap,
            'net_demand_gwh': net_demand_mwh / 1000,
        }

        return pd.DataFrame(stats), summary

    def create_stats_table_html(self, stats_df, summary):
        """Create styled HTML table for Plotly generation statistics"""
        if stats_df is None or stats_df.empty:
            return pn.pane.HTML("<p><em>No statistics available</em></p>")

        html = f'''
        <style>
            .stats-table {{
                font-family: Inter, -apple-system, sans-serif;
                font-size: 13px;
                border-collapse: collapse;
                background-color: {FLEXOKI_PAPER};
                color: {FLEXOKI_BLACK};
                max-width: 550px;
            }}
            .stats-table th {{
                text-align: right;
                padding: 8px 12px;
                border-bottom: 1.5px solid {FLEXOKI_BASE[600]};
                font-weight: 600;
            }}
            .stats-table th:first-child {{ text-align: left; }}
            .stats-table td {{
                text-align: right;
                padding: 6px 12px;
                border-bottom: 0.5px solid #E6E4D9;
                color: {FLEXOKI_BASE[800]};
            }}
            .stats-table td:first-child {{
                text-align: left;
                font-weight: 500;
                color: {FLEXOKI_BLACK};
            }}
            .stats-table tr.total-row td {{
                border-top: 1.5px solid {FLEXOKI_BASE[600]};
                font-weight: 600;
                color: {FLEXOKI_BLACK};
            }}
        </style>
        <table class="stats-table">
            <thead>
                <tr>
                    <th>Fuel</th>
                    <th>Avg GW</th>
                    <th>GWh</th>
                    <th>Share %</th>
                    <th>VWAP $/MWh</th>
                </tr>
            </thead>
            <tbody>
        '''

        for _, row in stats_df.iterrows():
            html += f'''
                <tr>
                    <td>{row['Fuel']}</td>
                    <td>{row['Avg GW']:.1f}</td>
                    <td>{row['Volume GWh']:.1f}</td>
                    <td>{row['Share']:.1f}</td>
                    <td>{row['VWAP']:.0f}</td>
                </tr>
            '''

        total_gwh = summary['total_gwh']  # Use actual total, not just displayed rows
        total_share = stats_df['Share'].sum()
        html += f'''
                <tr class="total-row">
                    <td>Total Supply</td>
                    <td>{summary['avg_total_gw']:.1f}</td>
                    <td>{total_gwh:.1f}</td>
                    <td></td>
                    <td>{summary['lwap']:.0f}</td>
                </tr>
        '''

        # Add Battery Charge row if available
        if summary.get('battery_charge_gwh') is not None and summary['battery_charge_gwh'] > 0:
            bc_avg = summary.get('battery_charge_avg_gw', 0) or 0
            bc_gwh = summary.get('battery_charge_gwh', 0) or 0
            bc_vwap = summary.get('battery_charge_vwap')
            bc_vwap_str = f"{bc_vwap:.0f}" if bc_vwap is not None else ""
            html += f'''
                <tr>
                    <td>Battery Charge</td>
                    <td>{bc_avg:.1f}</td>
                    <td>{bc_gwh:.1f}</td>
                    <td></td>
                    <td>{bc_vwap_str}</td>
                </tr>
            '''

        # Add Transmission Exports row if available
        if summary.get('trans_export_gwh') is not None and summary['trans_export_gwh'] > 0:
            te_avg = summary.get('trans_export_avg_gw', 0) or 0
            te_gwh = summary.get('trans_export_gwh', 0) or 0
            te_vwap = summary.get('trans_export_vwap')
            te_vwap_str = f"{te_vwap:.0f}" if te_vwap is not None else ""
            html += f'''
                <tr>
                    <td>Transmission Exports</td>
                    <td>{te_avg:.1f}</td>
                    <td>{te_gwh:.1f}</td>
                    <td></td>
                    <td>{te_vwap_str}</td>
                </tr>
            '''

        # Add Net Demand row
        net_demand_gwh = summary.get('net_demand_gwh', 0) or 0
        html += f'''
                <tr class="total-row">
                    <td>Net Demand</td>
                    <td></td>
                    <td>{net_demand_gwh:.1f}</td>
                    <td></td>
                    <td></td>
                </tr>
                <tr class="total-row">
                    <td>Flat load $/MWh</td>
                    <td></td>
                    <td></td>
                    <td></td>
                    <td>{summary['flat_price']:.0f}</td>
                </tr>
            </tbody>
        </table>
        '''
        return pn.pane.HTML(html, sizing_mode='stretch_width')

    def create_price_band_table_html(self, band_data, selected_regions):
        """Create styled HTML table for price band details

        Args:
            band_data: dict of region -> list of band dicts with keys:
                Price Band, Contr, Share, Time, Avg, Revenue
            selected_regions: list of regions in display order
        """
        if not band_data:
            return "<p><em>No price band data available</em></p>"

        html = f'''
        <style>
            .band-table {{
                font-family: Inter, -apple-system, sans-serif;
                font-size: 12px;
                border-collapse: collapse;
                background-color: {FLEXOKI_PAPER};
                color: {FLEXOKI_BLACK};
                width: 100%;
                max-width: 620px;
            }}
            .band-table th {{
                text-align: right;
                padding: 8px 10px;
                border-bottom: 1.5px solid {FLEXOKI_BASE[300]};
                font-weight: 600;
                font-size: 11px;
            }}
            .band-table th:nth-child(1),
            .band-table th:nth-child(2) {{ text-align: left; }}
            .band-table td {{
                text-align: right;
                padding: 6px 10px;
                border-bottom: 0.5px solid {FLEXOKI_BASE[100]};
            }}
            .band-table td:nth-child(1),
            .band-table td:nth-child(2) {{ text-align: left; }}
            .band-table td:nth-child(1) {{
                font-weight: 600;
            }}
            /* Muted columns: Time, Avg, Revenue */
            .band-table td:nth-child(5),
            .band-table td:nth-child(6),
            .band-table td:nth-child(7) {{
                color: {FLEXOKI_BASE[600]};
            }}
            .band-table tr.total-row td {{
                font-weight: 600;
                color: {FLEXOKI_BLACK};
            }}
            .band-table tr.region-divider td {{
                border-top: 1.5px solid {FLEXOKI_BASE[300]};
            }}
        </style>
        <table class="band-table">
            <thead>
                <tr>
                    <th>Region</th>
                    <th>Price Band</th>
                    <th>Contr. ($/MWh)</th>
                    <th>Share (%)</th>
                    <th>Time (%)</th>
                    <th>Avg ($/MWh)</th>
                    <th>Revenue ($m)</th>
                </tr>
            </thead>
            <tbody>
        '''

        # Band order for sorting
        band_order = ['Below $0', '$0-$300', '$301-$1000', 'Above $1000']

        for region_idx, region in enumerate(selected_regions):
            if region not in band_data:
                continue

            bands = band_data[region]
            # Sort bands by order
            sorted_bands = sorted(bands, key=lambda x: band_order.index(x['Price Band']) if x['Price Band'] in band_order else 99)

            # Calculate totals
            total_contr = sum(b['Contr'] for b in sorted_bands)
            total_share = sum(b['Share'] for b in sorted_bands)
            total_time = sum(b['Time'] for b in sorted_bands)
            total_revenue = sum(b['Revenue'] for b in sorted_bands)

            # Add divider class for regions after the first
            divider_class = ' class="region-divider"' if region_idx > 0 else ''

            # Band rows
            for i, band in enumerate(sorted_bands):
                row_class = divider_class if i == 0 else ''
                region_display = region if i == 0 else ''
                html += f'''
                <tr{row_class}>
                    <td>{region_display}</td>
                    <td>{band['Price Band']}</td>
                    <td>{band['Contr']:.1f}</td>
                    <td>{band['Share']:.1f}</td>
                    <td>{band['Time']:.1f}</td>
                    <td>{band['Avg']:,.0f}</td>
                    <td>{band['Revenue']:,.0f}</td>
                </tr>
                '''

            # Total row
            html += f'''
                <tr class="total-row">
                    <td></td>
                    <td>Total</td>
                    <td>{total_contr:.1f}</td>
                    <td>{total_share:.1f}</td>
                    <td>{total_time:.1f}</td>
                    <td>{total_contr:,.0f}</td>
                    <td>{total_revenue:,.0f}</td>
                </tr>
            '''

        html += '''
            </tbody>
        </table>
        '''
        return html

    def create_price_band_butterfly_chart(self, bands_df, selected_regions, date_range_text):
        """Create butterfly chart showing Time % (left) vs Contribution $/MWh (right)

        Args:
            bands_df: DataFrame with columns: Region, Price Band, Contribution, Percentage, Band Average
            selected_regions: list of regions to display
            date_range_text: string describing the date range

        Returns:
            matplotlib figure
        """
        import matplotlib.pyplot as plt
        import numpy as np

        # Band colors matching Flexoki theme
        BAND_COLORS = {
            'Below $0': FLEXOKI_ACCENT['red'],
            '$0-$300': FLEXOKI_ACCENT['green'],
            '$301-$1000': FLEXOKI_ACCENT['orange'],
            'Above $1000': FLEXOKI_ACCENT['magenta'],
        }

        band_order = ['Below $0', '$0-$300', '$301-$1000', 'Above $1000']

        # Filter to selected regions only
        regions = [r for r in selected_regions if r in bands_df['Region'].unique()]

        if len(regions) == 0:
            # Return empty figure with message
            fig, ax = plt.subplots(figsize=(10, 6))
            fig.patch.set_facecolor(FLEXOKI_PAPER)
            ax.set_facecolor(FLEXOKI_PAPER)
            ax.text(0.5, 0.5, 'No data available', ha='center', va='center',
                   fontsize=14, color=FLEXOKI_BASE[600])
            ax.axis('off')
            return fig

        # Calculate contribution % for each band (contribution / mean_price * 100)
        # Mean price per region is the sum of contributions
        region_means = bands_df.groupby('Region')['Contribution'].sum().to_dict()

        # Add Contribution % column
        bands_df = bands_df.copy()
        bands_df['Contribution %'] = bands_df.apply(
            lambda row: (row['Contribution'] / region_means[row['Region']] * 100)
                        if region_means[row['Region']] > 0 else 0,
            axis=1
        )

        # Determine grid layout based on number of regions
        n_regions = len(regions)
        if n_regions == 1:
            fig, axes = plt.subplots(1, 1, figsize=(10, 5))
            axes = [axes]
        elif n_regions == 2:
            fig, axes = plt.subplots(1, 2, figsize=(12, 5))
            axes = axes.flat
        elif n_regions <= 4:
            fig, axes = plt.subplots(2, 2, figsize=(12, 8))
            axes = axes.flat
        else:
            # 5 regions - use 2x3 grid
            fig, axes = plt.subplots(2, 3, figsize=(14, 8))
            axes = axes.flat

        fig.patch.set_facecolor(FLEXOKI_PAPER)

        for idx, region in enumerate(regions):
            ax = axes[idx]
            ax.set_facecolor(FLEXOKI_PAPER)

            # Remove spines
            for spine in ax.spines.values():
                spine.set_visible(False)

            region_df = bands_df[bands_df['Region'] == region]
            y_positions = np.arange(len(band_order))
            bar_height = 0.65

            # Get flat load avg for this region
            flat_load_avg = region_means.get(region, 0)

            for i, band in enumerate(band_order):
                band_row = region_df[region_df['Price Band'] == band]
                if len(band_row) == 0:
                    continue

                time_pct = band_row['Percentage'].values[0]
                contrib_pct = band_row['Contribution %'].values[0]
                contrib_dollars = band_row['Contribution'].values[0]
                color = BAND_COLORS[band]

                # Time % bar extends LEFT (negative direction) - lighter shade
                ax.barh(i, -time_pct, height=bar_height, color=color, alpha=0.5,
                        edgecolor='white', linewidth=0.5)

                # Contribution % bar extends RIGHT (positive direction) - full color
                ax.barh(i, contrib_pct, height=bar_height, color=color,
                        edgecolor='white', linewidth=0.5)

                # Label for Time % (outside left bar)
                ax.text(-time_pct - 1, i, f'{time_pct:.1f}%',
                        ha='right', va='center', fontsize=9, color=FLEXOKI_BASE[800])

                # $/MWh CONTRIBUTION label INSIDE the bar
                bar_width = abs(contrib_pct)
                if bar_width > 12:  # Wide enough for text inside
                    text_x = contrib_pct / 2 if contrib_pct > 0 else contrib_pct / 2
                    ax.text(text_x, i, f'${contrib_dollars:.0f}',
                            ha='center', va='center', fontsize=10, fontweight='bold',
                            color='white')
                elif bar_width > 5:  # Medium bar
                    text_x = contrib_pct * 0.6 if contrib_pct > 0 else contrib_pct * 0.6
                    ax.text(text_x, i, f'${contrib_dollars:.0f}',
                            ha='center', va='center', fontsize=9, fontweight='bold',
                            color='white')
                elif abs(contrib_dollars) > 1:  # Small bar but significant $
                    # Put label just outside the bar
                    if contrib_pct >= 0:
                        ax.text(contrib_pct + 8, i, f'${contrib_dollars:.0f}',
                                ha='left', va='center', fontsize=8, color=color)
                    else:
                        ax.text(contrib_pct - 2, i, f'${contrib_dollars:.0f}',
                                ha='right', va='center', fontsize=8, color=color)

            # Center line
            ax.axvline(x=0, color=FLEXOKI_BASE[300], linewidth=1.5, zorder=0)

            # Formatting
            ax.set_yticks(y_positions)
            ax.set_yticklabels(band_order, fontsize=10, color=FLEXOKI_BLACK)
            ax.tick_params(axis='x', colors=FLEXOKI_BASE[600], length=0)
            ax.tick_params(axis='y', length=0)

            # Title with flat load avg
            ax.set_title(f'{region}  (Flat load: ${flat_load_avg:.0f}/MWh)',
                         fontsize=12, fontweight='bold', color=FLEXOKI_BLACK, pad=8)

            # X-axis labels
            ax.set_xlabel('<-- Time %                    Contribution $/MWh -->',
                         fontsize=9, color=FLEXOKI_BASE[800])

            # X-axis grid
            ax.xaxis.grid(True, color=FLEXOKI_BASE[100], linewidth=0.5, alpha=0.5)
            ax.set_axisbelow(True)

            # Set symmetric x limits
            max_val = max(100,
                         region_df['Percentage'].max() if len(region_df) > 0 else 100,
                         abs(region_df['Contribution %']).max() if len(region_df) > 0 else 100)
            ax.set_xlim(-max_val - 12, max_val + 12)

            ax.invert_yaxis()  # Put first band at top

        # Hide unused axes
        for idx in range(len(regions), len(axes)):
            axes[idx].set_visible(False)

        # Main title
        fig.suptitle(f'Price Band Contribution ({date_range_text})',
                     fontsize=14, fontweight='bold', color=FLEXOKI_BLACK, y=0.98)

        # Subtitle
        fig.text(0.5, 0.93,
                 'Left = % of time  |  Right = $/MWh contribution to flat load average',
                 ha='center', fontsize=10, color=FLEXOKI_BASE[600])

        # Attribution
        fig.text(0.99, 0.01, "Data: AEMO", ha='right', va='bottom',
                 color=FLEXOKI_BASE[600], fontsize=9, style='italic')

        plt.tight_layout()
        plt.subplots_adjust(top=0.88, bottom=0.08, hspace=0.35)

        return fig

    def get_cache_stats_display(self):
        """Get cache statistics for display"""
        global _cache_stats
        total = _cache_stats['hits'] + _cache_stats['misses']
        hit_rate = (_cache_stats['hits'] / total * 100) if total > 0 else 0
        
        return f"Cache: {'ON' if ENABLE_PN_CACHE else 'OFF'} | Hits: {_cache_stats['hits']} | Misses: {_cache_stats['misses']} | Rate: {hit_rate:.1f}%"
    
    

    def create_plot(self):
        """Create Plotly visualization with generation stack and price chart"""
        try:
            # Load fresh data
            self.load_generation_data()
            data = self.process_data_for_region()

            if data.empty:
                return pn.pane.Markdown("**No data available**",
                    styles={'background': FLEXOKI_PAPER, 'padding': '20px'})

            # Normalize fuel types (combine Gas types, rename Battery Storage, etc.)
            data = self.normalize_fuel_types(data)

            # Get colors and time range
            fuel_colors = self.get_fuel_colors()
            time_range_display = self._get_time_range_display()

            # Load price and transmission data
            price_df = self.load_price_data()

            # Get transmission flows for this region
            if self.transmission_df is None:
                self.load_transmission_data()
            trans_result = self.calculate_regional_transmission_flows() if self.transmission_df is not None else (None, None)
            # Unpack tuple: (net_flows_df, line_data_df)
            trans_data = trans_result[0] if isinstance(trans_result, tuple) else trans_result

            # Create Plotly subplots
            fig = make_subplots(
                rows=2, cols=1,
                shared_xaxes=True,
                vertical_spacing=0.08,
                row_heights=[0.65, 0.35],
                subplot_titles=(
                    f'Generation by Fuel - {self.region} ({time_range_display})',
                    'Price'
                )
            )

            # Check for battery column
            battery_col = 'Battery' if 'Battery' in data.columns else None

            # Prepare transmission data
            has_imports = False
            has_exports = False
            imports = None
            exports = None
            if trans_data is not None and isinstance(trans_data, pd.DataFrame) and not trans_data.empty:
                # Column is 'net_transmission_mw' from calculate_regional_transmission_flows
                if 'net_transmission_mw' in trans_data.columns:
                    trans_indexed = trans_data.set_index('settlementdate')['net_transmission_mw']
                elif 'net_flow' in trans_data.columns:
                    trans_indexed = trans_data.set_index('settlementdate')['net_flow']
                else:
                    trans_indexed = None

                if trans_indexed is not None:
                    trans_aligned = trans_indexed.reindex(data.index, fill_value=0)
                    imports = trans_aligned.clip(lower=0)
                    exports = trans_aligned.clip(upper=0)
                    has_imports = (imports > 0).any()
                    has_exports = (exports < 0).any()

            # === POSITIVE STACKGROUP ===
            transmission_in_legend = False

            # Transmission imports first (closest to x-axis)
            if has_imports:
                fig.add_trace(go.Scatter(
                    x=data.index, y=imports,
                    name='Transmission', legendgroup='Transmission',
                    mode='lines', stackgroup='positive',
                    fillcolor=fuel_colors.get('Transmission', FLEXOKI_ACCENT['magenta']),
                    line=dict(width=0.5, color=fuel_colors.get('Transmission'))
                ), row=1, col=1)
                transmission_in_legend = True

            # Merit order for positive stack
            merit_order = ['Coal', 'Wind', 'Solar', 'Rooftop Solar', 'Battery', 'Hydro', 'Gas', 'Biomass', 'Other']

            # Core fuels always shown
            CORE_FUELS = {'Coal', 'Gas', 'Solar', 'Wind', 'Hydro', 'Battery'}
            total_positive = data.clip(lower=0).sum().sum()

            for fuel in merit_order:
                if fuel not in data.columns:
                    continue

                # Skip insignificant minor fuels
                if fuel not in CORE_FUELS:
                    fuel_total = data[fuel].clip(lower=0).sum()
                    if total_positive > 0 and fuel_total / total_positive < 0.01:
                        continue

                color = fuel_colors.get(fuel, '#888888')

                if fuel == battery_col:
                    values = data[fuel].clip(lower=0)
                    if (values == 0).all():
                        continue
                else:
                    values = data[fuel].clip(lower=0)

                fig.add_trace(go.Scatter(
                    x=data.index, y=values,
                    name=fuel, mode='lines', stackgroup='positive',
                    fillcolor=color, line=dict(width=0.5, color=color)
                ), row=1, col=1)

            # === NEGATIVE STACKGROUP ===
            # Transmission exports first
            if has_exports:
                fig.add_trace(go.Scatter(
                    x=data.index, y=exports,
                    name='Transmission', legendgroup='Transmission',
                    showlegend=not transmission_in_legend,
                    mode='lines', stackgroup='negative',
                    fillcolor=fuel_colors.get('Transmission', FLEXOKI_ACCENT['magenta']),
                    line=dict(width=0.5, color=fuel_colors.get('Transmission'))
                ), row=1, col=1)

            # Battery charging
            if battery_col and battery_col in data.columns:
                charging = data[battery_col].clip(upper=0)
                if (charging < 0).any():
                    fig.add_trace(go.Scatter(
                        x=data.index, y=charging,
                        name='Battery', legendgroup='Battery', showlegend=False,
                        mode='lines', stackgroup='negative',
                        fillcolor=fuel_colors.get('Battery', FLEXOKI_ACCENT['purple']),
                        line=dict(width=0.5, color=fuel_colors.get('Battery'))
                    ), row=1, col=1)

            # === PRICE LINE (Row 2) with asinh scale ===
            if not price_df.empty:
                price_col = 'RRP' if 'RRP' in price_df.columns else 'rrp'
                price_series = price_df.set_index('settlementdate')[price_col]
                price_aligned = price_series.reindex(data.index, method='nearest')

                PRICE_SCALE = 100
                price_transformed = np.arcsinh(price_aligned / PRICE_SCALE)

                fig.add_trace(go.Scatter(
                    x=data.index, y=price_transformed,
                    name='Price', mode='lines', showlegend=False,
                    line=dict(width=1.5, color=FLEXOKI_ACCENT['blue'])
                ), row=2, col=1)

                # Custom tick values for asinh scale
                price_min, price_max = price_aligned.min(), price_aligned.max()
                tick_values_raw = [-1000, -100, 0, 100, 300, 1000, 3000, 10000, 20000]
                tick_values_raw = [t for t in tick_values_raw if price_min - 100 <= t <= price_max + 1000]
                if not tick_values_raw:
                    tick_values_raw = [0, 100, 1000]
                tick_vals = [np.arcsinh(t / PRICE_SCALE) for t in tick_values_raw]
                tick_text = [f'${t:,.0f}' if t >= 0 else f'-${abs(t):,.0f}' for t in tick_values_raw]

                fig.update_yaxes(
                    tickmode='array', tickvals=tick_vals, ticktext=tick_text,
                    row=2, col=1
                )

            # Layout
            fig.update_layout(
                paper_bgcolor=FLEXOKI_PAPER,
                plot_bgcolor=FLEXOKI_PAPER,
                height=700,
                legend=dict(
                    orientation='v', yanchor='top', y=1, xanchor='left', x=1.02,
                    bgcolor=FLEXOKI_PAPER, font=dict(size=10)
                ),
                margin=dict(r=150, t=50)
            )

            fig.update_annotations(font=dict(size=12, color=FLEXOKI_BLACK))

            fig.update_yaxes(
                title_text='MW', showgrid=False, zeroline=True,
                zerolinecolor=FLEXOKI_BASE[600], zerolinewidth=1,
                tickfont=dict(color=FLEXOKI_BASE[800]),
                row=1, col=1
            )

            fig.update_yaxes(
                title_text='$/MWh', showgrid=False, zeroline=True,
                zerolinecolor=FLEXOKI_BASE[600], zerolinewidth=0.5,
                tickfont=dict(color=FLEXOKI_ACCENT['blue']),
                title_font=dict(color=FLEXOKI_ACCENT['blue']),
                row=2, col=1
            )

            fig.update_xaxes(showgrid=False, tickfont=dict(color=FLEXOKI_BASE[800]))

            # Create chart pane
            chart_pane = pn.pane.Plotly(fig, sizing_mode='stretch_width')

            # Calculate statistics and create table
            stats_df, summary = self.calculate_generation_statistics(data, price_df, trans_data)
            table_pane = self.create_stats_table_html(stats_df, summary)

            self.last_update = datetime.now()
            logger.info(f"Plotly plot updated for {self.region}, {self.time_range}")

            return pn.Column(chart_pane, table_pane, sizing_mode='stretch_width')

        except Exception as e:
            logger.error(f"Error creating plot: {e}")
            import traceback
            traceback.print_exc()
            return pn.pane.Markdown(f"**Error creating plot:** {str(e)}",
                styles={'background': FLEXOKI_PAPER, 'color': 'red', 'padding': '20px'})
    
    def create_transmission_plot(self):
        """Create transmission flow line chart with limit areas showing unused capacity"""
        try:
            import holoviews as hv
            
            # Skip transmission chart for NEM (no specific region)
            if self.region == 'NEM':
                return hv.Text(0.5, 0.5, 'Transmission flows not available for NEM view\nSelect a specific region').opts(
                    xlim=(0, 1),
                    ylim=(0, 1),
                    bgcolor=FLEXOKI_PAPER,
                    width=1200,
                    height=300,
                    color=FLEXOKI_BLACK,
                    fontsize=14
                )
            
            # Load transmission data if needed
            if self.transmission_df is None:
                self.load_transmission_data()
            
            # Get the raw transmission data with limits
            if self.transmission_df is None or self.transmission_df.empty:
                return hv.Text(0.5, 0.5, f'No transmission data for {self.region}').opts(
                    xlim=(0, 1),
                    ylim=(0, 1),
                    bgcolor=FLEXOKI_PAPER,
                    width=1200,
                    height=300,
                    color=FLEXOKI_BLACK,
                    fontsize=14
                )
            
            # Define interconnector mapping for each region
            interconnector_mapping = {
                'NSW1': {
                    'NSW1-QLD1': 'from_nsw',      # Positive = export to QLD
                    'VIC1-NSW1': 'to_nsw',        # Positive = import from VIC  
                    'N-Q-MNSP1': 'from_nsw'       # DirectLink: Positive = export to QLD
                },
                'QLD1': {
                    'NSW1-QLD1': 'to_qld',        # Positive = import from NSW
                    'N-Q-MNSP1': 'to_qld'         # DirectLink: Positive = import from NSW
                },
                'VIC1': {
                    'VIC1-NSW1': 'from_vic',      # Positive = export to NSW
                    'V-SA': 'from_vic',           # Positive = export to SA
                    'V-S-MNSP1': 'from_vic',      # Murraylink: Positive = export to SA
                    'T-V-MNSP1': 'to_vic'         # Basslink: Positive = import from TAS
                },
                'SA1': {
                    'V-SA': 'to_sa',              # Positive = import from VIC
                    'V-S-MNSP1': 'to_sa'          # Murraylink: Positive = import from VIC
                },
                'TAS1': {
                    'T-V-MNSP1': 'from_tas'       # Basslink: Positive = export to VIC
                }
            }
            
            region_interconnectors = interconnector_mapping.get(self.region, {})
            if not region_interconnectors:
                return hv.Text(0.5, 0.5, f'No transmission lines for {self.region}').opts(
                    xlim=(0, 1),
                    ylim=(0, 1),
                    bgcolor=FLEXOKI_PAPER,
                    width=1200,
                    height=300,
                    color=FLEXOKI_BLACK,
                    fontsize=14
                )
            
            # Filter transmission data for this region's interconnectors
            region_transmission = self.transmission_df[
                self.transmission_df['interconnectorid'].isin(region_interconnectors.keys())
            ].copy()
            
            # Debug logging
            logger.info(f"=== Transmission Plot Debug for {self.region} ===")
            logger.info(f"Total transmission records: {len(self.transmission_df)}")
            logger.info(f"Region interconnectors: {list(region_interconnectors.keys())}")
            logger.info(f"Filtered transmission records: {len(region_transmission)}")
            if not region_transmission.empty:
                logger.info(f"Date range: {region_transmission['settlementdate'].min()} to {region_transmission['settlementdate'].max()}")
                logger.info(f"Unique interconnectors in data: {region_transmission['interconnectorid'].unique()}")
                logger.info(f"Sample data (first 5 rows):")
                # Only log columns that exist in the data
                cols_to_log = ['settlementdate', 'interconnectorid', 'meteredmwflow']
                if 'exportlimit' in region_transmission.columns:
                    cols_to_log.append('exportlimit')
                if 'importlimit' in region_transmission.columns:
                    cols_to_log.append('importlimit')
                logger.info(region_transmission[cols_to_log].head())
            
            if region_transmission.empty:
                return hv.Text(0.5, 0.5, f'No transmission data for {self.region}').opts(
                    xlim=(0, 1),
                    ylim=(0, 1),
                    bgcolor=FLEXOKI_PAPER,
                    width=1200,
                    height=300,
                    color=FLEXOKI_BLACK,
                    fontsize=14
                )
            
            # Apply flow direction corrections and get limits
            def process_flow_and_limits(row):
                interconnector = row['interconnectorid']
                flow_type = region_interconnectors[interconnector]
                meteredmwflow = row['meteredmwflow']  # Original AEMO flow
                # Handle missing columns in 30-minute data
                import_limit = row.get('importlimit', np.nan)
                export_limit = row.get('exportlimit', np.nan)
                
                # Convert to regional perspective
                if flow_type.startswith('to_'):
                    # This interconnector brings power TO our region (import)
                    regional_flow = meteredmwflow  # Positive = import to our region
                else:
                    # This interconnector takes power FROM our region (export)  
                    regional_flow = -meteredmwflow  # Negative = export from our region
                
                # Determine applicable limit based on regional flow direction
                # The logic: always show the limit in the same direction as the actual flow
                # For positive regional flow (import): show positive limit (max import capacity)
                # For negative regional flow (export): show negative limit (max export capacity)
                
                if regional_flow >= 0:
                    # Importing to our region - use positive limit
                    if flow_type.startswith('to_'):
                        # This interconnector normally brings power TO our region
                        applicable_limit = export_limit if meteredmwflow >= 0 else import_limit
                    else:
                        # This interconnector normally takes power FROM our region, but flow is reversed
                        applicable_limit = import_limit if meteredmwflow < 0 else export_limit
                else:
                    # Exporting from our region - use negative limit
                    if flow_type.startswith('to_'):
                        # This interconnector normally brings power TO our region, but flow is reversed
                        applicable_limit = -(import_limit if meteredmwflow < 0 else export_limit)
                    else:
                        # This interconnector normally takes power FROM our region
                        applicable_limit = -(export_limit if meteredmwflow >= 0 else import_limit)
                
                return regional_flow, applicable_limit
            
            # Process flows and limits
            region_transmission[['regional_flow', 'applicable_limit']] = region_transmission.apply(
                lambda row: pd.Series(process_flow_and_limits(row)), axis=1
            )
            
            # Debug processed data
            logger.info(f"=== After Processing ===")
            logger.info(f"Processed data shape: {region_transmission.shape}")
            if not region_transmission.empty:
                logger.info(f"Regional flow range: {region_transmission['regional_flow'].min():.1f} to {region_transmission['regional_flow'].max():.1f}")
                logger.info(f"Sample processed data (first 5 rows):")
                logger.info(region_transmission[['settlementdate', 'interconnectorid', 'regional_flow', 'applicable_limit']].head())
            
            # Create visualizations for each interconnector separately
            plot_elements = []
            
            # Define colors for different interconnectors
            interconnector_colors = {
                'NSW1-QLD1': '#ff6b6b',    # Red
                'VIC1-NSW1': '#4ecdc4',    # Cyan
                'V-SA': '#45b7d1',         # Light Blue
                'T-V-MNSP1': '#96ceb4',    # Light Green
                'N-Q-MNSP1': '#ffd93d',    # Yellow
                'V-S-MNSP1': '#dda0dd'     # Plum
            }
            
            # Create plots for each interconnector
            for interconnector in region_interconnectors.keys():
                ic_data = region_transmission[region_transmission['interconnectorid'] == interconnector].copy()
                
                if ic_data.empty:
                    logger.info(f"No data for interconnector {interconnector}")
                    continue
                
                logger.info(f"=== Creating plot for {interconnector} ===")
                logger.info(f"Data points: {len(ic_data)}")
                
                # Sort by time
                ic_data = ic_data.sort_values('settlementdate')
                
                # Get color for this interconnector
                color = interconnector_colors.get(interconnector, '#ffb6c1')
                
                # Use the applicable limit directly (no filtering)
                ic_data['dynamic_limit'] = ic_data['applicable_limit']
                
                # Log limit info for debugging
                if not ic_data.empty:
                    avg_flow = ic_data['regional_flow'].mean()
                    avg_limit = ic_data['applicable_limit'].abs().mean()
                    logger.info(f"Interconnector {interconnector}: avg flow={avg_flow:.1f}MW, avg limit={avg_limit:.1f}MW")
                
                # Prepare data for filled area and hover
                area_data = []
                hover_data = []
                
                for _, row in ic_data.iterrows():
                    time = row['settlementdate']
                    flow = row['regional_flow']
                    limit = row['dynamic_limit']
                    
                    # Calculate percentage of limit used
                    if limit != 0:
                        percent_of_limit = abs(flow / limit) * 100
                    else:
                        percent_of_limit = 0
                    
                    # Create area from flow to limit (showing unused capacity)
                    if flow >= 0 and limit >= 0:  # Import scenario
                        area_data.append((time, flow, limit))
                    elif flow < 0 and limit < 0:  # Export scenario  
                        area_data.append((time, flow, limit))
                    else:
                        # No area when flow and limit have different signs
                        area_data.append((time, flow, flow))
                    
                    # Store hover data
                    hover_data.append({
                        'settlementdate': time,
                        'flow': flow,
                        'limit': limit,
                        'percent': percent_of_limit,
                        'direction': 'Import' if flow >= 0 else 'Export',
                        'interconnector': interconnector
                    })
                
                if not area_data:
                    continue
                
                area_df = pd.DataFrame(area_data, columns=['settlementdate', 'flow', 'limit'])
                hover_df = pd.DataFrame(hover_data)
                
                # Create the filled area for this interconnector
                filled_area = area_df.hvplot.area(
                    x='settlementdate',
                    y='flow',
                    y2='limit',
                    alpha=0.3,
                    color=color,
                    hover=False,
                    label=f'{interconnector} unused capacity'
                ).opts(
                    hooks=[self._get_datetime_formatter_hook(), self._get_flexoki_background_hook()]
                )

                # Create the main flow line with enhanced tooltips
                hover_df['capacity_status'] = hover_df['percent'].apply(
                    lambda x: 'At Capacity (>=95%)' if x >= 95 else
                             'High Utilization (>=80%)' if x >= 80 else
                             'Normal Operation'
                )

                flow_line = hover_df.hvplot.line(
                    x='settlementdate',
                    y='flow',
                    color=color,
                    line_width=3,
                    alpha=1.0,
                    label=interconnector,
                    hover_cols=['limit', 'percent', 'direction', 'interconnector', 'capacity_status'],
                    hover_tooltips=[
                        ('Interconnector', '@interconnector'),
                        ('Time', '@settlementdate{%F %H:%M}'),
                        ('Flow', '@flow{0.0f} MW'),
                        ('Limit', '@limit{0.0f} MW'),
                        ('Utilization', '@percent{0.1f}%'),
                        ('Status', '@capacity_status'),
                        ('Direction', '@direction')
                    ],
                    hover_formatters={'@settlementdate': 'datetime'}
                ).opts(
                    hooks=[self._get_datetime_formatter_hook(), self._get_flexoki_background_hook()]
                )

                plot_elements.extend([filled_area, flow_line])
            
            # Add horizontal line at y=0
            zero_line = hv.HLine(0).opts(
                color=FLEXOKI_BASE[600],  # Use comment gray for subtle grid line
                alpha=0.5,
                line_width=1,
                line_dash='dashed'
            )
            
            # Combine all elements
            if plot_elements:
                time_range_display = self._get_time_range_display()
                
                # Create a more robust datetime formatter hook
                def transmission_formatter_hook(plot, element):
                    # Try different ways to access the x-axis
                    xaxis = None
                    if hasattr(plot, 'handles') and 'xaxis' in plot.handles:
                        xaxis = plot.handles['xaxis']
                    elif hasattr(plot, 'state'):
                        # For composite plots, might need to access differently
                        try:
                            xaxis = plot.state.below[0]  # Bokeh puts x-axis in 'below'
                        except:
                            pass
                    
                    if xaxis:
                        if self.time_range == '1':
                            xaxis.formatter = DatetimeTickFormatter(hours="%H:%M", days="%H:%M")
                        elif self.time_range == '7':
                            xaxis.formatter = DatetimeTickFormatter(hours="%a %H:%M", days="%a %d", months="%b %d")
                        else:
                            xaxis.formatter = DatetimeTickFormatter(hours="%m/%d", days="%m/%d", months="%b %d", years="%Y")
                
                # Calculate the actual x-axis range from the data
                if region_transmission.empty:
                    x_range = None
                else:
                    x_min = region_transmission['settlementdate'].min()
                    x_max = region_transmission['settlementdate'].max()
                    # Add small padding (1% of range) - convert to pandas Timedelta
                    time_diff = x_max - x_min
                    padding = pd.Timedelta(seconds=time_diff.total_seconds() * 0.01)
                    x_range = (x_min - padding, x_max + padding)
                
                combined_plot = hv.Overlay(plot_elements + [zero_line]).opts(
                    width=1200,
                    height=400,  # Increased height to accommodate multiple lines
                    bgcolor=FLEXOKI_PAPER,
                    ylabel='Flow (MW)',
                    xlabel='Time',
                    title=f'Transmission Flows with Limits - {self.region} ({time_range_display})',
                    show_grid=False,
                    legend_position='right',
                    framewise=True,  # Force complete recomputation on updates
                    xlim=x_range,  # Explicitly set x-axis range
                    apply_ranges=False,  # Prevent automatic range determination
                    hooks=[self._get_datetime_formatter_hook(), self._get_flexoki_background_hook()]
                )
            else:
                combined_plot = hv.Text(0.5, 0.5, f'No transmission data available for {self.region}').opts(
                    xlim=(0, 1),
                    ylim=(0, 1),
                    bgcolor=FLEXOKI_PAPER,
                    width=1200,
                    height=400,
                    color=FLEXOKI_BLACK,
                    fontsize=14
                )
            
            return combined_plot
            
        except Exception as e:
            logger.error(f"Error creating transmission plot: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return hv.Text(0.5, 0.5, 'Error loading transmission data').opts(
                xlim=(0, 1),
                ylim=(0, 1),
                bgcolor=FLEXOKI_PAPER,
                width=1200,
                height=300,
                color=FLEXOKI_BLACK,
                fontsize=12
            )

    def create_generation_tod_plot(self):
        """Create time-of-day generation profile with average price overlay using Plotly"""
        try:
            # Load fresh data - following same pattern as create_plot()
            self.load_generation_data()
            data = self.process_data_for_region()

            if data.empty:
                return pn.pane.Markdown("**No generation data available**",
                    styles={'background': FLEXOKI_PAPER, 'padding': '20px'})

            # Normalize fuel types (combine Gas types, rename Battery Storage, etc.)
            data = self.normalize_fuel_types(data)

            # Prepare data for time-of-day analysis
            df = data.copy()

            # Extract hour (0-23) from the datetime index
            df['hour'] = df.index.hour

            # Group by hour and calculate mean for each fuel type
            hourly_avg = df.groupby('hour').mean()

            # Drop the 'hour' column if it was included in the groupby
            if 'hour' in hourly_avg.columns:
                hourly_avg = hourly_avg.drop(columns=['hour'])

            # Get fuel colors and time range
            fuel_colors = self.get_fuel_colors()
            time_range_display = self._get_time_range_display()

            # Load and process price data for TOD
            hourly_price = None
            try:
                start_datetime, end_datetime = self._get_effective_date_range()
                price_file = config.spot_hist_file
                if os.path.exists(price_file):
                    price_df = pd.read_parquet(price_file)
                    price_df['settlementdate'] = pd.to_datetime(price_df['settlementdate'])
                    price_df = price_df.set_index('settlementdate')

                    if start_datetime is not None and end_datetime is not None:
                        price_df = price_df[(price_df.index >= start_datetime) & (price_df.index <= end_datetime)]

                    if self.region != 'NEM' and 'regionid' in price_df.columns:
                        price_df = price_df[price_df['regionid'] == self.region]

                    if not price_df.empty and 'rrp' in price_df.columns:
                        price_df['hour'] = price_df.index.hour
                        hourly_price = price_df.groupby('hour')['rrp'].mean()
            except Exception as e:
                logger.warning(f"Could not load price data for TOD plot: {e}")

            # Create Plotly subplots
            fig = make_subplots(
                rows=2, cols=1,
                shared_xaxes=True,
                vertical_spacing=0.08,
                row_heights=[0.65, 0.35],
                subplot_titles=(
                    f'Average Generation by Time of Day - {self.region} ({time_range_display})',
                    'Average Price'
                )
            )

            # Check for battery column
            battery_col = 'Battery' if 'Battery' in hourly_avg.columns else None

            # Merit order for positive stack
            merit_order = ['Coal', 'Wind', 'Solar', 'Rooftop Solar', 'Battery', 'Hydro', 'Gas', 'Biomass', 'Other']

            # Core fuels always shown
            CORE_FUELS = {'Coal', 'Gas', 'Solar', 'Wind', 'Hydro', 'Battery'}
            total_positive = hourly_avg.clip(lower=0).sum().sum()

            # Hours for x-axis
            hours = list(range(24))

            # === POSITIVE STACKGROUP ===
            for fuel in merit_order:
                if fuel not in hourly_avg.columns:
                    continue

                # Skip insignificant minor fuels
                if fuel not in CORE_FUELS:
                    fuel_total = hourly_avg[fuel].clip(lower=0).sum()
                    if total_positive > 0 and fuel_total / total_positive < 0.01:
                        continue

                color = fuel_colors.get(fuel, '#888888')

                if fuel == battery_col:
                    values = hourly_avg[fuel].clip(lower=0)
                    if (values == 0).all():
                        continue
                else:
                    values = hourly_avg[fuel].clip(lower=0)

                fig.add_trace(go.Scatter(
                    x=hours, y=values.reindex(hours, fill_value=0),
                    name=fuel, mode='lines', stackgroup='positive',
                    fillcolor=color, line=dict(width=0.5, color=color)
                ), row=1, col=1)

            # === NEGATIVE STACKGROUP (Battery charging) ===
            if battery_col and battery_col in hourly_avg.columns:
                charging = hourly_avg[battery_col].clip(upper=0)
                if (charging < 0).any():
                    fig.add_trace(go.Scatter(
                        x=hours, y=charging.reindex(hours, fill_value=0),
                        name='Battery', legendgroup='Battery', showlegend=False,
                        mode='lines', stackgroup='negative',
                        fillcolor=fuel_colors.get('Battery', FLEXOKI_ACCENT['purple']),
                        line=dict(width=0.5, color=fuel_colors.get('Battery'))
                    ), row=1, col=1)

            # === PRICE LINE (Row 2) with asinh scale ===
            if hourly_price is not None and not hourly_price.empty:
                PRICE_SCALE = 100
                price_transformed = np.arcsinh(hourly_price / PRICE_SCALE)

                fig.add_trace(go.Scatter(
                    x=hours, y=price_transformed.reindex(hours, fill_value=0),
                    name='Price', mode='lines', showlegend=False,
                    line=dict(width=2, color=FLEXOKI_ACCENT['yellow'])
                ), row=2, col=1)

                # Custom tick values for asinh scale
                price_min, price_max = hourly_price.min(), hourly_price.max()
                tick_values_raw = [-100, 0, 50, 100, 200, 300, 500, 1000, 2000]
                tick_values_raw = [t for t in tick_values_raw if price_min - 50 <= t <= price_max + 200]
                if not tick_values_raw:
                    tick_values_raw = [0, 100, 500]
                tick_vals = [np.arcsinh(t / PRICE_SCALE) for t in tick_values_raw]
                tick_text = [f'${t:,.0f}' if t >= 0 else f'-${abs(t):,.0f}' for t in tick_values_raw]

                fig.update_yaxes(
                    tickmode='array', tickvals=tick_vals, ticktext=tick_text,
                    row=2, col=1
                )

            # Layout
            fig.update_layout(
                paper_bgcolor=FLEXOKI_PAPER,
                plot_bgcolor=FLEXOKI_PAPER,
                height=700,
                legend=dict(
                    orientation='v', yanchor='top', y=1, xanchor='left', x=1.02,
                    bgcolor=FLEXOKI_PAPER, font=dict(size=10)
                ),
                margin=dict(r=150, t=50)
            )

            fig.update_annotations(font=dict(size=12, color=FLEXOKI_BLACK))

            # Y-axis for generation (row 1)
            fig.update_yaxes(
                title_text='Average MW', showgrid=False, zeroline=True,
                zerolinecolor=FLEXOKI_BASE[600], zerolinewidth=1,
                tickfont=dict(color=FLEXOKI_BASE[800]),
                row=1, col=1
            )

            # Y-axis for price (row 2)
            fig.update_yaxes(
                title_text='$/MWh', showgrid=False, zeroline=True,
                zerolinecolor=FLEXOKI_BASE[600], zerolinewidth=0.5,
                tickfont=dict(color=FLEXOKI_ACCENT['yellow']),
                title_font=dict(color=FLEXOKI_ACCENT['yellow']),
                row=2, col=1
            )

            # X-axis with hour ticks
            fig.update_xaxes(
                showgrid=False, tickfont=dict(color=FLEXOKI_BASE[800]),
                tickmode='array', tickvals=list(range(0, 24, 2)),
                ticktext=[f'{h:02d}:00' for h in range(0, 24, 2)],
                title_text='Hour of Day',
                row=2, col=1
            )
            fig.update_xaxes(showgrid=False, row=1, col=1)

            logger.info(f"TOD Plotly plot created for {self.region}, {self.time_range}")

            return pn.pane.Plotly(fig, sizing_mode='stretch_width')

        except Exception as e:
            logger.error(f"Error creating TOD plot: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return pn.pane.Markdown(f"**Error creating TOD plot:** {str(e)}",
                styles={'background': FLEXOKI_PAPER, 'color': 'red', 'padding': '20px'})

    def create_utilization_plot(self):
        """Create capacity utilization line chart with proper document handling"""
        try:
            utilization_data = self.calculate_capacity_utilization()
            
            if utilization_data.empty:
                # Create empty plot with message
                empty_plot = hv.Text(0.5, 0.5, 'No utilization data available').opts(
                    xlim=(0, 1),
                    ylim=(0, 1),
                    bgcolor=FLEXOKI_PAPER,
                    width=1200,
                    height=400,
                    color=FLEXOKI_BLACK,
                    fontsize=14
                )
                return empty_plot
            
            # Get colors (same as generation chart for consistency)
            fuel_colors = self.get_fuel_colors()
            
            # Reset index to make time a column
            plot_data = utilization_data.reset_index()
            
            # Get available fuel types
            fuel_types = [col for col in utilization_data.columns if col in fuel_colors]
            
            if not fuel_types:
                return hv.Text(0.5, 0.5, 'No fuel data for utilization chart').opts(
                    bgcolor=FLEXOKI_PAPER,
                    color=FLEXOKI_BLACK,
                    fontsize=14
                )
            
            # Create line plot for capacity utilization with different Y dimension name
            time_range_display = self._get_time_range_display()
            line_plot = plot_data.hvplot.line(
                x='settlementdate',
                y=fuel_types,
                width=1200,
                height=400,
                title=f'Capacity Utilization by Fuel Type - {self.region} ({time_range_display}) | data:AEMO, design ITK',
                ylabel='Capacity Utilization (%)',
                xlabel='Time',
                grid=False,
                legend='right',
                bgcolor=FLEXOKI_PAPER,
                color=[fuel_colors.get(fuel, '#6272a4') for fuel in fuel_types],
                alpha=0.8,
                hover=True,
                hover_tooltips=[('Fuel Type', '$name'), ('Utilization', '@$name{0.1f}%')],
                ylim=(0, 100)  # Force Y-axis to 0-100%
            ).opts(
                show_grid=False,
                toolbar='above',
                bgcolor=FLEXOKI_PAPER,
                ylim=(0, 100),  # Double ensure Y-axis range
                yformatter='%.0f%%',  # Format Y-axis as percentage
                hooks=[self._get_flexoki_background_hook()]
            )

            # Rename the Y dimension to make it independent from generation MW axis
            line_plot = line_plot.redim(**{fuel: f'{fuel}_utilization' for fuel in fuel_types})

            return line_plot

        except Exception as e:
            logger.error(f"Error creating utilization plot: {e}")
            # Return fallback plot
            return hv.Text(0.5, 0.5, f'Error creating utilization plot: {str(e)}').opts(
                bgcolor=FLEXOKI_PAPER,
                color='red',
                fontsize=12
            )
    
    def update_plot(self):
        """Update all plots with fresh data and proper error handling"""
        try:
            logger.info("Starting plot update...")

            # Create new plots
            new_generation_plot = self.create_plot()  # Returns pn.Column with Plotly + stats
            new_utilization_plot = self.create_utilization_plot()
            new_transmission_plot = self.create_transmission_plot()
            new_tod_plot = self.create_generation_tod_plot()

            # Safely update the Plotly generation pane (pn.Column)
            if self.plot_pane is not None:
                self.plot_pane.clear()
                self.plot_pane.append(new_generation_plot)

            if self.utilization_pane is not None:
                self.utilization_pane.object = new_utilization_plot

            if self.transmission_pane is not None:
                self.transmission_pane.object = new_transmission_plot

            if self.generation_tod_pane is not None:
                self.generation_tod_pane.object = new_tod_plot

            # Summary table is now included in plot_pane, keep this for backward compat
            if self.summary_table_pane is not None:
                new_summary_table = self.create_generation_summary_table()
                self.summary_table_pane.clear()
                self.summary_table_pane.append(new_summary_table)
                
                # Post-render formatting: Access Bokeh figure after HoloViews renders it
                try:
                    # Get the Bokeh model from the pane
                    bokeh_model = self.transmission_pane.get_root()
                    if hasattr(bokeh_model, 'below'):
                        for axis in bokeh_model.below:
                            if hasattr(axis, 'formatter'):
                                # Apply formatter based on time range
                                if self.time_range == '1':
                                    axis.formatter = DatetimeTickFormatter(hours="%H:%M", days="%H:%M")
                                elif self.time_range == '7':
                                    axis.formatter = DatetimeTickFormatter(hours="%a %H:%M", days="%a %d", months="%b %d")
                                else:
                                    axis.formatter = DatetimeTickFormatter(hours="%m/%d", days="%m/%d", months="%b %d", years="%Y")
                except Exception as e:
                    logger.debug(f"Post-render formatting attempt failed: {e}")
            
            # Update the header with new time
            if self.header_section is not None:
                header_html = f"""
                <div class='header-container' style='background-color: {FLEXOKI_BASE[100]}; padding: 15px; margin: -10px -10px 20px -10px;'>
                    <h1 style='color: {FLEXOKI_BLACK}; margin: 0; text-align: center;'>Nem Analysis</h1>
                    <div style='text-align: center; color: {FLEXOKI_BLACK}; font-size: 16px; margin-top: 5px;'>
                        Last Updated: {datetime.now().strftime('%H:%M:%S')} | data:AEMO, design ITK
                    </div>
                </div>
                """
                self.header_section.object = header_html
            
            logger.info("Plot update completed successfully")
            
        except Exception as e:
            logger.error(f"Error updating plots: {e}")
            # Don't crash the application, just log and continue
    
    async def auto_update_loop(self):
        """Automatic update loop every 4.5 minutes with better error handling"""
        # Track last date range to detect changes
        last_date_range = None
        
        while True:
            try:
                await asyncio.sleep(270)  # 4.5 minutes
                
                # FIX for midnight rollover bug: Refresh date ranges for preset time ranges
                # This ensures the dashboard continues updating after midnight
                if self.time_range in ['1', '7', '30', '90', '365']:
                    # Store old date range
                    old_start_date = self.start_date
                    old_end_date = self.end_date
                    old_range = (old_start_date, old_end_date)
                    
                    # Update to new date range
                    self._update_date_range_from_preset()
                    new_start_date = self.start_date
                    new_end_date = self.end_date
                    new_range = (new_start_date, new_end_date)
                    
                    # CRITICAL FIX: Check if date RANGE changed (not just dates)
                    if old_range != new_range and last_date_range is not None:
                        logger.info(f"Date RANGE changed: {old_range} → {new_range}")
                        logger.info("Forcing Panel component refresh for display update")
                        
                        # Force component recreation to update display
                        self._force_component_refresh()
                    elif old_end_date != new_end_date:
                        logger.info(f"Date rollover detected: updated end_date from {old_end_date} to {new_end_date}")
                    
                    last_date_range = new_range
                
                # Update plots in both tabs
                self.update_plot()
                logger.info("Auto-update completed")
            except asyncio.CancelledError:
                logger.info("Auto-update loop cancelled")
                break
            except Exception as e:
                logger.error(f"Error in auto-update loop: {e}")
                await asyncio.sleep(60)  # Wait 1 minute before retrying
    
    def _force_component_refresh(self):
        """
        Force Panel components to refresh when date range changes.
        This is the critical fix for the midnight display freeze bug.
        """
        try:
            logger.info("Starting forced component refresh due to date range change")
            
            # Find all Panel panes and force them to refresh
            components_refreshed = []
            
            # List of ACTUAL pane attributes in the dashboard
            pane_attrs = [
                'plot_pane',              # Main generation plot
                'price_plot_pane',        # Price plot
                'transmission_pane',      # Transmission plot
                'utilization_pane',       # Utilization plot
                'generation_tod_pane',    # Generation time of day plot
                'bands_plot_pane',        # Price bands plot
                'tod_plot_pane',          # Time of day plot
                'renewable_gauge',        # Renewable gauge (if exists)
                'loading_indicator'       # Loading indicator
            ]
            
            for attr_name in pane_attrs:
                if hasattr(self, attr_name):
                    pane = getattr(self, attr_name)
                    
                    # Check if it's a Panel pane with an object property
                    if hasattr(pane, 'object') and hasattr(pane, 'param'):
                        try:
                            # Method 1: Use param.trigger to force refresh
                            if hasattr(pane.param, 'trigger'):
                                pane.param.trigger('object')
                                components_refreshed.append(f"{attr_name} (param.trigger)")
                                logger.debug(f"Triggered refresh for {attr_name}")
                            
                            # Method 2: Reassign object to force update
                            else:
                                current_object = pane.object
                                pane.object = None  # Clear first
                                pane.object = current_object  # Reassign
                                components_refreshed.append(f"{attr_name} (reassign)")
                                logger.debug(f"Reassigned object for {attr_name}")
                                
                        except Exception as e:
                            logger.warning(f"Could not refresh {attr_name}: {e}")
            
            # Also refresh any tabs that might exist
            if hasattr(self, 'tabs') and hasattr(self.tabs, 'param'):
                try:
                    self.tabs.param.trigger('objects')
                    components_refreshed.append("tabs")
                except Exception as e:
                    logger.debug(f"Could not refresh tabs: {e}")
            
            logger.info(f"Forced refresh completed for {len(components_refreshed)} components: {', '.join(components_refreshed)}")
            
        except Exception as e:
            logger.error(f"Error in _force_component_refresh: {e}")
    
    def start_auto_update(self):
        """Start the auto-update task - only when event loop is running"""
        try:
            # Cancel existing task if running
            if self.update_task is not None and not self.update_task.done():
                self.update_task.cancel()
            
            # Start new task
            self.update_task = asyncio.create_task(self.auto_update_loop())
            logger.info("Auto-update started")
        except RuntimeError as e:
            # No event loop running yet - will start later
            logger.info(f"Event loop not ready - auto-update will start when served: {e}")
            pass
    
    @param.depends('region', watch=True)
    def on_region_change(self):
        """Called when region parameter changes"""
        logger.info(f"Region changed to: {self.region}")
        self.update_plot()
    
    @param.depends('time_range', watch=True)
    def on_time_range_change(self):
        """Called when time range parameter changes"""
        logger.info(f"Time range changed to: {self.time_range}")
        # Update start/end dates based on preset selection
        self._update_date_range_from_preset()
        # Clear cached data so it reloads with new time range
        self.transmission_df = None
        self.rooftop_df = None
        self.update_plot()
    
    @param.depends('start_date', 'end_date', watch=True)
    def on_date_change(self):
        """Called when custom date range changes"""
        logger.info(f"Date range changed to: {self.start_date} - {self.end_date}")
        # Clear cached data so it reloads with new date range
        self.transmission_df = None
        self.rooftop_df = None
        self.update_plot()
    
    def _update_date_range_from_preset(self):
        """Update start_date and end_date based on time_range preset"""
        end_date = datetime.now().date()

        if self.time_range == '1':
            start_date = end_date - timedelta(days=1)
        elif self.time_range == '7':
            start_date = end_date - timedelta(days=7)
        elif self.time_range == '30':
            start_date = end_date - timedelta(days=30)
        elif self.time_range == '90':
            start_date = end_date - timedelta(days=90)
        elif self.time_range == '365':
            start_date = end_date - timedelta(days=365)
        elif self.time_range == 'All':
            start_date = datetime(2020, 1, 1).date()  # Approximate earliest data
        else:
            # Keep current custom dates
            return

        # Update the date parameters
        self.start_date = start_date
        self.end_date = end_date
    
    def _get_effective_date_range(self):
        """Get the effective start and end datetime for data filtering
        
        IMPORTANT: This method includes a fix for the midnight rollover issue.
        
        Problem: When the dashboard runs past midnight, if end_date is set to "today"
        (which becomes yesterday after midnight), the dashboard would query for 
        yesterday's data only, causing the display to freeze at 11:55 PM (the last
        data point before midnight).
        
        Solution: If the selected end_date is today or in the future, cap the query
        at the current time to prevent querying stale data after date rollover.
        """
        if self.time_range == 'All':
            # For all data, return full available range for auto resolution selection
            # This allows the enhanced adapters to choose 30-minute data for better performance
            start_datetime = datetime(2020, 2, 1)  # Actual start of historical data in the system
            end_datetime = datetime.now()          # Current time
            return start_datetime, end_datetime
        else:
            # Convert dates to datetime for filtering
            start_datetime = datetime.combine(self.start_date, datetime.min.time())
            
            # MIDNIGHT ROLLOVER FIX: Cap end_datetime at current time if end_date is today or future
            now = datetime.now()
            end_date_midnight = datetime.combine(self.end_date, datetime.max.time())
            
            # Check if the end_date is today or in the future
            if end_date_midnight >= now:
                # End date is today or future - cap at current time to avoid stale data
                end_datetime = now
                logger.info(f"Date range capped at current time: {now.strftime('%Y-%m-%d %H:%M:%S')} "
                           f"(requested end_date was {self.end_date})")
            else:
                # End date is in the past - use the full day as requested
                end_datetime = end_date_midnight
                logger.debug(f"Using full historical date range up to {end_date_midnight.strftime('%Y-%m-%d %H:%M:%S')}")
            
            return start_datetime, end_datetime
    
    
    def _get_time_range_display(self):
        """Get formatted time range string for chart titles"""
        if self.time_range == '1':
            return "Last 24 Hours"
        elif self.time_range == '7':
            return "Last 7 Days"
        elif self.time_range == '30':
            return "Last 30 Days"
        elif self.time_range == '90':
            return "Last 90 Days"
        elif self.time_range == '365':
            return "Last 365 Days"
        elif self.time_range == 'All':
            return "All Available Data"
        else:
            # Custom date range
            if self.start_date and self.end_date:
                if self.start_date == self.end_date:
                    return f"{self.start_date.strftime('%Y-%m-%d')}"
                else:
                    return f"{self.start_date.strftime('%Y-%m-%d')} to {self.end_date.strftime('%Y-%m-%d')}"
            return "Custom Range"
    
    def _get_datetime_formatter_hook(self):
        """Get appropriate datetime formatter based on time range"""
        def formatter_hook(plot, element):
            if self.time_range == '1':
                # For 24 hours, show hours
                plot.handles['xaxis'].formatter = DatetimeTickFormatter(
                    hours="%H:%M",
                    days="%H:%M"
                )
            elif self.time_range == '7':
                # For 7 days, show day names or dates
                plot.handles['xaxis'].formatter = DatetimeTickFormatter(
                    hours="%a %H:%M",  # Mon 14:00
                    days="%a %d",      # Mon 15
                    months="%b %d"
                )
            else:
                # For 30 days or longer, show dates
                plot.handles['xaxis'].formatter = DatetimeTickFormatter(
                    hours="%m/%d",
                    days="%m/%d",
                    months="%b %d",
                    years="%Y"
                )
        return formatter_hook
    
    def _get_attribution_hook(self, align='right', offset=-5):
        """Get a reusable attribution hook for hvplot charts
        
        Args:
            align: Alignment of attribution text ('left', 'center', 'right')
            offset: Vertical offset for the attribution text
            
        Returns:
            Hook function that adds attribution to plot
        """
        def add_attribution(plot, element):
            """Add attribution text to the plot after rendering"""
            try:
                from bokeh.models import Title
                # Get the plot figure
                p = plot.state
                # Add attribution as a subtitle below the plot
                attribution = Title(text='Design: ITK, Data: AEMO', 
                                  text_font_size='9pt',
                                  text_color='#6272a4',
                                  align=align,
                                  offset=offset)
                # Add to below center
                p.add_layout(attribution, 'below')
            except Exception as e:
                logger.debug(f"Could not add attribution: {e}")
        
        return add_attribution

    def _get_flexoki_background_hook(self):
        """Get a hook that ensures plot area and legend backgrounds are set to FLEXOKI_PAPER.

        This fixes white backgrounds that can appear on plot areas and legends
        even when bgcolor is set, by explicitly setting the Bokeh model properties.

        Returns:
            Hook function that sets backgrounds to FLEXOKI_PAPER (#FFFCF0)
        """
        def set_flexoki_backgrounds(plot, element):
            """Set plot area and legend backgrounds to Flexoki paper color"""
            try:
                p = plot.state
                # Set plot background (the inner plot area)
                p.background_fill_color = FLEXOKI_PAPER
                # Set border fill (the outer area including where legend sits) - KEY FIX
                p.border_fill_color = FLEXOKI_PAPER
                # Set outline color
                p.outline_line_color = FLEXOKI_BASE[150]
                # Set legend background if legend exists
                if hasattr(p, 'legend') and p.legend:
                    for legend in p.legend:
                        legend.background_fill_color = FLEXOKI_PAPER
                        legend.border_line_color = FLEXOKI_BASE[150]
                        legend.border_line_alpha = 1.0
            except Exception as e:
                logger.debug(f"Could not set Flexoki backgrounds: {e}")

        return set_flexoki_backgrounds

    def _apply_smoothing(self, data, smoothing_type, column_name, group_column=None):
        """Apply smoothing to data with various algorithms
        
        Args:
            data: DataFrame containing the data to smooth
            smoothing_type: String describing the smoothing method
            column_name: Name of column to smooth
            group_column: Optional column name to group by (e.g., 'REGIONID', 'duid')
            
        Returns:
            DataFrame with smoothed values in the specified column
        """
        import numpy as np
        
        # Create a copy to avoid modifying original
        smoothed_data = data.copy()
        
        if smoothing_type == 'None':
            return smoothed_data
            
        # Define groups to smooth
        if group_column and group_column in smoothed_data.columns:
            groups = smoothed_data[group_column].unique()
        else:
            groups = [None]  # Treat entire dataset as one group
            
        for group in groups:
            if group is not None:
                mask = smoothed_data[group_column] == group
            else:
                mask = np.ones(len(smoothed_data), dtype=bool)
                
            group_data = smoothed_data.loc[mask, column_name]
            
            try:
                if smoothing_type == 'Moving Avg (7 periods)':
                    smoothed_values = group_data.rolling(7, center=True).mean()
                elif smoothing_type == 'Moving Avg (30 periods)':
                    smoothed_values = group_data.rolling(30, center=True).mean()
                elif smoothing_type == 'Exponential (α=0.3)':
                    smoothed_values = group_data.ewm(alpha=0.3).mean()
                    
                elif smoothing_type.startswith('LOESS'):
                    from statsmodels.nonparametric.smoothers_lowess import lowess
                    
                    # Extract parameters from the option string
                    if '3 hours' in smoothing_type:
                        frac = 0.01
                    elif '1 day' in smoothing_type:
                        frac = 0.02
                    elif '7 days' in smoothing_type:
                        frac = 0.05
                    elif '30 days' in smoothing_type:
                        frac = 0.1
                    elif '90 days' in smoothing_type:
                        frac = 0.15
                    else:
                        frac = 0.1  # Default
                    
                    # Get valid data points
                    valid_mask = ~group_data.isna()
                    if valid_mask.sum() == 0:
                        continue
                        
                    valid_data = group_data[valid_mask].values
                    valid_indices = np.where(valid_mask)[0]
                    
                    # Check if we have enough points
                    min_points = max(3, int(frac * len(valid_data)) + 1)
                    if len(valid_data) >= min_points:
                        # Convert to numeric x values
                        x = np.arange(len(valid_data))
                        
                        # Apply LOESS
                        smoothed_result = lowess(
                            valid_data, 
                            x, 
                            frac=frac,
                            it=0,  # No robustness iterations for speed
                            delta=0.01 * len(valid_data) if len(valid_data) > 100 else 0
                        )
                        
                        # Extract smoothed values
                        smoothed_vals = smoothed_result[:, 1]
                        
                        # Put smoothed values back in the right places
                        smoothed_values = pd.Series(index=group_data.index, dtype=float)
                        smoothed_values.iloc[valid_indices] = smoothed_vals
                    else:
                        logger.warning(f"Not enough data points for LOESS smoothing")
                        smoothed_values = group_data
                        
                elif smoothing_type.startswith('EWM'):
                    # Exponentially Weighted Moving Average
                    
                    # Extract span from the option string
                    if '7 days' in smoothing_type:
                        # For high-frequency data (5-min or 30-min)
                        if hasattr(data, 'index') and len(data) > 1:
                            time_diff = (data.index[1] - data.index[0]).total_seconds() / 60
                            if time_diff <= 10:  # 5-minute data
                                span = 7 * 24 * 12  # 7 days * 288 periods/day
                            else:  # 30-minute data
                                span = 7 * 24 * 2   # 7 days * 48 periods/day
                        else:
                            span = 336  # Default to 30-min
                    elif '14 days' in smoothing_type:
                        if hasattr(data, 'index') and len(data) > 1:
                            time_diff = (data.index[1] - data.index[0]).total_seconds() / 60
                            if time_diff <= 10:  # 5-minute data
                                span = 14 * 24 * 12
                            else:  # 30-minute data
                                span = 14 * 24 * 2
                        else:
                            span = 672
                    elif '30 days' in smoothing_type:
                        if hasattr(data, 'index') and len(data) > 1:
                            time_diff = (data.index[1] - data.index[0]).total_seconds() / 60
                            if time_diff <= 10:  # 5-minute data
                                span = 30 * 24 * 12
                            else:  # 30-minute data
                                span = 30 * 24 * 2
                        else:
                            span = 1440
                    elif '60 days' in smoothing_type:
                        if hasattr(data, 'index') and len(data) > 1:
                            time_diff = (data.index[1] - data.index[0]).total_seconds() / 60
                            if time_diff <= 10:  # 5-minute data
                                span = 60 * 24 * 12
                            else:  # 30-minute data
                                span = 60 * 24 * 2
                        else:
                            span = 2880
                    else:
                        span = 336  # Default to 7 days
                    
                    # Apply EWM
                    smoothed_values = group_data.ewm(span=span, adjust=False).mean()
                else:
                    # Unknown smoothing type, return original
                    smoothed_values = group_data
                    
                # Apply the smoothed values back to the dataframe
                smoothed_data.loc[mask, column_name] = smoothed_values
                
            except Exception as e:
                logger.error(f"Error applying {smoothing_type} smoothing: {e}")
                # Keep original values on error
                
        return smoothed_data
    
    def test_vol_price(self):
        """Test method to verify vol_price functionality"""
        try:
            print("Testing vol_price method...")
            
            # Test loading price data
            price_data = self.load_price_data()
            print(f"Price data loaded: {len(price_data)} records")
            if not price_data.empty:
                print(f"Price range: ${price_data['RRP'].min():.2f} to ${price_data['RRP'].max():.2f}")
                print(f"Time range: {price_data['settlementdate'].min()} to {price_data['settlementdate'].max()}")
            
            # Test loading generation data
            self.load_generation_data()
            gen_data = self.process_data_for_region()
            print(f"Generation data loaded: {len(gen_data)} records")
            if not gen_data.empty:
                print(f"Fuel types: {list(gen_data.columns)}")
                print(f"Generation range: {gen_data.sum(axis=1).min():.1f} to {gen_data.sum(axis=1).max():.1f} MW")
            
            # Test creating the combined plot
            plot = self.vol_price()
            print("Combined plot created successfully!")
            return plot
            
        except Exception as e:
            print(f"Error in test_vol_price: {e}")
            import traceback
            traceback.print_exc()
            return None

    def _create_generation_tab(self):
        """Create the Generation by Fuel tab with left-side controls and right-side chart subtabs"""
        try:
            # Region selector for left side
            region_selector = pn.Param(
                self,
                parameters=['region'],
                widgets={'region': pn.widgets.Select},
                name="Region Selection",
                width=280,
                margin=(10, 0)
            )
            
            # Time range selector with compact radio buttons
            time_range_widget = pn.widgets.RadioBoxGroup(
                name="",  # Empty name since we add label separately
                value=self.time_range,
                options=['1', '7', '30', '90', '365', 'All'],
                inline=True,  # Horizontal layout
                width=350
            )
            time_range_widget.link(self, value='time_range')
            
            time_range_selector = pn.Column(
                pn.pane.HTML(f"<div style='color: {FLEXOKI_BLACK}; font-size: 11px; margin-bottom: 4px;'>Days</div>"),
                time_range_widget,
                width=350,
                margin=(10, 0)
            )
            
            # Custom date range pickers
            date_selectors = pn.Param(
                self,
                parameters=['start_date', 'end_date'],
                widgets={
                    'start_date': pn.widgets.DatePicker,
                    'end_date': pn.widgets.DatePicker
                },
                name="Custom Date Range",
                width=280,
                margin=(10, 0)
            )
            
            # Create left-side control panel with cleaner layout
            control_panel = pn.Column(
                "### Generation by Fuel Controls",
                region_selector,
                "---",
                time_range_selector,
                date_selectors,
                pn.pane.Markdown("*Custom dates override preset selection*"),
                width=280,  # Reduced width to match Station Analysis
                sizing_mode='fixed'
            )
            
            # Create subtabs for charts with constrained containers
            chart_subtabs = pn.Tabs(
                ("Generation Stack", pn.Column(
                    self.plot_pane,  # Plotly chart + stats table included
                    sizing_mode='stretch_width'
                )),
                ("Capacity Utilization", pn.Column(
                    "#### Capacity Utilization by Fuel",
                    pn.Column(self.utilization_pane, max_width=1250, sizing_mode='stretch_width'),
                    sizing_mode='stretch_width'
                )),
                ("Transmission Lines", pn.Column(
                    "#### Individual Transmission Line Flows",
                    pn.Column(self.transmission_pane, max_width=1250, sizing_mode='stretch_width'),
                    sizing_mode='stretch_width'
                )),
                ("Generation TOD", pn.Column(
                    "#### Average Generation by Time of Day",
                    pn.Column(self.generation_tod_pane, max_width=1250, sizing_mode='stretch_width'),
                    sizing_mode='stretch_width'
                )),
                dynamic=True,
                sizing_mode='stretch_width'
            )
            
            # Create main layout with controls on left, charts on right
            generation_tab_layout = pn.Row(
                control_panel,
                chart_subtabs,
                sizing_mode='stretch_width'
            )
            
            return generation_tab_layout
            
        except Exception as e:
            logger.error(f"Error creating generation tab: {e}")
            return pn.pane.Markdown(f"**Error creating Generation tab:** {e}")

    def create_dashboard(self):
        """Create the complete dashboard with tabbed interface"""
        try:
            # Dashboard title with update time in teal header
            header_html = f"""
            <div class='header-container' style='background-color: {FLEXOKI_BASE[100]}; padding: 15px; margin: -10px -10px 20px -10px;'>
                <h1 style='color: {FLEXOKI_BLACK}; margin: 0; text-align: center;'>Nem Analysis</h1>
                <div style='text-align: center; color: {FLEXOKI_BLACK}; font-size: 16px; margin-top: 5px;'>
                    Last Updated: {datetime.now().strftime('%H:%M:%S')} | data:AEMO, design ITK
                </div>
            </div>
            """
            
            self.header_section = pn.pane.HTML(
                header_html,
                sizing_mode='stretch_width'
            )
            
            # Create tabs for different views with lazy loading (except Today tab)
            logger.info("Setting up tabs with lazy loading...")
            
            # Create Today tab immediately (main overview - should be visible right away)
            try:
                logger.info("Creating Today tab...")
                nem_dash_tab = create_nem_dash_tab_with_updates(dashboard_instance=self, auto_update=True)
                logger.info("Today tab created successfully")
            except Exception as e:
                logger.error(f"Error creating Today tab: {e}")
                nem_dash_tab = pn.pane.Markdown(f"**Error loading Today tab:** {e}")
            
            # Initialize with loading placeholders for other tabs
            loading_html = """
            <div style="text-align: center; padding: 50px;">
                <h3>Loading...</h3>
                <p>This tab will load when you click on it.</p>
            </div>
            """
            
            # Create tabbed interface with Today loaded, others lazy
            tabs = pn.Tabs(
                ("Today", nem_dash_tab),  # Load immediately
                ("Generation mix", pn.pane.HTML(loading_html)),  # Lazy
                ("Prices", pn.pane.HTML(loading_html)),  # Lazy - NEW
                ("Pivot table", pn.pane.HTML(loading_html)),  # Lazy
                ("Station Analysis", pn.pane.HTML(loading_html)),  # Lazy
                ("Trends", pn.pane.HTML(loading_html)),  # Lazy
                ("Curtailment", pn.pane.HTML(loading_html)),  # Lazy - NEW
                ("Batteries", pn.pane.HTML(loading_html)),  # Lazy
                dynamic=True,
                closable=False,
                sizing_mode='stretch_width'
            )
            
            # Track which tabs have been loaded (Today is already loaded)
            self._loaded_tabs = {0}  # Mark Today as loaded
            
            # Store tab names for preservation - read them from the tabs widget
            self._tab_names = list(tabs._names)  # This will automatically include all tab names
            
            # Store tab creation functions (no need for Today since it's loaded)
            self._tab_creators = {
                1: self._create_generation_tab_lazy,
                2: self._create_prices_tab,  # NEW
                3: self._create_price_analysis_tab,  # Shifted from 2 to 3
                4: self._create_station_analysis_tab,  # Shifted from 3 to 4
                5: self._create_trends_tab,  # Shifted from 4 to 5
                6: self._create_curtailment_tab,  # NEW
                7: self._create_batteries_tab  # Renamed from insights, shifted to 7
            }
            
            # Watch for tab changes
            tabs.param.watch(self._on_tab_change, 'active')
            
            logger.info("Tab setup complete")
            
            # Add JavaScript auto-refresh for reliability - 9 minutes for production (2 collector cycles)
            auto_refresh_script = pn.pane.HTML(
                """
                <div style="position: fixed; top: 5px; right: 5px; background: rgba(226,224,217,0.9); color: #66800B; padding: 5px 10px; border-radius: 5px; font-size: 11px; z-index: 9999;">
                    Auto-refresh: 9min
                </div>
                <script>
                // State preservation for dashboard refresh
                function saveDashboardState() {
                    const state = {
                        // Save active tab index
                        activeTab: document.querySelector('.bk-tabs').getElementsByClassName('bk-tab-active')[0]?.getAttribute('data-index') || '0',
                        
                        // Save date range selections if they exist
                        dateRangeSelections: {},
                        
                        // Save region selections if they exist
                        regionSelections: {},
                        
                        // Timestamp for validation
                        savedAt: new Date().toISOString()
                    };
                    
                    // Try to save date picker values
                    const dateInputs = document.querySelectorAll('input[type="date"], input[type="datetime-local"]');
                    dateInputs.forEach((input, index) => {
                        if (input.value) {
                            state.dateRangeSelections['date_' + index] = input.value;
                        }
                    });
                    
                    // Try to save select/multiselect values
                    const selects = document.querySelectorAll('select');
                    selects.forEach((select, index) => {
                        if (select.value) {
                            state.regionSelections['select_' + index] = select.value;
                        }
                    });
                    
                    localStorage.setItem('aemo_dashboard_state', JSON.stringify(state));
                    console.log('Dashboard state saved:', state);
                }
                
                function restoreDashboardState() {
                    try {
                        const savedState = localStorage.getItem('aemo_dashboard_state');
                        if (!savedState) return;
                        
                        const state = JSON.parse(savedState);
                        
                        // Only restore if saved within last 15 minutes
                        const savedTime = new Date(state.savedAt);
                        const now = new Date();
                        const diffMinutes = (now - savedTime) / (1000 * 60);
                        
                        if (diffMinutes > 15) {
                            localStorage.removeItem('aemo_dashboard_state');
                            return;
                        }
                        
                        // Restore active tab after a short delay to ensure tabs are loaded
                        setTimeout(function() {
                            const tabIndex = parseInt(state.activeTab);
                            const tabs = document.querySelector('.bk-tabs');
                            if (tabs && tabIndex > 0) {
                                const tabHeaders = tabs.querySelectorAll('.bk-tab');
                                if (tabHeaders[tabIndex]) {
                                    tabHeaders[tabIndex].click();
                                    console.log('Restored to tab:', tabIndex);
                                }
                            }
                        }, 1000);
                        
                        // Clear the saved state after restoration
                        localStorage.removeItem('aemo_dashboard_state');
                        
                    } catch (e) {
                        console.error('Error restoring dashboard state:', e);
                    }
                }
                
                // Set up auto-refresh with state preservation
                console.log('Auto-refresh enabled: Page will reload every 9 minutes (2 data collector cycles)');
                setTimeout(function(){
                    console.log('Saving state and refreshing page...');
                    saveDashboardState();
                    window.location.reload(true);
                }, 540000);  // 540000ms = 9 minutes
                
                // Try to restore state on page load
                window.addEventListener('load', function() {
                    restoreDashboardState();
                });
                </script>
                """
            )
            
            # Complete dashboard layout
            dashboard = pn.Column(
                auto_refresh_script,  # Add auto-refresh script
                self.header_section,
                tabs,
                sizing_mode='stretch_width'
            )
            
            # Store tabs reference for lazy loading
            self.tabs = tabs
            
            # Don't initialize plots here - they'll be created when tabs are loaded
            
            return dashboard
            
        except Exception as e:
            logger.error(f"Error creating dashboard: {e}")
            # Return error message dashboard
            return pn.pane.HTML(f"<h1>Error creating dashboard: {str(e)}</h1>", 
                              sizing_mode='stretch_width')
    
    def _on_tab_change(self, event):
        """Handle tab change events for lazy loading"""
        tab_index = event.new
        
        # Skip if tab already loaded
        if tab_index in self._loaded_tabs:
            return
        
        logger.info(f"Loading tab {tab_index} on demand...")
        start_time = time.time()
        
        try:
            # Get the tab creator function
            creator_func = self._tab_creators.get(tab_index)
            if creator_func:
                # Create loading indicator
                loading_indicator = pn.Column(
                    pn.indicators.LoadingSpinner(value=True, size=100),
                    pn.pane.Markdown("Loading tab content..."),
                    align='center',
                    sizing_mode='stretch_width'
                )
                # Use tuple to preserve tab name
                tab_name = self._tab_names[tab_index]
                self.tabs[tab_index] = (tab_name, loading_indicator)
                
                # Create the actual tab content
                tab_content = creator_func()
                
                # Replace loading indicator with content (preserving name)
                self.tabs[tab_index] = (tab_name, tab_content)
                
                # Mark as loaded
                self._loaded_tabs.add(tab_index)
                
                # If this is generation tab, initialize the plot
                if tab_index == 1:
                    self.update_plot()
                
                elapsed = time.time() - start_time
                logger.info(f"Tab {tab_index} loaded in {elapsed:.2f} seconds")
            else:
                logger.error(f"No creator function for tab {tab_index}")
                
        except Exception as e:
            logger.error(f"Error loading tab {tab_index}: {e}")
            self.tabs[tab_index] = pn.pane.Markdown(f"**Error loading tab:** {e}")
    
    
    def _create_generation_tab_lazy(self):
        """Wrapper for lazy loading generation tab"""
        return self._create_generation_tab()
    
    def _create_prices_tab(self):
        """Create prices analysis tab with selectors and visualizations"""
        try:
            logger.info("Creating prices tab...")
            logger.info(f"Start date: {self.start_date}, End date: {self.end_date}")
            
            # Calculate date range - actual price data available from 2020-01-01
            # prices30.parquet has 5+ years of data, prices5.parquet only has ~52 days
            end_date = self.end_date
            start_date = self.start_date  # This should already be 5 years of data
            
            # Date preset radio buttons (vertical)
            date_presets = pn.widgets.RadioBoxGroup(
                name='',
                options=['1 day', '7 days', '30 days', '90 days', '1 year', 'All data'],
                value='1 year',
                inline=False,
                width=100
            )
            
            # Date pickers - simpler without start/end constraints that cause issues
            # Use explicit date objects to avoid timezone issues
            default_end = pd.Timestamp.now().date()
            default_start = default_end - pd.Timedelta(days=365)  # Match 1 year preset

            start_date_picker = pn.widgets.DatePicker(
                name='',  # Label handled externally
                value=default_start,
                width=110
            )

            end_date_picker = pn.widgets.DatePicker(
                name='',  # Label handled externally
                value=default_end,
                width=110
            )
            
            # Show selected dates clearly
            date_display = pn.pane.Markdown(
                f"**Selected Period:** {start_date_picker.value.strftime('%Y-%m-%d')} to {end_date_picker.value.strftime('%Y-%m-%d')}",
                width=300
            )
            
            # Region checkbox group for multi-selection
            regions = ['NSW1', 'QLD1', 'SA1', 'TAS1', 'VIC1']
            region_selector = pn.widgets.CheckBoxGroup(
                name='',  # Remove name as we'll use column header
                value=['NSW1', 'VIC1'],  # Default selection
                options=regions,
                inline=False,  # Vertical layout
                align='start',  # Left align
                margin=(0, 0, 0, 0)
            )
            
            # Aggregate level radio buttons (only 5 min and 30 min)
            aggregate_selector = pn.widgets.RadioBoxGroup(
                name='',
                value='30 min',
                options=['5 min', '30 min'],
                inline=False,
                width=80
            )
            
            # Smoothing options
            smoothing_selector = pn.widgets.Select(
                name='Smoothing',
                value='None',
                options=[
                    'None',
                    'LOESS (3 hours, frac=0.01)',  # For 5-minute data, ~36 points
                    'LOESS (1 day, frac=0.02)',    # ~288 points for 5-min data
                    'LOESS (7 days, frac=0.05)',
                    'LOESS (30 days, frac=0.1)',
                    'LOESS (90 days, frac=0.15)',
                    'EWM (7 days, fast response)',
                    'EWM (14 days, balanced)',
                    'EWM (30 days, smooth)',
                    'EWM (60 days, very smooth)'
                ],
                width=250
            )
            
            # Add log scale checkbox
            log_scale_checkbox = pn.widgets.Checkbox(
                name='Log Scale Y-axis',
                value=False,
                width=150
            )
            
            # Add Analyze button (prominent, at top)
            # Use inline CSS for reliable coloring - orange when dirty, blue when clean
            analyze_button = pn.widgets.Button(
                name='▶ Analyze Prices',
                button_type='default',
                width=180,
                height=40
            )
            # Apply initial orange style via stylesheets
            analyze_button.stylesheets = [f'''
                :host(.solid) .bk-btn {{
                    background: {FLEXOKI_ACCENT['orange']} !important;
                    color: white !important;
                    font-weight: bold !important;
                    border: none !important;
                }}
            ''']

            # Track if controls have changed
            controls_dirty = [True]  # Start dirty - needs initial analysis

            def mark_controls_dirty(event=None):
                """Mark that controls have changed and analysis needs re-running"""
                controls_dirty[0] = True
                analyze_button.name = '▶ Analyze Prices (update)'
                analyze_button.stylesheets = [f'''
                    :host(.solid) .bk-btn {{
                        background: {FLEXOKI_ACCENT['orange']} !important;
                        color: white !important;
                        font-weight: bold !important;
                        border: none !important;
                    }}
                ''']

            def mark_controls_clean():
                """Mark that analysis is up-to-date"""
                controls_dirty[0] = False
                analyze_button.name = '▶ Analyze Prices ✓'
                analyze_button.stylesheets = [f'''
                    :host(.solid) .bk-btn {{
                        background: {FLEXOKI_ACCENT['cyan']} !important;
                        color: white !important;
                        font-weight: bold !important;
                        border: none !important;
                    }}
                ''']
            
            # Create price plot pane - will be updated by load_price_data
            self.price_plot_pane = pn.pane.HoloViews(
                height=400,
                sizing_mode='stretch_width'
            )
            
            # Initialize with instruction message
            self.price_plot_pane.object = hv.Text(0.5, 0.5, "Click 'Analyze Prices' to load data").opts(
                xlim=(0, 1), ylim=(0, 1),
                bgcolor=FLEXOKI_PAPER,
                color=FLEXOKI_BLACK,
                fontsize=16,
                hooks=[_text_background_hook]
            )
            
            # Create statistics title pane
            self.stats_title_pane = pn.pane.Markdown(
                "### Price Statistics",
                styles={'color': FLEXOKI_BLACK, 'background-color': FLEXOKI_PAPER, 'padding': '10px'}
            )
            
            # Create statistics table pane
            # Initialize with empty DataFrame showing message
            empty_stats_df = pd.DataFrame({
                'Message': ['Click "Analyze Prices" to see statistics']
            })
            self.stats_pane = pn.widgets.Tabulator(
                value=empty_stats_df,  # Use 'value' parameter explicitly
                theme='fast',
                layout='fit_data_table',  # Changed to fit_data_table for more compact columns
                height=420,  # Height for 10 rows (removed variance)
                show_index=False,  # Changed to False for cleaner look
                sizing_mode='stretch_width',
                stylesheets=[f"""
                    .tabulator {{
                        background-color: {FLEXOKI_PAPER};
                        color: {FLEXOKI_BLACK};
                    }}
                    .tabulator-header {{
                        background-color: {FLEXOKI_BASE[100]};
                        color: {FLEXOKI_BLACK};
                        font-weight: bold;
                    }}
                    .tabulator-row {{
                        background-color: {FLEXOKI_PAPER};
                        color: {FLEXOKI_BLACK};
                    }}
                    .tabulator-row:nth-child(even) {{
                        background-color: {FLEXOKI_BASE[50]};
                    }}
                    .tabulator-row:hover {{
                        background-color: {FLEXOKI_BASE[100]};
                    }}
                    .tabulator-cell {{
                        padding: 4px 8px;  /* Reduced vertical padding for more compact look */
                    }}
                    .tabulator-col {{
                        min-width: auto !important;  /* Allow columns to be more compact */
                    }}
                    /* Make the first row (Mean) bold and highlighted */
                    .tabulator-row:first-child .tabulator-cell {{
                        font-weight: bold;
                        color: {FLEXOKI_ACCENT['green']};  /* Green color for emphasis */
                        background-color: {FLEXOKI_BASE[50]};  /* Slightly different background */
                    }}
                    /* Add light line after Min row (3rd row) */
                    .tabulator-row:nth-child(3) {{
                        border-bottom: 1px solid #6272a4;
                        padding-bottom: 8px;
                    }}
                """],
                configuration={
                    'columnDefaults': {
                        'headerFilter': False,
                        'tooltip': True
                    }
                }
            )
            
            # Create fuel-weighted prices table
            self.fuel_prices_pane = pn.widgets.Tabulator(
                value=pd.DataFrame(),  # Start with empty DataFrame
                theme='fast',
                layout='fit_data_table',
                height=250,  # Compact height for 6 fuel rows
                show_index=False,
                sizing_mode='stretch_width',
                stylesheets=[f"""
                    .tabulator {{
                        background-color: {FLEXOKI_PAPER};
                        color: {FLEXOKI_BLACK};
                        margin-top: -10px;  /* Bring closer to stats table */
                    }}
                    .tabulator-header {{
                        background-color: {FLEXOKI_BASE[100]};
                        color: {FLEXOKI_BLACK};
                        font-weight: bold;
                        padding-top: 8px;  /* Add some padding at top */
                    }}
                    .tabulator-row {{
                        background-color: {FLEXOKI_PAPER};
                        color: {FLEXOKI_BLACK};
                    }}
                    .tabulator-row:nth-child(even) {{
                        background-color: {FLEXOKI_BASE[50]};
                    }}
                    .tabulator-row:hover {{
                        background-color: {FLEXOKI_BASE[100]};
                    }}
                    .tabulator-cell {{
                        padding: 4px 8px;
                    }}
                    /* Make first column (Fuel Type) bold */
                    .tabulator-row .tabulator-cell:first-child {{
                        font-weight: bold;
                        color: {FLEXOKI_ACCENT['cyan']};  /* Cyan for fuel names */
                    }}
                """],
                configuration={
                    'columnDefaults': {
                        'headerFilter': False,
                        'tooltip': True,
                        'headerHozAlign': 'center',
                        'hozAlign': 'right'  # Right-align numbers
                    },
                    'columns': [
                        {'title': 'Fuel Type', 'field': 'Fuel Type', 'hozAlign': 'left', 'frozen': True}
                    ]
                }
            )
            
            # Create price bands plot pane as a Column for two charts
            self.bands_plot_pane = pn.Column(
                sizing_mode='stretch_width',
                height=550  # Height for both charts
            )
            # Initialize with instruction message
            initial_message = pn.pane.HoloViews(
                hv.Text(0.5, 0.5, "Price bands will appear here").opts(
                    xlim=(0, 1), ylim=(0, 1),
                    bgcolor=FLEXOKI_PAPER, color=FLEXOKI_BLACK, fontsize=14,
                    hooks=[_text_background_hook]
                ),
                sizing_mode='stretch_width',
                height=550
            )
            self.bands_plot_pane.clear()
            self.bands_plot_pane.append(initial_message)

            # Create fuel relatives plot pane (Plotly)
            self.fuel_relatives_plot_pane = pn.pane.Plotly(
                go.Figure().add_annotation(
                    text="Click '▶ Load Data' button above to load 5.5 years of price data",
                    xref="paper", yref="paper", x=0.5, y=0.5, showarrow=False,
                    font=dict(size=16, color=FLEXOKI_ACCENT['orange'])
                ).update_layout(
                    paper_bgcolor=FLEXOKI_PAPER, plot_bgcolor=FLEXOKI_PAPER,
                    xaxis=dict(visible=False), yaxis=dict(visible=False)
                ),
                sizing_mode='stretch_both',
                height=400
            )

            # Create price band contribution plot pane (Plotly stacked area)
            self.price_index_plot_pane = pn.pane.Plotly(
                go.Figure().add_annotation(
                    text="Price band contributions will appear here after loading data",
                    xref="paper", yref="paper", x=0.5, y=0.5, showarrow=False,
                    font=dict(size=14, color=FLEXOKI_BLACK)
                ).update_layout(
                    paper_bgcolor=FLEXOKI_PAPER, plot_bgcolor=FLEXOKI_PAPER,
                    xaxis=dict(visible=False), yaxis=dict(visible=False)
                ),
                sizing_mode='stretch_both',
                height=400
            )
            
            # Create price band details table pane (HTML table for better styling control)
            self.high_price_events_pane = pn.pane.HTML(
                "<p><em>Select regions and click 'Analyze Prices' to see price band details</em></p>",
                sizing_mode='stretch_width',
                styles={'background': FLEXOKI_PAPER}
            )
            
            # Create time-of-day price pattern pane
            self.tod_plot_pane = pn.pane.HoloViews(
                height=400,
                sizing_mode='stretch_width'
            )
            # Initialize with instruction message
            self.tod_plot_pane.object = hv.Text(0.5, 0.5, "Time of day analysis will appear here").opts(
                xlim=(0, 1), ylim=(0, 1),
                bgcolor=FLEXOKI_PAPER, color=FLEXOKI_BLACK, fontsize=14,
                hooks=[_text_background_hook]
            )
            
            # Left column - all controls with compact layout

            # Region selector
            region_group = pn.Column(
                "**Region**",
                region_selector,
                align='start',
                width=80
            )

            # Frequency selector (horizontal buttons)
            frequency_group = pn.Column(
                "**Frequency**",
                aggregate_selector,
                align='start'
            )

            # Period quick select (vertical)
            period_group = pn.Column(
                "**Period**",
                date_presets,
                align='start',
                width=100
            )

            # Date pickers in a column
            date_pickers = pn.Column(
                pn.Row("**Start**", start_date_picker, align='center'),
                pn.Row("**End**", end_date_picker, align='center'),
                align='start'
            )

            # Smoothing and options row
            options_group = pn.Row(
                pn.Column(
                    "**Smoothing**",
                    smoothing_selector,
                    width=200
                ),
                pn.Column(
                    pn.Spacer(height=20),  # Align with smoothing label
                    log_scale_checkbox,
                    width=120
                ),
                align='end'
            )

            # Combine all controls - button at top
            controls_column = pn.Column(
                analyze_button,
                pn.Spacer(height=15),
                pn.Row(region_group, pn.Spacer(width=15), frequency_group, align='start'),
                pn.Spacer(height=10),
                pn.Row(period_group, pn.Spacer(width=15), date_pickers, align='start'),
                date_display,
                pn.Spacer(height=10),
                options_group,
                width=350,
                margin=(0, 20, 0, 0),
                align='start'
            )
            
            # Right side - 2x2 grid layout
            # Top row: Statistics and High Price Events on left, Price Bands on right
            # Split into two sub-columns to give bands plot more space
            top_left = pn.Column(
                pn.Row(
                    pn.Column(
                        self.stats_title_pane,
                        self.stats_pane,
                        sizing_mode='stretch_both',
                        max_width=250
                    ),
                    pn.Spacer(width=10),
                    pn.Column(
                        self.high_price_events_pane,
                        width=550,  # Fixed width to show all columns
                        height=400
                    ),
                    sizing_mode='fixed'
                ),
                sizing_mode='fixed'
            )
            
            top_row = pn.Row(
                top_left,
                pn.Spacer(width=20),
                pn.Column(
                    self.bands_plot_pane,
                    sizing_mode='stretch_both',
                    min_width=600  # Ensure bands plot has adequate width
                ),
                sizing_mode='stretch_width',
                height=400
            )
            
            # Bottom row: Price Time Series and Time of Day Analysis
            bottom_row = pn.Row(
                pn.Column(
                    "## Price Time Series",
                    self.price_plot_pane,
                    sizing_mode='stretch_both',
                    width_policy='max'
                ),
                pn.Spacer(width=20),
                pn.Column(
                    "## Time of Day Pattern",
                    self.tod_plot_pane,
                    sizing_mode='stretch_width',
                    width=400
                ),
                sizing_mode='stretch_width'
            )
            
            # Create sub-tabs for price analysis
            # Sub-tab 1: Price Analysis (statistics, fuel-weighted prices, time series, time of day)
            price_analysis_content = pn.Column(
                pn.Row(
                    pn.Column(
                        "## Price Statistics ($)",
                        self.stats_pane,
                        sizing_mode='stretch_width',
                        width=550
                    ),
                    pn.Spacer(width=20),
                    pn.Column(
                        "## Time of Day Pattern",
                        self.tod_plot_pane,
                        sizing_mode='stretch_width',
                        width=400
                    ),
                    sizing_mode='stretch_width',
                    height=600  # Increased height to accommodate both tables
                ),
                pn.Spacer(height=20),
                pn.Column(
                    "## Price Time Series",
                    self.price_plot_pane,
                    sizing_mode='stretch_both',
                    width_policy='max'
                ),
                sizing_mode='stretch_both'
            )
            
            # Sub-tab 2: Price Bands (table on left, charts on right)
            price_bands_content = pn.Row(
                # Left side: Price Band Details table
                pn.Column(
                    "## Price Band Details",
                    self.high_price_events_pane,
                    sizing_mode='stretch_both',
                    width=500  # Fixed width for the table
                ),
                pn.Spacer(width=20),
                # Right side: Price Band Distribution charts (stacked vertically)
                pn.Column(
                    "## Price Band Distribution",
                    self.bands_plot_pane,
                    sizing_mode='stretch_width',
                    height=600  # Height for two stacked charts
                ),
                sizing_mode='stretch_both'
            )
            
            # Sub-tab 3: Fuel Relatives (30-day LOESS smoothed fuel-weighted prices)
            # Create region selector for fuel relatives
            fuel_relatives_region_selector = pn.widgets.RadioButtonGroup(
                name='Region',
                value='NSW1',  # Default to NSW
                options=['NSW1', 'QLD1', 'SA1', 'TAS1', 'VIC1'],
                button_type='primary',
                button_style='outline'
            )

            # Add Load button for fuel relatives - prominent styling
            fuel_relatives_load_button = pn.widgets.Button(
                name='▶ Load Data (20-30 sec)',
                button_type='warning',
                width=180,
                height=35
            )

            fuel_relatives_content = pn.Column(
                pn.Row(
                    fuel_relatives_load_button,
                    pn.Spacer(width=20),
                    pn.Column(
                        "**Region:**",
                        fuel_relatives_region_selector,
                    ),
                    align='center'
                ),
                pn.Spacer(height=10),
                pn.Column(
                    "## Fuel-Weighted vs Flat Load Prices (90-day LOESS smoothed)",
                    self.fuel_relatives_plot_pane,
                    pn.Spacer(height=20),
                    "## Price Band Contribution to Flat Load Average (90-day LOESS smoothed)",
                    self.price_index_plot_pane,
                    sizing_mode='stretch_both'
                ),
                sizing_mode='stretch_both'
            )
            
            # Create sub-tabs
            price_subtabs = pn.Tabs(
                ('Price Analysis', price_analysis_content),
                ('Price Bands', price_bands_content),
                ('Fuel Relatives', fuel_relatives_content),
                sizing_mode='stretch_both'
            )
            
            # Complete tab layout - controls on left, sub-tabs on right
            prices_tab = pn.Row(
                controls_column,
                price_subtabs,
                sizing_mode='stretch_both'
            )
            
            # Set up callbacks for date presets
            def update_date_range(event):
                """Update date range based on preset selection"""
                preset = event.new
                # Use the current end_date_picker value as the reference point
                current_end = end_date_picker.value
                
                if preset == '1 day':
                    new_start = current_end - pd.Timedelta(days=1)
                elif preset == '7 days':
                    new_start = current_end - pd.Timedelta(days=7)
                elif preset == '30 days':
                    new_start = current_end - pd.Timedelta(days=30)
                elif preset == '90 days':
                    new_start = current_end - pd.Timedelta(days=90)
                elif preset == '1 year':
                    new_start = current_end - pd.Timedelta(days=365)
                else:  # All data
                    # Use the actual available data range from prices30.parquet
                    new_start = pd.Timestamp('2020-01-01').date()
                
                start_date_picker.value = new_start
            
            # Set up callback for date picker changes
            def update_date_display(event):
                """Update the date display when date pickers change"""
                date_display.object = f"**Selected Period:** {start_date_picker.value.strftime('%Y-%m-%d')} to {end_date_picker.value.strftime('%Y-%m-%d')}"
                
                # Clear the date preset selection when dates are manually changed
                # Check if the current dates match any preset
                current_end = end_date_picker.value
                current_start = start_date_picker.value
                days_diff = (current_end - current_start).days
                
                # Only clear if it doesn't match the current preset
                matches_preset = False
                if date_presets.value == '1 day' and days_diff == 1:
                    matches_preset = True
                elif date_presets.value == '7 days' and days_diff == 7:
                    matches_preset = True
                elif date_presets.value == '30 days' and days_diff == 30:
                    matches_preset = True
                elif date_presets.value == '90 days' and days_diff == 90:
                    matches_preset = True
                elif date_presets.value == '1 year' and days_diff == 365:
                    matches_preset = True
                elif date_presets.value == 'All data' and current_start == pd.Timestamp('2020-01-01').date():
                    matches_preset = True
                
                if not matches_preset:
                    date_presets.value = None  # Clear preset selection
            
            # Function to load and update price data
            def load_and_plot_prices(event=None):
                """Load price data and create hvplot"""
                try:
                    # Show loading message
                    self.price_plot_pane.object = hv.Text(0.5, 0.5, 'Loading price data...').opts(
                        xlim=(0, 1), ylim=(0, 1), 
                        bgcolor=FLEXOKI_PAPER, color=FLEXOKI_BLACK, fontsize=14
                    )
                    
                    # Get current selections
                    selected_regions = region_selector.value
                    if not selected_regions:
                        self.price_plot_pane.object = hv.Text(0.5, 0.5, 'Please select at least one region').opts(
                            xlim=(0, 1), ylim=(0, 1), 
                            bgcolor=FLEXOKI_PAPER, color=FLEXOKI_ACCENT['red'], fontsize=14
                        )
                        # Clear statistics table
                        self.stats_pane.value = pd.DataFrame({'Message': ['Please select at least one region']})
                        # Clear high price events table
                        self.high_price_events_pane.object = "<p><em>Please select at least one region</em></p>"
                        # Clear bands plot
                        self.bands_plot_pane.object = hv.Text(0.5, 0.5, 'Please select at least one region').opts(
                            xlim=(0, 1), ylim=(0, 1), 
                            bgcolor=FLEXOKI_PAPER, color=FLEXOKI_ACCENT['red'], fontsize=14
                        )
                        # Clear time-of-day plot
                        self.tod_plot_pane.object = hv.Text(0.5, 0.5, 'Please select at least one region').opts(
                            xlim=(0, 1), ylim=(0, 1), 
                            bgcolor=FLEXOKI_PAPER, color=FLEXOKI_ACCENT['red'], fontsize=14
                        )
                        return
                    
                    # Import price adapter
                    from ..shared.price_adapter import load_price_data
                    
                    # Load price data
                    logger.info(f"Loading price data for regions: {selected_regions}")
                    # Convert date to datetime for the adapter
                    from datetime import datetime, time
                    start_datetime = datetime.combine(start_date_picker.value, time.min)
                    end_datetime = datetime.combine(end_date_picker.value, time.max)
                    
                    price_data = load_price_data(
                        start_date=start_datetime,
                        end_date=end_datetime,
                        regions=selected_regions,
                        resolution='auto'
                    )
                    
                    if price_data.empty:
                        self.price_plot_pane.object = hv.Text(0.5, 0.5, 'No data available for selected period').opts(
                            xlim=(0, 1), ylim=(0, 1), 
                            bgcolor=FLEXOKI_PAPER, color=FLEXOKI_BLACK, fontsize=14
                        )
                        # Clear statistics table
                        self.stats_pane.value = pd.DataFrame({'Message': ['No data available for selected period']})
                        # Clear high price events table
                        self.high_price_events_pane.object = "<p><em>No data available for selected period</em></p>"
                        # Clear bands plot
                        self.bands_plot_pane.object = hv.Text(0.5, 0.5, 'No data available').opts(
                            xlim=(0, 1), ylim=(0, 1), 
                            bgcolor=FLEXOKI_PAPER, color=FLEXOKI_BLACK, fontsize=14
                        )
                        # Clear time-of-day plot
                        self.tod_plot_pane.object = hv.Text(0.5, 0.5, 'No data available').opts(
                            xlim=(0, 1), ylim=(0, 1), 
                            bgcolor=FLEXOKI_PAPER, color=FLEXOKI_BLACK, fontsize=14
                        )
                        return
                    
                    # Ensure SETTLEMENTDATE is a column, not index
                    if price_data.index.name == 'SETTLEMENTDATE' and 'SETTLEMENTDATE' not in price_data.columns:
                        price_data = price_data.reset_index()
                    
                    # Handle negative prices for log scale
                    use_log = log_scale_checkbox.value
                    if use_log and (price_data['RRP'] <= 0).any():
                        # Option 1: Shift all values to make them positive
                        min_price = price_data['RRP'].min()
                        if min_price <= 0:
                            shift_value = abs(min_price) + 1
                            price_data['RRP_adjusted'] = price_data['RRP'] + shift_value
                            ylabel = f'Price ($/MWh) + {shift_value:.0f} [Log Scale]'
                            y_col = 'RRP_adjusted'
                        else:
                            y_col = 'RRP'
                            ylabel = 'Price ($/MWh) [Log Scale]'
                    else:
                        y_col = 'RRP'
                        ylabel = 'Price ($/MWh)'
                    
                    # Resample based on frequency selection
                    freq_map = {
                        '5 min': '5min',
                        '30 min': '30min',
                        '1 hour': 'h',
                        'Daily': 'D',
                        'Monthly': 'M',
                        'Quarterly': 'Q',
                        'Yearly': 'Y'
                    }
                    freq = freq_map.get(aggregate_selector.value, '30min')
                    
                    # Keep a copy of the original 30-min data BEFORE resampling for DWA calculation
                    original_30min_price_data = price_data.copy()
                    
                    # Resample data
                    if freq != '5min':  # Only resample if not 5 minute
                        # Set SETTLEMENTDATE as index if it's not already
                        if 'SETTLEMENTDATE' in price_data.columns:
                            price_data = price_data.set_index('SETTLEMENTDATE')
                        # Build aggregation dict based on available columns
                        agg_dict = {}
                        if y_col in price_data.columns:
                            agg_dict[y_col] = 'mean'
                        if 'RRP' in price_data.columns and y_col != 'RRP':
                            agg_dict['RRP'] = 'mean'  # Keep original for reference
                        # Ensure we have something to aggregate
                        if not agg_dict:
                            agg_dict['RRP'] = 'mean'
                        price_data = price_data.groupby('REGIONID').resample(freq).agg(agg_dict).reset_index()
                    
                    # Keep a copy of the resampled data for statistics and other charts
                    original_price_data = price_data.copy()
                    
                    # Apply smoothing if selected (only for time series plot)
                    if smoothing_selector.value != 'None':
                        for region in selected_regions:
                            region_mask = price_data['REGIONID'] == region
                            if smoothing_selector.value == 'Moving Avg (7 periods)':
                                price_data.loc[region_mask, y_col] = price_data.loc[region_mask, y_col].rolling(7, center=True).mean()
                            elif smoothing_selector.value == 'Moving Avg (30 periods)':
                                price_data.loc[region_mask, y_col] = price_data.loc[region_mask, y_col].rolling(30, center=True).mean()
                            elif smoothing_selector.value == 'Exponential (α=0.3)':
                                price_data.loc[region_mask, y_col] = price_data.loc[region_mask, y_col].ewm(alpha=0.3).mean()
                            elif smoothing_selector.value.startswith('Savitzky-Golay'):
                                # Import scipy for Savitzky-Golay filter
                                from scipy.signal import savgol_filter
                                
                                # Calculate window size based on days and frequency
                                # Extract days from the option string
                                if '7 days' in smoothing_selector.value:
                                    days = 7
                                elif '30 days' in smoothing_selector.value:
                                    days = 30
                                elif '90 days' in smoothing_selector.value:
                                    days = 90
                                else:
                                    days = 7  # Default
                                
                                # Calculate periods based on frequency
                                freq = aggregate_selector.value
                                if freq == '5 min':
                                    periods_per_day = 24 * 12  # 288 periods
                                elif freq == '30 min':
                                    periods_per_day = 24 * 2  # 48 periods
                                elif freq == '1 hour':
                                    periods_per_day = 24
                                elif freq == 'Daily':
                                    periods_per_day = 1
                                elif freq == 'Monthly':
                                    # For monthly, use days directly as window
                                    window_size = min(days, 12)  # Cap at 12 months
                                    poly_order = 3
                                elif freq == 'Quarterly':
                                    # For quarterly, use quarters
                                    window_size = min(days // 30, 4)  # Approximate quarters
                                    poly_order = 2  # Lower order for fewer points
                                elif freq == 'Yearly':
                                    # For yearly, very limited smoothing
                                    window_size = 3  # Minimum for poly order 2
                                    poly_order = 2
                                else:
                                    periods_per_day = 24  # Default to hourly
                                
                                # Calculate window size for sub-daily frequencies
                                if freq in ['5 min', '1 hour', 'Daily']:
                                    window_size = days * periods_per_day
                                    poly_order = 3  # Cubic polynomial
                                    
                                    # Ensure window size is odd
                                    if window_size % 2 == 0:
                                        window_size += 1
                                    
                                    # Cap window size to be reasonable
                                    max_window = min(len(price_data.loc[region_mask]) // 2, 2001)
                                    window_size = min(window_size, max_window)
                                    
                                logger.info(f"Savitzky-Golay: {days} days at {freq} frequency = {window_size} periods (poly_order={poly_order})")
                                
                                # Apply Savitzky-Golay filter
                                try:
                                    # Get the region data
                                    region_prices = price_data.loc[region_mask, y_col].values
                                    
                                    # Check if we have enough points for the window
                                    if len(region_prices) >= window_size:
                                        smoothed = savgol_filter(
                                            region_prices,
                                            window_size,
                                            poly_order,
                                            mode='nearest'  # Handle edges by extrapolating
                                        )
                                        price_data.loc[region_mask, y_col] = smoothed
                                    else:
                                        logger.warning(f"Not enough data points ({len(region_prices)}) for Savitzky-Golay window size {window_size}")
                                except Exception as e:
                                    logger.error(f"Error applying Savitzky-Golay filter: {e}")
                                    # Fall back to original data if smoothing fails
                                    pass
                            elif smoothing_selector.value.startswith('LOESS'):
                                # Import statsmodels for LOESS
                                from statsmodels.nonparametric.smoothers_lowess import lowess
                                
                                # Extract parameters from the option string
                                if '3 hours' in smoothing_selector.value:
                                    time_desc = '3 hours'
                                    frac = 0.01
                                elif '1 day' in smoothing_selector.value:
                                    time_desc = '1 day'
                                    frac = 0.02
                                elif '7 days' in smoothing_selector.value:
                                    time_desc = '7 days'
                                    frac = 0.05
                                elif '30 days' in smoothing_selector.value:
                                    time_desc = '30 days'
                                    frac = 0.1
                                elif '90 days' in smoothing_selector.value:
                                    time_desc = '90 days'
                                    frac = 0.15
                                else:
                                    time_desc = '30 days'  # Default
                                    frac = 0.1
                                
                                logger.info(f"LOESS: {time_desc} with frac={frac} for region {region}")
                                
                                # Apply LOESS filter
                                try:
                                    # Get the region data
                                    region_data = price_data.loc[region_mask].copy()
                                    region_prices = region_data[y_col].values
                                    
                                    # Remove NaN values
                                    valid_mask = ~np.isnan(region_prices)
                                    if valid_mask.sum() == 0:
                                        logger.warning(f"No valid data for LOESS in region {region}")
                                        continue
                                    
                                    valid_prices = region_prices[valid_mask]
                                    valid_indices = np.where(valid_mask)[0]
                                    
                                    # Check if we have enough points
                                    min_points = max(3, int(frac * len(valid_prices)) + 1)
                                    if len(valid_prices) >= min_points:
                                        # Convert to numeric x values
                                        x = np.arange(len(valid_prices))
                                        
                                        logger.info(f"Applying LOESS to {len(valid_prices)} valid points with frac={frac}")
                                        
                                        # Apply LOESS
                                        smoothed_result = lowess(
                                            valid_prices, 
                                            x, 
                                            frac=frac,
                                            it=0,  # No robustness iterations for speed
                                            delta=0.01 * len(valid_prices) if len(valid_prices) > 100 else 0  # Speed optimization for large data
                                        )
                                        
                                        # Extract the smoothed y values
                                        smoothed = smoothed_result[:, 1]
                                        
                                        # Put smoothed values back in the right places
                                        smoothed_full = np.full_like(region_prices, np.nan)
                                        smoothed_full[valid_mask] = smoothed
                                        
                                        # Update the data
                                        price_data.loc[region_mask, y_col] = smoothed_full
                                        logger.info(f"LOESS applied successfully to {region}, smoothed {len(valid_prices)} points")
                                    else:
                                        logger.warning(f"Not enough valid data points ({len(valid_prices)}) for LOESS with frac={frac} in region {region}")
                                except Exception as e:
                                    logger.error(f"Error applying LOESS filter for region {region}: {e}")
                                    import traceback
                                    logger.error(traceback.format_exc())
                                    # Fall back to original data if smoothing fails
                                    pass
                            elif smoothing_selector.value.startswith('EWM'):
                                # Handle EWM smoothing options
                                # Extract days from option string
                                if '7 days' in smoothing_selector.value:
                                    days = 7
                                elif '14 days' in smoothing_selector.value:
                                    days = 14
                                elif '30 days' in smoothing_selector.value:
                                    days = 30
                                elif '60 days' in smoothing_selector.value:
                                    days = 60
                                else:
                                    days = 14  # Default

                                # Calculate span based on frequency
                                freq = aggregate_selector.value
                                if freq == '5 min':
                                    periods_per_day = 288
                                elif freq == '30 min':
                                    periods_per_day = 48
                                elif freq == '1 hour':
                                    periods_per_day = 24
                                elif freq == 'Daily':
                                    periods_per_day = 1
                                elif freq == 'Monthly':
                                    periods_per_day = 1 / 30  # Approximate
                                elif freq == 'Quarterly':
                                    periods_per_day = 1 / 90
                                elif freq == 'Yearly':
                                    periods_per_day = 1 / 365
                                else:
                                    periods_per_day = 48  # Default to 30 min

                                span = max(3, int(days * periods_per_day))
                                logger.info(f"EWM: {days} days at {freq} = span of {span} periods for region {region}")

                                try:
                                    price_data.loc[region_mask, y_col] = price_data.loc[region_mask, y_col].ewm(span=span, min_periods=1).mean()
                                    logger.info(f"EWM applied successfully to {region}")
                                except Exception as e:
                                    logger.error(f"Error applying EWM for region {region}: {e}")

                    # Define Flexoki theme colors for regions
                    region_colors = {
                        'NSW1': FLEXOKI_ACCENT['green'],    # Green
                        'QLD1': FLEXOKI_ACCENT['orange'],   # Orange
                        'SA1': FLEXOKI_ACCENT['magenta'],   # Magenta
                        'TAS1': FLEXOKI_ACCENT['cyan'],     # Cyan
                        'VIC1': FLEXOKI_ACCENT['purple']    # Purple
                    }
                    
                    # Format date range for title
                    date_range_text = ""
                    if date_presets.value == '1 day':
                        date_range_text = "Last 24 hours"
                    elif date_presets.value == '7 days':
                        date_range_text = "Last 7 days"
                    elif date_presets.value == '30 days':
                        date_range_text = "Last 30 days"
                    elif date_presets.value == '90 days':
                        date_range_text = "Last 90 days"
                    elif date_presets.value == '1 year':
                        date_range_text = "Last year"
                    elif date_presets.value == 'All data':
                        date_range_text = "All available data"
                    else:
                        # Custom date range (including when date_presets.value is None or unrecognized)
                        # This handles both None (custom dates) and any other value
                        date_range_text = f"{start_date_picker.value.strftime('%Y-%m-%d')} to {end_date_picker.value.strftime('%Y-%m-%d')}"
                    
                    # Remove any NaN values that might cause rendering gaps
                    price_data = price_data.dropna(subset=[y_col])
                    
                    # Set explicit xlim based on date pickers to prevent cross-tab interference
                    xlim = (pd.Timestamp(start_date_picker.value), 
                           pd.Timestamp(end_date_picker.value) + pd.Timedelta(days=1))
                    
                    # Create hvplot
                    plot = price_data.hvplot.line(
                        x='SETTLEMENTDATE',
                        y=y_col,
                        by='REGIONID',
                        width=1200,
                        height=400,
                        xlabel='Time',
                        ylabel=ylabel,
                        title=f'Electricity Spot Prices by Region ({date_range_text})',
                        logy=use_log,
                        grid=True,
                        color=[region_colors.get(r, '#6272a4') for r in price_data['REGIONID'].unique()],
                        line_width=2,
                        hover=True,
                        hover_cols=['REGIONID', 'RRP'],  # Show original price in hover
                        bgcolor=FLEXOKI_PAPER,  # Dracula background
                        fontsize={'title': 14, 'labels': 12, 'ticks': 10}
                    ).opts(
                        xlim=xlim,  # Set xlim in opts() for proper range control
                        toolbar='above',
                        active_tools=['pan', 'wheel_zoom'],
                        tools=['hover', 'pan', 'wheel_zoom', 'box_zoom', 'reset', 'save'],
                        show_grid=True,
                        gridstyle={'grid_line_color': FLEXOKI_BASE[100], 'grid_line_alpha': 0.3}
                    )

                    # Apply attribution hook and Flexoki background hook to the plot
                    plot = plot.opts(hooks=[self._get_attribution_hook(), self._get_flexoki_background_hook()])
                    
                    self.price_plot_pane.object = plot
                    
                    # Calculate statistics for each region using ORIGINAL unsmoothed data
                    stats_list = []
                    for region in selected_regions:
                        region_data = original_price_data[original_price_data['REGIONID'] == region]['RRP']
                        stats = region_data.describe()
                        
                        # Simplified statistics - just Mean, Max, Min
                        stats_dict = {
                            'Statistic': ['Mean', 'Max', 'Min'],
                            region: [
                                f"{stats['mean']:.0f}",
                                f"{stats['max']:.0f}",
                                f"{stats['min']:.0f}"
                            ]
                        }
                        stats_list.append(pd.DataFrame(stats_dict))
                    
                    # Merge all statistics into one DataFrame
                    if stats_list:
                        stats_df = stats_list[0]
                        for df in stats_list[1:]:
                            stats_df = stats_df.merge(df, on='Statistic', how='outer')
                        
                        # Ensure the rows are in the correct order
                        stat_order = ['Mean', 'Max', 'Min']
                        stats_df['Statistic'] = pd.Categorical(stats_df['Statistic'], categories=stat_order, ordered=True)
                        stats_df = stats_df.sort_values('Statistic').reset_index(drop=True)
                        
                        # Store stats_df temporarily - we'll add fuel prices to it
                        base_stats_df = stats_df.copy()
                        
                        # Update statistics title
                        self.stats_title_pane.object = f"### Price Statistics ({date_range_text})"
                    
                    # Calculate fuel-weighted prices
                    try:
                        logger.info("Calculating fuel-weighted prices...")
                        
                        # Load DUID mapping to get fuel types
                        import pickle
                        with open(GEN_INFO_FILE, 'rb') as f:
                            duid_mapping = pickle.load(f)
                        
                        # Rename columns to match expected format
                        if 'Fuel' in duid_mapping.columns:
                            duid_mapping = duid_mapping.rename(columns={'Fuel': 'FUEL_TYPE', 'Region': 'REGIONID'})
                        
                        # Get generation data that's already loaded
                        # Use the same date range as the price data
                        if hasattr(self, 'generation_query_manager'):
                            gen_data = self.generation_query_manager.query_generation_by_fuel(
                                start_datetime,
                                end_datetime,
                                selected_regions
                            )
                        else:
                            # Fall back to loading directly if query manager not available
                            from ..shared.generation_adapter import load_generation_data
                            gen_data = load_generation_data(
                                start_date=start_datetime,
                                end_date=end_datetime,
                                region='NEM' if 'NEM' in selected_regions else selected_regions[0],
                                resolution='auto'
                            )
                        
                        if not gen_data.empty and not original_price_data.empty:
                            # Ensure consistent column names
                            if 'settlementdate' in gen_data.columns:
                                gen_data = gen_data.rename(columns={'settlementdate': 'SETTLEMENTDATE'})
                            if 'duid' in gen_data.columns:
                                gen_data = gen_data.rename(columns={'duid': 'DUID'})
                            if 'scadavalue' in gen_data.columns:
                                gen_data = gen_data.rename(columns={'scadavalue': 'SCADAVALUE'})
                            
                            # Add region info from DUID mapping
                            if 'REGIONID' not in gen_data.columns and 'DUID' in gen_data.columns:
                                gen_data = gen_data.merge(
                                    duid_mapping[['DUID', 'REGIONID', 'FUEL_TYPE']], 
                                    on='DUID', 
                                    how='left'
                                )
                            
                            # Filter for selected regions
                            gen_data = gen_data[gen_data['REGIONID'].isin(selected_regions)]
                            
                            # Map detailed fuel types to consolidated categories
                            fuel_type_mapping = {
                                'Battery Storage': 'Battery',
                                'OCGT': 'Gas',
                                'CCGT': 'Gas', 
                                'Gas other': 'Gas',
                                'Water': 'Hydro',
                                'Coal': 'Coal',
                                'Wind': 'Wind',
                                'Solar': 'Solar',
                                'Biomass': 'Other',
                                'Other': 'Other',
                                '': 'Other'
                            }
                            
                            # Apply fuel type mapping to consolidate gas types
                            gen_data['FUEL_TYPE_CONSOLIDATED'] = gen_data['FUEL_TYPE'].map(fuel_type_mapping).fillna('Other')
                            
                            # Define the fuel types we want to display
                            fuel_display_order = ['Battery', 'Gas', 'Hydro', 'Coal', 'Wind', 'Solar']
                            
                            # Calculate fuel-weighted prices for each region
                            fuel_price_results = []
                            
                            # Dictionary to store fuel prices by region
                            fuel_prices_by_region = {}
                            
                            for region in selected_regions:
                                fuel_prices_by_region[region] = {}
                                region_gen_data = gen_data[gen_data['REGIONID'] == region].copy()
                                region_price_data = original_price_data[original_price_data['REGIONID'] == region].copy()
                                
                                # For aggregated frequencies, we need original 30-min data for correct DWA
                                # Don't resample generation data yet - we'll handle it during DWA calculation
                                original_region_gen_data = region_gen_data.copy()
                                # Use the 30-min price data that was saved before resampling
                                original_region_price_data = original_30min_price_data[original_30min_price_data['REGIONID'] == region].copy()
                                
                                # Only resample for display purposes if needed (not for DWA calculation)
                                if not region_gen_data.empty and not region_price_data.empty:
                                    # Get the frequency of the price data
                                    price_periods = len(region_price_data['SETTLEMENTDATE'].unique())
                                    gen_periods = len(region_gen_data['SETTLEMENTDATE'].unique())
                                    
                                    # If price has fewer periods, it's been resampled - resample generation to match for display
                                    if price_periods < gen_periods and freq != '5min' and freq != '30min':
                                        logger.info(f"Resampling generation data to {freq} for display purposes")
                                        # Group by FUEL_TYPE first, then resample
                                        region_gen_data = region_gen_data.groupby(['FUEL_TYPE_CONSOLIDATED', pd.Grouper(key='SETTLEMENTDATE', freq=freq)]).agg({
                                            'SCADAVALUE': 'sum',  # Sum generation when aggregating
                                            'DUID': 'first',  # Keep first DUID for reference
                                            'REGIONID': 'first'  # Keep region
                                        }).reset_index()
                                
                                if not region_gen_data.empty and not region_price_data.empty:
                                    # Ensure datetime format
                                    region_gen_data['SETTLEMENTDATE'] = pd.to_datetime(region_gen_data['SETTLEMENTDATE'])
                                    region_price_data['SETTLEMENTDATE'] = pd.to_datetime(region_price_data['SETTLEMENTDATE'])
                                    
                                    for fuel_type in fuel_display_order:
                                        # For DWA calculation with aggregated frequencies, use original 30-min data
                                        if freq in ['D', 'M', 'Q', 'Y']:
                                            # Use original non-resampled data for correct DWA
                                            fuel_gen = original_region_gen_data[original_region_gen_data['FUEL_TYPE_CONSOLIDATED'] == fuel_type].copy()
                                            use_original_prices = True
                                        else:
                                            # Use resampled data for non-aggregated frequencies
                                            fuel_gen = region_gen_data[region_gen_data['FUEL_TYPE_CONSOLIDATED'] == fuel_type].copy()
                                            use_original_prices = False
                                        
                                        if not fuel_gen.empty:
                                            # For batteries, only consider discharge (positive values)
                                            if fuel_type == 'Battery':
                                                original_len = len(fuel_gen)
                                                fuel_gen = fuel_gen[fuel_gen['SCADAVALUE'] > 0].copy()
                                                logger.info(f"Battery filtering for {region}: {original_len} records -> {len(fuel_gen)} discharge records")
                                            
                                            # Aggregate generation by settlement date
                                            if not fuel_gen.empty:
                                                fuel_gen_agg = fuel_gen.groupby('SETTLEMENTDATE')['SCADAVALUE'].sum().reset_index()
                                            else:
                                                fuel_gen_agg = pd.DataFrame()
                                            
                                            # Merge with prices if we have data
                                            if not fuel_gen_agg.empty:
                                                if use_original_prices:
                                                    # For aggregated frequencies, use original 30-min prices
                                                    # Need to get original prices before they were resampled
                                                    merged = pd.merge(
                                                        fuel_gen_agg,
                                                        original_region_price_data[['SETTLEMENTDATE', 'RRP']],
                                                        on='SETTLEMENTDATE',
                                                        how='inner'
                                                    )
                                                else:
                                                    merged = pd.merge(
                                                        fuel_gen_agg,
                                                        region_price_data[['SETTLEMENTDATE', 'RRP']],
                                                        on='SETTLEMENTDATE',
                                                        how='inner'
                                                    )
                                            else:
                                                merged = pd.DataFrame()
                                            
                                            if not merged.empty and merged['SCADAVALUE'].sum() > 0:
                                                # For dispatch-weighted average:
                                                # DWA = Total Revenue / Total Energy
                                                
                                                if freq in ['D', 'M', 'Q', 'Y']:
                                                    # For aggregated frequencies, we have original 30-min data
                                                    # Calculate correctly: sum(MW * price * 0.5) / sum(MW * 0.5)
                                                    hours_per_period = 0.5  # Original data is 30-minute
                                                    revenue = (merged['SCADAVALUE'] * merged['RRP'] * hours_per_period).sum()
                                                    energy = (merged['SCADAVALUE'] * hours_per_period).sum()
                                                else:
                                                    # For 5min, 30min, hourly - original logic
                                                    if len(merged) > 1:
                                                        time_diff = merged['SETTLEMENTDATE'].iloc[1] - merged['SETTLEMENTDATE'].iloc[0]
                                                        hours_per_period = time_diff.total_seconds() / 3600
                                                    else:
                                                        hours_per_period = 0.5  # Default to 30-min
                                                    
                                                    # Calculate revenue and energy with proper time interval
                                                    revenue = (merged['SCADAVALUE'] * merged['RRP'] * hours_per_period).sum()
                                                    energy = (merged['SCADAVALUE'] * hours_per_period).sum()
                                                
                                                weighted_price = revenue / energy if energy > 0 else 0
                                                fuel_prices_by_region[region][fuel_type] = f"{weighted_price:.0f}"
                                            else:
                                                fuel_prices_by_region[region][fuel_type] = "-"
                                        else:
                                            fuel_prices_by_region[region][fuel_type] = "-"
                            
                            # Add fuel prices to the main stats table
                            if fuel_prices_by_region and 'base_stats_df' in locals():
                                # Create rows for each fuel type
                                fuel_rows = []
                                for fuel_type in fuel_display_order:
                                    fuel_row = {'Statistic': fuel_type}
                                    for region in selected_regions:
                                        fuel_row[region] = fuel_prices_by_region.get(region, {}).get(fuel_type, "-")
                                    fuel_rows.append(fuel_row)
                                
                                # Create fuel prices DataFrame
                                fuel_prices_df = pd.DataFrame(fuel_rows)
                                
                                # Combine with base stats
                                combined_stats_df = pd.concat([base_stats_df, fuel_prices_df], ignore_index=True)
                                
                                # Update the main stats table with combined data
                                self.stats_pane.value = combined_stats_df
                                logger.info(f"Combined statistics and fuel prices: \n{combined_stats_df}")
                            else:
                                # Just show base stats if fuel prices couldn't be calculated
                                if 'base_stats_df' in locals():
                                    self.stats_pane.value = base_stats_df
                        else:
                            logger.warning("No generation data for selected period")
                            
                    except Exception as e:
                        logger.error(f"Error calculating fuel-weighted prices: {e}", exc_info=True)
                        # If error, just show the base stats
                        if 'base_stats_df' in locals():
                            self.stats_pane.value = base_stats_df
                    
                    # Calculate price band contributions using ORIGINAL unsmoothed data
                    price_bands = [
                        ('Below $0', -float('inf'), 0),
                        ('$0-$300', 0, 300),
                        ('$301-$1000', 301, 1000),
                        ('Above $1000', 1000, float('inf'))
                    ]
                    
                    band_contributions = []
                    
                    for region in selected_regions:
                        # Always use RRP for price bands calculation (original unmodified prices)
                        if 'RRP' not in original_price_data.columns:
                            logger.warning(f"RRP column not found in original_price_data. Columns: {original_price_data.columns.tolist()}")
                            continue
                        region_data = original_price_data[original_price_data['REGIONID'] == region]['RRP']
                        mean_price = region_data.mean()
                        
                        for band_name, low, high in price_bands:
                            # Count periods in this band
                            if low == -float('inf'):
                                band_mask = region_data < high
                            elif high == float('inf'):
                                band_mask = region_data >= low
                            else:
                                band_mask = (region_data >= low) & (region_data < high)
                            
                            band_data = region_data[band_mask]
                            
                            if len(band_data) > 0:
                                # Calculate weighted contribution
                                band_proportion = len(band_data) / len(region_data)
                                band_avg = band_data.mean()
                                # Weighted contribution to overall mean
                                contribution = (band_proportion * band_avg)
                                
                                band_contributions.append({
                                    'Region': region,
                                    'Price Band': band_name,
                                    'Contribution': contribution,
                                    'Percentage': band_proportion * 100,
                                    'Band Average': band_avg
                                })
                    
                    if band_contributions:
                        # Create DataFrame for plotting
                        bands_df = pd.DataFrame(band_contributions)
                        
                        # Define the order of price bands for consistent coloring
                        band_order = ['Below $0', '$0-$300', '$301-$1000', 'Above $1000']
                        
                        # Convert Price Band to categorical with specified order
                        bands_df['Price Band'] = pd.Categorical(bands_df['Price Band'], categories=band_order, ordered=True)
                        
                        # Sort by Price Band to ensure consistent ordering
                        bands_df = bands_df.sort_values(['Region', 'Price Band'])
                        
                        # Create butterfly chart showing Time % (left) vs Contribution $/MWh (right)
                        butterfly_fig = self.create_price_band_butterfly_chart(
                            bands_df, selected_regions, date_range_text
                        )
                        
                        # Create price band details table for all bands
                        high_price_rows = []
                        
                        # Calculate mean price for each region to compute contribution percentages
                        region_means = {}
                        for region in selected_regions:
                            region_data = original_price_data[original_price_data['REGIONID'] == region]['RRP']
                            region_means[region] = region_data.mean()
                        
                        # Calculate total hours in the period for revenue calculation
                        # Determine if we're using 5-minute or 30-minute data
                        total_periods = len(original_price_data[original_price_data['REGIONID'] == selected_regions[0]])
                        if total_periods > 0:
                            # Check time difference between first two periods to determine resolution
                            sorted_times = original_price_data[original_price_data['REGIONID'] == selected_regions[0]]['SETTLEMENTDATE'].sort_values()
                            if len(sorted_times) > 1:
                                time_diff = pd.to_datetime(sorted_times.iloc[1]) - pd.to_datetime(sorted_times.iloc[0])
                                minutes_per_period = time_diff.total_seconds() / 60
                                periods_per_hour = 60 / minutes_per_period
                            else:
                                # Default to 30-minute periods if can't determine
                                periods_per_hour = 2
                        else:
                            periods_per_hour = 2
                        
                        total_hours = total_periods / periods_per_hour

                        # Calculate actual average demand per region from generation data
                        # This ensures revenue calculations reflect real regional demand patterns
                        region_avg_demand = {}
                        try:
                            # Query generation data for the selected time period
                            gen_data = self.query_manager.query_generation_by_fuel(
                                start_date=start_datetime,
                                end_date=end_datetime,
                                regions=selected_regions,
                                aggregation='raw'  # Get raw data to calculate accurate averages
                            )

                            if not gen_data.empty:
                                # Sum all generation columns (MW) for each timestamp and region
                                # Exclude transmission and storage columns
                                excluded_cols = ['timestamp', 'region', 'Transmission Flow',
                                               'Transmission Exports', 'Battery Storage']
                                gen_cols = [col for col in gen_data.columns if col not in excluded_cols]

                                # Calculate total generation per timestamp per region
                                gen_data['Total_MW'] = gen_data[gen_cols].sum(axis=1)

                                # Calculate average demand (MW) for each region
                                for region in selected_regions:
                                    region_data = gen_data[gen_data['region'] == region]
                                    if not region_data.empty:
                                        region_avg_demand[region] = region_data['Total_MW'].mean()
                                        logger.info(f"Calculated average demand for {region}: {region_avg_demand[region]:.0f} MW")
                                    else:
                                        # Fallback to reasonable estimate if no data
                                        region_avg_demand[region] = 1500
                                        logger.warning(f"No generation data for {region}, using fallback 1500 MW")
                            else:
                                # If no generation data, use reasonable regional estimates
                                logger.warning("No generation data available for demand calculation, using estimates")
                                default_demands = {
                                    'NSW1': 7500, 'QLD1': 6500, 'VIC1': 5500,
                                    'SA1': 1500, 'TAS1': 1000
                                }
                                for region in selected_regions:
                                    region_avg_demand[region] = default_demands.get(region, 1500)
                        except Exception as e:
                            logger.warning(f"Error calculating regional demand: {e}, using estimates")
                            # Fallback to reasonable regional estimates
                            default_demands = {
                                'NSW1': 7500, 'QLD1': 6500, 'VIC1': 5500,
                                'SA1': 1500, 'TAS1': 1000
                            }
                            for region in selected_regions:
                                region_avg_demand[region] = default_demands.get(region, 1500)

                        # Build price band data for HTML table
                        # Collect data by region for the HTML table generator
                        region_band_data = {}

                        for _, row in bands_df.iterrows():
                            region = row['Region']
                            if region not in region_band_data:
                                region_band_data[region] = []

                            # Calculate percentage contribution to mean (Share %)
                            mean_price = region_means[region]
                            pct_contribution = (row['Contribution'] / mean_price) * 100 if mean_price > 0 else 0

                            # Calculate revenue in this band
                            hours_in_band = (row['Percentage'] / 100) * total_hours
                            avg_demand_mw = region_avg_demand.get(region, 1500)
                            revenue_millions = (row['Band Average'] * hours_in_band * avg_demand_mw) / 1_000_000

                            # Store numeric values for HTML table
                            region_band_data[region].append({
                                'Price Band': row['Price Band'],
                                'Contr': row['Contribution'],
                                'Share': pct_contribution,
                                'Time': row['Percentage'],
                                'Avg': row['Band Average'],
                                'Revenue': revenue_millions,
                            })

                        # Generate HTML table and update pane
                        if region_band_data:
                            table_html = self.create_price_band_table_html(region_band_data, selected_regions)
                            self.high_price_events_pane.object = table_html
                        else:
                            self.high_price_events_pane.object = "<p><em>No price band data in selected period</em></p>"
                        
                        # Clear the column and add butterfly chart
                        self.bands_plot_pane.clear()

                        # Wrap matplotlib figure in a pane and add to column
                        import matplotlib.pyplot as plt
                        butterfly_pane = pn.pane.Matplotlib(butterfly_fig, sizing_mode='stretch_width', tight=True)
                        plt.close(butterfly_fig)  # Close to free memory

                        self.bands_plot_pane.append(butterfly_pane)
                    else:
                        # No band contributions data - show message
                        self.bands_plot_pane.clear()
                        no_data_message = pn.pane.HoloViews(
                            hv.Text(0.5, 0.5, 'No price band data available\nPlease select regions and click "Analyze Prices"').opts(
                                xlim=(0, 1), ylim=(0, 1), 
                                bgcolor=FLEXOKI_PAPER, color=FLEXOKI_BLACK, fontsize=14
                            ),
                            sizing_mode='stretch_width',
                            height=550
                        )
                        self.bands_plot_pane.append(no_data_message)
                        self.high_price_events_pane.object = "<p><em>No price band data available</em></p>"
                    
                    # Fuel relatives are now handled independently by update_fuel_relatives()
                    # which is triggered by the region selector change
                    
                    # Create time-of-day analysis using ORIGINAL unsmoothed data
                    # Extract hour from SETTLEMENTDATE
                    original_price_data['Hour'] = pd.to_datetime(original_price_data['SETTLEMENTDATE']).dt.hour
                    
                    # Calculate average price by hour for each region
                    tod_data = original_price_data.groupby(['Hour', 'REGIONID'])['RRP'].mean().reset_index()
                    tod_data.rename(columns={'RRP': 'Average Price'}, inplace=True)
                    
                    # Create time-of-day plot
                    # Create color list for time-of-day chart
                    tod_colors = [region_colors.get(r, '#6272a4') for r in tod_data['REGIONID'].unique()]
                    
                    tod_plot = tod_data.hvplot.line(
                        x='Hour',
                        y='Average Price',
                        by='REGIONID',
                        width=400,
                        height=400,
                        xlabel='Hour of Day',
                        ylabel='Average Price ($/MWh)',
                        title=f'Average Price by Hour ({date_range_text})',
                        color=tod_colors,
                        bgcolor=FLEXOKI_PAPER,
                        legend='top_right',
                        xticks=list(range(0, 24, 3)),  # Show every 3 hours
                        toolbar='above',
                        grid=True
                    ).opts(
                        show_grid=True,
                        gridstyle={'grid_line_color': FLEXOKI_BASE[100], 'grid_line_alpha': 0.3},
                        fontsize={'xlabel': 10, 'ylabel': 10, 'ticks': 9}
                    )

                    # Apply attribution hook and Flexoki background hook to the plot
                    tod_plot = tod_plot.opts(hooks=[self._get_attribution_hook(), self._get_flexoki_background_hook()])

                    self.tod_plot_pane.object = tod_plot
                    
                except Exception as e:
                    logger.error(f"Error loading price data: {e}", exc_info=True)
                    error_msg = f'Error: {str(e)}'
                    self.price_plot_pane.object = hv.Text(0.5, 0.5, error_msg).opts(
                        xlim=(0, 1), ylim=(0, 1), 
                        bgcolor=FLEXOKI_PAPER, color=FLEXOKI_ACCENT['red'], fontsize=14
                    )
                    # Clear statistics table with error message
                    self.stats_pane.value = pd.DataFrame({'Error': [str(e)]})
                    # Clear high price events table
                    self.high_price_events_pane.object = f"<p><em>Error: {str(e)}</em></p>"
                    # Clear bands plot
                    self.bands_plot_pane.object = hv.Text(0.5, 0.5, 'Error loading data').opts(
                        xlim=(0, 1), ylim=(0, 1), 
                        bgcolor=FLEXOKI_PAPER, color=FLEXOKI_ACCENT['red'], fontsize=14
                    )
                    # Clear time-of-day plot
                    self.tod_plot_pane.object = hv.Text(0.5, 0.5, 'Error loading data').opts(
                        xlim=(0, 1), ylim=(0, 1), 
                        bgcolor=FLEXOKI_PAPER, color=FLEXOKI_ACCENT['red'], fontsize=14
                    )
            
            # Set up callbacks for UI updates only
            date_presets.param.watch(update_date_range, 'value')
            start_date_picker.param.watch(update_date_display, 'value')
            end_date_picker.param.watch(update_date_display, 'value')

            # Mark controls dirty when any input changes
            region_selector.param.watch(mark_controls_dirty, 'value')
            aggregate_selector.param.watch(mark_controls_dirty, 'value')
            date_presets.param.watch(mark_controls_dirty, 'value')
            start_date_picker.param.watch(mark_controls_dirty, 'value')
            end_date_picker.param.watch(mark_controls_dirty, 'value')
            smoothing_selector.param.watch(mark_controls_dirty, 'value')
            log_scale_checkbox.param.watch(mark_controls_dirty, 'value')

            # Set up button click handler - mark clean after analysis
            def on_analyze_click(event):
                load_and_plot_prices()
                mark_controls_clean()

            analyze_button.on_click(on_analyze_click)
            
            # Create standalone function for fuel relatives calculation
            def update_fuel_relatives(event=None):
                """Update fuel relatives plot - completely independent of other price analysis"""
                try:
                    
                    selected_fuel_region = fuel_relatives_region_selector.value
                    logger.info(f"Updating fuel relatives for region: {selected_fuel_region}")
                    
                    # Show loading message
                    loading_fig = go.Figure().add_annotation(
                        text=f"Loading 5.5 years of data for {selected_fuel_region}...<br>This may take 20-30 seconds",
                        xref='paper', yref='paper', x=0.5, y=0.5, showarrow=False,
                        font=dict(size=14, color=FLEXOKI_ACCENT['green'])
                    ).update_layout(
                        paper_bgcolor=FLEXOKI_PAPER, plot_bgcolor=FLEXOKI_PAPER,
                        xaxis=dict(visible=False), yaxis=dict(visible=False)
                    )
                    self.fuel_relatives_plot_pane.object = loading_fig
                    
                    # Load ALL available data for long-term trend analysis
                    import sys
                    from pathlib import Path
                    sys.path.append(str(Path(__file__).parent.parent.parent))
                    from data_service.shared_data_duckdb import duckdb_data_service
                    
                    # Use full date range - approximately 5.5 years of data
                    start_dt = pd.to_datetime('2020-01-01')
                    end_dt = pd.to_datetime('now')
                    
                    logger.info(f"Loading data from {start_dt} to {end_dt} for fuel relatives")
                    
                    # Use DuckDB to efficiently calculate daily fuel-weighted prices
                    # This aggregates at the database level instead of loading raw data
                    query = f"""
                        WITH daily_generation AS (
                            SELECT 
                                DATE(g.settlementdate) as date,
                                CASE 
                                    WHEN d.Fuel IN ('Gas', 'Gas other', 'CCGT', 'OCGT') THEN 'Gas'
                                    ELSE d.Fuel
                                END as fuel_type,
                                g.duid,
                                SUM(g.scadavalue) as daily_generation
                            FROM generation_30min g
                            JOIN duid_mapping d ON g.duid = d.DUID
                            WHERE g.settlementdate >= '{start_dt.isoformat()}'
                              AND g.settlementdate <= '{end_dt.isoformat()}'
                              AND d.Region = '{selected_fuel_region}'
                              AND d.Fuel IN ('Coal', 'Gas', 'Gas other', 'Wind', 'Solar', 'Water', 'CCGT', 'OCGT', 'Battery Storage')
                              AND NOT (d.Fuel = 'Battery Storage' AND g.scadavalue < 0)  -- Exclude battery charging
                            GROUP BY DATE(g.settlementdate), 
                                     CASE 
                                         WHEN d.Fuel IN ('Gas', 'Gas other', 'CCGT', 'OCGT') THEN 'Gas'
                                         ELSE d.Fuel
                                     END, 
                                     g.duid
                        ),
                        daily_prices AS (
                            SELECT 
                                settlementdate,
                                DATE(settlementdate) as date,
                                rrp
                            FROM prices_30min
                            WHERE settlementdate >= '{start_dt.isoformat()}'
                              AND settlementdate <= '{end_dt.isoformat()}'
                              AND regionid = '{selected_fuel_region}'
                        ),
                        fuel_weighted AS (
                            SELECT 
                                dp.date,
                                dg.fuel_type,
                                SUM(g.scadavalue * dp.rrp) / NULLIF(SUM(g.scadavalue), 0) as weighted_price
                            FROM generation_30min g
                            JOIN duid_mapping d ON g.duid = d.DUID
                            JOIN daily_prices dp ON g.settlementdate = dp.settlementdate
                            JOIN daily_generation dg ON DATE(g.settlementdate) = dg.date 
                                AND CASE 
                                    WHEN d.Fuel IN ('Gas', 'Gas other', 'CCGT', 'OCGT') THEN 'Gas'
                                    ELSE d.Fuel
                                END = dg.fuel_type 
                                AND g.duid = dg.duid
                            WHERE g.settlementdate >= '{start_dt.isoformat()}'
                              AND g.settlementdate <= '{end_dt.isoformat()}'
                              AND d.Region = '{selected_fuel_region}'
                              AND NOT (d.Fuel = 'Battery Storage' AND g.scadavalue < 0)  -- Exclude battery charging
                            GROUP BY dp.date, dg.fuel_type
                        ),
                        flat_load AS (
                            SELECT 
                                DATE(settlementdate) as date,
                                AVG(rrp) as flat_load_price
                            FROM prices_30min
                            WHERE settlementdate >= '{start_dt.isoformat()}'
                              AND settlementdate <= '{end_dt.isoformat()}'
                              AND regionid = '{selected_fuel_region}'
                            GROUP BY DATE(settlementdate)
                        )
                        SELECT 
                            COALESCE(f.date, fw.date) as date,
                            f.flat_load_price,
                            fw.fuel_type,
                            fw.weighted_price
                        FROM flat_load f
                        FULL OUTER JOIN fuel_weighted fw ON f.date = fw.date
                        ORDER BY date, fuel_type
                    """
                    
                    logger.info("Executing optimized DuckDB query for fuel relatives...")
                    result_df = duckdb_data_service.conn.execute(query).df()
                    
                    if result_df.empty:
                        no_data_fig = go.Figure().add_annotation(
                            text=f'No data available for {selected_fuel_region}',
                            xref='paper', yref='paper', x=0.5, y=0.5, showarrow=False,
                            font=dict(size=14, color=FLEXOKI_BLACK)
                        ).update_layout(
                            paper_bgcolor=FLEXOKI_PAPER, plot_bgcolor=FLEXOKI_PAPER,
                            xaxis=dict(visible=False), yaxis=dict(visible=False)
                        )
                        self.fuel_relatives_plot_pane.object = no_data_fig
                        self.price_index_plot_pane.object = no_data_fig
                        return
                    
                    logger.info(f"Query returned {len(result_df)} rows")
                    
                    # Process the query results into the format needed for plotting
                    # Pivot the fuel-weighted prices
                    fuel_pivot = result_df[result_df['fuel_type'].notna()].pivot(
                        index='date', 
                        columns='fuel_type', 
                        values='weighted_price'
                    )
                    
                    # Get flat load prices (one per day)
                    flat_load = result_df[['date', 'flat_load_price']].drop_duplicates('date').set_index('date')
                    flat_load.columns = ['Flat Load']
                    
                    # Combine flat load and fuel-weighted prices
                    daily_prices = flat_load.join(fuel_pivot)
                    
                    # Fill any missing dates with NaN (in case there are gaps in the data)
                    # This ensures we have a continuous date range for smoothing
                    date_range = pd.date_range(start=daily_prices.index.min(), 
                                              end=daily_prices.index.max(), 
                                              freq='D')
                    daily_prices = daily_prices.reindex(date_range)
                    
                    # Log data size for debugging
                    logger.info(f"Daily prices shape: {daily_prices.shape}")
                    logger.info(f"Date range: {daily_prices.index.min()} to {daily_prices.index.max()}")
                    logger.info(f"Number of days: {len(daily_prices)}")
                    
                    # Apply 90-day LOESS smoothing
                    # For sparse data (like OCGT, Battery Storage), we'll use interpolation first
                    from statsmodels.nonparametric.smoothers_lowess import lowess
                    smoothed_data = pd.DataFrame(index=daily_prices.index)
                    
                    for col in daily_prices.columns:
                        y = daily_prices[col].values.copy()  # Make a copy to avoid modifying original
                        
                        # For Battery Storage, handle zero values as missing data
                        if col == 'Battery Storage':
                            # Convert zeros to NaN for battery storage (no battery = no data, not zero price)
                            y[y == 0] = np.nan
                        
                        mask = ~np.isnan(y)
                        valid_count = mask.sum()
                        logger.info(f"Column {col}: {valid_count} valid points out of {len(y)}")
                        
                        # For Battery Storage, trim data to start from first valid point
                        if col == 'Battery Storage' and valid_count > 0:
                            first_valid_idx = np.where(mask)[0][0]
                            # Set all values before first valid data point to NaN
                            y[:first_valid_idx] = np.nan
                            mask = ~np.isnan(y)
                            valid_count = mask.sum()
                            logger.info(f"  Battery Storage: Trimmed to start from index {first_valid_idx} ({daily_prices.index[first_valid_idx]})")
                        
                        if valid_count > 90:  # Need at least 90 points for meaningful smoothing
                            # For sparse data, first interpolate to fill gaps
                            # This helps with fuels that don't run every day
                            series = pd.Series(y, index=daily_prices.index)
                            
                            # Use forward fill then backward fill for small gaps (up to 7 days)
                            # This is especially important for Battery Storage which may have gaps
                            filled_series = series.ffill(limit=7).bfill(limit=7)
                            
                            # Apply 90-day LOESS smoothing
                            try:
                                # Get filled values and mask
                                filled_values = filled_series.values
                                filled_mask = ~np.isnan(filled_values)
                                
                                if filled_mask.sum() > 90:
                                    # Create numeric x values for LOESS
                                    x_numeric = np.arange(len(filled_values))
                                    
                                    # Calculate fraction for 90-day window
                                    frac = min(90.0 / filled_mask.sum(), 0.5)
                                    logger.info(f"  Applying LOESS with frac={frac:.3f} for {col}")
                                    
                                    # Apply LOESS smoothing only on non-NaN values
                                    smoothed = lowess(filled_values[filled_mask], 
                                                    x_numeric[filled_mask], 
                                                    frac=frac, 
                                                    it=0)
                                    
                                    # Map smoothed values back to full array
                                    smoothed_values = np.full(len(filled_values), np.nan)
                                    smoothed_values[filled_mask] = smoothed[:, 1]
                                    
                                    # Final interpolation for any remaining gaps
                                    smoothed_series = pd.Series(smoothed_values, index=daily_prices.index)
                                    smoothed_series = smoothed_series.interpolate(method='linear', limit_direction='both')
                                    
                                    # For Battery Storage, ensure NaN values before first valid data are preserved
                                    if col == 'Battery Storage':
                                        original_mask = ~np.isnan(daily_prices[col].values.copy())
                                        if original_mask.sum() > 0:
                                            first_valid = np.where(original_mask)[0][0]
                                            smoothed_series.iloc[:first_valid] = np.nan
                                    
                                    smoothed_data[col] = smoothed_series
                                    logger.info(f"  Applied 90-day LOESS smoothing to {col}")
                                else:
                                    logger.info(f"  Not enough data after filling for {col}, using raw values")
                                    smoothed_data[col] = filled_series
                            except Exception as e:
                                logger.warning(f"  LOESS failed for {col}: {e}, using interpolation only")
                                smoothed_data[col] = filled_series
                        elif col == 'Battery Storage' and valid_count > 0:
                            # For Battery Storage with limited data, still include but don't smooth
                            logger.info(f"  Battery Storage has data but not enough for smoothing ({valid_count} points)")
                            series = pd.Series(y, index=daily_prices.index)
                            smoothed_data[col] = series
                        else:
                            logger.info(f"  Insufficient data for {col} ({valid_count} points), skipping")
                    
                    # Create plots
                    logger.info(f"Smoothed data shape: {smoothed_data.shape}")
                    logger.info(f"Smoothed data columns: {smoothed_data.columns.tolist()}")
                    logger.info(f"Smoothed data empty: {smoothed_data.empty}")
                    logger.info(f"Smoothed data has any non-NaN: {smoothed_data.notna().any().any()}")

                    if not smoothed_data.empty and smoothed_data.notna().any().any():

                        # Define custom colors for fuel types
                        fuel_color_map = {
                            'Flat Load': FLEXOKI_BASE[600],  # Gray for flat load (will be dotted)
                            'Wind': FLEXOKI_ACCENT['green'],  # Green for wind
                            'Solar': FLEXOKI_ACCENT['yellow'],  # Yellow for solar
                            'Coal': FLEXOKI_BASE[400],  # Gray for coal
                            'Gas': FLEXOKI_ACCENT['red'],  # Red for gas
                            'Water': FLEXOKI_ACCENT['cyan'],  # Cyan for water/hydro
                            'Battery Storage': FLEXOKI_ACCENT['purple']  # Purple for battery
                        }

                        # Create Plotly figure for fuel-weighted prices
                        fig1 = go.Figure()

                        for col in smoothed_data.columns:
                            color = fuel_color_map.get(col, FLEXOKI_BASE[500])
                            dash = 'dot' if col == 'Flat Load' else 'solid'
                            fig1.add_trace(go.Scatter(
                                x=smoothed_data.index,
                                y=smoothed_data[col],
                                mode='lines',
                                name=col,
                                line=dict(color=color, width=2, dash=dash),
                                hovertemplate=f'{col}: $%{{y:.0f}}/MWh<extra></extra>'
                            ))

                        fig1.update_layout(
                            title=f'90-Day LOESS Smoothed Fuel-Weighted Prices - {selected_fuel_region}',
                            xaxis_title='Date',
                            yaxis_title='Price ($/MWh)',
                            paper_bgcolor=FLEXOKI_PAPER,
                            plot_bgcolor=FLEXOKI_PAPER,
                            font=dict(color=FLEXOKI_BLACK),
                            legend=dict(orientation='h', yanchor='bottom', y=1.02, xanchor='right', x=1),
                            hovermode='x unified',
                            margin=dict(l=60, r=20, t=60, b=40),
                            annotations=[dict(
                                text='Data: AEMO',
                                xref='paper', yref='paper',
                                x=1, y=-0.12,
                                showarrow=False,
                                font=dict(size=9, color=FLEXOKI_BASE[600], style='italic')
                            )]
                        )
                        fig1.update_xaxes(gridcolor=FLEXOKI_BASE[100], showgrid=True)
                        fig1.update_yaxes(gridcolor=FLEXOKI_BASE[100], showgrid=True)

                        self.fuel_relatives_plot_pane.object = fig1

                        # Now calculate price band contributions for the second chart
                        # Query daily price band contributions
                        band_query = f"""
                            WITH daily_bands AS (
                                SELECT
                                    DATE(settlementdate) as date,
                                    CASE
                                        WHEN rrp < 0 THEN 'Below $0'
                                        WHEN rrp >= 0 AND rrp < 300 THEN '$0-$300'
                                        WHEN rrp >= 300 AND rrp < 1000 THEN '$301-$1000'
                                        ELSE 'Above $1000'
                                    END as price_band,
                                    rrp,
                                    COUNT(*) OVER (PARTITION BY DATE(settlementdate)) as daily_count
                                FROM prices_30min
                                WHERE settlementdate >= '{start_dt.isoformat()}'
                                  AND settlementdate <= '{end_dt.isoformat()}'
                                  AND regionid = '{selected_fuel_region}'
                            )
                            SELECT
                                date,
                                price_band,
                                -- Contribution = (count_in_band / total_count) * avg_price_in_band
                                (COUNT(*) * 1.0 / MAX(daily_count)) * AVG(rrp) as contribution
                            FROM daily_bands
                            GROUP BY date, price_band
                            ORDER BY date, price_band
                        """

                        logger.info("Executing price band contribution query...")
                        band_result_df = duckdb_data_service.conn.execute(band_query).df()

                        if not band_result_df.empty:
                            # Pivot to get bands as columns
                            band_pivot = band_result_df.pivot(
                                index='date',
                                columns='price_band',
                                values='contribution'
                            ).fillna(0)

                            # Ensure consistent column order
                            band_order = ['Below $0', '$0-$300', '$301-$1000', 'Above $1000']
                            for band in band_order:
                                if band not in band_pivot.columns:
                                    band_pivot[band] = 0
                            band_pivot = band_pivot[band_order]

                            # Reindex to fill missing dates
                            date_range = pd.date_range(start=band_pivot.index.min(),
                                                      end=band_pivot.index.max(),
                                                      freq='D')
                            band_pivot = band_pivot.reindex(date_range).fillna(0)

                            logger.info(f"Band pivot shape: {band_pivot.shape}")

                            # Apply 90-day LOESS smoothing to each band
                            smoothed_bands = pd.DataFrame(index=band_pivot.index)

                            for col in band_pivot.columns:
                                y = band_pivot[col].values.copy()
                                mask = ~np.isnan(y)
                                valid_count = mask.sum()

                                if valid_count > 90:
                                    try:
                                        x_numeric = np.arange(len(y))
                                        frac = min(90.0 / valid_count, 0.5)
                                        smoothed = lowess(y[mask], x_numeric[mask], frac=frac, it=0)

                                        smoothed_values = np.full(len(y), np.nan)
                                        smoothed_values[mask] = smoothed[:, 1]

                                        smoothed_series = pd.Series(smoothed_values, index=band_pivot.index)
                                        smoothed_series = smoothed_series.interpolate(method='linear', limit_direction='both')
                                        smoothed_bands[col] = smoothed_series
                                        logger.info(f"  Applied LOESS smoothing to band {col}")
                                    except Exception as e:
                                        logger.warning(f"  LOESS failed for band {col}: {e}")
                                        smoothed_bands[col] = band_pivot[col]
                                else:
                                    smoothed_bands[col] = band_pivot[col]

                            # Price band colors matching the butterfly chart
                            band_color_map = {
                                'Below $0': FLEXOKI_ACCENT['red'],
                                '$0-$300': FLEXOKI_ACCENT['green'],
                                '$301-$1000': FLEXOKI_ACCENT['orange'],
                                'Above $1000': FLEXOKI_ACCENT['magenta'],
                            }

                            # Create Plotly stacked area chart
                            fig2 = go.Figure()

                            # Add "Below $0" as separate area below zero (not in stack)
                            if 'Below $0' in smoothed_bands.columns:
                                fig2.add_trace(go.Scatter(
                                    x=smoothed_bands.index,
                                    y=smoothed_bands['Below $0'],
                                    mode='lines',
                                    name='Below $0',
                                    fill='tozeroy',
                                    fillcolor=FLEXOKI_ACCENT['red'],
                                    line=dict(color=FLEXOKI_ACCENT['red'], width=0.5),
                                    hovertemplate='Below $0: $%{y:.1f}/MWh<extra></extra>'
                                ))

                            # Stack positive bands (in order from bottom to top)
                            positive_bands = ['$0-$300', '$301-$1000', 'Above $1000']
                            for band in positive_bands:
                                if band in smoothed_bands.columns:
                                    color = band_color_map.get(band, FLEXOKI_BASE[500])
                                    fig2.add_trace(go.Scatter(
                                        x=smoothed_bands.index,
                                        y=smoothed_bands[band],
                                        mode='lines',
                                        name=band,
                                        stackgroup='positive',
                                        fillcolor=color,
                                        line=dict(color=color, width=0.5),
                                        hovertemplate=f'{band}: $%{{y:.1f}}/MWh<extra></extra>'
                                    ))

                            fig2.update_layout(
                                title=f'Price Band Contribution to Flat Load Average - {selected_fuel_region}',
                                xaxis_title='Date',
                                yaxis_title='Contribution ($/MWh)',
                                paper_bgcolor=FLEXOKI_PAPER,
                                plot_bgcolor=FLEXOKI_PAPER,
                                font=dict(color=FLEXOKI_BLACK),
                                legend=dict(orientation='h', yanchor='bottom', y=1.02, xanchor='right', x=1),
                                hovermode='x unified',
                                margin=dict(l=60, r=20, t=60, b=40),
                                annotations=[dict(
                                    text='Data: AEMO',
                                    xref='paper', yref='paper',
                                    x=1, y=-0.12,
                                    showarrow=False,
                                    font=dict(size=9, color=FLEXOKI_BASE[600], style='italic')
                                )]
                            )
                            fig2.update_xaxes(gridcolor=FLEXOKI_BASE[100], showgrid=True)
                            fig2.update_yaxes(gridcolor=FLEXOKI_BASE[100], showgrid=True, zeroline=True, zerolinecolor=FLEXOKI_BASE[300])

                            self.price_index_plot_pane.object = fig2
                        else:
                            # No band data - show message
                            fig2 = go.Figure().add_annotation(
                                text='No price band data available',
                                xref='paper', yref='paper', x=0.5, y=0.5, showarrow=False,
                                font=dict(size=14, color=FLEXOKI_BLACK)
                            ).update_layout(
                                paper_bgcolor=FLEXOKI_PAPER, plot_bgcolor=FLEXOKI_PAPER,
                                xaxis=dict(visible=False), yaxis=dict(visible=False)
                            )
                            self.price_index_plot_pane.object = fig2
                    else:
                        # Insufficient data - show messages
                        fig_empty = go.Figure().add_annotation(
                            text='Insufficient data for smoothing',
                            xref='paper', yref='paper', x=0.5, y=0.5, showarrow=False,
                            font=dict(size=14, color=FLEXOKI_BLACK)
                        ).update_layout(
                            paper_bgcolor=FLEXOKI_PAPER, plot_bgcolor=FLEXOKI_PAPER,
                            xaxis=dict(visible=False), yaxis=dict(visible=False)
                        )
                        self.fuel_relatives_plot_pane.object = fig_empty
                        self.price_index_plot_pane.object = fig_empty
                
                except Exception as e:
                    logger.error(f"Error in fuel relatives calculation: {e}", exc_info=True)
                    fig_error = go.Figure().add_annotation(
                        text=f'Error: {str(e)}',
                        xref='paper', yref='paper', x=0.5, y=0.5, showarrow=False,
                        font=dict(size=14, color=FLEXOKI_BLACK)
                    ).update_layout(
                        paper_bgcolor=FLEXOKI_PAPER, plot_bgcolor=FLEXOKI_PAPER,
                        xaxis=dict(visible=False), yaxis=dict(visible=False)
                    )
                    self.fuel_relatives_plot_pane.object = fig_error
                    self.price_index_plot_pane.object = fig_error
            
            # Set up callbacks for fuel relatives - button click or region change triggers load
            fuel_relatives_region_selector.param.watch(update_fuel_relatives, 'value')
            fuel_relatives_load_button.on_click(update_fuel_relatives)
            
            logger.info("Prices tab created successfully")
            logger.info(f"Returning prices_tab of type: {type(prices_tab)}")
            return prices_tab
            
        except Exception as e:
            logger.error(f"Error creating prices tab: {e}", exc_info=True)
            return pn.pane.Markdown(f"**Error loading Prices tab:** {e}")
    
    def _create_price_analysis_tab(self):
        """Create price analysis tab"""
        try:
            logger.info("Creating price analysis tab...")
            price_analysis_tab = create_price_analysis_tab()
            logger.info("Price analysis tab created successfully")
            return price_analysis_tab
        except Exception as e:
            logger.error(f"Error creating price analysis tab: {e}")
            return pn.pane.Markdown(f"**Error loading Price Analysis:** {e}")
    
    def _create_station_analysis_tab(self):
        """Create station analysis tab"""
        try:
            logger.info("Creating station analysis tab...")
            station_analysis_tab = create_station_analysis_tab()
            logger.info("Station analysis tab created successfully")
            return station_analysis_tab
        except Exception as e:
            logger.error(f"Error creating station analysis tab: {e}")
            return pn.pane.Markdown(f"**Error loading Station Analysis:** {e}")
    
    def _create_trends_tab(self):
        """Create trends tab"""
        try:
            logger.info("Creating trends tab...")
            from aemo_dashboard.penetration import PenetrationTab
            trends_instance = PenetrationTab()
            trends_tab = trends_instance.create_layout()
            logger.info("Trends tab created successfully")
            return trends_tab
        except Exception as e:
            logger.error(f"Error creating trends tab: {e}")
            return pn.Column(
                pn.pane.Markdown("# Trends Analysis"),
                pn.pane.Markdown(f"**Error loading tab:** {e}"),
                sizing_mode='stretch_width'
            )
    
    def _create_curtailment_tab(self):
        """Create curtailment tab"""
        try:
            logger.info("Creating curtailment tab...")
            curtailment_tab = create_curtailment_tab()
            logger.info("Curtailment tab created successfully")
            return curtailment_tab
        except Exception as e:
            logger.error(f"Error creating curtailment tab: {e}")
            return pn.Column(
                pn.pane.Markdown("# Curtailment"),
                pn.pane.Markdown(f"**Error loading tab:** {e}"),
                sizing_mode='stretch_width'
            )

    def _create_batteries_tab(self):
        """Create batteries tab"""
        try:
            logger.info("Creating batteries tab...")
            from aemo_dashboard.insights import InsightsTab
            batteries_instance = InsightsTab()
            batteries_tab = batteries_instance.create_layout()
            logger.info("Batteries tab created successfully")
            return batteries_tab
        except Exception as e:
            logger.error(f"Error creating batteries tab: {e}")
            return pn.Column(
                pn.pane.Markdown("# Batteries"),
                pn.pane.Markdown(f"**Error loading tab:** {e}"),
                sizing_mode='stretch_width'
            )

def create_app():
    """Create the Panel application with proper session handling"""
    def _create_dashboard():
        """Factory function to create a new dashboard instance per session"""
        # Create initial loading screen that shows immediately
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
        
        # Container that will hold the actual dashboard
        dashboard_container = pn.Column(loading_screen, sizing_mode='stretch_width')
        
        # Function to initialize dashboard after loading screen is displayed
        def initialize_dashboard():
            try:
                # Create dashboard instance
                dashboard = EnergyDashboard()
                
                # Create the app
                app = dashboard.create_dashboard()
                
                # Replace loading screen with actual dashboard
                dashboard_container.clear()
                dashboard_container.append(app)
                
                # Start auto-update for this session
                def start_dashboard_updates():
                    try:
                        dashboard.start_auto_update()
                    except Exception as e:
                        logger.error(f"Error starting dashboard updates: {e}")
                
                # Hook into Panel's server startup
                pn.state.onload(start_dashboard_updates)
                
            except Exception as e:
                logger.error(f"Error creating app: {e}")
                dashboard_container.clear()
                dashboard_container.append(
                    pn.pane.HTML(f"<h1>Application Error: {str(e)}</h1>")
                )
        
        # Schedule dashboard initialization after loading screen renders
        pn.state.add_periodic_callback(initialize_dashboard, period=100, count=1)
        
        return dashboard_container
    
    return _create_dashboard

def main():
    """Enhanced main function with configuration management"""
    
    # Check for command line arguments FIRST
    if len(sys.argv) > 1:
        if sys.argv[1] == '--create-config':
            create_sample_env_file()
            return  # Exit after creating config
        elif sys.argv[1] == '--help':
            print("Energy Dashboard Utilities:")
            print("  --create-config   Create sample .env file")
            print("  --help           Show this help")
            print("  (no args)        Start dashboard server")
            return
        else:
            print(f"Unknown option: {sys.argv[1]}")
            print("Use --help for available options")
            return
    
    # Load environment variables if .env file exists
    env_file = Path('.env')
    if env_file.exists():
        load_dotenv(env_file)
        print(f"✅ Loaded configuration from {env_file}")
    else:
        print("⚠️  No .env file found - using default settings")
        print("💡 Run 'python gen_dash.py --create-config' to create one")
    
    # Set Panel to light theme globally (using Flexoki Light)
    pn.config.theme = 'default'

    # Check if required files exist (your existing code)
    if not os.path.exists(GEN_INFO_FILE):
        print(f"Error: {GEN_INFO_FILE} not found")
        print("Please ensure gen_info.pkl exists in the specified location")
        return
    
    if not os.path.exists(GEN_OUTPUT_FILE):
        print(f"Error: {GEN_OUTPUT_FILE} not found")
        print("Please ensure gen_output.parquet exists in the specified location")
        return
    
    # Create the app factory (your existing code)
    app_factory = create_app()
    
    # Determine port based on environment variable or default
    port = int(os.getenv('DASHBOARD_PORT', '5008'))
    
    print("Starting Interactive Energy Generation Dashboard...")
    print(f"Navigate to: http://localhost:{port}")
    print("Press Ctrl+C to stop the server")
    print("Auto-refresh: Page will reload every 9 minutes (2 data collector cycles)")
    
    # Serve the app with Flexoki Light theme and proper session handling
    pn.serve(
        app_factory,
        port=port,
        allow_websocket_origin=[f"localhost:{port}", "nemgen.itkservices2.com", "192.168.68.71:5008", "*"],
        show=True,
        autoreload=False,  # Disable autoreload in production
        threaded=True     # Enable threading for better concurrent handling
    )

if __name__ == "__main__":
    main()