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

from ..shared.config import config
from ..shared.logging_config import setup_logging, get_logger
from ..shared.email_alerts import EmailAlertManager
from ..analysis.price_analysis_ui import create_price_analysis_tab
from ..station.station_analysis_ui import create_station_analysis_tab
from ..nem_dash.nem_dash_tab import create_nem_dash_tab_with_updates
from .generation_query_manager import GenerationQueryManager

# Set up logging
setup_logging()
logger = get_logger(__name__)

# Configure Panel and HoloViews BEFORE extension loading
pn.config.theme = 'dark'
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
        bgcolor='black',
        color=[fuel_colors.get(fuel, '#6272a4') for fuel in fuel_types],
        alpha=0.8,
        hover=True,
        hover_tooltips=[('Fuel Type', '$name')],
        title=f'Generation by Fuel Type - {region} ({time_range}) | data:AEMO, design ITK'
    )
    
    area_plot = area_plot.opts(
        show_grid=False,
        bgcolor='black',
        xaxis=None
    )
    
    creation_time = time.time() - start_time
    logger.info(f"Plot creation took {creation_time:.2f}s")
    
    return area_plot

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
""")
hv.extension('bokeh')

# Logging is set up in imports

# Configure dark theme with grid enabled
hv.opts.defaults(
    hv.opts.Area(
        width=1200,  # Use larger fixed width
        height=500,
        alpha=0.8,
        show_grid=False,
        toolbar='above'
    ),
    hv.opts.Overlay(
        show_grid=False,
        toolbar='above'
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
        objects=['1', '7', '30', 'All'],
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
            gen_plot = self.create_plot()
            util_plot = self.create_utilization_plot()
            transmission_plot = self.create_transmission_plot()
            
            # Create panes with explicit sizing and disable axis linking
            self.plot_pane = pn.pane.HoloViews(
                gen_plot,
                sizing_mode='stretch_width',
                height=600,  # Back to normal height
                margin=(5, 5),
                linked_axes=False  # Prevent UFuncTypeError when switching tabs
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
            
            # Set initial visibility
            self.plot_pane.visible = True
            self.utilization_pane.visible = True
            self.transmission_pane.visible = True
            
        except Exception as e:
            logger.error(f"Error initializing panes: {e}")
            # Create fallback empty panes
            self.plot_pane = pn.pane.HTML("Loading generation chart...", height=600)
            self.utilization_pane = pn.pane.HTML("Loading utilization chart...", height=500)
            self.transmission_pane = pn.pane.HTML("Loading transmission chart...", height=400)
        
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
        """Define colors for different fuel types - all distinct and visually clear"""
        fuel_colors = {
            'Coal': '#4a4a4a',        # Dark gray - distinctive for coal
            'CCGT': '#ff5555',        # Bright red - gas turbine
            'OCGT': '#ff8c42',        # Orange-red - different gas turbine type
            'Gas other': '#ff9500',   # Pure orange - clearly different from yellow
            'Solar': '#ffd700',       # Gold/bright yellow - sunny color
            'Rooftop Solar': '#ffff80',  # Lighter yellow - distributed solar
            'Wind': '#00ff7f',        # Spring green - wind/renewable
            'Water': '#00bfff',       # Sky blue - water/hydro
            'Battery Storage': '#9370db',  # Medium purple - technology
            'Biomass': '#8b4513',     # Saddle brown - organic/wood
            'Other': '#ff69b4',       # Hot pink - catch-all category
            'Transmission Flow': '#ffb6c1',      # Light pink - both imports and exports
            'Transmission Imports': '#ffb6c1',   # Light pink - imports (inflow)  
            'Transmission Exports': '#ffb6c1'    # Same light pink color for exports
        }
        return fuel_colors
    
    def get_cache_stats_display(self):
        """Get cache statistics for display"""
        global _cache_stats
        total = _cache_stats['hits'] + _cache_stats['misses']
        hit_rate = (_cache_stats['hits'] / total * 100) if total > 0 else 0
        
        return f"Cache: {'ON' if ENABLE_PN_CACHE else 'OFF'} | Hits: {_cache_stats['hits']} | Misses: {_cache_stats['misses']} | Rate: {hit_rate:.1f}%"
    
    

    def create_plot(self):
        """Create the HvPlot visualization with generation and price charts stacked vertically"""
        try:
            # Load fresh data
            self.load_generation_data()
            data = self.process_data_for_region()
            
            if data.empty:
                # Create empty plot with message
                empty_plot = hv.Text(0.5, 0.5, 'No data available').opts(
                    xlim=(0, 1),
                    ylim=(0, 1),
                    bgcolor='black',
                    width=1200,
                    height=400,
                    color='white',
                    fontsize=16
                )
                return empty_plot
            
            # Get colors
            fuel_colors = self.get_fuel_colors()
            
            # Use all available fuel types in the order they appear (already sorted)
            fuel_types = list(data.columns)
            
            if not fuel_types:
                # Fallback empty plot
                return hv.Text(0.5, 0.5, 'No generation data for selected region').opts(
                    bgcolor='black',
                    color='white',
                    fontsize=14
                )
            
            # Create stacked area plot with special handling for negative values (battery & transmission exports)
            plot_data = data[fuel_types].copy().reset_index()
            
            # Check if Battery Storage exists and has negative values
            battery_col = 'Battery Storage'
            transmission_exports_col = 'Transmission Exports'
            has_battery = battery_col in plot_data.columns
            has_transmission_exports = transmission_exports_col in plot_data.columns
            
            # Determine if we need special negative value handling
            has_negative_values = (
                (has_battery and (plot_data[battery_col].values < 0).any()) or
                (has_transmission_exports and (plot_data[transmission_exports_col].values < 0).any())
            )
            
            if has_negative_values:
                # Prepare data for main positive stack (exclude transmission exports from main plot)
                positive_fuel_types = [f for f in fuel_types if f != transmission_exports_col]
                plot_data_positive = plot_data.copy()
                
                # Handle battery storage negative values
                if has_battery:
                    battery_data = plot_data[battery_col].copy()
                    plot_data_positive[battery_col] = pd.Series(
                        np.where(battery_data.values >= 0, battery_data.values, 0),
                        index=battery_data.index
                    )  # Only positive values
                
                # Create the main stacked area plot (positive values only, no transmission exports)
                main_plot = plot_data_positive.hvplot.area(
                    x='settlementdate',
                    y=positive_fuel_types,
                    stacked=True,
                    width=1200,
                    height=300,  # Reduced height to make room for price chart
                    ylabel='Generation (MW)',
                    xlabel='',  # Remove x-label since it will be on the price chart
                    grid=True,
                    legend='right',
                    bgcolor='black',
                    color=[fuel_colors.get(fuel, '#6272a4') for fuel in positive_fuel_types],
                    alpha=0.8,
                    hover=True,
                    hover_tooltips=[('Fuel Type', '$name')]
                )
                
                # Create negative values as a single stacked area plot
                negative_columns = []
                plot_data_negative = plot_data[['settlementdate']].copy()
                negative_colors = []
                
                # Add transmission exports negative values first (will appear at bottom)
                if has_transmission_exports and (plot_data[transmission_exports_col].values < 0).any():
                    plot_data_negative[transmission_exports_col] = pd.Series(
                        np.where(plot_data[transmission_exports_col].values < 0, plot_data[transmission_exports_col].values, 0),
                        index=plot_data.index
                    )
                    negative_columns.append(transmission_exports_col)
                    negative_colors.append(fuel_colors.get('Transmission Flow', '#ffb6c1'))
                
                # Add battery negative values second (will appear on top - higher priority)
                if has_battery and (plot_data[battery_col].values < 0).any():
                    plot_data_negative[battery_col] = pd.Series(
                        np.where(plot_data[battery_col].values < 0, plot_data[battery_col].values, 0),
                        index=plot_data.index
                    )
                    negative_columns.append(battery_col)
                    negative_colors.append(fuel_colors.get('Battery Storage', '#9370db'))
                
                # Create the negative stacked area plot if we have negative values
                if negative_columns:
                    # Log the order for debugging
                    logger.info(f"Negative columns order: {negative_columns}")
                    logger.info(f"Negative colors order: {negative_colors}")
                    logger.info(f"Negative data - Transmission min: {plot_data_negative.get('Transmission Exports', pd.Series()).min() if 'Transmission Exports' in plot_data_negative.columns else 'N/A'}")
                    logger.info(f"Negative data - Battery min: {plot_data_negative.get('Battery Storage', pd.Series()).min() if 'Battery Storage' in plot_data_negative.columns else 'N/A'}")
                    
                    # Create individual area plots for each negative component
                    negative_plots = []
                    
                    # First plot transmission exports (bottom layer)
                    if 'Transmission Exports' in plot_data_negative.columns:
                        transmission_plot = plot_data_negative.hvplot.area(
                            x='settlementdate',
                            y='Transmission Exports',
                            stacked=False,
                            width=1200,
                            height=300,
                            color='#ffb6c1',  # Light pink
                            alpha=0.8,
                            hover=True,
                            legend=False
                        )
                        negative_plots.append(transmission_plot)
                    
                    # Then plot battery storage (top layer)
                    if 'Battery Storage' in plot_data_negative.columns:
                        battery_plot = plot_data_negative.hvplot.area(
                            x='settlementdate',
                            y='Battery Storage',
                            stacked=False,
                            width=1200,
                            height=300,
                            color='#9370db',  # Purple
                            alpha=0.8,
                            hover=True,
                            legend=False
                        )
                        negative_plots.append(battery_plot)
                    
                    # Combine all plots
                    if negative_plots:
                        negative_combined = negative_plots[0]
                        for plot in negative_plots[1:]:
                            negative_combined = negative_combined * plot
                        area_plot = main_plot * negative_combined
                    else:
                        area_plot = main_plot
                else:
                    area_plot = main_plot
                
                time_range_display = self._get_time_range_display()
                area_plot = area_plot.opts(
                    title=f'Generation by Fuel Type - {self.region} ({time_range_display}) | data:AEMO, design ITK',
                    show_grid=False,
                    bgcolor='black',
                    xaxis=None,  # Hide x-axis since price chart will show it
                    hooks=[self._get_datetime_formatter_hook()]
                )
                
            else:
                # No negative values - exclude transmission exports from main plot (they should always be negative)
                positive_fuel_types = [f for f in fuel_types if f != transmission_exports_col]
                time_range_display = self._get_time_range_display()
                
                # Try cached plot creation
                if ENABLE_PN_CACHE:
                    try:
                        global _cache_stats
                        
                        # Prepare data for caching
                        plot_data_for_cache = plot_data[['settlementdate'] + positive_fuel_types].copy()
                        plot_data_json = plot_data_for_cache.to_json(date_format='iso', orient='records')
                        fuel_types_str = json.dumps(positive_fuel_types)
                        fuel_colors_json = json.dumps(fuel_colors)
                        
                        # Call cached function
                        area_plot = create_generation_plot_cached(
                            plot_data_json=plot_data_json,
                            fuel_types_str=fuel_types_str,
                            fuel_colors_json=fuel_colors_json,
                            region=self.region,
                            time_range=time_range_display,
                            width=1200,
                            height=300
                        )
                        
                        _cache_stats['hits'] += 1
                        total = _cache_stats['hits'] + _cache_stats['misses']
                        hit_rate = (_cache_stats['hits'] / total * 100) if total > 0 else 0
                        logger.debug(f"Cache hit! Stats: {_cache_stats}, hit_rate={hit_rate:.1f}%")
                        
                    except Exception as e:
                        logger.error(f"Cache error, using direct creation: {e}")
                        _cache_stats['errors'] += 1
                        # Fall through to direct creation
                        area_plot = None
                
                # Direct creation (fallback or if cache disabled)
                if not ENABLE_PN_CACHE or 'area_plot' not in locals() or area_plot is None:
                    area_plot = plot_data.hvplot.area(
                        x='settlementdate',
                        y=positive_fuel_types,
                        stacked=True,
                        width=1200,
                        height=300,  # Reduced height to make room for price chart
                        title=f'Generation by Fuel Type - {self.region} ({time_range_display}) | data:AEMO, design ITK',
                        ylabel='Generation (MW)',
                        xlabel='',  # Remove x-label since it will be on the price chart
                        grid=True,
                        legend='right',
                        bgcolor='black',
                        color=[fuel_colors.get(fuel, '#6272a4') for fuel in positive_fuel_types],
                        alpha=0.8,
                        hover=True,
                        hover_tooltips=[('Fuel Type', '$name')]
                    ).opts(
                        show_grid=False,
                        bgcolor='black',
                        xaxis=None  # Hide x-axis since price chart will show it
                )
            
            # Load and create price chart
            price_df = self.load_price_data()
            
            if price_df.empty:
                # If no price data, return just the generation plot with x-axis restored
                return area_plot.opts(xaxis='bottom', xlabel='Time')
            
            # Create price line chart
            price_plot = price_df.hvplot.line(
                x='settlementdate',
                y='RRP',
                width=1200,
                height=250,  # Smaller height - about half of generation chart
                ylabel='Price ($/MWh)',
                xlabel='Time',
                grid=True,
                bgcolor='black',
                color='white',
                line_width=2,
                alpha=0.8,
                hover=True,
                hover_tooltips=[('Price', '@RRP{$0.2f}')]
            ).opts(
                show_grid=False,
                bgcolor='black',
                hooks=[self._get_datetime_formatter_hook()]
            )
            
            # Stack the plots vertically using Layout (just generation + price)
            # Disable shared_axes to prevent UFuncTypeError when switching tabs
            combined_layout = (area_plot + price_plot).cols(1).opts(
                shared_axes=False,  # Disable to prevent UFuncTypeError
                merge_tools=True   # Merge toolbars into a single toolbar
            )
            
            self.last_update = datetime.now()
            logger.info(f"Plot updated for {self.region}, {self.time_range}")
            
            return combined_layout
            
        except Exception as e:
            logger.error(f"Error creating plot: {e}")
            # Return fallback plot
            return hv.Text(0.5, 0.5, f'Error creating plot: {str(e)}').opts(
                bgcolor='black',
                color='red',
                fontsize=12
            )
    
    def create_transmission_plot(self):
        """Create transmission flow line chart with limit areas showing unused capacity"""
        try:
            import holoviews as hv
            
            # Skip transmission chart for NEM (no specific region)
            if self.region == 'NEM':
                return hv.Text(0.5, 0.5, 'Transmission flows not available for NEM view\nSelect a specific region').opts(
                    xlim=(0, 1),
                    ylim=(0, 1),
                    bgcolor='black',
                    width=1200,
                    height=300,
                    color='white',
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
                    bgcolor='black',
                    width=1200,
                    height=300,
                    color='white',
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
                    bgcolor='black',
                    width=1200,
                    height=300,
                    color='white',
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
                    bgcolor='black',
                    width=1200,
                    height=300,
                    color='white',
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
                    hooks=[self._get_datetime_formatter_hook()]
                )
                
                # Create the main flow line with enhanced tooltips
                hover_df['capacity_status'] = hover_df['percent'].apply(
                    lambda x: 'At Capacity (≥95%)' if x >= 95 else 
                             'High Utilization (≥80%)' if x >= 80 else 
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
                    hooks=[self._get_datetime_formatter_hook()]
                )
                
                plot_elements.extend([filled_area, flow_line])
            
            # Add horizontal line at y=0
            zero_line = hv.HLine(0).opts(
                color='white',
                alpha=0.3,
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
                    bgcolor='black',
                    ylabel='Flow (MW)',
                    xlabel='Time',
                    title=f'Transmission Flows with Limits - {self.region} ({time_range_display})',
                    show_grid=False,
                    legend_position='right',
                    framewise=True,  # Force complete recomputation on updates
                    xlim=x_range,  # Explicitly set x-axis range
                    apply_ranges=False,  # Prevent automatic range determination
                    hooks=[self._get_datetime_formatter_hook()]  # Re-add hooks
                )
            else:
                combined_plot = hv.Text(0.5, 0.5, f'No transmission data available for {self.region}').opts(
                    xlim=(0, 1),
                    ylim=(0, 1),
                    bgcolor='black',
                    width=1200,
                    height=400,
                    color='white',
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
                bgcolor='black',
                width=1200,
                height=300,
                color='white',
                fontsize=12
            )

    
    def create_utilization_plot(self):
        """Create capacity utilization line chart with proper document handling"""
        try:
            utilization_data = self.calculate_capacity_utilization()
            
            if utilization_data.empty:
                # Create empty plot with message
                empty_plot = hv.Text(0.5, 0.5, 'No utilization data available').opts(
                    xlim=(0, 1),
                    ylim=(0, 1),
                    bgcolor='black',
                    width=1200,
                    height=400,
                    color='white',
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
                    bgcolor='black',
                    color='white',
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
                bgcolor='black',
                color=[fuel_colors.get(fuel, '#6272a4') for fuel in fuel_types],
                alpha=0.8,
                hover=True,
                hover_tooltips=[('Fuel Type', '$name'), ('Utilization', '@$name{0.1f}%')],
                ylim=(0, 100)  # Force Y-axis to 0-100%
            ).opts(
                show_grid=False,
                toolbar='above',
                bgcolor='black',
                ylim=(0, 100),  # Double ensure Y-axis range
                yformatter='%.0f%%'  # Format Y-axis as percentage
            )
            
            # Rename the Y dimension to make it independent from generation MW axis
            line_plot = line_plot.redim(**{fuel: f'{fuel}_utilization' for fuel in fuel_types})
            
            return line_plot
            
        except Exception as e:
            logger.error(f"Error creating utilization plot: {e}")
            # Return fallback plot
            return hv.Text(0.5, 0.5, f'Error creating utilization plot: {str(e)}').opts(
                bgcolor='black',
                color='red',
                fontsize=12
            )
    
    def update_plot(self):
        """Update all plots with fresh data and proper error handling"""
        try:
            logger.info("Starting plot update...")
            
            # Create new plots
            new_generation_plot = self.create_plot()
            new_utilization_plot = self.create_utilization_plot()
            new_transmission_plot = self.create_transmission_plot()
            
            # Safely update the panes
            if self.plot_pane is not None:
                self.plot_pane.object = new_generation_plot
            
            if self.utilization_pane is not None:
                self.utilization_pane.object = new_utilization_plot
            
            if self.transmission_pane is not None:
                self.transmission_pane.object = new_transmission_plot
                
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
                <div class='header-container' style='background-color: #008B8B; padding: 15px; margin: -10px -10px 20px -10px;'>
                    <h1 style='color: white; margin: 0; text-align: center;'>Nem Analysis</h1>
                    <div style='text-align: center; color: white; font-size: 16px; margin-top: 5px;'>
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
        while True:
            try:
                await asyncio.sleep(270)  # 4.5 minutes
                # Update plots in both tabs
                self.update_plot()
                logger.info("Auto-update completed")
            except asyncio.CancelledError:
                logger.info("Auto-update loop cancelled")
                break
            except Exception as e:
                logger.error(f"Error in auto-update loop: {e}")
                await asyncio.sleep(60)  # Wait 1 minute before retrying
    
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
        elif self.time_range == 'All':
            start_date = datetime(2020, 1, 1).date()  # Approximate earliest data
        else:
            # Keep current custom dates
            return
        
        # Update the date parameters
        self.start_date = start_date
        self.end_date = end_date
    
    def _get_effective_date_range(self):
        """Get the effective start and end datetime for data filtering"""
        if self.time_range == 'All':
            # For all data, return full available range for auto resolution selection
            # This allows the enhanced adapters to choose 30-minute data for better performance
            start_datetime = datetime(2020, 2, 1)  # Actual start of historical data in the system
            end_datetime = datetime.now()          # Current time
            return start_datetime, end_datetime
        else:
            # Convert dates to datetime for filtering
            start_datetime = datetime.combine(self.start_date, datetime.min.time())
            end_datetime = datetime.combine(self.end_date, datetime.max.time())
            return start_datetime, end_datetime
    
    
    def _get_time_range_display(self):
        """Get formatted time range string for chart titles"""
        if self.time_range == '1':
            return "Last 24 Hours"
        elif self.time_range == '7':
            return "Last 7 Days"  
        elif self.time_range == '30':
            return "Last 30 Days"
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
                options=['1', '7', '30', 'All'],
                inline=True,  # Horizontal layout
                width=200
            )
            time_range_widget.link(self, value='time_range')
            
            time_range_selector = pn.Column(
                pn.pane.HTML("<div style='color: #aaa; font-size: 11px; margin-bottom: 4px;'>Days</div>"),
                time_range_widget,
                width=200,
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
                    "#### Generation by Fuel Type + Price",
                    pn.Column(self.plot_pane, max_width=1250, sizing_mode='stretch_width'),
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
            <div class='header-container' style='background-color: #008B8B; padding: 15px; margin: -10px -10px 20px -10px;'>
                <h1 style='color: white; margin: 0; text-align: center;'>Nem Analysis</h1>
                <div style='text-align: center; color: white; font-size: 16px; margin-top: 5px;'>
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
                ("Pivot table", pn.pane.HTML(loading_html)),  # Lazy
                ("Station Analysis", pn.pane.HTML(loading_html)),  # Lazy
                ("Penetration", pn.pane.HTML(loading_html)),  # Lazy
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
                2: self._create_price_analysis_tab,
                3: self._create_station_analysis_tab,
                4: self._create_penetration_tab
            }
            
            # Watch for tab changes
            tabs.param.watch(self._on_tab_change, 'active')
            
            logger.info("Tab setup complete")
            
            # Complete dashboard layout
            dashboard = pn.Column(
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
    
    def _create_penetration_tab(self):
        """Create penetration tab"""
        try:
            logger.info("Creating penetration tab...")
            from aemo_dashboard.penetration import PenetrationTab
            penetration_instance = PenetrationTab()
            penetration_tab = penetration_instance.create_layout()
            logger.info("Penetration tab created successfully")
            return penetration_tab
        except Exception as e:
            logger.error(f"Error creating penetration tab: {e}")
            return pn.Column(
                pn.pane.Markdown("# Penetration Analysis"),
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
    
    # Set Panel to dark theme globally
    pn.config.theme = 'dark'
    
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
    
    # Serve the app with dark theme and proper session handling
    pn.serve(
        app_factory, 
        port=port, 
        allow_websocket_origin=[f"localhost:{port}", "nemgen.itkservices2.com"],
        show=True,
        autoreload=False,  # Disable autoreload in production
        threaded=True     # Enable threading for better concurrent handling
    )

if __name__ == "__main__":
    main()