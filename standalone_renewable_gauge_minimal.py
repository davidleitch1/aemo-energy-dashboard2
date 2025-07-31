#!/usr/bin/env python3
"""
Standalone Renewable Energy Gauge Server - Minimal Version
Serves ONLY the gauge without any Panel UI elements
"""

import os
import sys
from pathlib import Path
import argparse

# Add the src directory to path
sys.path.insert(0, 'src')

# Don't load env yet - wait for command line args
from dotenv import load_dotenv

import panel as pn
import pandas as pd
from datetime import datetime, timedelta
import plotly.graph_objects as go
import json

# Enable Panel extensions
pn.extension('plotly')

# These will be initialized in main() after env is loaded
logger = None
query_manager = None
DATA_DIR = None
RECORDS_FILE = None
RENEWABLE_FUELS = ['Wind', 'Solar', 'Water', 'Rooftop Solar']


def init_globals():
    """Initialize global variables after environment is loaded"""
    global logger, query_manager, DATA_DIR, RECORDS_FILE
    
    from aemo_dashboard.nem_dash.nem_dash_query_manager import NEMDashQueryManager
    from aemo_dashboard.shared.logging_config import get_logger
    from aemo_dashboard.shared.config import config
    
    logger = get_logger(__name__)
    query_manager = NEMDashQueryManager()
    
    # Records file location - use production data directory from env
    DATA_DIR = os.getenv('DATA_DIR', config.data_dir)
    RECORDS_FILE = Path(DATA_DIR) / 'renewable_records.json'
    
    print(f"Using data directory: {DATA_DIR}")
    print(f"Records file location: {RECORDS_FILE}")
    
    # Debug: Show parquet file paths
    print(f"\nData file paths from environment:")
    print(f"  GEN_OUTPUT_FILE: {os.getenv('GEN_OUTPUT_FILE', 'Not set')}")
    print(f"  SPOT_HIST_FILE: {os.getenv('SPOT_HIST_FILE', 'Not set')}")
    print(f"  ROOFTOP_SOLAR_FILE: {os.getenv('ROOFTOP_SOLAR_FILE', 'Not set')}")
    print(f"  TRANSMISSION_OUTPUT_FILE: {os.getenv('TRANSMISSION_OUTPUT_FILE', 'Not set')}")


def load_renewable_records():
    """Load historical renewable energy records"""
    try:
        if RECORDS_FILE.exists():
            with open(RECORDS_FILE, 'r') as f:
                records = json.load(f)
            if 'all_time' in records and 'hourly' in records:
                logger.info(f"Loaded records from {RECORDS_FILE}")
                return records
    except Exception as e:
        logger.error(f"Error loading records: {e}")
    
    # Return default records
    logger.info("Using default records")
    return {
        'all_time': {'value': 68.5, 'timestamp': '2024-10-13T13:30:00'},
        'hourly': {str(h): {'value': 45 + h * 0.5, 'timestamp': '2024-01-01T00:00:00'} 
                  for h in range(24)}
    }


def save_renewable_records(records):
    """Save renewable energy records"""
    try:
        RECORDS_FILE.parent.mkdir(parents=True, exist_ok=True)
        with open(RECORDS_FILE, 'w') as f:
            json.dump(records, f, indent=2)
        logger.info(f"Saved records to {RECORDS_FILE}")
    except Exception as e:
        logger.error(f"Error saving records: {e}")


def update_records(current_percentage, current_time, current_hour, records):
    """Update renewable records if new highs are reached"""
    timestamp = current_time.isoformat()
    records_updated = False
    
    # Check all-time record
    if current_percentage > records['all_time']['value']:
        records['all_time'] = {
            'value': current_percentage,
            'timestamp': timestamp
        }
        records_updated = True
        logger.info(f"New all-time renewable record: {current_percentage:.1f}%")
    
    # Check hourly record
    hour_key = str(current_hour)
    if hour_key not in records['hourly']:
        records['hourly'][hour_key] = {
            'value': current_percentage,
            'timestamp': timestamp
        }
        records_updated = True
    elif current_percentage > records['hourly'][hour_key]['value']:
        records['hourly'][hour_key] = {
            'value': current_percentage,
            'timestamp': timestamp
        }
        records_updated = True
        logger.info(f"New {current_hour}:00 hour record: {current_percentage:.1f}%")
    
    if records_updated:
        save_renewable_records(records)
    
    all_time_record = records['all_time']['value']
    hour_record = records['hourly'][hour_key]['value']
    
    return all_time_record, hour_record, records_updated


def create_gauge_figure(current_value, all_time_record, hour_record):
    """Create the Plotly gauge figure"""
    fig = go.Figure()
    
    # Main gauge
    fig.add_trace(go.Indicator(
        mode="gauge+number",
        value=current_value,
        title={'text': "Renewable Energy %", 'font': {'size': 16, 'color': "white"}},
        number={'suffix': "%", 'font': {'size': 18, 'color': "white"}, 'valueformat': '.0f'},
        domain={'x': [0, 1], 'y': [0.15, 1]},
        gauge={
            'axis': {
                'range': [0, 100],
                'tickmode': 'linear',
                'tick0': 0,
                'dtick': 20,
                'tickwidth': 1,
                'tickcolor': "white",
                'tickfont': {'color': "white"}
            },
            'bar': {'color': "#50fa7b", 'thickness': 0.6, 'line': {'color': "#50fa7b", 'width': 4}},
            'bgcolor': "#44475a",
            'borderwidth': 2,
            'bordercolor': "#6272a4",
            'steps': [],
            'threshold': {
                'line': {'color': "gold", 'width': 4},
                'thickness': 0.75,
                'value': all_time_record
            }
        }
    ))
    
    # Hour record gauge (invisible except for threshold)
    fig.add_trace(go.Indicator(
        mode="gauge",
        value=0,
        domain={'x': [0, 1], 'y': [0.15, 1]},
        gauge={
            'axis': {'range': [0, 100], 'visible': False},
            'bar': {'color': "rgba(0,0,0,0)", 'thickness': 0},
            'bgcolor': "rgba(0,0,0,0)",
            'borderwidth': 0,
            'threshold': {
                'line': {'color': "#5DCED0", 'width': 4},
                'thickness': 0.75,
                'value': hour_record
            }
        }
    ))
    
    # Add legend
    fig.add_annotation(
        x=0.5, y=0.10,
        text="<b>Records:</b>",
        showarrow=False,
        xref="paper", yref="paper",
        align="center",
        font=dict(size=10, color="white")
    )
    
    # All-time record
    fig.add_shape(
        type="line",
        x0=0.15, y0=0.05,
        x1=0.20, y1=0.05,
        line=dict(color="gold", width=4),
        xref="paper", yref="paper"
    )
    
    fig.add_annotation(
        x=0.21, y=0.05,
        text=f"All-time: {all_time_record:.0f}%",
        showarrow=False,
        xref="paper", yref="paper",
        align="left",
        font=dict(size=9, color="white")
    )
    
    # Hour record
    fig.add_shape(
        type="line",
        x0=0.55, y0=0.05,
        x1=0.60, y1=0.05,
        line=dict(color="#5DCED0", width=4),
        xref="paper", yref="paper"
    )
    
    fig.add_annotation(
        x=0.61, y=0.05,
        text=f"Hour: {hour_record:.0f}%",
        showarrow=False,
        xref="paper", yref="paper",
        align="left",
        font=dict(size=9, color="white")
    )
    
    fig.update_layout(
        paper_bgcolor="#282a36",
        height=350,
        width=400,
        margin=dict(l=30, r=30, t=60, b=30),
        showlegend=False
    )
    
    return fig


def create_gauge_component():
    """Create the gauge component that matches the main dashboard implementation"""
    
    # Create a placeholder for the gauge
    gauge_pane = pn.pane.Plotly(height=350, width=400, sizing_mode='fixed')
    
    def update_gauge():
        """Update the gauge with current data"""
        try:
            logger.info("Updating gauge...")
            
            # Debug: Try to get generation data directly
            try:
                # Get current renewable data - returns a dict
                renewable_data = query_manager.get_renewable_data()
                logger.info(f"Renewable data type: {type(renewable_data)}")
                logger.info(f"Renewable data: {renewable_data}")
                
                # The query manager returns a dict with renewable_mw, total_mw, renewable_pct
                if renewable_data and renewable_data.get('total_mw', 0) > 0:
                    current_percentage = renewable_data['renewable_pct']
                    logger.info(f"Renewable percentage from query manager: {current_percentage:.1f}%")
                else:
                    logger.warning("No renewable data available, using test value")
                    current_percentage = 42.5  # Test value so we can see the gauge
            except Exception as e:
                logger.error(f"Error getting renewable data: {e}")
                current_percentage = 42.5  # Test value
            
            # Get current time info
            current_time = datetime.now()
            current_hour = current_time.hour
            
            # Load and update records
            records = load_renewable_records()
            all_time_record, hour_record, _ = update_records(
                current_percentage, current_time, current_hour, records
            )
            
            # Create and update gauge
            fig = create_gauge_figure(current_percentage, all_time_record, hour_record)
            gauge_pane.object = fig
            
        except Exception as e:
            logger.error(f"Error updating gauge: {e}")
            import traceback
            traceback.print_exc()
            # Show error gauge
            fig = go.Figure().add_trace(go.Indicator(
                mode="gauge+number",
                value=0,
                title={'text': "Error Loading Data", 'font': {'color': "red"}},
                gauge={'axis': {'range': [0, 100]}}
            ))
            gauge_pane.object = fig
    
    # Initial update
    update_gauge()
    
    # Set up periodic updates (every 5 minutes = 300000 ms)
    cb = pn.state.add_periodic_callback(update_gauge, period=300000, count=None)
    
    return gauge_pane


def create_app():
    """Create the Panel application factory"""
    def _create_gauge_app():
        """Factory function to create a new gauge instance per session"""
        # Create just the gauge component - no template
        gauge_component = create_gauge_component()
        
        # Return the gauge directly without any template
        # This creates a minimal app with just the gauge
        return pn.Column(
            gauge_component,
            sizing_mode='fixed',
            width=400,
            height=350,
            margin=(0, 0),
            css_classes=['gauge-only']
        )
    
    return _create_gauge_app


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(description='Standalone Renewable Energy Gauge Server - Minimal')
    parser.add_argument('--port', type=int, default=5007, help='Port to serve on (default: 5007)')
    parser.add_argument('--host', type=str, default='0.0.0.0', help='Host to serve on (default: 0.0.0.0)')
    parser.add_argument('--allow-websocket-origin', type=str, nargs='*', 
                       help='Allowed websocket origins for iframe embedding')
    parser.add_argument('--env-file', type=str, help='Path to .env file to use')
    parser.add_argument('--show-config', action='store_true', help='Show configuration and exit')
    args = parser.parse_args()
    
    # Debug: show current working directory
    print(f"Current working directory: {os.getcwd()}")
    
    # Load environment after parsing args
    if args.env_file:
        env_path = Path(args.env_file)
        if env_path.exists():
            load_dotenv(env_path)
            print(f"Loaded environment from: {env_path}")
        else:
            print(f"Error: Specified env file not found: {args.env_file}")
            sys.exit(1)
    else:
        # Check for custom env file path from environment
        custom_env_path = os.getenv('GAUGE_ENV_FILE')
        
        if custom_env_path and Path(custom_env_path).exists():
            load_dotenv(custom_env_path)
            print(f"Loaded environment from custom path: {custom_env_path}")
        else:
            # Try to load .env from current directory first, then parent directories
            env_loaded = False
            current_path = Path.cwd()
            for _ in range(3):  # Check up to 3 levels up
                env_file = current_path / '.env'
                if env_file.exists():
                    load_dotenv(env_file)
                    print(f"Loaded environment from: {env_file}")
                    env_loaded = True
                    break
                current_path = current_path.parent
            
            if not env_loaded:
                print("Warning: No .env file found. Using system environment variables.")
                print("Tip: Use --env-file option or set GAUGE_ENV_FILE environment variable")
    
    # Initialize globals after environment is loaded
    init_globals()
    
    # Show configuration if requested
    if args.show_config:
        print("\nConfiguration:")
        print(f"  DATA_DIR: {DATA_DIR}")
        print(f"  RECORDS_FILE: {RECORDS_FILE}")
        print(f"  GEN_OUTPUT_FILE: {os.getenv('GEN_OUTPUT_FILE', 'Not set')}")
        print(f"  SPOT_HIST_FILE: {os.getenv('SPOT_HIST_FILE', 'Not set')}")
        print(f"  ROOFTOP_SOLAR_FILE: {os.getenv('ROOFTOP_SOLAR_FILE', 'Not set')}")
        print(f"  GEN_INFO_FILE: {os.getenv('GEN_INFO_FILE', 'Not set')}")
        return
    
    # Create the app factory
    app_factory = create_app()
    
    # Configure websocket origins
    if args.allow_websocket_origin:
        websocket_origins = args.allow_websocket_origin
    else:
        # For production behind Cloudflare, allow common patterns
        websocket_origins = [
            f"localhost:{args.port}",
            f"127.0.0.1:{args.port}",
            "*"  # Allow all for Cloudflare tunnel
        ]
    
    print(f"\n{'='*60}")
    print(f"Renewable Energy Gauge Server - Minimal Version")
    print(f"{'='*60}")
    print(f"Configuration:")
    print(f"  Port: {args.port}")
    print(f"  Host: {args.host}")
    print(f"  Data directory: {DATA_DIR}")
    print(f"  Records file: {RECORDS_FILE}")
    print(f"  Websocket origins: {websocket_origins}")
    
    print(f"\nServer URL: http://{args.host}:{args.port}")
    print(f"\nTo embed in iframe:")
    print(f"<iframe src='http://your-cloudflare-url' width='450' height='400' frameborder='0'></iframe>")
    print(f"\nPress Ctrl+C to stop the server")
    print(f"{'='*60}\n")
    
    # Add custom CSS to hide any Panel UI elements
    pn.config.raw_css.append("""
    /* Hide all Panel UI elements */
    .bk-root .bk-toolbar { display: none !important; }
    .pn-loading { display: none !important; }
    .pn-busy { display: none !important; }
    body { margin: 0; padding: 0; background: #282a36; }
    .gauge-only { background: #282a36; }
    """)
    
    # Serve the app using pn.serve
    pn.serve(
        app_factory,
        port=args.port,
        address=args.host,
        allow_websocket_origin=websocket_origins,
        show=False,  # Don't auto-open browser
        autoreload=False,
        threaded=True,
        title='Renewable Gauge'
    )


if __name__ == '__main__':
    main()