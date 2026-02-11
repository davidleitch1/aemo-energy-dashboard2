#!/usr/bin/env python3
"""
Standalone Renewable Energy Gauge Server - Fixed Production Version
Properly handles updates and data format from query manager
"""

import os
import sys
from pathlib import Path

# Add the src directory to path
sys.path.insert(0, 'src')

# Load environment variables from .env file if it exists
from dotenv import load_dotenv

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

import panel as pn
import pandas as pd
from datetime import datetime, timedelta
import plotly.graph_objects as go
import json
import argparse

# Enable Panel extensions
pn.extension('plotly')

# Import after environment is loaded
from aemo_dashboard.nem_dash.nem_dash_query_manager import NEMDashQueryManager
from aemo_dashboard.shared.logging_config import get_logger
from aemo_dashboard.shared.config import config

logger = get_logger(__name__)

# Lazy initialization of query manager
_query_manager = None

def get_query_manager():
    """Lazy initialization - only create query manager on first use"""
    global _query_manager
    if _query_manager is None:
        _query_manager = NEMDashQueryManager()
    return _query_manager

# Renewable fuel types
RENEWABLE_FUELS = ['Wind', 'Solar', 'Water', 'Rooftop Solar']

# Records file location - use production data directory from env
DATA_DIR = os.getenv('DATA_DIR', config.data_dir)
RECORDS_FILE = Path(DATA_DIR) / 'renewable_records.json'


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
    """Create the gauge component with deferred loading.

    Shows a loading spinner immediately, then loads data after page renders.
    Uses pn.state.onload to defer heavy data loading until after the page is served.
    """

    # Loading indicator HTML with spinner
    loading_html = """
    <div style="width:400px;height:350px;display:flex;flex-direction:column;
                align-items:center;justify-content:center;background:#282a36;">
        <div style="width:40px;height:40px;border:3px solid #44475a;
                    border-top-color:#50fa7b;border-radius:50%;
                    animation:spin 1s linear infinite;"></div>
        <p style="margin-top:15px;font-size:12px;color:#6272a4;">Loading gauge...</p>
    </div>
    <style>@keyframes spin {from{transform:rotate(0deg)}to{transform:rotate(360deg)}}</style>
    """

    # Create container that starts with loading indicator
    loading_pane = pn.pane.HTML(loading_html, width=400, height=350)
    container = pn.Column(loading_pane, sizing_mode='fixed', width=400, height=350)

    def load_gauge():
        """Load gauge data - called after page loads via onload callback."""
        logger.info("onload callback triggered - loading gauge data")
        try:
            # Get current renewable data - returns a dict
            renewable_data = get_query_manager().get_renewable_data()

            # The query manager returns a dict with renewable_mw, total_mw, renewable_pct
            if renewable_data and renewable_data.get('total_mw', 0) > 0:
                current_percentage = renewable_data['renewable_pct']
                logger.info(f"Renewable percentage from query manager: {current_percentage:.1f}%")
            else:
                logger.warning("No renewable data available")
                current_percentage = 0

            # Get current time info
            current_time = datetime.now()
            current_hour = current_time.hour

            # Load and update records
            records = load_renewable_records()
            all_time_record, hour_record, _ = update_records(
                current_percentage, current_time, current_hour, records
            )

            # Create gauge figure
            fig = create_gauge_figure(current_percentage, all_time_record, hour_record)
            gauge_pane = pn.pane.Plotly(fig, sizing_mode='fixed', width=400, height=350)

            # Replace loading indicator with gauge
            container[:] = [gauge_pane]
            logger.info("Gauge loaded successfully")

        except Exception as e:
            logger.error(f"Error loading gauge: {e}")
            container[:] = [pn.pane.HTML(
                f"<div style='width:400px;height:350px;display:flex;align-items:center;justify-content:center;background:#282a36;'>"
                f"<p style='color:#ff5555;'>Gauge Error: {e}</p></div>",
                width=400, height=350
            )]

    # Schedule loading after page is served to browser
    pn.state.onload(load_gauge)

    return container


def create_app():
    """Create the Panel application factory"""
    def _create_gauge_app():
        """Factory function to create a new gauge instance per session"""
        # Create the gauge component
        gauge_component = create_gauge_component()
        
        # Create the template with minimal styling
        template = pn.template.FastListTemplate(
            title="Renewable Energy Gauge",
            sidebar=[],  # No sidebar
            main=[gauge_component],
            header_background='#282a36',
            background_color='#282a36',
            raw_css=["""
            body {
                background-color: #282a36;
            }
            .mdc-typography {
                background-color: #282a36;
            }
            """]
        )
        
        return template
    
    return _create_gauge_app


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(description='Standalone Renewable Energy Gauge Server')
    parser.add_argument('--port', type=int, default=5007, help='Port to serve on (default: 5007)')
    parser.add_argument('--host', type=str, default='0.0.0.0', help='Host to serve on (default: 0.0.0.0 for all interfaces)')
    parser.add_argument('--allow-websocket-origin', type=str, nargs='*', 
                       help='Allowed websocket origins for iframe embedding (e.g., localhost:8080 mysite.com)')
    parser.add_argument('--show-config', action='store_true', help='Show configuration and exit')
    args = parser.parse_args()
    
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
    print(f"Renewable Energy Gauge Server - Production")
    print(f"{'='*60}")
    print(f"Configuration:")
    print(f"  Port: {args.port}")
    print(f"  Host: {args.host}")
    print(f"  Data directory: {DATA_DIR}")
    print(f"  Records file: {RECORDS_FILE}")
    print(f"  Websocket origins: {websocket_origins}")
    
    print(f"\nServer URL: http://{args.host}:{args.port}")
    print(f"\nTo embed in iframe:")
    print(f"<iframe src='http://your-cloudflare-url' width='450' height='400'></iframe>")
    print(f"\nPress Ctrl+C to stop the server")
    print(f"{'='*60}\n")
    
    # Serve the app using pn.serve (like the main dashboard)
    pn.serve(
        app_factory,
        port=args.port,
        address=args.host,
        allow_websocket_origin=websocket_origins,
        show=False,  # Don't auto-open browser
        autoreload=False,
        threaded=True,
        title='Renewable Energy Gauge'
    )


if __name__ == '__main__':
    main()