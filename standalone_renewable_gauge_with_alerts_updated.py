#!/usr/bin/env python3
"""
Standalone Renewable Energy Gauge Server with SMS Alerts
Enhanced version that tracks individual fuel records and sends Twilio alerts
"""

import os
import sys
from pathlib import Path
import argparse

# Add the src directory to path
sys.path.insert(0, 'src')

# Load environment variables
from dotenv import load_dotenv

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import plotly.graph_objects as go
import json
import panel as pn
from twilio.rest import Client
import threading
import time

# Renewable fuel types
RENEWABLE_FUELS = ['Wind', 'Solar', 'Water', 'Rooftop Solar', 'Hydro', 'Biomass']

# Excluded fuel types
EXCLUDED_FUELS = ['Battery Storage', 'Transmission Flow']

# Main rooftop regions (exclude sub-regions to avoid double-counting)
# QLDN, QLDS, QLDC are subsets of QLD1
# TASN, TASS are subsets of TAS1
MAIN_ROOFTOP_REGIONS = ['NSW1', 'QLD1', 'VIC1', 'SA1', 'TAS1']

# Global Twilio client
twilio_client = None

def load_environment(env_file=None):
    """Load environment variables"""
    if env_file:
        env_path = Path(env_file)
        if env_path.exists():
            load_dotenv(env_path)
            print(f"Loaded environment from: {env_path}")
            return True
        else:
            print(f"Error: Specified env file not found: {env_file}")
            return False
    else:
        # Try current directory
        if Path('.env').exists():
            load_dotenv('.env')
            print(f"Loaded environment from: .env")
            return True
    return False

def initialize_twilio():
    """Initialize Twilio client"""
    global twilio_client
    
    account_sid = os.getenv('TWILIO_ACCOUNT_SID')
    auth_token = os.getenv('TWILIO_AUTH_TOKEN')
    
    if account_sid and auth_token:
        try:
            twilio_client = Client(account_sid, auth_token)
            print("Twilio client initialized successfully")
            return True
        except Exception as e:
            print(f"Error initializing Twilio: {e}")
            return False
    else:
        print("Twilio credentials not found in environment")
        return False

def send_record_alert(record_type, new_value, old_value, new_time, old_time):
    """Send SMS alert for new record"""
    if not twilio_client:
        print("Twilio not initialized, skipping SMS alert")
        return
    
    from_number = os.getenv('TWILIO_FROM_NUMBER')
    to_number = os.getenv('ALERT_PHONE_NUMBER')
    
    if not from_number or not to_number:
        print("Phone numbers not configured")
        return
    
    # Format the message based on record type
    if record_type == 'renewable_pct':
        message = f"""🎉 NEW RENEWABLE RECORD!
All-time: {new_value:.1f}% (was {old_value:.1f}%)
Time: {new_time.strftime('%Y-%m-%d %H:%M')}
Previous: {datetime.fromisoformat(old_time).strftime('%Y-%m-%d %H:%M')}"""
    elif record_type == 'wind_mw':
        message = f"""🌬️ NEW WIND RECORD!
{new_value:,.0f} MW (was {old_value:,.0f} MW)
Time: {new_time.strftime('%Y-%m-%d %H:%M')}
Previous: {datetime.fromisoformat(old_time).strftime('%Y-%m-%d %H:%M')}"""
    elif record_type == 'solar_mw':
        message = f"""☀️ NEW SOLAR RECORD!
{new_value:,.0f} MW (was {old_value:,.0f} MW)
Time: {new_time.strftime('%Y-%m-%d %H:%M')}
Previous: {datetime.fromisoformat(old_time).strftime('%Y-%m-%d %H:%M')}"""
    elif record_type == 'rooftop_mw':
        message = f"""🏠 NEW ROOFTOP RECORD!
{new_value:,.0f} MW (was {old_value:,.0f} MW)
Time: {new_time.strftime('%Y-%m-%d %H:%M')}
Previous: {datetime.fromisoformat(old_time).strftime('%Y-%m-%d %H:%M')}"""
    elif record_type == 'water_mw':
        message = f"""💧 NEW HYDRO RECORD!
{new_value:,.0f} MW (was {old_value:,.0f} MW)
Time: {new_time.strftime('%Y-%m-%d %H:%M')}
Previous: {datetime.fromisoformat(old_time).strftime('%Y-%m-%d %H:%M')}"""
    else:
        return  # Don't alert for other fuel types
    
    try:
        message_obj = twilio_client.messages.create(
            body=message,
            from_=from_number,
            to=to_number
        )
        print(f"SMS alert sent: {message_obj.sid}")
    except Exception as e:
        print(f"Error sending SMS: {e}")

def henderson_weights(n=13):
    """Generate Henderson filter weights"""
    if n == 5:
        return np.array([-0.073, 0.294, 0.558, 0.294, -0.073])
    elif n == 9:
        return np.array([-0.041, -0.010, 0.119, 0.267, 0.330, 0.267, 0.119, -0.010, -0.041])
    elif n == 13:
        return np.array([-0.019, -0.028, 0.0, 0.066, 0.147, 0.214, 0.240, 
                        0.214, 0.147, 0.066, 0.0, -0.028, -0.019])
    else:
        raise ValueError(f"Henderson weights not defined for n={n}")

def henderson_smooth(data, n=13):
    """Apply Henderson filter to smooth data"""
    weights = henderson_weights(n)
    half_window = n // 2
    
    smoothed = np.copy(data)
    for i in range(half_window, len(data) - half_window):
        smoothed[i] = np.sum(weights * data[i-half_window:i+half_window+1])
    
    return smoothed

def interpolate_rooftop_to_5min(df_30min):
    """Convert 30-minute rooftop data to 5-minute using Henderson smoothing"""
    # Create 5-minute index
    start = df_30min.index.min()
    end = df_30min.index.max() + pd.Timedelta(minutes=25)
    index_5min = pd.date_range(start=start, end=end, freq='5min')
    
    # Initialize result DataFrame
    df_5min = pd.DataFrame(index=index_5min)
    
    # Process each region
    for col in df_30min.columns:
        if col != 'NEM':  # Skip NEM total, we'll recalculate
            # Linear interpolation
            series_5min = df_30min[col].reindex(index_5min).interpolate(method='linear')
            series_5min = series_5min.ffill().bfill().fillna(0)
            
            # Apply Henderson smoothing
            values = series_5min.values
            smoothed = henderson_smooth(values)
            
            df_5min[col] = smoothed
    
    # Recalculate NEM total
    region_cols = [c for c in df_5min.columns if c != 'NEM']
    df_5min['NEM'] = df_5min[region_cols].sum(axis=1)
    
    return df_5min

def get_current_generation_stats():
    """Get current generation statistics including individual fuel types"""
    try:
        # Get file paths from environment
        gen_5min_path = os.getenv('GEN_OUTPUT_FILE_5MIN')
        gen_30min_path = os.getenv('GEN_OUTPUT_FILE')
        rooftop_path = os.getenv('ROOFTOP_SOLAR_FILE')
        gen_info_path = os.getenv('GEN_INFO_FILE')
        
        # If 5-minute path not set, try to derive it
        if not gen_5min_path and gen_30min_path:
            gen_5min_path = gen_30min_path.replace('scada30.parquet', 'scada5.parquet')
        
        # Load DUID mapping
        import pickle
        with open(gen_info_path, 'rb') as f:
            gen_info = pickle.load(f)
        
        # Create DUID to fuel mapping
        duid_to_fuel = gen_info.set_index('DUID')['Fuel'].to_dict()
        
        # Get time range - use latest data timestamp to avoid timezone issues
        # AEMO data is in AEST (Queensland time), but system may be in AEDT during daylight saving
        # Load data first to get its latest timestamp
        gen_df = pd.read_parquet(gen_5min_path)
        gen_df['settlementdate'] = pd.to_datetime(gen_df['settlementdate'])
        
        # Use the latest timestamp in the data as our end time
        end_time = gen_df['settlementdate'].max()
        start_time = end_time - timedelta(minutes=15)
        
        # Filter to recent data
        gen_df = gen_df[(gen_df['settlementdate'] >= start_time) & 
                        (gen_df['settlementdate'] <= end_time)]
        
        if gen_df.empty:
            print("No recent generation data found")
            return None
        
        # Map fuel types
        gen_df['fuel_type'] = gen_df['duid'].map(duid_to_fuel)
        gen_df = gen_df.dropna(subset=['fuel_type'])
        
        # Exclude battery storage and transmission
        gen_df = gen_df[~gen_df['fuel_type'].isin(EXCLUDED_FUELS)]
        
        # Get latest timestamp
        latest_time = gen_df['settlementdate'].max()
        latest_gen = gen_df[gen_df['settlementdate'] == latest_time]
        
        # Aggregate by fuel type
        fuel_totals = latest_gen.groupby('fuel_type')['scadavalue'].sum()
        
        # Load and process rooftop data
        rooftop_df = pd.read_parquet(rooftop_path)
        rooftop_df['settlementdate'] = pd.to_datetime(rooftop_df['settlementdate'])
        rooftop_df = rooftop_df[(rooftop_df['settlementdate'] >= start_time - timedelta(hours=1)) & 
                                (rooftop_df['settlementdate'] <= end_time)]
        
        rooftop_mw = 0
        if not rooftop_df.empty and 'regionid' in rooftop_df.columns:
            # Pivot and interpolate rooftop data
            rooftop_wide = rooftop_df.pivot(
                index='settlementdate',
                columns='regionid',
                values='power'
            ).fillna(0)

            # Filter to main regions only (exclude sub-regions to avoid double-counting)
            available_regions = [r for r in MAIN_ROOFTOP_REGIONS if r in rooftop_wide.columns]
            rooftop_wide = rooftop_wide[available_regions]

            rooftop_5min = interpolate_rooftop_to_5min(rooftop_wide)
            
            if latest_time in rooftop_5min.index:
                rooftop_mw = rooftop_5min.loc[latest_time, 'NEM']
            else:
                closest_idx = rooftop_5min.index.get_indexer([latest_time], method='nearest')[0]
                rooftop_mw = rooftop_5min.iloc[closest_idx]['NEM']
        
        # Calculate statistics
        stats = {
            'timestamp': latest_time,
            'wind_mw': fuel_totals.get('Wind', 0),
            'solar_mw': fuel_totals.get('Solar', 0),
            'rooftop_mw': rooftop_mw,
            'water_mw': fuel_totals.get('Water', 0) + fuel_totals.get('Hydro', 0),
            'biomass_mw': fuel_totals.get('Biomass', 0)
        }
        
        # Calculate renewable percentage
        renewable_mw = fuel_totals[fuel_totals.index.isin(RENEWABLE_FUELS)].sum() + rooftop_mw
        total_mw = fuel_totals.sum() + rooftop_mw
        
        stats['renewable_mw'] = renewable_mw
        stats['total_mw'] = total_mw
        stats['renewable_pct'] = (renewable_mw / total_mw * 100) if total_mw > 0 else 0
        
        return stats
            
    except Exception as e:
        print(f"Error calculating generation stats: {e}")
        import traceback
        traceback.print_exc()
        return None

def load_renewable_records(records_file):
    """Load historical renewable energy records"""
    try:
        if records_file.exists():
            with open(records_file, 'r') as f:
                records = json.load(f)
            
            # Check if we have the full records (with individual fuels)
            if 'all_time' in records and isinstance(records['all_time'], dict):
                if 'renewable_pct' in records['all_time']:
                    # We have the full format
                    return records
                else:
                    # Old format - convert it
                    old_all_time = records['all_time']
                    records['all_time'] = {
                        'renewable_pct': old_all_time,
                        'wind_mw': {'value': 0, 'timestamp': '2020-01-01T00:00:00'},
                        'solar_mw': {'value': 0, 'timestamp': '2020-01-01T00:00:00'},
                        'rooftop_mw': {'value': 0, 'timestamp': '2020-01-01T00:00:00'},
                        'water_mw': {'value': 0, 'timestamp': '2020-01-01T00:00:00'}
                    }
            
            return records
    except Exception as e:
        print(f"Error loading records: {e}")
    
    # Return default records
    # Note: Rooftop and renewable % reduced after fixing double-counting (2025-10-15)
    return {
        'all_time': {
            'renewable_pct': {'value': 63.0, 'timestamp': '2024-11-06T13:00:00'},  # Reduced from 78.7% (double-counting fix)
            'wind_mw': {'value': 9757, 'timestamp': '2025-07-10T23:10:00'},
            'solar_mw': {'value': 7459, 'timestamp': '2025-02-27T14:30:00'},
            'rooftop_mw': {'value': 15000, 'timestamp': '2024-12-27T12:30:00'},  # Reduced from 19297 MW (double-counting fix)
            'water_mw': {'value': 6494, 'timestamp': '2023-06-20T18:00:00'}
        },
        'hourly': {str(h): {'value': 45 + h * 0.5, 'timestamp': '2024-01-01T00:00:00'}
                  for h in range(24)}
    }

def save_renewable_records(records, records_file):
    """Save renewable energy records"""
    try:
        records_file.parent.mkdir(parents=True, exist_ok=True)
        with open(records_file, 'w') as f:
            json.dump(records, f, indent=2)
    except Exception as e:
        print(f"Error saving records: {e}")

def update_records_with_alerts(stats, records, records_file):
    """Update renewable records and send alerts if new highs are reached"""
    timestamp = stats['timestamp'].isoformat()
    current_hour = stats['timestamp'].hour
    alerts_sent = []
    
    # Check all-time renewable percentage record
    if stats['renewable_pct'] > records['all_time']['renewable_pct']['value']:
        old_value = records['all_time']['renewable_pct']['value']
        old_time = records['all_time']['renewable_pct']['timestamp']
        
        records['all_time']['renewable_pct'] = {
            'value': stats['renewable_pct'],
            'timestamp': timestamp
        }
        
        print(f"NEW ALL-TIME RENEWABLE RECORD: {stats['renewable_pct']:.1f}% (was {old_value:.1f}%)")
        send_record_alert('renewable_pct', stats['renewable_pct'], old_value, stats['timestamp'], old_time)
        alerts_sent.append('renewable_pct')
    
    # Check individual fuel records
    fuel_checks = [
        ('wind_mw', '🌬️ WIND'),
        ('solar_mw', '☀️ SOLAR'),
        ('rooftop_mw', '🏠 ROOFTOP'),
        ('water_mw', '💧 HYDRO')
    ]
    
    for fuel_key, fuel_name in fuel_checks:
        if fuel_key in stats and stats[fuel_key] > records['all_time'][fuel_key]['value']:
            old_value = records['all_time'][fuel_key]['value']
            old_time = records['all_time'][fuel_key]['timestamp']
            
            records['all_time'][fuel_key] = {
                'value': stats[fuel_key],
                'timestamp': timestamp
            }
            
            print(f"NEW {fuel_name} RECORD: {stats[fuel_key]:,.0f} MW (was {old_value:,.0f} MW)")
            send_record_alert(fuel_key, stats[fuel_key], old_value, stats['timestamp'], old_time)
            alerts_sent.append(fuel_key)
    
    # Check hourly renewable percentage record
    hour_key = str(current_hour)
    if hour_key not in records['hourly']:
        records['hourly'][hour_key] = {
            'value': stats['renewable_pct'],
            'timestamp': timestamp
        }
    elif stats['renewable_pct'] > records['hourly'][hour_key]['value']:
        old_value = records['hourly'][hour_key]['value']
        records['hourly'][hour_key] = {
            'value': stats['renewable_pct'],
            'timestamp': timestamp
        }
        print(f"New {current_hour}:00 hour record: {stats['renewable_pct']:.1f}% (was {old_value:.1f}%)")
        # Don't send SMS for hourly records to avoid too many messages
    
    # Save records if any were updated
    if alerts_sent or len(alerts_sent) > 0:
        save_renewable_records(records, records_file)
    
    return records, alerts_sent

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

# Global variable to hold the current gauge
current_gauge_pane = None

def update_gauge_loop():
    """Background thread to update gauge every 4.5 minutes"""
    global current_gauge_pane
    data_dir = os.getenv('DATA_DIR', '/tmp')
    records_file = Path(data_dir) / 'renewable_records_calculated.json'
    
    while True:
        try:
            print(f"\n[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Updating gauge...")
            
            # Get current generation statistics
            stats = get_current_generation_stats()
            
            if stats:
                # Load and update records
                records = load_renewable_records(records_file)
                records, alerts_sent = update_records_with_alerts(stats, records, records_file)
                
                # Get values for gauge display
                current_percentage = stats['renewable_pct']
                all_time_record = records['all_time']['renewable_pct']['value']
                hour_record = records['hourly'][str(stats['timestamp'].hour)]['value']
                
                # Create gauge figure
                fig = create_gauge_figure(current_percentage, all_time_record, hour_record)
                
                # Update the pane if it exists
                if current_gauge_pane is not None:
                    current_gauge_pane.object = fig
                
                print(f"Gauge updated: {current_percentage:.1f}% renewable")
                if alerts_sent:
                    print(f"Alerts sent for: {', '.join(alerts_sent)}")
            
        except Exception as e:
            print(f"Error in update loop: {e}")
            import traceback
            traceback.print_exc()
        
        # Wait 4.5 minutes
        time.sleep(270)  # 4.5 * 60 = 270 seconds

def create_gauge_app():
    """Create the gauge app"""
    global current_gauge_pane
    
    # Initial gauge creation
    try:
        # Get data for initial display
        stats = get_current_generation_stats()
        
        if stats:
            data_dir = os.getenv('DATA_DIR', '/tmp')
            records_file = Path(data_dir) / 'renewable_records_calculated.json'
            records = load_renewable_records(records_file)
            
            # Don't send alerts on startup
            current_percentage = stats['renewable_pct']
            all_time_record = records['all_time']['renewable_pct']['value']
            hour_record = records['hourly'][str(stats['timestamp'].hour)]['value']
        else:
            current_percentage = 0
            all_time_record = 78.7
            hour_record = 45
        
        # Create initial gauge
        fig = create_gauge_figure(current_percentage, all_time_record, hour_record)
        current_gauge_pane = pn.pane.Plotly(fig, sizing_mode='fixed', width=400, height=350)
        
    except Exception as e:
        print(f"Error creating initial gauge: {e}")
        # Create error gauge
        fig = go.Figure().add_trace(go.Indicator(
            mode="gauge+number",
            value=0,
            title={'text': "Error Loading Data", 'font': {'color': "red"}},
            gauge={'axis': {'range': [0, 100]}}
        ))
        current_gauge_pane = pn.pane.Plotly(fig, sizing_mode='fixed', width=400, height=350)
    
    # Start update thread (daemon so it stops when main program exits)
    update_thread = threading.Thread(target=update_gauge_loop, daemon=True)
    update_thread.start()
    
    # Create app
    app = pn.Column(
        current_gauge_pane,
        sizing_mode='fixed',
        width=400,
        height=350,
        margin=(0, 0),
        css_classes=['gauge-only']
    )
    
    return app

def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(description='Standalone Renewable Energy Gauge Server with Alerts')
    parser.add_argument('--port', type=int, default=5007, help='Port to serve on (default: 5007)')
    parser.add_argument('--host', type=str, default='0.0.0.0', help='Host to serve on (default: 0.0.0.0)')
    parser.add_argument('--env-file', type=str, help='Path to .env file to use')
    parser.add_argument('--test', action='store_true', help='Test data loading and exit')
    parser.add_argument('--test-alerts', action='store_true', help='Test alert functionality')
    args = parser.parse_args()
    
    # Load environment
    if not load_environment(args.env_file):
        print("Warning: Could not load environment file")
    
    # Initialize Twilio
    initialize_twilio()
    
    # If test mode, just test data loading
    if args.test:
        print("\n=== Testing Generation Data Loading ===")
        stats = get_current_generation_stats()
        if stats:
            print(f"Renewable: {stats['renewable_pct']:.1f}%")
            print(f"Wind: {stats['wind_mw']:,.0f} MW")
            print(f"Solar: {stats['solar_mw']:,.0f} MW")
            print(f"Rooftop: {stats['rooftop_mw']:,.0f} MW")
            print(f"Hydro: {stats['water_mw']:,.0f} MW")
        return
    
    # If test alerts mode
    if args.test_alerts:
        print("\n=== Testing Alert System ===")
        # Create a fake stats with record-breaking values
        test_stats = {
            'timestamp': datetime.now(),
            'renewable_pct': 70.0,  # Higher than 63.0% (after double-counting fix)
            'wind_mw': 10000,       # Higher than 9757 MW
            'solar_mw': 8000,       # Higher than 7459 MW
            'rooftop_mw': 16000,    # Higher than 15000 MW (after double-counting fix)
            'water_mw': 7000,       # Higher than 6494 MW
            'renewable_mw': 41000,
            'total_mw': 58571
        }
        
        data_dir = os.getenv('DATA_DIR', '/tmp')
        records_file = Path(data_dir) / 'renewable_records_test.json'
        records = load_renewable_records(records_file)
        
        print("Simulating record-breaking values...")
        records, alerts_sent = update_records_with_alerts(test_stats, records, records_file)
        print(f"Alerts that would be sent: {alerts_sent}")
        return
    
    # Enable Panel extensions (only when serving)
    pn.extension('plotly')
    
    # Add custom CSS
    pn.config.raw_css.append("""
    /* Hide all Panel UI elements */
    .bk-root .bk-toolbar { display: none !important; }
    .pn-loading { display: none !important; }
    body { margin: 0; padding: 0; background: #282a36; }
    .gauge-only { background: #282a36; }
    """)
    
    print(f"\n{'='*60}")
    print(f"Renewable Energy Gauge Server with SMS Alerts")
    print(f"{'='*60}")
    print(f"Port: {args.port}")
    print(f"Host: {args.host}")
    print(f"Twilio: {'Enabled' if twilio_client else 'Disabled'}")
    print(f"\nTracking records for:")
    print(f"  - Overall renewable percentage")
    print(f"  - Wind generation")
    print(f"  - Solar generation")
    print(f"  - Rooftop solar")
    print(f"  - Hydro generation")
    print(f"\nAccess the gauge at:")
    print(f"  Local: http://localhost:{args.port}")
    if args.host == '0.0.0.0':
        print(f"  Network: http://<your-ip>:{args.port}")
    print(f"\nTo embed in your webpage:")
    print(f"<iframe src='http://localhost:{args.port}' width='450' height='400' frameborder='0'></iframe>")
    print(f"\nPress Ctrl+C to stop the server")
    print(f"{'='*60}\n")
    
    # Serve the app using a factory function
    pn.serve(
        create_gauge_app,
        port=args.port,
        address=args.host,
        allow_websocket_origin=['*'],
        show=False,
        title='Renewable Gauge with Alerts'
    )

if __name__ == '__main__':
    main()