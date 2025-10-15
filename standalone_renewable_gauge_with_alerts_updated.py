#!/usr/bin/env python3
"""
Standalone Renewable Energy Gauge with SMS Alerts
Fixed version with correct renewable calculation
"""

import os
import sys
import json
import pickle
import argparse
import asyncio
from pathlib import Path
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import panel as pn
import plotly.graph_objects as go
from dotenv import load_dotenv

# Twilio imports
try:
    from twilio.rest import Client as TwilioClient
    TWILIO_AVAILABLE = True
except ImportError:
    print("Warning: Twilio not installed. SMS alerts disabled.")
    TWILIO_AVAILABLE = False

# Renewable fuel types to include
RENEWABLE_FUELS = ['Wind', 'Solar', 'Rooftop Solar', 'Water', 'Hydro', 'Biomass']

# Define actual generation fuel types (excludes storage and transmission)
GENERATION_FUELS = ['Coal', 'CCGT', 'OCGT', 'Gas other', 'Other',
                    'Wind', 'Solar', 'Rooftop Solar', 'Water', 'Hydro', 'Biomass']

# Global Twilio client
twilio_client = None

def load_environment(env_file=None):
    """Load environment variables"""
    if env_file:
        env_path = Path(env_file)
        if env_path.exists():
            load_dotenv(env_path)
            print(f"Loaded environment from {env_path}")
    else:
        # Try default locations
        for path in ['.env', '../.env', '../../.env']:
            env_path = Path(path)
            if env_path.exists():
                load_dotenv(env_path)
                print(f"Loaded environment from {env_path}")
                break

def initialize_twilio():
    """Initialize Twilio client"""
    global twilio_client

    if not TWILIO_AVAILABLE:
        return False

    account_sid = os.getenv('TWILIO_ACCOUNT_SID')
    auth_token = os.getenv('TWILIO_AUTH_TOKEN')

    if account_sid and auth_token:
        try:
            twilio_client = TwilioClient(account_sid, auth_token)
            print("Twilio client initialized successfully")
            return True
        except Exception as e:
            print(f"Failed to initialize Twilio: {e}")
            return False
    else:
        print("Twilio credentials not found in environment")
        return False

def send_sms_alert(message, record_type=None):
    """Send SMS alert via Twilio"""
    if not twilio_client:
        print(f"SMS Alert (disabled): {message}")
        return False

    try:
        from_phone = os.getenv('TWILIO_PHONE_NUMBER')
        to_phone = os.getenv('MY_PHONE_NUMBER')

        if not from_phone or not to_phone:
            print("Phone numbers not configured")
            return False

        msg = twilio_client.messages.create(
            body=message,
            from_=from_phone,
            to=to_phone
        )
        print(f"SMS sent: {msg.sid}")
        return True

    except Exception as e:
        print(f"Failed to send SMS: {e}")
        return False

def interpolate_rooftop_to_5min(rooftop_30min):
    """Interpolate 30-minute rooftop data to 5-minute intervals"""
    try:
        # Create 5-minute index
        start = rooftop_30min.index.min()
        end = rooftop_30min.index.max()
        index_5min = pd.date_range(start=start, end=end, freq='5min')

        # Reindex and interpolate
        rooftop_5min = rooftop_30min.reindex(index_5min)
        rooftop_5min = rooftop_5min.interpolate(method='time')

        # Add NEM total column
        rooftop_5min['NEM'] = rooftop_5min.sum(axis=1)

        return rooftop_5min
    except Exception as e:
        print(f"Error interpolating rooftop data: {e}")
        return pd.DataFrame()

def load_duid_mapping(gen_info_path):
    """Load DUID to fuel type mapping"""
    try:
        with open(gen_info_path, 'rb') as f:
            gen_info = pickle.load(f)

        # Create DUID to fuel mapping
        duid_to_fuel = {}
        for _, row in gen_info.iterrows():
            duid = row.get('DUID')
            fuel = row.get('Fuel')
            if duid and fuel:
                duid_to_fuel[duid] = fuel

        print(f"Loaded {len(duid_to_fuel)} DUID mappings")
        return duid_to_fuel

    except Exception as e:
        print(f"Error loading DUID mapping: {e}")
        return {}

def get_latest_generation_stats(data_dir):
    """Get latest generation statistics including renewable percentage"""
    try:
        # Data paths
        gen_5min_path = Path(data_dir) / 'scada5.parquet'
        rooftop_path = Path(data_dir) / 'rooftop30.parquet'
        gen_info_path = Path(data_dir) / 'gen_info.pkl'

        # Validate paths exist
        for path in [gen_5min_path, rooftop_path, gen_info_path]:
            if not path.exists():
                print(f"Data file not found: {path}")
                return None

        # Load DUID mapping
        duid_to_fuel = load_duid_mapping(gen_info_path)
        if not duid_to_fuel:
            print("Failed to load DUID mapping")
            return None

        # Get time range - last 15 minutes
        end_time = datetime.now()
        start_time = end_time - timedelta(minutes=15)

        # Load generation data
        gen_df = pd.read_parquet(gen_5min_path)
        gen_df['settlementdate'] = pd.to_datetime(gen_df['settlementdate'])
        gen_df = gen_df[(gen_df['settlementdate'] >= start_time) &
                        (gen_df['settlementdate'] <= end_time)]

        if gen_df.empty:
            print("No recent generation data found")
            return None

        # Map fuel types
        gen_df['fuel_type'] = gen_df['duid'].map(duid_to_fuel)
        gen_df = gen_df.dropna(subset=['fuel_type'])

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

        # FIXED CALCULATION: Only sum actual generation sources
        # Calculate renewable generation from GENERATION_FUELS only
        renewable_mw = 0
        total_mw = 0

        for fuel_type, value in fuel_totals.items():
            # Only include actual generation fuels
            if fuel_type in GENERATION_FUELS:
                total_mw += value
                if fuel_type in RENEWABLE_FUELS:
                    renewable_mw += value

        # Add rooftop solar to both renewable and total
        renewable_mw += rooftop_mw
        total_mw += rooftop_mw

        stats['renewable_mw'] = renewable_mw
        stats['total_mw'] = total_mw
        stats['renewable_pct'] = (renewable_mw / total_mw * 100) if total_mw > 0 else 0

        print(f"Fixed calculation: Renewable: {renewable_mw:.1f}MW / Total: {total_mw:.1f}MW = {stats['renewable_pct']:.1f}%")

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

            # Validate structure
            if 'all_time' in records and 'hourly' in records:
                return records
            else:
                print("Invalid records structure, creating new")
                return initialize_records()
        else:
            print("No records file found, creating new")
            records = initialize_records()
            save_renewable_records(records, records_file)
            return records
    except Exception as e:
        print(f"Error loading records: {e}, creating new")
        return initialize_records()

def initialize_records():
    """Initialize empty records structure"""
    records = {
        'all_time': {'value': 0.0, 'timestamp': datetime.now().isoformat()},
        'hourly': {}
    }
    for hour in range(24):
        records['hourly'][str(hour)] = {
            'value': 0.0,
            'timestamp': datetime.now().isoformat()
        }
    return records

def save_renewable_records(records, records_file):
    """Save renewable energy records"""
    try:
        records_file.parent.mkdir(exist_ok=True)
        with open(records_file, 'w') as f:
            json.dump(records, f, indent=2)
        print(f"Saved records to {records_file}")
    except Exception as e:
        print(f"Error saving records: {e}")

def update_records_with_alerts(stats, records, records_file):
    """Update records and send SMS alerts for new records"""
    if not stats:
        return records, []

    current_percentage = stats['renewable_pct']
    current_time = stats['timestamp']
    current_hour = current_time.hour
    timestamp = current_time.isoformat()
    records_updated = False
    alerts_sent = []

    # Check all-time record
    if current_percentage > records['all_time']['value']:
        old_record = records['all_time']['value']
        records['all_time'] = {
            'value': current_percentage,
            'timestamp': timestamp
        }
        records_updated = True
        print(f"NEW ALL-TIME RECORD: {current_percentage:.1f}% (was {old_record:.1f}%)")

        # Send SMS alert
        message = f"🎉 NEW RENEWABLE RECORD!\n" \
                 f"All-time: {current_percentage:.1f}%\n" \
                 f"Previous: {old_record:.1f}%\n" \
                 f"Time: {current_time.strftime('%H:%M')}"
        if send_sms_alert(message, 'all_time'):
            alerts_sent.append('all_time')

    # Check hourly record
    hour_key = str(current_hour)
    if hour_key not in records['hourly']:
        records['hourly'][hour_key] = {
            'value': current_percentage,
            'timestamp': timestamp
        }
        records_updated = True
    elif current_percentage > records['hourly'][hour_key]['value']:
        old_hour_record = records['hourly'][hour_key]['value']
        records['hourly'][hour_key] = {
            'value': current_percentage,
            'timestamp': timestamp
        }
        records_updated = True
        print(f"New {current_hour}:00 hour record: {current_percentage:.1f}% (was {old_hour_record:.1f}%)")

        # Send SMS alert for significant hourly records
        if current_percentage > 60:  # Only alert for >60% hourly records
            message = f"📈 New hourly renewable record!\n" \
                     f"{current_hour}:00 hour: {current_percentage:.1f}%\n" \
                     f"Previous: {old_hour_record:.1f}%"
            if send_sms_alert(message, 'hourly'):
                alerts_sent.append('hourly')

    # Check component records
    component_alerts = []

    # Wind record (>5000 MW)
    if stats['wind_mw'] > 5000:
        component_alerts.append(f"Wind: {stats['wind_mw']:.0f}MW")

    # Solar record (>4000 MW)
    if stats['solar_mw'] > 4000:
        component_alerts.append(f"Solar: {stats['solar_mw']:.0f}MW")

    # Rooftop record (>19000 MW)
    if stats['rooftop_mw'] > 19000:
        component_alerts.append(f"Rooftop: {stats['rooftop_mw']:.0f}MW")

    if component_alerts:
        message = f"⚡ High renewable generation!\n" + "\n".join(component_alerts)
        if send_sms_alert(message, 'components'):
            alerts_sent.append('components')

    if records_updated:
        save_renewable_records(records, records_file)

    return records, alerts_sent

def create_renewable_gauge_plotly(current_value, all_time_record=0, hour_record=0, last_update=None):
    """Create Plotly gauge with record markers"""
    try:
        fig = go.Figure()

        # Main gauge
        fig.add_trace(go.Indicator(
            mode="gauge+number",
            value=current_value,
            title={'text': "Renewable Energy %", 'font': {'size': 20, 'color': "white"}},
            number={'suffix': "%", 'font': {'size': 24, 'color': "white"}, 'valueformat': '.0f'},
            domain={'x': [0, 1], 'y': [0.25, 1]},
            gauge={
                'axis': {
                    'range': [0, 100],
                    'tickmode': 'linear',
                    'tick0': 0,
                    'dtick': 20,
                    'tickwidth': 1,
                    'tickcolor': "white",
                    'tickfont': {'color': "white", 'size': 12}
                },
                'bar': {'color': "#50fa7b", 'thickness': 0.6},
                'bgcolor': "#44475a",
                'borderwidth': 2,
                'bordercolor': "#6272a4",
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
            domain={'x': [0, 1], 'y': [0.25, 1]},
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

        # Add timestamp
        if last_update:
            timestamp_str = last_update.strftime("%Y-%m-%d %H:%M:%S")
            fig.add_annotation(
                x=0.5, y=0.20,
                text=f"<b>Updated: {timestamp_str}</b>",
                showarrow=False,
                xref="paper", yref="paper",
                align="center",
                font=dict(size=12, color="white")
            )

        # Add Records header
        fig.add_annotation(
            x=0.5, y=0.12,
            text="<b>Records:</b>",
            showarrow=False,
            xref="paper", yref="paper",
            align="center",
            font=dict(size=11, color="white")
        )

        # Legend with side-by-side layout
        # Gold line (all-time)
        fig.add_shape(
            type="line",
            x0=0.20, y0=0.06,
            x1=0.25, y1=0.06,
            line=dict(color="gold", width=4),
            xref="paper", yref="paper"
        )

        fig.add_annotation(
            x=0.26, y=0.06,
            text=f"All-time: {all_time_record:.0f}%",
            showarrow=False,
            xref="paper", yref="paper",
            align="left",
            font=dict(size=10, color="white")
        )

        # Light teal line (hour)
        fig.add_shape(
            type="line",
            x0=0.55, y0=0.06,
            x1=0.60, y1=0.06,
            line=dict(color="#5DCED0", width=4),
            xref="paper", yref="paper"
        )

        fig.add_annotation(
            x=0.61, y=0.06,
            text=f"Hour: {hour_record:.0f}%",
            showarrow=False,
            xref="paper", yref="paper",
            align="left",
            font=dict(size=10, color="white")
        )

        fig.update_layout(
            paper_bgcolor="#282a36",
            font={'color': "white"},
            margin=dict(l=20, r=20, t=40, b=40),
            height=450,
            width=500,
            showlegend=False
        )

        return fig

    except Exception as e:
        print(f"Error creating gauge: {e}")
        return go.Figure()

async def update_gauge_periodically(gauge_pane, data_dir, records_file, update_interval):
    """Update gauge periodically"""
    while True:
        try:
            # Get latest stats
            stats = get_latest_generation_stats(data_dir)

            if stats:
                # Load and update records with alerts
                records = load_renewable_records(records_file)
                records, alerts_sent = update_records_with_alerts(stats, records, records_file)

                # Get record values
                all_time_record = records['all_time']['value']
                current_hour = stats['timestamp'].hour
                hour_record = records['hourly'].get(str(current_hour), {}).get('value', 0)

                # Create updated gauge
                fig = create_renewable_gauge_plotly(
                    stats['renewable_pct'],
                    all_time_record,
                    hour_record,
                    stats['timestamp']
                )

                # Update the pane
                gauge_pane.object = fig

                # Log status
                print(f"[{datetime.now().strftime('%H:%M:%S')}] "
                      f"Renewable: {stats['renewable_pct']:.1f}% | "
                      f"Records - All: {all_time_record:.1f}% Hour: {hour_record:.1f}%")

                if alerts_sent:
                    print(f"Alerts sent: {', '.join(alerts_sent)}")
            else:
                print(f"[{datetime.now().strftime('%H:%M:%S')}] No data available")

        except Exception as e:
            print(f"Error updating gauge: {e}")

        await asyncio.sleep(update_interval)

def main():
    parser = argparse.ArgumentParser(description='Standalone Renewable Energy Gauge with SMS Alerts')
    parser.add_argument('--port', type=int, default=5009, help='Port to run the server on')
    parser.add_argument('--data-dir', help='Data directory path')
    parser.add_argument('--env-file', help='Path to .env file')
    parser.add_argument('--update-interval', type=int, default=60, help='Update interval in seconds')
    parser.add_argument('--test-alerts', action='store_true', help='Test SMS alerts with simulated records')

    args = parser.parse_args()

    # Load environment
    load_environment(args.env_file)

    # Set data directory
    data_dir = args.data_dir or os.getenv('DATA_DIR', '/Volumes/davidleitch/aemo_production/data')
    data_dir = Path(data_dir)

    if not data_dir.exists():
        print(f"Error: Data directory not found: {data_dir}")
        sys.exit(1)

    print(f"Using data directory: {data_dir}")

    # Initialize Twilio
    twilio_enabled = initialize_twilio()
    print(f"SMS alerts: {'Enabled' if twilio_enabled else 'Disabled'}")

    # Test mode
    if args.test_alerts:
        print("\n=== TEST MODE: Simulating record-breaking values ===\n")
        test_stats = {
            'timestamp': datetime.now(),
            'renewable_pct': 75.5,
            'wind_mw': 5500,
            'solar_mw': 4200,
            'rooftop_mw': 20000,
            'water_mw': 7000,
            'renewable_mw': 45000,
            'total_mw': 52941
        }

        records_file = data_dir / 'renewable_records_test.json'
        records = load_renewable_records(records_file)

        print("Simulating record-breaking values...")
        records, alerts_sent = update_records_with_alerts(test_stats, records, records_file)
        print(f"Alerts that would be sent: {alerts_sent}")
        return

    # Records file
    records_file = data_dir / 'renewable_records.json'

    # Initialize Panel
    pn.extension('plotly')

    # Get initial stats
    print("Loading initial data...")
    stats = get_latest_generation_stats(data_dir)

    if stats:
        # Load records
        records = load_renewable_records(records_file)
        records, _ = update_records_with_alerts(stats, records, records_file)

        all_time_record = records['all_time']['value']
        current_hour = stats['timestamp'].hour
        hour_record = records['hourly'].get(str(current_hour), {}).get('value', 0)

        # Create initial gauge
        fig = create_renewable_gauge_plotly(
            stats['renewable_pct'],
            all_time_record,
            hour_record,
            stats['timestamp']
        )

        print(f"Initial renewable: {stats['renewable_pct']:.1f}%")
        print(f"All-time record: {all_time_record:.1f}%")
        print(f"Hour record: {hour_record:.1f}%")
    else:
        print("No initial data available, showing empty gauge")
        fig = create_renewable_gauge_plotly(0, 0, 0)

    # Create Panel app
    gauge_pane = pn.pane.Plotly(fig, sizing_mode='fixed', width=500, height=450)

    template = pn.template.MaterialTemplate(
        title="Renewable Energy Monitor",
        theme=pn.template.DarkTheme
    )
    template.main.append(gauge_pane)

    # Start periodic updates
    async def start_updates():
        await update_gauge_periodically(gauge_pane, data_dir, records_file, args.update_interval)

    # Start the server with background task
    print(f"\nStarting server on http://localhost:{args.port}")
    print(f"Update interval: {args.update_interval} seconds")
    print("Press Ctrl+C to stop\n")

    pn.serve(
        template,
        port=args.port,
        show=False,
        start=True,
        loop=asyncio.get_event_loop()
    )

    # Run the update loop
    asyncio.get_event_loop().run_until_complete(start_updates())

if __name__ == "__main__":
    main()