#!/usr/bin/env python3
"""
Standalone Renewable Energy Gauge Server - Simple Direct Read Version
Reads directly from parquet files without complex DuckDB setup
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

# Renewable fuel types
RENEWABLE_FUELS = ['Wind', 'Solar', 'Water', 'Rooftop Solar', 'Hydro', 'Biomass']

# Excluded fuel types
EXCLUDED_FUELS = ['Battery Storage', 'Transmission Flow']

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

def get_current_renewable_percentage():
    """Get current renewable percentage by reading parquet files directly"""
    try:
        # Get file paths from environment
        gen_5min_path = os.getenv('GEN_OUTPUT_FILE_5MIN')
        gen_30min_path = os.getenv('GEN_OUTPUT_FILE')
        rooftop_path = os.getenv('ROOFTOP_SOLAR_FILE')
        gen_info_path = os.getenv('GEN_INFO_FILE')
        
        # If 5-minute path not set, try to derive it
        if not gen_5min_path and gen_30min_path:
            gen_5min_path = gen_30min_path.replace('scada30.parquet', 'scada5.parquet')
        
        print(f"Reading from:")
        print(f"  Generation 5min: {gen_5min_path}")
        print(f"  Rooftop 30min: {rooftop_path}")
        print(f"  Gen info: {gen_info_path}")
        
        # Load DUID mapping
        import pickle
        with open(gen_info_path, 'rb') as f:
            gen_info = pickle.load(f)
        
        # Create DUID to fuel mapping
        duid_to_fuel = gen_info.set_index('DUID')['Fuel'].to_dict()
        
        # Get time range - last 15 minutes
        end_time = datetime.now()
        start_time = end_time - timedelta(minutes=15)
        
        # Load generation data
        print(f"\nLoading generation data from {start_time} to {end_time}")
        gen_df = pd.read_parquet(gen_5min_path)
        
        # Filter by time
        gen_df['settlementdate'] = pd.to_datetime(gen_df['settlementdate'])
        gen_df = gen_df[(gen_df['settlementdate'] >= start_time) & 
                        (gen_df['settlementdate'] <= end_time)]
        
        if gen_df.empty:
            print("No recent generation data found")
            return 0
        
        # Map fuel types
        gen_df['fuel_type'] = gen_df['duid'].map(duid_to_fuel)
        gen_df = gen_df.dropna(subset=['fuel_type'])
        
        # Exclude battery storage and transmission
        excluded_count = gen_df[gen_df['fuel_type'].isin(EXCLUDED_FUELS)]['scadavalue'].sum()
        if excluded_count > 0:
            print(f"Excluding {excluded_count:.0f} MW from battery/transmission")
        gen_df = gen_df[~gen_df['fuel_type'].isin(EXCLUDED_FUELS)]
        
        # Get latest timestamp
        latest_time = gen_df['settlementdate'].max()
        latest_gen = gen_df[gen_df['settlementdate'] == latest_time]
        
        # Aggregate by fuel type
        fuel_totals = latest_gen.groupby('fuel_type')['scadavalue'].sum()
        
        # Debug: Show fuel totals
        print(f"\nFuel totals at {latest_time}:")
        for fuel, mw in fuel_totals.items():
            if mw > 0:
                print(f"  {fuel}: {mw:.0f} MW")
        
        # Load and interpolate rooftop data
        print("\nLoading rooftop data...")
        rooftop_df = pd.read_parquet(rooftop_path)
        
        # Debug: Show columns
        print(f"Rooftop columns: {list(rooftop_df.columns)}")
        print(f"Rooftop shape: {rooftop_df.shape}")
        if len(rooftop_df) > 0:
            print(f"First row: {rooftop_df.iloc[0].to_dict()}")
        
        rooftop_df['settlementdate'] = pd.to_datetime(rooftop_df['settlementdate'])
        rooftop_df = rooftop_df[(rooftop_df['settlementdate'] >= start_time - timedelta(hours=1)) & 
                                (rooftop_df['settlementdate'] <= end_time)]
        
        if not rooftop_df.empty:
            # Check if this is already wide format or needs pivoting
            if 'regionid' in rooftop_df.columns:
                # Long format - need to pivot
                # Find the value column (might be scadavalue or something else)
                value_col = None
                for col in ['rooftop_mw', 'scadavalue', 'value', 'mw', 'power']:
                    if col in rooftop_df.columns:
                        value_col = col
                        break
                
                if value_col:
                    print(f"Pivoting using value column: {value_col}")
                    rooftop_wide = rooftop_df.pivot(
                        index='settlementdate',
                        columns='regionid',
                        values=value_col
                    ).fillna(0)
                else:
                    print(f"ERROR: Cannot find value column in rooftop data. Columns: {list(rooftop_df.columns)}")
                    rooftop_mw = 0
                    rooftop_wide = None
            else:
                # Already wide format
                print("Rooftop data is already in wide format")
                rooftop_wide = rooftop_df.set_index('settlementdate')
                # Remove non-region columns
                region_cols = ['NSW1', 'QLD1', 'SA1', 'TAS1', 'VIC1']
                rooftop_wide = rooftop_wide[[c for c in rooftop_wide.columns if c in region_cols]]
            
            if rooftop_wide is not None:
                # Interpolate to 5-minute
                rooftop_5min = interpolate_rooftop_to_5min(rooftop_wide)
                
                # Get value at latest timestamp
                if latest_time in rooftop_5min.index:
                    rooftop_mw = rooftop_5min.loc[latest_time, 'NEM']
                else:
                    # Find closest timestamp
                    closest_idx = rooftop_5min.index.get_indexer([latest_time], method='nearest')[0]
                    rooftop_mw = rooftop_5min.iloc[closest_idx]['NEM']
                
                print(f"Rooftop Solar: {rooftop_mw:.0f} MW")
            else:
                rooftop_mw = 0
        else:
            rooftop_mw = 0
            print("No rooftop data available")
        
        # Calculate renewable percentage
        renewable_mw = fuel_totals[fuel_totals.index.isin(RENEWABLE_FUELS)].sum() + rooftop_mw
        # Total MW excludes batteries since we filtered them out above
        total_mw = fuel_totals.sum() + rooftop_mw
        
        if total_mw > 0:
            renewable_pct = (renewable_mw / total_mw) * 100
            print(f"\nRenewable: {renewable_mw:.0f} MW / {total_mw:.0f} MW = {renewable_pct:.1f}%")
            return renewable_pct
        else:
            return 0
            
    except Exception as e:
        print(f"Error calculating renewable percentage: {e}")
        import traceback
        traceback.print_exc()
        return 0

def load_renewable_records(records_file):
    """Load historical renewable energy records"""
    try:
        if records_file.exists():
            with open(records_file, 'r') as f:
                records = json.load(f)
            if 'all_time' in records and 'hourly' in records:
                return records
    except Exception as e:
        print(f"Error loading records: {e}")
    
    # Return default records
    return {
        'all_time': {'value': 68.5, 'timestamp': '2024-10-13T13:30:00'},
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

def update_records(current_percentage, current_time, current_hour, records, records_file):
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
        print(f"New all-time renewable record: {current_percentage:.1f}%")
    
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
        print(f"New {current_hour}:00 hour record: {current_percentage:.1f}%")
    
    if records_updated:
        save_renewable_records(records, records_file)
    
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

import threading
import time

# Global variable to hold the current gauge
current_gauge_pane = None

def update_gauge_loop():
    """Background thread to update gauge every 4.5 minutes"""
    global current_gauge_pane
    data_dir = os.getenv('DATA_DIR', '/tmp')
    records_file = Path(data_dir) / 'renewable_records.json'
    
    while True:
        try:
            print(f"\n[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Updating gauge...")
            
            # Get current renewable percentage
            current_percentage = get_current_renewable_percentage()
            
            # Get current time info
            current_time = datetime.now()
            current_hour = current_time.hour
            
            # Load and update records
            records = load_renewable_records(records_file)
            all_time_record, hour_record, _ = update_records(
                current_percentage, current_time, current_hour, records, records_file
            )
            
            # Create gauge figure
            fig = create_gauge_figure(current_percentage, all_time_record, hour_record)
            
            # Update the pane if it exists
            if current_gauge_pane is not None:
                current_gauge_pane.object = fig
            
            print(f"Gauge updated: {current_percentage:.1f}% renewable")
            
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
        current_percentage = get_current_renewable_percentage()
        current_time = datetime.now()
        current_hour = current_time.hour
        
        data_dir = os.getenv('DATA_DIR', '/tmp')
        records_file = Path(data_dir) / 'renewable_records.json'
        records = load_renewable_records(records_file)
        all_time_record, hour_record, _ = update_records(
            current_percentage, current_time, current_hour, records, records_file
        )
        
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
    parser = argparse.ArgumentParser(description='Standalone Renewable Energy Gauge Server - Simple Version')
    parser.add_argument('--port', type=int, default=5007, help='Port to serve on (default: 5007)')
    parser.add_argument('--host', type=str, default='0.0.0.0', help='Host to serve on (default: 0.0.0.0)')
    parser.add_argument('--env-file', type=str, help='Path to .env file to use')
    parser.add_argument('--test', action='store_true', help='Test data loading and exit')
    args = parser.parse_args()
    
    # Load environment
    if not load_environment(args.env_file):
        print("Warning: Could not load environment file")
    
    # If test mode, just test data loading
    if args.test:
        print("\n=== Testing Renewable Data Loading ===")
        percentage = get_current_renewable_percentage()
        print(f"\nFinal result: {percentage:.1f}%")
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
    print(f"Renewable Energy Gauge Server - Simple Version")
    print(f"{'='*60}")
    print(f"Port: {args.port}")
    print(f"Host: {args.host}")
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
        title='Renewable Gauge'
    )

if __name__ == '__main__':
    main()