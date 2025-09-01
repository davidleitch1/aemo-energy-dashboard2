#!/usr/bin/env python3
"""
Standalone Generation Stack Chart for South Australia
Shows the last 72 hours of generation by fuel type with battery charging/discharging
Serves an interactive plot on localhost
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from pathlib import Path
import pickle

# Panel and hvplot for interactive visualization
import panel as pn
import hvplot.pandas
import holoviews as hv
from bokeh.models import HoverTool

# Initialize Panel extensions
pn.extension('tabulator')
hv.extension('bokeh')

# Production data path
DATA_PATH = Path("/Volumes/davidleitch/aemo_production/data")

# Color scheme for fuel types (matching the dashboard)
FUEL_COLORS = {
    'Battery Storage': '#B8860B',  # DarkGoldenrod
    'Coal': '#2F4F4F',             # DarkSlateGray
    'Gas': '#FF6347',              # Tomato
    'CCGT': '#FF6347',             # Tomato (subtype of Gas)
    'OCGT': '#FFA07A',             # LightSalmon (subtype of Gas)
    'Gas other': '#FA8072',        # Salmon (subtype of Gas)
    'Hydro': '#4682B4',            # SteelBlue
    'Water': '#4682B4',            # SteelBlue
    'Wind': '#90EE90',             # LightGreen
    'Solar': '#FFD700',            # Gold
    'Rooftop Solar': '#FFA500',    # Orange
    'Other': '#D3D3D3',            # LightGray
    'Biomass': '#8B4513',          # SaddleBrown
    'Liquid Fuel': '#8B008B',      # DarkMagenta
}

def load_sa_generation_data(hours=72):
    """Load South Australian generation data for the specified number of hours"""
    
    print(f"Loading last {hours} hours of SA generation data...")
    
    # Calculate date range
    end_date = datetime.now()
    start_date = end_date - timedelta(hours=hours)
    
    # Load generator info for fuel type mapping
    gen_info_file = DATA_PATH / "gen_info.pkl"
    with open(gen_info_file, 'rb') as f:
        gen_info = pickle.load(f)
    
    # Filter for SA generators
    sa_gen_info = gen_info[gen_info['Region'] == 'SA1'].copy()
    duid_to_fuel = dict(zip(sa_gen_info['DUID'], sa_gen_info['Fuel']))
    sa_duids = sa_gen_info['DUID'].tolist()
    
    # Load 30-minute generation data
    print("Loading generation data...")
    gen_data = pd.read_parquet(DATA_PATH / "scada30.parquet")
    gen_data['settlementdate'] = pd.to_datetime(gen_data['settlementdate'])
    
    # Filter for SA and date range
    sa_gen = gen_data[
        (gen_data['duid'].isin(sa_duids)) & 
        (gen_data['settlementdate'] >= start_date) &
        (gen_data['settlementdate'] <= end_date)
    ].copy()
    
    # Add fuel type
    sa_gen['fuel_type'] = sa_gen['duid'].map(duid_to_fuel)
    
    # Load rooftop solar
    print("Loading rooftop solar data...")
    rooftop_data = pd.read_parquet(DATA_PATH / "rooftop30.parquet")
    rooftop_data['settlementdate'] = pd.to_datetime(rooftop_data['settlementdate'])
    
    sa_rooftop = rooftop_data[
        (rooftop_data['regionid'] == 'SA1') & 
        (rooftop_data['settlementdate'] >= start_date) &
        (rooftop_data['settlementdate'] <= end_date)
    ].copy()
    
    # Load price data
    print("Loading price data...")
    price_data = pd.read_parquet(DATA_PATH / "prices30.parquet")
    price_data['settlementdate'] = pd.to_datetime(price_data['settlementdate'])
    
    sa_prices = price_data[
        (price_data['regionid'] == 'SA1') & 
        (price_data['settlementdate'] >= start_date) &
        (price_data['settlementdate'] <= end_date)
    ][['settlementdate', 'rrp']].copy()
    
    return sa_gen, sa_rooftop, sa_prices

def prepare_generation_stack_data(sa_gen, sa_rooftop):
    """Prepare data for stacked area chart"""
    
    # Aggregate generation by fuel type and time
    gen_by_fuel = sa_gen.groupby(['settlementdate', 'fuel_type'])['scadavalue'].sum().reset_index()
    gen_by_fuel = gen_by_fuel.rename(columns={'scadavalue': 'generation_mw'})
    
    # Add rooftop solar
    rooftop_agg = sa_rooftop.groupby('settlementdate')['power'].sum().reset_index()
    rooftop_agg['fuel_type'] = 'Rooftop Solar'
    rooftop_agg = rooftop_agg.rename(columns={'power': 'generation_mw'})
    
    # Combine all generation
    all_gen = pd.concat([gen_by_fuel, rooftop_agg], ignore_index=True)
    
    # Pivot to wide format for stacking
    gen_pivot = all_gen.pivot_table(
        index='settlementdate',
        columns='fuel_type',
        values='generation_mw',
        fill_value=0
    )
    
    # Separate positive and negative values for battery storage
    if 'Battery Storage' in gen_pivot.columns:
        battery_data = gen_pivot['Battery Storage'].copy()
        gen_pivot['Battery Charging'] = battery_data.clip(upper=0)  # Negative values only
        gen_pivot['Battery Discharging'] = battery_data.clip(lower=0)  # Positive values only
        gen_pivot = gen_pivot.drop(columns=['Battery Storage'])
    
    return gen_pivot

def create_generation_stack_plot(gen_pivot, sa_prices):
    """Create interactive stacked area chart with price overlay"""
    
    print("Creating generation stack plot...")
    
    # Reset index for plotting
    plot_data = gen_pivot.reset_index()
    
    # Define plot order (renewable at bottom, fossil at top)
    renewable_cols = ['Rooftop Solar', 'Solar', 'Wind', 'Water', 'Hydro', 'Battery Discharging']
    fossil_cols = ['CCGT', 'OCGT', 'Gas other', 'Gas', 'Coal', 'Liquid Fuel']
    other_cols = ['Other', 'Biomass']
    charging_cols = ['Battery Charging']
    
    # Order columns
    col_order = []
    for col_list in [renewable_cols, other_cols, fossil_cols, charging_cols]:
        for col in col_list:
            if col in plot_data.columns and col != 'settlementdate':
                col_order.append(col)
    
    # Add any remaining columns
    for col in plot_data.columns:
        if col not in col_order and col != 'settlementdate':
            col_order.append(col)
    
    # Create color map
    color_map = {}
    for col in col_order:
        if col == 'Battery Charging':
            color_map[col] = '#8B4513'  # Brown for charging (negative)
        elif col == 'Battery Discharging':
            color_map[col] = '#DAA520'  # Goldenrod for discharging
        else:
            color_map[col] = FUEL_COLORS.get(col, '#808080')
    
    # Create the stacked area plot with a list of colors
    colors_list = [color_map.get(col, '#808080') for col in col_order]
    
    generation_plot = plot_data.hvplot.area(
        x='settlementdate',
        y=col_order,
        stacked=True,
        title='South Australia Generation Stack (Last 72 Hours)',
        xlabel='Time',
        ylabel='Generation (MW)',
        width=1200,
        height=600,
        color=colors_list,
        alpha=0.8,
        legend='right',
        hover_cols=['settlementdate'] + col_order,
        grid=True
    ).opts(
        tools=['pan', 'wheel_zoom', 'box_zoom', 'reset', 'save'],
        active_tools=['pan', 'wheel_zoom']
    )
    
    # Add price line overlay
    if not sa_prices.empty:
        price_line = sa_prices.hvplot.line(
            x='settlementdate',
            y='rrp',
            color='red',
            width=2,
            alpha=0.7,
            label='Spot Price',
            ylabel='Price ($/MWh)',
            yaxis='right'
        )
        
        # Combine plots
        combined_plot = (generation_plot * price_line).opts(
            title='SA Generation Stack with Spot Prices',
            legend_position='right'
        )
    else:
        combined_plot = generation_plot
    
    return combined_plot

def create_summary_table(gen_pivot):
    """Create summary statistics table"""
    
    summary_data = []
    
    for col in gen_pivot.columns:
        if col == 'Battery Charging':
            # For charging, show absolute values
            values = gen_pivot[col].abs()
            total_mwh = values.sum() / 2  # Convert MW to MWh for 30-min periods
            summary_data.append({
                'Fuel Type': col,
                'Total (MWh)': f"{total_mwh:,.0f}",
                'Average (MW)': f"{values[values > 0].mean():.1f}" if (values > 0).any() else "0.0",
                'Max (MW)': f"{values.max():.1f}",
                'Periods Active': f"{(values > 0).sum()}"
            })
        else:
            total_mwh = gen_pivot[col].sum() / 2  # Convert MW to MWh for 30-min periods
            summary_data.append({
                'Fuel Type': col,
                'Total (MWh)': f"{total_mwh:,.0f}",
                'Average (MW)': f"{gen_pivot[col].mean():.1f}",
                'Max (MW)': f"{gen_pivot[col].max():.1f}",
                'Periods Active': f"{(gen_pivot[col] > 0).sum()}"
            })
    
    summary_df = pd.DataFrame(summary_data)
    summary_df = summary_df[summary_df['Total (MWh)'] != '0']  # Remove zero rows
    
    # Create tabulator widget
    summary_table = pn.widgets.Tabulator(
        summary_df,
        pagination='local',
        page_size=20,
        sizing_mode='stretch_width',
        height=400,
        show_index=False,
        theme='site',
        configuration={
            'columnDefaults': {
                'headerFilter': True,
                'headerFilterPlaceholder': 'Filter...'
            }
        }
    )
    
    return summary_table

def create_battery_analysis(sa_gen):
    """Create battery-specific analysis"""
    
    battery_data = sa_gen[sa_gen['fuel_type'] == 'Battery Storage'].copy()
    
    if battery_data.empty:
        return pn.pane.Markdown("No battery data available")
    
    # Aggregate by time
    battery_agg = battery_data.groupby('settlementdate')['scadavalue'].sum().reset_index()
    
    # Calculate statistics
    charging = battery_agg[battery_agg['scadavalue'] < 0]
    discharging = battery_agg[battery_agg['scadavalue'] > 0]
    
    stats_md = f"""
    ## Battery Storage Analysis
    
    ### Overall Statistics
    - **Total periods**: {len(battery_agg)}
    - **Charging periods**: {len(charging)} ({len(charging)/len(battery_agg)*100:.1f}%)
    - **Discharging periods**: {len(discharging)} ({len(discharging)/len(battery_agg)*100:.1f}%)
    
    ### Power Statistics
    - **Max charging rate**: {abs(charging['scadavalue'].min()):.1f} MW
    - **Max discharge rate**: {discharging['scadavalue'].max():.1f} MW
    - **Average charging**: {abs(charging['scadavalue'].mean()):.1f} MW
    - **Average discharge**: {discharging['scadavalue'].mean():.1f} MW
    
    ### Energy Throughput
    - **Total energy charged**: {abs(charging['scadavalue'].sum())/2:,.0f} MWh
    - **Total energy discharged**: {discharging['scadavalue'].sum()/2:,.0f} MWh
    - **Round-trip efficiency**: {discharging['scadavalue'].sum()/abs(charging['scadavalue'].sum())*100:.1f}%
    """
    
    # Create battery power plot
    battery_plot = battery_agg.hvplot.area(
        x='settlementdate',
        y='scadavalue',
        title='Battery Power (Positive = Discharge, Negative = Charge)',
        ylabel='Power (MW)',
        xlabel='Time',
        color=['brown', 'goldenrod'],
        width=1200,
        height=300,
        grid=True
    )
    
    return pn.Column(
        pn.pane.Markdown(stats_md),
        battery_plot
    )

def create_dashboard():
    """Create the complete dashboard"""
    
    # Load data
    sa_gen, sa_rooftop, sa_prices = load_sa_generation_data(hours=72)
    
    # Check for battery charging
    battery_data = sa_gen[sa_gen['fuel_type'] == 'Battery Storage']
    charging_count = (battery_data['scadavalue'] < 0).sum()
    print(f"\nBattery charging records found: {charging_count}")
    
    # Prepare data
    gen_pivot = prepare_generation_stack_data(sa_gen, sa_rooftop)
    
    # Create components
    main_plot = create_generation_stack_plot(gen_pivot, sa_prices)
    summary_table = create_summary_table(gen_pivot)
    battery_analysis = create_battery_analysis(sa_gen)
    
    # Create header
    header = pn.pane.Markdown(
        f"""
        # South Australia Generation Stack
        **Last 72 Hours** | Data updated: {datetime.now().strftime('%Y-%m-%d %H:%M')}
        
        This dashboard shows generation by fuel type including battery storage charging (negative) and discharging (positive).
        """,
        sizing_mode='stretch_width'
    )
    
    # Create layout
    template = pn.template.FastListTemplate(
        title="SA Generation Stack",
        sidebar=[
            pn.pane.Markdown("## Summary Statistics"),
            summary_table
        ],
        main=[
            header,
            main_plot,
            battery_analysis
        ],
        header_background='#2596be',
        theme=pn.template.DarkTheme
    )
    
    return template

def main():
    """Main execution"""
    print("="*60)
    print("South Australia Generation Stack Dashboard")
    print("="*60)
    
    # Create dashboard
    dashboard = create_dashboard()
    
    # Serve on localhost
    print("\nStarting server...")
    print("Dashboard will be available at: http://localhost:5006")
    print("Press Ctrl+C to stop the server")
    
    dashboard.show(port=5006, open=True)

if __name__ == "__main__":
    main()