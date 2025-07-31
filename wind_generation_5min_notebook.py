# Jupyter Notebook code for plotting NEM wind generation with 5-minute data
# This version specifically uses 5-minute data for recent periods to capture the 10.1 GW peak

# Cell 1: Imports and setup
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotx
import pickle
from datetime import datetime, timedelta
import os

# Cell 2: Load data paths
# Update these to your actual paths
scada5_path = '/Users/davidleitch/Library/Mobile Documents/com~apple~CloudDocs/snakeplay/AEMO_spot/aemo-data-updater/data 2/scada5.parquet'
scada30_path = '/Users/davidleitch/Library/Mobile Documents/com~apple~CloudDocs/snakeplay/AEMO_spot/aemo-data-updater/data 2/scada30.parquet'
gen_info_path = '/Users/davidleitch/Library/Mobile Documents/com~apple~CloudDocs/snakeplay/AEMO_spot/aemo-energy-dashboard/data/gen_info.pkl'

# Load DUID mapping
with open(gen_info_path, 'rb') as f:
    gen_info = pickle.load(f)

# Get wind DUIDs
wind_duids = gen_info[gen_info['Fuel'] == 'Wind']['DUID'].tolist()
print(f"Found {len(wind_duids)} wind generators")
print(f"Total wind capacity: {gen_info[gen_info['Fuel'] == 'Wind']['Capacity(MW)'].sum():,.0f} MW")

# Cell 3: Load and combine data
# Strategy: Use 30-minute data for historical, 5-minute for recent (last 2 months)
cutoff_date = datetime.now() - timedelta(days=60)

# Load 30-minute historical data
print("Loading 30-minute historical data...")
gen_30 = pd.read_parquet(scada30_path)
gen_30['settlementdate'] = pd.to_datetime(gen_30['settlementdate'])
gen_30_historical = gen_30[gen_30['settlementdate'] < cutoff_date]

# Load 5-minute recent data
print("Loading 5-minute recent data...")
gen_5 = pd.read_parquet(scada5_path)
gen_5['settlementdate'] = pd.to_datetime(gen_5['settlementdate'])
gen_5_recent = gen_5[gen_5['settlementdate'] >= cutoff_date]

# Combine the datasets
gen_combined = pd.concat([gen_30_historical, gen_5_recent], ignore_index=True)
gen_combined = gen_combined.sort_values('settlementdate')

print(f"Combined data: {len(gen_combined):,} records")
print(f"Date range: {gen_combined['settlementdate'].min()} to {gen_combined['settlementdate'].max()}")

# Cell 4: Process wind data
# Filter for wind generation
wind_gen = gen_combined[gen_combined['duid'].isin(wind_duids)].copy()

# Aggregate by time
wind_agg = wind_gen.groupby('settlementdate')['scadavalue'].sum().reset_index()
wind_agg.columns = ['time', 'wind_mw']
wind_agg = wind_agg.sort_values('time')

# Check for the peak
print(f"\nWind generation statistics:")
print(f"Maximum: {wind_agg['wind_mw'].max():,.1f} MW")
print(f"Date of maximum: {wind_agg.loc[wind_agg['wind_mw'].idxmax(), 'time']}")

# Cell 5: Find peak points
# Find highest and second highest
top_2 = wind_agg.nlargest(2, 'wind_mw')
highest_point = top_2.iloc[0]
second_highest = top_2.iloc[1]

print(f"\nTop 2 peaks:")
print(f"1st: {highest_point['wind_mw']:,.1f} MW on {highest_point['time']}")
print(f"2nd: {second_highest['wind_mw']:,.1f} MW on {second_highest['time']}")

# Cell 6: Create the main plot
plt.figure(figsize=(16, 10), dpi=100)

with plt.style.context(matplotx.styles.dracula):
    # Separate 30-min and 5-min data for different plotting
    is_5min = wind_agg['time'] >= cutoff_date
    
    # Plot 30-minute data with larger dots
    plt.scatter(wind_agg.loc[~is_5min, 'time'], wind_agg.loc[~is_5min, 'wind_mw'], 
                alpha=0.4, s=2, color='#50fa7b', label='30-min data')
    
    # Plot 5-minute data with smaller dots for density
    plt.scatter(wind_agg.loc[is_5min, 'time'], wind_agg.loc[is_5min, 'wind_mw'], 
                alpha=0.6, s=0.5, color='#8be9fd', label='5-min data')
    
    # Highlight top 2 points
    plt.scatter(highest_point['time'], highest_point['wind_mw'], 
                color='#ff5555', s=300, zorder=5, marker='*', 
                edgecolors='white', linewidth=2)
    plt.scatter(second_highest['time'], second_highest['wind_mw'], 
                color='#ffb86c', s=300, zorder=5, marker='*',
                edgecolors='white', linewidth=2)
    
    # Add annotations with exact values
    plt.annotate(f"Peak: {highest_point['wind_mw']/1000:.2f} GW\n{highest_point['time'].strftime('%d %b %Y %H:%M')}", 
                xy=(highest_point['time'], highest_point['wind_mw']),
                xytext=(30, 30), textcoords='offset points',
                bbox=dict(boxstyle='round,pad=0.5', facecolor='#ff5555', alpha=0.9),
                fontsize=12, color='white', weight='bold',
                arrowprops=dict(arrowstyle='->', connectionstyle='arc3,rad=0.1', 
                               color='#ff5555', lw=2))
    
    plt.annotate(f"2nd: {second_highest['wind_mw']/1000:.2f} GW\n{second_highest['time'].strftime('%d %b %Y %H:%M')}", 
                xy=(second_highest['time'], second_highest['wind_mw']),
                xytext=(30, -50), textcoords='offset points',
                bbox=dict(boxstyle='round,pad=0.5', facecolor='#ffb86c', alpha=0.9),
                fontsize=12, color='white', weight='bold',
                arrowprops=dict(arrowstyle='->', connectionstyle='arc3,rad=-0.1', 
                               color='#ffb86c', lw=2))
    
    # Formatting
    plt.xlabel('Time', fontsize=14, weight='bold')
    plt.ylabel('Wind Generation (MW)', fontsize=14, weight='bold')
    plt.title('NEM Wind Generation - Combined 30-min Historical & 5-min Recent Data', 
              fontsize=18, weight='bold', pad=20)
    
    # Grid
    plt.grid(True, alpha=0.3, linestyle='--', linewidth=0.5)
    
    # Format axes
    ax = plt.gca()
    ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'{x:,.0f}'))
    
    # Statistics box
    stats_text = f'Wind Generation Statistics:\n'
    stats_text += f'All-time Peak: {wind_agg["wind_mw"].max()/1000:.2f} GW\n'
    stats_text += f'Average: {wind_agg["wind_mw"].mean():,.0f} MW\n'
    stats_text += f'Current: {wind_agg.iloc[-1]["wind_mw"]:,.0f} MW\n'
    stats_text += f'Data points: {len(wind_agg):,}'
    
    props = dict(boxstyle='round,pad=0.5', facecolor='#44475a', alpha=0.9)
    ax.text(0.02, 0.97, stats_text, transform=ax.transAxes, fontsize=11,
            verticalalignment='top', bbox=props, color='white', weight='bold')
    
    # Add 10 GW reference line
    plt.axhline(y=10000, color='#bd93f9', linestyle=':', alpha=0.5, 
                label='10 GW threshold')
    
    # Total capacity line
    total_capacity = gen_info[gen_info['Fuel'] == 'Wind']['Capacity(MW)'].sum()
    plt.axhline(y=total_capacity, color='#f1fa8c', linestyle='--', alpha=0.5, 
                label=f'Total Capacity: {total_capacity:,.0f} MW')
    
    plt.legend(loc='upper right', fontsize=11)
    plt.tight_layout()
    plt.show()

# Cell 7: Zoom plot on recent 5-minute data
# Create a second plot focusing on the last 30 days
recent_30d = wind_agg[wind_agg['time'] >= (datetime.now() - timedelta(days=30))]

plt.figure(figsize=(16, 8), dpi=100)

with plt.style.context(matplotx.styles.dracula):
    plt.scatter(recent_30d['time'], recent_30d['wind_mw'], 
                alpha=0.7, s=1, color='#8be9fd')
    
    # Find peak in recent data
    recent_peak = recent_30d.loc[recent_30d['wind_mw'].idxmax()]
    plt.scatter(recent_peak['time'], recent_peak['wind_mw'], 
                color='#ff5555', s=300, zorder=5, marker='*', 
                edgecolors='white', linewidth=2)
    
    plt.annotate(f"Recent Peak: {recent_peak['wind_mw']/1000:.2f} GW\n{recent_peak['time'].strftime('%d %b %Y %H:%M')}", 
                xy=(recent_peak['time'], recent_peak['wind_mw']),
                xytext=(30, -30), textcoords='offset points',
                bbox=dict(boxstyle='round,pad=0.5', facecolor='#ff5555', alpha=0.9),
                fontsize=12, color='white', weight='bold',
                arrowprops=dict(arrowstyle='->', connectionstyle='arc3,rad=0.1', 
                               color='#ff5555', lw=2))
    
    plt.xlabel('Time', fontsize=14, weight='bold')
    plt.ylabel('Wind Generation (MW)', fontsize=14, weight='bold')
    plt.title('NEM Wind Generation - Last 30 Days (5-minute data)', 
              fontsize=18, weight='bold', pad=20)
    
    plt.grid(True, alpha=0.3, linestyle='--', linewidth=0.5)
    
    ax = plt.gca()
    ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'{x:,.0f}'))
    
    # 10 GW line
    plt.axhline(y=10000, color='#bd93f9', linestyle=':', alpha=0.5, 
                label='10 GW threshold')
    
    plt.legend(loc='upper right', fontsize=11)
    plt.tight_layout()
    plt.show()

# Cell 8: Verify the peak value
# Double-check by looking at the exact peak time window
peak_time = wind_agg.loc[wind_agg['wind_mw'].idxmax(), 'time']
window_start = peak_time - timedelta(hours=1)
window_end = peak_time + timedelta(hours=1)

peak_window = wind_agg[(wind_agg['time'] >= window_start) & (wind_agg['time'] <= window_end)]
print(f"\nDetailed view around peak ({peak_time}):")
print(peak_window.sort_values('wind_mw', ascending=False).head(10))