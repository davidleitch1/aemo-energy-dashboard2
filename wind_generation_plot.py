#!/usr/bin/env python3
"""
Jupyter notebook code for plotting wind generation with matplotx dracula theme
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotx
import pickle
from datetime import datetime, timedelta
import os

# Set up paths - adjust these to your actual file locations
# These should be the paths to your AEMO data files
gen_30min_path = 'data/gen_output.parquet'  # 30-minute data
gen_5min_path = gen_30min_path.replace('gen_output.parquet', 'scada5.parquet') if 'scada5.parquet' in os.listdir('data/') else None
gen_info_path = 'data/gen_info.pkl'  # DUID mapping file

# Load DUID mapping to identify wind generators
with open(gen_info_path, 'rb') as f:
    gen_info = pickle.load(f)

# Get wind DUIDs
wind_duids = gen_info[gen_info['Fuel'] == 'Wind']['DUID'].tolist()
print(f"Found {len(wind_duids)} wind generators")

# Load generation data
# Try 5-minute data first, fall back to 30-minute
if gen_5min_path and os.path.exists(gen_5min_path):
    gen_df = pd.read_parquet(gen_5min_path)
    print("Using 5-minute data")
else:
    gen_df = pd.read_parquet(gen_30min_path)
    print("Using 30-minute data")

# Convert to datetime
gen_df['settlementdate'] = pd.to_datetime(gen_df['settlementdate'])

# Filter for wind generation only
wind_gen = gen_df[gen_df['duid'].isin(wind_duids)].copy()

# Aggregate wind generation by time
wind_agg = wind_gen.groupby('settlementdate')['scadavalue'].sum().reset_index()
wind_agg.columns = ['time', 'wind_mw']

# Sort by time
wind_agg = wind_agg.sort_values('time')

# For better performance with large datasets, sample if needed
if len(wind_agg) > 100000:
    # Take every nth point to reduce data
    n = len(wind_agg) // 50000
    wind_agg = wind_agg.iloc[::n]
    print(f"Sampled data to {len(wind_agg)} points for plotting")

# Find the highest and second highest points
top_2_idx = wind_agg.nlargest(2, 'wind_mw').index
highest_point = wind_agg.loc[top_2_idx[0]]
second_highest = wind_agg.loc[top_2_idx[1]]

# Create the plot with dracula theme
with plt.style.context(matplotx.styles.dracula):
    fig, ax = plt.subplots(figsize=(14, 8))
    
    # Scatter plot
    scatter = ax.scatter(wind_agg['time'], wind_agg['wind_mw'], 
                        alpha=0.6, s=1, color='#50fa7b', label='Wind Generation')
    
    # Highlight the top 2 points
    ax.scatter(highest_point['time'], highest_point['wind_mw'], 
              color='#ff5555', s=100, zorder=5, marker='*')
    ax.scatter(second_highest['time'], second_highest['wind_mw'], 
              color='#ffb86c', s=100, zorder=5, marker='*')
    
    # Label the highest points
    ax.annotate(f"Highest: {highest_point['wind_mw']:.0f} MW\n{highest_point['time'].strftime('%Y-%m-%d %H:%M')}", 
                xy=(highest_point['time'], highest_point['wind_mw']),
                xytext=(10, 10), textcoords='offset points',
                bbox=dict(boxstyle='round,pad=0.3', facecolor='#ff5555', alpha=0.7),
                fontsize=10, color='white',
                arrowprops=dict(arrowstyle='->', connectionstyle='arc3,rad=0', color='#ff5555'))
    
    ax.annotate(f"2nd: {second_highest['wind_mw']:.0f} MW\n{second_highest['time'].strftime('%Y-%m-%d %H:%M')}", 
                xy=(second_highest['time'], second_highest['wind_mw']),
                xytext=(10, -30), textcoords='offset points',
                bbox=dict(boxstyle='round,pad=0.3', facecolor='#ffb86c', alpha=0.7),
                fontsize=10, color='white',
                arrowprops=dict(arrowstyle='->', connectionstyle='arc3,rad=0', color='#ffb86c'))
    
    # Formatting
    ax.set_xlabel('Time', fontsize=12)
    ax.set_ylabel('Wind Generation (MW)', fontsize=12)
    ax.set_title('NEM Wind Generation Over Time', fontsize=16, fontweight='bold')
    
    # Grid
    ax.grid(True, alpha=0.3, linestyle='--')
    
    # Format y-axis
    ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'{x:,.0f}'))
    
    # Rotate x-axis labels
    plt.xticks(rotation=45)
    
    # Add some statistics
    textstr = f'Max: {wind_agg["wind_mw"].max():,.0f} MW\n'
    textstr += f'Mean: {wind_agg["wind_mw"].mean():,.0f} MW\n'
    textstr += f'Current: {wind_agg.iloc[-1]["wind_mw"]:,.0f} MW'
    
    props = dict(boxstyle='round', facecolor='#44475a', alpha=0.8)
    ax.text(0.02, 0.98, textstr, transform=ax.transAxes, fontsize=10,
            verticalalignment='top', bbox=props, color='white')
    
    plt.tight_layout()
    plt.show()

# Print summary statistics
print("\nWind Generation Summary:")
print(f"Data period: {wind_agg['time'].min()} to {wind_agg['time'].max()}")
print(f"Total points: {len(wind_agg):,}")
print(f"Maximum: {wind_agg['wind_mw'].max():,.0f} MW at {wind_agg.loc[wind_agg['wind_mw'].idxmax(), 'time']}")
print(f"Average: {wind_agg['wind_mw'].mean():,.0f} MW")
print(f"Current: {wind_agg.iloc[-1]['wind_mw']:,.0f} MW at {wind_agg.iloc[-1]['time']}")