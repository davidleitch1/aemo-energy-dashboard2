# Jupyter Notebook code for plotting NEM wind generation with matplotx dracula theme
# Copy and paste this code into your Jupyter notebook cells

# Cell 1: Imports and setup
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotx
import pickle
from datetime import datetime, timedelta
import os

# Cell 2: Load data
# Update these paths to match your data location
gen_30min_path = '/Users/davidleitch/Library/Mobile Documents/com~apple~CloudDocs/snakeplay/AEMO_spot/aemo-energy-dashboard/data/gen_output.parquet'
gen_info_path = '/Users/davidleitch/Library/Mobile Documents/com~apple~CloudDocs/snakeplay/AEMO_spot/aemo-energy-dashboard/data/gen_info.pkl'

# Load DUID mapping
with open(gen_info_path, 'rb') as f:
    gen_info = pickle.load(f)

# Get wind DUIDs
wind_duids = gen_info[gen_info['Fuel'] == 'Wind']['DUID'].tolist()
print(f"Found {len(wind_duids)} wind generators")

# Load 30-minute generation data
gen_df = pd.read_parquet(gen_30min_path)
gen_df['settlementdate'] = pd.to_datetime(gen_df['settlementdate'])

# Cell 3: Process wind data
# Filter for wind generation
wind_gen = gen_df[gen_df['duid'].isin(wind_duids)].copy()

# Aggregate by time
wind_agg = wind_gen.groupby('settlementdate')['scadavalue'].sum().reset_index()
wind_agg.columns = ['time', 'wind_mw']
wind_agg = wind_agg.sort_values('time')

# Optional: limit date range for better visualization
# Uncomment to use last 2 years of data
# cutoff_date = wind_agg['time'].max() - pd.Timedelta(days=730)
# wind_agg = wind_agg[wind_agg['time'] >= cutoff_date]

print(f"Data points: {len(wind_agg):,}")
print(f"Date range: {wind_agg['time'].min()} to {wind_agg['time'].max()}")

# Cell 4: Find peak points
# Find highest and second highest
top_2 = wind_agg.nlargest(2, 'wind_mw')
highest_point = top_2.iloc[0]
second_highest = top_2.iloc[1]

print(f"Highest: {highest_point['wind_mw']:,.0f} MW on {highest_point['time']}")
print(f"2nd highest: {second_highest['wind_mw']:,.0f} MW on {second_highest['time']}")

# Cell 5: Create the plot
# Set figure size and DPI for crisp display
plt.figure(figsize=(16, 10), dpi=100)

# Apply dracula theme
with plt.style.context(matplotx.styles.dracula):
    # Create scatter plot
    plt.scatter(wind_agg['time'], wind_agg['wind_mw'], 
                alpha=0.5, s=0.5, color='#50fa7b', label='Wind Generation')
    
    # Highlight top 2 points
    plt.scatter(highest_point['time'], highest_point['wind_mw'], 
                color='#ff5555', s=200, zorder=5, marker='*', 
                edgecolors='white', linewidth=1)
    plt.scatter(second_highest['time'], second_highest['wind_mw'], 
                color='#ffb86c', s=200, zorder=5, marker='*',
                edgecolors='white', linewidth=1)
    
    # Add annotations
    plt.annotate(f"Peak: {highest_point['wind_mw']:,.0f} MW\n{highest_point['time'].strftime('%d %b %Y %H:%M')}", 
                xy=(highest_point['time'], highest_point['wind_mw']),
                xytext=(20, 20), textcoords='offset points',
                bbox=dict(boxstyle='round,pad=0.5', facecolor='#ff5555', alpha=0.8),
                fontsize=11, color='white', weight='bold',
                arrowprops=dict(arrowstyle='->', connectionstyle='arc3,rad=0.1', 
                               color='#ff5555', lw=2))
    
    plt.annotate(f"2nd Peak: {second_highest['wind_mw']:,.0f} MW\n{second_highest['time'].strftime('%d %b %Y %H:%M')}", 
                xy=(second_highest['time'], second_highest['wind_mw']),
                xytext=(20, -40), textcoords='offset points',
                bbox=dict(boxstyle='round,pad=0.5', facecolor='#ffb86c', alpha=0.8),
                fontsize=11, color='white', weight='bold',
                arrowprops=dict(arrowstyle='->', connectionstyle='arc3,rad=-0.1', 
                               color='#ffb86c', lw=2))
    
    # Formatting
    plt.xlabel('Time', fontsize=14, weight='bold')
    plt.ylabel('Wind Generation (MW)', fontsize=14, weight='bold')
    plt.title('NEM Wind Generation - Historical Data', fontsize=18, weight='bold', pad=20)
    
    # Grid
    plt.grid(True, alpha=0.3, linestyle='--', linewidth=0.5)
    
    # Format axes
    ax = plt.gca()
    ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'{x:,.0f}'))
    
    # Statistics box
    stats_text = f'Statistics:\n'
    stats_text += f'Maximum: {wind_agg["wind_mw"].max():,.0f} MW\n'
    stats_text += f'Average: {wind_agg["wind_mw"].mean():,.0f} MW\n'
    stats_text += f'Minimum: {wind_agg["wind_mw"].min():,.0f} MW\n'
    stats_text += f'Latest: {wind_agg.iloc[-1]["wind_mw"]:,.0f} MW'
    
    props = dict(boxstyle='round,pad=0.5', facecolor='#44475a', alpha=0.9)
    ax.text(0.02, 0.97, stats_text, transform=ax.transAxes, fontsize=11,
            verticalalignment='top', bbox=props, color='white', weight='bold')
    
    # Add capacity line (optional)
    total_capacity = gen_info[gen_info['Fuel'] == 'Wind']['Capacity(MW)'].sum()
    plt.axhline(y=total_capacity, color='#bd93f9', linestyle='--', alpha=0.5, 
                label=f'Total Capacity: {total_capacity:,.0f} MW')
    
    plt.legend(loc='upper right', fontsize=11)
    plt.tight_layout()
    plt.show()

# Cell 6: Additional analysis (optional)
# Monthly statistics
wind_agg['month'] = wind_agg['time'].dt.to_period('M')
monthly_stats = wind_agg.groupby('month')['wind_mw'].agg(['mean', 'max', 'min', 'std'])
print("\nMonthly Wind Generation Statistics (last 12 months):")
print(monthly_stats.tail(12).round(0))