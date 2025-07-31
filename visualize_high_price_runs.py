#!/usr/bin/env python3
"""
Visualize high price runs across regions
Static plots for document inclusion
"""

import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import seaborn as sns
from datetime import datetime
import os

# Change to the directory containing the CSV files
os.chdir('/Users/davidleitch/Library/Mobile Documents/com~apple~CloudDocs/snakeplay/AEMO_spot/aemo-energy-dashboard')

# Set up Dracula theme colors
dracula_bg = '#282a36'
dracula_fg = '#f8f8f2'
dracula_purple = '#bd93f9'
dracula_pink = '#ff79c6'
dracula_cyan = '#8be9fd'
dracula_green = '#50fa7b'
dracula_orange = '#ffb86c'
dracula_red = '#ff5555'
dracula_yellow = '#f1fa8c'

# Region colors
region_colors = {
    'NSW1': dracula_cyan,
    'QLD1': dracula_orange,
    'SA1': dracula_pink,
    'TAS1': dracula_green,
    'VIC1': dracula_purple
}

# Load all CSV files
regions = ['NSW1', 'QLD1', 'SA1', 'TAS1', 'VIC1']
all_runs = []

for region in regions:
    df = pd.read_csv(f'high_price_runs_{region}.csv')
    df['region'] = region
    all_runs.append(df)

# Combine all data
runs_df = pd.concat(all_runs, ignore_index=True)
runs_df['start_time'] = pd.to_datetime(runs_df['start_time'])

# Set up matplotlib style
plt.style.use('dark_background')
plt.rcParams['figure.facecolor'] = dracula_bg
plt.rcParams['axes.facecolor'] = dracula_bg
plt.rcParams['axes.edgecolor'] = dracula_fg
plt.rcParams['axes.labelcolor'] = dracula_fg
plt.rcParams['text.color'] = dracula_fg
plt.rcParams['xtick.color'] = dracula_fg
plt.rcParams['ytick.color'] = dracula_fg
plt.rcParams['grid.color'] = '#44475a'
plt.rcParams['grid.alpha'] = 0.3

# 1. SCATTER PLOT WITH SIZED DOTS
fig, ax = plt.subplots(figsize=(14, 8))

# Plot each region
for region in regions:
    region_data = runs_df[runs_df['region'] == region]
    # Size dots by max price (scaled for visibility)
    sizes = np.sqrt(region_data['max_price'] / 1000) * 20
    
    ax.scatter(region_data['start_time'], 
              region_data['duration_hours'],
              c=region_colors[region],
              s=sizes,
              alpha=0.6,
              label=region,
              edgecolors='white',
              linewidth=0.5)

ax.set_yscale('log')
ax.set_ylabel('Duration (hours)', fontsize=12)
ax.set_xlabel('Date', fontsize=12)
ax.set_title('High Price Events (>$1000/MWh) Duration by Region\nDot size indicates peak price', 
             fontsize=14, pad=20)
ax.legend(frameon=True, facecolor=dracula_bg, edgecolor=dracula_fg)
ax.grid(True, which="both", ls="-", alpha=0.2)

# Format x-axis dates
import matplotlib.dates as mdates
ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m'))
ax.xaxis.set_major_locator(mdates.MonthLocator(interval=6))
plt.xticks(rotation=45)

plt.tight_layout()
plt.savefig('high_price_runs_scatter.png', dpi=300, facecolor=dracula_bg)
plt.show()

# 2. VIOLIN PLOT BY REGION
fig, ax = plt.subplots(figsize=(10, 8))

# Create violin plot
parts = ax.violinplot([runs_df[runs_df['region'] == r]['duration_hours'].values for r in regions],
                      positions=range(len(regions)),
                      showmeans=True,
                      showmedians=True)

# Color the violins
for pc, region in zip(parts['bodies'], regions):
    pc.set_facecolor(region_colors[region])
    pc.set_alpha(0.7)

# Style the other elements
for partname in ('cbars', 'cmins', 'cmaxes', 'cmedians', 'cmeans'):
    if partname in parts:
        parts[partname].set_edgecolor(dracula_fg)
        parts[partname].set_linewidth(1.5)

ax.set_yscale('log')
ax.set_ylabel('Duration (hours)', fontsize=12)
ax.set_xlabel('Region', fontsize=12)
ax.set_title('Distribution of High Price Event Durations by Region', fontsize=14, pad=20)
ax.set_xticks(range(len(regions)))
ax.set_xticklabels(regions)
ax.grid(True, axis='y', alpha=0.3)

plt.tight_layout()
plt.savefig('high_price_runs_violin.png', dpi=300, facecolor=dracula_bg)
plt.show()

# 3. BOX PLOT WITH SCATTER OVERLAY
fig, ax = plt.subplots(figsize=(10, 8))

# Create box plot
box_data = [runs_df[runs_df['region'] == r]['duration_hours'].values for r in regions]
bp = ax.boxplot(box_data, 
                positions=range(len(regions)),
                patch_artist=True,
                showfliers=False)  # We'll add points separately

# Color the boxes
for patch, region in zip(bp['boxes'], regions):
    patch.set_facecolor(region_colors[region])
    patch.set_alpha(0.5)

# Style whiskers, caps, and medians
for element in ['whiskers', 'caps', 'medians']:
    for item in bp[element]:
        item.set_color(dracula_fg)
        item.set_linewidth(1.5)

# Add scatter points on top
for i, region in enumerate(regions):
    region_data = runs_df[runs_df['region'] == region]
    y = region_data['duration_hours'].values
    x = np.random.normal(i, 0.04, size=len(y))  # Add jitter
    
    ax.scatter(x, y, 
              c=region_colors[region],
              s=30,
              alpha=0.6,
              edgecolors='white',
              linewidth=0.5)

ax.set_yscale('log')
ax.set_ylabel('Duration (hours)', fontsize=12)
ax.set_xlabel('Region', fontsize=12)
ax.set_title('High Price Event Durations by Region\nBox plot with individual events', 
             fontsize=14, pad=20)
ax.set_xticks(range(len(regions)))
ax.set_xticklabels(regions)
ax.grid(True, axis='y', alpha=0.3)

# Add count annotations
for i, region in enumerate(regions):
    count = len(runs_df[runs_df['region'] == region])
    ax.text(i, 0.3, f'n={count}', ha='center', va='top', fontsize=10, color=dracula_fg)

plt.tight_layout()
plt.savefig('high_price_runs_boxplot.png', dpi=300, facecolor=dracula_bg)
plt.show()

# 4. TEMPORAL HEATMAP (Bonus visualization)
# Shows when high price events occur by hour and month
fig, ax = plt.subplots(figsize=(12, 8))

# Extract hour and month
runs_df['hour'] = runs_df['start_time'].dt.hour
runs_df['month'] = runs_df['start_time'].dt.month

# Create pivot table for heatmap
heatmap_data = runs_df.pivot_table(
    values='duration_hours',
    index='hour',
    columns='month',
    aggfunc='count',
    fill_value=0
)

# Create heatmap
im = ax.imshow(heatmap_data, cmap='plasma', aspect='auto', interpolation='nearest')

# Set ticks
ax.set_xticks(range(12))
ax.set_xticklabels(['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 
                    'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'])
ax.set_yticks(range(24))
ax.set_yticklabels(range(24))

ax.set_xlabel('Month', fontsize=12)
ax.set_ylabel('Hour of Day', fontsize=12)
ax.set_title('Timing of High Price Events (All Regions)\nDarker = More Events', 
             fontsize=14, pad=20)

# Add colorbar
cbar = plt.colorbar(im, ax=ax)
cbar.set_label('Number of Events', rotation=270, labelpad=20)

plt.tight_layout()
plt.savefig('high_price_runs_heatmap.png', dpi=300, facecolor=dracula_bg)
plt.show()

print("\nPlots saved as:")
print("- high_price_runs_scatter.png")
print("- high_price_runs_violin.png") 
print("- high_price_runs_boxplot.png")
print("- high_price_runs_heatmap.png")