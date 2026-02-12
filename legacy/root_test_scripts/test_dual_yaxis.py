#!/usr/bin/env python3
"""
Minimal demonstration of secondary y-axis in HoloViews/hvPlot
Shows the correct approach: normalize data to primary scale, then add visual axis
"""

import pandas as pd
import hvplot.pandas
import holoviews as hv
from bokeh.models import LinearAxis, Range1d

hv.extension('bokeh')

# Sample data
hours = list(range(24))
volumes = [100 + 50 * (h % 12) for h in hours]  # MW data
prices = [50 + 30 * (h % 12) for h in hours]     # $/MWh data

df_volume = pd.DataFrame({'hour': hours, 'mw': volumes})
df_price = pd.DataFrame({'hour': hours, 'price': prices})

# Define ranges
price_min, price_max = min(prices) * 0.9, max(prices) * 1.1
mw_min, mw_max = min(volumes) * 0.9, max(volumes) * 1.1

# KEY STEP: Normalize price data to MW scale
def normalize_to_mw_scale(price):
    if price_max == price_min:
        return mw_min + (mw_max - mw_min) / 2
    return mw_min + ((price - price_min) / (price_max - price_min)) * (mw_max - mw_min)

df_price['price_normalized'] = df_price['price'].apply(normalize_to_mw_scale)

# Create plots
volume_line = df_volume.hvplot.line(
    x='hour', y='mw',
    label='Volume (MW)',
    color='blue',
    width=800, height=400,
    ylabel='MW'
)

price_scatter = df_price.hvplot.scatter(
    x='hour', y='price_normalized',  # Use normalized values
    label='Price ($/MWh)',
    color='red',
    size=100
)

# Hook to add secondary y-axis
def add_second_yaxis(plot, element):
    p = plot.state

    # Add extra y-range with ACTUAL price values
    p.extra_y_ranges = {"price": Range1d(start=price_min, end=price_max)}

    # Add visible axis on the right
    second_axis = LinearAxis(
        y_range_name="price",
        axis_label="Price ($/MWh)",
        axis_label_text_color="red"
    )
    p.add_layout(second_axis, 'right')

# Combine and apply hook
combined = (volume_line * price_scatter).opts(
    hooks=[add_second_yaxis],
    title='Dual Y-Axis Example (Normalized Approach)'
)

print("Solution Explanation:")
print("=" * 70)
print("1. Normalize price data to MW scale BEFORE plotting")
print("2. Create scatter plot with normalized values")
print("3. Use Bokeh hook to add secondary axis with ACTUAL price range")
print("4. The secondary axis displays correct price labels")
print("=" * 70)
print(f"\nOriginal price range: ${price_min:.0f} - ${price_max:.0f}")
print(f"MW range: {mw_min:.0f} - {mw_max:.0f}")
print(f"Normalized price maps to MW coordinates")
