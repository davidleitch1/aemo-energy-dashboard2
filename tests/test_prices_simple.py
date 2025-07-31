#!/usr/bin/env python3
"""Simple test to verify price plot functionality"""

import panel as pn
import pandas as pd
import holoviews as hv
import hvplot.pandas  # Required for df.hvplot
from datetime import datetime, timedelta
import numpy as np
import os

# Ensure we don't try to open a browser
os.environ['BOKEH_BROWSER'] = 'none'

# Initialize Panel and HoloViews
pn.extension()
hv.extension('bokeh')

# Create simple test data
end_date = datetime.now()
start_date = end_date - timedelta(days=7)
date_range = pd.date_range(start=start_date, end=end_date, freq='h')

# Create price data for two regions
data = []
for region in ['NSW1', 'VIC1']:
    base = 80 if region == 'NSW1' else 75
    prices = base + 20 * np.sin(np.arange(len(date_range)) * 2 * np.pi / 24)
    prices += np.random.normal(0, 10, len(date_range))
    
    # Add a few negative prices
    neg_idx = np.random.choice(len(prices), 3)
    prices[neg_idx] = -20
    
    for date, price in zip(date_range, prices):
        data.append({
            'SETTLEMENTDATE': date,
            'REGIONID': region,
            'RRP': price
        })

df = pd.DataFrame(data)

# Test log scale handling with negative values
min_price = df['RRP'].min()
if min_price <= 0:
    shift_value = abs(min_price) + 1
    df['RRP_adjusted'] = df['RRP'] + shift_value
    ylabel = f'Price ($/MWh) + {shift_value:.0f} [Log Scale]'
    y_col = 'RRP_adjusted'
else:
    y_col = 'RRP'
    ylabel = 'Price ($/MWh) [Log Scale]'

# Create plot with Dracula theme colors
region_colors = {
    'NSW1': '#8be9fd',  # Cyan
    'VIC1': '#bd93f9'   # Purple
}

plot = df.hvplot.line(
    x='SETTLEMENTDATE',
    y=y_col,
    by='REGIONID',
    width=1200,
    height=400,
    xlabel='Time',
    ylabel=ylabel,
    title='Electricity Spot Prices by Region (Log Scale Test)',
    logy=True,
    grid=True,
    color=[region_colors.get(r, '#6272a4') for r in df['REGIONID'].unique()],
    line_width=2,
    hover=True,
    hover_cols=['REGIONID', 'RRP'],
    fontsize={'title': 14, 'labels': 12, 'ticks': 10}
).opts(
    bgcolor='#282a36',
    toolbar='above',
    active_tools=['pan', 'wheel_zoom']
)

# Save to HTML
pn.pane.HoloViews(plot).save('test_prices_log_scale.html')
print("Test completed successfully!")
print(f"Min price: {df['RRP'].min():.2f}")
print(f"Shift value applied: {shift_value:.0f}")
print("HTML saved to: test_prices_log_scale.html")