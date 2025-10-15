# Dimension Name Solution for X-Axis Interference

## The Problem
HoloViews automatically links axes with the same dimension names across the entire application, causing cross-tab interference.

## The Solution
Use unique dimension names for each tab's plots:

### Batteries Tab
- All plots use dimension name: `'BatteryTime'`
- Power plot: `.redim(x='BatteryTime')`
- Price plot: `.redim(x='BatteryTime')`
- Result: These plots link with each other but not with other tabs

### Prices Tab
- Uses default dimension names (no redim)
- Default x dimension is typically the datetime index name
- Result: Won't link with Batteries tab because dimension names differ

## Implementation
```python
# In Batteries tab:
discharge_plot = power_df['Discharge'].hvplot.step(...).redim(x='BatteryTime')
charge_plot = power_df['Charge'].hvplot.step(...).redim(x='BatteryTime')
price_line = plot_data['RRP'].hvplot.line(...).redim(x='BatteryTime')

# Then combine with shared_axes=True:
combined_plot = (power_plot + price_plot).cols(1).opts(
    hv.opts.Layout(shared_axes=True)  # Links only plots with same dimension name
)
```

## How It Works
- HoloViews only links axes with matching dimension names
- `'BatteryTime'` dimension only exists in Batteries tab
- Prices tab uses default dimensions
- No dimension name overlap = no cross-tab interference
- Within each tab, plots with same dimension names still link properly

## Testing
1. Go to Batteries tab
2. Set date range (e.g., 2025-08-25 to 2025-09-01)
3. Verify power and price plots zoom together
4. Switch to Prices tab and interact
5. Return to Batteries tab - range should be preserved