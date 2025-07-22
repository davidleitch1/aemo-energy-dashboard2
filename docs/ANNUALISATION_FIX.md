# Annualisation Formula Fix

*Date: July 19, 2025, 10:30 PM AEST*

## Issue Identified

The VRE production chart was showing values that were too low because the annualisation formula was being applied incorrectly. The issue was with the order of operations:

**Incorrect approach:**
1. Calculate daily average MW
2. Annualise to TWh 
3. Apply EWM smoothing to TWh values

**Correct approach:**
1. Calculate daily average MW
2. Apply EWM smoothing to MW values
3. Annualise the smoothed MW values to TWh

## Formula

For EWM smoothing with span=30 days, the correct annualisation is:
```
Annualised TWh = Smoothed_MW × 24 hours × 365 days / 1,000,000
```

## Changes Made

1. Moved EWM smoothing to operate on MW values instead of TWh values
2. Applied annualisation after smoothing
3. Added resampling logic to handle mixed 5-minute (rooftop) and 30-minute (generation) data

## Expected Values

Based on 2024 data:
- Wind: ~28 TWh
- Solar: ~16 TWh  
- Rooftop: ~20-30 TWh
- **Total VRE: 64-74 TWh**

This matches the 50-90 TWh range shown in the screenshot.

## Code Changes

In `penetration_tab.py`:
```python
# Apply EWM smoothing to daily MW values FIRST
year_data['mw_smoothed'] = apply_ewm_smoothing(
    year_data['total_generation_mw'],
    span=30
)

# Then annualise the smoothed values
year_data['twh_annualised'] = year_data['mw_smoothed'] * 24 * 365 / 1_000_000
```

The chart should now display correctly with VRE production values in the expected range.