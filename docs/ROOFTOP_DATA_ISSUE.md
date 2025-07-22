# Rooftop Data Issue - Penetration Tab

*Date: July 19, 2025, 11:00 PM AEST*

## Issue Summary

The VRE Production chart in the Penetration tab is showing incorrect patterns because rooftop solar data is missing for most of 2025.

## Root Cause

The rooftop data file (`/Users/davidleitch/Library/Mobile Documents/com~apple~CloudDocs/snakeplay/AEMO_spot/aemo-data-updater/data 2/rooftop30.parquet`) only contains data from **June 18, 2025 onwards** for the year 2025.

### Data Availability by Year:
- 2020: 175,670 records (Full year)
- 2021: 175,200 records (Full year)
- 2022: 175,200 records (Full year) 
- 2023: 175,200 records (Full year)
- 2024: 102,730 records (Jan 1 - Aug 2)
- **2025: 15,030 records (June 18 - July 19 only)**

### Missing Data:
- January 1, 2025 - June 17, 2025: **NO ROOFTOP DATA**

## Impact

1. **Chart Accuracy**: The VRE production values for 2025 are significantly understated for the first half of the year
2. **Pattern Distortion**: The seasonal pattern is completely wrong because rooftop solar (which peaks in summer) is missing for the summer months
3. **Total VRE**: Without rooftop data, we're missing approximately 20-30 TWh of annualised renewable generation

## Current Behavior

- Wind data: Complete for all years
- Solar (utility-scale) data: Complete for all years  
- Rooftop data: Missing for Jan-June 2025

This explains why the chart shows:
- Lower overall VRE values for 2025
- Strange dips and patterns that don't match the reference
- Incorrect seasonal variations

## Next Steps

To fix this issue, we need to either:
1. Obtain the missing rooftop data for January-June 2025
2. Use a different data source that has complete rooftop data
3. Implement data interpolation/estimation for the missing period
4. Check if other dashboard tabs are handling this differently

## Note

User indicated that other tabs in the dashboard are showing rooftop data properly, suggesting there may be:
- Another rooftop data file with complete data
- A different data loading approach being used
- Data being sourced from a different location

*Investigation paused at user request*