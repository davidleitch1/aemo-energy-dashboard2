# Penetration Tab Data Fix

*Date: July 19, 2025, 10:15 PM AEST*

## Issue Identified

The VRE production chart was displaying with correct axes and styling but no data lines were visible. Investigation revealed that:

1. The `GenerationQueryManager` only returns Wind and Solar data from the generation parquet files
2. Rooftop solar data is stored separately in a different parquet file
3. The penetration tab was not loading rooftop data, so VRE totals were incomplete

## Solution Implemented

### 1. Added Rooftop Data Loading
- Imported `load_rooftop_data` from the rooftop adapter
- Modified `_get_generation_data()` to also load rooftop data for each year
- Handled the different data format (wide format with regions as columns)

### 2. Data Format Conversion
- Rooftop data comes in wide format: `[settlementdate, NSW1, QLD1, SA1, ...]`
- Converted to long format matching generation data: `[settlementdate, fuel_type, total_generation_mw]`
- For NEM selection: sum across all regions
- For specific region: extract just that region's column

### 3. Files Modified
- `src/aemo_dashboard/penetration/penetration_tab.py`
  - Added rooftop data loading in `_get_generation_data()` method
  - Handled wide-to-long format conversion
  - Added proper error handling for missing regions

## Test Results

Before fix:
- Only Wind and Solar data loaded
- No Rooftop data
- Chart showed empty lines

After fix:
- Wind: 17,568 records (96.66 - 8280.08 MW)
- Solar: 17,568 records (-0.38 - 6990.71 MW)  
- Rooftop: 61,638 records (0.00 - 18649.12 MW)
- Chart now displays properly with all VRE components

## Next Steps

The dashboard should now display the VRE production chart correctly with:
- All three renewable sources (Wind + Solar + Rooftop) combined
- Proper EWM smoothing applied
- Year-over-year comparison for 2023, 2024, and 2025

To run:
```bash
cd /Users/davidleitch/Library/Mobile Documents/com~apple~CloudDocs/snakeplay/AEMO_spot/aemo-energy-dashboard
.venv/bin/python run_dashboard_duckdb.py
```

Navigate to the "Penetration" tab to see the working chart.