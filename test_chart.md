# Generation Stack Chart Test Documentation

## Objective
Create a standalone test of the generation stack plot using real South Australian electricity market data to verify the chart rendering and stacking behavior.

## Data Preparation
- **Source**: Production data from `/Volumes/davidleitch/aemo_production/data/`
- **Region**: South Australia (SA1)
- **Time Period**: Last 7 days (August 13-20, 2025)
- **Resolution**: 30-minute intervals
- **Output File**: `sa_generation_price_test_data.csv`

## Data Characteristics

### Columns
1. `settlementdate` - Timestamp of the 30-minute period
2. `region` - Always 'SA1' for South Australia
3. `Battery Storage` - Battery discharge in MW
4. `CCGT` - Combined Cycle Gas Turbine generation in MW
5. `Gas other` - Other gas generation in MW
6. `OCGT` - Open Cycle Gas Turbine generation in MW
7. `Other` - Other generation sources in MW
8. `Solar` - Solar generation in MW
9. `Wind` - Wind generation in MW
10. `rrp` - Regional Reference Price in $/MWh

### Key Findings
- **Battery Storage**: Only positive values (0 to 411 MW)
  - Shows discharge (generation) only, no charging (negative values)
  - Active in 314 out of 333 periods (94.3%)
  - Zero in 19 periods (5.7%)
  - **IMPORTANT DATA LIMITATION**: AEMO SCADA data only reports generation (discharge), not consumption (charging). This is confirmed in both 5-minute and 30-minute SCADA files. Battery charging data would need to come from a different AEMO report (e.g., DISPATCHIS or market participant data)
  
- **Transmission**: No transmission flow data in this extract

- **All Fuel Types**: Only positive or zero values (no negative generation)

### Data Summary
- **Total Records**: 333 (7 days × 48 periods/day - some missing)
- **File Size**: 32.7 KB
- **Total Generation by Fuel**:
  - Wind: 161,532 MWh (largest)
  - CCGT: 41,556 MWh
  - Gas other: 32,892 MWh
  - Solar: 21,342 MWh
  - OCGT: 16,766 MWh
  - Battery Storage: 6,220 MWh
  - Other: 107 MWh (smallest)

- **Price Statistics**:
  - Mean: $110.60/MWh
  - Min: -$102.00/MWh (negative pricing event)
  - Max: $583.30/MWh

## Test Plan

### 1. Basic Stacked Area Chart
Test the standard generation stack with all positive values:
- Verify proper stacking order
- Check color mapping for each fuel type
- Confirm legend displays correctly
- Test hover tooltips

### 2. Price Overlay
Add price as a secondary y-axis line:
- Verify dual y-axis scaling
- Check axis labels and formatting
- Test interaction between area stack and line

### 3. Handling Zero Values
Verify chart behavior with:
- Battery Storage zero periods
- "Other" fuel type with minimal generation

### 4. Missing Data Handling
Check behavior with:
- NaN values in price column (several periods have missing prices)
- Gaps in time series if any

### 5. Future Enhancements
For complete testing, would need to:
- Add synthetic negative battery values to test charging display
- Add transmission flow data (both import/export)
- Test with larger datasets for performance

## Implementation Notes
- Use hvplot.area() for stacked area chart
- Apply AEMO dashboard color scheme
- Include attribution: "Design: ITK, Data: AEMO"
- Target dimensions: 1000px width × 500px height
- Dark theme (Dracula color scheme)

## Next Steps
1. Create `test_generation_stack.py` script
2. Implement basic stacked area chart
3. Add price overlay with secondary y-axis
4. Test interactivity features
5. Document any issues or limitations found