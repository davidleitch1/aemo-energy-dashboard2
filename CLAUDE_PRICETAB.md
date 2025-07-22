# Prices Tab Implementation - AEMO Energy Dashboard

## Summary (July 22, 2025)

Successfully added a new "Prices" tab to the AEMO Energy Dashboard with full hvplot price visualization functionality. The tab has been positioned between "Generation mix" and "Pivot table" in the tab order. The implementation includes comprehensive controls, an "Analyze Prices" button to trigger data loading, and an interactive price chart with log scale support for handling negative prices.

## What Was Accomplished

### 1. Added New Tab to Dashboard Structure ✅
- Modified `gen_dash.py` to include a new "Prices" tab at index 2
- Updated all tab indices to accommodate the new tab:
  - Generation mix: index 1 (unchanged)
  - **Prices: index 2 (NEW)**
  - Pivot table: index 3 (shifted from 2)
  - Station Analysis: index 4 (shifted from 3) 
  - Penetration: index 5 (shifted from 4)
- Implemented lazy loading for the Prices tab

### 2. Created Control Layout ✅
Implemented a horizontal layout with the following columns:

#### Region Selection (Column 1)
- **Widget**: CheckBoxGroup (vertical)
- **Options**: NSW1, QLD1, SA1, TAS1, VIC1
- **Default**: NSW1 and VIC1 selected
- **Width**: 150px

#### Frequency Selection (Column 2)
- **Widget**: RadioBoxGroup (vertical)
- **Options**: 5 min, 1 hour, Daily, Monthly, Quarterly, Yearly
- **Default**: 1 hour
- **Width**: 120px

#### Date Quick Select (Column 3)
- **Widget**: RadioBoxGroup (vertical)
- **Options**: 1 day, 7 days, 30 days, 90 days, 1 year, All data
- **Default**: 30 days
- **Width**: 100px

#### Date Range Pickers (Column 4)
- **Widgets**: Two DatePicker widgets (Start Date, End Date)
- **Range**: 5 years of historical data available
- **Default**: Last 30 days
- **Width**: 180px total
- Includes date display showing selected period

#### Smoothing Options & Actions (Column 5)
- **Widget**: Select dropdown + Checkbox + Button
- **Smoothing Options**: None, 7-period MA, 30-period MA, Exponential (α=0.3)
- **Default**: None
- **Width**: 200px
- **Log Scale**: Checkbox for Y-axis log scaling (handles negative prices)
- **Analyze Button**: Primary button to trigger data loading and visualization

### 3. Implemented Callbacks ✅
- Date preset buttons update the date pickers automatically
- Date pickers update the date display text
- **Analyze button** triggers data loading and chart creation
- Initial state shows instruction message: "Click 'Analyze Prices' to load data"
- Loading feedback shown while data is being fetched
- No automatic data loading on parameter changes - user must click button

### 4. Created Interactive Price Chart ✅
**Main hvplot visualization features:**
- Multi-region line chart with Dracula theme colors
- Automatic resampling based on frequency selection (5min to yearly)
- Smoothing options (7-period MA, 30-period MA, Exponential)
- Log scale support with automatic shifting for negative prices
- Hover tooltips showing region and price information
- Interactive pan, zoom, and save tools

**Dracula Theme Colors:**
- NSW1: #8be9fd (Cyan)
- QLD1: #50fa7b (Green)
- SA1: #ffb86c (Orange)
- TAS1: #ff79c6 (Pink)
- VIC1: #bd93f9 (Purple)
- Background: #282a36 (Dark)

### 5. Negative Price Handling for Log Scale ✅
When log scale is enabled and negative prices exist:
- Automatically shifts all prices by |min_price| + 1
- Updates Y-axis label to show shift amount
- Preserves original price values in hover tooltips
- Ensures all values are positive for log transformation

### 6. Placeholder Areas for Future Development
1. **Statistics Table** - Average price and volatility metrics (pending)
2. **Price Band Contribution Chart** - Stacked column chart showing price bands (pending)

## Technical Implementation

### Files Modified
1. **src/aemo_dashboard/generation/gen_dash.py**
   - Added `_create_prices_tab()` method (lines 2481-2836)
   - Implemented `load_and_plot_prices()` function for data loading
   - Added price plot pane with HoloViews integration
   - Updated tab creation in `create_dashboard_layout()`
   - Modified `_tab_creators` dictionary to include new tab
   - Fixed deprecated pandas frequency aliases (5T→5min, H→h)
   - Fixed datetime type conversion for date pickers (date → datetime)

### Key Design Decisions
1. **Consistent UI**: Used RadioBoxGroup widgets to match Generation tab style
2. **Space Efficient**: Horizontal layout maximizes screen real estate
3. **Date Flexibility**: Combined quick presets with manual date pickers
4. **Multi-Region**: Checkbox group allows comparing multiple regions simultaneously
5. **User-Triggered Loading**: Data only loads when "Analyze Prices" button is clicked, avoiding automatic loading of large datasets
6. **Column Name Handling**: Fixed SETTLEMENTDATE column appearing as index in loaded data
7. **Date Type Handling**: Convert date picker values (date objects) to datetime objects for compatibility with price adapter

## Still To Do

### Visualizations
- [x] ~~Create hvplot time series chart for prices~~ ✅ Completed
- [x] ~~Implement price data loading from DuckDB~~ ✅ Using price_adapter
- [x] ~~Add resampling logic for different frequencies~~ ✅ Implemented
- [x] ~~Implement smoothing algorithms~~ ✅ MA and EWM implemented
- [ ] Build statistics table with average and volatility
- [ ] Implement stacked column chart for price bands

### Price Band Analysis
Price bands to implement:
- Below $0
- $0 - $50
- $51 - $100
- $101 - $300
- $301 - $1000
- Above $1000

### Integration
- [ ] Connect controls to data loading
- [ ] Add loading indicators
- [ ] Implement error handling
- [ ] Add data export functionality

## Testing

A test script has been created at `test_prices_tab_layout.py` to verify the layout and control interactions independently of the main dashboard.

## Next Steps

The foundation is complete with all controls in place. The next phase is to:
1. Implement the data loading logic using the hybrid query manager
2. Create the three visualization components
3. Connect all controls to update the visualizations reactively
4. Add proper error handling and loading states