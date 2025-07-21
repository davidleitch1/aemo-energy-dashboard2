# AEMO Energy Dashboard Documentation

## Overview

The AEMO Energy Dashboard is a web-based visualization platform for Australian electricity market data. It provides real-time and historical analysis of generation, prices, and transmission flows across the National Electricity Market (NEM).

## Dashboard Architecture

### Technology Stack
- **Framework**: Panel with HoloViews/hvPlot for interactive visualizations
- **Template**: Material design theme
- **Data Source**: Parquet files maintained by the AEMO Data Updater
- **Update Model**: Read-only access to parquet files (no downloads)

### Main Components

#### Generation by Fuel Tab
- **Purpose**: Shows electricity generation by fuel type in stacked area charts
- **Features**:
  - Region selector (NEM, NSW1, QLD1, SA1, TAS1, VIC1)
  - Time range selector (24 hours, 7 days, 30 days, All Data, Custom)
  - Subtabs: Generation Stack, Capacity Utilization
  - Integrated transmission flows as virtual "fuel" type
  - Rooftop solar displayed as separate band
- **Data**: 5-minute resolution from AEMO SCADA

#### Average Price Analysis Tab
- **Purpose**: Analyze prices and revenue by region and fuel type
- **Features**:
  - User-driven grouping (Region, Fuel combinations)
  - Multi-select filters for regions and fuels
  - Column selection (Gen GWh, Rev $M, Price $/MWh, etc.)
  - Hierarchical table with expandable fuel groups
  - Date range controls
- **Visualization**: Interactive Tabulator table

#### Station Analysis Tab
- **Purpose**: Detailed analysis of individual power stations or units
- **Features**:
  - Station vs DUID toggle
  - Fuzzy search with popular stations dropdown
  - Dual-axis charts (Generation MW + Price $/MWh)
  - Time series and time-of-day analysis
  - Performance metrics (revenue, capacity factor, etc.)
- **Smart Features**: Automatic DUID grouping for multi-unit stations

#### Nem-dash Tab (NEW - Primary Overview)
- **Purpose**: Main dashboard view combining key metrics and visualizations
- **Status**: ✅ **IMPLEMENTED** (positioned as first tab)
- **Layout**:
  ```
  ┌─────────────────────────┐  ┌──────────────────────┐
  │  Generation Chart       │  │   Price Section      │
  │  (800x400px)           │  │  - Price Table       │
  │  24-hour stacked area   │  │  - Price Chart       │
  │  by fuel type          │  │  (450px width)       │
  └─────────────────────────┘  └──────────────────────┘
  ┌─────────────────────────┐
  │  Renewable Energy Gauge │
  │  (400x350px)           │
  │  Real-time % with      │
  │  reference markers     │
  └─────────────────────────┘
  ```
- **Features**:
  - ✅ **Generation Chart**: 24-hour stacked area showing NEM generation by fuel type
  - ✅ **Price Section**: Real-time 5-minute spot prices table + smoothed price chart
  - ✅ **Renewable Gauge**: Plotly gauge showing current renewable energy percentage
    - Real-time calculation: (Wind + Solar + Water + Rooftop Solar) / Total Generation*
    - *Total Generation excludes Battery Storage and Transmission Flow
    - Reference markers: 👑 All-time record, 🕐 Hour record
    - Pink Dracula theme (#ff79c6) with semi-transparent axis labels
    - Whole number display (e.g., "36%" not "35.8%")
    - Legend positioned below gauge to avoid overlap
  - ✅ **Auto-update**: 4.5-minute refresh cycle
  - ✅ **Data Integration**: Uses dashboard's processed data for consistency

### **Recent Implementation Progress (2025-07-15)**

#### ✅ **Completed Features**:
1. **Tab Integration**: Nem-dash tab added as first tab in main dashboard
2. **Data Access**: Fixed to use dashboard's `process_data_for_region()` method
3. **Layout Implementation**: New 2-row layout with generation chart + price section on top, gauge below
4. **Gauge Functionality**: 
   - Real renewable percentage calculation (showing ~32%)
   - Pink Dracula theming for all text elements
   - Reference markers with crown/clock emojis
   - Whole number formatting
5. **Generation Chart**: 24-hour view using dashboard's processed data
6. **Price Components**: Adapted from aemo-spot-dashboard with ITK styling
7. **Error Handling**: Matplotlib backend fixes for threading compatibility

#### ✅ **Latest Fixes (2025-07-15)**:
1. **Chart Container**: ✅ Removed blue outline border from generation chart frame using custom CSS
2. **Gauge Legend**: ✅ Repositioned reference markers legend to center-bottom area with pink Dracula theming
3. **UFuncTypeError**: ✅ Fixed datetime/float comparison errors when switching tabs by disabling axis linking
4. **Rooftop Solar Interpolation**: ✅ Implemented comprehensive fix for flat-lining issues:
   - Dashboard now detects poor interpolation patterns in 5-minute data
   - Applies cubic smoothing to fix flat segments (5+ identical values)
   - Handles both 30-minute and 5-minute input data automatically
   - Fixes sharp drop-offs at end of day with smooth transitions

#### ✅ **Temporary Fixes Applied**:
1. **Rooftop Solar Data Gaps**: When rooftop data is less recent than generation data (common at end of day)
   - Forward-fills last known values (up to 2 hours) with exponential decay (2% per 5-min)
   - Prevents sharp drop to zero when rooftop data ends before generation data
   - **Note**: This is a temporary fix - proper solution is to store 30-minute data and convert in dashboard

#### 🎯 **Remaining Enhancements**:
- Fix rooftop solar data downloads to store 30-minute data
- Implement proper 30-min to 5-min conversion in dashboard
- Auto-update functionality refinements
- Performance optimization for large datasets  
- Further layout refinements based on user feedback

## Data Quality Issues

### 🔧 **Rooftop Solar Conversion Issue (30-min to 5-min)** 🔧 TEMPORARY FIX

#### **Temporary Solution Implemented (2025-07-15)**

The dashboard now includes a comprehensive fix that handles rooftop solar data regardless of format:

1. **Automatic Detection**: 
   - Detects if data is in 30-minute intervals → converts using cubic spline
   - Detects if data has flat-lining issues → applies smoothing fix
   - Works with existing 5-minute data that was poorly interpolated

2. **Flat-lining Fix Algorithm**:
   ```python
   # Detects sequences of 5+ identical values
   # Applies cubic interpolation between segments
   # Creates smooth transitions instead of sharp drops
   ```

3. **Data Gap Handling** (NEW):
   - Detects when rooftop data ends before generation data
   - Forward-fills missing periods with exponential decay (2% per 5-min)
   - Prevents sharp drops to zero at end of day

4. **Benefits**:
   - No more sharp drop-offs at end of day
   - Smooth, natural solar generation curves
   - Works with data from any updater
   - No need to reprocess existing data files
   
**Note**: This is a temporary solution. The proper fix is to:
- Store original 30-minute data in parquet files
- Always perform conversion to 5-minute in the dashboard
- This ensures consistency and allows algorithm improvements

#### **Original Implementation Analysis**

The rooftop solar data conversion from 30-minute to 5-minute intervals currently uses a **linear interpolation with weighted averaging** approach:

```python
# Current Algorithm: Weighted Linear Interpolation
for j in range(6):  # Create 6 five-minute intervals per 30-minute period
    if next_value_exists:
        value = ((6 - j) * current_value + j * next_value) / 6
    else:
        value = current_value  # Repeat last value for all 6 intervals
```

**Interpolation Pattern:**
- **j=0** (0 min): `100% current + 0% next`
- **j=1** (5 min): `83% current + 17% next`  
- **j=2** (10 min): `67% current + 33% next`
- **j=3** (15 min): `50% current + 50% next`
- **j=4** (20 min): `33% current + 67% next`
- **j=5** (25 min): `17% current + 83% next`

#### **Problems with Current Method**

1. **End-Point Flat-Lining Issue**: 
   - After the last available 30-minute data point, the algorithm simply repeats the same value for all 6 five-minute intervals
   - This creates artificial flat-line periods that don't reflect natural solar generation patterns
   - Real solar output has smooth transitions, not sudden plateaus

2. **Oversimplified Linear Interpolation**:
   - Solar irradiance follows **natural curves** (not straight lines) due to:
     - Cloud movements and shadows
     - Atmospheric conditions
     - Sun angle changes
   - Linear interpolation can create unrealistic sharp transitions between data points

3. **Missing Forward-Looking Intelligence**:
   - No consideration of time-of-day patterns
   - No seasonal or weather-based adjustments
   - No trend analysis for better end-point estimation

4. **Data Discontinuities**:
   - When transitioning between different 30-minute periods, linear interpolation can create visible discontinuities in charts
   - These artifacts are particularly noticeable during rapid weather changes

#### **Research-Based Improvement Recommendations**

Based on analysis of time series interpolation methods for solar generation data, several superior approaches are available:

##### **1. Cubic Spline Interpolation**
```python
# Recommended: Use pandas cubic interpolation
df_5min = df_30min.resample('5min').interpolate(method='cubic')
```
**Advantages:**
- Better captures natural solar irradiance curves
- Smoother transitions between data points
- Maintains mathematical properties (continuity, differentiability)

##### **2. LOESS (Locally Weighted Regression) Smoothing**
```python
# For noisy data with seasonal patterns
from statsmodels.tsa.seasonal import STL
stl = STL(solar_data, seasonal=7)  # Weekly seasonality
result = stl.fit()
```
**Advantages:**
- Excellent for handling outliers in solar data
- Captures seasonal patterns and trends
- Robust against measurement noise

##### **3. Forward-Looking End-Point Handling**
Instead of flat-lining at the end, implement intelligent forecasting:

```python
# Option A: Use trend analysis
last_trend = calculate_recent_trend(last_few_periods)
forecast_value = last_value + (trend * time_ahead)

# Option B: Use seasonal patterns
hour_of_day = current_time.hour
seasonal_multiplier = get_seasonal_pattern(hour_of_day, day_of_year)
forecast_value = last_value * seasonal_multiplier

# Option C: Use moving average with decay
forecast_value = exponential_weighted_average(recent_values, decay_factor=0.1)
```

##### **4. Hybrid Approach (Recommended)**
1. **Use cubic spline** for interpolation between known data points
2. **Apply LOESS smoothing** to reduce noise and handle outliers
3. **Implement intelligent end-point forecasting** using:
   - Recent trend analysis
   - Time-of-day solar patterns
   - Exponential decay for missing periods

#### **Implementation Priority**

**High Priority:**
- Fix end-point flat-lining issue (affects real-time dashboard accuracy)
- Implement cubic spline interpolation for smoother curves

**Medium Priority:**
- Add LOESS smoothing for noise reduction
- Implement seasonal pattern-based forecasting

**Low Priority:**
- Advanced machine learning forecasting models
- Integration with weather forecast data

#### **Impact on Dashboard**

This improvement would significantly enhance:
- **Nem-dash renewable energy gauge accuracy** (currently affected by poor end-point handling)
- **Generation by Fuel tab smoothness** (visible interpolation artifacts in rooftop solar band)
- **Real-time data quality** (better handling of the most recent data periods)

The current method works adequately for historical analysis but creates noticeable quality issues in real-time visualization, particularly in the last 30 minutes of data where flat-lining occurs.

## Running the Dashboard

### Start Dashboard
```bash
cd /Users/davidleitch/Library/Mobile Documents/com~apple~CloudDocs/snakeplay/AEMO_spot/aemo-energy-dashboard
source .venv/bin/activate
python -m src.aemo_dashboard.generation.gen_dash
```

### Access Dashboard
- URL: http://localhost:5010
- Title: "Nem Analysis"
- Footer: "Last Updated: [time] | data:AEMO, design ITK"

### Access Updater Status UI
- URL: http://localhost:5011
- Title: "AEMO Data Updater Status"
- Shows: Update status for all collectors (Generation, Price, Transmission, Rooftop)

### Stop Dashboard
```bash
# Find process
lsof -ti:5010

# Kill process
lsof -ti:5010 | xargs kill -9
```

## Configuration

### Environment Variables (.env)
```bash
# Data file locations (read-only access)
GEN_OUTPUT_FILE=/path/to/gen_output.parquet
SPOT_HIST_FILE=/path/to/spot_hist.parquet
TRANSMISSION_OUTPUT_FILE=/path/to/transmission_flows.parquet
ROOFTOP_SOLAR_FILE=/path/to/rooftop_solar.parquet

# Email alerts (dashboard-specific notifications)
ALERT_EMAIL=your-email@icloud.com
ALERT_PASSWORD=your-app-specific-password
RECIPIENT_EMAIL=recipient@example.com

# Dashboard settings
DEFAULT_REGION=NEM
UPDATE_INTERVAL_MINUTES=5
```

## Dashboard Features

### Time Range Selection
- **Quick Select Buttons**: Last 24 Hours, Last 7 Days, Last 30 Days, All Data
- **Custom Date Pickers**: For precise date range selection
- **Smart Display**: Context-aware x-axis labels (hours for day view, dates for week view)
- **Performance**: All data stays at 5-minute resolution (no resampling)

### Regional Analysis
- **NEM View**: Combined data for entire National Electricity Market
- **State Views**: Individual analysis for NSW, QLD, SA, TAS, VIC
- **Transmission Integration**: Shows interconnector flows as imports/exports

### Data Integration
- **Generation + Prices**: Automatic joining for revenue calculations
- **Capacity Factors**: Real-time utilization percentages
- **Station Grouping**: Smart DUID pattern matching (e.g., ER01-04 → Eraring)

## UI Components

### Color Scheme
```python
fuel_colors = {
    'Solar': '#FFD700',           # Gold
    'Rooftop Solar': '#FFF59D',   # Light yellow
    'Wind': '#87CEEB',            # Sky blue
    'Water': '#4682B4',           # Steel blue
    'Battery Storage': '#9370DB',  # Medium purple
    'Coal': '#8B4513',            # Saddle brown
    'Gas other': '#FF7F50',       # Coral
    'OCGT': '#FF6347',            # Tomato
    'CCGT': '#FF4500',            # Orange red
    'Biomass': '#228B22',         # Forest green
    'Other': '#A9A9A9',           # Dark gray
    'Transmission Flow': '#FFB6C1' # Light pink
}
```

### Chart Types
- **Stacked Area**: Generation by fuel type
- **Line Charts**: Capacity utilization, transmission flows
- **Dual-Axis**: Combined generation and price analysis
- **Tables**: Tabulator with hierarchical grouping

## Performance Optimization

### Data Loading
- **Caching**: 5-minute TTL for loaded data
- **Lazy Loading**: Only loads data when tab is viewed
- **Efficient Queries**: Filters data before visualization

### Chart Rendering
- **Smart Decimation**: Reduces points for large date ranges
- **Responsive Design**: Adjusts to browser window size
- **Interactive Tools**: Zoom, pan, hover tooltips

## Troubleshooting

### Common Issues

#### Port Already in Use
```bash
lsof -ti:5010 | xargs kill -9
```

#### Missing Data
- Check parquet file paths in .env
- Verify updater service is running
- Run data integrity check

#### Slow Performance
- Check data file sizes
- Reduce time range selection
- Clear browser cache

### Log Files
```bash
# View dashboard logs
tail -f logs/aemo_dashboard.log

# Check for errors
grep ERROR logs/aemo_dashboard.log
```

## Development Notes

### Code Organization
```
src/aemo_dashboard/
├── generation/
│   └── gen_dash.py          # Main dashboard file
├── analysis/
│   └── price_analysis_ui.py # Price analysis tab
├── station/
│   └── station_analysis_ui.py # Station analysis tab
├── shared/
│   ├── config.py            # Configuration
│   ├── logging_config.py    # Logging setup
│   └── email_alerts.py      # Alert system
└── diagnostics/
    └── data_validity_check.py # Data integrity checks
```

### Adding New Features

1. **New Tab**: Create UI module in appropriate directory
2. **Import in gen_dash.py**: Add to tab creation
3. **Data Access**: Use read-only parquet operations
4. **Styling**: Follow Material theme guidelines

### Testing
```bash
# Run dashboard locally
python -m src.aemo_dashboard.generation.gen_dash

# Check with playwright
playwright test dashboard_functionality.spec.js
```

## Immediate UI Improvements

### Time Range Selection Consistency
**Problem**: Time range selectors are inconsistent across tabs and take up too much space
**Current Issues**:
- Generation by Fuel tab: Has "Time Range Options" label (unnecessary)
- Station Analysis tab: Uses radio buttons for presets
- No "1 Day" option (users frequently need 24-hour view)
- Duplicated controls across all tabs

**Proposed Solution**:
1. Create a unified time range component using compact radio buttons:
   ```python
   time_options = ["1 Day", "7 Days", "30 Days", "All Data", "Custom"]
   ```
2. Remove redundant labels like "Time Range Options"
3. Use consistent styling across all tabs
4. Consider using a segmented control (like iOS/Material) for better space efficiency

**Specific Changes Needed**:
1. **Generation by Fuel tab** (`gen_dash.py`):
   - Change "Last 24 Hours" to "1 Day" throughout
   - Remove the "**Time Range Options:**" markdown section (lines 1921-1926)
   - Remove "**Time Range:**" label (line 1886)
   - Keep the RadioButtonGroup but make it more compact

2. **Station Analysis tab**:
   - Already uses RadioButtonGroup (good!)
   - Add "1 Day" option to match other tabs
   - Ensure consistent button styling

3. **Average Price Analysis tab**:
   - Check current implementation and align with others
   - Add "1 Day" option if missing

**Implementation Priority**: High - affects all tabs

### Space Optimization Tasks
1. **Remove redundant text labels**:
   - "Time Range Options" on Generation by Fuel tab
   - "Select Station/Unit" (the dropdown makes it obvious)
   - Any other descriptive text that duplicates UI function

2. **Consolidate controls**:
   - Group related controls more tightly
   - Use consistent spacing (8px Material grid)
   - Reduce vertical spacing between elements

3. **Button improvements**:
   - Use button groups instead of individual buttons where possible
   - Implement toggle behavior for mutually exclusive options
   - Add visual feedback for active states

## Planned Features & Tasks

### New Nem-dash Tab (PRIMARY TAB)
**Objective**: Create the primary dashboard tab that users see when opening the application. Provides an at-a-glance view of key NEM metrics with immediate access to current prices, renewable energy status, and generation overview.

**Tab Priority**: **FIRST TAB** - This will be the default tab that loads when users access the dashboard.

**Layout Design**:
```
┌─────────────────────────────────────────────────────────────────┐
│                           Nem-dash Tab                         │
├─────────────────────────────┬───────────────────────────────────┤
│  PRICE SECTION (Top-Left)   │  RENEWABLE GAUGE (Top-Right)     │
│  ┌─────────────────────────┐ │  ┌─────────────────────────────┐ │
│  │ Price Table (Recent 5)  │ │  │                             │ │
│  │ • Current spot prices   │ │  │    🌱 Renewable Energy     │ │
│  │ • 1hr & 24hr averages   │ │  │         Gauge               │ │
│  │ • All NEM regions       │ │  │                             │ │
│  └─────────────────────────┘ │  │  • Current: XX.X%           │ │
│  ┌─────────────────────────┐ │  │  • 👑 All-time: XX.X%      │ │
│  │ Price Chart (Smoothed)  │ │  │  • 🕐 Hour record: XX.X%   │ │
│  │ • Last 10 hours trend   │ │  │                             │ │
│  │ • EWM smoothed lines    │ │  └─────────────────────────────┘ │
│  │ • All regions           │ │  Width: ~400px, Height: ~350px   │
│  └─────────────────────────┘ │                                 │
│  Width: ~450px              │                                 │
├─────────────────────────────┴───────────────────────────────────┤
│              GENERATION OVERVIEW (Bottom - Full Width)         │
│  ┌─────────────────────────────────────────────────────────────┐ │
│  │              NEM 24-Hour Stacked Generation                 │ │
│  │                                                             │ │
│  │  • Stacked area chart by fuel type                         │ │
│  │  • Fixed 24-hour time range (last 24 hours)                │ │
│  │  • All fuel types including transmission & rooftop solar   │ │
│  │  • Height: ~400px for detailed view                        │ │
│  │  • Auto-updates every 4.5 minutes                          │ │
│  │                                                             │ │
│  └─────────────────────────────────────────────────────────────┘ │
│  Width: Full dashboard width (~1200px)                         │
└─────────────────────────────────────────────────────────────────┘
```

**Component Specifications**:

#### 1. **Price Section (Top-Left)**
**Source**: Adapted from `/Users/davidleitch/.../aemo-spot-dashboard/display_spot.py`
- **Price Table Component**:
  - Display last 5 price intervals (5-minute data)
  - Show all NEM regions (NSW1, QLD1, SA1, VIC1, TAS1)
  - Include calculated averages: Last hour, Last 24 hours
  - Use ITK dark theme styling with teal accents
  - Bold formatting for most recent entries
  - Compact format: ~450px width, ~200px height

- **Price Chart Component**:
  - Smoothed price trends using EWM (alpha=0.22)
  - Last 10 hours of data (120 points max)
  - Multi-region line chart with current values in legend
  - Dark "Dracula" theme consistent with existing dashboard
  - Size: ~450px width, ~250px height
  - Update frequency: Every 4.5 minutes

#### 2. **Renewable Energy Gauge (Top-Right)**
**Technology**: Plotly gauge with custom reference markers
- **Primary Metric**: (Wind + Solar + Hydro + Rooftop Solar) / Total Generation × 100
- **Visual Design**:
  - Gauge range: 0-100%
  - Color gradient: Red (0%) → Orange (20%) → Yellow (40%) → Light Green (60%) → Green (80-100%)
  - Semi-circular gauge layout
  - Size: ~400px width, ~350px height

- **Reference Markers** (on gauge rim):
  - **👑 All-time Record**: Gold marker with crown icon, pointing to highest ever renewable %
  - **🕐 Hour Record**: Silver marker with clock icon, pointing to best % for current hour-of-day
  - **Visual Implementation**: Small icons positioned on gauge exterior with arrows pointing to scale
  - **Legend**: Top-left corner showing current values for both records

- **Data Calculation**:
  - Real-time calculation from current generation data
  - Historical records stored and updated with each new data point
  - Graceful handling of missing data (show as "N/A")

#### 3. **NEM Generation Overview (Bottom - Full Width)**
**Source**: Existing generation stacked area chart from main dashboard
- **Time Range**: Fixed 24-hour view (last 24 hours)
- **Data Resolution**: 5-minute intervals (288 points)
- **Fuel Types**: All standard fuel types plus transmission flows and rooftop solar
- **Chart Features**:
  - Stacked area chart showing generation mix over time
  - Consistent color scheme with other dashboard components
  - X-axis: Time labels (hourly markers)
  - Y-axis: Generation in MW
  - Hover tooltips with detailed breakdown
  - Full dashboard width (~1200px), height ~400px for good visibility

- **Special Handling**:
  - Include rooftop solar as separate band
  - Show transmission imports/exports appropriately
  - Handle negative values (battery charging, exports) correctly

**Technical Implementation**:

#### Tab Structure
```python
nem_dash_tab = pn.Column(
    # Top row: Price section + Renewable gauge
    pn.Row(
        price_section,      # Column containing table + chart
        renewable_gauge,    # Plotly gauge with references
        sizing_mode='stretch_width'
    ),
    # Bottom row: Full-width generation chart
    generation_overview,
    sizing_mode='stretch_width',
    margin=(10, 10)
)
```

#### Data Integration
- **Price Data**: Read from shared `spot_hist.parquet` file
- **Generation Data**: Use existing dashboard data pipeline
- **Update Coordination**: All components refresh on 4.5-minute cycle
- **Error Handling**: Graceful degradation if any component fails to load

#### Performance Considerations
- **Data Caching**: Reuse loaded data across components where possible
- **Selective Updates**: Only recalculate renewable % when generation data changes
- **Lazy Rendering**: Defer expensive calculations until tab is viewed
- **Memory Management**: Limit historical data retention for records

**Navigation Priority**:
- **Tab Order**: Nem-dash, Generation by Fuel, Average Price Analysis, Station Analysis
- **Default Selection**: Nem-dash tab selected on dashboard startup
- **URL Routing**: Root URL (`/`) should load Nem-dash tab directly

**Success Criteria**:
1. Users immediately see current electricity market status
2. Price trends and renewable energy share visible at a glance
3. Quick access to detailed generation breakdown
4. All components update automatically without user intervention
5. Professional appearance suitable for public presentation
6. Responsive design works on different screen sizes (minimum 1200px width)

### Auto-Update Functionality
**Requirement**: The dashboard MUST automatically update with new data as it becomes available.

**Current Implementation**:
- Dashboard has auto-update loop that runs every 4.5 minutes
- Updates are triggered in `auto_update_loop()` method
- Currently failing due to UFuncTypeError (see Known Issues)

**Expected Behavior**:
- Dashboard should refresh all charts/tables automatically
- No manual refresh should be required
- Update interval should match data collection frequency (5 minutes)
- Updates should be seamless without visible flicker
- Failed updates should not crash the dashboard

**Implementation Notes**:
- Consider using Panel's `Indicator` widget or custom Bokeh/HoloViews implementation
- For echarts integration, may need to use Panel's ReactiveHTML
- Ensure data calculations are efficient (cache renewable percentages)
- Add configuration for gauge thresholds and colors

## Known Issues

### UFuncTypeError with DateTime Comparison (FIXED)
**Problem**: The dashboard encountered a numpy ufunc error when updating plots, specifically when comparing datetime64 and float types.

**Error Message**:
```
Error updating plots: ufunc 'greater' did not contain a loop with signature matching types 
(<class 'numpy.dtypes.DateTime64DType'>, <class 'numpy.dtypes._PyFloatDType'>) -> None
```

**Root Cause**: 
- When using pandas DataFrame comparisons like `df[col] < 0` or `.where()` operations, numpy was trying to compare the datetime64 index with scalar values
- This happened in battery storage and transmission flow calculations where negative values needed to be separated

**Fix Applied**:
1. Changed comparisons to use `.values` to compare only the data, not the index:
   ```python
   # Before: (plot_data[battery_col] < 0).any()
   # After:  (plot_data[battery_col].values < 0).any()
   ```

2. Replaced `.where()` operations with `np.where()` on values:
   ```python
   # Before: transmission_values.where(transmission_values > 0, 0)
   # After:  pd.Series(np.where(transmission_values.values > 0, transmission_values.values, 0), index=transmission_values.index)
   ```

**Status**: ✅ FIXED - Auto-updates now work without errors

### Transmission Plot X-Axis Datetime Formatting
**Problem**: The transmission flows chart shows all time labels as "00:00" instead of proper hourly times when switching between time ranges (1 day, 7 days, 30 days).

**UPDATE**: The issue was actually an x-axis range initialization problem, not a formatter issue. The datetime formatting works correctly when the user manually zooms/pans the x-axis, but the initial view shows incorrect range.

**Technical Details**:
- The transmission plot is an `hv.Overlay` containing multiple hvplot elements (area plots for unused capacity and line plots for actual flows)
- The datetime formatter hook is applied to the Overlay using `.opts(hooks=[self._get_datetime_formatter_hook()])`
- The formatter works correctly, but the initial x-axis range is not properly set
- This causes the view to show a compressed range where all labels appear as "00:00"

**Attempted Fixes**:
1. **Modified global formatter hook** - Updated `_get_datetime_formatter_hook()` to handle different plot structures:
   - Checked for `plot.handles['xaxis']`, `plot.state.xaxis`, `plot.state.xaxis[0]`
   - Added try/except blocks to handle missing attributes
   - **Result**: Failed - formatter still didn't apply to overlay components

2. **Applied formatter to individual elements** - Added `.opts(hooks=[self._get_datetime_formatter_hook()])` to both:
   - `filled_area` (hvplot.area for unused capacity)
   - `flow_line` (hvplot.line for actual flows)
   - **Result**: Failed - x-axis still showed "00:00" after switching time ranges

3. **Created custom formatter hook for transmission** - Built a specialized hook within `create_transmission_plot()`:
   - Tried accessing axis via `plot.state.below[0]` (Bokeh's axis storage)
   - Applied DatetimeTickFormatter based on current time range
   - **Result**: Failed - same issue persists

4. **Post-render formatting** - Attempted to apply formatting after HoloViews renders the plot:
   - Added hooks to individual plot elements (filled_area and flow_line)
   - In `update_plot()`, tried to access Bokeh model via `self.transmission_pane.get_root()`
   - Applied DatetimeTickFormatter to axes found in `bokeh_model.below`
   - **Result**: Initial render shows correct formatting, but switching from 7-day to 1-day reverts to all "00:00" labels

5. **Added `framewise=True` to Overlay** - Forces complete recomputation on updates:
   - Added `framewise=True` to the overlay opts
   - **Result**: Partially successful - formatting works but x-axis range is incorrect

6. **Explicitly set x-axis range (`xlim`)** - Calculate and set the x-axis range from data:
   - Calculate min/max from transmission data with small padding
   - Set `xlim=(x_min, x_max)` in overlay opts
   - **Result**: No change - the issue persists

7. **Use `xformatter` parameter directly in hvplot** - Bypass the hooks system entirely:
   - Create DatetimeTickFormatter based on time range
   - Pass `xformatter=dt_formatter` to both area and line plots
   - Remove hooks from overlay
   - **Result**: Runtime error - xformatter not compatible with area/line plots in this context

8. **Add `apply_ranges=False` to overlay** - Prevent automatic range determination:
   - Keep framewise=True and explicit xlim
   - Add apply_ranges=False to overlay opts
   - Re-add hooks to overlay
   - **Result**: Testing in progress

**Why This Is Challenging**:
- The transmission plot is complex with:
  - Shaded areas showing unused capacity (flow to limit)
  - Positive/negative flows representing import/export directions
  - Multiple interconnectors overlaid on the same plot
  - Custom hover tooltips with utilization percentages
- Any fix must preserve this sophisticated visualization logic

**Root Cause Analysis**:
- The issue appears to be with HoloViews/hvplot's handling of datetime formatters in Overlay plots
- When the plot is first created, the formatter works correctly
- After changing time ranges and recreating the plot, the formatter hook fails to apply
- This suggests a timing issue or state management problem in the HoloViews rendering pipeline

**Research Findings from HoloViz Documentation**:

After extensive research into HoloViews/hvplot documentation and GitHub issues, the following key insights were discovered:

1. **Known Issue**: Datetime formatters not persisting after plot updates is a documented issue in HoloViews, particularly with Overlays and DynamicMaps (GitHub issues #1713, #1744, #2284)

2. **Root Cause**: HoloViews maintains internal state that doesn't properly propagate formatters to overlay components during updates. When plots are updated (not recreated), the formatter hooks may be ignored or overridden.

3. **Hook Behavior**: 
   - `finalize_hooks` are meant to be applied after plot creation
   - Hooks on individual elements within an Overlay may not persist
   - The bokeh backend's handling of formatters in overlays is problematic

4. **Common Workarounds Found**:
   - Using `framewise=True` to force complete recomputation on updates
   - Applying `xformatter` parameter directly in hvplot calls
   - Using backend-specific options via `backend_opts`
   - Forcing plot recreation with unique identifiers

**Recommended Solutions (in order of preference)**:

1. **Use `framewise=True` on the Overlay**
   ```python
   combined_plot = hv.Overlay(plot_elements).opts(
       framewise=True,  # Forces axis recalculation on every update
       # ... other options
   )
   ```
   - Pros: Simple, designed for this use case
   - Cons: Slight performance overhead

2. **Apply `xformatter` directly in hvplot**
   ```python
   from bokeh.models.formatters import DatetimeTickFormatter
   
   formatter = DatetimeTickFormatter(hours="%H:%M", days="%a %d")
   plot = df.hvplot.area(x='time', xformatter=formatter)
   ```
   - Pros: Bypasses hook system, more direct
   - Cons: Need to create formatter for each element

3. **Force plot recreation with unique keys**
   ```python
   plot_id = f"plot_{region}_{time_range}_{timestamp}"
   plot.opts(name=plot_id)  # Forces new plot instance
   ```
   - Pros: Guarantees fresh state
   - Cons: Less efficient than updates

4. **Clear HoloViews renderer cache**
   ```python
   from holoviews import Store
   Store.renderers['bokeh'].reset()
   ```
   - Pros: Resets all cached state
   - Cons: Affects all plots, not just transmission

5. **Data reload (last resort)**
   - Force data reload to trigger complete plot recreation
   - Pros: Guaranteed to work
   - Cons: Inefficient, addresses symptom not cause

**Why Current Approaches Failed**:
- Hooks are applied at plot creation but don't persist through updates
- Overlay plots have complex internal structure that doesn't propagate formatters properly
- The post-render approach partially worked but state management issues persist

**Potential Solutions to Investigate**:
1. **Use Bokeh directly** - Build the transmission plot using Bokeh's low-level API instead of hvplot
2. **Single plot approach** - Combine all data into a single DataFrame and use hvplot with multiple y-columns
3. **Panel ReactiveHTML** - Use Panel's ReactiveHTML to have more control over plot updates
4. **Force complete re-render** - Clear the pane completely before assigning new plot
5. **Post-render formatting** - Access the Bokeh figure after HoloViews renders it and apply formatting directly

### Transmission Data Availability Issues
**Problem**: The 7-day transmission chart shows significant data gaps, particularly at the beginning of the time range.

**Data Analysis** (as of July 15, 2025):
- Total transmission data: 6,480 records spanning July 9-15
- Data coverage by day:
  - July 9-10: Only 0.3% coverage (6 records each day)
  - July 11: 31.5% coverage (partial data)
  - July 12-14: 85.7% coverage (good data)
  - July 15: 32.1% coverage (partial day)

**Impact**:
- 7-day view shows empty/missing data for the first 2-3 days
- This appears to be a data collection issue rather than a dashboard problem
- The updater service may have started collecting transmission data later than other data types

**Potential Solutions**:
1. Investigate why transmission data collection started late
2. Implement backfill functionality in the updater service
3. Add data availability indicators in the dashboard
4. Consider filtering out days with insufficient data

### Current Implementation vs. Alternatives
**Current**: `RadioButtonGroup` creates large button segments (like iOS segmented control)
- **Pros**: Clear selection, touch-friendly, looks modern
- **Cons**: Takes significant horizontal space, especially with 5 options

**Better Alternative**: `RadioBoxGroup` (traditional radio buttons)
```python
# More compact implementation
time_selector = pn.widgets.RadioBoxGroup(
    name='Time Range',
    options=['1 Day', '7 Days', '30 Days', 'All Data'],
    inline=True  # Horizontal layout
)
```
- **Pros**: Much more compact, familiar UI pattern, still clear selection
- **Cons**: Smaller touch targets

**Space Comparison**:
- RadioButtonGroup: ~280px width for 4 options
- RadioBoxGroup inline: ~200px width for same options
- RadioBoxGroup vertical: Minimal width but more height

**Recommendation**: Switch to `RadioBoxGroup` with `inline=True` for time ranges. This provides:
- 30% space savings
- Cleaner appearance
- Consistent with typical dashboard controls
- Still accessible on mobile with proper spacing

## Future Enhancements

### Planned Features
1. **WebSocket Updates**: Real-time data refresh
2. **Export Functionality**: Download charts/data
3. **Comparison Mode**: Side-by-side analysis
4. **Forecasting**: Integration with pre-dispatch data
5. **Mobile Responsive**: Tablet/phone optimization

### Dashboard Extensions
1. **Transmission Tab**: Dedicated interconnector analysis
2. **Summary Dashboard**: Key metrics overview
3. **Alert Configuration**: User-defined thresholds
4. **Historical Comparison**: Year-over-year analysis
5. **Market Events**: Highlight price spikes/constraints

## Design System & Best Practices

### Material Design Implementation

Following the [HoloViz Panel Material UI announcement](https://blog.holoviz.org/posts/panel_material_ui_announcement/), we are implementing the new Material theme consistently across all dashboard components.

#### Key Material Theme Features
- **Comprehensive Component Set**: Over 70 Material UI-based components
- **Global Theme Configuration**: Using `theme_config` for consistent styling
- **Accessibility Support**: Built-in accessibility features for all components
- **API Compatibility**: Fully compatible with existing Panel code

#### Theme Configuration
```python
# Global theme configuration
pn.extension('tabulator', template='material')
pn.config.theme = 'dark'

# Component-level theming
Card(
    content,
    theme_config={
        "palette": {"primary": {"main": "#1976d2"}},
        "typography": {"fontFamily": "Roboto"},
        "shape": {"borderRadius": 8}
    }
)
```

### Dashboard Design Principles (2025)

#### 1. Cognitive Load Minimization
- **Progressive Disclosure**: Start with overview, allow drill-down to details
- **5-Second Rule**: Essential information visible within 5 seconds
- **Clean Interface**: Minimize visual noise and focus on actionable metrics

#### 2. Visual Hierarchy & Information Architecture
- **Sequential Ordering**: Follow natural reading patterns (Z or F pattern)
- **Data Prioritization**: Essential metrics first, supporting data secondary
- **Consistent Visual Language**: Uniform colors, fonts, and chart types

#### 3. Accessibility & Internationalization
- **Screen Reader Support**: Proper ARIA labels and table headers
- **Color Accessibility**: High contrast ratios and color-blind friendly palettes
- **Global Support**: Multi-language and cultural considerations
- **Interactive Elements**: Touch-friendly for mobile, precise for desktop

#### 4. Performance Optimization
- **Real-time Capabilities**: Seamless updates for live data
- **Data Efficiency**: Batch API calls, implement caching strategies
- **Progressive Loading**: Load critical data first, defer secondary content
- **Client-side Operations**: Filter and sort locally when possible

#### 5. Interactive Design Patterns
- **User Control**: Empower users with filtering and customization
- **Cross-Dashboard Integration**: Seamless navigation between views
- **Contextual Actions**: Right-click menus, hover tooltips
- **Responsive Feedback**: Clear loading states and error handling

### Material Design Data Visualization Guidelines

#### Color System
```python
# Recommended color palette for energy data
fuel_colors = {
    'Solar': '#FFD700',           # Material Yellow 500
    'Rooftop Solar': '#FFF59D',   # Material Yellow 200
    'Wind': '#2196F3',            # Material Blue 500
    'Water': '#0D47A1',           # Material Blue 900
    'Battery Storage': '#9C27B0',  # Material Purple 500
    'Coal': '#5D4037',            # Material Brown 700
    'Gas other': '#FF5722',       # Material Deep Orange 500
    'OCGT': '#FF9800',            # Material Orange 500
    'CCGT': '#F57C00',            # Material Orange 700
    'Biomass': '#4CAF50',         # Material Green 500
    'Other': '#757575',           # Material Grey 600
    'Transmission Flow': '#E91E63' # Material Pink 500
}
```

#### Typography Scale
- **Headlines**: Roboto Medium, 24px (H1), 20px (H2), 16px (H3)
- **Body Text**: Roboto Regular, 14px
- **Captions**: Roboto Regular, 12px
- **Labels**: Roboto Medium, 12px

#### Layout & Spacing
- **Grid System**: 8dp base grid for consistent spacing
- **Card Elevation**: Use Material elevation system (1-8dp)
- **Margins**: 16dp for sections, 8dp for related elements
- **Responsive Breakpoints**: Mobile (<600px), Tablet (600-960px), Desktop (>960px)

### Component-Specific Guidelines

#### Chart Design
- **Consistent Axis Labels**: Always include units and proper scaling
- **Tooltip Information**: Show precise values and context
- **Animation**: Smooth transitions for data updates (200-300ms)
- **Interaction**: Pan, zoom, and selection capabilities

#### Table Design
- **Header Styling**: Material surface color with elevated appearance
- **Row Alternation**: Subtle background color differences
- **Sorting Indicators**: Clear visual feedback for active sorts
- **Pagination**: Material pagination component for large datasets

#### Navigation
- **Tab System**: Material tabs with clear active states
- **Breadcrumbs**: Show current location in complex hierarchies
- **Side Navigation**: Collapsible drawer for secondary actions

### Accessibility Requirements

#### Visual Accessibility
- **Contrast Ratios**: Minimum 4.5:1 for normal text, 3:1 for large text
- **Color Independence**: Information conveyed through more than color alone
- **Focus Indicators**: Clear keyboard navigation paths
- **Scalable Text**: Support for 200% zoom without horizontal scrolling

#### Interaction Accessibility
- **Keyboard Navigation**: All functions accessible via keyboard
- **Screen Reader Support**: Proper semantic markup and ARIA labels
- **Touch Targets**: Minimum 44px touch targets for mobile
- **Error Messages**: Clear, actionable error descriptions

### Performance Targets

#### Loading Performance
- **Initial Load**: <3 seconds for basic dashboard view
- **Data Updates**: <500ms for filtered views
- **Chart Rendering**: <1 second for complex visualizations
- **Memory Usage**: Monitor for memory leaks in long-running sessions

#### Data Handling
- **Client-side Caching**: 5-15 minute TTL for data files
- **Lazy Loading**: Load charts only when tabs are viewed
- **Virtual Scrolling**: For tables with >1000 rows
- **Data Aggregation**: Pre-compute common aggregations

### Implementation Checklist

#### Material Theme Setup
- [ ] Configure global Material theme with dark mode support
- [ ] Implement consistent color palette across all components
- [ ] Apply Material typography scale to all text elements
- [ ] Use Material elevation system for card hierarchies

#### Accessibility Compliance
- [ ] Add ARIA labels to all interactive elements
- [ ] Ensure keyboard navigation works throughout interface
- [ ] Test with screen readers (NVDA, JAWS)
- [ ] Validate color contrast ratios meet WCAG 2.1 AA standards

#### Performance Optimization
- [ ] Implement lazy loading for non-critical components
- [ ] Add data caching with appropriate TTL values
- [ ] Monitor and optimize chart rendering performance
- [ ] Test on mobile devices for responsive behavior

#### User Experience
- [ ] Progressive disclosure from overview to details
- [ ] Clear loading states and error messages
- [ ] Consistent interaction patterns across all tabs
- [ ] User preference persistence (dark/light mode, column selections)

### Panel Material UI Implementation Guide

#### Key Integration Points

##### 1. Bokeh/HoloViews Integration
- Material UI automatically themes Bokeh, hvPlot, and HoloViews plots
- Plots adapt to active theme (dark/light modes, primary colors, fonts)
- Use `pmu.Page` or include `pmu.ThemeToggle` for seamless theming

##### 2. Customization Methods
**Component-Level Styling (`sx` parameter):**
```python
Button(
    label="Custom Button",
    sx={
        "backgroundColor": "#1976d2",
        "&:hover": {"backgroundColor": "#1565c0"}
    }
)
```

**Theme-Level Configuration (`theme_config`):**
```python
Card(
    content,
    theme_config={
        "palette": {"primary": {"main": "#1976d2"}},
        "typography": {"fontFamily": "Roboto"},
        "shape": {"borderRadius": 8}
    }
)
```

##### 3. Dark Mode Implementation
- Enable globally: `pn.config.theme = 'dark'`
- Component-specific: `Button(label="Dark", dark_theme=True)`
- Automatic integration with Panel's dark mode configuration

##### 4. Branding Best Practices
**Brand Architecture:**
```
aemo_dashboard/
└── brand/
    ├── colors.py      # Color palette definitions
    ├── mui.py         # Material UI theme configs
    └── assets/        # Logos, icons, custom CSS
```

**Theme Configuration Example:**
```python
AEMO_THEME_CONFIG = {
    "palette": {
        "primary": {"main": "#1976d2"},    # AEMO blue
        "secondary": {"main": "#dc004e"},  # Alert red
        "background": {
            "default": "#121212",           # Dark mode background
            "paper": "#1e1e1e"             # Card background
        }
    },
    "typography": {
        "fontFamily": "Roboto, Arial, sans-serif",
        "h1": {"fontSize": "2.5rem", "fontWeight": 600}
    }
}
```

##### 5. Migration Path
To upgrade existing Panel code to Material UI:
1. Replace `pn.widgets.*` with Material UI equivalents where available
2. Wrap layout sections in `pmu.Card` instead of `pn.Column`
3. Apply consistent `theme_config` across components
4. Use `sx` parameter for component-specific styling

### Reference Implementation

For detailed implementation examples, see:
- **HoloViz Panel Material UI Blog**: https://blog.holoviz.org/posts/panel_material_ui_announcement/
- **Material Design 3 Guidelines**: https://m3.material.io/
- **Panel Material UI Documentation**: https://panel-material-ui.holoviz.org/
- **Bokeh/HoloViews Integration**: https://panel-material-ui.holoviz.org/how_to/bokeh_holoviews.html
- **Customization Guide**: https://panel-material-ui.holoviz.org/how_to/customize.html