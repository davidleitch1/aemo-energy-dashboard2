# AEMO Energy Dashboard - Technical Documentation

*Last Updated: July 20, 2025, 12:10 AM AEST*

## ✅ Penetration Tab Implementation Complete (July 19-20, 2025)

### Summary
Successfully implemented all three renewable energy penetration charts with interactive controls and proper data handling. The implementation included fixing critical data gaps and ensuring accurate calculations.

### What Was Accomplished

#### 1. **Rooftop Solar Data Backfill** ✅
- **Problem**: Discovered missing rooftop data for 2024-2025:
  - 2024: Missing August 3 - December 31 (153 days)
  - 2025: Missing January 1 - June 17 (168 days)
- **Solution**: Created comprehensive backfill script (`backfill_rooftop_2024_2025.py`)
- **Result**: Successfully backfilled 152,180 records, achieving >99% data coverage

#### 2. **VRE Production Charts Implementation** ✅
Created three interactive charts in the Penetration tab:

1. **VRE Production Annualised Over Last 30 Days**
   - Year-over-year comparison (2023, 2024, 2025)
   - Interactive region and fuel type selectors
   - Fixed calculation method: Apply 30-day rolling average on 30-minute data BEFORE annualisation
   - Natural Y-axis scaling (removed fixed limits)

2. **VRE Production by Fuel Type**
   - Long-term trends from 2018 to present
   - Shows Wind, Solar, and Rooftop separately
   - 30-day rolling average with proper annualisation
   - Always shows NEM-wide data

3. **Thermal vs Renewables**
   - 180-day rolling average showing transition to renewables
   - Renewables = Wind + Solar + Rooftop + Hydro (Water)
   - Thermal = Coal + all gas types (CCGT, OCGT, Gas other)
   - Shows clear seasonal patterns

#### 3. **Technical Improvements** ✅
- **Chart Organization**: Implemented tabbed layout to prevent axis linking issues
- **Calculation Fix**: Corrected rolling average to apply on 30-minute data (1440 periods) before daily sampling
- **Selector Placement**: Moved region/fuel selectors to relevant tabs only
- **Chart Consistency**: Set all charts to 1000px width for uniform appearance
- **Smoothing Module**: Created reusable smoothing functions in `shared/smoothing.py`

### Key Fixes Applied

1. **Data Resolution**: 
   - Original: Daily average → 30-day rolling → Annualise ❌
   - Fixed: 30-day rolling on 30-min data → Daily sampling → Annualise ✅

2. **Fuel Filtering**: Fixed issue where individual fuel selection showed all fuel types

3. **Layout**: Organized charts into sub-tabs with appropriate controls per tab

### Files Created/Modified

- Created: `src/aemo_dashboard/shared/smoothing.py`
- Created: `src/aemo_dashboard/penetration/penetration_tab.py`
- Created: `backfill_rooftop_2024_2025.py` (in aemo-data-updater)
- Modified: `src/aemo_dashboard/generation/gen_dash.py` (integrated penetration tab)

### Performance Notes

- Charts load quickly with DuckDB backend
- Memory usage remains low despite processing 5+ years of data
- Interactive updates are responsive

## 🎯 Current Task: Implement Penetration Tab Charts

### Task Overview
Create three hvplot-based charts for the new "Penetration" tab that replicate the style and functionality of the provided screenshots:

1. **VRE Production Annualised Over Last 30 Days** (Image #1)
   - Shows year-over-year comparison of Variable Renewable Energy (VRE) production
   - VRE = Wind + Solar + Rooftop combined
   - Uses 30-day moving average (to be improved with better smoothing)
   - Displays data by day of year for 2023, 2024, and 2025
   - Y-axis: TWh annualised
   - Colors: 2023 (light blue), 2024 (orange), 2025 (green)

2. **VRE Production by Fuel Rolling 30 Day Average** (Image #2)
   - Shows long-term trends for each renewable type
   - Three separate lines: nem_rooftop, nem_solar, nem_wind
   - Time series from 2018 to present
   - Y-axis: TWh annualised
   - Colors: rooftop (light blue), solar (orange), wind (green)

3. **Thermal vs Renewables 180 Day Annualised** (Image #3)
   - Compares renewable vs coal+gas generation
   - Uses 180-day rolling average
   - Shows the transition from thermal to renewable generation
   - Y-axis: TWh
   - Colors: renewable (light blue), coal+gas (orange)

### Implementation Requirements

1. **Interactive Controls**:
   - Region selector: [NEM, NSW1, QLD1, SA1, TAS1, VIC1]
   - Fuel selector for VRE components: [Wind, Solar, Rooftop]
   - All charts should update reactively based on selections

2. **Smoothing Enhancement**:
   - Current: Simple 30-day moving average
   - Investigate better smoothing options:
     - Exponential Weighted Moving Average (EWM)
     - Centered moving average
     - Savitzky-Golay filter
     - LOESS/LOWESS smoothing
   - Make smoothing method configurable and reusable across dashboard

3. **Technical Considerations**:
   - Use hvplot for consistency with other dashboard components
   - Match color scheme from screenshots as closely as possible
   - Ensure proper data aggregation and annualisation calculations
   - Handle missing data gracefully
   - Optimize performance for large date ranges

### Data Calculations

**Annualisation Formula**:
- For daily data: `annualised_TWh = daily_average_MW * 24 * 365 / 1_000_000`
- For 30-day average: `annualised_TWh = 30_day_avg_MW * 24 * 365 / 1_000_000`

**VRE Definition**:
```python
VRE = generation[generation['fuel_type'].isin(['Wind', 'Solar', 'Rooftop'])].groupby('settlementdate')['scadavalue'].sum()
```

### Next Steps
1. Research and implement optimal smoothing technique ✓ (Research complete)
2. Create configurable smoothing system in shared utilities
3. Implement Chart #1 with interactive controls
4. Test performance and accuracy
5. Proceed to Charts #2 and #3

### Configurable Smoothing System Design

Create a reusable smoothing module (`src/aemo_dashboard/shared/smoothing.py`) that provides:

1. **Smoothing Methods**:
   - **Centered Moving Average** (default): Best for historical trend analysis
     - `df.rolling(window=window_size, center=True).mean()`
   - **Exponential Weighted Moving Average**: For responsive real-time analysis
     - `df.ewm(span=window_size, adjust=False).mean()`
   - **LOESS/LOWESS**: For complex non-linear patterns
     - `statsmodels.nonparametric.smoothers_lowess.lowess()`
   - **Savitzky-Golay Filter**: For preserving peaks and features
     - `scipy.signal.savgol_filter()`

2. **Configuration Interface**:
   ```python
   class SmootherConfig:
       method: str = 'centered_ma'  # 'centered_ma', 'ewm', 'loess', 'savgol'
       window_size: int = 30  # days for MA/EWM
       loess_frac: float = 0.1  # fraction for LOESS
       savgol_order: int = 3  # polynomial order for Savitzky-Golay
   ```

3. **Usage Pattern**:
   ```python
   from aemo_dashboard.shared.smoothing import apply_smoothing, SmootherConfig
   
   config = SmootherConfig(method='centered_ma', window_size=30)
   smoothed_data = apply_smoothing(time_series_data, config)
   ```

4. **UI Integration**:
   - Add smoothing method selector to Penetration tab
   - Allow users to adjust window size/parameters
   - Show both raw and smoothed data options

## Overview

The AEMO Energy Dashboard is a comprehensive web-based visualization platform for analyzing Australian electricity market data. It provides real-time insights into generation, pricing, transmission flows, and market dynamics through an interactive Panel-based interface.

**Key Features:**
- Real-time data visualization updated every 4.5 minutes
- Interactive charts and tables for generation, pricing, and transmission analysis
- Station-level performance analysis with revenue calculations
- Automatic resolution selection for optimal performance
- Hybrid data loading with intelligent fallback strategies

## Architecture

### System Design

The dashboard follows a **read-only architecture** that consumes data from the separate [aemo-data-updater](https://github.com/davidleitch1/aemo-data-updater) service:

```
┌─────────────────────────────────────┐
│         AEMO Data Updater           │
│    (Separate Repository)            │
│                                     │
│  • Downloads data every 4.5 min    │
│  • Processes & stores in parquet    │
│  • Handles 5min & 30min resolutions│
│  • Manages data gaps & backfills   │
└─────────────────┬───────────────────┘
                  │
                  │ Shared Parquet Files
                  │
                  ▼
┌─────────────────────────────────────┐
│       AEMO Energy Dashboard         │
│      (This Repository)              │
│                                     │
│  • Read-only data access            │
│  • Intelligent data loading        │
│  • Hybrid resolution strategies     │
│  • Interactive visualizations      │
└─────────────────────────────────────┘
```

### Directory Structure

```
aemo-energy-dashboard/
├── src/aemo_dashboard/
│   ├── shared/                       # Core data processing layer
│   │   ├── config.py                 # Configuration management
│   │   ├── logging_config.py         # Logging setup
│   │   ├── resolution_manager.py     # Intelligent resolution selection
│   │   ├── generation_adapter.py     # Generation data loading
│   │   ├── price_adapter.py          # Price data loading with hybrid support
│   │   ├── transmission_adapter.py   # Transmission data loading
│   │   ├── rooftop_adapter.py        # Rooftop solar with Henderson smoothing
│   │   └── performance_optimizer.py  # Performance optimization
│   │
│   ├── generation/                   # Generation analysis tab
│   │   ├── gen_dash.py              # Main generation dashboard
│   │   └── components/               # Generation-specific components
│   │
│   ├── analysis/                     # Price analysis tab
│   │   ├── price_analysis.py        # Price analysis dashboard
│   │   └── components/               # Price analysis components
│   │
│   ├── station/                      # Station analysis tab
│   │   ├── station_analysis.py      # Station analysis engine
│   │   ├── station_analysis_ui.py   # Station analysis UI
│   │   └── components/               # Station-specific components
│   │
│   └── nem_dash/                     # NEM overview tab
│       ├── generation_overview.py   # Market overview
│       ├── price_components.py      # Price analysis components
│       └── market_summary.py        # Market summaries
│
├── data/                             # Local data cache (legacy)
├── logs/                             # Application logs
├── .env                              # Environment configuration
├── pyproject.toml                    # Python dependencies
└── start_dashboard.sh               # Production startup script
```

## Data Processing Flow

### 1. Data Sources

The dashboard reads from standardized parquet files created by aemo-data-updater:

| Data Type | File | Resolution | Records | Update Frequency |
|-----------|------|------------|---------|------------------|
| Generation | `scada5.parquet` | 5-minute | 6M+ | Every 4.5 minutes |
| Generation | `scada30.parquet` | 30-minute | 38M+ | Every 30 minutes |
| Prices | `prices5.parquet` | 5-minute | 69K+ | Every 4.5 minutes |
| Prices | `prices30.parquet` | 30-minute | 1.7M+ | Every 30 minutes |
| Transmission | `transmission5.parquet` | 5-minute | 46K+ | Every 4.5 minutes |
| Transmission | `transmission30.parquet` | 30-minute | 1.9M+ | Every 30 minutes |
| Rooftop Solar | `rooftop30.parquet` | 30-minute | 811K+ | Every 30 minutes |

### 2. Resolution Manager

The `resolution_manager.py` module provides intelligent data source selection:

```python
def get_optimal_resolution_with_fallback(start_date, end_date, data_type):
    """
    Smart resolution selection based on:
    - Date range duration (< 7 days: 5min, >= 7 days: 30min)
    - Data availability (fallback to alternative if gaps exist)
    - Performance considerations (memory usage, load times)
    """
```

**Selection Logic:**
- **Short ranges (< 7 days)**: Use 5-minute data for maximum detail
- **Long ranges (≥ 7 days)**: Use 30-minute data for performance
- **Hybrid strategy**: Automatically fallback when primary data has gaps
- **Data availability**: Check actual data coverage before selection

### 3. Data Adapters

#### Generation Adapter
- **Purpose**: Load and process generation data by DUID
- **Features**: Hybrid loading, DUID filtering, memory optimization
- **Fallback**: 5min → 30min or 30min → 5min based on availability

#### Price Adapter
- **Purpose**: Load regional price data with standardization
- **Features**: Column mapping, hybrid concatenation, datetime preservation
- **Key Fix**: Preserves SETTLEMENTDATE column during hybrid loading

#### Transmission Adapter
- **Purpose**: Load interconnector flow data
- **Features**: Flow direction analysis, capacity calculations

#### Rooftop Adapter
- **Purpose**: Convert 30-minute solar data to 5-minute resolution
- **Features**: Henderson filter smoothing, natural generation curves
- **Algorithm**: Cubic interpolation with exponential decay forecasting

### 4. Hybrid Loading Strategy

The dashboard implements a sophisticated hybrid loading approach:

```python
def load_with_hybrid_fallback(start_date, end_date, data_type):
    """
    1. Determine optimal resolution (5min vs 30min)
    2. Check data availability for the chosen resolution
    3. If gaps exist, split date range:
       - Use 30min data for periods with complete coverage
       - Use 5min data for periods with 30min gaps
    4. Combine results seamlessly
    """
```

**Benefits:**
- **Maximizes data coverage**: Uses best available data for each period
- **Maintains performance**: Avoids loading unnecessary high-resolution data
- **Transparent operation**: User sees no difference in interface
- **Automatic recovery**: Handles data collection interruptions gracefully

## User Interface

### Dashboard Tabs

#### 1. Generation Tab
- **Purpose**: Analyze generation patterns by fuel type and region
- **Features**: Generation by fuel stacks, capacity factors, time series
- **Data Sources**: scada5.parquet, scada30.parquet, rooftop30.parquet
- **Performance**: Uses resolution manager for optimal loading

#### 2. Price Analysis Tab
- **Purpose**: Analyze electricity prices across regions and time
- **Features**: Price trends, volatility analysis, regional comparisons
- **Data Sources**: prices5.parquet, prices30.parquet
- **Performance**: Hybrid loading for seamless historical analysis

#### 3. Station Analysis Tab
- **Purpose**: Detailed analysis of individual power stations
- **Features**: Revenue calculations, performance metrics, comparison tools
- **Data Sources**: All parquet files + DUID mapping
- **Integration**: Combines generation, price, and capacity data

#### 4. NEM Overview Tab
- **Purpose**: Market-wide overview and summary statistics
- **Features**: Market trends, generation mix, price summaries
- **Data Sources**: All data sources with aggregation
- **Performance**: Optimized for quick overview loading

### Time Range Controls

**Available Ranges:**
- **Real-time**: Last 24 hours (5-minute resolution)
- **Recent**: Last 7 days (5-minute resolution)
- **Monthly**: Last 30 days (30-minute resolution)
- **Historical**: All available data (30-minute resolution)
- **Custom**: User-defined ranges (automatic resolution selection)

### Auto-Resolution Selection

The dashboard automatically selects the optimal data resolution based on:

1. **Date Range Duration:**
   - < 1 day: 5-minute data (maximum detail)
   - 1-7 days: 5-minute data (detailed analysis)
   - 7-30 days: 30-minute data (performance optimization)
   - > 30 days: 30-minute data (historical trends)

2. **Data Availability:**
   - Checks for gaps in preferred resolution
   - Automatically falls back to alternative resolution
   - Combines multiple sources when needed

3. **Performance Considerations:**
   - Memory usage limits
   - Browser rendering capabilities
   - Chart density optimization

## Technical Implementation

### Data Loading Pipeline

```python
# 1. User selects time range
start_date, end_date = user_selection()

# 2. Resolution manager determines strategy
strategy = resolution_manager.get_optimal_resolution_with_fallback(
    start_date, end_date, 'generation'
)

# 3. Data adapter loads with fallback
data = generation_adapter.load_generation_data(
    start_date=start_date,
    end_date=end_date,
    resolution='auto'  # Uses strategy automatically
)

# 4. Dashboard displays results
display_charts(data)
```

### Column Standardization

The dashboard standardizes data formats across different sources:

**Generation Data:**
- `settlementdate` → datetime column
- `duid` → generator identifier
- `scadavalue` → generation output (MW)

**Price Data:**
- `settlementdate` → datetime column
- `regionid` → region identifier (NSW1, QLD1, etc.)
- `rrp` → regional reference price ($/MWh)

**Transmission Data:**
- `settlementdate` → datetime column
- `interconnectorid` → interconnector identifier
- `meteredmwflow` → measured flow (MW)

### Performance Optimizations

#### Memory Management
- **Lazy loading**: Only load data when needed
- **Chunk processing**: Handle large datasets in chunks
- **Garbage collection**: Explicit cleanup of unused data

#### Rendering Optimization
- **Chart density**: Adjust point density based on data volume
- **Aggregation**: Pre-aggregate data for overview displays
- **Caching**: Cache frequently accessed data

#### User Experience
- **Loading indicators**: Show progress during data loading
- **Responsive design**: Adapt to different screen sizes
- **Error handling**: Graceful degradation on data issues

## Configuration

### Environment Variables

```bash
# Data file locations (pointing to aemo-data-updater files)
GEN_OUTPUT_FILE=/path/to/scada5.parquet
TRANSMISSION_OUTPUT_FILE=/path/to/transmission5.parquet
SPOT_HIST_FILE=/path/to/prices5.parquet
ROOFTOP_SOLAR_FILE=/path/to/rooftop30.parquet
GEN_INFO_FILE=/path/to/gen_info.pkl

# Dashboard settings
DASHBOARD_PORT=5006
DASHBOARD_HOST=0.0.0.0
LOG_LEVEL=INFO
```

### Data File Requirements

The dashboard expects parquet files with specific schemas:

**Generation Files (scada5.parquet, scada30.parquet):**
```python
columns = ['settlementdate', 'duid', 'scadavalue']
dtypes = {
    'settlementdate': 'datetime64[ns]',
    'duid': 'object',
    'scadavalue': 'float64'
}
```

**Price Files (prices5.parquet, prices30.parquet):**
```python
columns = ['settlementdate', 'regionid', 'rrp']
dtypes = {
    'settlementdate': 'datetime64[ns]',
    'regionid': 'object',
    'rrp': 'float64'
}
```

## Deployment

### Local Development

```bash
# Clone repository
git clone https://github.com/davidleitch1/aemo-energy-dashboard.git
cd aemo-energy-dashboard

# Set up environment
uv venv
source .venv/bin/activate
uv pip install -e .

# Configure environment
cp .env.example .env
# Edit .env with your data file paths

# Run dashboard
.venv/bin/python src/aemo_dashboard/generation/gen_dash.py
# Access at http://localhost:5006
```

### Production Deployment

```bash
# Use production startup script
./start_dashboard.sh

# Or manual deployment
./deploy_production.sh
```

## Recent Fixes (July 18, 2025)

### ✅ SETTLEMENTDATE Column Standardization Error
- **Issue**: Dashboard failed to load data due to missing SETTLEMENTDATE column
- **Root Cause**: Hybrid price loading was losing DatetimeIndex during concatenation
- **Solution**: Modified price_adapter.py to preserve SETTLEMENTDATE column
- **Result**: Dashboard now works with both short and long date ranges

### ✅ 2025 Price Data Gaps
- **Issue**: Severe gaps in 2025 price data (January 19.3%, March 9.7%, June 6.7%)
- **Solution**: Backfilled using Public_Prices archives (handled in aemo-data-updater)
- **Result**: 100% coverage for January-June 2025 (81,050 new records)

### ✅ Data Migration Complete
- **Achievement**: Successfully migrated from legacy data structure
- **Removed**: All data collection code (dashboard is now read-only)
- **Added**: Comprehensive data adapters for new parquet structure
- **Result**: 50-87% more data across all datasets

## DuckDB Migration (July 2025)

### Problem Identified
The dashboard was using 21GB of memory due to loading 5.5 years of data (38M+ rows) into pandas DataFrames. This was caused by:
- Loading all historical data into memory at startup
- Creating multiple copies through merges and aggregations
- Using inefficient data types (object strings, float64)

### Solution Implemented
DuckDB provides a zero-memory-footprint solution that queries parquet files directly:
- **Memory usage**: 56MB (vs 21GB) - 99.7% reduction
- **Startup time**: Instant (vs 10-15 seconds)
- **Query performance**: 10-100ms for all operations
- **No data duplication**: Works with existing parquet files

### Migration Status
- ✅ DuckDB service implemented (`shared_data_duckdb.py`)
- ✅ All data adapters converted to DuckDB (generation, price, transmission, rooftop)
- ✅ Performance tested: 162MB memory usage with full functionality
- ✅ Hybrid Query Manager implemented with smart caching (July 19, 2025)
- ✅ DuckDB Views created for optimized queries (July 19, 2025)
- ✅ Comprehensive test suite: 100% pass rate (July 19, 2025)
- 🔄 Price analysis module refactoring ready to start
- 📋 Detailed implementation guide in `HYBRID_DUCKDB_IMPLEMENTATION.md`

### Architecture Decision
Hybrid approach (DuckDB + Smart Caching):
```
Panel Dashboard → Hybrid Query Manager → DuckDB Service → Parquet Files
                         ↓
                    Smart Cache (TTL)
```

This approach maintains complex pandas operations while using DuckDB for data loading.

## Price Analysis Refactoring Plan (July 2025)

### Overview
Refactor the price analysis module to use DuckDB for data loading while maintaining existing functionality. The goal is to reduce memory usage from 21GB to <500MB and improve load times from 60s to <5s.

### Architecture: Two-Layer Approach

#### 1. Query-Based Service Layer (DuckDB)
- **Purpose**: Handles all data storage and retrieval from parquet files
- **Function**: Executes SQL queries directly on parquet files without loading into memory
- **Key Features**:
  - Creates SQL views for common joins (generation + price + DUID mapping)
  - Returns only requested data subsets
  - Handles date filtering and aggregation in SQL

#### 2. Hybrid Query Manager (Bridge Layer)
- **Purpose**: Bridges between DuckDB queries and pandas operations
- **Function**: Manages smart caching and progressive loading
- **Key Features**:
  - Caches frequently used aggregations (5-minute TTL)
  - Loads data in 50k row chunks with progress updates
  - Keeps complex pandas operations (hierarchical data, pivot tables)

### Performance Targets

#### Memory Usage
- **Current**: 21 GB (loads all parquet files)
- **Target**: 300-500 MB
  - DuckDB connection: ~50 MB
  - DUID mapping: ~10 MB  
  - Active query results: ~200 MB (typical month of data)
  - Smart cache: ~100 MB (capped)
  - UI components: ~40 MB

#### Load Times
**Initial Dashboard Load (New User)**
- **Current**: 60-90 seconds
- **Target**: 3-5 seconds
  - DuckDB initialization: 0.5s
  - Create SQL views: 0.5s
  - Load DUID mapping: 0.2s
  - Initial UI render: 1-2s
  - First data query: 1-2s

**Tab Switching Performance**
- **Current**: 5-10 seconds (re-processes all data)
- **Target**:
  - Cached data: <0.5 seconds
  - New date range: 1-2 seconds
  - Complex aggregation: 2-3 seconds

### Implementation Plan

#### Phase 1: Create Shared DuckDB Query Layer (Week 1)
1. **Hybrid Query Manager** (`src/aemo_dashboard/shared/hybrid_query_manager.py`)
   - Query-based data retrieval with smart caching
   - Progressive loading with chunk support
   - TTL-based cache management
   
2. **Integration Views** (`src/aemo_dashboard/shared/duckdb_views.py`)
   - Pre-joined views for common operations
   - Aggregation views for performance
   - Materialized views for complex calculations

3. **Testing**:
   - Test query performance < 1s for month of data
   - Verify memory usage < 100MB per query
   - Validate cache hit rates > 80%

#### Phase 2: Refactor PriceAnalysisMotor (Week 2)
1. **Remove upfront data loading**
   - Change `__init__` to lightweight initialization
   - Convert `load_data()` to metadata checking only
   - Make `integrate_data()` query on-demand

2. **Maintain complex operations**
   - Keep hierarchical data creation in pandas
   - Preserve existing aggregation logic
   - Add caching for computed results

3. **Testing**:
   - Verify all existing functionality works
   - Test memory doesn't exceed 500MB
   - Ensure UI responsiveness

#### Phase 3: Update Dashboard Integration (Week 3)
1. **Add loading states**
   - Progress indicators for long operations
   - Threaded data loading to prevent UI freeze
   - User feedback during queries

2. **Implement lazy tab loading**
   - Load data only when tab activated
   - Cache results between tab switches
   - Clear old data when switching contexts

3. **Testing**:
   - UI remains responsive during loading
   - Tab switches < 2s
   - Progress indicators work correctly

#### Phase 4: Deployment and Monitoring (Week 4)
1. **Feature flag deployment**
   - Add USE_HYBRID_DUCKDB environment variable
   - Gradual rollout with monitoring
   - Quick rollback capability

2. **Performance monitoring**
   - Track query times and cache hits
   - Monitor memory usage
   - Log slow operations

### Key Design Decisions

1. **Hybrid Approach**: Use DuckDB for data loading but keep complex pandas operations
2. **Smart Caching**: Cache aggregated results, not raw data
3. **Progressive Loading**: Load data as needed with progress indicators
4. **Threading**: Keep UI responsive during data operations
5. **Backward Compatibility**: Maintain existing API for smooth migration

### Development Notes

**Important**: Always use the venv Python to run tests and scripts:
```bash
cd /Users/davidleitch/Library/Mobile Documents/com~apple~CloudDocs/snakeplay/AEMO_spot/aemo-energy-dashboard
.venv/bin/python <script_name>.py
```

### Known Challenges

1. **Hierarchical Data Creation**: Complex pandas operations for tabulator widget
2. **Reactive UI Updates**: Panel dashboard expects synchronous data
3. **Complex Calculations**: Numpy operations and conditional logic
4. **Multi-level Aggregations**: Dynamic grouping with multiple hierarchies

These challenges are addressed through the hybrid approach that maintains pandas for complex operations while using DuckDB for efficient data loading.

## Outstanding Issues

### 🚨 Issue 1: Missing Data in Long Time Period Charts
- **Problem**: Generation by fuel charts show significant data gaps when viewing "All Available Data" (2024-2025)
- **Symptoms**: 
  - Charts display normally for most of the timeline but show dramatic drops/gaps toward the end
  - Recent periods (July 2025) appear to have missing or corrupted generation data
  - Short time periods (30 days) display correctly with proper fuel stacking
  - Long time periods (>6 months) show data discontinuities
- **Root Cause**: Critical data collection gaps in aemo-data-updater for June 18 - July 3, 2025
- **Evidence** (July 18, 2025 Analysis): 
  - **Solar Data**: Only 150 records in June 18 - July 3 period (expected ~7000+)
    - Early period range: 2025-07-02 11:30:00 to 2025-07-03 00:00:00
    - Missing ~95% of expected solar generation data
  - **Transmission Data**: ZERO records in June 18 - July 3 period
    - Transmission data only starts from July 3, 2025 onwards
    - Complete absence of interconnector flow data for 15+ days
  - **Dashboard logs**: Confirm transmission date range starts 2025-07-03 07:00:00
- **Immediate Solutions**:
  - Investigate data collection gaps in aemo-data-updater for June 18 - July 3, 2025
  - Check for missing SCADA data files or processing errors in this specific period
  - Implement data quality validation in dashboard loading
  - Add data gap indicators to inform users of missing periods

### 🚨 Issue 2: Slow Initial Dashboard Load
- **Problem**: Dashboard takes 30-60 seconds to load on first access
- **Symptoms**: Blank screen during initial load, poor user experience
- **Root Cause**: Synchronous loading of large datasets during initialization
- **Proposed Solutions**:
  - Implement lazy loading for dashboard tabs
  - Add loading indicators and progress bars
  - Load only essential data on startup
  - Cache frequently accessed data
  - Implement background data loading

### Priority Fixes Needed

1. **Data Quality Issues** (High Priority)
   - **URGENT**: Fix critical data gaps in aemo-data-updater for June 18 - July 3, 2025
     - Solar data: Missing 95% of expected records (only 150 vs ~7000+)
     - Transmission data: Completely missing (0 records for 15+ days)
   - Investigate data collection failures in this specific period
   - Implement data validation to catch missing periods in real-time
   - Add visual indicators for data gaps in charts

2. **Initial Load Performance** (High Priority)
   - Redesign dashboard initialization flow
   - Add loading states and progress indicators
   - Implement tab-based lazy loading

3. **User Experience Improvements** (Medium Priority)
   - Add resolution selection controls
   - Implement data quality indicators
   - Add export functionality for charts and data

## Development Guidelines

### Code Standards
- Follow PEP 8 style guidelines
- Use type hints for all function signatures
- Implement comprehensive error handling
- Add docstrings for all public functions

### Testing Requirements
- Unit tests for all data adapters
- Integration tests for full dashboard workflows
- Performance tests for large datasets
- Regression tests for fixed issues

### Documentation
- Update CLAUDE.md for significant changes
- Document new features and API changes
- Maintain inline code documentation
- Update configuration examples

## Support and Maintenance

### Monitoring
- Check dashboard logs regularly: `tail -f logs/aemo_dashboard.log`
- Monitor data freshness and quality
- Track performance metrics and user feedback

### Updates
- Dashboard updates require no data collection changes
- Data format changes require adapter updates
- New features should maintain backward compatibility

### Troubleshooting
- Check data file accessibility and permissions
- Verify environment configuration
- Review recent logs for error patterns
- Test with smaller date ranges to isolate issues

---

*This documentation reflects the current state of the AEMO Energy Dashboard as of July 18, 2025. The dashboard is production-ready with known performance limitations that are being actively addressed.*

## Generation Dashboard Refactoring (July 19, 2025)

### What Was Completed

#### Phase 1: Infrastructure ✅
1. **Created Generation-Specific DuckDB Views**
   - `generation_by_fuel_30min` - Aggregates 38M records → ~173K records (99.5% reduction)
   - `generation_by_fuel_5min` - For detailed short-range views
   - `capacity_utilization_30min` - Pre-calculated utilization percentages
   - `generation_with_prices_30min` - Integrated view with pricing
   - `daily_generation_by_fuel` - Daily summaries for overview displays
   
   **Performance Results:**
   - 24-hour query: 21,368 → 1,722 records (91.9% reduction)
   - Query time: 60s → 0.01-0.77s
   - Year of NEM data: 38M → 173K records

2. **Created GenerationQueryManager Class**
   - Smart caching with 200MB limit and 5-minute TTL
   - Automatic resolution selection (5min vs 30min based on date range)
   - Cache performance: 100-700x faster on repeated queries
   - Region-specific and NEM-wide query support

#### Phase 2: Dashboard Integration (Partial) ⚠️
1. **Refactored load_generation_data()**
   - Uses GenerationQueryManager for ranges > 30 days
   - Falls back to raw data for short ranges (< 30 days)
   - Maintains backward compatibility with existing code

2. **Refactored process_data_for_region()**
   - Handles both aggregated and raw data seamlessly
   - Re-queries for specific regions when needed
   - Preserves all existing functionality

3. **Updated calculate_capacity_utilization()**
   - Uses pre-computed utilization from DuckDB views
   - Falls back to calculation for raw data

### What Needs to Be Done Next 🚨

#### Critical: Testing and Validation
The refactoring is complete but NOT YET TESTED with the actual dashboard. The next programmer must:

1. **Create Comprehensive Test Suite**
   ```python
   # test_generation_dashboard_refactor.py
   
   # Test 1: Memory usage with "All Available Data"
   - Should use < 500MB (vs current 21GB)
   - Monitor memory before/during/after load
   
   # Test 2: Performance with different date ranges
   - 24 hours: < 1s load time
   - 30 days: < 2s load time
   - 1 year: < 3s load time
   - All data (5+ years): < 5s load time
   
   # Test 3: Data accuracy
   - Fuel type totals must match current implementation
   - Compare aggregated vs raw data results
   - Verify no data loss or duplication
   
   # Test 4: Region filtering
   - Test all regions (NSW1, QLD1, VIC1, SA1, TAS1)
   - Verify NEM totals = sum of regions
   
   # Test 5: UI functionality
   - Chart displays correctly
   - Time range selector works
   - Region selector works
   - Auto-update continues to function
   ```

2. **Integration Testing with Live Dashboard**
   - Kill current dashboard process
   - Start dashboard with refactored code
   - Test all tabs and functionality
   - Monitor logs for errors

3. **Edge Case Testing**
   - Missing data periods
   - Single DUID with no fuel mapping
   - Boundary conditions (exactly 30 days)
   - Cache eviction under memory pressure

4. **Performance Monitoring**
   - Add timing logs for each operation
   - Monitor cache hit rates
   - Track memory usage over time
   - Check for memory leaks

### Files Modified

1. **src/aemo_dashboard/shared/duckdb_views.py**
   - Added `_create_generation_dashboard_views()` method
   - Created 5 new generation-specific views

2. **src/aemo_dashboard/generation/generation_query_manager.py** (NEW)
   - Complete implementation of query manager
   - Handles caching and region-specific queries

3. **src/aemo_dashboard/generation/gen_dash.py**
   - Added GenerationQueryManager to imports
   - Updated `__init__` to include query manager
   - Refactored `load_generation_data()` 
   - Refactored `process_data_for_region()`
   - Updated `calculate_capacity_utilization()`

4. **src/aemo_dashboard/generation/gen_dash_original.py** (BACKUP)
   - Original file backed up for rollback if needed

### Testing Commands

```bash
# Test the views are working
cd /path/to/dashboard
.venv/bin/python test_generation_views.py

# Test the query manager
.venv/bin/python test_generation_query_manager.py

# Test the full dashboard (MUST DO)
.venv/bin/python test_generation_dashboard_refactor.py  # Create this file!

# Run the dashboard
kill [current_pid]
.venv/bin/python run_dashboard_duckdb.py
```

### Expected Results After Testing

1. **Memory**: 21GB → <500MB for all operations
2. **Load Time**: 60s → <5s for "All Available Data"
3. **UI**: No freezing, smooth interactions
4. **Accuracy**: Identical fuel totals to current implementation

### Rollback Plan

If issues are found:
```bash
# Restore original
mv src/aemo_dashboard/generation/gen_dash_original.py src/aemo_dashboard/generation/gen_dash.py
# Remove new file
rm src/aemo_dashboard/generation/generation_query_manager.py
# Restart dashboard
```

### Summary for Next Programmer

The heavy lifting is done - the infrastructure is in place and the dashboard is refactored. However, it's untested with real data. Your critical tasks are:

1. **Write comprehensive tests** (template provided above)
2. **Test with live dashboard** - this is where issues will surface
3. **Monitor performance** - ensure targets are met
4. **Fix any integration issues** - likely around data type mismatches or edge cases
5. **Document results** - update this file with test results

The refactoring follows the same successful pattern used for Price Analysis and Station Analysis modules, so confidence is high that it will work once properly tested.

## ⚠️ Startup Optimization Attempt - Issues Encountered (July 19, 2025)

### What Was Attempted
Created an experimental "optimized" dashboard version with aggressive performance improvements:
- `gen_dash_optimized.py` - Lazy tab loading and shared query managers
- `nem_dash_tab_optimized.py` - Progressive component loading
- `run_dashboard_optimized.py` - Optimized startup script

### Problems Encountered
1. **Integration Errors**: The optimized version had multiple integration issues:
   - `ERROR: OptimizedEnergyDashboard.load_generation_data() missing 2 required positional arguments`
   - `ERROR: ClassSelector parameter 'interactive._pane' value must be an instance of Viewable, not 26`
   - `ERROR: 'fuel_type'` in generation overview

2. **Threading Issues**: Background loading caused race conditions and async errors:
   - `ERROR: no running event loop` when creating components
   - Components not properly initialized when accessed

3. **Inheritance Problems**: The optimized class didn't properly override all parent methods

### Working Solution
The basic optimizations that DO work and should be used:
- ✅ **DuckDB mode by default** (`USE_DUCKDB=true`)
- ✅ **Fixed data_service/__init__.py** to use DuckDB consistently
- ✅ **Memory usage reduced** from 21GB to ~200MB
- ✅ **Startup time improved** from 8-9s to ~6s (33% improvement)

### Correct Startup Command
```bash
# Use the reliable DuckDB version:
.venv/bin/python run_dashboard_duckdb.py
```

### Lessons Learned
1. **Don't over-optimize**: The experimental lazy loading introduced more problems than benefits
2. **Test thoroughly**: The optimized version passed basic tests but failed in real usage
3. **DuckDB alone provides huge benefits**: Just using DuckDB mode gives 99% memory reduction
4. **Keep it simple**: The standard DuckDB version is fast enough for production use

### Files to Avoid
- ❌ `run_dashboard_optimized.py` - Has errors
- ❌ `gen_dash_optimized.py` - Integration issues
- ❌ `nem_dash_tab_optimized.py` - Component errors

### Recommendation
Use the standard DuckDB version (`run_dashboard_duckdb.py`) which provides:
- Fast startup (~6 seconds)
- Low memory usage (200MB vs 21GB)
- Full functionality without errors
- Stable performance

*Documented: July 19, 2025, 6:35 PM AEST*