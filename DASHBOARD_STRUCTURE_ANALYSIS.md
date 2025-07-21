# AEMO Energy Dashboard - Complete Structure Analysis

## Overview
The AEMO Energy Dashboard is built with Panel/HoloViews and uses a hybrid data architecture that can operate in two modes:
1. **DuckDB Mode** - Queries parquet files directly without loading into memory
2. **Legacy Mode** - Loads parquet files into pandas DataFrames

## Main Dashboard Entry Point
- **File**: `src/aemo_dashboard/generation/gen_dash.py`
- **Runner**: `run_dashboard.py` (wrapper script)
- **Template**: Material template with dark theme
- **Port**: 5006 (default)

## Tab Structure

### 1. **Nem-dash Tab** (Primary Dashboard)
**Module**: `src/aemo_dashboard/nem_dash/nem_dash_tab.py`

This tab contains three main components arranged in a specific layout:

#### Components:
1. **Price Section** (`price_components.py`)
   - Shows current spot prices by region
   - Data source: Hybrid - uses `load_price_adapter` from `shared.adapter_selector`
   - Updates: Real-time with 4.5-minute intervals

2. **Renewable Energy Gauge** (`renewable_gauge.py`)
   - Circular gauge showing renewable percentage
   - Data source: Direct parquet read (`pd.read_parquet`) from generation files
   - Calculates renewable % from fuel types: Wind, Solar, Hydro

3. **Generation Overview** (`generation_overview.py`)
   - 24-hour generation chart by fuel type
   - Data sources:
     - Generation: `pd.read_parquet(gen_file)`
     - Transmission: `pd.read_parquet(transmission_file)`
     - Rooftop Solar: Uses `load_rooftop_adapter`
   - Shows stacked area chart with interconnector flows

#### Layout:
```
Top Row:    [Generation Chart] [Price Section]
Bottom Row: [Renewable Gauge]
```

### 2. **Generation by Fuel Tab**
**Module**: `src/aemo_dashboard/generation/gen_dash.py` (main dashboard class)

#### Components:
1. **Left Panel - Controls**:
   - Region selector (All, NSW1, VIC1, QLD1, SA1, TAS1)
   - Time range selector (1hr to 30 days)
   - Update controls

2. **Right Panel - Chart Subtabs**:
   - **Generation Stack**: Stacked area chart by fuel type
   - **Capacity Utilization**: Line charts showing utilization %

#### Data Source:
- Uses `GenerationQueryManager` class
- Which uses `HybridQueryManager` underneath
- Can query via DuckDB or load DataFrames based on configuration

### 3. **Average Price Analysis Tab**
**Module**: `src/aemo_dashboard/analysis/price_analysis_ui.py`

#### Components:
1. **Controls Section**:
   - Date range pickers with presets (7 days, 30 days, All)
   - Grouping checkboxes (Fuel, Region, Station)
   - Column selection (Generation MWh, Revenue, Average Price)

2. **Main Table**:
   - Tabulator widget showing aggregated data
   - Dynamically groups by selected hierarchies
   - Shows revenue calculations

3. **Detail Table**:
   - Shows DUID-level details when drilling down

#### Data Source:
- Uses `PriceAnalysisMotor` class
- Which uses `HybridQueryManager` for data loading
- Integrates generation + price data for revenue calculations

### 4. **Station Analysis Tab**
**Module**: `src/aemo_dashboard/station/station_analysis_ui.py`

#### Components:
1. **Search Interface**:
   - Station search by name/DUID
   - Autocomplete functionality
   - Date range selection

2. **Analysis Subtabs**:
   - **Time Series**: Generation and price charts
   - **Time of Day**: Hourly average patterns
   - **Summary Stats**: Key metrics and tables

#### Data Source:
- Uses `StationAnalysisMotor` class
- Which uses `HybridQueryManager`
- Queries individual station data

## Data Access Architecture

### Hybrid Query Manager (`shared/hybrid_query_manager.py`)
- Central data access layer used by most components
- Can operate in two modes:
  1. **DuckDB Mode**: Queries parquet files directly
  2. **DataFrame Mode**: Loads and caches DataFrames
- Provides caching (TTL-based) and memory management

### DuckDB Data Service (`data_service/shared_data_duckdb.py`)
- Zero-memory footprint approach
- Creates views for parquet files:
  - `generation_5min`, `generation_30min`
  - `price_5min`, `price_30min`
  - `transmission_5min`, `transmission_30min`
  - `rooftop_solar`
- Handles complex queries with joins and aggregations

### Direct Parquet Reads
Some components still use direct pandas reads:
- NEM dash components (generation overview, renewable gauge)
- These bypass the hybrid system for simplicity

## Data Files Used

### 5-Minute Data:
- `scada5.parquet` - Generation data
- `prices5.parquet` - Spot prices
- `transmission5.parquet` - Interconnector flows

### 30-Minute Data:
- `scada30.parquet` - Aggregated generation
- `prices30.parquet` - Aggregated prices
- `transmission30.parquet` - Aggregated flows

### Other:
- `rooftop_solar.parquet` - Rooftop solar estimates
- `gen_info.pickle` - DUID metadata and mappings

## Update Mechanisms

1. **Auto-refresh**: 
   - NEM dash tab updates every 4.5 minutes
   - Uses Panel's `add_periodic_callback`

2. **Manual refresh**:
   - Each tab has refresh/update buttons
   - Triggers data reload from source

3. **Reactive updates**:
   - Parameter changes trigger immediate updates
   - Uses Panel's param system for reactivity

## Performance Optimizations

1. **DuckDB Mode**:
   - No memory usage for data storage
   - Queries only needed columns/rows
   - Pushes filtering to storage layer

2. **Caching**:
   - HybridQueryManager caches query results
   - TTL-based cache expiration
   - Configurable cache size limits

3. **Lazy Loading**:
   - Data loaded on-demand
   - Initial page load is fast
   - Heavy queries deferred until needed

## Summary

The dashboard uses a sophisticated hybrid architecture that balances performance and functionality:
- **Modern components** use the HybridQueryManager for efficient data access
- **Legacy components** still use direct parquet reads but are being migrated
- **DuckDB integration** provides zero-memory querying when enabled
- **Material Design theme** provides consistent, modern UI