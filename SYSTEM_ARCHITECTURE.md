# AEMO Energy System - Complete Architecture Documentation

*Last Updated: July 20, 2025, 12:30 PM AEST*

## Table of Contents
1. [System Overview](#system-overview)
2. [Architecture Diagram](#architecture-diagram)
3. [Data Collection Service](#data-collection-service)
4. [Data Storage Layer](#data-storage-layer)
5. [Dashboard Visualization](#dashboard-visualization)
6. [Data Flow Pipeline](#data-flow-pipeline)
7. [Time Resolution Management](#time-resolution-management)
8. [DuckDB Integration](#duckdb-integration)
9. [Key Interfaces](#key-interfaces)
10. [Development Guide](#development-guide)

## System Overview

The AEMO Energy System consists of two complementary services that work together to collect, process, and visualize Australian electricity market data:

1. **Data Collection Service** (`aemo-data-updater`) - Downloads and processes real-time market data
2. **Visualization Dashboard** (`aemo-energy-dashboard`) - Provides interactive data analysis and visualization

### Design Principles
- **Separation of Concerns**: Data collection is completely separate from visualization
- **Read-Only Dashboard**: Dashboard never modifies data, only reads from parquet files
- **Shared Storage**: Both services use a common parquet file storage layer
- **Time-Based Resolution**: Automatic selection between 5-minute and 30-minute data
- **Memory Efficiency**: DuckDB provides SQL queries without loading data into memory

## Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────────────┐
│                           AEMO Market Data                              │
│         (Market Operator APIs - Updated every 5 minutes)                │
└────────────────────────┬────────────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                    aemo-data-updater Service                            │
│  ┌─────────────────────────────────────────────────────────────────┐   │
│  │                        Data Collectors                           │   │
│  │  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  ┌────────┐│   │
│  │  │Price        │  │Generation   │  │Transmission │  │Rooftop ││   │
│  │  │Collector    │  │Collector    │  │Collector    │  │Solar   ││   │
│  │  └─────────────┘  └─────────────┘  └─────────────┘  └────────┘│   │
│  └─────────────────────────────────────────────────────────────────┘   │
│                                │                                        │
│                                ▼                                        │
│  ┌─────────────────────────────────────────────────────────────────┐   │
│  │                     Alert Systems                                │   │
│  │  ┌─────────────────────┐  ┌─────────────────────────────────┐  │   │
│  │  │ SMS Alerts (Twilio) │  │ Email Alerts (SMTP)             │  │   │
│  │  │ High Price: $1000   │  │ New DUID Discovery              │  │   │
│  │  │ Extreme: $10000     │  │ Generation Changes              │  │   │
│  │  └─────────────────────┘  └─────────────────────────────────┘  │   │
│  └─────────────────────────────────────────────────────────────────┘   │
└────────────────────────────────┬────────────────────────────────────────┘
                                 │
                                 ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                      Parquet Storage Layer                              │
│  ┌──────────────────┐  ┌──────────────────┐  ┌──────────────────────┐  │
│  │ 5-Minute Data    │  │ 30-Minute Data   │  │ Reference Data       │  │
│  │ • prices5.parquet│  │ • prices30.parquet│  │ • gen_info.pkl      │  │
│  │ • scada5.parquet │  │ • scada30.parquet │  │ • duid_mapping.csv  │  │
│  │ • trans5.parquet │  │ • trans30.parquet │  │                     │  │
│  │                  │  │ • rooftop30.parquet│ │                     │  │
│  └──────────────────┘  └──────────────────┘  └──────────────────────┘  │
└────────────────────────────────┬────────────────────────────────────────┘
                                 │
                                 ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                    aemo-energy-dashboard                                │
│  ┌─────────────────────────────────────────────────────────────────┐   │
│  │                      DuckDB Query Engine                         │   │
│  │  • SQL Views for common aggregations                            │   │
│  │  • Direct parquet file queries (no memory load)                 │   │
│  │  • Intelligent resolution selection                             │   │
│  │  • Smart caching with TTL                                       │   │
│  └────────────────────────┬────────────────────────────────────────┘   │
│                           │                                             │
│  ┌─────────────────────────────────────────────────────────────────┐   │
│  │                    Dashboard Components                          │   │
│  │  ┌────────────┐  ┌────────────┐  ┌────────────┐  ┌──────────┐ │   │
│  │  │ Today Tab  │  │Generation  │  │Price       │  │Station   │ │   │
│  │  │(NEM Dash)  │  │Tab         │  │Analysis    │  │Analysis  │ │   │
│  │  └────────────┘  └────────────┘  └────────────┘  └──────────┘ │   │
│  └─────────────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────────┘
```

## Data Collection Service

### Repository Structure
```
aemo-data-updater/
├── src/aemo_updater/
│   ├── collectors/                   # Data collection modules
│   │   ├── price_collector.py        # 5-min spot prices & 30-min trading prices
│   │   ├── generation_collector.py   # SCADA generation data by DUID
│   │   ├── transmission_collector.py # Interconnector flows
│   │   ├── rooftop_collector.py      # Rooftop solar estimates
│   │   └── unified_collector.py      # Orchestrates all collectors
│   │
│   ├── alerts/                       # Alert infrastructure
│   │   ├── email_alerts.py          # New DUID notifications
│   │   └── twilio_price_alerts.py   # High price SMS alerts
│   │
│   ├── ui/                          # Monitoring interface
│   │   └── status_dashboard.py      # Web UI on port 5011
│   │
│   └── config.py                    # Configuration management
│
├── scripts/                         # Maintenance scripts
│   ├── backfill_*.py               # Historical data recovery
│   └── recalculate_scada30.py      # Fix aggregation issues
│
└── .env                            # Production credentials
```

### Data Collection Process

#### 1. Price Collector
- **Frequency**: Every 4.5 minutes
- **Data Sources**: 
  - 5-minute dispatch prices: `http://www.nemweb.com.au/Reports/CURRENT/DispatchIS_Reports/`
  - 30-minute trading prices: `http://www.nemweb.com.au/Reports/CURRENT/TradingIS_Reports/`
- **Output Files**:
  - `prices5.parquet`: 5-minute dispatch prices (69K+ records)
  - `prices30.parquet`: 30-minute trading prices (1.7M+ records)
- **Key Fields**: settlementdate, regionid, rrp (price in $/MWh)

#### 2. Generation Collector
- **Frequency**: Every 4.5 minutes
- **Data Source**: `http://nemweb.com.au/Reports/CURRENT/Dispatch_SCADA/`
- **Output Files**:
  - `scada5.parquet`: 5-minute SCADA values (6M+ records)
  - `scada30.parquet`: 30-minute aggregated values (38M+ records)
- **Aggregation**: 
  - Pre-August 2024: Native 30-minute SCADA from AEMO
  - Post-August 2024: Calculated as `mean()` of 5-minute values
- **Key Fields**: settlementdate, duid, scadavalue (MW)

#### 3. Transmission Collector
- **Frequency**: Every 4.5 minutes
- **Data Source**: DISPATCHINTERCONNECTORRES table
- **Output Files**:
  - `transmission5.parquet`: 5-minute flows (46K+ records)
  - `transmission30.parquet`: 30-minute aggregated (1.9M+ records)
- **Key Fields**: settlementdate, interconnectorid, meteredmwflow (MW)

#### 4. Rooftop Solar Collector
- **Frequency**: Every 30 minutes
- **Data Source**: AEMO rooftop PV estimates
- **Output File**: `rooftop30.parquet` (811K+ records)
- **Special Processing**: Henderson filter for smooth 5-minute interpolation
- **Key Fields**: settlementdate, regionid, measurement (MW)

### Alert Systems

#### SMS Alerts (Twilio)
```python
# Triggered when spot prices exceed thresholds
if current_price >= 10000:  # $10,000/MWh
    send_sms("🚨🚨🚨 EXTREME PRICE ALERT")
elif current_price >= 1000:  # $1,000/MWh
    send_sms("⚠️ HIGH PRICE ALERT")
elif was_high and current_price < 300:  # Recovery
    send_sms("✅ Price recovered")
```

#### Email Alerts (SMTP)
```python
# Triggered when new generation units detected
if new_duids_found:
    send_email(
        subject="New Generation Units Discovered",
        body=f"Found {len(new_duids)} new DUIDs: {new_duids}"
    )
```

## Data Storage Layer

### Parquet File Schema

#### Generation Data Schema
```python
# scada5.parquet / scada30.parquet
{
    'settlementdate': datetime64[ns],  # Settlement period timestamp
    'duid': object,                    # Dispatchable Unit ID
    'scadavalue': float64              # Generation in MW
}
```

#### Price Data Schema
```python
# prices5.parquet / prices30.parquet
{
    'settlementdate': datetime64[ns],  # Settlement period timestamp
    'regionid': object,                # Region (NSW1, QLD1, etc.)
    'rrp': float64                     # Regional Reference Price $/MWh
}
```

#### Transmission Data Schema
```python
# transmission5.parquet / transmission30.parquet
{
    'settlementdate': datetime64[ns],  # Settlement period timestamp
    'interconnectorid': object,        # Interconnector name
    'meteredmwflow': float64,         # Measured flow in MW
    'mwflow': float64,                # Target flow in MW
    'mwlosses': float64               # Calculated losses in MW
}
```

### Reference Data

#### gen_info.pkl
```python
# Pickled dictionary with DUID metadata
{
    'DUID': {
        'station_name': str,           # Power station name
        'fuel_type': str,              # Coal, Gas, Wind, Solar, etc.
        'region': str,                 # NSW1, QLD1, VIC1, SA1, TAS1
        'capacity_mw': float,          # Nameplate capacity
        'technology': str              # Technology type
    }
}
```

## Dashboard Visualization

### Repository Structure
```
aemo-energy-dashboard/
├── src/aemo_dashboard/
│   ├── shared/                       # Core infrastructure
│   │   ├── duckdb_views.py          # SQL view definitions
│   │   ├── hybrid_query_manager.py  # Smart caching layer
│   │   ├── resolution_manager.py    # Time resolution logic
│   │   ├── generation_adapter.py    # Generation data adapter
│   │   ├── price_adapter.py         # Price data adapter
│   │   ├── transmission_adapter.py  # Transmission adapter
│   │   └── rooftop_adapter.py       # Rooftop solar adapter
│   │
│   ├── nem_dash/                    # Today tab (NEM overview)
│   │   ├── price_components.py      # 5-min spot prices
│   │   ├── renewable_gauge.py       # Renewable % gauge
│   │   ├── generation_overview.py   # 24-hour generation
│   │   └── nem_dash_tab.py         # Tab orchestration
│   │
│   ├── generation/                  # Generation analysis tab
│   │   ├── gen_dash.py             # Main generation dashboard
│   │   └── generation_query_manager.py # Specialized queries
│   │
│   ├── analysis/                    # Price analysis tab
│   │   ├── price_analysis.py       # Price analysis engine
│   │   └── price_analysis_ui.py    # UI components
│   │
│   └── station/                     # Station analysis tab
│       ├── station_analysis.py      # Station calculations
│       └── station_analysis_ui.py   # Station UI
│
└── run_dashboard_duckdb.py         # Main entry point
```

## Data Flow Pipeline

### 1. Real-Time Data Collection (Every 4.5 minutes)
```python
# Unified collector orchestrates all data collection
async def collect_all_data():
    # Download latest files from AEMO
    price_data = await price_collector.collect()
    gen_data = await generation_collector.collect()
    trans_data = await transmission_collector.collect()
    
    # Append to parquet files
    append_to_parquet(price_data, 'prices5.parquet')
    append_to_parquet(gen_data, 'scada5.parquet')
    append_to_parquet(trans_data, 'transmission5.parquet')
    
    # Aggregate to 30-minute if needed
    if is_30_minute_boundary():
        aggregate_to_30_minute()
```

### 2. Dashboard Data Loading
```python
# DuckDB loads data without memory overhead
def load_generation_data(start_date, end_date):
    # Determine optimal resolution
    if (end_date - start_date).days < 7:
        resolution = '5min'
    else:
        resolution = '30min'
    
    # Query directly from parquet
    query = f"""
    SELECT settlementdate, duid, scadavalue
    FROM read_parquet('scada{resolution}.parquet')
    WHERE settlementdate BETWEEN '{start_date}' AND '{end_date}'
    """
    
    return duckdb.execute(query).df()
```

### 3. Data Aggregation Pipeline
```sql
-- DuckDB view for generation by fuel type
CREATE VIEW generation_by_fuel_30min AS
SELECT 
    g.settlementdate,
    i.fuel_type,
    i.region,
    SUM(g.scadavalue) as total_generation_mw
FROM scada30 g
JOIN gen_info i ON g.duid = i.duid
GROUP BY g.settlementdate, i.fuel_type, i.region
```

## Time Resolution Management

### Automatic Resolution Selection
The system intelligently selects data resolution based on the time range:

```python
def get_optimal_resolution(start_date, end_date):
    duration = (end_date - start_date).days
    
    if duration < 1:
        return '5min'      # Maximum detail for < 24 hours
    elif duration < 7:
        return '5min'      # Good detail for weekly view
    elif duration < 30:
        return '30min'     # Performance optimization
    else:
        return '30min'     # Historical analysis
```

### Time Funneling Strategy
1. **Recent Data (< 7 days)**: Use 5-minute resolution for detailed analysis
2. **Medium Range (7-30 days)**: Use 30-minute data for balance
3. **Historical (> 30 days)**: Use 30-minute data with optional daily aggregation
4. **All Available**: Daily aggregation for 5+ years of data

### Hybrid Fallback Logic
When data gaps exist, the system automatically falls back:
```python
def load_with_fallback(start_date, end_date, primary_resolution):
    # Try primary resolution first
    data = load_data(f'scada{primary_resolution}.parquet', start_date, end_date)
    
    # Check for gaps
    expected_periods = calculate_expected_periods(start_date, end_date, primary_resolution)
    actual_periods = len(data)
    
    if actual_periods < expected_periods * 0.9:  # >10% gap
        # Fall back to alternative resolution
        alt_resolution = '5min' if primary_resolution == '30min' else '30min'
        alt_data = load_data(f'scada{alt_resolution}.parquet', start_date, end_date)
        
        # Combine data intelligently
        data = merge_data_sources(data, alt_data)
    
    return data
```

## DuckDB Integration

### Why DuckDB?
1. **Zero Memory Footprint**: Queries parquet files directly without loading into RAM
2. **SQL Interface**: Familiar SQL syntax for complex aggregations
3. **Performance**: 10-100ms query times on 38M+ row datasets
4. **Integration**: Works seamlessly with pandas DataFrames

### Key DuckDB Views

#### generation_by_fuel_30min
```sql
-- Pre-aggregated generation by fuel type
CREATE VIEW generation_by_fuel_30min AS
SELECT 
    settlementdate,
    fuel_type,
    region,
    SUM(scadavalue) as total_generation_mw,
    COUNT(DISTINCT duid) as unit_count
FROM scada30
JOIN gen_info ON scada30.duid = gen_info.duid
GROUP BY settlementdate, fuel_type, region
```

#### daily_generation_by_fuel
```sql
-- Daily aggregation for long-term trends
CREATE VIEW daily_generation_by_fuel AS
SELECT 
    DATE_TRUNC('day', settlementdate) as date,
    fuel_type,
    region,
    AVG(total_generation_mw) as avg_generation_mw,
    MAX(total_generation_mw) as max_generation_mw
FROM generation_by_fuel_30min
GROUP BY date, fuel_type, region
```

#### generation_with_prices_30min
```sql
-- Join generation with prices for revenue calculations
CREATE VIEW generation_with_prices_30min AS
SELECT 
    g.settlementdate,
    g.duid,
    g.scadavalue,
    i.fuel_type,
    i.station_name,
    i.region,
    p.rrp as spot_price
FROM scada30 g
JOIN gen_info i ON g.duid = i.duid
JOIN prices30 p ON g.settlementdate = p.settlementdate 
    AND i.region = p.regionid
```

### Query Manager Pattern
```python
class GenerationQueryManager:
    def __init__(self):
        self.conn = duckdb.connect(':memory:')
        self._create_views()
        self.cache = {}
        self.cache_ttl = 300  # 5 minutes
    
    def get_generation_by_fuel(self, start_date, end_date, region='NEM'):
        # Check cache first
        cache_key = f"gen_fuel_{start_date}_{end_date}_{region}"
        if cache_key in self.cache:
            return self.cache[cache_key]
        
        # Query from view
        query = f"""
        SELECT * FROM generation_by_fuel_30min
        WHERE settlementdate BETWEEN '{start_date}' AND '{end_date}'
        AND region = '{region}'
        """
        
        result = self.conn.execute(query).df()
        self.cache[cache_key] = result
        return result
```

## Key Interfaces

### 1. Data Adapter Interface
All data adapters follow this pattern:
```python
class DataAdapter:
    def load_data(self, start_date, end_date, resolution='auto'):
        """Load data for specified date range"""
        pass
    
    def get_latest_data(self, hours=24):
        """Get most recent data"""
        pass
    
    def check_data_availability(self, start_date, end_date):
        """Check if data exists for date range"""
        pass
```

### 2. Query Manager Interface
Query managers provide optimized data access:
```python
class QueryManager:
    def __init__(self):
        self.conn = duckdb.connect()
        self._create_views()
        self.cache = TTLCache(maxsize=100, ttl=300)
    
    def get_aggregated_data(self, **kwargs):
        """Get pre-aggregated data from views"""
        pass
```

### 3. Dashboard Component Interface
Dashboard components follow this pattern:
```python
def create_component(dashboard_instance=None):
    """Create a Panel component"""
    
    def update_component():
        """Update logic for the component"""
        try:
            # Get data
            data = query_manager.get_data()
            
            # Create visualization
            chart = create_chart(data)
            
            return chart
        except Exception as e:
            return error_panel(e)
    
    return pn.pane.panel(update_component)
```

## Development Guide

### Setting Up Development Environment

1. **Clone Both Repositories**
```bash
# Data collection service
git clone https://github.com/davidleitch1/aemo-data-updater.git

# Dashboard
git clone https://github.com/davidleitch1/aemo-energy-dashboard.git
```

2. **Configure Environment**
```bash
# Copy environment template
cp .env.example .env

# Edit with your paths
vim .env
```

3. **Install Dependencies**
```bash
# Using uv for fast installation
uv venv
source .venv/bin/activate
uv pip install -e .
```

### Common Development Tasks

#### Adding a New Data Source
1. Create collector in `aemo-data-updater/src/aemo_updater/collectors/`
2. Add to unified collector orchestration
3. Define parquet schema
4. Create corresponding adapter in dashboard

#### Adding a New Dashboard View
1. Create DuckDB view in `shared/duckdb_views.py`
2. Add query method to appropriate query manager
3. Create UI component following the pattern
4. Integrate into dashboard tab

#### Performance Optimization
1. Always use DuckDB views for aggregations
2. Implement caching in query managers
3. Use appropriate time resolution
4. Lazy load components where possible

### Testing

#### Data Collection Tests
```bash
cd aemo-data-updater
python test_collectors.py
```

#### Dashboard Tests
```bash
cd aemo-energy-dashboard
python test_dashboard_functionality.py
python test_duckdb_service.py
```

### Debugging

#### Check Data Quality
```python
# Verify parquet files
import pandas as pd
df = pd.read_parquet('scada30.parquet')
print(f"Records: {len(df)}")
print(f"Date range: {df['settlementdate'].min()} to {df['settlementdate'].max()}")
print(f"Missing values: {df.isnull().sum()}")
```

#### Monitor DuckDB Performance
```python
# Enable query profiling
conn.execute("PRAGMA enable_profiling")
conn.execute("PRAGMA profiling_output='profile.json'")
```

## Best Practices

### 1. Data Collection
- Always validate downloaded data before storing
- Implement retry logic for network failures
- Log all operations for debugging
- Monitor data gaps and trigger backfills

### 2. Dashboard Development
- Use DuckDB views instead of loading full datasets
- Implement progressive loading for large queries
- Cache frequently accessed aggregations
- Provide user feedback during long operations

### 3. Performance
- Pre-aggregate data in DuckDB views
- Use appropriate time resolution for date ranges
- Implement smart caching with TTL
- Monitor memory usage and query times

### 4. Error Handling
- Graceful degradation when data is missing
- Clear error messages for users
- Log errors with context for debugging
- Implement automatic recovery where possible

## Troubleshooting

### Common Issues

1. **Missing Data**
   - Check collector logs for download failures
   - Verify AEMO website availability
   - Run appropriate backfill script

2. **Slow Dashboard**
   - Check if using DuckDB mode (`USE_DUCKDB=true`)
   - Verify views are created properly
   - Monitor cache hit rates

3. **Memory Issues**
   - Ensure DuckDB mode is enabled
   - Check for pandas operations on large datasets
   - Review query manager cache settings

### Support Resources
- GitHub Issues: Report bugs and feature requests
- Logs: Check `logs/` directory for detailed information
- Monitoring: Use status dashboard on port 5011

---

*This documentation provides a complete understanding of the AEMO Energy System architecture. For specific implementation details, refer to the source code and inline documentation.*