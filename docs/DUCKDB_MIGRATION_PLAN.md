# DuckDB Migration Plan for AEMO Dashboard

## Overview

This document outlines the migration path from the current memory-intensive pandas approach (21GB) to the efficient DuckDB approach (56MB).

## Architecture Decision: Direct Integration (No FastAPI)

**Recommended Architecture:**
```
Panel Dashboard
     ↓
DuckDB Service (in-process)
     ↓
Parquet Files
```

**Why no FastAPI?**
- Panel runs in a single Python process
- DuckDB connection can be shared within that process
- No need for HTTP overhead between Panel and data
- Simpler deployment (one process instead of two)
- Better performance (no serialization/deserialization)

FastAPI would only be needed if:
- You want to serve data to multiple different applications
- You need a REST API for external consumers
- You want to separate data service from dashboard service

## Migration Steps

### Step 1: Install DuckDB (✅ Already Done)
```bash
cd /path/to/aemo-energy-dashboard
uv add duckdb
```

### Step 2: Update Data Adapters

Currently, your dashboard uses adapters like:
- `generation_adapter.py`
- `price_adapter.py`
- `transmission_adapter.py`

We'll modify these to use DuckDB instead of loading parquet files into pandas.

#### Example: Update generation_adapter.py

**Current approach:**
```python
def load_generation_data(start_date, end_date, resolution='auto'):
    # Loads entire parquet file into memory
    df = pd.read_parquet('scada30.parquet')
    # Filter in memory
    return df[(df['settlementdate'] >= start_date) & 
              (df['settlementdate'] <= end_date)]
```

**New DuckDB approach:**
```python
from data_service.shared_data_duckdb import duckdb_data_service

def load_generation_data(start_date, end_date, resolution='auto'):
    # Query only what's needed
    return duckdb_data_service.get_generation_by_fuel(
        start_date=start_date,
        end_date=end_date,
        resolution=resolution
    )
```

### Step 3: Update Panel Dashboard Components

The Panel dashboards themselves need minimal changes since DuckDB returns pandas DataFrames.

#### In gen_dash.py:

**Current:**
```python
# Loads all data at startup
gen_data = load_generation_data(start_date, end_date)
```

**New:**
```python
# Initialize DuckDB service once
from data_service.shared_data_duckdb import duckdb_data_service

# Query data on-demand
@pn.depends(date_range_slider.param.value)
def update_generation_chart(date_range):
    start_date, end_date = date_range
    gen_data = duckdb_data_service.get_generation_by_fuel(
        start_date=start_date,
        end_date=end_date
    )
    # Rest of the chart code remains the same
    return create_chart(gen_data)
```

### Step 4: Update Station Analysis

Station analysis is more complex but follows the same pattern:

```python
# Instead of loading all data and filtering
def get_station_data(station_name, start_date, end_date):
    return duckdb_data_service.get_station_data(
        station_name=station_name,
        start_date=start_date,
        end_date=end_date
    )
```

### Step 5: Initialize DuckDB Service at Startup

In your main dashboard file:

```python
# src/aemo_dashboard/generation/gen_dash.py

import panel as pn
from data_service.shared_data_duckdb import duckdb_data_service

# Initialize once when dashboard starts
logger.info("Initializing DuckDB data service...")
data_service = duckdb_data_service  # This is a singleton

# Rest of your dashboard code
```

### Step 6: Update Configuration

Ensure all parquet file paths are correctly configured in your `.env` file:
```env
GEN_OUTPUT_FILE=/path/to/scada5.parquet
SPOT_HIST_FILE=/path/to/prices5.parquet
TRANSMISSION_OUTPUT_FILE=/path/to/transmission5.parquet
ROOFTOP_SOLAR_FILE=/path/to/rooftop30.parquet
GEN_INFO_FILE=/path/to/gen_info.pkl
```

### Step 7: Test Migration

1. **Test data service independently:**
   ```bash
   python test_duckdb_service.py
   ```

2. **Test with one dashboard component:**
   - Start with generation dashboard
   - Verify data loads correctly
   - Check performance

3. **Migrate remaining components:**
   - Price analysis
   - Station analysis
   - Transmission flows

## File-by-File Migration Checklist

- [ ] `src/aemo_dashboard/shared/generation_adapter.py` - Update to use DuckDB
- [ ] `src/aemo_dashboard/shared/price_adapter.py` - Update to use DuckDB
- [ ] `src/aemo_dashboard/shared/transmission_adapter.py` - Update to use DuckDB
- [ ] `src/aemo_dashboard/generation/gen_dash.py` - Update data loading
- [ ] `src/aemo_dashboard/analysis/price_analysis.py` - Update data loading
- [ ] `src/aemo_dashboard/station/station_analysis.py` - Update queries
- [ ] `src/aemo_dashboard/nem_dash/generation_overview.py` - Update data access

## Code Patterns

### Pattern 1: Simple Data Query
```python
# Old way
df = pd.read_parquet('prices30.parquet')
filtered = df[(df['settlementdate'] >= start) & (df['settlementdate'] <= end)]

# New way
filtered = duckdb_data_service.get_regional_prices(start, end)
```

### Pattern 2: Aggregated Data
```python
# Old way
df = pd.read_parquet('scada30.parquet')
merged = df.merge(duid_info, on='duid')
grouped = merged.groupby(['settlementdate', 'fuel_type'])['scadavalue'].sum()

# New way (SQL handles this internally)
grouped = duckdb_data_service.get_generation_by_fuel(start, end)
```

### Pattern 3: Custom Queries
```python
# For complex queries not covered by existing methods
query = """
    SELECT * FROM generation_30min 
    WHERE duid = ? AND settlementdate BETWEEN ? AND ?
"""
result = duckdb_data_service.conn.execute(query, [duid, start, end]).df()
```

## Testing Strategy

1. **Memory Test**: Verify memory usage stays under 100MB
2. **Performance Test**: Ensure queries complete in < 500ms
3. **Data Accuracy**: Compare results with current implementation
4. **User Experience**: No noticeable lag when changing date ranges

## Rollback Plan

If issues arise:
1. The old adapters can coexist with new ones
2. Use a feature flag to switch between implementations
3. Both approaches read the same parquet files

## Benefits After Migration

- **Memory**: 21GB → 56MB (99.7% reduction)
- **Startup**: 10-15 seconds → instant
- **Queries**: Still fast (10-100ms)
- **Maintenance**: Simpler data service
- **Scalability**: Can handle more concurrent users

## Next Steps

1. Review this plan
2. Start with one adapter (recommend `generation_adapter.py`)
3. Test thoroughly
4. Proceed with remaining adapters
5. Remove old pandas-based code once stable