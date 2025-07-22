# Station Analysis Refactoring Plan

**Date**: July 19, 2025  
**Purpose**: Refactor StationAnalysisMotor to use hybrid query manager for memory efficiency

## Current State Analysis

### Memory Issues
- Loads all generation and price data into memory on demand
- Uses pandas merge operations creating multiple copies
- No caching mechanism for repeated queries
- Memory usage grows with date range

### Current Architecture
```python
StationAnalysisMotor:
  - __init__(): Initializes empty containers
  - load_data(): Loads DUID mapping only
  - load_data_for_date_range(): Loads gen + price data
  - standardize_columns(): Fixes column names
  - integrate_data(): Merges gen + DUID + price
  - filter_station_data(): Filters to specific DUID(s)
  - calculate_time_of_day_averages(): Time analysis
  - calculate_performance_metrics(): Performance stats
```

### Key Differences from Price Analysis
1. **Two-step loading**: First loads metadata, then loads data for date range
2. **Station filtering**: Filters to specific DUID(s) after integration
3. **Time-based analysis**: Calculates hourly/time-of-day patterns
4. **Already uses on-demand loading**: Better starting point than price analysis

## Refactoring Plan

### Phase 1: Integrate Hybrid Query Manager

#### 1.1 Update `__init__` method
```python
def __init__(self):
    self.query_manager = HybridQueryManager(cache_size_mb=100, cache_ttl=300)
    self.duid_mapping = None
    self.station_data = None  # Filtered data cache
    self.data_available = False
    view_manager.create_all_views()
```

#### 1.2 Keep `load_data()` as-is
- Already only loads DUID mapping
- This is the right approach

#### 1.3 Replace `load_data_for_date_range()` + `integrate_data()`
```python
def load_integrated_data(self, start_date, end_date, duid=None):
    """Load integrated data using hybrid query manager"""
    # Use query manager to get integrated data
    if duid:
        # Query specific DUID
        query = get_station_data_query(duid, start_date, end_date)
        return self.query_manager.query_with_progress(query)
    else:
        # Query all data
        return self.query_manager.query_integrated_data(start_date, end_date)
```

#### 1.4 Remove `standardize_columns()`
- DuckDB views handle standardization

### Phase 2: Optimize Station-Specific Queries

#### 2.1 Create DuckDB Views for Station Analysis
```sql
-- Station time series view
CREATE VIEW station_time_series AS
SELECT 
    g.settlementdate,
    g.duid,
    g.scadavalue,
    p.rrp as price,
    g.scadavalue * p.rrp * 0.0833 as revenue_5min,
    d.station_name,
    d.capacity_mw
FROM generation_5min g
JOIN duid_mapping d ON g.duid = d.duid
JOIN prices_5min p ON g.settlementdate = p.settlementdate 
    AND d.region = p.regionid
WHERE g.duid = ?  -- Parameterized for specific station
```

#### 2.2 Update `filter_station_data()`
```python
def filter_station_data(self, duid_or_duids, start_date, end_date):
    """Query station data directly from DuckDB"""
    duids = [duid_or_duids] if isinstance(duid_or_duids, str) else duid_or_duids
    
    # Build query for specific DUIDs
    query = f"""
    SELECT * FROM station_time_series
    WHERE duid IN ({','.join(['?']*len(duids))})
    AND settlementdate >= ?
    AND settlementdate <= ?
    ORDER BY settlementdate
    """
    
    # Use hybrid query manager
    self.station_data = self.query_manager.query(
        query, 
        params=duids + [start_date, end_date]
    )
```

### Phase 3: Optimize Calculations

#### 3.1 Move Time-of-Day Calculations to SQL
```sql
-- Time of day averages view
CREATE VIEW time_of_day_averages AS
SELECT 
    duid,
    EXTRACT(hour FROM settlementdate) as hour,
    AVG(scadavalue) as avg_generation,
    AVG(price) as avg_price,
    COUNT(*) as data_points
FROM station_time_series
GROUP BY duid, hour
```

#### 3.2 Move Performance Metrics to SQL
```sql
-- Station performance metrics
CREATE VIEW station_metrics AS
SELECT 
    duid,
    MIN(settlementdate) as start_date,
    MAX(settlementdate) as end_date,
    SUM(scadavalue * 0.0833) as total_generation_mwh,
    SUM(revenue_5min) as total_revenue,
    AVG(scadavalue) as avg_generation_mw,
    MAX(scadavalue) as max_generation_mw,
    AVG(price) as avg_price,
    COUNT(*) as intervals
FROM station_time_series
GROUP BY duid
```

### Phase 4: UI Integration

#### 4.1 Minimal Changes to UI
- Remove calls to `standardize_columns()`
- Update data loading sequence
- Keep all visualization code as-is

#### 4.2 Update Data Loading Flow
```python
# Old flow:
motor.load_data()  # Load DUID mapping
motor.load_data_for_date_range(start, end)  # Load raw data
motor.integrate_data()  # Merge data
motor.filter_station_data(duid)  # Filter to station

# New flow:
motor.load_data()  # Load DUID mapping (unchanged)
motor.filter_station_data(duid, start, end)  # Direct query
```

## Implementation Steps

### Step 1: Create Station-Specific Views
1. Add views to `duckdb_views.py`
2. Test views with sample queries
3. Verify performance

### Step 2: Refactor Motor Core
1. Add hybrid query manager to `__init__`
2. Create `load_integrated_data()` method
3. Update `filter_station_data()` to use queries
4. Remove `standardize_columns()`

### Step 3: Optimize Calculations
1. Convert `calculate_time_of_day_averages()` to use SQL
2. Convert `calculate_performance_metrics()` to use SQL
3. Test calculation accuracy

### Step 4: Update UI
1. Remove `standardize_columns()` calls
2. Update data loading sequence
3. Test all functionality

### Step 5: Performance Testing
1. Compare memory usage (target: <200MB)
2. Test query performance
3. Verify cache effectiveness

## Testing Plan

### Unit Tests
1. Test DUID mapping loading
2. Test station data queries
3. Test calculation methods
4. Test error handling

### Integration Tests
1. Test full workflow with UI
2. Test multiple station selection
3. Test date range changes
4. Test performance with large date ranges

### Performance Tests
1. Memory usage with 1 year of data
2. Query time for single station
3. Cache hit rates
4. UI responsiveness

## Risk Mitigation

### Backward Compatibility
- Keep original file as backup
- Maintain same public API
- Return same data structures

### Data Integrity
- Verify calculations match original
- Test edge cases (missing data, gaps)
- Validate against known results

### Performance Risks
- Start with conservative cache size
- Monitor query performance
- Add progress indicators for long queries

## Success Criteria

1. **Memory Usage**: <200MB for typical usage
2. **Query Performance**: <1s for month of single station
3. **Cache Effectiveness**: >80% hit rate for repeated queries
4. **UI Responsiveness**: No blocking operations
5. **Feature Parity**: All existing features work

## Timeline

- Step 1-2: 2 hours (core refactoring)
- Step 3: 1 hour (calculation optimization)
- Step 4: 1 hour (UI updates)
- Step 5: 1 hour (testing)

Total: ~5 hours