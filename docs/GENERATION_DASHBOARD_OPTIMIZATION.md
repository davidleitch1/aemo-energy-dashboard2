# Generation Dashboard Performance Issue

**Date**: July 19, 2025  
**Issue**: Generation by fuel tab is slow when viewing "All Available Data"

## Problem Analysis

### Current Behavior
When "All Available Data" is selected:
1. **38,427,783 records** are loaded from DuckDB (entire 5.5 years of 30-min data)
2. All records are loaded into a pandas DataFrame
3. Multiple pivot operations are performed in memory
4. This causes significant lag and memory usage

### Root Cause
The generation dashboard (`gen_dash.py`) hasn't been refactored to use the hybrid query manager. It's using DuckDB to load data but then processing everything in pandas memory.

## Immediate Optimization Solutions

### Option 1: Pre-aggregate in DuckDB (Quick Fix) ✅
Modify the generation query to aggregate by fuel type in SQL instead of loading raw DUID data:

```python
def load_generation_data_aggregated(start_date, end_date, resolution='auto'):
    """Load generation data pre-aggregated by fuel type"""
    
    query = f"""
    SELECT 
        g.settlementdate,
        d.Fuel as fuel,
        SUM(g.scadavalue) as scadavalue
    FROM generation_{resolution} g
    JOIN duid_mapping d ON g.duid = d.DUID
    WHERE g.settlementdate >= '{start_date}'
    AND g.settlementdate <= '{end_date}'
    GROUP BY g.settlementdate, d.Fuel
    ORDER BY g.settlementdate, d.Fuel
    """
    
    return duckdb_data_service.conn.execute(query).df()
```

This would reduce 38M records to ~100K records (10 fuel types × 10K time periods).

### Option 2: Implement Sampling for Long Ranges
For ranges > 1 year, sample the data:
- Show daily averages instead of 30-min data
- Or show every Nth point

### Option 3: Full Refactor with Hybrid Query Manager (Best Long-term)
Like we did for Price Analysis and Station Analysis:
1. Create views for fuel aggregation
2. Use hybrid query manager with caching
3. Load data on-demand

## Quick Implementation Plan

### Step 1: Create Aggregated View in DuckDB
Add to `duckdb_views.py`:

```sql
CREATE OR REPLACE VIEW generation_by_fuel_30min AS
SELECT 
    g.settlementdate,
    d.Fuel as fuel,
    d.Region as region,
    SUM(g.scadavalue) as total_mw,
    COUNT(DISTINCT g.duid) as unit_count
FROM generation_30min g
JOIN duid_mapping d ON g.duid = d.DUID
GROUP BY g.settlementdate, d.Fuel, d.Region
```

### Step 2: Modify Generation Adapter
Update `generation_adapter_duckdb.py` to use the aggregated view when appropriate:

```python
def load_generation_data(..., aggregate_by_fuel=False):
    if aggregate_by_fuel:
        # Use pre-aggregated view
        table = f'generation_by_fuel_{resolution}'
    else:
        # Use raw data
        table = f'generation_{resolution}'
```

### Step 3: Update gen_dash.py
Modify `load_generation_data()` to request aggregated data:

```python
def load_generation_data(self):
    # For long date ranges, load pre-aggregated data
    if (end_time - start_time).days > 365:
        df = load_generation_data(
            start_date=start_time,
            end_date=end_time,
            resolution='auto',
            aggregate_by_fuel=True  # New parameter
        )
    else:
        # Normal loading for shorter ranges
        ...
```

## Expected Results

### Performance Improvements
- **Data reduction**: 38M → 100K records (99.7% reduction)
- **Load time**: 60s → 2-3s
- **Memory usage**: Minimal increase instead of GB+
- **UI responsiveness**: Immediate instead of frozen

### Trade-offs
- Lose DUID-level detail for long ranges (acceptable for overview)
- Need to handle edge cases (unknown DUIDs, missing fuel types)

## Testing Plan

1. Test with various date ranges
2. Verify fuel totals match current implementation
3. Check memory usage stays reasonable
4. Ensure smooth UI interaction

## Recommendation

Implement **Option 1** as an immediate fix. This requires minimal code changes but will dramatically improve performance for long date ranges. Later, consider full refactor with hybrid query manager for consistency.