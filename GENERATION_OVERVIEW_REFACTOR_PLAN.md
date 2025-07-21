# Generation Overview Refactoring Plan

**Date**: July 19, 2025  
**Purpose**: Refactor generation_overview.py to use hybrid query manager

## Current State Analysis

### Module Characteristics
- **Fixed time window**: Always shows last 24 hours of data
- **Simple data needs**: Just loads and displays generation by fuel type
- **Already optimized**: Only loads 24 hours of data (288 records)
- **No complex calculations**: Just aggregation by fuel type
- **Integration**: Used by NEM Overview tab

### Current Implementation
```python
def load_generation_data():
    # Loads last 24 hours from parquet file
    gen_data = pd.read_parquet(gen_file)
    # Filters to last 24 hours
    # Falls back to last 7 days if no recent data
```

### Memory Impact
- **Current**: ~50-100MB (only 24 hours of data)
- **Not a major bottleneck**: Already constrained to recent data
- **Opportunity**: Can still benefit from DuckDB efficiency

## Refactoring Decision

### Option 1: Minimal Refactor (Recommended) ✅
Since this module is already optimized:
1. Add DuckDB query for last 24 hours only
2. Keep existing fallback logic
3. Minimal code changes
4. Low risk, moderate benefit

### Option 2: Full Refactor (Not Recommended)
Would involve:
1. Creating specialized views for 24-hour windows
2. Complex date handling in SQL
3. High effort for minimal gain

## Implementation Plan (Minimal Refactor)

### Step 1: Update Data Loading Functions

#### 1.1 Modify `load_generation_data()`
```python
def load_generation_data():
    """Load generation data for the last 24 hours using DuckDB"""
    try:
        from ..shared.hybrid_query_manager import HybridQueryManager
        query_manager = HybridQueryManager()
        
        end_time = datetime.now()
        start_time = end_time - timedelta(hours=24)
        
        # Query last 24 hours directly
        query = f"""
        SELECT settlementdate, duid, scadavalue
        FROM generation_5min
        WHERE settlementdate >= '{start_time.strftime('%Y-%m-%d %H:%M:%S')}'
        AND settlementdate <= '{end_time.strftime('%Y-%m-%d %H:%M:%S')}'
        ORDER BY settlementdate
        """
        
        gen_data = query_manager.query_with_progress(query)
        
        # Apply existing fallback logic if needed
        if len(gen_data) == 0:
            # Try last 7 days fallback
            start_time_7d = end_time - timedelta(days=7)
            query_7d = f"""
            SELECT settlementdate, duid, scadavalue
            FROM generation_5min
            WHERE settlementdate >= '{start_time_7d.strftime('%Y-%m-%d %H:%M:%S')}'
            AND settlementdate <= '{end_time.strftime('%Y-%m-%d %H:%M:%S')}'
            ORDER BY settlementdate DESC
            LIMIT 288
            """
            gen_data = query_manager.query_with_progress(query_7d)
        
        return gen_data
```

#### 1.2 Update `load_transmission_data()`
Similar pattern - query last 24 hours from DuckDB

#### 1.3 Keep `load_rooftop_solar_data()` as-is
Already uses the adapter which handles DuckDB

### Step 2: Update Data Preparation

The `prepare_generation_for_stacking()` function can remain mostly unchanged since it works with DataFrames returned by DuckDB.

### Step 3: Integration Points

#### 3.1 Dashboard Integration
The module already accepts a `dashboard_instance` parameter and tries to use processed data first. This should continue to work.

#### 3.2 Standalone Usage
When used standalone, it will use the DuckDB queries directly.

## Testing Plan

### 1. Unit Tests
```python
def test_load_24hour_generation():
    """Test loading last 24 hours of generation data"""
    data = load_generation_data()
    assert len(data) <= 288  # Max 24 hours of 5-min data
    assert 'settlementdate' in data.columns
    assert 'duid' in data.columns
    assert 'scadavalue' in data.columns
```

### 2. Integration Tests
- Test with dashboard integration
- Test standalone usage
- Test fallback scenarios

### 3. Performance Tests
- Measure memory usage (should stay <100MB)
- Query time (should be <1s)

## Benefits

### Performance
- **Query speed**: DuckDB queries will be faster
- **Memory**: Slight reduction (avoid pandas read_parquet overhead)
- **Consistency**: Uses same data access pattern as other modules

### Maintenance
- **Unified approach**: All modules use hybrid query manager
- **Easier debugging**: Consistent logging and error handling
- **Future-proof**: Ready for any DuckDB optimizations

## Risks and Mitigation

### Low Risk
- Module already works with constrained data
- Fallback logic preserved
- No complex calculations affected

### Mitigation
- Keep original file as backup
- Test thoroughly with edge cases
- Monitor performance in production

## Timeline

- Implementation: 1-2 hours
- Testing: 1 hour
- Total: 2-3 hours

## Decision

**Recommendation**: Proceed with minimal refactor. While this module is already optimized, converting to DuckDB will provide consistency across the codebase and minor performance improvements.

The main benefits are:
1. Consistent data access pattern
2. Slightly better performance
3. Easier maintenance
4. Future optimization opportunities

## Next Steps

1. Implement DuckDB queries for 24-hour data
2. Test with existing dashboard
3. Verify fallback scenarios work
4. Update documentation