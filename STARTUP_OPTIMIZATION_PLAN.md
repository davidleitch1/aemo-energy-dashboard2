# Dashboard Startup Optimization Plan

*Date: July 19, 2025*

## Problem Analysis

The dashboard experiences an 8-9 second startup delay due to:

1. **SharedDataService initialization** (when `USE_DUCKDB=false`):
   - Loads 41M+ records into memory from parquet files
   - `create_enriched_data`: ~4 seconds (merges 38M records with DUID mapping)
   - `precalculate_aggregations`: ~3.5 seconds (creates aggregated views)
   - Total memory usage: 21GB

2. **Inconsistent DuckDB defaults**:
   - `adapter_selector.py`: Defaults to `'true'` (DuckDB mode)
   - `data_service/__init__.py`: Defaults to `'false'` (pandas mode)
   - This inconsistency causes mixed behavior

## Immediate Fix (Phase 1) 🚨

### 1. Align DuckDB Defaults
Change `data_service/__init__.py` to default to DuckDB mode:

```python
# Line 11 in src/data_service/__init__.py
USE_DUCKDB = os.getenv('USE_DUCKDB', 'true').lower() == 'true'  # Change 'false' to 'true'
```

**Benefits**:
- Instant startup (< 1 second)
- 56MB memory usage vs 21GB
- No pre-loading of data
- Queries execute on-demand

### 2. Update Startup Scripts
Ensure all startup methods use DuckDB:

```bash
# start_dashboard.sh
export USE_DUCKDB=true
python src/aemo_dashboard/generation/gen_dash.py

# run_dashboard_duckdb.py (already correct)
os.environ['USE_DUCKDB'] = 'true'
```

## Long-term Optimizations (Phase 2)

### 1. Lazy Loading for SharedDataService
If pandas mode must be supported, implement lazy loading:

```python
class SharedDataService:
    def __init__(self):
        self._initialized = False
        self._generation_30min = None
        self._enriched_data = None
        # Don't load data at startup!
    
    @property
    def generation_30min(self):
        if self._generation_30min is None:
            self._load_generation_data()
        return self._generation_30min
    
    @property
    def generation_enriched(self):
        if self._enriched_data is None:
            self._create_enriched_data()
        return self._enriched_data
```

### 2. Progressive Loading UI
Add loading indicators during startup:

```python
# In gen_dash.py
loading_indicator = pn.indicators.LoadingSpinner(
    value=True, 
    size=100,
    name="Initializing Dashboard..."
)

# Update during initialization
loading_indicator.name = "Loading generation data..."
loading_indicator.name = "Creating visualizations..."
loading_indicator.value = False  # Hide when done
```

### 3. Background Initialization
Load non-critical data in background:

```python
import threading

def background_init():
    # Load aggregations in background
    threading.Thread(
        target=lambda: data_service._precalculate_aggregations(),
        daemon=True
    ).start()
```

### 4. Cache Startup Data
Cache enriched data between sessions:

```python
CACHE_FILE = "cache/startup_data.parquet"

def load_or_create_enriched_data():
    if os.path.exists(CACHE_FILE):
        # Check if cache is fresh (< 1 hour old)
        if time.time() - os.path.getmtime(CACHE_FILE) < 3600:
            return pd.read_parquet(CACHE_FILE)
    
    # Create and cache
    enriched = create_enriched_data()
    enriched.to_parquet(CACHE_FILE)
    return enriched
```

## Implementation Priority

### Phase 1: Immediate Fix (Today)
1. ✅ Change `data_service/__init__.py` default to `'true'`
2. ✅ Verify all startup scripts use DuckDB mode
3. ✅ Test startup time < 1 second

### Phase 2: UI Improvements (Next Week)
1. Add loading spinner during initialization
2. Show progress messages
3. Implement tab-based lazy loading

### Phase 3: Legacy Support (If Needed)
1. Implement lazy properties for SharedDataService
2. Add background loading for aggregations
3. Cache frequently used data

## Testing Plan

### Performance Targets
- **Startup time**: < 1 second (DuckDB mode)
- **Memory usage**: < 200MB initial
- **First interaction**: < 2 seconds
- **Tab switching**: < 1 second

### Test Commands
```bash
# Test startup time
time .venv/bin/python run_dashboard_duckdb.py

# Monitor memory usage
ps aux | grep python | grep dashboard

# Test with pandas mode (if needed)
USE_DUCKDB=false .venv/bin/python src/aemo_dashboard/generation/gen_dash.py
```

## Expected Results

### Before Optimization
- Startup: 8-9 seconds
- Memory: 21GB (pandas mode)
- User sees blank screen during load

### After Optimization
- Startup: < 1 second
- Memory: 56-200MB
- User sees immediate UI with loading indicators

## Rollback Plan

If issues occur:
```bash
# Revert to pandas mode
export USE_DUCKDB=false
# Or change defaults back in code
```

## Summary

The primary issue is that the dashboard defaults to pandas mode in some places, causing the slow startup. By ensuring DuckDB mode is used consistently, we can achieve instant startup with minimal memory usage. The long-term optimizations are only needed if pandas mode must be maintained for compatibility.