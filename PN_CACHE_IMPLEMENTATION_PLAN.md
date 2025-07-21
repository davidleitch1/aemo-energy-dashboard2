# Panel Cache Implementation Plan

## Overview
Implement `pn.cache` decorator to cache expensive plot creation operations, targeting the 14-second chart rendering bottleneck.

## Expected Benefits
- **140x faster** for cached views (14s → 0.1s)
- **Shared across all users** - one user's cache benefits everyone
- **No infrastructure changes** - just decorators
- **Easy rollback** - just remove decorators

## Implementation Strategy

### Phase 1: Identify Caching Targets

#### High-Priority Targets (>5s creation time)
1. **Generation Dashboard** (`gen_dash.py`)
   - `create_generation_by_fuel()` - 14+ seconds
   - `create_capacity_utilization()` - 3-5 seconds
   - `create_transmission_plot()` - 2-3 seconds

2. **Price Analysis** (`price_analysis_ui.py`)
   - Average price by hour plots
   - Price distribution plots
   - Hierarchical data tables

3. **Station Analysis** (`station_analysis_ui.py`)
   - Revenue calculations with plots
   - Performance comparisons

#### Medium-Priority Targets (1-5s)
4. **NEM Dashboard** (`nem_dash_tab.py`)
   - Current spot price displays
   - Generation overview
   - Renewable gauge

5. **Penetration Tab** (`penetration_tab.py`)
   - VRE production charts
   - Thermal vs renewables

### Phase 2: Refactoring Pattern

#### Current Code Pattern
```python
def create_generation_by_fuel(self):
    # Data loading
    data = self.load_generation_data()
    
    # Direct plot creation (expensive)
    plot = data[fuel_types].hvplot.area(
        x='settlementdate',
        stacked=True,
        # ... many parameters
    )
    return plot
```

#### Refactored Pattern
```python
# Extract plot creation to standalone cacheable function
@pn.cache(max_items=100, policy='LRU', ttl=3600)  # 1 hour TTL
def _create_generation_plot_cached(
    data_json: str,  # Serialize DataFrame to JSON for cache key
    fuel_types: tuple,  # Use tuple instead of list for hashability
    region: str,
    time_range: str,
    width: int,
    height: int
) -> hv.DynamicMap:
    """Cached plot creation function"""
    # Deserialize data
    data = pd.read_json(data_json)
    data['settlementdate'] = pd.to_datetime(data['settlementdate'])
    
    # Create plot (expensive operation)
    plot = data[list(fuel_types)].hvplot.area(
        x='settlementdate',
        stacked=True,
        width=width,
        height=height,
        # ... other parameters
    )
    return plot

def create_generation_by_fuel(self):
    # Data loading (fast with DuckDB)
    data = self.load_generation_data()
    
    # Prepare cache-friendly parameters
    data_json = data.to_json(date_format='iso')
    fuel_types = tuple(self.fuel_types)  # Convert to hashable
    
    # Call cached function
    plot = _create_generation_plot_cached(
        data_json=data_json,
        fuel_types=fuel_types,
        region=self.region_selector.value,
        time_range=self.time_range_selector.value,
        width=1000,
        height=400
    )
    return plot
```

### Phase 3: Cache Key Optimization

#### Problem: DataFrames aren't hashable
**Solution**: Create efficient cache keys

```python
def _create_cache_key(df: pd.DataFrame, prefix: str) -> str:
    """Create efficient cache key from DataFrame"""
    # Use data characteristics instead of full data
    key_parts = [
        prefix,
        str(len(df)),
        str(df.index[0]) if len(df) > 0 else 'empty',
        str(df.index[-1]) if len(df) > 0 else 'empty',
        str(hash(tuple(df.columns))),
        str(df.iloc[0].sum()) if len(df) > 0 else '0'  # Data fingerprint
    ]
    return '|'.join(key_parts)

# Alternative: Hash subset of data
def _create_data_hash(df: pd.DataFrame) -> str:
    """Create hash from DataFrame subset"""
    # Sample first, last, and middle rows
    sample_size = min(10, len(df))
    sample_indices = np.linspace(0, len(df)-1, sample_size, dtype=int)
    sample = df.iloc[sample_indices]
    
    # Create hash from sample
    return hashlib.md5(
        sample.to_json().encode()
    ).hexdigest()
```

### Phase 4: Implementation Order

#### Week 1: High-Impact, Low-Risk
1. **Generation Dashboard - Main Plot**
   ```python
   # gen_dash.py
   @pn.cache(max_items=50, policy='LRU', ttl=300)  # 5 min TTL
   def _create_generation_plot_cached(...)
   ```

2. **Test and Measure**
   - Add logging for cache hits/misses
   - Measure performance improvement
   - Monitor memory usage

#### Week 2: Expand Coverage
3. **Price Analysis Plots**
   ```python
   # price_analysis_ui.py
   @pn.cache(max_items=30, policy='LRU', ttl=600)
   def _create_price_distribution_cached(...)
   
   @pn.cache(max_items=30, policy='LRU', ttl=600)
   def _create_hourly_average_cached(...)
   ```

4. **Station Analysis**
   ```python
   # station_analysis_ui.py
   @pn.cache(max_items=20, policy='LRU', ttl=1800)  # 30 min
   def _create_revenue_plot_cached(...)
   ```

#### Week 3: Complete Coverage
5. **NEM Dashboard Components**
6. **Penetration Tab**
7. **Remaining visualizations**

### Phase 5: Cache Management

#### Configuration
```python
# shared/cache_config.py
CACHE_CONFIG = {
    'generation_plot': {
        'max_items': 50,
        'policy': 'LRU',
        'ttl': 300  # 5 minutes
    },
    'price_analysis': {
        'max_items': 30,
        'policy': 'LRU', 
        'ttl': 600  # 10 minutes
    },
    'station_analysis': {
        'max_items': 20,
        'policy': 'LRU',
        'ttl': 1800  # 30 minutes
    }
}

# Environment variable override
ENABLE_PN_CACHE = os.getenv('ENABLE_PN_CACHE', 'true').lower() == 'true'
```

#### Monitoring
```python
# Add cache statistics
def log_cache_stats():
    """Log cache performance metrics"""
    if hasattr(pn.state, 'cache'):
        stats = {
            'size': len(pn.state.cache),
            'hits': getattr(pn.state.cache, 'hits', 0),
            'misses': getattr(pn.state.cache, 'misses', 0)
        }
        logger.info(f"Cache stats: {stats}")
```

### Phase 6: Testing Strategy

#### 1. Performance Tests
```python
# test_cache_performance.py
def test_generation_plot_cache():
    # First call - cache miss
    start = time.time()
    plot1 = create_generation_plot(data, params)
    time1 = time.time() - start
    
    # Second call - cache hit
    start = time.time()
    plot2 = create_generation_plot(data, params)
    time2 = time.time() - start
    
    assert time2 < time1 * 0.1  # 10x faster
    assert plot1 == plot2  # Same result
```

#### 2. Multi-User Tests
- Simulate 4 users accessing same views
- Verify cache sharing works
- Measure aggregate performance improvement

#### 3. Memory Tests
- Monitor memory growth with cache
- Verify LRU eviction works
- Check for memory leaks

### Rollback Plan

#### Quick Disable
```python
# Option 1: Environment variable
ENABLE_PN_CACHE=false

# Option 2: Conditional decorator
def conditional_cache(**cache_kwargs):
    def decorator(func):
        if ENABLE_PN_CACHE:
            return pn.cache(**cache_kwargs)(func)
        return func
    return decorator

# Usage
@conditional_cache(max_items=50, policy='LRU')
def _create_plot_cached(...):
    ...
```

#### Complete Removal
- Simply remove `@pn.cache` decorators
- No other code changes needed

## Success Metrics

### Performance
- ✅ Generation tab load time: 14s → <1s for cached views
- ✅ Average response time: 11.5s → <3s across users
- ✅ Cache hit rate > 60% after warmup

### User Experience
- ✅ Instant tab switching for recently viewed data
- ✅ Snappy response for common operations
- ✅ No UI freezing during chart creation

### System Health
- ✅ Memory usage increase < 500MB
- ✅ No memory leaks after 24 hours
- ✅ Cache eviction working properly

## Risk Mitigation

### Potential Issues
1. **Stale Data**: Mitigated by short TTL (5-30 min)
2. **Memory Growth**: Mitigated by max_items limit
3. **Cache Invalidation**: Use data characteristics in cache key
4. **Serialization Overhead**: Only cache expensive operations

### Monitoring
- Add cache statistics to dashboard footer
- Log cache performance metrics
- Alert on low hit rates

## Next Steps

1. Start with generation plot (highest impact)
2. Measure improvement
3. Gradually add more cached operations
4. Monitor system health
5. Tune cache parameters based on usage patterns