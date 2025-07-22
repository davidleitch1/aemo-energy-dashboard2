# Safari Refresh Issue - Root Cause Analysis

## Problem Summary
When implementing `defer_load=True` to optimize dashboard startup, Safari browser refreshes hang and become unresponsive.

## Root Cause Analysis

### 1. Performance Bottleneck Identified
From profiling the price components:
```
Create price chart: 3.215s (36.2%) - matplotlib is slow
Create price section: 4.390s (49.4%) - re-runs everything
Data loading: 0.451s (5.1%) - actually fast!
```

### 2. Why defer_load Breaks Safari Refresh
- `pn.panel(func, defer_load=True)` creates a special deferred component
- When Safari refreshes, it tries to reconnect the WebSocket
- The deferred components don't serialize/deserialize properly during reconnection
- Panel's server waits for component initialization that never completes
- Result: Browser hangs

### 3. The Real Problem
We were trying to defer the wrong thing. The slow part is matplotlib chart rendering, not data loading.

## Recommended Solution

### Option 1: Replace Matplotlib with HoloViews (Best)
```python
def create_price_chart(prices):
    """Create price chart using HoloViews (much faster)"""
    import holoviews as hv
    
    # Convert to long format for hvplot
    last_48h = prices.last('48h').reset_index()
    last_48h_long = last_48h.melt(
        id_vars=['SETTLEMENTDATE'], 
        var_name='Region', 
        value_name='Price'
    )
    
    # Create hvplot chart (10x faster than matplotlib)
    chart = last_48h_long.hvplot.line(
        x='SETTLEMENTDATE', 
        y='Price', 
        by='Region',
        width=550, 
        height=250,
        title='Spot Prices - Last 48 Hours',
        ylabel='Price ($/MWh)',
        legend='top_right'
    )
    
    return pn.pane.HoloViews(chart, sizing_mode='fixed')
```

### Option 2: Cache Matplotlib Figures
```python
# Cache the matplotlib figure
_chart_cache = {'fig': None, 'last_update': None}

def create_price_chart(prices):
    current_time = prices.index[-1] if not prices.empty else None
    
    # Use cached chart if less than 1 minute old
    if (_chart_cache['fig'] is not None and 
        _chart_cache['last_update'] is not None and
        current_time - _chart_cache['last_update'] < pd.Timedelta(minutes=1)):
        return pn.pane.Matplotlib(_chart_cache['fig'])
    
    # Create new chart
    fig = create_matplotlib_chart(prices)
    _chart_cache['fig'] = fig
    _chart_cache['last_update'] = current_time
    
    return pn.pane.Matplotlib(fig)
```

### Option 3: Pre-render Charts
Generate charts in the background and serve as static images, updating every 5 minutes.

## Why NOT to Use defer_load

1. **Browser Compatibility**: Issues with Safari (and potentially other browsers)
2. **WebSocket Complexity**: Adds complexity to Panel's WebSocket reconnection
3. **Wrong Target**: We were deferring the wrong operations
4. **Marginal Benefit**: Data loading is already fast (0.45s)

## Immediate Action Plan

1. **Remove defer_load**: Already done ✅
2. **Replace Matplotlib with HoloViews**: 10x faster rendering
3. **Add pn.cache to expensive operations**: Cache computed results
4. **Optimize chart updates**: Only update when data changes

## Performance Impact

Current (with matplotlib):
- Chart creation: 3.2s
- Total section: 4.4s

Expected (with HoloViews):
- Chart creation: ~0.3s
- Total section: ~1.2s

This gives us 3.2s improvement without any defer_load complexity!