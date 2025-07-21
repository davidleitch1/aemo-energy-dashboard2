# Dashboard Creation Optimization Plan

## Current State
- **Dashboard creation time: 5.67 seconds (70.2% of startup)**
- **Total startup time: 8.07 seconds**
- **Target: Reduce dashboard creation to < 2 seconds**

## Optimization Strategy

### Phase 1: Analyze Current Dashboard Creation Process

#### 1.1 Profile Component Creation
- Measure time for each tab creation
- Identify which components take longest
- Find unnecessary initialization work

#### 1.2 Current Issues to Investigate
- All tabs are created upfront (even non-visible ones)
- Complex widgets may be doing unnecessary work on init
- Data might be loaded multiple times
- Callbacks might be triggering during initialization

### Phase 2: Implement True Lazy Loading

#### 2.1 Defer Non-Visible Tab Creation
```python
# Current approach (suspected):
tabs = pn.Tabs(
    ('Today', create_today_tab()),      # Created immediately
    ('Generation', create_gen_tab()),    # Created immediately
    ('Price Analysis', create_price_tab()),  # Created immediately
    ('Station Analysis', create_station_tab()),  # Created immediately
)

# Optimized approach:
tabs = pn.Tabs()
tabs.append(('Today', create_today_tab()))  # Create only Today tab
tabs.append(('Generation', pn.pane.HTML("Loading...")))  # Placeholder
tabs.append(('Price Analysis', pn.pane.HTML("Loading...")))  # Placeholder
tabs.append(('Station Analysis', pn.pane.HTML("Loading...")))  # Placeholder

# Create other tabs on first access
@pn.depends(tabs.param.active)
def load_tab_on_demand(active_tab):
    if active_tab == 1 and is_placeholder(tabs[1]):
        tabs[1] = create_gen_tab()
```

#### 2.2 Lazy Widget Initialization
- Create widgets without data binding initially
- Bind data only when tab becomes visible
- Use lightweight placeholders

### Phase 3: Optimize Component Creation

#### 3.1 Defer Heavy Operations
- **Plots**: Create plot containers, render data later
- **Tables**: Show loading state, populate on display
- **Gauges**: Initialize with default values, update async
- **Layouts**: Use simpler initial layouts

#### 3.2 Parallel Initialization
```python
# Use threading for independent components
import concurrent.futures

with concurrent.futures.ThreadPoolExecutor() as executor:
    future_price_display = executor.submit(create_price_display)
    future_gauge = executor.submit(create_renewable_gauge)
    future_overview = executor.submit(create_generation_overview)
    
    # Collect results
    price_display = future_price_display.result()
    gauge = future_gauge.result()
    overview = future_overview.result()
```

#### 3.3 Component Pooling
- Pre-create generic components
- Reuse and reconfigure instead of creating new
- Cache component templates

### Phase 4: Optimize Panel/Bokeh Usage

#### 4.1 Minimize Initial DOM
- Start with minimal DOM structure
- Add complexity progressively
- Use CSS display:none instead of removing elements

#### 4.2 Defer Bokeh Model Creation
```python
# Instead of creating full Bokeh models upfront
plot = data.hvplot.line()  # This creates Bokeh models immediately

# Use lazy evaluation
def create_plot():
    return data.hvplot.line()

plot_pane = pn.pane.HoloViews(create_plot, lazy=True)
```

#### 4.3 Optimize Callbacks
- Defer callback registration
- Use debouncing/throttling
- Batch updates

### Phase 5: Data Loading Optimization

#### 5.1 Shared Data Loading
- Load common data once, share across components
- Use a data service pattern
- Implement request coalescing

#### 5.2 Progressive Data Loading
```python
# Load minimal data for initial display
initial_data = load_last_hour()
display_initial(initial_data)

# Load full data in background
pn.state.add_periodic_callback(
    lambda: load_full_data_async(),
    period=100,
    count=1
)
```

### Phase 6: UI/UX Improvements

#### 6.1 Perceived Performance
- Show dashboard shell immediately
- Use skeleton screens
- Progressive enhancement
- Optimistic UI updates

#### 6.2 Loading States
```python
# Better loading feedback
loading_template = pn.template.MaterialTemplate(
    title="NEM Energy Dashboard",
    sidebar=[
        pn.pane.HTML("Loading navigation...")
    ],
    main=[
        pn.Column(
            pn.indicators.LoadingSpinner(value=True, size=50),
            pn.pane.HTML("<h3>Initializing dashboard components...</h3>"),
            sizing_mode='stretch_both',
            align='center'
        )
    ]
)
```

### Phase 7: Caching Strategies

#### 7.1 Component Caching
```python
# Cache expensive component creation
@lru_cache(maxsize=10)
def create_cached_plot(data_hash, plot_type):
    return create_plot(data, plot_type)
```

#### 7.2 Session Persistence
- Save component state between sessions
- Restore previous state quickly
- Use browser localStorage for preferences

## Implementation Priority

1. **High Impact, Low Risk** (Do First)
   - True lazy tab loading
   - Defer non-Today tab creation
   - Simple loading indicators

2. **High Impact, Medium Risk** (Do Second)
   - Component pooling
   - Parallel initialization
   - Progressive data loading

3. **Medium Impact, Low Risk** (Do Third)
   - Better loading states
   - Skeleton screens
   - Optimistic updates

4. **Experimental** (Test Carefully)
   - Bokeh model deferral
   - Advanced caching
   - WebAssembly components

## Success Metrics

### Performance Targets
- Dashboard shell visible: < 1 second
- Today tab interactive: < 2 seconds
- All tabs available: < 5 seconds
- Memory usage: < 300MB

### User Experience Targets
- No blank screens
- Clear loading progress
- Responsive during loading
- No UI freezing

## Risk Mitigation

### Potential Issues
1. **Race conditions** from async loading
2. **Callback timing** issues
3. **Memory leaks** from component pooling
4. **Browser compatibility** issues

### Mitigation Strategies
1. Comprehensive error handling
2. Fallback to synchronous loading
3. Resource cleanup protocols
4. Progressive enhancement approach

## Testing Plan

1. **Performance Testing**
   - Measure each optimization impact
   - Profile memory usage
   - Test on slow connections

2. **User Testing**
   - A/B test loading approaches
   - Gather perceived performance feedback
   - Monitor error rates

3. **Regression Testing**
   - Ensure all functionality preserved
   - Test edge cases
   - Verify data accuracy

## Next Steps

1. Review current code implementation
2. Identify quick wins
3. Implement Phase 1 optimizations
4. Measure and iterate