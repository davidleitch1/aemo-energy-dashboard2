# Generation Dashboard Full Refactoring Plan

**Date**: July 19, 2025  
**Purpose**: Complete refactor of gen_dash.py to use hybrid query manager approach

## Executive Summary

Refactor the generation dashboard to use the same hybrid query manager pattern successfully implemented in Price Analysis and Station Analysis modules. This will reduce memory usage from GB+ to <500MB and improve load times from 60s to 2-3s when viewing "All Available Data".

## Current State Analysis

### Problem
- Loading **38.4 million records** when viewing "All Available Data"
- All data processed in pandas memory with multiple pivot operations
- Causes significant UI lag and memory usage
- Already uses DuckDB for loading but not for aggregation

### Current Architecture
```python
GenerationDashboard:
  - load_generation_data() -> Loads ALL raw DUID data via DuckDB
  - process_data_for_region() -> Pivots in pandas memory
  - create_stacked_area_plot() -> Renders with hvplot
```

## Refactoring Pattern (Based on Successful Modules)

### Pattern from Price Analysis
1. **Hybrid Query Manager**: Bridges DuckDB and pandas operations
2. **Smart Caching**: LRU cache with TTL for aggregated results
3. **Lazy Loading**: Data loaded only when needed
4. **DuckDB Views**: Pre-joined and aggregated views for performance

### Pattern from Station Analysis  
1. **Direct SQL Queries**: Query exactly what's needed
2. **Resolution Selection**: Auto-select 5min vs 30min based on range
3. **Minimal UI Changes**: Keep existing UI working without modification
4. **Progressive Enhancement**: Add features without breaking existing

## Implementation Plan

### Phase 1: Create DuckDB Views and Infrastructure

#### Step 1.1: Add Generation-Specific Views to duckdb_views.py
```python
def _create_generation_dashboard_views(self) -> None:
    """Create views specifically for generation dashboard"""
    
    # View 1: Generation by fuel type with region (30min)
    self.conn.execute("""
        CREATE OR REPLACE VIEW generation_by_fuel_30min AS
        SELECT 
            g.settlementdate,
            d.Fuel as fuel_type,
            d.Region as region,
            SUM(g.scadavalue) as total_generation_mw,
            COUNT(DISTINCT g.duid) as unit_count,
            SUM(d."Capacity(MW)") as total_capacity_mw
        FROM generation_30min g
        JOIN duid_mapping d ON g.duid = d.DUID
        GROUP BY g.settlementdate, d.Fuel, d.Region
        ORDER BY g.settlementdate, d.Fuel
    """)
    
    # View 2: Generation by fuel type with region (5min)
    self.conn.execute("""
        CREATE OR REPLACE VIEW generation_by_fuel_5min AS
        SELECT 
            g.settlementdate,
            d.Fuel as fuel_type,
            d.Region as region,
            SUM(g.scadavalue) as total_generation_mw,
            COUNT(DISTINCT g.duid) as unit_count,
            SUM(d."Capacity(MW)") as total_capacity_mw
        FROM generation_5min g
        JOIN duid_mapping d ON g.duid = d.DUID
        GROUP BY g.settlementdate, d.Fuel, d.Region
        ORDER BY g.settlementdate, d.Fuel
    """)
    
    # View 3: Generation with price data (integrated view)
    self.conn.execute("""
        CREATE OR REPLACE VIEW generation_with_prices_30min AS
        SELECT 
            g.settlementdate,
            g.fuel_type,
            g.region,
            g.total_generation_mw,
            g.unit_count,
            g.total_capacity_mw,
            p.rrp as price,
            g.total_generation_mw * p.rrp * 0.5 as revenue_30min
        FROM generation_by_fuel_30min g
        LEFT JOIN prices_30min p 
            ON g.settlementdate = p.SETTLEMENTDATE 
            AND g.region = p.REGIONID
    """)
    
    # View 4: Capacity utilization by fuel
    self.conn.execute("""
        CREATE OR REPLACE VIEW capacity_utilization_30min AS
        SELECT 
            settlementdate,
            fuel_type,
            region,
            total_generation_mw,
            total_capacity_mw,
            CASE 
                WHEN total_capacity_mw > 0 
                THEN (total_generation_mw / total_capacity_mw) * 100
                ELSE 0 
            END as utilization_pct
        FROM generation_by_fuel_30min
    """)
```

#### Step 1.2: Create GenerationQueryManager Class
```python
class GenerationQueryManager:
    """Specialized query manager for generation dashboard"""
    
    def __init__(self):
        self.query_manager = HybridQueryManager(cache_size_mb=200, cache_ttl=300)
        view_manager.create_all_views()
        
    def query_generation_by_fuel(self, start_date, end_date, region='NEM', resolution='auto'):
        """Query generation aggregated by fuel type"""
        
        # Determine resolution
        if resolution == 'auto':
            days_diff = (end_date - start_date).days
            resolution = '5min' if days_diff <= 7 else '30min'
        
        view_name = f'generation_by_fuel_{resolution}'
        
        # Build query
        if region == 'NEM':
            query = f"""
                SELECT 
                    settlementdate,
                    fuel_type,
                    SUM(total_generation_mw) as total_generation_mw
                FROM {view_name}
                WHERE settlementdate >= '{start_date.strftime('%Y-%m-%d %H:%M:%S')}'
                AND settlementdate <= '{end_date.strftime('%Y-%m-%d %H:%M:%S')}'
                GROUP BY settlementdate, fuel_type
                ORDER BY settlementdate, fuel_type
            """
        else:
            query = f"""
                SELECT 
                    settlementdate,
                    fuel_type,
                    total_generation_mw
                FROM {view_name}
                WHERE settlementdate >= '{start_date.strftime('%Y-%m-%d %H:%M:%S')}'
                AND settlementdate <= '{end_date.strftime('%Y-%m-%d %H:%M:%S')}'
                AND region = '{region}'
                ORDER BY settlementdate, fuel_type
            """
        
        # Use cache key
        cache_key = f"gen_by_fuel_{region}_{start_date}_{end_date}_{resolution}"
        
        # Query with caching
        return self.query_manager.query_with_cache(query, cache_key)
```

### Phase 2: Refactor GenerationDashboard Class

#### Step 2.1: Update __init__ Method
```python
def __init__(self):
    """Initialize dashboard with query manager"""
    self.query_manager = GenerationQueryManager()
    
    # Keep existing attributes for compatibility
    self.gen_output_df = None
    self.transmission_df = None
    self.rooftop_df = None
    self.spot_price_df = None
    
    # Data state
    self.data_loaded = False
    self.current_date_range = None
    
    # Initialize existing UI components...
```

#### Step 2.2: Refactor load_generation_data()
```python
def load_generation_data(self):
    """Load generation data using query manager"""
    try:
        start_time, end_time = self._get_effective_date_range()
        
        # Check if we should load pre-aggregated data
        days_diff = (end_time - start_time).days
        
        if days_diff > 30:  # For long ranges, use pre-aggregated data
            logger.info(f"Loading pre-aggregated data for {days_diff} days")
            
            # Query aggregated data
            gen_data = self.query_manager.query_generation_by_fuel(
                start_date=start_time,
                end_date=end_time,
                region='NEM'  # Load all regions, filter later
            )
            
            # Add DUID info for compatibility (create synthetic DUIDs)
            gen_data['duid'] = gen_data['fuel_type'] + '_AGG'
            gen_data['scadavalue'] = gen_data['total_generation_mw']
            
            # Load region mapping from DUID mapping
            with open(config.gen_info_file, 'rb') as f:
                duid_mapping = pickle.load(f)
            
            # Create region mapping for fuel types
            fuel_regions = {}
            for _, row in duid_mapping.iterrows():
                fuel = row.get('Fuel', 'Unknown')
                region = row.get('Region', 'Unknown')
                if fuel not in fuel_regions:
                    fuel_regions[fuel] = set()
                fuel_regions[fuel].add(region)
            
            # Add region column (for NEM, include all regions)
            gen_data['region'] = 'NEM'
            gen_data['fuel'] = gen_data['fuel_type']
            
        else:
            # For short ranges, load raw data (existing approach)
            from ..shared.adapter_selector import load_generation_data
            gen_data = load_generation_data(
                start_date=start_time,
                end_date=end_time,
                resolution='auto'
            )
            
            # Process as before...
            
        self.gen_output_df = gen_data
        logger.info(f"Loaded {len(gen_data)} generation records")
        
    except Exception as e:
        logger.error(f"Error loading generation data: {e}")
        self.gen_output_df = pd.DataFrame()
```

#### Step 2.3: Optimize process_data_for_region()
```python
def process_data_for_region(self):
    """Process generation data for selected region"""
    if self.gen_output_df is None or self.gen_output_df.empty:
        return pd.DataFrame()
    
    # Check if data is already aggregated
    if 'fuel_type' in self.gen_output_df.columns and '_AGG' in str(self.gen_output_df['duid'].iloc[0]):
        # Data is pre-aggregated, just filter and pivot
        df = self.gen_output_df.copy()
        
        # Filter by region if needed
        if self.region != 'NEM' and 'region' in df.columns:
            # For aggregated data, we need to re-query for specific region
            start_time, end_time = self._get_effective_date_range()
            df = self.query_manager.query_generation_by_fuel(
                start_date=start_time,
                end_date=end_time,
                region=self.region
            )
        
        # Pivot directly (data is already aggregated)
        pivot_df = df.pivot(
            index='settlementdate',
            columns='fuel_type',
            values='total_generation_mw'
        ).fillna(0)
        
    else:
        # Raw data - use existing logic
        df = self.gen_output_df.copy()
        
        # Filter by region
        if self.region != 'NEM':
            df = df[df['region'] == self.region]
        
        # Group and pivot (existing logic)
        result = df.groupby([
            pd.Grouper(key='settlementdate', freq='5min'),
            'fuel'
        ])['scadavalue'].sum().reset_index()
        
        pivot_df = result.pivot(
            index='settlementdate',
            columns='fuel',
            values='scadavalue'
        ).fillna(0)
    
    # Add transmission and rooftop (existing logic)
    # ... (keep existing transmission and rooftop logic)
    
    return pivot_df
```

### Phase 3: Testing Plan

#### Test 3.1: Create Unit Test File
```python
# test_generation_dashboard_refactor.py

def test_generation_query_manager():
    """Test the generation query manager"""
    print("Testing Generation Query Manager...")
    
    manager = GenerationQueryManager()
    
    # Test 1: Query 24 hours of data
    end_date = datetime.now()
    start_date = end_date - timedelta(days=1)
    
    t1 = time.time()
    data = manager.query_generation_by_fuel(start_date, end_date, 'NSW1')
    t1_duration = time.time() - t1
    
    print(f"✓ 24-hour query: {len(data)} records in {t1_duration:.2f}s")
    print(f"  Fuel types: {data['fuel_type'].unique()}")
    
    # Test 2: Query 1 year of data
    start_date_year = end_date - timedelta(days=365)
    
    t2 = time.time()
    data_year = manager.query_generation_by_fuel(start_date_year, end_date, 'NEM')
    t2_duration = time.time() - t2
    
    print(f"✓ 1-year query: {len(data_year)} records in {t2_duration:.2f}s")
    
    # Test 3: Cache effectiveness
    t3 = time.time()
    data_cached = manager.query_generation_by_fuel(start_date, end_date, 'NSW1')
    t3_duration = time.time() - t3
    
    print(f"✓ Cached query: {t3_duration:.2f}s (vs {t1_duration:.2f}s original)")
    
    return True
```

#### Test 3.2: Memory Usage Test
```python
def test_memory_usage():
    """Test memory usage with refactored dashboard"""
    import psutil
    process = psutil.Process()
    
    print("\nTesting Memory Usage...")
    
    # Baseline memory
    start_memory = process.memory_info().rss / 1024 / 1024
    print(f"Baseline memory: {start_memory:.1f} MB")
    
    # Create dashboard
    dashboard = GenerationDashboard()
    
    # Load 5 years of data
    dashboard.time_period.value = "All Available Data"
    dashboard.load_generation_data()
    data = dashboard.process_data_for_region()
    
    # Check memory after load
    current_memory = process.memory_info().rss / 1024 / 1024
    memory_increase = current_memory - start_memory
    
    print(f"Memory after 5-year load: {current_memory:.1f} MB")
    print(f"Memory increase: {memory_increase:.1f} MB")
    
    if memory_increase < 500:
        print("✅ Memory usage is excellent!")
    else:
        print("⚠️ Memory usage is higher than expected")
    
    return memory_increase < 500
```

#### Test 3.3: Performance Comparison Test
```python
def test_performance_comparison():
    """Compare performance before and after refactoring"""
    print("\nPerformance Comparison Test...")
    
    # Test different date ranges
    test_ranges = [
        ("24 hours", 1),
        ("7 days", 7),
        ("30 days", 30),
        ("1 year", 365),
        ("All data", 2000)
    ]
    
    dashboard = GenerationDashboard()
    
    for range_name, days in test_ranges:
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days)
        
        # Time the full pipeline
        t_start = time.time()
        
        # Set date range
        dashboard.date_range_slider.value = (start_date, end_date)
        
        # Load and process
        dashboard.load_generation_data()
        data = dashboard.process_data_for_region()
        
        t_duration = time.time() - t_start
        
        print(f"{range_name}: {len(data)} records in {t_duration:.2f}s")
        
        # Check if performance meets targets
        if days <= 30 and t_duration > 2:
            print(f"  ⚠️ Performance below target for {range_name}")
        elif days > 30 and t_duration > 5:
            print(f"  ⚠️ Performance below target for {range_name}")
        else:
            print(f"  ✅ Performance meets target")
```

### Phase 4: Integration and Rollout

#### Step 4.1: Update imports in gen_dash.py
```python
from ..shared.hybrid_query_manager import HybridQueryManager
from ..shared.duckdb_views import view_manager
```

#### Step 4.2: Add backward compatibility flag
```python
USE_HYBRID_GENERATION = os.getenv('USE_HYBRID_GENERATION', 'true').lower() == 'true'

if USE_HYBRID_GENERATION:
    # Use new refactored approach
    from .generation_query_manager import GenerationQueryManager
else:
    # Keep existing implementation
    pass
```

#### Step 4.3: Gradual rollout plan
1. Test with developer environment
2. Enable for specific date ranges first
3. Monitor performance metrics
4. Full rollout after validation

## Success Criteria

### Performance Metrics
- **Memory Usage**: < 500MB for all date ranges
- **Load Time**: 
  - < 2s for ranges up to 30 days
  - < 5s for ranges up to 5 years
- **Cache Hit Rate**: > 80% for repeated queries
- **UI Responsiveness**: No freezing during data load

### Functional Requirements
- All existing features continue to work
- Chart displays remain identical
- Transmission and rooftop solar integration works
- Region filtering works correctly
- Capacity utilization calculations accurate

### Data Accuracy
- Fuel type totals match current implementation
- Time aggregations are correct
- No data loss or duplication

## Risk Mitigation

### Risks and Mitigations
1. **Risk**: Aggregated data might not match exact DUID sums
   - **Mitigation**: Validate totals extensively, keep raw data option
   
2. **Risk**: UI might expect specific data formats
   - **Mitigation**: Maintain backward compatible data structures
   
3. **Risk**: Cache might grow too large
   - **Mitigation**: Implement aggressive cache eviction, monitor size

4. **Risk**: Query performance on large ranges
   - **Mitigation**: Use materialized views for common aggregations

## Timeline

### Week 1: Infrastructure
- Day 1-2: Create DuckDB views and test
- Day 3-4: Implement GenerationQueryManager
- Day 5: Unit testing

### Week 2: Integration  
- Day 1-2: Refactor load_generation_data()
- Day 3-4: Refactor process_data_for_region()
- Day 5: Integration testing

### Week 3: Validation
- Day 1-2: Performance testing
- Day 3-4: User acceptance testing
- Day 5: Documentation and rollout

## Conclusion

This refactoring follows the proven pattern from Price Analysis and Station Analysis modules. By pre-aggregating data in DuckDB and using smart caching, we can reduce the 38M record load to ~100K records while maintaining all functionality. The hybrid approach ensures complex pandas operations continue to work while gaining the performance benefits of DuckDB.