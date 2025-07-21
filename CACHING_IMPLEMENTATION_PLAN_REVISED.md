# AEMO Dashboard Caching Implementation Plan (Revised)

*Created: July 21, 2025*
*Revised after code review*

## Overview

This revised plan works WITH the existing singleton architecture rather than against it, focusing on real performance bottlenecks.

## Current Architecture Reality

1. **DuckDBDataService**: Singleton pattern with global instance
2. **Existing Caching**: SmartCache with TTL already implemented
3. **Query Managers**: Already have 100-200MB caches each
4. **Connection Model**: Single shared connection is actually fine for read-only analytics

## Phase 1: Enhance Existing SmartCache (High Priority)

### Goal
Improve the existing caching system rather than replace it.

### Implementation Steps

1. **Add Persistent Cache Layer**
   ```python
   # In src/aemo_dashboard/shared/smart_cache.py
   import diskcache
   
   class SmartCache:
       def __init__(self, max_size_mb=100, ttl_seconds=300, 
                    enable_disk_cache=True, disk_cache_path=None):
           self.memory_cache = {}  # Existing
           self.enable_disk_cache = enable_disk_cache
           
           if enable_disk_cache:
               cache_dir = disk_cache_path or Path.home() / '.aemo_cache'
               self.disk_cache = diskcache.Cache(
                   str(cache_dir),
                   size_limit=1_073_741_824  # 1GB
               )
   ```

2. **Update get() Method**
   ```python
   def get(self, key, default=None):
       # Check memory first
       if key in self.memory_cache:
           return self._get_from_memory(key)
       
       # Check disk cache
       if self.enable_disk_cache and key in self.disk_cache:
           value = self.disk_cache[key]
           # Promote to memory cache
           self._add_to_memory(key, value)
           return value
       
       return default
   ```

### Testing Plan

```python
# test_enhanced_cache.py
from aemo_dashboard.shared.smart_cache import SmartCache
import time

# Test memory + disk cache
cache = SmartCache(max_size_mb=10, enable_disk_cache=True)

# Add large data
large_data = pd.DataFrame(np.random.rand(100000, 10))
cache.set('test_key', large_data, ttl=60)

# Force eviction from memory
for i in range(20):
    cache.set(f'evict_{i}', large_data, ttl=60)

# Should still be in disk cache
assert cache.get('test_key') is not None
print("✅ Disk cache working")

# Test persistence
del cache
cache2 = SmartCache(max_size_mb=10, enable_disk_cache=True)
assert cache2.get('test_key') is not None
print("✅ Cache persisted across restarts")
```

---

## Phase 2: Optimize Panel Initialization (High Priority)

### Goal
Fix the actual startup bottleneck - Panel/HoloViews initialization.

### Implementation Steps

1. **Lazy Tab Loading**
   ```python
   # In gen_dash.py
   class EnergyDashboard:
       def __init__(self):
           # Don't create all tabs immediately
           self.tabs = pn.Tabs(
               ('Generation', pn.pane.Markdown("Loading...")),
               ('Price Analysis', pn.pane.Markdown("Loading...")),
               ('Station Analysis', pn.pane.Markdown("Loading...")),
               ('NEM Overview', pn.pane.Markdown("Loading..."))
           )
           self.tabs.param.watch(self._on_tab_change, 'active')
           self._loaded_tabs = set()
           
       def _on_tab_change(self, event):
           tab_index = event.new
           if tab_index not in self._loaded_tabs:
               self._load_tab(tab_index)
               self._loaded_tabs.add(tab_index)
   ```

2. **Defer Heavy Imports**
   ```python
   def _load_tab(self, index):
       if index == 0:  # Generation
           from .generation_components import create_generation_tab
           self.tabs[0] = create_generation_tab(self.query_manager)
       elif index == 1:  # Price Analysis
           from .price_components import create_price_tab
           self.tabs[1] = create_price_tab(self.price_motor)
   ```

### Testing Plan

```bash
# Measure startup time before changes
time python -c "import gen_dash; print('imported')"

# After changes, should be much faster
# Only Panel core imports, not all components
```

---

## Phase 3: Add Query Result Caching (Medium Priority)

### Goal
Cache expensive DuckDB query results at the query manager level.

### Implementation Steps

1. **Decorator for Query Methods**
   ```python
   # In query managers
   from functools import wraps
   
   def cache_query_result(ttl=300):
       def decorator(method):
           @wraps(method)
           def wrapper(self, *args, **kwargs):
               # Create cache key from method name and args
               cache_key = f"{method.__name__}:{args}:{kwargs}"
               
               # Check cache first
               result = self.cache.get(cache_key)
               if result is not None:
                   self.logger.debug(f"Cache hit for {method.__name__}")
                   return result
               
               # Execute query
               result = method(self, *args, **kwargs)
               
               # Cache result
               self.cache.set(cache_key, result, ttl=ttl)
               return result
           return wrapper
       return decorator
   ```

2. **Apply to Expensive Queries**
   ```python
   @cache_query_result(ttl=600)  # 10 minutes
   def get_capacity_factors(self, start_date, end_date, duids=None):
       # Expensive calculation
       pass
   ```

---

## Phase 4: Pre-warm Common Queries (Medium Priority)

### Goal
Background task to keep common queries fresh in cache.

### Implementation Steps

1. **Cache Warmer Service**
   ```python
   # cache_warmer.py
   import threading
   import time
   from datetime import datetime, timedelta
   
   class CacheWarmer:
       def __init__(self, query_managers):
           self.query_managers = query_managers
           self.running = False
           
       def warm_common_queries(self):
           """Refresh common queries in background"""
           ranges = [
               timedelta(days=1),
               timedelta(days=7),
               timedelta(days=30)
           ]
           
           for delta in ranges:
               end = datetime.now()
               start = end - delta
               
               # Warm each query manager
               for qm in self.query_managers:
                   try:
                       qm.get_generation_by_fuel(start, end, 'NEM')
                   except Exception as e:
                       logger.error(f"Cache warming failed: {e}")
   ```

2. **Start with Dashboard**
   ```python
   # In dashboard init
   self.cache_warmer = CacheWarmer([
       self.generation_query_manager,
       self.price_query_manager
   ])
   self.cache_warmer.start()
   ```

---

## Phase 5: Connection Pool for Multi-User (Low Priority)

### Goal
Add connection pooling WITHIN the singleton for better multi-user support.

### Implementation Steps

1. **Connection Pool in Singleton**
   ```python
   class DuckDBDataService:
       def __init__(self):
           self._main_conn = duckdb.connect(':memory:')
           self._setup_connection(self._main_conn)
           
           # Read-only connection pool for queries
           self._read_connections = []
           for i in range(3):
               conn = self._main_conn.cursor()
               self._read_connections.append(conn)
           
       def execute_query(self, query):
           """Get connection from pool for read queries"""
           conn = self._get_available_connection()
           try:
               return conn.execute(query).df()
           finally:
               self._release_connection(conn)
   ```

---

## Phase 6: Multi-User Testing (High Priority)

### Goal
Test existing system's multi-user capability.

### Testing Plan

```python
# test_multiuser_current.py
import concurrent.futures
import requests
import time

def test_user_session(user_id):
    """Test current system with multiple users"""
    session = requests.Session()
    
    start = time.time()
    resp = session.get('http://localhost:5006')
    load_time = time.time() - start
    
    # Navigate tabs
    for tab in ['#generation', '#price-analysis']:
        resp = session.get(f'http://localhost:5006{tab}')
    
    return user_id, load_time, resp.status_code

# Test with current architecture
print("Testing current multi-user capability...")
with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
    futures = [executor.submit(test_user_session, i) for i in range(10)]
    results = [f.result() for f in futures]

# Current system might actually handle multiple users fine
# since DuckDB is thread-safe for read operations
```

---

## Revised Timeline

| Phase | Priority | Duration | Real Impact |
|-------|----------|----------|-------------|
| 1. Enhance SmartCache | High | 0.5 day | Persistence across restarts |
| 2. Lazy Tab Loading | High | 0.5 day | 50%+ faster startup |
| 3. Query Result Caching | Medium | 0.5 day | Better cache utilization |
| 4. Cache Warming | Medium | 0.5 day | Instant common queries |
| 5. Connection Pooling | Low | 1 day | Marginal improvement |
| 6. Multi-User Testing | High | 0.5 day | Verify current capability |

**Total: 3.5 days (vs 6.5 days original)**

## Key Insights

1. **Don't Fight the Architecture**: The singleton pattern works fine for read-only analytics
2. **Fix Real Bottlenecks**: Panel initialization, not DuckDB
3. **Enhance, Don't Replace**: Existing caching is sophisticated
4. **Test First**: Current system might already handle multi-user well
5. **Incremental Improvements**: Smaller changes, less risk

## Success Metrics

- Startup time: <3 seconds (achievable with lazy loading)
- Cache persistence: Survives restarts
- Multi-user: Test shows current capability (likely already good)
- Risk: Minimal (working with existing architecture)