# AEMO Dashboard Caching Implementation Plan

*Created: July 21, 2025*

## Overview

This plan implements the caching and multi-user improvements from CACHING_AND_MULTIUSER_STRATEGY.md with comprehensive end-to-end testing at each phase.

## Phase 1: Fix DuckDB Connection Management (Critical) 🚨

### Goal
Fix the singleton connection pattern that causes crashes with multiple users.

### Implementation Steps

1. **Backup Current Code**
   ```bash
   cp src/data_service/shared_data_duckdb.py src/data_service/shared_data_duckdb.py.backup
   ```

2. **Modify DuckDBDataService** 
   - Change from singleton to thread-local connections
   - Add proper connection cleanup
   - Reduce memory per connection for multi-user

3. **Update Connection Usage**
   - Ensure all queries use `get_connection()` method
   - Add connection cleanup in error handlers

### Testing Plan

#### Test 1.1: Single User Functionality
```bash
# Kill existing dashboard
pkill -f "gen_dash.py"

# Start dashboard
cd /path/to/dashboard
.venv/bin/python run_dashboard_duckdb.py

# Manual tests:
1. Load dashboard at http://localhost:5006
2. Test all tabs (Generation, Price Analysis, Station, NEM)
3. Test all date ranges (24h, 7d, 30d, All)
4. Verify data loads correctly
5. Check logs for errors
```

#### Test 1.2: Connection Isolation
```python
# test_connection_isolation.py
import threading
import time
from src.data_service.shared_data_duckdb import DuckDBDataService

def test_thread_connection(thread_id):
    service = DuckDBDataService()
    conn1 = service.get_connection()
    conn2 = service.get_connection()
    
    # Should be same connection in same thread
    assert conn1 is conn2, f"Thread {thread_id}: Connections should be identical"
    
    # Query should work
    result = conn1.execute("SELECT 1").fetchone()
    assert result[0] == 1, f"Thread {thread_id}: Query failed"
    
    print(f"Thread {thread_id}: PASS")

# Test with 5 threads
threads = []
for i in range(5):
    t = threading.Thread(target=test_thread_connection, args=(i,))
    threads.append(t)
    t.start()

for t in threads:
    t.join()

print("Connection isolation test: PASS")
```

#### Test 1.3: Memory Usage
```bash
# Before changes
ps aux | grep gen_dash | awk '{print $6}'  # Note RSS memory

# After changes with 5 concurrent users
# Memory should not exceed: base_memory + (100MB * 5 users)
```

### Success Criteria
- ✅ Dashboard works normally for single user
- ✅ Each thread gets its own connection
- ✅ No connection conflicts with concurrent access
- ✅ Memory usage scales linearly with users

---

## Phase 2: Implement Panel Caching Decorators

### Goal
Add @pn.cache decorators to expensive query operations.

### Implementation Steps

1. **Update Query Managers**
   - Add `import panel as pn` to query managers
   - Add @pn.cache decorators to key methods
   - Configure appropriate TTL and cache sizes

2. **Target Methods**
   ```python
   # High-frequency, fast queries (5 min TTL)
   - get_generation_by_fuel()
   - get_current_spot_prices()
   - get_price_history()
   
   # Expensive calculations (10 min TTL)
   - get_capacity_factors()
   - calculate_station_revenue()
   - get_generation_overview()
   ```

### Testing Plan

#### Test 2.1: Cache Hit Rate
```python
# test_cache_performance.py
import time
from datetime import datetime, timedelta

# First call - should be slow
start = time.time()
data1 = query_manager.get_generation_by_fuel(
    datetime.now() - timedelta(days=7),
    datetime.now(),
    'NEM'
)
first_call = time.time() - start

# Second call - should be instant (from cache)
start = time.time()
data2 = query_manager.get_generation_by_fuel(
    datetime.now() - timedelta(days=7),
    datetime.now(),
    'NEM'
)
cached_call = time.time() - start

print(f"First call: {first_call:.3f}s")
print(f"Cached call: {cached_call:.3f}s")
print(f"Speedup: {first_call/cached_call:.1f}x")

assert cached_call < first_call * 0.1, "Cache not working"
assert data1.equals(data2), "Cached data mismatch"
```

#### Test 2.2: TTL Expiration
```python
# Wait for TTL to expire
time.sleep(301)  # 5 min + 1 sec

# Should be slow again
start = time.time()
data3 = query_manager.get_generation_by_fuel(...)
expired_call = time.time() - start

assert expired_call > cached_call * 10, "Cache didn't expire"
```

#### Test 2.3: Full Dashboard Test
```bash
# Monitor dashboard performance
1. Clear browser cache
2. Load dashboard and time initial load
3. Switch between tabs rapidly
4. Note any performance improvements
5. Check memory usage doesn't grow unbounded
```

### Success Criteria
- ✅ Cache hit rate > 80% for common queries
- ✅ 10x+ speedup on cached queries
- ✅ TTL expiration works correctly
- ✅ Memory usage stays within limits

---

## Phase 3: Fix Dashboard Factory Function

### Goal
Ensure each user session gets isolated dashboard instance.

### Implementation Steps

1. **Create Factory Function**
   ```python
   def create_dashboard():
       """Factory for per-session instances"""
       dashboard = EnergyDashboard()
       pn.state.on_session_destroyed(lambda: dashboard.cleanup())
       return dashboard.template
   ```

2. **Update Startup Code**
   - Change from `dashboard.template.servable()`
   - To `pn.serve(create_dashboard, ...)`

### Testing Plan

#### Test 3.1: Session Isolation
```python
# test_session_isolation.py
import requests
import concurrent.futures

def access_dashboard(session_id):
    with requests.Session() as session:
        # Each session should get unique instance
        resp1 = session.get('http://localhost:5006')
        session_cookie = session.cookies.get('session')
        
        # Make changes (select different region)
        resp2 = session.post('http://localhost:5006/api/region', 
                            json={'region': f'NSW{session_id}'})
        
        return session_id, session_cookie, resp2.status_code

# Test with 3 concurrent sessions
with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
    futures = [executor.submit(access_dashboard, i) for i in range(3)]
    results = [f.result() for f in futures]

# Each should have unique session
cookies = [r[1] for r in results]
assert len(set(cookies)) == 3, "Sessions not isolated"
```

#### Test 3.2: Cleanup Verification
```bash
# Monitor process during session creation/destruction
1. Note initial memory usage
2. Open 5 browser tabs to dashboard
3. Close all tabs
4. Wait 30 seconds
5. Memory should return close to initial level
```

### Success Criteria
- ✅ Each user gets unique session
- ✅ User selections don't affect other users
- ✅ Memory cleaned up on session end
- ✅ No shared state between sessions

---

## Phase 4: Add Session Cleanup Hooks

### Goal
Ensure resources are properly released when sessions end.

### Implementation Steps

1. **Add Cleanup Method**
   ```python
   def cleanup(self):
       """Clean up session resources"""
       logger.info(f"Cleaning up session")
       if hasattr(self, 'data_service'):
           self.data_service.close_connection()
       if hasattr(self, '_session_cache'):
           self._session_cache.clear()
   ```

2. **Register Cleanup Hooks**
   - Use `pn.state.on_session_destroyed`
   - Add logging for verification

### Testing Plan

#### Test 4.1: Resource Release
```bash
# Monitor DuckDB connections
watch -n 1 'lsof -p $(pgrep -f gen_dash) | grep -c duckdb'

# Open/close sessions and verify connection count
```

#### Test 4.2: Memory Leak Test
```python
# test_memory_leak.py
import psutil
import time
import requests

process = psutil.Process()  # Dashboard process
initial_memory = process.memory_info().rss / 1024 / 1024

# Create and destroy 20 sessions
for i in range(20):
    session = requests.Session()
    session.get('http://localhost:5006')
    session.close()
    time.sleep(2)  # Let cleanup happen

final_memory = process.memory_info().rss / 1024 / 1024
memory_growth = final_memory - initial_memory

print(f"Memory growth after 20 sessions: {memory_growth:.1f} MB")
assert memory_growth < 100, "Possible memory leak"
```

### Success Criteria
- ✅ Cleanup method called on session end
- ✅ DuckDB connections properly closed
- ✅ No memory leak over multiple sessions
- ✅ Logs show cleanup activity

---

## Phase 5: Implement Persistent Disk Caching

### Goal
Add disk-based caching for common queries to survive restarts.

### Implementation Steps

1. **Install diskcache**
   ```bash
   uv pip install diskcache
   ```

2. **Create Persistent Cache Module**
   - Implement persistent_cache decorator
   - Configure 1GB disk cache limit
   - Add to expensive/stable queries

3. **Target Queries for Disk Cache**
   - Historical data (changes rarely)
   - Capacity factors (expensive calculation)
   - DUID mappings (static data)

### Testing Plan

#### Test 5.1: Cache Persistence
```bash
# First run - populate cache
.venv/bin/python run_dashboard_duckdb.py
# Load dashboard, navigate all tabs
# Note load times

# Kill dashboard
pkill -f gen_dash

# Restart dashboard
.venv/bin/python run_dashboard_duckdb.py
# Load times should be faster (data from disk cache)
```

#### Test 5.2: Cache Size Management
```python
# test_disk_cache_size.py
from pathlib import Path
import shutil

cache_dir = Path.home() / '.aemo_dashboard_cache'
initial_size = sum(f.stat().st_size for f in cache_dir.rglob('*'))

# Generate lots of cached data
for days in range(1, 365):
    query_manager.get_generation_by_fuel(
        datetime.now() - timedelta(days=days),
        datetime.now(),
        'NEM'
    )

final_size = sum(f.stat().st_size for f in cache_dir.rglob('*'))
size_mb = final_size / 1024 / 1024

print(f"Cache size: {size_mb:.1f} MB")
assert size_mb < 1100, "Cache exceeded 1GB limit"
```

### Success Criteria
- ✅ Dashboard starts faster after restart
- ✅ Cache persists between sessions
- ✅ Size limit enforced (1GB)
- ✅ Old entries evicted properly

---

## Phase 6: Create Cache Warming Strategy

### Goal
Pre-populate caches with common queries for instant loading.

### Implementation Steps

1. **Create Cache Warmer Script**
   - Pre-load common date ranges
   - Pre-calculate expensive aggregations
   - Run on dashboard startup

2. **Add Scheduled Pre-computation**
   - Daily refresh of common queries
   - Run during low-usage hours

### Testing Plan

#### Test 6.1: Startup Performance
```bash
# Cold start (no cache)
rm -rf ~/.aemo_dashboard_cache
time .venv/bin/python run_dashboard_duckdb.py --no-warm
# Note time to responsive

# Warm start
.venv/bin/python warm_cache.py
time .venv/bin/python run_dashboard_duckdb.py --warm
# Should be significantly faster
```

#### Test 6.2: Cache Hit Rate
```python
# After warming, test hit rate
hits = 0
misses = 0

for query in common_queries:
    start = time.time()
    result = execute_query(query)
    duration = time.time() - start
    
    if duration < 0.1:  # Likely cache hit
        hits += 1
    else:
        misses += 1

hit_rate = hits / (hits + misses) * 100
print(f"Cache hit rate: {hit_rate:.1f}%")
assert hit_rate > 90, "Cache warming not effective"
```

### Success Criteria
- ✅ Dashboard loads in <2s with warm cache
- ✅ Cache hit rate >90% for common queries
- ✅ Warming completes in <30s
- ✅ No errors during warming

---

## Phase 7: Multi-User Load Testing

### Goal
Verify dashboard handles concurrent users without issues.

### Testing Plan

#### Test 7.1: Concurrent User Simulation
```python
# test_concurrent_users.py
import concurrent.futures
import requests
import time
import statistics

def simulate_user(user_id):
    """Simulate real user behavior"""
    session = requests.Session()
    timings = []
    
    # Initial load
    start = time.time()
    resp = session.get('http://localhost:5006')
    timings.append(time.time() - start)
    
    # Navigate tabs
    for tab in ['generation', 'prices', 'station']:
        time.sleep(1)  # Human delay
        start = time.time()
        resp = session.get(f'http://localhost:5006/{tab}')
        timings.append(time.time() - start)
    
    return user_id, statistics.mean(timings), max(timings)

# Test with increasing users
for num_users in [1, 5, 10, 20, 50]:
    print(f"\nTesting with {num_users} concurrent users...")
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=num_users) as executor:
        futures = [executor.submit(simulate_user, i) for i in range(num_users)]
        results = [f.result() for f in futures]
    
    avg_times = [r[1] for r in results]
    max_times = [r[2] for r in results]
    
    print(f"Average response time: {statistics.mean(avg_times):.2f}s")
    print(f"Max response time: {max(max_times):.2f}s")
    
    # Performance should degrade gracefully
    assert statistics.mean(avg_times) < 5.0, f"Poor performance with {num_users} users"
```

#### Test 7.2: Resource Usage Under Load
```bash
# Monitor during load test
top -p $(pgrep -f gen_dash)

# Track:
# - CPU usage (should scale with users)
# - Memory usage (should be ~100MB per user)
# - Thread count
# - No crashes or errors
```

### Success Criteria
- ✅ Handles 50+ concurrent users
- ✅ Response time <5s under load
- ✅ Linear memory scaling
- ✅ No crashes or data corruption

---

## Phase 8: Production Deployment Configuration

### Goal
Configure optimized multi-process deployment.

### Implementation Steps

1. **Create Production Start Script**
   ```bash
   #!/bin/bash
   panel serve src/aemo_dashboard/generation/gen_dash.py \
       --num-procs 4 \
       --port 5006 \
       --allow-websocket-origin="*" \
       --setup warm_cache.py \
       --warm \
       --log-level info
   ```

2. **Configure Process Monitoring**
   - Add systemd service
   - Configure auto-restart
   - Set up log rotation

### Testing Plan

#### Test 8.1: Multi-Process Stability
```bash
# Run for extended period
./start_production.sh

# After 1 hour:
1. Check all processes still running
2. Verify no memory leaks
3. Test user experience still good
4. Check logs for errors
```

#### Test 8.2: Process Recovery
```bash
# Kill a worker process
kill -9 <worker_pid>

# Verify:
1. Other workers continue serving
2. Dead worker gets replaced
3. No user impact
```

### Success Criteria
- ✅ Runs stable for 24+ hours
- ✅ Automatic process recovery
- ✅ Load distributed across workers
- ✅ Production-ready configuration

---

## Implementation Timeline

| Phase | Priority | Duration | Dependencies |
|-------|----------|----------|--------------|
| 1. DuckDB Connections | Critical | 1 day | None |
| 2. Panel Caching | High | 1 day | Phase 1 |
| 3. Factory Function | High | 0.5 day | Phase 1 |
| 4. Cleanup Hooks | Medium | 0.5 day | Phase 3 |
| 5. Disk Caching | Medium | 1 day | Phase 2 |
| 6. Cache Warming | Medium | 1 day | Phase 5 |
| 7. Load Testing | Medium | 1 day | Phases 1-6 |
| 8. Production Config | Low | 0.5 day | All phases |

**Total: 6.5 days of implementation**

## Risk Mitigation

1. **Always backup before changes**
2. **Test each phase thoroughly**
3. **Keep fallback options ready**
4. **Monitor production closely**
5. **Have rollback plan**

## Success Metrics

After full implementation:
- Initial load time: <2 seconds (from >8 seconds)
- Concurrent users: 50+ (from single user)
- Memory per user: ~100MB (from 2GB shared)
- Cache hit rate: >90%
- Uptime: 99.9%