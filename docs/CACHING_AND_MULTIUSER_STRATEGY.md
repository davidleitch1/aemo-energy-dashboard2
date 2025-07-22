# AEMO Dashboard - Caching and Multi-User Strategy

*Date: July 20, 2025*

## Current Status Analysis

### ✅ What's Working Well

1. **DuckDB Implementation**
   - Using in-memory connection (good for read-only analytics)
   - Pre-created views for common queries
   - Memory limit set to 2GB (appropriate)
   - Thread count configured to 4

2. **Hybrid Query Manager**
   - Smart TTL-based caching at application level
   - Memory-limited cache (200MB)
   - Cache key generation includes resolution

3. **Data Architecture**
   - Read-only access pattern (perfect for multi-user)
   - Pre-aggregated views reduce computation
   - Parquet files enable efficient queries

### ⚠️ Issues to Address

1. **Connection Management**
   - Currently using singleton pattern - NOT safe for multi-user
   - No connection pooling or per-session connections
   - Risk of connection conflicts with multiple users

2. **Session Management**
   - Dashboard not wrapped in factory function
   - Risk of shared state between users
   - No session cleanup hooks

3. **Caching Strategy**
   - Not using Panel's @pn.cache decorator
   - Missing cache warming on startup
   - No coordination between DuckDB and Panel caching

## Recommended Implementation

### 1. Fix DuckDB Connection Management

**Current Issue**: Singleton connection shared across all users
**Solution**: Connection per session with proper cleanup

```python
# src/data_service/shared_data_duckdb.py

class DuckDBDataService:
    """Thread-safe DuckDB service with per-session connections"""
    
    def __init__(self):
        # Don't create connection here
        self._thread_local = threading.local()
        self._parquet_paths = self._get_parquet_paths()
        self._duid_mapping = self._load_duid_mapping_once()
    
    def get_connection(self):
        """Get thread-local DuckDB connection"""
        if not hasattr(self._thread_local, 'conn'):
            # Create read-only connection per thread
            self._thread_local.conn = duckdb.connect(':memory:', read_only=False)
            self._thread_local.conn.execute("SET memory_limit='1GB'")  # Per-thread limit
            self._thread_local.conn.execute("SET threads=2")  # Reduce for multi-user
            self._register_views(self._thread_local.conn)
        return self._thread_local.conn
    
    def close_connection(self):
        """Close thread-local connection"""
        if hasattr(self._thread_local, 'conn'):
            self._thread_local.conn.close()
            delattr(self._thread_local, 'conn')
```

### 2. Implement Panel Caching

**Add strategic caching at the query level:**

```python
# src/aemo_dashboard/generation/generation_query_manager.py

from panel import cache

class GenerationQueryManager:
    
    @cache(max_items=100, policy='LRU', ttl=300)  # 5-minute TTL
    def get_generation_by_fuel(self, start_date, end_date, region='NEM', 
                               resolution='auto', include_rooftop=True):
        """Cached generation query"""
        # Existing implementation
        # Panel will automatically cache based on parameters
    
    @cache(max_items=50, ttl=600)  # 10-minute TTL for expensive ops
    def get_capacity_factors(self, start_date, end_date, duids=None):
        """Cached capacity factor calculation"""
        # Expensive calculation cached longer
```

### 3. Fix Dashboard Initialization

**Wrap dashboard in factory function for session isolation:**

```python
# src/aemo_dashboard/generation/gen_dash.py

def create_dashboard():
    """Factory function for per-session dashboard instances"""
    
    # Create dashboard instance
    dashboard = EnergyDashboard()
    
    # Register session cleanup
    pn.state.on_session_destroyed(lambda: dashboard.cleanup())
    
    return dashboard.template

# Change startup from:
# dashboard.template.servable()

# To:
pn.serve(create_dashboard, port=5006, title="AEMO Energy Dashboard")
```

### 4. Add Session Cleanup Hooks

```python
class EnergyDashboard:
    def cleanup(self):
        """Clean up resources on session end"""
        logger.info("Cleaning up dashboard session")
        
        # Close DuckDB connections
        if hasattr(self, 'data_service'):
            self.data_service.close_connection()
        
        # Clear any session-specific caches
        if hasattr(self, '_session_cache'):
            self._session_cache.clear()
```

### 5. Optimize Startup Performance

**Enable cache warming and lazy loading:**

```python
# startup_cache_warmer.py
def warm_cache():
    """Pre-populate commonly accessed data"""
    from datetime import datetime, timedelta
    
    # Common date ranges
    ranges = [
        (datetime.now() - timedelta(days=1), datetime.now()),  # Last 24h
        (datetime.now() - timedelta(days=7), datetime.now()),  # Last week
        (datetime.now() - timedelta(days=30), datetime.now()), # Last month
    ]
    
    # Pre-load common queries
    for start, end in ranges:
        query_manager.get_generation_by_fuel(start, end, 'NEM')
        query_manager.get_current_spot_prices()

# Run with --setup flag
# panel serve app.py --setup startup_cache_warmer.py
```

### 6. Production Deployment Configuration

**For serving to the internet:**

```bash
# Multi-process deployment (recommended)
panel serve src/aemo_dashboard/generation/gen_dash.py \
    --num-procs 4 \
    --port 5006 \
    --allow-websocket-origin="*" \
    --setup startup_cache_warmer.py \
    --warm

# With nginx reverse proxy for better performance
# Configure nginx to handle static assets and load balancing
```

## Performance Targets

With these changes:
- **Initial load**: < 2 seconds (with cache warming)
- **Concurrent users**: 50+ users without degradation
- **Memory per user**: ~50-100MB
- **Query response**: < 200ms for cached data

## Implementation Priority

1. **High Priority** (Do First)
   - Fix DuckDB connection management (prevents crashes)
   - Wrap dashboard in factory function (ensures session isolation)
   - Add @pn.cache to expensive operations

2. **Medium Priority**
   - Implement session cleanup hooks
   - Add cache warming script
   - Configure multi-process deployment

3. **Low Priority**
   - Fine-tune cache policies
   - Add cache monitoring
   - Implement client-side caching

## Testing Multi-User Scenarios

```python
# test_multiuser.py
import concurrent.futures
import requests
import time

def simulate_user(user_id):
    """Simulate a user session"""
    start = time.time()
    response = requests.get(f'http://localhost:5006')
    load_time = time.time() - start
    return user_id, response.status_code, load_time

# Test with 20 concurrent users
with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
    futures = [executor.submit(simulate_user, i) for i in range(20)]
    results = [f.result() for f in futures]
    
print(f"Average load time: {sum(r[2] for r in results) / len(results):.2f}s")
```

## 7. Persistent Caching for Fast Startup

### Problem
Even with optimizations, each new user session experiences:
- DuckDB initialization (~1-2s)
- View registration (~0.5s)
- Initial data queries (~1-3s)
- UI component creation (~1s)
Total: 3.5-6.5 seconds

### Solution: Multi-Level Persistent Caching

#### Level 1: Disk-Based Result Caching

```python
# src/aemo_dashboard/shared/persistent_cache.py
import diskcache
from pathlib import Path

# Create persistent cache directory
CACHE_DIR = Path.home() / '.aemo_dashboard_cache'
CACHE_DIR.mkdir(exist_ok=True)

# Initialize disk cache with 1GB limit
cache = diskcache.Cache(
    directory=str(CACHE_DIR),
    size_limit=1_073_741_824,  # 1GB
    eviction_policy='least-recently-used'
)

# Decorator for persistent caching
def persistent_cache(expire=3600):  # 1 hour default
    def decorator(func):
        def wrapper(*args, **kwargs):
            # Create cache key from function name and args
            key = f"{func.__name__}:{str(args)}:{str(kwargs)}"
            
            # Check cache first
            result = cache.get(key)
            if result is not None:
                return result
            
            # Compute and cache
            result = func(*args, **kwargs)
            cache.set(key, result, expire=expire)
            return result
        return wrapper
    return decorator

# Use in query managers
class GenerationQueryManager:
    @persistent_cache(expire=1800)  # 30 min for semi-static data
    def get_generation_overview(self, hours=24):
        """This will be cached to disk"""
        return self._query_generation_data(hours)
```

#### Level 2: Pre-computed Daily Aggregates

```python
# src/aemo_dashboard/shared/precompute_service.py
import schedule
import time

class PrecomputeService:
    """Background service to pre-compute common queries"""
    
    def __init__(self, query_manager):
        self.qm = query_manager
        
    def precompute_daily_stats(self):
        """Run daily at 2 AM"""
        logger.info("Pre-computing daily statistics...")
        
        # Common date ranges
        ranges = [
            ('24h', timedelta(hours=24)),
            ('7d', timedelta(days=7)),
            ('30d', timedelta(days=30)),
            ('90d', timedelta(days=90))
        ]
        
        # Pre-compute for each region
        regions = ['NEM', 'NSW1', 'QLD1', 'VIC1', 'SA1', 'TAS1']
        
        for label, delta in ranges:
            end_date = datetime.now()
            start_date = end_date - delta
            
            for region in regions:
                # These will be cached persistently
                self.qm.get_generation_by_fuel(start_date, end_date, region)
                self.qm.get_price_statistics(start_date, end_date, region)
                
        logger.info("Pre-computation complete")
    
    def run(self):
        # Schedule daily pre-computation
        schedule.every().day.at("02:00").do(self.precompute_daily_stats)
        
        while True:
            schedule.run_pending()
            time.sleep(60)
```

#### Level 3: Materialized Views in DuckDB

```python
# src/aemo_dashboard/shared/materialized_views.py

def create_materialized_views(conn):
    """Create materialized views for common aggregations"""
    
    # Daily generation summary (refreshed nightly)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS daily_generation_summary AS
        SELECT 
            DATE_TRUNC('day', settlementdate) as date,
            fuel_type,
            region,
            SUM(scadavalue) as total_generation,
            AVG(scadavalue) as avg_generation,
            COUNT(*) as data_points
        FROM generation_with_fuel_30min
        WHERE settlementdate >= CURRENT_DATE - INTERVAL '2 years'
        GROUP BY 1, 2, 3
    """)
    
    # Hourly price summary
    conn.execute("""
        CREATE TABLE IF NOT EXISTS hourly_price_summary AS
        SELECT 
            DATE_TRUNC('hour', settlementdate) as hour,
            regionid,
            AVG(rrp) as avg_price,
            MAX(rrp) as max_price,
            MIN(rrp) as min_price,
            STDDEV(rrp) as price_volatility
        FROM prices_30min
        WHERE settlementdate >= CURRENT_DATE - INTERVAL '6 months'
        GROUP BY 1, 2
    """)
    
    # Create indexes for fast lookup
    conn.execute("CREATE INDEX idx_dgs_date ON daily_generation_summary(date)")
    conn.execute("CREATE INDEX idx_hps_hour ON hourly_price_summary(hour)")
```

#### Level 4: Browser-Side Caching

```python
# src/aemo_dashboard/templates/cache_manager.js
// Progressive Web App approach with Service Worker
self.addEventListener('fetch', event => {
    event.respondWith(
        caches.match(event.request)
            .then(response => {
                // Cache hit - return response
                if (response) {
                    return response;
                }
                
                return fetch(event.request).then(
                    response => {
                        // Check if valid response
                        if(!response || response.status !== 200) {
                            return response;
                        }
                        
                        // Clone and cache the response
                        var responseToCache = response.clone();
                        caches.open('aemo-dashboard-v1')
                            .then(cache => {
                                cache.put(event.request, responseToCache);
                            });
                        
                        return response;
                    }
                );
            })
    );
});
```

### Implementation Strategy

1. **Immediate (Session 1)**: User experiences normal load time
2. **Subsequent Sessions**: 
   - Static assets served from browser cache (instant)
   - Common queries served from disk cache (<100ms)
   - Pre-computed aggregates for overview tabs (<50ms)
   - Only user-specific filters require computation

### Startup Time Improvements

| Component | Current | With Caching | Improvement |
|-----------|---------|--------------|-------------|
| Static Assets | 1-2s | <50ms | 95% |
| DuckDB Init | 1-2s | 1-2s* | - |
| Initial Query | 2-3s | <200ms | 90% |
| UI Render | 1s | <100ms | 90% |
| **Total** | **5-8s** | **<2s** | **75%** |

*DuckDB init still required but happens in parallel with cached data loading

### Cache Management

```python
# src/aemo_dashboard/cli/cache_manager.py
import click

@click.group()
def cache_cli():
    """AEMO Dashboard cache management"""
    pass

@cache_cli.command()
def clear():
    """Clear all caches"""
    cache.clear()
    click.echo("Cache cleared")

@cache_cli.command()
def stats():
    """Show cache statistics"""
    stats = cache.stats()
    click.echo(f"Cache size: {stats['size'] / 1024 / 1024:.1f} MB")
    click.echo(f"Hit rate: {stats['hits'] / (stats['hits'] + stats['misses']) * 100:.1f}%")

@cache_cli.command()
def warm():
    """Warm up cache with common queries"""
    from aemo_dashboard.shared.cache_warmer import warm_all_caches
    warm_all_caches()
    click.echo("Cache warmed")
```

## Summary

The dashboard has a solid foundation with DuckDB and smart caching. To achieve sub-2-second startup times:

1. **Critical fixes** for multi-user safety (connection management, session isolation)
2. **In-memory caching** with Panel's @pn.cache decorator
3. **Persistent disk caching** for common queries across sessions
4. **Pre-computed aggregates** refreshed nightly
5. **Browser caching** for static assets and API responses

This multi-level approach ensures:
- First visit: 3-5 seconds (acceptable)
- Subsequent visits: <2 seconds (excellent)
- Common queries: <200ms (instant feel)
- Static assets: <50ms (from browser cache)