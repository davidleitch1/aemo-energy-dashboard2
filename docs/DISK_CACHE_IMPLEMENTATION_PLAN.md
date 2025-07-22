# Disk Cache Implementation Plan for SmartCache

## Current SmartCache Analysis

### Architecture
- **SmartCache**: In-memory LRU cache with TTL and size limits
- **Storage**: OrderedDict for LRU ordering
- **Size tracking**: Estimates DataFrame memory usage
- **Thread safety**: Uses threading.Lock for all operations
- **Eviction**: LRU eviction when size limit reached

### Current Usage
1. **HybridQueryManager**: Base class using 100MB cache, 300s TTL
2. **GenerationQueryManager**: Uses 200MB cache, 300s TTL
3. **StationAnalysis**: Uses 100MB cache, 300s TTL
4. **PriceAnalysis**: Uses 100MB cache, 300s TTL
5. **NEMDashQueryManager**: Uses 100MB cache, 300s TTL

### Key Methods
- `get()`: Returns cached DataFrame copy if valid
- `put()`: Stores DataFrame copy with size tracking
- `_evict_lru()`: Removes oldest entries to make space
- `clear()`: Clears all cache entries
- `get_stats()`: Returns cache statistics

## Disk Cache Design

### 1. Architecture Decision
**Approach**: Extend SmartCache with optional disk persistence layer

```
Memory Cache (current SmartCache)
    ↓ write-through
Disk Cache (new layer)
    ↓ read fallback
Query Execution
```

### 2. Implementation Strategy

#### Option A: Pickle-based (Recommended)
- **Pros**: Fast serialization, preserves all pandas features
- **Cons**: Python-specific, version sensitivity
- **File structure**: `{cache_dir}/{cache_key}.pkl` + `{cache_key}.meta`

#### Option B: Parquet-based
- **Pros**: Efficient compression, cross-language
- **Cons**: Slower than pickle, may lose some pandas metadata
- **File structure**: `{cache_dir}/{cache_key}.parquet` + `{cache_key}.meta`

### 3. Detailed Implementation Plan

#### Phase 1: Add Disk Cache Configuration
```python
class SmartCache:
    def __init__(self, 
                 max_size_mb: int = 100, 
                 default_ttl: int = 300,
                 disk_cache_dir: Optional[str] = None,
                 disk_cache_max_size_mb: int = 1024,
                 disk_cache_format: str = 'pickle'):
```

#### Phase 2: Implement Disk Operations
1. **Cache directory management**
   - Create directory if not exists
   - Handle permissions errors
   - Implement size tracking

2. **Serialization methods**
   ```python
   def _save_to_disk(self, key: str, data: pd.DataFrame, metadata: dict) -> bool
   def _load_from_disk(self, key: str) -> Optional[Tuple[pd.DataFrame, dict]]
   def _delete_from_disk(self, key: str) -> None
   ```

3. **Disk cache management**
   ```python
   def _get_disk_cache_size(self) -> int
   def _evict_disk_lru(self, required_space: int) -> None
   def _list_disk_entries(self) -> List[Tuple[str, float, int]]
   ```

#### Phase 3: Integrate with Existing Methods
1. **Modified `get()` method**
   - Check memory cache first
   - If miss, check disk cache
   - If disk hit, optionally promote to memory

2. **Modified `put()` method**
   - Store in memory as before
   - Asynchronously write to disk
   - Handle disk write failures gracefully

3. **Modified `clear()` method**
   - Clear memory cache
   - Optionally clear disk cache

#### Phase 4: Add Background Tasks
1. **Disk sync thread**
   - Batch disk writes for efficiency
   - Handle write queue
   - Periodic disk cleanup

2. **Cache warming**
   - Load frequently used entries on startup
   - Background prefetch based on patterns

### 4. Configuration Schema
```python
DISK_CACHE_CONFIG = {
    'enabled': os.getenv('ENABLE_DISK_CACHE', 'true').lower() == 'true',
    'directory': os.getenv('DISK_CACHE_DIR', '~/.aemo_dashboard_cache'),
    'max_size_mb': int(os.getenv('DISK_CACHE_MAX_MB', '1024')),
    'format': os.getenv('DISK_CACHE_FORMAT', 'pickle'),  # 'pickle' or 'parquet'
    'sync_interval': int(os.getenv('DISK_CACHE_SYNC_INTERVAL', '5')),
    'cleanup_interval': int(os.getenv('DISK_CACHE_CLEANUP_INTERVAL', '300')),
    'compression': os.getenv('DISK_CACHE_COMPRESSION', 'zstd')  # for pickle
}
```

### 5. Error Handling
1. **Disk full**: Log warning, continue with memory-only
2. **Permission denied**: Disable disk cache, log error
3. **Corrupt cache file**: Delete and regenerate
4. **Version mismatch**: Clear old cache entries

### 6. Monitoring & Metrics
```python
disk_cache_stats = {
    'hits': 0,
    'misses': 0,
    'writes': 0,
    'write_failures': 0,
    'size_mb': 0,
    'entry_count': 0,
    'evictions': 0
}
```

## Implementation Order

1. **Step 1**: Add configuration parameters (non-breaking)
2. **Step 2**: Implement disk I/O methods (isolated)
3. **Step 3**: Add disk fallback to get() (transparent)
4. **Step 4**: Add disk write to put() (async, non-blocking)
5. **Step 5**: Add background sync thread
6. **Step 6**: Add monitoring and management tools

## Testing Strategy

1. **Unit Tests**
   - Test disk I/O operations
   - Test cache eviction logic
   - Test error handling

2. **Integration Tests**
   - Test with real dashboard queries
   - Test concurrent access
   - Test cache persistence across restarts

3. **Performance Tests**
   - Measure disk cache overhead
   - Compare pickle vs parquet performance
   - Test with various cache sizes

## Potential Issues & Solutions

### Issue 1: Concurrent File Access
**Problem**: Multiple processes accessing same cache files
**Solution**: Use file locking or process-specific cache directories

### Issue 2: Cache Invalidation
**Problem**: Data updates making cache stale
**Solution**: Add data version tracking or timestamp-based invalidation

### Issue 3: Disk I/O Blocking UI
**Problem**: Disk writes could block UI updates
**Solution**: Use background thread with write queue

### Issue 4: Cache Key Collisions
**Problem**: Different queries producing same cache key
**Solution**: Include query hash in cache key generation

### Issue 5: Memory → Disk Synchronization
**Problem**: Memory and disk cache getting out of sync
**Solution**: Write-through approach with async disk writes

## Rollback Plan
1. Set `ENABLE_DISK_CACHE=false` environment variable
2. System continues with memory-only cache
3. No code changes required for rollback

## Success Criteria
1. ✅ Dashboard restart with warm cache < 2 seconds
2. ✅ Cache hit rate > 80% for repeated queries
3. ✅ No UI blocking during disk operations
4. ✅ Disk cache size stays under configured limit
5. ✅ Multi-user scenarios work without conflicts