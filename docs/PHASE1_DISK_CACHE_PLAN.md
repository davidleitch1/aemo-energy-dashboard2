# Phase 1: Enhance SmartCache with Disk Persistence

## Overview
Add disk persistence to the existing SmartCache to survive dashboard restarts and improve response times for repeated queries.

## Current State
- SmartCache exists but is memory-only
- Cache is lost on dashboard restart
- Every restart requires re-computing all queries

## Implementation Plan

### 1. Enhance SmartCache Class
Location: `src/aemo_dashboard/shared/hybrid_query_manager.py`

Add:
- Disk cache directory configuration
- Serialize cache entries to disk on write
- Load cache from disk on startup
- Background sync to prevent I/O blocking

### 2. Cache Key Strategy
- Include query hash, date range, and parameters
- Add version identifier for cache invalidation
- Use file-safe naming convention

### 3. Storage Format
- Use pickle for DataFrame serialization (fastest)
- Alternative: Parquet for better compression
- Store metadata separately (JSON)

### 4. Cache Management
- Set max disk cache size (e.g., 1GB)
- Implement LRU eviction for disk cache
- Add cache statistics tracking

### 5. Configuration
```python
DISK_CACHE_CONFIG = {
    'enabled': True,
    'directory': '~/.aemo_dashboard_cache',
    'max_size_mb': 1024,
    'sync_interval': 60,  # seconds
    'format': 'pickle'  # or 'parquet'
}
```

## Expected Benefits
1. **Faster Restarts**: Common queries load from disk (~100ms vs 11s)
2. **Persistent Performance**: Cache survives restarts
3. **Shared Cache**: Multiple processes can share cache
4. **Reduced Load**: Less repeated computation

## Success Metrics
- Dashboard restart time < 2 seconds (with warm cache)
- Cache hit rate > 80% for common queries
- Disk cache size < 1GB for typical usage

## Testing Plan
1. Implement disk cache
2. Test cache persistence across restarts
3. Measure performance improvement
4. Verify cache invalidation works
5. Test concurrent access to disk cache