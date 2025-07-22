# Revised Disk Cache Implementation Plan

## Critical Issues Identified and Solutions

### 1. ⚠️ File Naming Issue
**Problem**: Cache keys can exceed OS filename limits (255 chars)
**Solution**: Use MD5 hash for filenames
```python
import hashlib

def _get_disk_filename(self, key: str) -> str:
    """Convert cache key to safe filename"""
    key_hash = hashlib.md5(key.encode()).hexdigest()
    return f"{key_hash}.cache"
```

### 2. ⚠️ Threading with Panel
**Problem**: Raw threads can interfere with Panel's event loop
**Solution**: Use Panel-compatible async approach
```python
import asyncio
from concurrent.futures import ThreadPoolExecutor

class SmartCache:
    def __init__(self, ...):
        # Single-threaded executor for disk I/O
        self._disk_executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="DiskCache")
        self._write_queue = asyncio.Queue(maxsize=100)
```

### 3. ⚠️ Memory Promotion Logic
**Problem**: Loading from disk could trigger memory evictions
**Solution**: Check available space before promotion
```python
def _can_fit_in_memory(self, size: int) -> bool:
    """Check if data can fit without excessive eviction"""
    available = self.max_size_bytes - self.current_size
    # Only promote if we have 20% headroom after loading
    return size < (available + 0.2 * self.max_size_bytes)
```

### 4. ⚠️ Startup Performance
**Problem**: Cache warming could slow startup
**Solution**: Lazy metadata loading
```python
class DiskCacheManifest:
    """Lightweight manifest of disk cache contents"""
    def __init__(self, cache_dir: str):
        self.manifest_file = os.path.join(cache_dir, "manifest.json")
        self.entries = {}  # key -> (filename, size, timestamp)
        self._load_manifest()  # Fast JSON load
```

## Revised Implementation

### Phase 1: Safe File Operations
```python
import os
import json
import hashlib
import fcntl  # For file locking on Unix
import pickle
import zstandard as zstd
from pathlib import Path
from typing import Optional, Tuple, Dict, Any

class DiskCache:
    """Disk cache layer for SmartCache"""
    
    def __init__(self, cache_dir: str, max_size_mb: int = 1024):
        self.cache_dir = Path(cache_dir).expanduser()
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.max_size_bytes = max_size_mb * 1024 * 1024
        self.manifest = DiskCacheManifest(self.cache_dir)
        self.compressor = zstd.ZstdCompressor(level=3)
        self.decompressor = zstd.ZstdDecompressor()
        
    def _get_cache_path(self, key: str) -> Tuple[Path, Path]:
        """Get paths for data and metadata files"""
        key_hash = hashlib.md5(key.encode()).hexdigest()
        data_path = self.cache_dir / f"{key_hash}.pkl.zst"
        meta_path = self.cache_dir / f"{key_hash}.meta"
        return data_path, meta_path
        
    def save(self, key: str, data: pd.DataFrame, metadata: dict) -> bool:
        """Save DataFrame to disk with compression"""
        try:
            data_path, meta_path = self._get_cache_path(key)
            
            # Save metadata first (fast)
            metadata['key'] = key  # Store original key
            metadata['size'] = data.memory_usage(deep=True).sum()
            with open(meta_path, 'w') as f:
                json.dump(metadata, f)
            
            # Save compressed data
            serialized = pickle.dumps(data, protocol=5)
            compressed = self.compressor.compress(serialized)
            
            # Atomic write with temp file
            temp_path = data_path.with_suffix('.tmp')
            with open(temp_path, 'wb') as f:
                f.write(compressed)
            temp_path.rename(data_path)
            
            # Update manifest
            self.manifest.add_entry(key, data_path.name, len(compressed), metadata)
            
            return True
            
        except Exception as e:
            logger.warning(f"Failed to save to disk cache: {e}")
            return False
            
    def load(self, key: str) -> Optional[Tuple[pd.DataFrame, dict]]:
        """Load DataFrame from disk"""
        try:
            data_path, meta_path = self._get_cache_path(key)
            
            if not data_path.exists() or not meta_path.exists():
                return None
                
            # Load metadata
            with open(meta_path, 'r') as f:
                metadata = json.load(f)
                
            # Load compressed data
            with open(data_path, 'rb') as f:
                compressed = f.read()
                
            # Decompress and deserialize
            serialized = self.decompressor.decompress(compressed)
            data = pickle.loads(serialized)
            
            return data, metadata
            
        except Exception as e:
            logger.warning(f"Failed to load from disk cache: {e}")
            # Remove corrupted entry
            self.delete(key)
            return None
```

### Phase 2: Enhanced SmartCache
```python
class SmartCache:
    """LRU cache with TTL and optional disk persistence"""
    
    def __init__(self, 
                 max_size_mb: int = 100, 
                 default_ttl: int = 300,
                 disk_cache_dir: Optional[str] = None,
                 disk_cache_max_size_mb: int = 1024):
        
        # Memory cache (existing)
        self.max_size_bytes = max_size_mb * 1024 * 1024
        self.default_ttl = default_ttl
        self.cache = OrderedDict()
        self.size_tracker = {}
        self.current_size = 0
        self._lock = threading.Lock()
        
        # Disk cache (new)
        self.disk_cache = None
        if disk_cache_dir:
            try:
                self.disk_cache = DiskCache(disk_cache_dir, disk_cache_max_size_mb)
                logger.info(f"Disk cache enabled: {disk_cache_dir}")
            except Exception as e:
                logger.error(f"Failed to initialize disk cache: {e}")
                
        # Stats
        self.stats = {
            'memory_hits': 0,
            'disk_hits': 0,
            'misses': 0,
            'disk_writes': 0,
            'disk_write_failures': 0
        }
    
    def get(self, key: str) -> Optional[pd.DataFrame]:
        """Get item from cache (memory first, then disk)"""
        with self._lock:
            # Try memory cache first
            if key in self.cache:
                timestamp, data, size = self.cache[key]
                
                # Check TTL
                if time.time() - timestamp > self.default_ttl:
                    # Expired - remove from memory
                    self.current_size -= size
                    del self.cache[key]
                    del self.size_tracker[key]
                else:
                    # Valid memory hit
                    self.cache.move_to_end(key)
                    self.stats['memory_hits'] += 1
                    return data.copy()
            
            # Try disk cache if enabled
            if self.disk_cache:
                disk_result = self.disk_cache.load(key)
                if disk_result:
                    data, metadata = disk_result
                    
                    # Check TTL
                    if time.time() - metadata.get('timestamp', 0) > self.default_ttl:
                        # Expired - remove from disk
                        self.disk_cache.delete(key)
                    else:
                        # Valid disk hit
                        self.stats['disk_hits'] += 1
                        
                        # Optionally promote to memory if space available
                        size = metadata.get('size', 0)
                        if self._can_fit_in_memory(size):
                            self._put_memory_only(key, data, size)
                            
                        return data.copy()
            
            # Cache miss
            self.stats['misses'] += 1
            return None
    
    def put(self, key: str, data: pd.DataFrame, ttl: Optional[int] = None) -> None:
        """Store item in cache (memory and optionally disk)"""
        with self._lock:
            size = self._estimate_dataframe_size(data)
            
            # Memory cache
            if size <= self.max_size_bytes:
                self._evict_lru(size)
                timestamp = time.time()
                self.cache[key] = (timestamp, data.copy(), size)
                self.size_tracker[key] = size
                self.current_size += size
                
                # Async disk write if enabled
                if self.disk_cache:
                    metadata = {
                        'timestamp': timestamp,
                        'ttl': ttl or self.default_ttl,
                        'size': size
                    }
                    # Non-blocking disk write
                    self._schedule_disk_write(key, data, metadata)
            else:
                logger.warning(f"DataFrame too large for cache: {size/1024/1024:.1f}MB")
    
    def _schedule_disk_write(self, key: str, data: pd.DataFrame, metadata: dict):
        """Schedule async disk write without blocking"""
        def write_task():
            if self.disk_cache.save(key, data, metadata):
                self.stats['disk_writes'] += 1
            else:
                self.stats['disk_write_failures'] += 1
                
        # Submit to thread pool (non-blocking)
        try:
            from concurrent.futures import ThreadPoolExecutor
            if not hasattr(self, '_disk_executor'):
                self._disk_executor = ThreadPoolExecutor(max_workers=1)
            self._disk_executor.submit(write_task)
        except Exception as e:
            logger.warning(f"Failed to schedule disk write: {e}")
```

### Phase 3: Integration Points

1. **No changes needed to existing code** - all managers will automatically get disk caching if configured

2. **Configuration via environment variables**:
```bash
# In .env file
ENABLE_DISK_CACHE=true
DISK_CACHE_DIR=~/.aemo_dashboard_cache
DISK_CACHE_MAX_MB=1024
```

3. **Startup sequence remains fast**:
- Disk cache manifest loads quickly (JSON file)
- No eager loading of cached data
- First access might be slightly slower (disk read)

### Phase 4: Panel-Safe Implementation

```python
# In HybridQueryManager.__init__
if os.getenv('ENABLE_DISK_CACHE', 'false').lower() == 'true':
    cache_dir = os.getenv('DISK_CACHE_DIR', '~/.aemo_dashboard_cache')
    disk_size = int(os.getenv('DISK_CACHE_MAX_MB', '1024'))
    self.cache = SmartCache(
        max_size_mb=cache_size_mb, 
        default_ttl=cache_ttl,
        disk_cache_dir=cache_dir,
        disk_cache_max_size_mb=disk_size
    )
else:
    # Standard memory-only cache
    self.cache = SmartCache(max_size_mb=cache_size_mb, default_ttl=cache_ttl)
```

## Testing Plan

1. **Unit tests** for DiskCache class
2. **Integration tests** with SmartCache
3. **Performance tests** comparing memory vs disk
4. **Multi-process tests** for concurrent access
5. **Panel dashboard tests** for UI responsiveness

## Rollback
Simply set `ENABLE_DISK_CACHE=false` to disable disk caching completely.

## Success Metrics
- ✅ No changes required to existing code
- ✅ Dashboard startup time unchanged (lazy loading)
- ✅ Second dashboard start < 2s with warm cache
- ✅ No UI blocking during disk operations
- ✅ Graceful degradation on disk errors