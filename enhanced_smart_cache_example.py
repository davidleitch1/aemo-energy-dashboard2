"""
Enhanced SmartCache with Disk Persistence - Example Implementation
This shows how the cache would work with both memory and disk layers
"""

import diskcache
import pandas as pd
import time
from pathlib import Path
from typing import Optional, Dict, Any
import pickle
import logging

logger = logging.getLogger(__name__)

class EnhancedSmartCache:
    """
    Two-level cache: Memory (fast) + Disk (persistent)
    
    How it works:
    1. Check memory cache first (microseconds)
    2. If not in memory, check disk cache (milliseconds)
    3. If found on disk, promote to memory
    4. If not found anywhere, compute and store in both
    """
    
    def __init__(self, 
                 max_memory_mb: int = 100,
                 max_disk_gb: float = 1.0,
                 default_ttl: int = 300,
                 cache_dir: Optional[Path] = None):
        
        # Memory cache (existing implementation)
        self.memory_cache = {}  # Simplified for example
        self.memory_size = 0
        self.max_memory_bytes = max_memory_mb * 1024 * 1024
        self.default_ttl = default_ttl
        
        # Disk cache (new feature)
        cache_path = cache_dir or Path.home() / '.aemo_dashboard_cache'
        self.disk_cache = diskcache.Cache(
            str(cache_path),
            size_limit=int(max_disk_gb * 1024 * 1024 * 1024),
            eviction_policy='least-recently-used'
        )
        
        logger.info(f"Enhanced cache initialized: Memory={max_memory_mb}MB, Disk={max_disk_gb}GB")
    
    def get(self, key: str) -> Optional[pd.DataFrame]:
        """
        Retrieval process:
        1. Check memory cache (fastest)
        2. Check disk cache (fast)
        3. Return None if not found
        """
        
        # 1. Check memory cache first
        if key in self.memory_cache:
            timestamp, data, size = self.memory_cache[key]
            
            # Check if expired
            if time.time() - timestamp > self.default_ttl:
                del self.memory_cache[key]
                self.memory_size -= size
                logger.debug(f"Memory cache expired: {key}")
            else:
                logger.debug(f"Memory cache hit: {key}")
                return data.copy()
        
        # 2. Check disk cache
        try:
            disk_data = self.disk_cache.get(key)
            if disk_data is not None:
                # Found on disk - promote to memory if space available
                df = pd.DataFrame(disk_data)  # Reconstruct DataFrame
                logger.debug(f"Disk cache hit: {key}")
                
                # Try to promote to memory (but don't fail if no space)
                self._promote_to_memory(key, df)
                
                return df
        except Exception as e:
            logger.warning(f"Disk cache read error: {e}")
        
        # 3. Not found anywhere
        logger.debug(f"Cache miss: {key}")
        return None
    
    def put(self, key: str, data: pd.DataFrame, ttl: Optional[int] = None) -> None:
        """
        Storage process:
        1. Always try to store in memory (for speed)
        2. Always store on disk (for persistence)
        """
        
        ttl = ttl or self.default_ttl
        size = data.memory_usage(deep=True).sum()
        
        # 1. Try to store in memory
        if size <= self.max_memory_bytes:
            # Make space if needed
            while self.memory_size + size > self.max_memory_bytes and self.memory_cache:
                # Remove oldest item
                oldest_key = next(iter(self.memory_cache))
                _, _, old_size = self.memory_cache.pop(oldest_key)
                self.memory_size -= old_size
                logger.debug(f"Evicted from memory: {oldest_key}")
            
            # Store in memory
            self.memory_cache[key] = (time.time(), data.copy(), size)
            self.memory_size += size
            logger.debug(f"Stored in memory: {key} ({size/1024/1024:.1f}MB)")
        
        # 2. Store on disk (always, for persistence)
        try:
            # Convert DataFrame to dict for efficient storage
            disk_data = data.to_dict('records')
            self.disk_cache.set(key, disk_data, expire=ttl)
            logger.debug(f"Stored on disk: {key}")
        except Exception as e:
            logger.error(f"Disk cache write error: {e}")
    
    def _promote_to_memory(self, key: str, data: pd.DataFrame) -> None:
        """Promote disk cache entry to memory if space available"""
        size = data.memory_usage(deep=True).sum()
        if self.memory_size + size <= self.max_memory_bytes:
            self.memory_cache[key] = (time.time(), data.copy(), size)
            self.memory_size += size
            logger.debug(f"Promoted to memory: {key}")
    
    def clear_memory(self) -> None:
        """Clear only memory cache (keep disk cache)"""
        self.memory_cache.clear()
        self.memory_size = 0
        logger.info("Memory cache cleared")
    
    def clear_all(self) -> None:
        """Clear both memory and disk caches"""
        self.clear_memory()
        self.disk_cache.clear()
        logger.info("All caches cleared")
    
    def get_stats(self) -> Dict[str, Any]:
        """Get comprehensive cache statistics"""
        disk_stats = self.disk_cache.stats()
        
        return {
            'memory': {
                'entries': len(self.memory_cache),
                'size_mb': self.memory_size / 1024 / 1024,
                'max_size_mb': self.max_memory_bytes / 1024 / 1024,
                'utilization': (self.memory_size / self.max_memory_bytes * 100) if self.max_memory_bytes > 0 else 0
            },
            'disk': {
                'entries': len(self.disk_cache),
                'size_mb': disk_stats.get('size', 0) / 1024 / 1024,
                'hits': disk_stats.get('hits', 0),
                'misses': disk_stats.get('misses', 0)
            }
        }


# Example usage showing benefits:
if __name__ == "__main__":
    import numpy as np
    
    # Create cache
    cache = EnhancedSmartCache(max_memory_mb=50, max_disk_gb=0.5)
    
    # Simulate expensive query result
    print("1. Creating large DataFrame...")
    large_df = pd.DataFrame(np.random.rand(100000, 20))
    
    # Store it
    print("2. Storing in cache...")
    cache.put('expensive_query', large_df, ttl=3600)  # 1 hour TTL
    
    # Retrieve from memory (instant)
    print("3. Retrieving from memory cache...")
    start = time.time()
    df1 = cache.get('expensive_query')
    print(f"   Memory retrieval: {(time.time() - start)*1000:.1f}ms")
    
    # Clear memory to simulate restart
    print("4. Simulating dashboard restart...")
    cache.clear_memory()
    
    # Retrieve from disk (still fast)
    print("5. Retrieving from disk cache...")
    start = time.time()
    df2 = cache.get('expensive_query')
    print(f"   Disk retrieval: {(time.time() - start)*1000:.1f}ms")
    
    # Show stats
    print("\n6. Cache statistics:")
    stats = cache.get_stats()
    print(f"   Memory: {stats['memory']['entries']} entries, {stats['memory']['size_mb']:.1f}MB")
    print(f"   Disk: {stats['disk']['entries']} entries, {stats['disk']['size_mb']:.1f}MB")
    
    print("\n✅ Benefits demonstrated:")
    print("   - Data survives restarts")
    print("   - Fast retrieval from disk")
    print("   - Automatic memory/disk management")
    print("   - No need to recompute expensive queries")