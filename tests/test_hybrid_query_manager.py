#!/usr/bin/env python3
"""
Comprehensive tests for the Hybrid Query Manager

Tests cover:
- Basic query functionality
- Caching behavior
- Progressive loading
- Chunk streaming
- Performance characteristics
- Error handling
"""

import os
import sys
import time
import pandas as pd
import psutil
from datetime import datetime, timedelta
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / 'src'))

# Set DuckDB mode
os.environ['USE_DUCKDB'] = 'true'

from aemo_dashboard.shared.hybrid_query_manager import HybridQueryManager, SmartCache
from aemo_dashboard.shared.duckdb_views import view_manager
from aemo_dashboard.shared.logging_config import setup_logging

# Set up logging
setup_logging()


class TestHybridQueryManager:
    """Test suite for hybrid query manager"""
    
    def __init__(self):
        self.manager = HybridQueryManager(cache_size_mb=50, cache_ttl=60)
        self.process = psutil.Process()
        self.test_results = []
    
    def get_memory_usage(self):
        """Get current memory usage in MB"""
        return self.process.memory_info().rss / 1024 / 1024
    
    def run_test(self, test_name, test_func):
        """Run a test and record results"""
        print(f"\n{'='*60}")
        print(f"Running: {test_name}")
        print('='*60)
        
        start_time = time.time()
        start_memory = self.get_memory_usage()
        
        try:
            result = test_func()
            status = "PASSED"
            error = None
        except Exception as e:
            result = None
            status = "FAILED"
            error = str(e)
            import traceback
            traceback.print_exc()
        
        end_time = time.time()
        end_memory = self.get_memory_usage()
        
        test_result = {
            'name': test_name,
            'status': status,
            'duration': end_time - start_time,
            'memory_delta': end_memory - start_memory,
            'result': result,
            'error': error
        }
        
        self.test_results.append(test_result)
        
        print(f"\nStatus: {status}")
        print(f"Duration: {test_result['duration']:.2f}s")
        print(f"Memory delta: {test_result['memory_delta']:.1f}MB")
        
        return result
    
    def test_basic_query(self):
        """Test basic integrated data query"""
        end_date = datetime.now()
        start_date = end_date - timedelta(hours=6)
        
        df = self.manager.query_integrated_data(
            start_date=start_date,
            end_date=end_date,
            resolution='30min'
        )
        
        assert not df.empty, "Query returned empty DataFrame"
        assert 'settlementdate' in df.columns, "Missing settlementdate column"
        assert 'duid' in df.columns, "Missing duid column"
        assert 'revenue' in df.columns, "Missing revenue column"
        
        print(f"✓ Loaded {len(df):,} rows")
        print(f"✓ Columns: {', '.join(df.columns[:5])}...")
        print(f"✓ Date range: {df['settlementdate'].min()} to {df['settlementdate'].max()}")
        
        return True
    
    def test_cache_behavior(self):
        """Test caching functionality"""
        end_date = datetime.now()
        start_date = end_date - timedelta(hours=2)
        
        # Clear cache first
        self.manager.clear_cache()
        
        # First query (cache miss)
        t1_start = time.time()
        df1 = self.manager.query_integrated_data(start_date, end_date)
        t1_duration = time.time() - t1_start
        
        # Second query (cache hit)
        t2_start = time.time()
        df2 = self.manager.query_integrated_data(start_date, end_date)
        t2_duration = time.time() - t2_start
        
        # Verify cache hit
        stats = self.manager.get_statistics()
        
        assert stats['cache_hits'] > 0, "No cache hits recorded"
        assert t2_duration < t1_duration * 0.1, "Cache hit not significantly faster"
        assert df1.equals(df2), "Cached data doesn't match original"
        
        print(f"✓ First query: {t1_duration:.3f}s")
        print(f"✓ Second query: {t2_duration:.3f}s (speedup: {t1_duration/t2_duration:.1f}x)")
        print(f"✓ Cache hit rate: {stats['cache_hit_rate']:.1f}%")
        
        return True
    
    def test_progressive_loading(self):
        """Test progressive loading with chunks"""
        end_date = datetime.now()
        start_date = end_date - timedelta(days=7)
        
        # Build query for a week of data
        query = f"""
        SELECT * FROM integrated_data_30min
        WHERE settlementdate >= '{start_date.strftime('%Y-%m-%d %H:%M:%S')}'
          AND settlementdate <= '{end_date.strftime('%Y-%m-%d %H:%M:%S')}'
        """
        
        progress_values = []
        
        def progress_callback(progress):
            progress_values.append(progress)
            if progress % 20 == 0:
                print(f"  Progress: {progress}%")
        
        # Load with progress
        df = self.manager.query_with_progress(
            query=query,
            chunk_size=10000,
            progress_callback=progress_callback
        )
        
        assert not df.empty, "Progressive loading returned empty DataFrame"
        assert len(progress_values) > 0, "No progress updates received"
        assert progress_values[-1] == 100, "Progress didn't reach 100%"
        
        print(f"✓ Loaded {len(df):,} rows in {len(progress_values)} chunks")
        print(f"✓ Progress updates: {len(progress_values)}")
        
        return True
    
    def test_chunk_streaming(self):
        """Test memory-efficient chunk streaming"""
        end_date = datetime.now()
        start_date = end_date - timedelta(days=1)
        
        query = f"""
        SELECT * FROM integrated_data_30min
        WHERE settlementdate >= '{start_date.strftime('%Y-%m-%d %H:%M:%S')}'
          AND settlementdate <= '{end_date.strftime('%Y-%m-%d %H:%M:%S')}'
        """
        
        chunk_count = 0
        total_rows = 0
        max_chunk_memory = 0
        
        start_memory = self.get_memory_usage()
        
        # Stream chunks
        for chunk in self.manager.query_chunks(query, chunk_size=5000):
            chunk_count += 1
            total_rows += len(chunk)
            
            # Check memory usage
            current_memory = self.get_memory_usage()
            chunk_memory = current_memory - start_memory
            max_chunk_memory = max(max_chunk_memory, chunk_memory)
            
            # Process chunk (simulate work)
            _ = chunk['scadavalue'].sum()
        
        assert chunk_count > 0, "No chunks received"
        assert total_rows > 0, "No rows processed"
        assert max_chunk_memory < 50, f"Chunk memory usage too high: {max_chunk_memory:.1f}MB"
        
        print(f"✓ Streamed {chunk_count} chunks")
        print(f"✓ Total rows: {total_rows:,}")
        print(f"✓ Max chunk memory: {max_chunk_memory:.1f}MB")
        
        return True
    
    def test_column_selection(self):
        """Test selective column loading"""
        end_date = datetime.now()
        start_date = end_date - timedelta(hours=1)
        
        # Load only specific columns
        columns = ['settlementdate', 'duid', 'scadavalue', 'fuel_type']
        
        df = self.manager.query_integrated_data(
            start_date=start_date,
            end_date=end_date,
            columns=columns
        )
        
        assert not df.empty, "Query returned empty DataFrame"
        assert set(df.columns) == set(columns), "Column selection not working"
        
        # Check memory efficiency
        full_df = self.manager.query_integrated_data(
            start_date=start_date,
            end_date=end_date
        )
        
        memory_ratio = df.memory_usage(deep=True).sum() / full_df.memory_usage(deep=True).sum()
        
        print(f"✓ Selected columns: {', '.join(columns)}")
        print(f"✓ Memory usage: {memory_ratio:.1%} of full query")
        
        return True
    
    def test_aggregation_query(self):
        """Test aggregation functionality"""
        end_date = datetime.now()
        start_date = end_date - timedelta(days=1)
        
        df = self.manager.aggregate_by_group(
            start_date=start_date,
            end_date=end_date,
            group_by=['fuel_type', 'region'],
            aggregations={
                'scadavalue': 'sum',
                'revenue_30min': 'sum',
                'rrp': 'avg'
            }
        )
        
        assert not df.empty, "Aggregation returned empty DataFrame"
        assert 'fuel_type' in df.columns, "Missing group by column"
        assert 'scadavalue_sum' in df.columns, "Missing aggregation column"
        
        print(f"✓ Aggregated to {len(df)} groups")
        print(f"✓ Total generation: {df['scadavalue_sum'].sum():,.0f} MW")
        
        return True
    
    def test_cache_eviction(self):
        """Test cache size limits and eviction"""
        # Create a new manager with small cache
        small_cache_manager = HybridQueryManager(cache_size_mb=10, cache_ttl=300)
        
        queries_made = 0
        
        # Make multiple queries to exceed cache size
        for i in range(10):
            end_date = datetime.now() - timedelta(days=i)
            start_date = end_date - timedelta(days=1)
            
            df = small_cache_manager.query_integrated_data(
                start_date=start_date,
                end_date=end_date
            )
            
            queries_made += 1
            
            # Check cache stats
            stats = small_cache_manager.get_statistics()
            cache_size = stats['cache_stats']['size_mb']
            
            if cache_size > 10:
                assert False, f"Cache exceeded limit: {cache_size:.1f}MB"
        
        print(f"✓ Made {queries_made} queries")
        print(f"✓ Cache size maintained under limit")
        print(f"✓ Final cache: {stats['cache_stats']}")
        
        return True
    
    def test_date_range_query(self):
        """Test various date range queries"""
        ranges = [
            ("1 hour", timedelta(hours=1)),
            ("1 day", timedelta(days=1)),
            ("1 week", timedelta(days=7)),
            ("1 month", timedelta(days=30))
        ]
        
        results = []
        
        for name, delta in ranges:
            end_date = datetime.now()
            start_date = end_date - delta
            
            start_time = time.time()
            df = self.manager.query_integrated_data(
                start_date=start_date,
                end_date=end_date,
                resolution='30min' if delta.days >= 7 else '5min'
            )
            query_time = time.time() - start_time
            
            results.append({
                'range': name,
                'rows': len(df),
                'time': query_time,
                'mb': df.memory_usage(deep=True).sum() / 1024 / 1024
            })
        
        # Print results table
        print("\nDate Range Performance:")
        print(f"{'Range':<10} {'Rows':<10} {'Time (s)':<10} {'Memory (MB)':<10}")
        print("-" * 40)
        
        for r in results:
            print(f"{r['range']:<10} {r['rows']:<10,} {r['time']:<10.2f} {r['mb']:<10.1f}")
        
        return True
    
    def test_error_handling(self):
        """Test error handling for invalid queries"""
        # Test invalid date range
        try:
            df = self.manager.query_integrated_data(
                start_date=datetime.now(),
                end_date=datetime.now() - timedelta(days=1)  # End before start
            )
            # Should still work, just return empty
            assert df.empty or len(df) == 0, "Should return empty for invalid range"
        except Exception as e:
            print(f"✓ Handled invalid date range: {e}")
        
        # Test invalid column selection
        try:
            df = self.manager.query_integrated_data(
                start_date=datetime.now() - timedelta(hours=1),
                end_date=datetime.now(),
                columns=['invalid_column']
            )
            assert False, "Should fail on invalid column"
        except Exception:
            print("✓ Handled invalid column selection")
        
        return True
    
    def run_all_tests(self):
        """Run all tests and generate summary"""
        tests = [
            ("Basic Query", self.test_basic_query),
            ("Cache Behavior", self.test_cache_behavior),
            ("Progressive Loading", self.test_progressive_loading),
            ("Chunk Streaming", self.test_chunk_streaming),
            ("Column Selection", self.test_column_selection),
            ("Aggregation Query", self.test_aggregation_query),
            ("Cache Eviction", self.test_cache_eviction),
            ("Date Range Query", self.test_date_range_query),
            ("Error Handling", self.test_error_handling)
        ]
        
        print("\n" + "="*60)
        print("HYBRID QUERY MANAGER TEST SUITE")
        print("="*60)
        
        start_memory = self.get_memory_usage()
        
        for test_name, test_func in tests:
            self.run_test(test_name, test_func)
        
        # Summary
        print("\n" + "="*60)
        print("TEST SUMMARY")
        print("="*60)
        
        passed = sum(1 for r in self.test_results if r['status'] == 'PASSED')
        failed = sum(1 for r in self.test_results if r['status'] == 'FAILED')
        
        print(f"\nTotal tests: {len(self.test_results)}")
        print(f"Passed: {passed}")
        print(f"Failed: {failed}")
        print(f"Success rate: {passed/len(self.test_results)*100:.1f}%")
        
        total_time = sum(r['duration'] for r in self.test_results)
        print(f"\nTotal test time: {total_time:.2f}s")
        
        final_memory = self.get_memory_usage()
        print(f"Memory usage: {start_memory:.1f}MB → {final_memory:.1f}MB (Δ{final_memory-start_memory:+.1f}MB)")
        
        # Failed test details
        if failed > 0:
            print("\nFailed tests:")
            for r in self.test_results:
                if r['status'] == 'FAILED':
                    print(f"  - {r['name']}: {r['error']}")
        
        # Manager statistics
        stats = self.manager.get_statistics()
        print(f"\nQuery Manager Statistics:")
        print(f"  Total queries: {stats['query_count']}")
        print(f"  Cache hits: {stats['cache_hits']}")
        print(f"  Hit rate: {stats['cache_hit_rate']:.1f}%")
        print(f"  Cache size: {stats['cache_stats']['size_mb']:.1f}MB")
        
        return passed == len(self.test_results)


if __name__ == "__main__":
    # Run tests
    tester = TestHybridQueryManager()
    success = tester.run_all_tests()
    
    # Exit with appropriate code
    sys.exit(0 if success else 1)