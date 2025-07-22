#!/usr/bin/env python3
"""
Test the generation dashboard DuckDB views
"""

import os
import sys
from pathlib import Path
from datetime import datetime, timedelta

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / 'src'))

# Set DuckDB mode
os.environ['USE_DUCKDB'] = 'true'

from aemo_dashboard.shared.duckdb_views import view_manager
from data_service.shared_data_duckdb import duckdb_data_service
from aemo_dashboard.shared.logging_config import setup_logging

# Set up logging
setup_logging()


def test_generation_views():
    """Test the generation dashboard views"""
    print("="*60)
    print("TESTING GENERATION DASHBOARD VIEWS")
    print("="*60)
    
    # Ensure views are created
    view_manager.create_all_views()
    
    # Test 1: Check if views were created
    print("\n1. Checking created views...")
    views = view_manager.get_view_list()
    generation_views = [v for v in views if 'generation' in v or 'fuel' in v or 'capacity' in v]
    
    print(f"Found {len(generation_views)} generation-related views:")
    for view in generation_views:
        print(f"  - {view}")
    
    # Test 2: Query generation_by_fuel_30min view
    print("\n2. Testing generation_by_fuel_30min view...")
    try:
        # Query last 24 hours
        end_date = datetime.now()
        start_date = end_date - timedelta(days=1)
        
        query = f"""
        SELECT 
            fuel_type,
            COUNT(DISTINCT settlementdate) as time_periods,
            COUNT(DISTINCT region) as regions,
            SUM(total_generation_mw) as total_generation,
            AVG(total_generation_mw) as avg_generation
        FROM generation_by_fuel_30min
        WHERE settlementdate >= '{start_date.strftime('%Y-%m-%d %H:%M:%S')}'
        AND settlementdate <= '{end_date.strftime('%Y-%m-%d %H:%M:%S')}'
        GROUP BY fuel_type
        ORDER BY avg_generation DESC
        """
        
        result = duckdb_data_service.conn.execute(query).df()
        print(f"✓ Query successful: {len(result)} fuel types found")
        print("\nFuel Type Summary (24 hours):")
        print(result.head(10))
        
    except Exception as e:
        print(f"✗ Error querying generation_by_fuel_30min: {e}")
    
    # Test 3: Query capacity_utilization_30min view
    print("\n3. Testing capacity_utilization_30min view...")
    try:
        query = f"""
        SELECT 
            fuel_type,
            AVG(utilization_pct) as avg_utilization,
            MAX(utilization_pct) as max_utilization,
            MIN(utilization_pct) as min_utilization
        FROM capacity_utilization_30min
        WHERE settlementdate >= '{start_date.strftime('%Y-%m-%d %H:%M:%S')}'
        AND settlementdate <= '{end_date.strftime('%Y-%m-%d %H:%M:%S')}'
        AND region = 'NSW1'
        GROUP BY fuel_type
        ORDER BY avg_utilization DESC
        """
        
        result = duckdb_data_service.conn.execute(query).df()
        print(f"✓ Query successful: Capacity utilization for NSW1")
        print(result.head(10))
        
    except Exception as e:
        print(f"✗ Error querying capacity_utilization_30min: {e}")
    
    # Test 4: Performance comparison - raw vs aggregated
    print("\n4. Performance comparison - raw vs aggregated...")
    
    # Query raw data
    import time
    
    # Test raw query (what current dashboard does)
    t1 = time.time()
    raw_query = f"""
    SELECT COUNT(*) as record_count
    FROM generation_30min
    WHERE settlementdate >= '{start_date.strftime('%Y-%m-%d %H:%M:%S')}'
    AND settlementdate <= '{end_date.strftime('%Y-%m-%d %H:%M:%S')}'
    """
    raw_count = duckdb_data_service.conn.execute(raw_query).fetchone()[0]
    t1_duration = time.time() - t1
    print(f"Raw data: {raw_count:,} records, query time: {t1_duration:.3f}s")
    
    # Test aggregated query
    t2 = time.time()
    agg_query = f"""
    SELECT COUNT(*) as record_count
    FROM generation_by_fuel_30min
    WHERE settlementdate >= '{start_date.strftime('%Y-%m-%d %H:%M:%S')}'
    AND settlementdate <= '{end_date.strftime('%Y-%m-%d %H:%M:%S')}'
    """
    agg_count = duckdb_data_service.conn.execute(agg_query).fetchone()[0]
    t2_duration = time.time() - t2
    print(f"Aggregated data: {agg_count:,} records, query time: {t2_duration:.3f}s")
    
    reduction_ratio = (1 - agg_count/raw_count) * 100 if raw_count > 0 else 0
    print(f"Data reduction: {reduction_ratio:.1f}%")
    
    # Test 5: Test long date range query
    print("\n5. Testing long date range (1 year)...")
    year_start = end_date - timedelta(days=365)
    
    t3 = time.time()
    year_query = f"""
    SELECT 
        fuel_type,
        COUNT(*) as records,
        AVG(total_generation_mw) as avg_generation
    FROM generation_by_fuel_30min
    WHERE settlementdate >= '{year_start.strftime('%Y-%m-%d %H:%M:%S')}'
    AND settlementdate <= '{end_date.strftime('%Y-%m-%d %H:%M:%S')}'
    AND region = 'NEM'
    GROUP BY fuel_type
    """
    
    # For NEM, we need to sum across regions
    year_query_nem = f"""
    SELECT 
        fuel_type,
        settlementdate,
        SUM(total_generation_mw) as total_generation_mw
    FROM generation_by_fuel_30min
    WHERE settlementdate >= '{year_start.strftime('%Y-%m-%d %H:%M:%S')}'
    AND settlementdate <= '{end_date.strftime('%Y-%m-%d %H:%M:%S')}'
    GROUP BY fuel_type, settlementdate
    """
    
    year_result = duckdb_data_service.conn.execute(year_query_nem).df()
    t3_duration = time.time() - t3
    
    print(f"✓ Year query completed in {t3_duration:.3f}s")
    print(f"Records returned: {len(year_result):,}")
    
    # Summary by fuel type
    fuel_summary = year_result.groupby('fuel_type')['total_generation_mw'].agg(['mean', 'sum', 'count'])
    print("\nYear summary by fuel type:")
    print(fuel_summary.sort_values('mean', ascending=False))
    
    print("\n" + "="*60)
    print("VIEW TESTING COMPLETE")
    print("="*60)


if __name__ == "__main__":
    test_generation_views()