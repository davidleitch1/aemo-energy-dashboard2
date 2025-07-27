#!/usr/bin/env python3
"""Verify that the backfill successfully filled the gaps in scada30.parquet"""

import pandas as pd
import duckdb
from pathlib import Path
from datetime import datetime
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)

# Data path
DATA_PATH = '/Users/davidleitch/Library/Mobile Documents/com~apple~CloudDocs/snakeplay/AEMO_spot/aemo-data-updater/data 2'

def verify_gaps_filled():
    """Check if the previously identified gaps are now filled"""
    
    scada30_path = Path(DATA_PATH) / "scada30.parquet"
    
    logger.info(f"Checking scada30.parquet at: {scada30_path}")
    
    # Connect to DuckDB
    conn = duckdb.connect(':memory:')
    
    # Create view
    conn.execute(f"""
        CREATE VIEW scada30 AS 
        SELECT * FROM parquet_scan('{scada30_path}')
    """)
    
    # Define the gaps we were filling
    gaps = [
        ("December 2020", "2020-12-01 00:30:00", "2020-12-31 23:30:00"),
        ("October 2021", "2021-10-01 00:30:00", "2021-10-31 23:30:00"),
        ("June 2022", "2022-06-01 00:30:00", "2022-06-30 23:30:00")
    ]
    
    logger.info("\nChecking previously identified gaps:")
    logger.info("="*60)
    
    all_filled = True
    
    for period_name, start_str, end_str in gaps:
        # Count records in this period
        result = conn.execute(f"""
            SELECT 
                COUNT(*) as record_count,
                COUNT(DISTINCT duid) as unique_duids,
                MIN(settlementdate) as min_date,
                MAX(settlementdate) as max_date
            FROM scada30
            WHERE settlementdate >= '{start_str}'
              AND settlementdate <= '{end_str}'
        """).fetchone()
        
        record_count, unique_duids, min_date, max_date = result
        
        logger.info(f"\n{period_name}:")
        logger.info(f"  Expected range: {start_str} to {end_str}")
        
        if record_count > 0:
            logger.info(f"  ✅ FILLED: {record_count:,} records found")
            logger.info(f"  Unique DUIDs: {unique_duids}")
            logger.info(f"  Actual range: {min_date} to {max_date}")
            
            # Check for completeness - should have 48 intervals per day
            days_in_period = (pd.Timestamp(end_str).date() - pd.Timestamp(start_str).date()).days + 1
            expected_intervals = days_in_period * 48
            intervals_per_duid = record_count / unique_duids if unique_duids > 0 else 0
            
            logger.info(f"  Intervals per DUID: {intervals_per_duid:.1f} (expected ~{expected_intervals})")
            
            # Sample some data
            sample = conn.execute(f"""
                SELECT settlementdate, duid, scadavalue
                FROM scada30
                WHERE settlementdate >= '{start_str}'
                  AND settlementdate <= '{end_str}'
                ORDER BY RANDOM()
                LIMIT 5
            """).fetchall()
            
            logger.info("  Sample records:")
            for row in sample:
                logger.info(f"    {row[0]} | {row[1]} | {row[2]:.2f} MW")
        else:
            logger.info(f"  ❌ GAP STILL EXISTS: No records found")
            all_filled = False
    
    # Check overall statistics
    logger.info("\n" + "="*60)
    logger.info("Overall Statistics:")
    
    total_stats = conn.execute("""
        SELECT 
            COUNT(*) as total_records,
            COUNT(DISTINCT duid) as total_duids,
            MIN(settlementdate) as earliest_date,
            MAX(settlementdate) as latest_date
        FROM scada30
    """).fetchone()
    
    logger.info(f"Total records: {total_stats[0]:,}")
    logger.info(f"Unique DUIDs: {total_stats[1]}")
    logger.info(f"Date range: {total_stats[2]} to {total_stats[3]}")
    
    # Check for any remaining gaps
    logger.info("\nChecking for any remaining gaps...")
    
    gap_check = conn.execute("""
        WITH date_series AS (
            SELECT 
                settlementdate,
                LEAD(settlementdate) OVER (ORDER BY settlementdate) as next_date
            FROM (
                SELECT DISTINCT settlementdate 
                FROM scada30 
                WHERE settlementdate >= '2020-01-01'
                ORDER BY settlementdate
            )
        )
        SELECT 
            settlementdate,
            next_date,
            EXTRACT(EPOCH FROM (next_date - settlementdate)) / 60 as gap_minutes
        FROM date_series
        WHERE EXTRACT(EPOCH FROM (next_date - settlementdate)) / 60 > 30
          AND settlementdate < '2025-01-01'
        ORDER BY gap_minutes DESC
        LIMIT 10
    """).fetchall()
    
    if gap_check:
        logger.info("⚠️  Found potential gaps:")
        for row in gap_check:
            logger.info(f"  Gap: {row[0]} to {row[1]} ({row[2]:.0f} minutes)")
    else:
        logger.info("✅ No significant gaps found in the data!")
    
    conn.close()
    
    if all_filled:
        logger.info("\n✅ SUCCESS: All targeted gaps have been filled!")
    else:
        logger.info("\n❌ FAILURE: Some gaps still exist!")
    
    return all_filled

if __name__ == "__main__":
    verify_gaps_filled()