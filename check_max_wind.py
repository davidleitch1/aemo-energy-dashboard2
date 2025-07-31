#!/usr/bin/env python3
"""Check maximum NEM-wide wind generation over past 5 days"""

import pandas as pd
import duckdb
from datetime import datetime, timedelta
from pathlib import Path

# Data path - use production
DATA_PATH = '/Volumes/davidleitch/aemo_production/data'

def check_max_wind():
    """Find maximum wind generation over past 5 days"""
    
    scada30_path = Path(DATA_PATH) / "scada30.parquet"
    gen_info_path = Path(DATA_PATH) / "gen_info.pkl"
    
    # Load generator info to map DUIDs to fuel types
    gen_info = pd.read_pickle(gen_info_path)
    print(f"Gen info columns: {gen_info.columns.tolist()}")
    
    # Get wind DUIDs
    wind_duids = gen_info[gen_info['Fuel'] == 'Wind']['DUID'].unique()
    print(f"Found {len(wind_duids)} wind DUIDs")
    
    # Connect to DuckDB
    conn = duckdb.connect(':memory:')
    
    # Create view
    conn.execute(f"""
        CREATE VIEW scada30 AS 
        SELECT * FROM parquet_scan('{scada30_path}')
    """)
    
    # Calculate date range
    end_date = datetime.now()
    start_date = end_date - timedelta(days=5)
    
    print(f"\nChecking wind generation from {start_date} to {end_date}")
    
    # Create list of wind DUIDs for SQL query
    wind_duids_str = "','".join(wind_duids)
    
    # Query for maximum wind generation
    result = conn.execute(f"""
        SELECT 
            settlementdate,
            SUM(scadavalue) as total_wind_mw
        FROM scada30
        WHERE duid IN ('{wind_duids_str}')
          AND settlementdate >= '{start_date}'
          AND settlementdate <= '{end_date}'
        GROUP BY settlementdate
        ORDER BY total_wind_mw DESC
        LIMIT 10
    """).fetchall()
    
    print("\nTop 10 wind generation periods:")
    print("Timestamp                    Wind MW")
    print("-" * 40)
    
    max_wind = 0
    max_time = None
    
    for row in result:
        timestamp, wind_mw = row
        print(f"{timestamp}    {wind_mw:,.0f}")
        if wind_mw > max_wind:
            max_wind = wind_mw
            max_time = timestamp
    
    # Also check 5-minute data for more precision
    scada5_path = Path(DATA_PATH) / "scada5.parquet"
    if scada5_path.exists():
        print("\nChecking 5-minute data for more precision...")
        
        conn.execute(f"""
            CREATE VIEW scada5 AS 
            SELECT * FROM parquet_scan('{scada5_path}')
        """)
        
        # Query 5-minute data around the peak time
        if max_time:
            check_start = max_time - timedelta(hours=1)
            check_end = max_time + timedelta(hours=1)
            
            result_5min = conn.execute(f"""
                SELECT 
                    settlementdate,
                    SUM(scadavalue) as total_wind_mw
                FROM scada5
                WHERE duid IN ('{wind_duids_str}')
                  AND settlementdate >= '{check_start}'
                  AND settlementdate <= '{check_end}'
                GROUP BY settlementdate
                ORDER BY total_wind_mw DESC
                LIMIT 5
            """).fetchall()
            
            print(f"\n5-minute peaks around {max_time}:")
            print("Timestamp                    Wind MW")
            print("-" * 40)
            
            for row in result_5min:
                timestamp, wind_mw = row
                print(f"{timestamp}    {wind_mw:,.0f}")
    
    print(f"\n✅ Maximum NEM-wide wind generation over past 5 days: {max_wind:,.0f} MW")
    print(f"   Occurred at: {max_time}")
    
    conn.close()

if __name__ == "__main__":
    check_max_wind()