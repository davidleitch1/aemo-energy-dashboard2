#!/usr/bin/env python3
"""
Check rooftop data via DuckDB.
"""
import duckdb
from pathlib import Path

def check_rooftop_duckdb():
    """Check rooftop data using DuckDB."""
    # Path to the data directory
    data_dir = Path("/Users/davidleitch/Library/Mobile Documents/com~apple~CloudDocs/snakeplay/AEMO_spot/aemo-data-updater/data 2")
    rooftop_file = data_dir / "rooftop30.parquet"
    
    print(f"Checking rooftop file: {rooftop_file}")
    
    if not rooftop_file.exists():
        print("File not found!")
        return
    
    # Connect to DuckDB and query the file
    conn = duckdb.connect(':memory:')
    
    # Query basic info
    result = conn.execute(f"""
        SELECT COUNT(*) as total_records,
               MIN(settlementdate) as min_date,
               MAX(settlementdate) as max_date
        FROM '{rooftop_file}'
    """).fetchone()
    
    print(f"\nTotal records: {result[0]:,}")
    print(f"Date range: {result[1]} to {result[2]}")
    
    # Check by year
    print("\nRecords by year:")
    year_counts = conn.execute(f"""
        SELECT YEAR(settlementdate) as year,
               COUNT(*) as count,
               MIN(settlementdate) as min_date,
               MAX(settlementdate) as max_date
        FROM '{rooftop_file}'
        GROUP BY year
        ORDER BY year
    """).fetchall()
    
    for year, count, min_date, max_date in year_counts:
        print(f"  {year}: {count:,} records ({min_date} to {max_date})")
    
    # Check 2025 specifically for January data
    print("\n2025 Monthly breakdown:")
    monthly_2025 = conn.execute(f"""
        SELECT MONTH(settlementdate) as month,
               COUNT(*) as count,
               MIN(settlementdate) as min_date,
               MAX(settlementdate) as max_date
        FROM '{rooftop_file}'
        WHERE YEAR(settlementdate) = 2025
        GROUP BY month
        ORDER BY month
    """).fetchall()
    
    for month, count, min_date, max_date in monthly_2025:
        print(f"  Month {month}: {count:,} records ({min_date} to {max_date})")
    
    # Check the actual structure
    print("\nFile structure:")
    schema = conn.execute(f"""
        SELECT * FROM '{rooftop_file}' LIMIT 1
    """).description
    
    print("Columns:", [col[0] for col in schema])
    
    # Check if it's wide or long format
    sample = conn.execute(f"""
        SELECT * FROM '{rooftop_file}' 
        WHERE settlementdate >= '2025-01-01'
        LIMIT 5
    """).fetchall()
    
    print("\nSample 2025 data:")
    for row in sample:
        print(row)
    
    conn.close()

if __name__ == "__main__":
    check_rooftop_duckdb()