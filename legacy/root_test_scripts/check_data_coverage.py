#!/usr/bin/env python3
"""
Check data coverage for all parquet files in the data directory
Focus on October 2025 coverage and identify gaps
"""

import duckdb
from pathlib import Path
from datetime import datetime, timedelta

print("=" * 100)
print("DATA COVERAGE ANALYSIS - OCTOBER 2025")
print("=" * 100)
print(f"Analysis run at: {datetime.now()}")
print(f"Target end time: 2025-10-16 11:30:00")
print("=" * 100)

conn = duckdb.connect(':memory:')
data_dir = Path('/Users/davidleitch/aemo_production/data')

# Find all parquet files
parquet_files = sorted(data_dir.glob('*.parquet'))

# Target period for October 2025
oct_start = datetime(2025, 10, 1, 0, 0, 0)
target_end = datetime(2025, 10, 16, 11, 30, 0)

print(f"\nFound {len(parquet_files)} parquet files\n")

for pfile in parquet_files:
    print("=" * 100)
    print(f"FILE: {pfile.name}")
    print("=" * 100)

    try:
        # Get file size
        size_mb = pfile.stat().st_size / (1024 * 1024)
        print(f"Size: {size_mb:,.1f} MB")

        # Detect timestamp column (try common names)
        schema_query = f"DESCRIBE SELECT * FROM read_parquet('{pfile}') LIMIT 1"
        schema = conn.execute(schema_query).df()

        # Find timestamp column
        timestamp_col = None
        for col in schema['column_name']:
            col_lower = col.lower()
            if 'settlement' in col_lower or 'timestamp' in col_lower or 'date' in col_lower:
                timestamp_col = col
                break

        if not timestamp_col:
            print("⚠ No timestamp column found - skipping")
            print()
            continue

        print(f"Timestamp column: {timestamp_col}")

        # Overall coverage
        coverage_query = f"""
        SELECT
            MIN({timestamp_col}) as first_timestamp,
            MAX({timestamp_col}) as last_timestamp,
            COUNT(*) as total_records,
            COUNT(DISTINCT {timestamp_col}) as unique_timestamps
        FROM read_parquet('{pfile}')
        """
        coverage = conn.execute(coverage_query).df()

        first_ts = coverage['first_timestamp'].iloc[0]
        last_ts = coverage['last_timestamp'].iloc[0]
        total_records = coverage['total_records'].iloc[0]
        unique_ts = coverage['unique_timestamps'].iloc[0]

        print(f"\nOverall Coverage:")
        print(f"  First timestamp:    {first_ts}")
        print(f"  Last timestamp:     {last_ts}")
        print(f"  Total records:      {total_records:,}")
        print(f"  Unique timestamps:  {unique_ts:,}")

        # October 2025 coverage
        oct_query = f"""
        SELECT
            MIN({timestamp_col}) as oct_first,
            MAX({timestamp_col}) as oct_last,
            COUNT(*) as oct_records,
            COUNT(DISTINCT {timestamp_col}) as oct_unique_timestamps
        FROM read_parquet('{pfile}')
        WHERE {timestamp_col} >= '{oct_start}'
          AND {timestamp_col} <= '{target_end}'
        """
        oct_cov = conn.execute(oct_query).df()

        oct_first = oct_cov['oct_first'].iloc[0]
        oct_last = oct_cov['oct_last'].iloc[0]
        oct_records = oct_cov['oct_records'].iloc[0]
        oct_unique = oct_cov['oct_unique_timestamps'].iloc[0]

        print(f"\nOctober 2025 Coverage (until {target_end}):")
        if oct_records > 0:
            print(f"  First timestamp:    {oct_first}")
            print(f"  Last timestamp:     {oct_last}")
            print(f"  Total records:      {oct_records:,}")
            print(f"  Unique timestamps:  {oct_unique:,}")

            # Calculate expected intervals based on file type
            if '30' in pfile.name or 'thirty' in pfile.name.lower():
                interval_min = 30
            elif '5' in pfile.name or 'five' in pfile.name.lower():
                interval_min = 5
            else:
                interval_min = 30  # default assumption

            # Calculate expected number of intervals
            time_diff = (target_end - oct_start).total_seconds() / 60  # minutes
            expected_intervals = int(time_diff / interval_min)

            print(f"\nInterval Analysis:")
            print(f"  Detected interval:  {interval_min} minutes")
            print(f"  Expected intervals: {expected_intervals:,}")
            print(f"  Actual intervals:   {oct_unique:,}")

            if oct_unique < expected_intervals:
                missing = expected_intervals - oct_unique
                coverage_pct = (oct_unique / expected_intervals) * 100
                print(f"  Missing intervals:  {missing:,} ({100-coverage_pct:.1f}%)")
                print(f"  Coverage:           {coverage_pct:.1f}%")

                # Check for gaps
                gap_query = f"""
                WITH timestamps AS (
                    SELECT DISTINCT {timestamp_col} as ts
                    FROM read_parquet('{pfile}')
                    WHERE {timestamp_col} >= '{oct_start}'
                      AND {timestamp_col} <= '{target_end}'
                    ORDER BY ts
                ),
                gaps AS (
                    SELECT
                        ts as gap_start,
                        LEAD(ts) OVER (ORDER BY ts) as gap_end,
                        LEAD(ts) OVER (ORDER BY ts) - ts as gap_duration
                    FROM timestamps
                )
                SELECT
                    gap_start,
                    gap_end,
                    gap_duration
                FROM gaps
                WHERE EXTRACT(EPOCH FROM gap_duration) > {interval_min * 60 * 1.5}  -- gaps > 1.5x expected interval
                ORDER BY gap_duration DESC
                LIMIT 5
                """

                try:
                    gaps = conn.execute(gap_query).df()
                    if len(gaps) > 0:
                        print(f"\n  Top Gaps (> {interval_min * 1.5:.0f} min):")
                        for _, gap in gaps.iterrows():
                            duration_hours = gap['gap_duration'].total_seconds() / 3600
                            print(f"    {gap['gap_start']} → {gap['gap_end']} ({duration_hours:.1f} hours)")
                except Exception as e:
                    print(f"  (Could not analyze gaps: {e})")
            else:
                print(f"  ✓ Complete coverage - no missing intervals")
        else:
            print("  ⚠ NO DATA for October 2025")

    except Exception as e:
        print(f"ERROR analyzing file: {e}")

    print()

print("=" * 100)
print("ANALYSIS COMPLETE")
print("=" * 100)
