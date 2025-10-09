#!/usr/bin/env python3
"""
Curtailment Query Manager - DuckDB-based queries for curtailment analysis.

This module provides efficient DuckDB queries for curtailment data without
loading entire datasets into memory. Based on the generation_query_manager pattern.
"""

import duckdb
import pandas as pd
import logging
from datetime import datetime, timedelta
from typing import Optional, Dict, Any, Tuple, List
from pathlib import Path

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class CurtailmentQueryManager:
    """DuckDB-based query manager for curtailment analysis"""

    def __init__(self):
        """Initialize DuckDB connection and register parquet files"""
        self.conn = duckdb.connect(':memory:')

        # Get data paths from config (works on both dev and production machines)
        from ..shared.config import config
        self.curtailment5_path = config.curtailment5_file
        self.scada5_path = config.scada5_file
        self.scada30_path = config.scada30_file
        self.gen_info_path = config.gen_info_file

        # Create views for parquet files
        self._create_base_views()

        # Create optimized curtailment views
        self._create_curtailment_views()

        # Cache for frequently accessed data
        self.cache = {}
        self.cache_ttl = 300  # 5 minutes
        self.cache_timestamps = {}

        logger.info("CurtailmentQueryManager initialized with DuckDB backend")

    def _create_base_views(self):
        """Create base views for parquet files - called AFTER _create_curtailment_views sets up duid_regions"""
        try:
            # Register curtailment5 data (collected by production collector)
            # Contains: settlementdate, duid, availability, totalcleared, semidispatchcap, curtailment
            self.conn.execute(f"""
                CREATE OR REPLACE VIEW curtailment5 AS
                SELECT
                    settlementdate as timestamp,
                    duid,
                    availability as availgen,
                    totalcleared as dispatchcap,
                    semidispatchcap,
                    curtailment as curtailment_calc
                FROM read_parquet('{self.curtailment5_path}')
            """)

            logger.info("Base curtailment view created successfully")

        except Exception as e:
            logger.error(f"Error creating base views: {e}")
            raise

    def _create_scada_views(self):
        """Create SCADA views - called AFTER duid_regions table is created"""
        try:
            # Register 5-minute SCADA data (filter by wind/solar DUIDs from duid_regions)
            self.conn.execute(f"""
                CREATE OR REPLACE VIEW scada AS
                SELECT
                    settlementdate as timestamp,
                    duid,
                    scadavalue as scada
                FROM read_parquet('{self.scada5_path}')
                WHERE duid IN (SELECT DISTINCT duid FROM duid_regions)
            """)

            # Register 30-minute SCADA data
            self.conn.execute(f"""
                CREATE OR REPLACE VIEW scada30 AS
                SELECT
                    settlementdate as timestamp,
                    duid,
                    scadavalue as scada
                FROM read_parquet('{self.scada30_path}')
                WHERE duid IN (SELECT DISTINCT duid FROM duid_regions)
            """)

            logger.info("SCADA views created successfully")

        except Exception as e:
            logger.error(f"Error creating SCADA views: {e}")
            raise

    def _create_curtailment_views(self):
        """Create optimized curtailment calculation views"""
        try:
            # Load comprehensive wind/solar mapping (extracted from gen_info)
            ws_mapping_path = Path(__file__).parent / 'wind_solar_regions_complete.pkl'

            region_data = []

            # Load wind/solar mapping
            if ws_mapping_path.exists():
                import pickle
                with open(ws_mapping_path, 'rb') as f:
                    ws_mapping = pickle.load(f)

                for duid, info in ws_mapping.items():
                    region_data.append({
                        'duid': duid,
                        'region': info.get('region', 'Unknown'),
                        'fuel': info.get('fuel', 'Unknown')
                    })

                logger.info(f"Loaded {len(ws_mapping)} wind/solar mappings")
            else:
                logger.warning("Wind/solar mapping file not found!")

            if region_data:
                import pandas as pd
                region_df = pd.DataFrame(region_data)
                self.conn.execute("CREATE OR REPLACE TABLE duid_regions AS SELECT * FROM region_df")
                logger.info(f"Created duid_regions table with {len(region_data)} entries")
            else:
                # Create fallback region table
                self.conn.execute("""
                    CREATE OR REPLACE TABLE duid_regions AS
                    SELECT 'DUMMY' as duid, 'Unknown' as region, 'Unknown' as fuel WHERE 1=0
                """)
                logger.warning("No region mapping data found, created empty duid_regions table")

            # Create merged curtailment view using production curtailment5 data
            # Note: We don't join SCADA here - actual generation is queried separately in query methods
            self.conn.execute("""
                CREATE OR REPLACE VIEW curtailment_merged AS
                SELECT
                    c.timestamp,
                    c.duid,
                    c.availgen,
                    c.dispatchcap,
                    GREATEST(0, COALESCE(c.curtailment_calc, 0)) as curtailment,
                    -- Determine if curtailed based on SEMIDISPATCHCAP flag
                    CASE
                        WHEN c.semidispatchcap = 1 AND c.curtailment_calc > 0 THEN true
                        ELSE false
                    END as is_curtailed,
                    -- Curtailment type based on SEMIDISPATCHCAP
                    CASE
                        WHEN c.semidispatchcap = 1 AND c.curtailment_calc > 0 THEN 'network'
                        WHEN c.semidispatchcap = 0 AND c.dispatchcap < c.availgen * 0.95 THEN 'economic'
                        ELSE 'none'
                    END as curtailment_type,
                    -- Get region from mapping table or use fallback
                    COALESCE(r.region,
                        CASE
                            -- Common patterns for region detection
                            WHEN c.duid LIKE 'NSW%' OR c.duid LIKE 'BW%' OR c.duid LIKE 'LD%' THEN 'NSW1'
                            WHEN c.duid LIKE 'QLD%' OR c.duid LIKE 'BRA%' OR c.duid LIKE 'DAR%' THEN 'QLD1'
                            WHEN c.duid LIKE 'SA%' OR c.duid LIKE 'LK%' OR c.duid LIKE 'TO%' THEN 'SA1'
                            WHEN c.duid LIKE 'TAS%' OR c.duid LIKE 'BAS%' THEN 'TAS1'
                            WHEN c.duid LIKE 'VIC%' OR c.duid LIKE 'MUR%' OR c.duid LIKE 'YAL%' THEN 'VIC1'
                            -- Additional patterns
                            WHEN c.duid LIKE '%NSW%' THEN 'NSW1'
                            WHEN c.duid LIKE '%QLD%' THEN 'QLD1'
                            WHEN c.duid LIKE '%SA' THEN 'SA1'
                            WHEN c.duid LIKE '%TAS%' THEN 'TAS1'
                            WHEN c.duid LIKE '%VIC%' THEN 'VIC1'
                            ELSE 'Unknown'
                        END
                    ) as region,
                    -- Get fuel type from mapping or use pattern
                    COALESCE(r.fuel,
                        CASE
                            WHEN c.duid LIKE '%WF%' OR UPPER(c.duid) LIKE '%WIND%' THEN 'Wind'
                            WHEN c.duid LIKE '%SF%' OR UPPER(c.duid) LIKE '%SOLAR%' OR c.duid LIKE '%PV%' THEN 'Solar'
                            ELSE 'Unknown'
                        END
                    ) as fuel
                FROM curtailment5 c
                LEFT JOIN duid_regions r
                    ON c.duid = r.duid
            """)

            # Create 30-minute curtailment view
            # Aggregates 5-min curtailment data to 30-minute intervals
            self.conn.execute("""
                CREATE OR REPLACE VIEW curtailment_30min AS
                SELECT
                    -- Round timestamp to 30-minute boundary
                    date_trunc('hour', timestamp) +
                    INTERVAL '30 minutes' * FLOOR(EXTRACT(MINUTE FROM timestamp) / 30) as timestamp,
                    duid,
                    region,
                    fuel,
                    -- For availgen: take MAX within 30-min period (plant's max capacity)
                    MAX(availgen) as availgen,
                    -- For dispatch cap: take AVG within 30-min period
                    AVG(dispatchcap) as dispatchcap,
                    -- For curtailment: average MW over the 30-min period
                    AVG(curtailment) as curtailment
                FROM curtailment_merged
                GROUP BY 1, 2, 3, 4
            """)

            # Create hourly aggregation view
            self.conn.execute("""
                CREATE OR REPLACE VIEW curtailment_hourly AS
                SELECT
                    date_trunc('hour', timestamp) as timestamp,
                    region,
                    fuel,
                    AVG(availgen) as availgen,
                    AVG(dispatchcap) as dispatchcap,
                    AVG(curtailment) as curtailment,
                    AVG(CASE WHEN is_curtailed THEN 1.0 ELSE 0.0 END) as curtailment_rate,
                    SUM(CASE WHEN curtailment_type = 'local' THEN 1 ELSE 0 END) as local_count,
                    SUM(CASE WHEN curtailment_type = 'network' THEN 1 ELSE 0 END) as network_count,
                    SUM(CASE WHEN curtailment_type = 'economic' THEN 1 ELSE 0 END) as economic_count
                FROM curtailment_merged
                GROUP BY 1, 2, 3
            """)

            # Create daily aggregation view
            self.conn.execute("""
                CREATE OR REPLACE VIEW curtailment_daily AS
                SELECT
                    date_trunc('day', timestamp) as timestamp,
                    region,
                    fuel,
                    duid,
                    MAX(availgen) as availgen,  -- Max capacity for the day
                    MIN(CASE WHEN dispatchcap > 0 THEN dispatchcap END) as min_dispatchcap,  -- Minimum constraint
                    AVG(dispatchcap) as avg_dispatchcap,
                    AVG(curtailment) as curtailment,
                    SUM(curtailment) / 12 as curtailment_mwh,  -- Total curtailment in MWh (5min to MWh)
                    -- Only count curtailment rate when dispatch cap was constraining
                    AVG(CASE WHEN dispatchcap < availgen - 5 THEN 1.0 ELSE 0.0 END) as constraint_rate,
                    COUNT(CASE WHEN dispatchcap < availgen - 5 THEN 1 END) as constrained_intervals
                FROM curtailment_merged
                GROUP BY 1, 2, 3, 4
            """)

            logger.info("Curtailment views created successfully")

        except Exception as e:
            logger.error(f"Error creating curtailment views: {e}")
            raise

    def query_curtailment_data(
        self,
        start_date: datetime,
        end_date: datetime,
        region: Optional[str] = None,
        fuel: Optional[str] = None,
        duid: Optional[str] = None,
        resolution: str = 'auto'
    ) -> pd.DataFrame:
        """
        Query curtailment data with flexible filtering.

        Args:
            start_date: Start of date range
            end_date: End of date range
            region: Optional region filter ('NSW1', 'QLD1', etc.)
            fuel: Optional fuel type filter ('Wind', 'Solar')
            duid: Optional specific DUID filter
            resolution: Data resolution ('auto', '5min', '30min', 'hourly', 'daily')

        Returns:
            DataFrame with curtailment data
        """
        try:
            # Determine resolution and aggregate for daily when needed
            if resolution == 'auto':
                days_diff = (end_date - start_date).days
                if days_diff > 30:
                    resolution = 'daily'
                    view = 'curtailment_daily'
                elif days_diff > 7:
                    resolution = 'hourly'
                    view = 'curtailment_hourly'
                elif days_diff > 2:
                    resolution = '30min'
                    view = 'curtailment_30min'
                else:
                    resolution = '5min'
                    view = 'curtailment_merged'
            elif resolution == 'daily':
                view = 'curtailment_daily'
                # For daily, we need to aggregate by region/fuel if not looking at specific DUID
                if not (duid and duid != 'All'):
                    # Aggregate daily data by region/fuel
                    view = 'curtailment_daily_agg'
                    self.conn.execute("""
                        CREATE OR REPLACE TEMP VIEW curtailment_daily_agg AS
                        SELECT
                            timestamp,
                            region,
                            fuel,
                            SUM(generation_mwh) as scada,
                            SUM(curtailment_mwh) as curtailment,
                            MAX(availgen) as availgen,
                            MIN(min_dispatchcap) as dispatchcap,
                            AVG(constraint_rate) * 100 as curtailment_rate
                        FROM curtailment_daily
                        WHERE timestamp >= '{start_date.strftime('%Y-%m-%d')}'
                          AND timestamp <= '{end_date.strftime('%Y-%m-%d')}'
                          {" AND region = '" + region + "'" if region and region != 'All' else ""}
                          {" AND fuel = '" + fuel + "'" if fuel and fuel != 'All' else ""}
                        GROUP BY timestamp, region, fuel
                    """)
            elif resolution == 'hourly':
                view = 'curtailment_hourly'
            elif resolution == '30min':
                view = 'curtailment_30min'
            elif resolution == '5min':
                view = 'curtailment_merged'
            else:
                # Default to 30min for unknown resolution
                logger.warning(f"Unknown resolution {resolution}, defaulting to 30min")
                view = 'curtailment_30min'

            # Build WHERE clause
            conditions = [
                f"timestamp >= '{start_date.strftime('%Y-%m-%d %H:%M:%S')}'",
                f"timestamp <= '{end_date.strftime('%Y-%m-%d %H:%M:%S')}'"
            ]

            if region and region != 'All':
                conditions.append(f"region = '{region}'")

            if fuel and fuel != 'All':
                conditions.append(f"fuel = '{fuel}'")

            if duid and duid != 'All':
                conditions.append(f"duid = '{duid}'")
                # Don't force 5min view, respect the resolution parameter

            where_clause = " AND ".join(conditions)

            # Build query
            query = f"""
                SELECT *
                FROM {view}
                WHERE {where_clause}
                ORDER BY timestamp
            """

            # Check cache
            cache_key = f"{view}_{start_date}_{end_date}_{region}_{fuel}_{duid}"
            if cache_key in self.cache:
                cache_time = self.cache_timestamps.get(cache_key, 0)
                # Fix: Use total_seconds() instead of .seconds to get full duration
                if (datetime.now() - datetime.fromtimestamp(cache_time)).total_seconds() < self.cache_ttl:
                    logger.debug(f"Cache hit for {cache_key}")
                    return self.cache[cache_key].copy()

            # Execute query
            logger.info(f"Querying {resolution} curtailment data from {start_date} to {end_date}")
            result = self.conn.execute(query).df()

            # Cache result
            self.cache[cache_key] = result
            self.cache_timestamps[cache_key] = datetime.now().timestamp()

            logger.info(f"Loaded {len(result):,} records")
            return result

        except Exception as e:
            logger.error(f"Error querying curtailment data: {e}")
            return pd.DataFrame()

    def query_region_summary(
        self,
        start_date: datetime,
        end_date: datetime
    ) -> pd.DataFrame:
        """
        Get summary statistics by region.

        Returns:
            DataFrame with regional curtailment statistics
        """
        try:
            # Query curtailment data (without rate - we'll calculate it after joining with actual output)
            curt_query = f"""
                SELECT
                    region,
                    COUNT(DISTINCT duid) as unit_count,
                    SUM(curtailment) / 12 as total_curtailment_mwh,
                    AVG(CASE WHEN availgen > 0 THEN curtailment ELSE NULL END) as avg_curtailment_mw,
                    MAX(curtailment) as max_curtailment_mw,
                    SUM(CASE WHEN curtailment_type = 'local' THEN 1 ELSE 0 END) as local_events,
                    SUM(CASE WHEN curtailment_type = 'network' THEN 1 ELSE 0 END) as network_events,
                    SUM(CASE WHEN curtailment_type = 'economic' THEN 1 ELSE 0 END) as economic_events
                FROM curtailment_merged
                WHERE timestamp >= '{start_date.strftime('%Y-%m-%d %H:%M:%S')}'
                  AND timestamp <= '{end_date.strftime('%Y-%m-%d %H:%M:%S')}'
                  AND region != 'Unknown'
                GROUP BY region
            """

            # Load gen_info and register it (can't use read_parquet on .pkl file)
            import pickle
            import pandas as pd
            with open(self.gen_info_path, 'rb') as f:
                gen_info = pickle.load(f)

            # If gen_info is a dict, convert to DataFrame
            if isinstance(gen_info, dict):
                gen_info_df = pd.DataFrame.from_dict(gen_info, orient='index')
                gen_info_df = gen_info_df.reset_index().rename(columns={'index': 'DUID'})
            else:
                gen_info_df = gen_info

            # Register as temp table
            self.conn.execute("CREATE OR REPLACE TEMP TABLE temp_gen_info AS SELECT * FROM gen_info_df")

            # Query actual generation using scada30 and gen_info (same pattern as Generation mix tab)
            gen_query = f"""
                SELECT
                    d.Region as region,
                    SUM(g.scadavalue) * 0.5 as actual_generation_mwh
                FROM read_parquet('{self.scada30_path}') g
                JOIN temp_gen_info d ON g.duid = d.DUID
                WHERE g.settlementdate >= '{start_date.strftime('%Y-%m-%d %H:%M:%S')}'
                  AND g.settlementdate <= '{end_date.strftime('%Y-%m-%d %H:%M:%S')}'
                  AND d.Fuel IN ('Wind', 'Solar')
                  AND d.Region IS NOT NULL
                GROUP BY d.Region
            """

            # Merge the results and calculate curtailment rate as: Curtailed / (Curtailed + Actual)
            query = f"""
                SELECT
                    c.*,
                    COALESCE(g.actual_generation_mwh, 0) as actual_generation_mwh,
                    (c.total_curtailment_mwh / NULLIF(c.total_curtailment_mwh + COALESCE(g.actual_generation_mwh, 0), 0)) * 100 as curtailment_rate_pct
                FROM ({curt_query}) c
                LEFT JOIN ({gen_query}) g ON c.region = g.region
                ORDER BY c.total_curtailment_mwh DESC
            """

            return self.conn.execute(query).df()

        except Exception as e:
            logger.error(f"Error querying region summary: {e}")
            return pd.DataFrame()

    def query_top_curtailed_units(
        self,
        start_date: datetime,
        end_date: datetime,
        limit: int = 10,
        region: Optional[str] = None
    ) -> pd.DataFrame:
        """
        Get top curtailed units.

        Args:
            start_date: Start datetime
            end_date: End datetime
            limit: Maximum number of units to return
            region: Optional region filter (e.g., 'NSW1', 'VIC1')

        Returns:
            DataFrame with top curtailed units
        """
        try:
            # Build region filter
            region_filter = ""
            if region:
                region_filter = f"AND region = '{region}'"

            query = f"""
                SELECT
                    duid,
                    region,
                    fuel,
                    COUNT(*) as curtailed_intervals,
                    -- Curtailment rate: % of potential energy curtailed
                    (SUM(curtailment) / NULLIF(SUM(CASE WHEN availgen > 0 THEN availgen ELSE 0 END), 0)) * 100 as curtailment_rate_pct,
                    SUM(curtailment) / 12 as total_curtailment_mwh,
                    AVG(CASE WHEN availgen > 0 THEN curtailment ELSE NULL END) as avg_curtailment_mw,
                    MAX(curtailment) as max_curtailment_mw
                FROM curtailment_merged
                WHERE timestamp >= '{start_date.strftime('%Y-%m-%d %H:%M:%S')}'
                  AND timestamp <= '{end_date.strftime('%Y-%m-%d %H:%M:%S')}'
                  AND region != 'Unknown'
                  {region_filter}
                GROUP BY duid, region, fuel
                HAVING SUM(curtailment) > 0  -- Only show units that have curtailment
                ORDER BY total_curtailment_mwh DESC
                LIMIT {limit}
            """

            return self.conn.execute(query).df()

        except Exception as e:
            logger.error(f"Error querying top curtailed units: {e}")
            return pd.DataFrame()

    def get_statistics(self) -> Dict[str, Any]:
        """Get query manager statistics"""
        stats = {
            'cache_size': len(self.cache),
            'cache_keys': list(self.cache.keys())
        }

        # Get data coverage
        try:
            coverage = self.conn.execute("""
                SELECT
                    MIN(timestamp) as earliest,
                    MAX(timestamp) as latest,
                    COUNT(DISTINCT duid) as unit_count,
                    COUNT(*) as total_records
                FROM curtailment_merged
            """).df()

            if not coverage.empty:
                stats['data_coverage'] = {
                    'earliest': coverage['earliest'].iloc[0],
                    'latest': coverage['latest'].iloc[0],
                    'unit_count': coverage['unit_count'].iloc[0],
                    'total_records': coverage['total_records'].iloc[0]
                }
        except:
            pass

        return stats

    def clear_cache(self):
        """Clear the query cache"""
        self.cache.clear()
        self.cache_timestamps.clear()
        logger.info("Cache cleared")


# Testing
if __name__ == "__main__":
    import time

    print("Testing CurtailmentQueryManager...")

    manager = CurtailmentQueryManager()

    # Test 1: Query last 24 hours
    end_date = datetime.now()
    start_date = end_date - timedelta(days=1)

    print(f"\n1. Querying 24 hours of data...")
    t1 = time.time()
    data = manager.query_curtailment_data(start_date, end_date, region='NSW1')
    print(f"✓ Query completed in {time.time() - t1:.2f}s")
    print(f"✓ Records: {len(data):,}")

    # Test 2: Regional summary
    print(f"\n2. Querying regional summary...")
    t2 = time.time()
    summary = manager.query_region_summary(start_date, end_date)
    print(f"✓ Query completed in {time.time() - t2:.2f}s")
    print(summary)

    # Test 3: Top curtailed units
    print(f"\n3. Querying top curtailed units...")
    t3 = time.time()
    top_units = manager.query_top_curtailed_units(start_date, end_date, limit=5)
    print(f"✓ Query completed in {time.time() - t3:.2f}s")
    print(top_units)

    # Show statistics
    print(f"\n4. Statistics:")
    stats = manager.get_statistics()
    for key, value in stats.items():
        print(f"  {key}: {value}")