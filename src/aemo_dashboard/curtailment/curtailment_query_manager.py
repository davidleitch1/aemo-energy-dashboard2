#!/usr/bin/env python3
"""
Curtailment Query Manager - DuckDB-based queries for regional curtailment analysis.

Uses curtailment_regional5.parquet which contains UIGF-based curtailment data
aggregated by region (NSW1, QLD1, SA1, TAS1, VIC1).

Schema:
    settlementdate, regionid, solar_uigf, solar_cleared, solar_curtailment,
    wind_uigf, wind_cleared, wind_curtailment, total_curtailment
"""

import duckdb
import pandas as pd
import logging
from datetime import datetime, timedelta
from typing import Optional, Dict, Any, List
from pathlib import Path

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class CurtailmentQueryManager:
    """DuckDB-based query manager for regional curtailment analysis"""

    def __init__(self):
        """Initialize DuckDB connection and register parquet files"""
        self.conn = duckdb.connect(':memory:')

        # Get data paths from config
        from ..shared.config import config
        self.curtailment_regional_path = config.curtailment_regional5_file
        self.curtailment_duid_path = config.curtailment_duid5_file
        self.gen_info_path = config.gen_info_file
        self.prices_path = config.spot_hist_file

        # Load DUID to region mapping from gen_info
        self._load_duid_region_mapping()

        # Create views for parquet files
        self._create_views()

        # Cache for frequently accessed data
        self.cache = {}
        self.cache_ttl = 300  # 5 minutes
        self.cache_timestamps = {}

        logger.info("CurtailmentQueryManager initialized with regional and DUID UIGF data")

    def _load_duid_region_mapping(self):
        """Load DUID to region mapping from gen_info"""
        try:
            import pickle
            with open(self.gen_info_path, 'rb') as f:
                gen_info = pickle.load(f)

            if isinstance(gen_info, pd.DataFrame) and 'DUID' in gen_info.columns and 'Region' in gen_info.columns:
                # Create mapping dict: DUID -> Region (e.g., 'NSW1', 'VIC1', etc.)
                self.duid_to_region = dict(zip(gen_info['DUID'], gen_info['Region']))
                logger.info(f"Loaded {len(self.duid_to_region)} DUID->Region mappings")
            else:
                self.duid_to_region = {}
                logger.warning("Could not load DUID->Region mapping from gen_info")
        except Exception as e:
            self.duid_to_region = {}
            logger.warning(f"Error loading DUID->Region mapping: {e}")

    def _create_views(self):
        """Create DuckDB views for curtailment data"""
        try:
            # Create base view for regional curtailment data
            self.conn.execute(f"""
                CREATE OR REPLACE VIEW curtailment_regional AS
                SELECT
                    settlementdate as timestamp,
                    regionid as region,
                    solar_uigf,
                    solar_cleared,
                    solar_curtailment,
                    wind_uigf,
                    wind_cleared,
                    wind_curtailment,
                    total_curtailment,
                    -- Calculate totals for compatibility
                    solar_uigf + wind_uigf as total_uigf,
                    solar_cleared + wind_cleared as total_cleared
                FROM read_parquet('{self.curtailment_regional_path}')
            """)

            # Create 30-minute aggregation view
            self.conn.execute("""
                CREATE OR REPLACE VIEW curtailment_30min AS
                SELECT
                    date_trunc('hour', timestamp) +
                    INTERVAL '30 minutes' * FLOOR(EXTRACT(MINUTE FROM timestamp) / 30) as timestamp,
                    region,
                    AVG(solar_uigf) as solar_uigf,
                    AVG(solar_cleared) as solar_cleared,
                    AVG(solar_curtailment) as solar_curtailment,
                    AVG(wind_uigf) as wind_uigf,
                    AVG(wind_cleared) as wind_cleared,
                    AVG(wind_curtailment) as wind_curtailment,
                    AVG(total_curtailment) as total_curtailment
                FROM curtailment_regional
                GROUP BY 1, 2
            """)

            # Create hourly aggregation view
            self.conn.execute("""
                CREATE OR REPLACE VIEW curtailment_hourly AS
                SELECT
                    date_trunc('hour', timestamp) as timestamp,
                    region,
                    AVG(solar_uigf) as solar_uigf,
                    AVG(solar_cleared) as solar_cleared,
                    AVG(solar_curtailment) as solar_curtailment,
                    AVG(wind_uigf) as wind_uigf,
                    AVG(wind_cleared) as wind_cleared,
                    AVG(wind_curtailment) as wind_curtailment,
                    AVG(total_curtailment) as total_curtailment
                FROM curtailment_regional
                GROUP BY 1, 2
            """)

            # Create daily aggregation view (converts to MWh)
            self.conn.execute("""
                CREATE OR REPLACE VIEW curtailment_daily AS
                SELECT
                    date_trunc('day', timestamp) as timestamp,
                    region,
                    -- Convert 5-min MW values to MWh (multiply by 5/60 = 1/12)
                    SUM(solar_curtailment) / 12 as solar_curtailment_mwh,
                    SUM(wind_curtailment) / 12 as wind_curtailment_mwh,
                    SUM(total_curtailment) / 12 as total_curtailment_mwh,
                    SUM(solar_cleared) / 12 as solar_generation_mwh,
                    SUM(wind_cleared) / 12 as wind_generation_mwh,
                    -- Average MW values
                    AVG(solar_curtailment) as avg_solar_curtailment_mw,
                    AVG(wind_curtailment) as avg_wind_curtailment_mw,
                    AVG(total_curtailment) as avg_total_curtailment_mw,
                    -- Max values
                    MAX(solar_curtailment) as max_solar_curtailment_mw,
                    MAX(wind_curtailment) as max_wind_curtailment_mw,
                    MAX(total_curtailment) as max_total_curtailment_mw
                FROM curtailment_regional
                GROUP BY 1, 2
            """)

            # Create DUID-level curtailment view
            self.conn.execute(f"""
                CREATE OR REPLACE VIEW curtailment_duid AS
                SELECT
                    settlementdate as timestamp,
                    duid,
                    uigf,
                    totalcleared,
                    curtailment
                FROM read_parquet('{self.curtailment_duid_path}')
            """)

            # Create prices view for economic/grid curtailment classification
            self.conn.execute(f"""
                CREATE OR REPLACE VIEW prices AS
                SELECT
                    settlementdate as timestamp,
                    regionid as region,
                    rrp as price
                FROM read_parquet('{self.prices_path}')
            """)

            logger.info("Curtailment views created successfully (regional + DUID + prices)")

        except Exception as e:
            logger.error(f"Error creating views: {e}")
            raise

    def query_curtailment_data(
        self,
        start_date: datetime,
        end_date: datetime,
        region: Optional[str] = None,
        fuel: Optional[str] = None,
        resolution: str = 'auto'
    ) -> pd.DataFrame:
        """
        Query curtailment data with flexible filtering.

        Args:
            start_date: Start of date range
            end_date: End of date range
            region: Optional region filter ('NSW1', 'QLD1', etc.)
            fuel: Optional fuel type filter ('Wind', 'Solar', 'All')
            resolution: Data resolution ('auto', '5min', '30min', 'hourly', 'daily')

        Returns:
            DataFrame with curtailment data
        """
        try:
            # Determine resolution
            if resolution == 'auto':
                days_diff = (end_date - start_date).days
                if days_diff > 30:
                    resolution = 'daily'
                elif days_diff > 7:
                    resolution = 'hourly'
                elif days_diff > 2:
                    resolution = '30min'
                else:
                    resolution = '5min'

            # Select view based on resolution
            view_map = {
                '5min': 'curtailment_regional',
                '30min': 'curtailment_30min',
                'hourly': 'curtailment_hourly',
                'daily': 'curtailment_daily'
            }
            view = view_map.get(resolution, 'curtailment_30min')

            # Build WHERE clause
            conditions = [
                f"timestamp >= '{start_date.strftime('%Y-%m-%d %H:%M:%S')}'",
                f"timestamp <= '{end_date.strftime('%Y-%m-%d %H:%M:%S')}'"
            ]

            if region and region != 'All':
                conditions.append(f"region = '{region}'")

            where_clause = " AND ".join(conditions)

            # Build SELECT based on fuel filter
            if fuel and fuel != 'All':
                if fuel == 'Solar':
                    select_cols = """
                        timestamp, region,
                        solar_uigf as uigf,
                        solar_cleared as cleared,
                        solar_curtailment as curtailment,
                        'Solar' as fuel
                    """
                elif fuel == 'Wind':
                    select_cols = """
                        timestamp, region,
                        wind_uigf as uigf,
                        wind_cleared as cleared,
                        wind_curtailment as curtailment,
                        'Wind' as fuel
                    """
                else:
                    select_cols = "*"
            else:
                select_cols = "*"

            query = f"""
                SELECT {select_cols}
                FROM {view}
                WHERE {where_clause}
                ORDER BY timestamp, region
            """

            # Check cache
            cache_key = f"{view}_{start_date}_{end_date}_{region}_{fuel}"
            if cache_key in self.cache:
                cache_time = self.cache_timestamps.get(cache_key, 0)
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
            query = f"""
                SELECT
                    region,
                    -- Curtailment totals (MWh)
                    SUM(solar_curtailment) / 12 as solar_curtailment_mwh,
                    SUM(wind_curtailment) / 12 as wind_curtailment_mwh,
                    SUM(total_curtailment) / 12 as total_curtailment_mwh,
                    -- Generation totals (MWh)
                    SUM(solar_cleared) / 12 as solar_generation_mwh,
                    SUM(wind_cleared) / 12 as wind_generation_mwh,
                    SUM(solar_cleared + wind_cleared) / 12 as total_generation_mwh,
                    -- Curtailment rates (%)
                    (SUM(solar_curtailment) / NULLIF(SUM(solar_uigf), 0)) * 100 as solar_curtailment_rate_pct,
                    (SUM(wind_curtailment) / NULLIF(SUM(wind_uigf), 0)) * 100 as wind_curtailment_rate_pct,
                    (SUM(total_curtailment) / NULLIF(SUM(solar_uigf + wind_uigf), 0)) * 100 as total_curtailment_rate_pct,
                    -- Max curtailment
                    MAX(solar_curtailment) as max_solar_curtailment_mw,
                    MAX(wind_curtailment) as max_wind_curtailment_mw,
                    MAX(total_curtailment) as max_total_curtailment_mw
                FROM curtailment_regional
                WHERE timestamp >= '{start_date.strftime('%Y-%m-%d %H:%M:%S')}'
                  AND timestamp <= '{end_date.strftime('%Y-%m-%d %H:%M:%S')}'
                GROUP BY region
                ORDER BY total_curtailment_mwh DESC
            """

            return self.conn.execute(query).df()

        except Exception as e:
            logger.error(f"Error querying region summary: {e}")
            return pd.DataFrame()

    def query_fuel_summary(
        self,
        start_date: datetime,
        end_date: datetime,
        region: Optional[str] = None
    ) -> pd.DataFrame:
        """
        Get summary statistics by fuel type.

        Returns:
            DataFrame with fuel-type curtailment statistics
        """
        try:
            region_filter = f"AND region = '{region}'" if region and region != 'All' else ""

            query = f"""
                WITH fuel_data AS (
                    SELECT
                        'Solar' as fuel,
                        SUM(solar_curtailment) / 12 as curtailment_mwh,
                        SUM(solar_cleared) / 12 as generation_mwh,
                        SUM(solar_uigf) / 12 as potential_mwh,
                        (SUM(solar_curtailment) / NULLIF(SUM(solar_uigf), 0)) * 100 as curtailment_rate_pct,
                        MAX(solar_curtailment) as max_curtailment_mw
                    FROM curtailment_regional
                    WHERE timestamp >= '{start_date.strftime('%Y-%m-%d %H:%M:%S')}'
                      AND timestamp <= '{end_date.strftime('%Y-%m-%d %H:%M:%S')}'
                      {region_filter}

                    UNION ALL

                    SELECT
                        'Wind' as fuel,
                        SUM(wind_curtailment) / 12 as curtailment_mwh,
                        SUM(wind_cleared) / 12 as generation_mwh,
                        SUM(wind_uigf) / 12 as potential_mwh,
                        (SUM(wind_curtailment) / NULLIF(SUM(wind_uigf), 0)) * 100 as curtailment_rate_pct,
                        MAX(wind_curtailment) as max_curtailment_mw
                    FROM curtailment_regional
                    WHERE timestamp >= '{start_date.strftime('%Y-%m-%d %H:%M:%S')}'
                      AND timestamp <= '{end_date.strftime('%Y-%m-%d %H:%M:%S')}'
                      {region_filter}
                )
                SELECT * FROM fuel_data
                ORDER BY curtailment_mwh DESC
            """

            return self.conn.execute(query).df()

        except Exception as e:
            logger.error(f"Error querying fuel summary: {e}")
            return pd.DataFrame()

    def query_top_duids(
        self,
        start_date: datetime,
        end_date: datetime,
        top_n: int = 20,
        region: Optional[str] = None,
        curtailment_type: str = 'all'
    ) -> pd.DataFrame:
        """
        Get top N DUIDs by total curtailment for the period.

        Args:
            start_date: Start of date range
            end_date: End of date range
            top_n: Number of top DUIDs to return
            region: Optional region filter ('NSW1', 'VIC1', etc.)
            curtailment_type: 'all', 'economic' (price < 0), or 'grid' (price >= 0)

        Returns:
            DataFrame with DUID curtailment statistics sorted by total curtailment
        """
        try:
            # For curtailment type filtering, we need to join with prices
            # First get raw curtailment data with price info
            query = f"""
                SELECT
                    c.timestamp,
                    c.duid,
                    c.uigf,
                    c.totalcleared,
                    c.curtailment
                FROM curtailment_duid c
                WHERE c.timestamp >= '{start_date.strftime('%Y-%m-%d %H:%M:%S')}'
                  AND c.timestamp <= '{end_date.strftime('%Y-%m-%d %H:%M:%S')}'
                  AND c.curtailment > 0
            """

            curt_df = self.conn.execute(query).df()

            if curt_df.empty:
                logger.info(f"No curtailment data found for period")
                return pd.DataFrame()

            # Add region from mapping
            curt_df['region'] = curt_df['duid'].map(self.duid_to_region)

            # Join with prices - need to get prices for the same period
            prices_query = f"""
                SELECT timestamp, region, price
                FROM prices
                WHERE timestamp >= '{start_date.strftime('%Y-%m-%d %H:%M:%S')}'
                  AND timestamp <= '{end_date.strftime('%Y-%m-%d %H:%M:%S')}'
            """
            prices_df = self.conn.execute(prices_query).df()

            # Merge curtailment with prices
            merged = curt_df.merge(prices_df, on=['timestamp', 'region'], how='left')

            # Classify curtailment type
            merged['curt_type'] = merged['price'].apply(
                lambda x: 'economic' if pd.notna(x) and x < 0 else 'grid'
            )

            # Filter by curtailment type if specified
            if curtailment_type == 'economic':
                merged = merged[merged['curt_type'] == 'economic']
            elif curtailment_type == 'grid':
                merged = merged[merged['curt_type'] == 'grid']

            # Filter by region if specified
            if region and region != 'All':
                merged = merged[merged['region'] == region]

            if merged.empty:
                logger.info(f"No {curtailment_type} curtailment data found")
                return pd.DataFrame()

            # Aggregate by DUID
            result = merged.groupby('duid').agg({
                'curtailment': ['sum', 'max', 'mean', 'count'],
                'totalcleared': 'sum',
                'uigf': 'sum'
            }).reset_index()

            # Flatten column names
            result.columns = ['duid', 'curtailment_sum', 'max_curtailment_mw',
                            'avg_curtailment_mw', 'curtailment_intervals',
                            'totalcleared_sum', 'uigf_sum']

            # Convert to MWh (5-min intervals / 12)
            result['curtailment_mwh'] = result['curtailment_sum'] / 12
            result['generation_mwh'] = result['totalcleared_sum'] / 12
            result['uigf_mwh'] = result['uigf_sum'] / 12

            # Calculate curtailment rate
            result['curtailment_rate_pct'] = (
                result['curtailment_sum'] / result['uigf_sum'].replace(0, pd.NA) * 100
            )

            # Sort and limit
            result = result.sort_values('curtailment_mwh', ascending=False).head(top_n)

            # Select final columns
            result = result[['duid', 'curtailment_mwh', 'generation_mwh', 'uigf_mwh',
                           'curtailment_rate_pct', 'max_curtailment_mw',
                           'avg_curtailment_mw', 'curtailment_intervals']]

            logger.info(f"Queried top {top_n} DUIDs for region={region}, type={curtailment_type}, found {len(result)}")
            return result

        except Exception as e:
            logger.error(f"Error querying top DUIDs: {e}")
            import traceback
            traceback.print_exc()
            return pd.DataFrame()

    def query_top_duids_by_type(
        self,
        start_date: datetime,
        end_date: datetime,
        top_n: int = 20,
        region: Optional[str] = None
    ) -> pd.DataFrame:
        """
        Get top N DUIDs with curtailment split by type (economic vs grid).

        Args:
            start_date: Start of date range
            end_date: End of date range
            top_n: Number of top DUIDs to return
            region: Optional region filter ('NSW1', 'VIC1', etc.)

        Returns:
            DataFrame with DUID curtailment by type, suitable for side-by-side bar chart
        """
        try:
            # Get raw curtailment data
            query = f"""
                SELECT
                    c.timestamp,
                    c.duid,
                    c.uigf,
                    c.totalcleared,
                    c.curtailment
                FROM curtailment_duid c
                WHERE c.timestamp >= '{start_date.strftime('%Y-%m-%d %H:%M:%S')}'
                  AND c.timestamp <= '{end_date.strftime('%Y-%m-%d %H:%M:%S')}'
                  AND c.curtailment > 0
            """

            curt_df = self.conn.execute(query).df()

            if curt_df.empty:
                return pd.DataFrame()

            # Add region from mapping
            curt_df['region'] = curt_df['duid'].map(self.duid_to_region)

            # Filter by region if specified
            if region and region != 'All':
                curt_df = curt_df[curt_df['region'] == region]

            if curt_df.empty:
                return pd.DataFrame()

            # Get prices
            prices_query = f"""
                SELECT timestamp, region, price
                FROM prices
                WHERE timestamp >= '{start_date.strftime('%Y-%m-%d %H:%M:%S')}'
                  AND timestamp <= '{end_date.strftime('%Y-%m-%d %H:%M:%S')}'
            """
            prices_df = self.conn.execute(prices_query).df()

            # Merge
            merged = curt_df.merge(prices_df, on=['timestamp', 'region'], how='left')

            # Classify
            merged['curt_type'] = merged['price'].apply(
                lambda x: 'Economic' if pd.notna(x) and x < 0 else 'Grid'
            )

            # Aggregate by DUID and type
            agg = merged.groupby(['duid', 'curt_type']).agg({
                'curtailment': 'sum',
                'uigf': 'sum'
            }).reset_index()

            # Convert to MWh
            agg['curtailment_mwh'] = agg['curtailment'] / 12
            agg['uigf_mwh'] = agg['uigf'] / 12

            # Pivot to get economic and grid side by side
            pivot = agg.pivot_table(
                index='duid',
                columns='curt_type',
                values='curtailment_mwh',
                fill_value=0
            ).reset_index()

            # Ensure both columns exist
            if 'Economic' not in pivot.columns:
                pivot['Economic'] = 0
            if 'Grid' not in pivot.columns:
                pivot['Grid'] = 0

            # Calculate total and sort
            pivot['Total'] = pivot['Economic'] + pivot['Grid']
            pivot = pivot.sort_values('Total', ascending=False).head(top_n)

            # Melt back to long format for plotting
            result = pivot.melt(
                id_vars=['duid', 'Total'],
                value_vars=['Economic', 'Grid'],
                var_name='curtailment_type',
                value_name='curtailment_mwh'
            )

            logger.info(f"Queried top {top_n} DUIDs by type for region={region}, found {len(pivot)} DUIDs")
            return result

        except Exception as e:
            logger.error(f"Error querying top DUIDs by type: {e}")
            import traceback
            traceback.print_exc()
            return pd.DataFrame()

    def query_duid_timeseries(
        self,
        start_date: datetime,
        end_date: datetime,
        duid: str,
        resolution: str = 'hourly'
    ) -> pd.DataFrame:
        """
        Get time series data for a specific DUID.

        Args:
            start_date: Start of date range
            end_date: End of date range
            duid: The DUID to query
            resolution: Data resolution ('5min', '30min', 'hourly', 'daily')

        Returns:
            DataFrame with DUID curtailment time series
        """
        try:
            # Build aggregation based on resolution
            if resolution == '5min':
                time_expr = "timestamp"
                agg_suffix = ""
            elif resolution == '30min':
                time_expr = "date_trunc('hour', timestamp) + INTERVAL '30 minutes' * FLOOR(EXTRACT(MINUTE FROM timestamp) / 30)"
                agg_suffix = "AVG"
            elif resolution == 'hourly':
                time_expr = "date_trunc('hour', timestamp)"
                agg_suffix = "AVG"
            else:  # daily
                time_expr = "date_trunc('day', timestamp)"
                agg_suffix = "SUM"  # Sum for daily to get MWh

            if resolution == '5min':
                query = f"""
                    SELECT
                        timestamp,
                        duid,
                        uigf,
                        totalcleared,
                        curtailment
                    FROM curtailment_duid
                    WHERE timestamp >= '{start_date.strftime('%Y-%m-%d %H:%M:%S')}'
                      AND timestamp <= '{end_date.strftime('%Y-%m-%d %H:%M:%S')}'
                      AND duid = '{duid}'
                    ORDER BY timestamp
                """
            else:
                divisor = "/ 12" if resolution == 'daily' else ""
                query = f"""
                    SELECT
                        {time_expr} as timestamp,
                        duid,
                        {agg_suffix}(uigf) {divisor} as uigf,
                        {agg_suffix}(totalcleared) {divisor} as totalcleared,
                        {agg_suffix}(curtailment) {divisor} as curtailment
                    FROM curtailment_duid
                    WHERE timestamp >= '{start_date.strftime('%Y-%m-%d %H:%M:%S')}'
                      AND timestamp <= '{end_date.strftime('%Y-%m-%d %H:%M:%S')}'
                      AND duid = '{duid}'
                    GROUP BY 1, 2
                    ORDER BY timestamp
                """

            result = self.conn.execute(query).df()
            logger.info(f"Queried {len(result)} {resolution} records for {duid}")
            return result

        except Exception as e:
            logger.error(f"Error querying DUID timeseries: {e}")
            return pd.DataFrame()

    def get_duid_list(self) -> List[str]:
        """Get list of all DUIDs with curtailment data"""
        try:
            query = "SELECT DISTINCT duid FROM curtailment_duid ORDER BY duid"
            result = self.conn.execute(query).df()
            return result['duid'].tolist()
        except Exception as e:
            logger.error(f"Error getting DUID list: {e}")
            return []

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
                    COUNT(DISTINCT region) as region_count,
                    COUNT(*) as total_records
                FROM curtailment_regional
            """).df()

            if not coverage.empty:
                stats['data_coverage'] = {
                    'earliest': coverage['earliest'].iloc[0],
                    'latest': coverage['latest'].iloc[0],
                    'region_count': coverage['region_count'].iloc[0],
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

    print("Testing CurtailmentQueryManager with regional data...")

    manager = CurtailmentQueryManager()

    # Test 1: Query last 7 days
    end_date = datetime.now()
    start_date = end_date - timedelta(days=7)

    print(f"\n1. Querying 7 days of data...")
    t1 = time.time()
    data = manager.query_curtailment_data(start_date, end_date, region='NSW1')
    print(f"   Query completed in {time.time() - t1:.2f}s")
    print(f"   Records: {len(data):,}")
    if not data.empty:
        print(f"   Columns: {list(data.columns)}")
        print(f"   Sample:\n{data.head()}")

    # Test 2: Regional summary
    print(f"\n2. Querying regional summary...")
    t2 = time.time()
    summary = manager.query_region_summary(start_date, end_date)
    print(f"   Query completed in {time.time() - t2:.2f}s")
    print(summary)

    # Test 3: Fuel summary
    print(f"\n3. Querying fuel summary...")
    t3 = time.time()
    fuel_summary = manager.query_fuel_summary(start_date, end_date)
    print(f"   Query completed in {time.time() - t3:.2f}s")
    print(fuel_summary)

    # Show statistics
    print(f"\n4. Statistics:")
    stats = manager.get_statistics()
    for key, value in stats.items():
        print(f"   {key}: {value}")
