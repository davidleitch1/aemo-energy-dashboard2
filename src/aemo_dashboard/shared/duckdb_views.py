"""
DuckDB Views - Pre-defined views and materialized views for common operations

This module creates and manages DuckDB views that optimize common query patterns
used throughout the dashboard. It includes integrated views, aggregation views,
and helper functions for view management.
"""

import logging
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional

from .logging_config import get_logger
from .performance_logging import PerformanceLogger, performance_monitor
from .constants import MINUTES_5_TO_HOURS, MINUTES_30_TO_HOURS
from data_service.shared_data_duckdb import duckdb_data_service

logger = get_logger(__name__)
perf_logger = PerformanceLogger(__name__)


class DuckDBViewManager:
    """Manages DuckDB views for optimized querying"""

    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._initialized = False
            cls._instance._views_created = False
        return cls._instance

    def __init__(self):
        """Initialize view manager - actual work deferred to first access"""
        pass

    @property
    def conn(self):
        """Lazy access to DuckDB connection"""
        return duckdb_data_service.conn

    def _ensure_initialized(self):
        """Ensure views are created on first use"""
        if not self._initialized:
            self._initialized = True
            self.create_all_views()
    
    @performance_monitor(threshold=1.0)
    def create_all_views(self) -> None:
        """Create all optimization views (skips if already exist in persistent DB)"""
        if self._views_created:
            logger.debug("Views already created this session, skipping")
            return

        # Check if views already exist in persistent DB
        if self._check_views_exist():
            logger.info("Views already exist in persistent DB, skipping creation")
            self._views_created = True
            return

        logger.info("Creating DuckDB optimization views...")

        with perf_logger.timer("create_views", threshold=0.5):
            self._create_integration_views()
            self._create_aggregation_views()
            self._create_helper_views()
            self._create_materialized_views()

        self._views_created = True
        logger.info("All DuckDB views created successfully")

    def _check_views_exist(self) -> bool:
        """Check if optimization views already exist in persistent DB"""
        try:
            result = self.conn.execute("""
                SELECT COUNT(*) FROM information_schema.tables
                WHERE table_name = 'integrated_data_30min' AND table_type = 'VIEW'
            """).fetchone()
            return result[0] > 0
        except Exception:
            return False

    def force_recreate_views(self) -> None:
        """Force recreation of all views (useful after schema changes)"""
        logger.info("Force recreating all views...")
        self._views_created = False
        # Drop existing views first
        for view in ['integrated_data_30min', 'integrated_data_5min',
                     'hourly_by_fuel_region', 'daily_by_fuel', 'daily_by_station',
                     'active_stations', 'price_stats_by_region', 'high_price_events',
                     'station_time_series_5min', 'station_time_series_30min',
                     'station_time_of_day', 'station_performance_metrics',
                     'generation_by_fuel_30min', 'generation_by_fuel_5min',
                     'generation_with_prices_30min', 'capacity_utilization_30min',
                     'daily_generation_by_fuel']:
            try:
                self.conn.execute(f"DROP VIEW IF EXISTS {view}")
            except Exception:
                pass
        try:
            self.conn.execute("DROP TABLE IF EXISTS monthly_summary")
        except Exception:
            pass
        # Recreate all
        self._create_integration_views()
        self._create_aggregation_views()
        self._create_helper_views()
        self._create_materialized_views()
        self._views_created = True
        logger.info("All views recreated")

    def _create_integration_views(self) -> None:
        """Create views that integrate generation, price, and DUID data"""
        
        # 30-minute integrated data view
        # Revenue = MW × $/MWh × 0.5 hours
        self.conn.execute(f"""
            CREATE OR REPLACE VIEW integrated_data_30min AS
            SELECT
                g.settlementdate,
                g.duid,
                g.scadavalue,
                d."Site Name" as station_name,
                d.Owner as owner,
                d.Fuel as fuel_type,
                d.Region as region,
                d."Capacity(MW)" as nameplate_capacity,
                p.settlementdate as price_settlementdate,
                p.regionid as price_region,
                p.RRP as rrp,
                g.scadavalue * p.RRP * {MINUTES_30_TO_HOURS} as revenue_30min,
                -- Additional calculated fields
                CASE
                    WHEN d."Capacity(MW)" > 0
                    THEN g.scadavalue / d."Capacity(MW)" * 100
                    ELSE 0
                END as capacity_factor_pct,
                date_trunc('hour', g.settlementdate) as hour,
                date_trunc('day', g.settlementdate) as date
            FROM generation_30min g
            LEFT JOIN duid_mapping d ON g.duid = d.DUID
            LEFT JOIN prices_30min p
                ON g.settlementdate = p.settlementdate
                AND d.Region = p.regionid
        """)
        
        # 5-minute integrated data view
        # Revenue = MW × $/MWh × (5/60) hours
        self.conn.execute(f"""
            CREATE OR REPLACE VIEW integrated_data_5min AS
            SELECT
                g.settlementdate,
                g.duid,
                g.scadavalue,
                d."Site Name" as station_name,
                d.Owner as owner,
                d.Fuel as fuel_type,
                d.Region as region,
                d."Capacity(MW)" as nameplate_capacity,
                p.settlementdate as price_settlementdate,
                p.regionid as price_region,
                p.rrp as rrp,
                g.scadavalue * p.rrp * {MINUTES_5_TO_HOURS} as revenue_5min,
                -- Additional calculated fields
                CASE
                    WHEN d."Capacity(MW)" > 0
                    THEN g.scadavalue / d."Capacity(MW)" * 100
                    ELSE 0
                END as capacity_factor_pct,
                date_trunc('hour', g.settlementdate) as hour,
                date_trunc('day', g.settlementdate) as date
            FROM generation_5min g
            LEFT JOIN duid_mapping d ON g.duid = d.DUID
            LEFT JOIN prices_5min p
                ON g.settlementdate = p.settlementdate
                AND d.Region = p.regionid
        """)
        
        logger.debug("Created integration views")
    
    def _create_aggregation_views(self) -> None:
        """Create pre-aggregated views for common groupings"""
        
        # Hourly aggregation by fuel type and region
        self.conn.execute("""
            CREATE OR REPLACE VIEW hourly_by_fuel_region AS
            SELECT 
                date_trunc('hour', settlementdate) as hour,
                fuel_type,
                region,
                COUNT(DISTINCT duid) as unit_count,
                SUM(scadavalue) as total_generation_mw,
                AVG(scadavalue) as avg_generation_mw,
                MAX(scadavalue) as max_generation_mw,
                SUM(revenue_30min) as total_revenue,
                AVG(rrp) as avg_price,
                MAX(rrp) as max_price,
                MIN(rrp) as min_price
            FROM integrated_data_30min
            GROUP BY 1, 2, 3
        """)
        
        # Daily aggregation by fuel type
        self.conn.execute("""
            CREATE OR REPLACE VIEW daily_by_fuel AS
            SELECT 
                date_trunc('day', settlementdate) as date,
                fuel_type,
                COUNT(DISTINCT duid) as unit_count,
                COUNT(DISTINCT region) as region_count,
                SUM(scadavalue) as total_generation_mw,
                SUM(revenue_30min) as total_revenue,
                AVG(rrp) as avg_price,
                SUM(nameplate_capacity) / COUNT(DISTINCT duid) as total_capacity_mw
            FROM integrated_data_30min
            GROUP BY 1, 2
        """)
        
        # Station-level daily aggregation
        self.conn.execute("""
            CREATE OR REPLACE VIEW daily_by_station AS
            SELECT 
                date_trunc('day', settlementdate) as date,
                station_name,
                owner,
                fuel_type,
                region,
                COUNT(DISTINCT duid) as unit_count,
                SUM(scadavalue) as total_generation_mw,
                SUM(revenue_30min) as total_revenue,
                AVG(rrp) as avg_price,
                AVG(capacity_factor_pct) as avg_capacity_factor,
                MAX(nameplate_capacity) as station_capacity_mw
            FROM integrated_data_30min
            WHERE station_name IS NOT NULL
            GROUP BY 1, 2, 3, 4, 5
        """)
        
        logger.debug("Created aggregation views")
        
        # Station-specific views for station analysis
        self._create_station_analysis_views()
        
        # Generation dashboard-specific views
        self._create_generation_dashboard_views()
    
    def _create_helper_views(self) -> None:
        """Create helper views for common queries"""
        
        # Active stations view
        self.conn.execute("""
            CREATE OR REPLACE VIEW active_stations AS
            SELECT DISTINCT
                station_name,
                owner,
                fuel_type,
                region,
                MAX(nameplate_capacity) as capacity_mw,
                COUNT(DISTINCT duid) as unit_count
            FROM integrated_data_30min
            WHERE settlementdate >= CURRENT_DATE - INTERVAL '7 days'
              AND scadavalue > 0
              AND station_name IS NOT NULL
            GROUP BY 1, 2, 3, 4
            ORDER BY capacity_mw DESC
        """)
        
        # Price statistics by region
        self.conn.execute("""
            CREATE OR REPLACE VIEW price_stats_by_region AS
            SELECT 
                regionid,
                COUNT(*) as record_count,
                AVG(rrp) as avg_price,
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY rrp) as median_price,
                PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY rrp) as q1_price,
                PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY rrp) as q3_price,
                MIN(rrp) as min_price,
                MAX(rrp) as max_price,
                STDDEV(rrp) as price_volatility
            FROM prices_30min
            WHERE settlementdate >= CURRENT_DATE - INTERVAL '30 days'
            GROUP BY regionid
        """)
        
        # Recent high price events
        self.conn.execute("""
            CREATE OR REPLACE VIEW high_price_events AS
            SELECT 
                settlementdate,
                regionid,
                rrp
            FROM prices_30min
            WHERE rrp > 300
              AND settlementdate >= CURRENT_DATE - INTERVAL '30 days'
            ORDER BY settlementdate DESC
        """)
        
        logger.debug("Created helper views")
    
    def _create_station_analysis_views(self) -> None:
        """Create views specifically for station analysis"""
        logger.debug("Creating station analysis views...")
        
        # Station time series for detailed analysis (5min)
        # Revenue = MW × $/MWh × (5/60) hours
        self.conn.execute(f"""
            CREATE OR REPLACE VIEW station_time_series_5min AS
            SELECT
                g.settlementdate,
                g.duid,
                g.scadavalue,
                p.rrp as price,
                g.scadavalue * p.rrp * {MINUTES_5_TO_HOURS} as revenue_5min,
                d."Site Name" as station_name,
                d."Owner" as owner,
                d."Region" as region,
                d."Fuel" as fuel_type,
                d."Capacity(MW)" as capacity_mw
            FROM generation_5min g
            JOIN duid_mapping d ON g.duid = d.duid
            JOIN prices_5min p ON g.settlementdate = p.settlementdate
                AND d."Region" = p.regionid
        """)
        
        # Station time series (30min version)
        # Revenue = MW × $/MWh × 0.5 hours
        self.conn.execute(f"""
            CREATE OR REPLACE VIEW station_time_series_30min AS
            SELECT
                g.settlementdate,
                g.duid,
                g.scadavalue,
                p.rrp as price,
                g.scadavalue * p.rrp * {MINUTES_30_TO_HOURS} as revenue_30min,
                d."Site Name" as station_name,
                d."Owner" as owner,
                d."Region" as region,
                d."Fuel" as fuel_type,
                d."Capacity(MW)" as capacity_mw
            FROM generation_30min g
            JOIN duid_mapping d ON g.duid = d.duid
            JOIN prices_30min p ON g.settlementdate = p.settlementdate
                AND d."Region" = p.regionid
        """)
        
        # Time of day averages for station analysis
        # Average revenue uses 5-minute data
        self.conn.execute(f"""
            CREATE OR REPLACE VIEW station_time_of_day AS
            SELECT
                duid,
                EXTRACT(hour FROM settlementdate) as hour,
                AVG(scadavalue) as avg_generation_mw,
                AVG(price) as avg_price,
                AVG(scadavalue * price * {MINUTES_5_TO_HOURS}) as avg_revenue,
                COUNT(*) as data_points
            FROM station_time_series_5min
            GROUP BY duid, hour
            ORDER BY duid, hour
        """)
        
        # Station performance metrics
        # Uses 5-minute data for calculations
        self.conn.execute(f"""
            CREATE OR REPLACE VIEW station_performance_metrics AS
            SELECT
                duid,
                MAX(station_name) as station_name,
                MAX(owner) as owner,
                MAX(region) as region,
                MAX(fuel_type) as fuel_type,
                MAX(capacity_mw) as capacity_mw,
                MIN(settlementdate) as start_date,
                MAX(settlementdate) as end_date,
                SUM(scadavalue * {MINUTES_5_TO_HOURS}) as total_generation_mwh,
                SUM(revenue_5min) as total_revenue,
                AVG(scadavalue) as avg_generation_mw,
                MAX(scadavalue) as max_generation_mw,
                MIN(scadavalue) as min_generation_mw,
                STDDEV(scadavalue) as std_generation_mw,
                AVG(price) as avg_price_received,
                COUNT(*) as intervals,
                SUM(CASE WHEN scadavalue > 0 THEN 1 ELSE 0 END) as intervals_generating
            FROM station_time_series_5min
            GROUP BY duid
        """)
        
        logger.debug("Created station analysis views")
    
    def _create_generation_dashboard_views(self) -> None:
        """Create views specifically for generation dashboard performance"""
        logger.debug("Creating generation dashboard views...")
        
        # Generation by fuel type with region (30min) - for long date ranges
        self.conn.execute("""
            CREATE OR REPLACE VIEW generation_by_fuel_30min AS
            SELECT 
                g.settlementdate,
                d.Fuel as fuel_type,
                d.Region as region,
                SUM(g.scadavalue) as total_generation_mw,
                COUNT(DISTINCT g.duid) as unit_count,
                SUM(d."Capacity(MW)") as total_capacity_mw
            FROM generation_30min g
            JOIN duid_mapping d ON g.duid = d.DUID
            WHERE d.Fuel IS NOT NULL
            GROUP BY g.settlementdate, d.Fuel, d.Region
            ORDER BY g.settlementdate, d.Fuel
        """)
        
        # Generation by fuel type with region (5min) - for short date ranges
        self.conn.execute("""
            CREATE OR REPLACE VIEW generation_by_fuel_5min AS
            SELECT 
                g.settlementdate,
                d.Fuel as fuel_type,
                d.Region as region,
                SUM(g.scadavalue) as total_generation_mw,
                COUNT(DISTINCT g.duid) as unit_count,
                SUM(d."Capacity(MW)") as total_capacity_mw
            FROM generation_5min g
            JOIN duid_mapping d ON g.duid = d.DUID
            WHERE d.Fuel IS NOT NULL
            GROUP BY g.settlementdate, d.Fuel, d.Region
            ORDER BY g.settlementdate, d.Fuel
        """)
        
        # Generation with price data (integrated view for revenue calculations)
        # Revenue = MW × $/MWh × 0.5 hours
        self.conn.execute(f"""
            CREATE OR REPLACE VIEW generation_with_prices_30min AS
            SELECT
                g.settlementdate,
                g.fuel_type,
                g.region,
                g.total_generation_mw,
                g.unit_count,
                g.total_capacity_mw,
                p.RRP as price,
                g.total_generation_mw * p.RRP * {MINUTES_30_TO_HOURS} as revenue_30min
            FROM generation_by_fuel_30min g
            LEFT JOIN prices_30min p
                ON g.settlementdate = p.settlementdate
                AND g.region = p.regionid
        """)
        
        # Capacity utilization by fuel type
        self.conn.execute("""
            CREATE OR REPLACE VIEW capacity_utilization_30min AS
            SELECT 
                settlementdate,
                fuel_type,
                region,
                total_generation_mw,
                total_capacity_mw,
                CASE 
                    WHEN total_capacity_mw > 0 
                    THEN (total_generation_mw / total_capacity_mw) * 100
                    ELSE 0 
                END as utilization_pct
            FROM generation_by_fuel_30min
            WHERE total_capacity_mw > 0
        """)
        
        # Daily generation summary by fuel (for overview displays)
        # Convert 30-min MW readings to MWh
        self.conn.execute(f"""
            CREATE OR REPLACE VIEW daily_generation_by_fuel AS
            SELECT
                DATE_TRUNC('day', settlementdate) as date,
                fuel_type,
                region,
                AVG(total_generation_mw) as avg_generation_mw,
                MAX(total_generation_mw) as max_generation_mw,
                MIN(total_generation_mw) as min_generation_mw,
                SUM(total_generation_mw * {MINUTES_30_TO_HOURS}) as total_generation_mwh,
                AVG(utilization_pct) as avg_utilization_pct
            FROM capacity_utilization_30min
            GROUP BY 1, 2, 3
        """)
        
        logger.debug("Created generation dashboard views")
    
    def _create_materialized_views(self) -> None:
        """Create materialized views for expensive queries"""
        
        # Note: DuckDB doesn't support true materialized views yet,
        # so we create tables that can be refreshed periodically
        
        # Monthly summary table
        self.conn.execute("""
            CREATE OR REPLACE TABLE monthly_summary AS
            SELECT 
                date_trunc('month', settlementdate) as month,
                fuel_type,
                region,
                owner,
                COUNT(DISTINCT duid) as unit_count,
                COUNT(*) as record_count,
                SUM(scadavalue) as total_generation_mw,
                SUM(revenue_30min) as total_revenue,
                AVG(rrp) as avg_price,
                AVG(capacity_factor_pct) as avg_capacity_factor
            FROM integrated_data_30min
            GROUP BY 1, 2, 3, 4
        """)
        
        logger.debug("Created materialized views (as tables)")
    
    def refresh_materialized_views(self) -> None:
        """Refresh materialized views (recreate tables)"""
        self._ensure_initialized()
        logger.info("Refreshing materialized views...")

        with perf_logger.timer("refresh_materialized_views", threshold=1.0):
            # Drop and recreate monthly summary
            self.conn.execute("DROP TABLE IF EXISTS monthly_summary")
            self._create_materialized_views()

        logger.info("Materialized views refreshed")
    
    def get_view_list(self) -> List[str]:
        """Get list of available views"""
        self._ensure_initialized()
        result = self.conn.execute("""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_type = 'VIEW'
            ORDER BY table_name
        """).fetchall()

        return [row[0] for row in result]

    def get_view_info(self, view_name: str) -> Dict[str, Any]:
        """Get information about a specific view"""
        self._ensure_initialized()
        # Get column information
        columns = self.conn.execute(f"""
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_name = '{view_name}'
            ORDER BY ordinal_position
        """).fetchall()
        
        # Get row count (sample for performance)
        row_count = self.conn.execute(f"""
            SELECT COUNT(*) FROM {view_name} LIMIT 1
        """).fetchone()[0]
        
        return {
            'name': view_name,
            'columns': [{'name': col[0], 'type': col[1]} for col in columns],
            'row_count': row_count
        }
    
    def create_custom_view(
        self,
        view_name: str,
        query: str,
        replace: bool = True
    ) -> bool:
        """Create a custom view from a query"""
        self._ensure_initialized()
        try:
            if replace:
                create_stmt = f"CREATE OR REPLACE VIEW {view_name} AS {query}"
            else:
                create_stmt = f"CREATE VIEW {view_name} AS {query}"
            
            self.conn.execute(create_stmt)
            logger.info(f"Created custom view: {view_name}")
            return True
            
        except Exception as e:
            logger.error(f"Error creating view {view_name}: {e}")
            return False
    
    def drop_view(self, view_name: str) -> bool:
        """Drop a view"""
        self._ensure_initialized()
        try:
            self.conn.execute(f"DROP VIEW IF EXISTS {view_name}")
            logger.info(f"Dropped view: {view_name}")
            return True
            
        except Exception as e:
            logger.error(f"Error dropping view {view_name}: {e}")
            return False


# Lazy singleton - views created on first use, not at import
view_manager = DuckDBViewManager()


# Convenience functions
def get_integrated_data_query(
    start_date: datetime,
    end_date: datetime,
    resolution: str = '30min',
    columns: Optional[List[str]] = None
) -> str:
    """
    Build query for integrated data with optional column selection.
    
    Args:
        start_date: Start date
        end_date: End date
        resolution: '5min' or '30min'
        columns: List of columns to select (None = all)
        
    Returns:
        SQL query string
    """
    view_name = f"integrated_data_{resolution}"
    
    if columns:
        select_clause = ", ".join(columns)
    else:
        select_clause = "*"
    
    query = f"""
    SELECT {select_clause}
    FROM {view_name}
    WHERE settlementdate >= '{start_date.strftime('%Y-%m-%d %H:%M:%S')}'
      AND settlementdate <= '{end_date.strftime('%Y-%m-%d %H:%M:%S')}'
    ORDER BY settlementdate
    """
    
    return query


def get_aggregation_query(
    aggregation_level: str,
    group_by: List[str],
    start_date: Optional[datetime] = None,
    end_date: Optional[datetime] = None
) -> str:
    """
    Build query for pre-aggregated data.
    
    Args:
        aggregation_level: 'hourly' or 'daily'
        group_by: List of grouping columns
        start_date: Optional start date filter
        end_date: Optional end date filter
        
    Returns:
        SQL query string
    """
    # Map to appropriate view
    if aggregation_level == 'hourly' and 'fuel_type' in group_by and 'region' in group_by:
        view_name = 'hourly_by_fuel_region'
        date_col = 'hour'
    elif aggregation_level == 'daily' and 'fuel_type' in group_by:
        view_name = 'daily_by_fuel'
        date_col = 'date'
    elif aggregation_level == 'daily' and 'station_name' in group_by:
        view_name = 'daily_by_station'
        date_col = 'date'
    else:
        # Fallback to integrated data
        view_name = 'integrated_data_30min'
        date_col = 'settlementdate'
    
    query = f"SELECT * FROM {view_name}"
    
    # Add date filters if provided
    filters = []
    if start_date:
        filters.append(f"{date_col} >= '{start_date.strftime('%Y-%m-%d %H:%M:%S')}'")
    if end_date:
        filters.append(f"{date_col} <= '{end_date.strftime('%Y-%m-%d %H:%M:%S')}'")
    
    if filters:
        query += " WHERE " + " AND ".join(filters)
    
    query += f" ORDER BY {date_col}"
    
    return query


# Example usage and testing
if __name__ == "__main__":
    # Test view creation
    print("Testing DuckDB view manager...")
    
    # List available views
    views = view_manager.get_view_list()
    print(f"\nAvailable views: {len(views)}")
    for view in views[:5]:  # Show first 5
        print(f"  - {view}")
    
    # Get view info
    if views:
        view_info = view_manager.get_view_info('integrated_data_30min')
        print(f"\nView info for 'integrated_data_30min':")
        print(f"  Columns: {len(view_info['columns'])}")
        print(f"  Row count: {view_info['row_count']:,}")
    
    # Test query building
    end_date = datetime.now()
    start_date = end_date - timedelta(days=1)
    
    query = get_integrated_data_query(start_date, end_date, '30min', ['settlementdate', 'fuel_type', 'scadavalue'])
    print(f"\nSample query:\n{query[:200]}...")
    
    print("\nDuckDB views initialized successfully!")