"""
NEM Dashboard Query Manager - Specialized query manager for real-time dashboard data

This module provides optimized queries for the NEM dashboard using DuckDB
to replace direct parquet reads with efficient, memory-conscious queries.
"""

import pandas as pd
import logging
from datetime import datetime, timedelta
from typing import Optional, Dict, Any, Tuple

from ..shared.logging_config import get_logger
from ..shared.performance_logging import PerformanceLogger, performance_monitor
from ..shared.hybrid_query_manager import HybridQueryManager
from ..shared.duckdb_views import view_manager
from ..generation.generation_query_manager import GenerationQueryManager
from ..shared import adapter_selector

logger = get_logger(__name__)
perf_logger = PerformanceLogger(__name__)


class NEMDashQueryManager:
    """Specialized query manager for NEM dashboard real-time data"""
    
    def __init__(self):
        """Initialize with hybrid query manager and other specialized managers"""
        self.query_manager = HybridQueryManager(cache_size_mb=100, cache_ttl=300)
        self.generation_manager = GenerationQueryManager()
        
        # Ensure all views are created
        view_manager.create_all_views()
        
        logger.info("NEMDashQueryManager initialized")
    
    @performance_monitor(threshold=0.5)
    def get_current_spot_prices(self) -> pd.DataFrame:
        """
        Get current spot prices for all regions.
        Returns the most recent price for each region.
        
        Returns:
            DataFrame with columns: REGIONID, RRP, SETTLEMENTDATE
        """
        try:
            # Get last 2 hours of data to account for QLD/NSW timezone offset during DST
            # AEMO data timestamps are in QLD time (no DST), system may be NSW time (+1hr during DST)
            end_date = datetime.now()
            start_date = end_date - timedelta(hours=2)
            
            # Use price adapter function which handles DuckDB when enabled
            prices = adapter_selector.load_price_data(
                start_date=start_date,
                end_date=end_date,
                resolution='5min'
            )
            
            if prices.empty:
                logger.warning("No price data available")
                return pd.DataFrame()

            # Reset index to access SETTLEMENTDATE as a column (adapter returns it as index)
            prices_reset = prices.reset_index()

            # Get the most recent price for each region
            latest_prices = prices_reset.groupby('REGIONID').last()
            latest_prices['SETTLEMENTDATE'] = prices_reset.groupby('REGIONID')['SETTLEMENTDATE'].last()
            
            logger.info(f"Loaded current prices for {len(latest_prices)} regions")
            
            return latest_prices.reset_index()
            
        except Exception as e:
            logger.error(f"Error getting current spot prices: {e}")
            return pd.DataFrame()
    
    @performance_monitor(threshold=0.5)
    def get_price_history(self, hours: int = 10) -> pd.DataFrame:
        """
        Get price history for the specified number of hours.
        
        Args:
            hours: Number of hours of history to retrieve
            
        Returns:
            DataFrame pivoted with SETTLEMENTDATE as index and regions as columns
        """
        try:
            end_date = datetime.now()
            start_date = end_date - timedelta(hours=hours)
            
            # Use price adapter function
            prices = adapter_selector.load_price_data(
                start_date=start_date,
                end_date=end_date,
                resolution='5min'
            )
            
            if prices.empty:
                logger.warning("No price history available")
                return pd.DataFrame()

            # Reset index to access SETTLEMENTDATE as a column (adapter returns it as index)
            prices_reset = prices.reset_index()

            # Pivot to match expected format
            price_pivot = prices_reset.pivot(
                index='SETTLEMENTDATE',
                columns='REGIONID',
                values='RRP'
            )
            
            logger.info(f"Loaded {len(price_pivot)} price records for {hours} hours")
            
            return price_pivot
            
        except Exception as e:
            logger.error(f"Error getting price history: {e}")
            return pd.DataFrame()
    
    @performance_monitor(threshold=1.0)
    def get_generation_overview(self, hours: int = 24) -> pd.DataFrame:
        """
        Get generation data for overview chart.
        
        Args:
            hours: Number of hours of data to retrieve
            
        Returns:
            DataFrame with generation by fuel type
        """
        try:
            end_date = datetime.now()
            start_date = end_date - timedelta(hours=hours)
            
            # Use generation query manager for optimized queries
            generation_data = self.generation_manager.query_generation_by_fuel(
                start_date=start_date,
                end_date=end_date,
                region='NEM',
                resolution='5min'
            )
            
            if generation_data.empty:
                logger.warning("No generation data available")
                return pd.DataFrame()
            
            # Pivot to get fuel types as columns
            if 'fuel_type' in generation_data.columns:
                pivot_data = generation_data.pivot(
                    index='settlementdate',
                    columns='fuel_type',
                    values='total_generation_mw'
                ).fillna(0)
            else:
                # Data might already be pivoted
                pivot_data = generation_data.set_index('settlementdate')
            
            logger.info(f"Loaded generation data: {pivot_data.shape}")
            
            return pivot_data
            
        except Exception as e:
            logger.error(f"Error getting generation overview: {e}")
            return pd.DataFrame()
    
    @performance_monitor(threshold=0.5)
    def get_renewable_data(self) -> Dict[str, float]:
        """
        Get current renewable generation data for gauge.

        Returns:
            Dictionary with renewable and total generation
        """
        try:
            # Get last 2 hours of data to account for QLD/NSW timezone offset during DST
            # AEMO data timestamps are in QLD time (no DST), system may be NSW time (+1hr during DST)
            end_date = datetime.now()
            start_date = end_date - timedelta(hours=2)

            # Query generation by fuel
            generation_data = self.generation_manager.query_generation_by_fuel(
                start_date=start_date,
                end_date=end_date,
                region='NEM',
                resolution='5min'
            )

            if generation_data.empty:
                # Fallback: Try last 4 hours if initial query fails
                start_date = end_date - timedelta(hours=4)
                generation_data = self.generation_manager.query_generation_by_fuel(
                    start_date=start_date,
                    end_date=end_date,
                    region='NEM',
                    resolution='5min'
                )

            if generation_data.empty:
                logger.warning("No generation data for renewable calculation")
                return {'renewable_mw': 0, 'total_mw': 0, 'renewable_pct': 0}

            # Get the most recent data point
            latest = generation_data.groupby('fuel_type')['total_generation_mw'].last()

            # Define renewable fuel types
            renewable_fuels = ['Wind', 'Solar', 'Rooftop Solar', 'Water', 'Hydro', 'Biomass']

            # Define actual generation fuel types (excludes storage and transmission)
            generation_fuels = ['Coal', 'CCGT', 'OCGT', 'Gas other', 'Other',
                              'Wind', 'Solar', 'Rooftop Solar', 'Water', 'Hydro', 'Biomass']

            renewable_mw = latest[latest.index.isin(renewable_fuels)].sum()
            # Only sum actual generation sources, not storage or transmission
            total_mw = latest[latest.index.isin(generation_fuels)].sum()

            # IMPORTANT: Add rooftop solar separately as it's not in the generation_by_fuel view
            # The generation_by_fuel view only includes DUID-based generation from scada data
            # Rooftop solar comes from a separate source with regionids
            rooftop_mw = 0
            try:
                # Query rooftop solar directly from rooftop_30min table
                # Note: rooftop data has 'power' column, not 'value', and is split by regionid
                import os
                data_dir = os.environ.get('DATA_DIR', os.environ.get('AEMO_DATA_PATH', '/Users/davidleitch/aemo_production/data'))
                rooftop_path = os.path.join(data_dir, "rooftop30.parquet")

                rooftop_query = """
                    SELECT SUM(CAST(power AS DOUBLE)) as total_rooftop
                    FROM read_parquet(?)
                    WHERE settlementdate >= ?
                    AND settlementdate <= ?
                    AND settlementdate = (
                        SELECT MAX(settlementdate)
                        FROM read_parquet(?)
                        WHERE settlementdate <= ?
                    )
                """

                rooftop_result = self.query_manager.conn.execute(
                    rooftop_query,
                    [rooftop_path, start_date, end_date, rooftop_path, end_date]
                ).fetchone()

                if rooftop_result and rooftop_result[0]:
                    rooftop_mw = float(rooftop_result[0])
                    logger.info(f"Rooftop solar: {rooftop_mw:.0f}MW")
                else:
                    # Fallback: try last hour of rooftop data
                    rooftop_result = self.query_manager.conn.execute(
                        rooftop_query,
                        [rooftop_path, end_date - timedelta(hours=1), end_date, rooftop_path, end_date]
                    ).fetchone()
                    if rooftop_result and rooftop_result[0]:
                        rooftop_mw = float(rooftop_result[0])
                        logger.info(f"Rooftop solar (fallback): {rooftop_mw:.0f}MW")
            except Exception as e:
                logger.warning(f"Could not get rooftop solar data: {e}")

            # Add rooftop to both renewable and total
            renewable_mw += rooftop_mw
            total_mw += rooftop_mw

            renewable_pct = (renewable_mw / total_mw * 100) if total_mw > 0 else 0

            logger.info(f"Renewable: {renewable_mw:.0f}MW / {total_mw:.0f}MW = {renewable_pct:.1f}%")

            return {
                'renewable_mw': renewable_mw,
                'total_mw': total_mw,
                'renewable_pct': renewable_pct
            }

        except Exception as e:
            logger.error(f"Error getting renewable data: {e}")
            return {'renewable_mw': 0, 'total_mw': 0, 'renewable_pct': 0}
    
    @performance_monitor(threshold=0.5)
    def get_transmission_flows(self, hours: int = 24) -> pd.DataFrame:
        """
        Get transmission flow data for the specified hours.
        
        Args:
            hours: Number of hours of data to retrieve
            
        Returns:
            DataFrame with transmission flows by interconnector
        """
        try:
            end_date = datetime.now()
            start_date = end_date - timedelta(hours=hours)
            
            # Use transmission adapter function
            transmission_data = adapter_selector.load_transmission_data(
                start_date=start_date,
                end_date=end_date
            )
            
            if transmission_data.empty:
                logger.warning("No transmission data available")
                return pd.DataFrame()
            
            logger.info(f"Loaded {len(transmission_data)} transmission records")
            
            return transmission_data
            
        except Exception as e:
            logger.error(f"Error getting transmission flows: {e}")
            return pd.DataFrame()
    
    def get_statistics(self) -> Dict[str, Any]:
        """Get query manager statistics"""
        stats = {
            'nem_dash_stats': self.query_manager.get_statistics(),
            'generation_stats': self.generation_manager.get_statistics()
        }
        return stats
    
    def clear_cache(self) -> None:
        """Clear all caches"""
        self.query_manager.clear_cache()
        self.generation_manager.clear_cache()
        logger.info("NEM dash caches cleared")


# Example usage and testing
if __name__ == "__main__":
    import time
    
    print("Testing NEMDashQueryManager...")
    
    manager = NEMDashQueryManager()
    
    # Test 1: Current spot prices
    print("\n1. Testing current spot prices...")
    t1 = time.time()
    prices = manager.get_current_spot_prices()
    print(f"✓ Loaded {len(prices)} current prices in {time.time() - t1:.2f}s")
    if not prices.empty:
        print(prices)
    
    # Test 2: Price history
    print("\n2. Testing price history (10 hours)...")
    t2 = time.time()
    price_history = manager.get_price_history(hours=10)
    print(f"✓ Loaded price history {price_history.shape} in {time.time() - t2:.2f}s")
    
    # Test 3: Generation overview
    print("\n3. Testing generation overview...")
    t3 = time.time()
    generation = manager.get_generation_overview(hours=24)
    print(f"✓ Loaded generation data {generation.shape} in {time.time() - t3:.2f}s")
    if not generation.empty:
        print(f"Fuel types: {list(generation.columns)}")
    
    # Test 4: Renewable data
    print("\n4. Testing renewable data...")
    t4 = time.time()
    renewable = manager.get_renewable_data()
    print(f"✓ Loaded renewable data in {time.time() - t4:.2f}s")
    print(f"Renewable: {renewable['renewable_pct']:.1f}% "
          f"({renewable['renewable_mw']:.0f}MW / {renewable['total_mw']:.0f}MW)")
    
    # Test 5: Transmission flows
    print("\n5. Testing transmission flows...")
    t5 = time.time()
    transmission = manager.get_transmission_flows(hours=24)
    print(f"✓ Loaded {len(transmission)} transmission records in {time.time() - t5:.2f}s")
    
    # Show statistics
    print("\n6. Cache statistics:")
    stats = manager.get_statistics()
    print(stats)