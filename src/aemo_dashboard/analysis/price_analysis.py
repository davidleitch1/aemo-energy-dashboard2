"""
Price Analysis Motor - Core calculation engine for average price analysis.

This module provides the data integration and calculation functions needed to 
analyze generation revenues across flexible aggregation hierarchies.
"""

import pandas as pd
import numpy as np
import logging
from pathlib import Path
from typing import Dict, List, Tuple, Optional, Union
import pickle
from datetime import datetime
from ..shared.config import config
from ..shared.logging_config import get_logger
from ..shared.performance_logging import PerformanceLogger, performance_monitor
from ..shared.hybrid_query_manager import HybridQueryManager
from ..shared.duckdb_views import view_manager

logger = get_logger(__name__)
perf_logger = PerformanceLogger(__name__)

class PriceAnalysisMotor:
    """Core calculation engine for price analysis"""
    
    def __init__(self):
        """Initialize the price analysis motor with hybrid query manager"""
        self.query_manager = HybridQueryManager(cache_size_mb=100, cache_ttl=300)
        self.duid_mapping = None
        self.integrated_data = None
        self.resolution = '30min'  # Default resolution
        self.data_available = False
        self.date_ranges = {}
        self._last_query_params = None
        
        # Ensure DuckDB views are created
        view_manager.create_all_views()
        
        logger.info("Price Analysis Motor initialized with hybrid query manager")
    
    def load_data(self, use_30min_data: bool = True) -> bool:
        """
        Check data availability and set resolution preference.
        Note: Actual data loading is deferred until integrate_data() is called.
        
        Args:
            use_30min_data: If True, use 30-minute data for better performance
        
        Returns:
            bool: True if data sources are available
        """
        try:
            with perf_logger.timer("metadata_check"):
                # Set resolution preference
                self.resolution = '30min' if use_30min_data else '5min'
                logger.info(f"Set resolution preference to {self.resolution}")
                
                # Get available date ranges from DuckDB
                self.date_ranges = self.query_manager.get_date_ranges()
                
                # Check if required data sources are available
                required_sources = ['generation', 'prices']
                missing_sources = [src for src in required_sources if src not in self.date_ranges]
                
                if missing_sources:
                    logger.error(f"Missing data sources: {missing_sources}")
                    self.data_available = False
                    return False
                
                # Load DUID mapping (small, keep in memory)
                with perf_logger.timer("duid_mapping_load", threshold=0.2):
                    with open(config.gen_info_file, 'rb') as f:
                        self.duid_mapping = pickle.load(f)
                
                logger.info(f"Data available - Generation: {self.date_ranges['generation']['start'].date()} to {self.date_ranges['generation']['end'].date()}, "
                           f"Prices: {self.date_ranges['prices']['start'].date()} to {self.date_ranges['prices']['end'].date()}")
                
                self.data_available = True
                return True
                
        except Exception as e:
            logger.error(f"Error checking data availability: {e}")
            self.data_available = False
            return False
    
    @performance_monitor(threshold=2.0)
    def integrate_data(self, start_date: Optional[str] = None, end_date: Optional[str] = None, force_reload: bool = False) -> bool:
        """
        Load and integrate generation, price, and DUID mapping data using hybrid query manager.
        
        Args:
            start_date: Filter start date (YYYY-MM-DD format), if None uses available data
            end_date: Filter end date (YYYY-MM-DD format), if None uses available data
            force_reload: Force reload even if data already cached
        
        Returns:
            bool: True if integration successful
        """
        try:
            # Check if data is available
            if not self.data_available:
                logger.error("Data not available. Call load_data() first.")
                return False
            
            # Convert string dates to datetime
            if start_date:
                start_dt = pd.to_datetime(start_date)
            else:
                start_dt = self.date_ranges['generation']['start']
            
            if end_date:
                # Add 1 day to end_date to include the full end day
                end_dt = pd.to_datetime(end_date) + pd.Timedelta(days=1)
            else:
                end_dt = self.date_ranges['generation']['end']
            
            # Check if we need to reload
            query_params = (start_dt, end_dt, self.resolution)
            if not force_reload and query_params == self._last_query_params and self.integrated_data is not None:
                logger.info("Using cached integrated data")
                return True
            
            with perf_logger.timer("data_integration", threshold=1.0):
                # Load integrated data using hybrid query manager
                logger.info(f"Loading integrated data for {start_dt.date()} to {end_dt.date()} at {self.resolution} resolution")
                
                self.integrated_data = self.query_manager.query_integrated_data(
                    start_date=start_dt,
                    end_date=end_dt,
                    resolution=self.resolution,
                    use_cache=not force_reload
                )
                
                if self.integrated_data.empty:
                    logger.warning("No data returned from query")
                    return False
                
                perf_logger.log_data_operation(
                    "Integrated data loaded",
                    len(self.integrated_data),
                    metadata={"shape": str(self.integrated_data.shape)}
                )
                
                # The DuckDB view already includes revenue calculation, but we need to ensure consistency
                # Check which revenue column we have
                if self.resolution == '5min' and 'revenue_5min' not in self.integrated_data.columns and 'revenue' in self.integrated_data.columns:
                    self.integrated_data['revenue_5min'] = self.integrated_data['revenue']
                elif self.resolution == '30min' and 'revenue_30min' not in self.integrated_data.columns and 'revenue' in self.integrated_data.columns:
                    self.integrated_data['revenue_30min'] = self.integrated_data['revenue']
                
                # Ensure RRP column exists for backward compatibility
                if 'RRP' not in self.integrated_data.columns and 'rrp' in self.integrated_data.columns:
                    self.integrated_data['RRP'] = self.integrated_data['rrp']
                
                # Store query parameters
                self._last_query_params = query_params
                
                # Report data period
                if logger.isEnabledFor(logging.INFO):
                    overlap_start = self.integrated_data['settlementdate'].min()
                    overlap_end = self.integrated_data['settlementdate'].max()
                    overlap_days = (overlap_end - overlap_start).days
                    logger.info(f"Data period: {overlap_start.date()} to {overlap_end.date()} ({overlap_days} days, {len(self.integrated_data):,} records)")
                
                return True
            
        except Exception as e:
            logger.error(f"Error integrating data: {e}")
            return False
    
    @performance_monitor(threshold=1.0)
    def calculate_aggregated_prices(self, hierarchy: List[str], region_filters: List[str] = None, fuel_filters: List[str] = None) -> pd.DataFrame:
        """
        Calculate aggregated average prices for a given hierarchy.
        
        Args:
            hierarchy: List of columns to group by, in order (e.g., ['Fuel', 'Region', 'duid'])
            region_filters: List of regions to include (filter)
            fuel_filters: List of fuels to include (filter)
            
        Returns:
            DataFrame with aggregated results
        """
        try:
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug(f"Calculating aggregated prices for hierarchy: {hierarchy}")
                logger.debug(f"Applying filters - Regions: {region_filters}, Fuels: {fuel_filters}")
            
            if self.integrated_data is None:
                raise ValueError("Data not integrated yet. Call integrate_data() first.")
            
            with perf_logger.timer("price_aggregation", threshold=0.5):
                # Filter out any missing values in hierarchy columns
                data = self.integrated_data.dropna(subset=hierarchy)
                
                # Apply filters before aggregation
                if region_filters and 'region' in data.columns:
                    data = data[data['region'].isin(region_filters)]
                    if logger.isEnabledFor(logging.DEBUG):
                        logger.debug(f"After region filter: {len(data)} records")
                
                if fuel_filters and 'fuel_type' in data.columns:
                    data = data[data['fuel_type'].isin(fuel_filters)]
                    if logger.isEnabledFor(logging.DEBUG):
                        logger.debug(f"After fuel filter: {len(data)} records")
                
                # Determine which revenue column to use based on resolution
                if self.resolution == '5min':
                    revenue_col = 'revenue_5min' if 'revenue_5min' in data.columns else 'revenue'
                else:  # 30min
                    revenue_col = 'revenue_30min' if 'revenue_30min' in data.columns else 'revenue'
                
                # Group by hierarchy and calculate aggregations
                grouped = data.groupby(hierarchy).agg({
                    'scadavalue': 'sum',        # Total generation (sum of MW readings across intervals)
                    revenue_col: 'sum',         # Total revenue ($)
                    'settlementdate': ['min', 'max', 'count']  # Date range and record count
                }).round(2)
                
                # Flatten column names
                grouped.columns = ['total_generation_sum', 'total_revenue_dollars', 'start_date', 'end_date', 'record_count']
                
                # Calculate actual MWh generation based on resolution
                # scadavalue sum = sum of MW readings across intervals
                if self.resolution == '5min':
                    # Convert to MWh: sum(MW) × (5min / 60min/hr) = sum(MW) × (1/12) = MWh
                    grouped['generation_mwh'] = grouped['total_generation_sum'] * (5.0 / 60.0)
                else:  # 30min
                    # Convert to MWh: sum(MW) × (30min / 60min/hr) = sum(MW) × 0.5 = MWh
                    grouped['generation_mwh'] = grouped['total_generation_sum'] * 0.5
                grouped['average_price_per_mwh'] = np.where(
                    grouped['generation_mwh'] > 0,
                    grouped['total_revenue_dollars'] / grouped['generation_mwh'],
                    0
                )
                
                # Add capacity factor information if we can
                capacity_col = None
                if 'Capacity(MW)' in data.columns:
                    capacity_col = 'Capacity(MW)'
                elif 'nameplate_capacity' in data.columns:
                    capacity_col = 'nameplate_capacity'
                
                if capacity_col:
                    # Sum the capacities of all units in the group
                    capacity_info = data.groupby(hierarchy)[capacity_col].sum()
                    grouped['capacity_mw'] = capacity_info
                    
                    # Calculate capacity utilization using correct formula
                    # Time span in hours = (end_date - start_date).total_seconds() / 3600
                    time_span_hours = (grouped['end_date'] - grouped['start_date']).dt.total_seconds() / 3600
                    grouped['capacity_utilization_pct'] = np.where(
                        (grouped['capacity_mw'] > 0) & (time_span_hours > 0),
                        (grouped['generation_mwh'] / (grouped['capacity_mw'] * time_span_hours)) * 100,
                        0
                    ).round(1)
                
                # Reset index to make hierarchy columns regular columns
                result = grouped.reset_index()
                
                # Sort by total revenue (highest first)
                result = result.sort_values('total_revenue_dollars', ascending=False)
                
                # Apply rounding to match the formatting in create_hierarchical_data
                # Note: These are raw values, not yet converted to GWh or millions
                # The conversion and proper rounding happens in the UI layer
                
                # Don't round the raw data here - let the UI handle formatting
                # This preserves precision for further calculations
            
            perf_logger.log_data_operation(
                "Aggregated prices",
                len(result),
                metadata={"hierarchy": str(hierarchy)}
            )
            
            return result
            
        except Exception as e:
            logger.error(f"Error calculating aggregated prices: {hierarchy}")
            logger.error(f"Exception details: {type(e).__name__}: {str(e)}")
            import traceback
            logger.error(f"Traceback: {traceback.format_exc()}")
            return pd.DataFrame()
    
    def get_available_date_range(self) -> Tuple[Optional[str], Optional[str]]:
        """
        Get the available date range from source data files, not just currently loaded data.
        
        Returns:
            Tuple of (start_date, end_date) as strings in YYYY-MM-DD format
        """
        try:
            # If we haven't loaded data yet, get fresh date ranges
            if not self.date_ranges:
                self.date_ranges = self.query_manager.get_date_ranges()
            
            # Use the date ranges we already have
            if 'prices' in self.date_ranges and 'generation' in self.date_ranges:
                # Find the overlapping date range between generation and prices
                gen_start = self.date_ranges['generation']['start']
                gen_end = self.date_ranges['generation']['end']
                price_start = self.date_ranges['prices']['start']
                price_end = self.date_ranges['prices']['end']
                
                # Use the intersection of the date ranges
                start_date = max(gen_start, price_start).strftime('%Y-%m-%d')
                end_date = min(gen_end, price_end).strftime('%Y-%m-%d')
                
                logger.info(f"Full available date range: {start_date} to {end_date}")
                return start_date, end_date
            else:
                # Fallback to loading a small sample
                from ..shared.resolution_manager import resolution_manager
                
                # Get the full price data range by loading a small sample
                # Use 30-minute data for efficiency when checking date ranges
                price_file = resolution_manager.get_file_path('price', '30min')
                logger.debug("Checking date range from price data")
                
                # Use query manager to get date ranges
                date_ranges = self.query_manager.get_date_ranges()
                
                if not date_ranges or 'prices' not in date_ranges:
                    logger.warning("No price data date ranges available")
                    return None, None
                
                price_ranges = date_ranges['prices']
                if not price_ranges:
                    logger.warning("No price data found in source")
                    return None, None
                    
                # Get date ranges from the price data
                start_date = price_ranges.get('start_date', '')
                end_date = price_ranges.get('end_date', '')
                
                if start_date and end_date:
                    # Convert to string format if they're datetime objects
                    if hasattr(start_date, 'strftime'):
                        start_date = start_date.strftime('%Y-%m-%d')
                    if hasattr(end_date, 'strftime'):
                        end_date = end_date.strftime('%Y-%m-%d')
                
                logger.info(f"Full available price date range: {start_date} to {end_date}")
                return start_date, end_date
            
        except Exception as e:
            logger.error(f"Error getting available date range from source files: {e}")
            # Fallback to integrated data if source file check fails
            if self.integrated_data is None or len(self.integrated_data) == 0:
                return None, None
            
            start_date = self.integrated_data['settlementdate'].min().strftime('%Y-%m-%d')
            end_date = self.integrated_data['settlementdate'].max().strftime('%Y-%m-%d')
            
            return start_date, end_date
    
    def get_available_hierarchies(self) -> Dict[str, List[str]]:
        """
        Get available grouping hierarchies based on available data columns.
        
        Returns:
            Dict mapping hierarchy names to column lists
        """
        if self.integrated_data is None:
            return {}
        
        available_columns = set(self.integrated_data.columns)
        
        hierarchies = {}
        
        # Map display names to actual column names
        column_mapping = {
            'Fuel': 'fuel_type' if 'fuel_type' in available_columns else 'Fuel',
            'Region': 'region' if 'region' in available_columns else 'Region',
            'Owner': 'owner' if 'owner' in available_columns else 'Owner'
        }
        
        # Check what columns are available using mapped names
        fuel_col = column_mapping['Fuel']
        region_col = column_mapping['Region']
        owner_col = column_mapping['Owner']
        
        if fuel_col in available_columns and region_col in available_columns:
            # Multi-level aggregations (without DUID for proper grouping)
            hierarchies['Fuel → Region'] = [fuel_col, region_col]
            hierarchies['Region → Fuel'] = [region_col, fuel_col]
            
        if owner_col in available_columns and fuel_col in available_columns:
            hierarchies['Owner → Fuel'] = [owner_col, fuel_col]
            hierarchies['Fuel → Owner'] = [fuel_col, owner_col]
        
        # Single-level aggregations
        if fuel_col in available_columns:
            hierarchies['Fuel Type'] = [fuel_col]
        if region_col in available_columns:
            hierarchies['Region'] = [region_col]
        if owner_col in available_columns:
            hierarchies['Owner'] = [owner_col]
        
        return hierarchies
    
    def calculate_duid_details(self, hierarchy: List[str]) -> pd.DataFrame:
        """
        Calculate detailed DUID-level data for expandable detail rows.
        
        Args:
            hierarchy: The hierarchy used for grouping (without DUID)
            
        Returns:
            DataFrame with DUID-level details that can be used for expandable rows
        """
        try:
            if self.integrated_data is None:
                raise ValueError("Data not integrated yet. Call integrate_data() first.")
            
            # Create hierarchy with DUID added for detail calculation
            detail_hierarchy = hierarchy + ['duid']
            
            # Filter out any missing values in hierarchy columns
            data = self.integrated_data.dropna(subset=detail_hierarchy)
            
            # Determine which revenue column to use based on resolution
            if self.resolution == '5min':
                revenue_col = 'revenue_5min' if 'revenue_5min' in data.columns else 'revenue'
            else:  # 30min
                revenue_col = 'revenue_30min' if 'revenue_30min' in data.columns else 'revenue'
            
            # Group by full hierarchy including DUID for details
            grouped = data.groupby(detail_hierarchy).agg({
                'scadavalue': 'sum',        # Total generation (sum of MW readings across intervals)
                revenue_col: 'sum',         # Total revenue ($)
                'settlementdate': ['min', 'max', 'count']  # Date range and record count
            }).round(2)
            
            # Flatten column names
            grouped.columns = ['total_generation_sum', 'total_revenue_dollars', 'start_date', 'end_date', 'record_count']
            
            # Calculate actual MWh generation based on resolution
            # scadavalue sum = sum of MW readings across intervals
            if self.resolution == '5min':
                # Convert to MWh: sum(MW) × (5min / 60min/hr) = sum(MW) × (1/12) = MWh
                grouped['generation_mwh'] = grouped['total_generation_sum'] * (5.0 / 60.0)
            else:  # 30min
                # Convert to MWh: sum(MW) × (30min / 60min/hr) = sum(MW) × 0.5 = MWh
                grouped['generation_mwh'] = grouped['total_generation_sum'] * 0.5
            grouped['average_price_per_mwh'] = np.where(
                grouped['generation_mwh'] > 0,
                grouped['total_revenue_dollars'] / grouped['generation_mwh'],
                0
            )
            
            # Add capacity factor information if we can
            # Check for capacity column - could be 'Capacity(MW)' or 'nameplate_capacity'
            capacity_col = None
            if 'Capacity(MW)' in data.columns:
                capacity_col = 'Capacity(MW)'
            elif 'nameplate_capacity' in data.columns:
                capacity_col = 'nameplate_capacity'
            
            if capacity_col:
                capacity_info = data.groupby(detail_hierarchy)[capacity_col].first()
                grouped['capacity_mw'] = capacity_info
                
                # Calculate capacity utilization using correct formula
                time_span_hours = (grouped['end_date'] - grouped['start_date']).dt.total_seconds() / 3600
                grouped['capacity_utilization_pct'] = np.where(
                    (grouped['capacity_mw'] > 0) & (time_span_hours > 0),
                    (grouped['generation_mwh'] / (grouped['capacity_mw'] * time_span_hours)) * 100,
                    0
                ).round(1)
            
            # Reset index to make hierarchy columns regular columns
            result = grouped.reset_index()
            
            # Sort by hierarchy and then by total revenue
            sort_columns = hierarchy + ['total_revenue_dollars']
            result = result.sort_values(sort_columns, ascending=[True] * len(hierarchy) + [False])
            
            logger.info(f"Calculated DUID details for {len(result)} individual DUIDs")
            
            return result
            
        except Exception as e:
            logger.error(f"Error calculating DUID details: {e}")
            import traceback
            logger.error(f"Traceback: {traceback.format_exc()}")
            return pd.DataFrame()
    
    def prepare_tabulator_data(self, df: pd.DataFrame, hierarchy: List[str]) -> pd.DataFrame:
        """
        Prepare data for tabulator table with hierarchical structure.
        
        Args:
            df: Aggregated dataframe
            hierarchy: The hierarchy used for grouping
            
        Returns:
            DataFrame ready for Panel's groupby functionality
        """
        try:
            # Make a copy to avoid modifying the original
            prepared_df = df.copy()
            
            # Ensure hierarchy columns are regular columns (not index)
            if any(col in prepared_df.index.names for col in hierarchy if prepared_df.index.names):
                prepared_df = prepared_df.reset_index()
            
            # Add formatted columns for better display
            if 'total_revenue_dollars' in prepared_df.columns:
                prepared_df['total_revenue_formatted'] = prepared_df['total_revenue_dollars'].apply(
                    lambda x: f"${x:,.0f}" if pd.notna(x) else ""
                )
            if 'average_price_per_mwh' in prepared_df.columns:
                prepared_df['average_price_formatted'] = prepared_df['average_price_per_mwh'].apply(
                    lambda x: f"${x:,.2f}/MWh" if pd.notna(x) else ""
                )
            if 'generation_mwh' in prepared_df.columns:
                prepared_df['generation_formatted'] = prepared_df['generation_mwh'].apply(
                    lambda x: f"{x:,.1f} MWh" if pd.notna(x) else ""
                )
            
            # Format date columns
            for date_col in ['start_date', 'end_date']:
                if date_col in prepared_df.columns:
                    prepared_df[f"{date_col}_formatted"] = prepared_df[date_col].apply(
                        lambda x: x.strftime("%Y-%m-%d") if pd.notna(x) else ""
                    )
            
            logger.info(f"Prepared DataFrame with {len(prepared_df)} records for tabulator")
            return prepared_df
            
        except Exception as e:
            logger.error(f"Error preparing tabulator data: {e}")
            return df.copy()  # Return copy of original on error
    
    @performance_monitor(threshold=2.0)
    def create_combined_hierarchical_data(self, hierarchy: List[str], region_filters: List[str] = None, fuel_filters: List[str] = None) -> pd.DataFrame:
        """
        Create a combined hierarchical DataFrame that includes both aggregated totals and individual DUID details
        in a flat structure suitable for visual hierarchy without relying on Panel's groupby feature.
        
        Args:
            hierarchy: List of columns to group by (e.g., ['fuel_type', 'region'])
            region_filters: List of regions to include (filter)
            fuel_filters: List of fuels to include (filter)
            
        Returns:
            DataFrame with both group totals and individual DUIDs combined with a 'level' indicator
        """
        try:
            if self.integrated_data is None:
                raise ValueError("Data not integrated yet. Call integrate_data() first.")
            
            logger.info(f"Creating combined hierarchical data for hierarchy: {hierarchy}")
            logger.info(f"Applying filters - Regions: {region_filters}, Fuels: {fuel_filters}")
            
            with perf_logger.timer("combined_hierarchical_data_creation", threshold=1.0):
                # Step 1: Calculate aggregated totals (group level)
                aggregated_data = self.calculate_aggregated_prices(hierarchy, region_filters, fuel_filters)
                
                if aggregated_data.empty:
                    logger.warning("No aggregated data available")
                    return pd.DataFrame()
                
                # Step 2: Calculate DUID-level details (with same filters applied)
                # Apply filters to the integrated data before calculating DUID details
                filtered_data = self.integrated_data.copy()
                
                if region_filters and 'region' in filtered_data.columns:
                    filtered_data = filtered_data[filtered_data['region'].isin(region_filters)]
                
                if fuel_filters and 'fuel_type' in filtered_data.columns:
                    filtered_data = filtered_data[filtered_data['fuel_type'].isin(fuel_filters)]
                
                # Temporarily store the original data and use filtered data
                original_data = self.integrated_data
                self.integrated_data = filtered_data
                
                # Calculate DUID details with the filtered data
                duid_data = self.calculate_duid_details(hierarchy)
                
                # Restore original data
                self.integrated_data = original_data
                
                if duid_data.empty:
                    logger.warning("No DUID data available")
                    # Return just aggregated data with level indicator
                    aggregated_data['level'] = 0
                    aggregated_data['duid'] = aggregated_data.apply(
                        lambda row: f"[{int(row.get('record_count', 0) / len(hierarchy))} DUIDs]" if 'record_count' in row else "[Group Total]",
                        axis=1
                    )
                    return aggregated_data
                
                # Step 3: Add metadata columns to DUID data
                # Get station name and owner info from original data
                # The columns are already named 'station_name' and 'owner' from the query
                agg_dict = {}
                if 'station_name' in self.integrated_data.columns:
                    agg_dict['station_name'] = 'first'
                if 'owner' in self.integrated_data.columns:
                    agg_dict['owner'] = 'first'
                
                # Only proceed if we have columns to aggregate
                if agg_dict:
                    duid_metadata = self.integrated_data.groupby('duid').agg(agg_dict).reset_index()
                else:
                    # Create empty dataframe with just duid column
                    unique_duids = duid_data['duid'].unique()
                    duid_metadata = pd.DataFrame({'duid': unique_duids})
                
                # Merge metadata into DUID data
                duid_data = duid_data.merge(
                    duid_metadata,
                    on='duid',
                    how='left'
                )
                
                # The columns are already named correctly from the merge
                # No need to rename as they come from the query as 'station_name' and 'owner'
                
                # Step 4: Prepare aggregated data for combination
                # Add level indicator and format DUID column for group totals
                aggregated_data['level'] = 0
                
                # Count unique DUIDs per group from the DUID data
                duid_counts = duid_data.groupby(hierarchy)['duid'].nunique().reset_index()
                duid_counts.columns = hierarchy + ['duid_count']
                
                # Merge DUID counts into aggregated data
                aggregated_data = aggregated_data.merge(duid_counts, on=hierarchy, how='left')
                aggregated_data['duid'] = aggregated_data['duid_count'].apply(
                    lambda x: f"[{int(x)} DUIDs]" if pd.notna(x) and x > 0 else "[Group Total]"
                )
                aggregated_data = aggregated_data.drop('duid_count', axis=1)
                
                # Add empty station_name and owner columns to aggregated data if they exist in DUID data
                if 'station_name' in duid_data.columns:
                    aggregated_data['station_name'] = ''
                if 'owner' in duid_data.columns and 'owner' not in aggregated_data.columns:
                    aggregated_data['owner'] = ''
                
                # Step 5: Prepare DUID data for combination
                duid_data['level'] = 1
                
                # Step 6: Ensure both DataFrames have the same columns
                # Get all columns from both dataframes
                all_columns = set(aggregated_data.columns) | set(duid_data.columns)
                
                # Add missing columns to each dataframe
                for col in all_columns:
                    if col not in aggregated_data.columns:
                        aggregated_data[col] = ''
                    if col not in duid_data.columns:
                        duid_data[col] = ''
                
                # Ensure columns are in the same order
                column_order = hierarchy + ['level', 'duid']
                if 'station_name' in all_columns:
                    column_order.append('station_name')
                if 'owner' in all_columns and 'owner' not in hierarchy:
                    column_order.append('owner')
                
                # Add data columns in a consistent order
                data_columns = ['generation_mwh', 'total_revenue_dollars', 'average_price_per_mwh', 
                               'capacity_mw', 'capacity_utilization_pct', 'start_date', 'end_date', 'record_count']
                for col in data_columns:
                    if col in all_columns:
                        column_order.append(col)
                
                # Add any remaining columns
                for col in sorted(all_columns):
                    if col not in column_order:
                        column_order.append(col)
                
                # Reorder columns
                aggregated_data = aggregated_data[column_order]
                duid_data = duid_data[column_order]
                
                # Step 7: Combine the dataframes
                combined_data = pd.concat([aggregated_data, duid_data], ignore_index=True)
                
                # Step 8: Sort by hierarchy columns and level
                sort_columns = hierarchy + ['level']
                if 'total_revenue_dollars' in combined_data.columns:
                    sort_columns.append('total_revenue_dollars')
                
                # Create sort order: True for hierarchy columns (ascending), False for level and revenue (descending)
                ascending_order = [True] * len(hierarchy) + [True] + [False] * (len(sort_columns) - len(hierarchy) - 1)
                
                combined_data = combined_data.sort_values(sort_columns, ascending=ascending_order)
                
                # Step 9: Apply formatting for better display
                if 'generation_mwh' in combined_data.columns:
                    # Convert MWh to GWh
                    combined_data['generation_gwh'] = combined_data['generation_mwh'] / 1000
                    # Round based on value
                    combined_data['generation_gwh'] = combined_data['generation_gwh'].apply(
                        lambda x: round(x, 0) if x > 10 else round(x, 1) if pd.notna(x) else 0
                    )
                    combined_data = combined_data.drop('generation_mwh', axis=1)
                
                if 'total_revenue_dollars' in combined_data.columns:
                    # Convert to millions
                    combined_data['revenue_millions'] = combined_data['total_revenue_dollars'] / 1_000_000
                    # Round based on value
                    combined_data['revenue_millions'] = combined_data['revenue_millions'].apply(
                        lambda x: round(x, 0) if x > 10 else round(x, 1) if pd.notna(x) else 0
                    )
                    combined_data = combined_data.drop('total_revenue_dollars', axis=1)
                
                if 'average_price_per_mwh' in combined_data.columns:
                    # Round price
                    combined_data['avg_price'] = combined_data['average_price_per_mwh'].apply(
                        lambda x: round(x, 0) if x > 10 else round(x, 1) if pd.notna(x) else 0
                    )
                    combined_data = combined_data.drop('average_price_per_mwh', axis=1)
                
                if 'capacity_utilization_pct' in combined_data.columns:
                    combined_data['capacity_utilization'] = combined_data['capacity_utilization_pct'].apply(
                        lambda x: round(x, 1) if pd.notna(x) else 0
                    )
                    combined_data = combined_data.drop('capacity_utilization_pct', axis=1)
                
                if 'capacity_mw' in combined_data.columns:
                    combined_data['capacity_mw'] = combined_data['capacity_mw'].apply(
                        lambda x: round(x, 1) if pd.notna(x) else 0
                    )
                
                # Reset index for clean output
                combined_data = combined_data.reset_index(drop=True)
                
                logger.info(f"Created combined hierarchical data with {len(combined_data)} total rows "
                           f"({len(aggregated_data)} groups, {len(duid_data)} DUIDs)")
                
                perf_logger.log_data_operation(
                    "Created combined hierarchical data",
                    len(combined_data),
                    metadata={
                        "groups": len(aggregated_data),
                        "duids": len(duid_data),
                        "columns": len(combined_data.columns)
                    }
                )
                
                return combined_data
                
        except Exception as e:
            logger.error(f"Error creating combined hierarchical data: {e}")
            import traceback
            logger.error(f"Traceback: {traceback.format_exc()}")
            return pd.DataFrame()
    
    @performance_monitor(threshold=2.0)
    def create_hierarchical_data(self, hierarchy: List[str], selected_columns: List[str], region_filters: List[str] = None, fuel_filters: List[str] = None) -> pd.DataFrame:
        """
        Create a hierarchical DataFrame that includes both aggregated totals and individual DUIDs
        for proper groupby functionality in Panel's Tabulator.
        
        Args:
            hierarchy: List of columns to group by (e.g., ['Fuel', 'Region'])
            selected_columns: List of data columns to include
            region_filters: List of regions to include (filter)
            fuel_filters: List of fuels to include (filter)
            
        Returns:
            DataFrame with both group totals and individual DUIDs for hierarchical display
        """
        try:
            if self.integrated_data is None:
                raise ValueError("Data not integrated yet. Call integrate_data() first.")
            
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug(f"Creating hierarchical data for hierarchy: {hierarchy}")
                logger.debug(f"Applying filters - Regions: {region_filters}, Fuels: {fuel_filters}")
            
            with perf_logger.timer("hierarchical_data_creation", threshold=1.0):
                # Apply filters to the integrated data before processing
                filtered_data = self.integrated_data.copy()
                
                # Apply region filter
                if region_filters:
                    filtered_data = filtered_data[filtered_data['region'].isin(region_filters)]
                    if logger.isEnabledFor(logging.DEBUG):
                        logger.debug(f"After region filter: {len(filtered_data)} records")
                
                # Apply fuel filter  
                if fuel_filters:
                    filtered_data = filtered_data[filtered_data['fuel_type'].isin(fuel_filters)]
                    if logger.isEnabledFor(logging.DEBUG):
                        logger.debug(f"After fuel filter: {len(filtered_data)} records")
            
            if len(filtered_data) == 0:
                logger.warning("No data left after applying filters")
                return pd.DataFrame()
            
            # Temporarily store the original data and use filtered data
            original_data = self.integrated_data
            self.integrated_data = filtered_data
            
            # Calculate DUID-level details (these will be the leaf nodes)
            duid_hierarchy = hierarchy + ['duid']
            duid_data = self.calculate_duid_details(hierarchy)
            
            # Restore original data
            self.integrated_data = original_data
            
            if duid_data.empty:
                if logger.isEnabledFor(logging.WARNING):
                    logger.warning("No DUID data available for hierarchical display")
                return pd.DataFrame()
            
            # Ensure we only include the columns we need
            available_columns = set(duid_data.columns)
            hierarchy_cols = [col for col in hierarchy if col in available_columns]
            data_cols = [col for col in selected_columns if col in available_columns]
            
            if not hierarchy_cols:
                logger.warning("No valid hierarchy columns found")
                return pd.DataFrame()
                
            # Build the columns we need for display
            display_columns = hierarchy_cols + ['duid'] + data_cols
            
            # Log available vs requested columns for debugging capacity issue
            logger.info(f"Available columns in duid_data: {list(available_columns)}")
            logger.info(f"Requested data columns: {data_cols}")
            logger.info(f"Display columns to extract: {display_columns}")
            
            # Filter the DUID data to only include display columns
            filtered_duid_data = duid_data[display_columns].copy()
            
            # Add Station Name and Owner columns from the original integrated data
            if not filtered_duid_data.empty:
                # Get unique DUIDs from the filtered data
                duids = filtered_duid_data['duid'].unique()
                
                # Extract Station Name and Owner info for these DUIDs
                # The columns are already named 'station_name' and 'owner' from the query
                agg_dict = {}
                if 'station_name' in original_data.columns:
                    agg_dict['station_name'] = 'first'
                if 'owner' in original_data.columns:
                    agg_dict['owner'] = 'first'
                
                # Only proceed if we have columns to aggregate
                if agg_dict:
                    duid_info = original_data.groupby('duid').agg(agg_dict).reset_index()
                else:
                    # Create empty dataframe with just duid column
                    duid_info = pd.DataFrame({'duid': duids})
                
                # Merge this info into the filtered data
                filtered_duid_data = filtered_duid_data.merge(
                    duid_info[duid_info['duid'].isin(duids)],
                    on='duid',
                    how='left'
                )
                
                # The columns are already named correctly from the merge
                # No need to rename as they come from the query as 'station_name' and 'owner'
            
            # Apply formatting transformations
            if 'generation_mwh' in filtered_duid_data.columns:
                # Convert MWh to GWh
                filtered_duid_data['generation_gwh'] = filtered_duid_data['generation_mwh'] / 1000
                # Round to 0 decimal places if > 10, otherwise 1 decimal place
                filtered_duid_data['generation_gwh'] = filtered_duid_data['generation_gwh'].apply(
                    lambda x: round(x, 0) if x > 10 else round(x, 1)
                )
                # Remove original MWh column
                filtered_duid_data = filtered_duid_data.drop('generation_mwh', axis=1)
            
            if 'total_revenue_dollars' in filtered_duid_data.columns:
                # Convert dollars to millions
                filtered_duid_data['revenue_millions'] = filtered_duid_data['total_revenue_dollars'] / 1_000_000
                # Round to 0 decimal places if > 10, otherwise 1 decimal place
                filtered_duid_data['revenue_millions'] = filtered_duid_data['revenue_millions'].apply(
                    lambda x: round(x, 0) if x > 10 else round(x, 1)
                )
                # Remove original dollars column
                filtered_duid_data = filtered_duid_data.drop('total_revenue_dollars', axis=1)
            
            if 'average_price_per_mwh' in filtered_duid_data.columns:
                # Round price to 0 decimal places if > 10, otherwise 1 decimal place
                filtered_duid_data['avg_price'] = filtered_duid_data['average_price_per_mwh'].apply(
                    lambda x: round(x, 0) if x > 10 else round(x, 1)
                )
                # Remove original column
                filtered_duid_data = filtered_duid_data.drop('average_price_per_mwh', axis=1)
            
            if 'capacity_utilization_pct' in filtered_duid_data.columns:
                # Keep capacity utilization as-is, just ensure proper rounding
                filtered_duid_data['capacity_utilization'] = filtered_duid_data['capacity_utilization_pct'].apply(
                    lambda x: round(x, 1) if pd.notna(x) else 0.0
                )
                # Remove original column
                filtered_duid_data = filtered_duid_data.drop('capacity_utilization_pct', axis=1)
            
            # Keep capacity_mw column as-is (already in MW)
            if 'capacity_mw' in filtered_duid_data.columns:
                # Round to 1 decimal place
                filtered_duid_data['capacity_mw'] = filtered_duid_data['capacity_mw'].apply(
                    lambda x: round(x, 1) if pd.notna(x) else 0.0
                )
            
            perf_logger.log_data_operation(
                "Created hierarchical data",
                len(filtered_duid_data),
                metadata={"columns": len(filtered_duid_data.columns)}
            )
            
            return filtered_duid_data
            
        except Exception as e:
            logger.error(f"Error creating hierarchical data: {e}")
            return pd.DataFrame()

# Example usage and testing
if __name__ == "__main__":
    motor = PriceAnalysisMotor()
    
    # Test data loading
    if motor.load_data():
        print("✓ Data loaded successfully")
        
        # Test integration (standardization now handled by DuckDB views)
        if motor.integrate_data():
            print("✓ Data integrated successfully")
            
            # Test hierarchies
            hierarchies = motor.get_available_hierarchies()
            print(f"✓ Available hierarchies: {list(hierarchies.keys())}")
            
            # Test calculation
            if hierarchies:
                first_hierarchy = list(hierarchies.values())[0]
                result = motor.calculate_aggregated_prices(first_hierarchy)
                print(f"✓ Calculated aggregations: {len(result)} groups")
                print(result.head())