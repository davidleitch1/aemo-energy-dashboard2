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
from ..shared.config import config
from ..shared.logging_config import get_logger
from ..shared.performance_logging import PerformanceLogger, performance_monitor

logger = get_logger(__name__)
perf_logger = PerformanceLogger(__name__)

class PriceAnalysisMotor:
    """Core calculation engine for price analysis"""
    
    def __init__(self):
        """Initialize the price analysis motor"""
        self.gen_data = None
        self.price_data = None 
        self.duid_mapping = None
        self.integrated_data = None
        logger.info("Price Analysis Motor initialized")
    
    def load_data(self, use_30min_data: bool = True) -> bool:
        """
        Load all required data files.
        
        Args:
            use_30min_data: If True, use 30-minute data files for full historical range
        
        Returns:
            bool: True if all data loaded successfully
        """
        try:
            with perf_logger.timer("total_data_loading"):
                # Import adapters
                from ..shared.adapter_selector import load_generation_data, load_price_data
                
                if use_30min_data:
                    # Load 30-minute generation data using adapter
                    with perf_logger.timer("generation_30min_load", threshold=0.5):
                        self.gen_data = load_generation_data(resolution='30min')
                    
                    perf_logger.log_data_operation(
                        "Loaded generation data",
                        len(self.gen_data),
                        metadata={"resolution": "30min"}
                    )
                    
                    # Load 30-minute price data using adapter
                    with perf_logger.timer("price_30min_load", threshold=0.5):
                        self.price_data = load_price_data(resolution='30min')
                    
                    perf_logger.log_data_operation(
                        "Loaded price data",
                        len(self.price_data),
                        metadata={"resolution": "30min"}
                    )
                else:
                    # Load 5-minute generation data using adapter
                    with perf_logger.timer("generation_5min_load", threshold=0.5):
                        self.gen_data = load_generation_data(resolution='5min')
                    
                    perf_logger.log_data_operation(
                        "Loaded generation data",
                        len(self.gen_data),
                        metadata={"resolution": "5min"}
                    )
                    
                    # Load 5-minute price data using adapter
                    with perf_logger.timer("price_5min_load", threshold=0.5):
                        self.price_data = load_price_data(resolution='5min')
                    
                    perf_logger.log_data_operation(
                        "Loaded price data",
                        len(self.price_data),
                        metadata={"resolution": "5min"}
                    )
                
                # Load DUID mapping
                with perf_logger.timer("duid_mapping_load", threshold=0.2):
                    with open(config.gen_info_file, 'rb') as f:
                        self.duid_mapping = pickle.load(f)
                
                if logger.isEnabledFor(logging.DEBUG):
                    logger.debug(f"Loaded {len(self.duid_mapping)} DUID mappings")
                
                # Only inspect data in DEBUG mode
                if logger.isEnabledFor(logging.DEBUG):
                    self._inspect_data()
                
                return True
            
        except Exception as e:
            logger.error(f"Error loading data: {e}")
            return False
    
    def _inspect_data(self):
        """Inspect loaded data structure and report key information"""
        logger.info("=== Data Inspection ===")
        
        # Generation data
        logger.info(f"Generation columns: {list(self.gen_data.columns)}")
        logger.info(f"Generation date range: {self.gen_data['settlementdate'].min()} to {self.gen_data['settlementdate'].max()}")
        logger.info(f"Unique DUIDs in generation: {self.gen_data['duid'].nunique()}")
        
        # Price data
        logger.info(f"Price data shape: {self.price_data.shape}")
        logger.info(f"Price columns: {list(self.price_data.columns)}")
        if isinstance(self.price_data.index, pd.DatetimeIndex):
            logger.info(f"Price date range: {self.price_data.index.min()} to {self.price_data.index.max()}")
        else:
            logger.info(f"Price index type: {type(self.price_data.index)}")
        
        # DUID mapping
        if isinstance(self.duid_mapping, pd.DataFrame):
            logger.info(f"DUID mapping columns: {list(self.duid_mapping.columns)}")
        else:
            logger.info(f"DUID mapping type: {type(self.duid_mapping)}")
    
    def standardize_columns(self) -> bool:
        """
        Standardize column names across datasets to ensure consistent joining.
        
        Returns:
            bool: True if standardization successful
        """
        try:
            logger.info("Standardizing column names...")
            
            # Standardize generation data - lowercase
            gen_columns_before = list(self.gen_data.columns)
            self.gen_data.columns = self.gen_data.columns.str.lower()
            logger.info(f"Generation columns: {gen_columns_before} -> {list(self.gen_data.columns)}")
            
            # Price data standardization - handle both formats
            if isinstance(self.price_data.index, pd.DatetimeIndex):
                # Case 1: Price data has datetime index (from price adapter)
                self.price_data = self.price_data.reset_index()
                logger.info(f"Price data: Reset datetime index to column")
            elif 'settlementdate' in self.price_data.columns:
                # Case 2: Price data has 'settlementdate' column (from direct 30-min file)
                self.price_data = self.price_data.rename(columns={
                    'settlementdate': 'SETTLEMENTDATE',
                    'regionid': 'REGIONID',
                    'rrp': 'RRP'
                })
                logger.info(f"Price data: Renamed columns to uppercase format")
            
            logger.info(f"Price columns: {list(self.price_data.columns)}")
            
            # Ensure generation data has proper datetime
            if self.gen_data['settlementdate'].dtype == 'object':
                self.gen_data['settlementdate'] = pd.to_datetime(self.gen_data['settlementdate'])
            
            # Ensure price data has proper datetime
            if 'SETTLEMENTDATE' in self.price_data.columns:
                if self.price_data['SETTLEMENTDATE'].dtype == 'object':
                    self.price_data['SETTLEMENTDATE'] = pd.to_datetime(self.price_data['SETTLEMENTDATE'])
            
            logger.info("Column standardization completed")
            return True
            
        except Exception as e:
            logger.error(f"Error standardizing columns: {e}")
            return False
    
    @performance_monitor(threshold=2.0)
    def integrate_data(self, start_date: Optional[str] = None, end_date: Optional[str] = None) -> bool:
        """
        Integrate generation, price, and DUID mapping data.
        
        Args:
            start_date: Filter start date (YYYY-MM-DD format), if None uses all data
            end_date: Filter end date (YYYY-MM-DD format), if None uses all data
        
        Returns:
            bool: True if integration successful
        """
        try:
            with perf_logger.timer("data_integration", threshold=1.0):
                # First, join generation with DUID mapping
                with perf_logger.timer("duid_mapping_merge", threshold=0.5):
                    gen_with_mapping = self.gen_data.merge(
                        self.duid_mapping,
                        left_on='duid',
                        right_on='DUID',
                        how='left'
                    )
                
                missing_duids = gen_with_mapping[gen_with_mapping['Region'].isna()]['duid'].unique()
                if len(missing_duids) > 0 and logger.isEnabledFor(logging.WARNING):
                    logger.warning(f"Found {len(missing_duids)} DUIDs without mapping")
                
                if logger.isEnabledFor(logging.DEBUG):
                    logger.debug(f"Generation+DUID data shape: {gen_with_mapping.shape}")
                
                # Join with price data
                with perf_logger.timer("price_data_merge", threshold=0.5):
                    self.integrated_data = gen_with_mapping.merge(
                        self.price_data,
                        left_on=['settlementdate', 'Region'],
                        right_on=['SETTLEMENTDATE', 'REGIONID'],
                        how='inner'  # Only keep records where we have both generation and price
                    )
                
                perf_logger.log_data_operation(
                    "Integrated data",
                    len(self.integrated_data),
                    metadata={"shape": str(self.integrated_data.shape)}
                )
                
                # Apply date filtering if specified
                if start_date or end_date:
                    original_len = len(self.integrated_data)
                    
                    if start_date:
                        start_dt = pd.to_datetime(start_date)
                        self.integrated_data = self.integrated_data[self.integrated_data['settlementdate'] >= start_dt]
                    
                    if end_date:
                        # Add 1 day to end_date to include the full end day
                        end_dt = pd.to_datetime(end_date) + pd.Timedelta(days=1)
                        self.integrated_data = self.integrated_data[self.integrated_data['settlementdate'] < end_dt]
                    
                    if logger.isEnabledFor(logging.DEBUG):
                        logger.debug(f"Date filter applied: {original_len:,} -> {len(self.integrated_data):,} records")
                
                # Calculate 5-minute revenue
                with perf_logger.timer("revenue_calculation", threshold=0.5):
                    self.integrated_data['revenue_5min'] = (
                        self.integrated_data['scadavalue'] * 
                        self.integrated_data['RRP'] * 
                        (5.0 / 60.0)  # Convert MW×5min to MWh, then × $/MWh = $
                    )
                
                # Report data overlap period only in debug mode
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
    def calculate_aggregated_prices(self, hierarchy: List[str]) -> pd.DataFrame:
        """
        Calculate aggregated average prices for a given hierarchy.
        
        Args:
            hierarchy: List of columns to group by, in order (e.g., ['Fuel', 'Region', 'duid'])
            
        Returns:
            DataFrame with aggregated results
        """
        try:
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug(f"Calculating aggregated prices for hierarchy: {hierarchy}")
            
            if self.integrated_data is None:
                raise ValueError("Data not integrated yet. Call integrate_data() first.")
            
            with perf_logger.timer("price_aggregation", threshold=0.5):
                # Filter out any missing values in hierarchy columns
                data = self.integrated_data.dropna(subset=hierarchy)
                
                # Group by hierarchy and calculate aggregations
                grouped = data.groupby(hierarchy).agg({
                    'scadavalue': 'sum',        # Total generation (sum of MW readings across 5min intervals)
                    'revenue_5min': 'sum',      # Total revenue ($)
                    'settlementdate': ['min', 'max', 'count']  # Date range and record count
                }).round(2)
                
                # Flatten column names
                grouped.columns = ['total_generation_sum', 'total_revenue_dollars', 'start_date', 'end_date', 'record_count']
                
                # Calculate actual MWh generation
                # scadavalue sum = sum of MW readings across intervals
                # Convert to MWh: sum(MW) × (5min / 60min/hr) = sum(MW) × (1/12) = MWh
                grouped['generation_mwh'] = grouped['total_generation_sum'] * (5.0 / 60.0)
                grouped['average_price_per_mwh'] = np.where(
                    grouped['generation_mwh'] > 0,
                    grouped['total_revenue_dollars'] / grouped['generation_mwh'],
                    0
                )
                
                # Add capacity factor information if we can
                if 'Capacity(MW)' in data.columns:
                    capacity_info = data.groupby(hierarchy)['Capacity(MW)'].first()
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
            
            perf_logger.log_data_operation(
                "Aggregated prices",
                len(result),
                metadata={"hierarchy": str(hierarchy)}
            )
            
            return result
            
        except Exception as e:
            logger.error(f"Error calculating aggregated prices: {e}")
            return pd.DataFrame()
    
    def get_available_date_range(self) -> Tuple[Optional[str], Optional[str]]:
        """
        Get the available date range from source data files, not just currently loaded data.
        
        Returns:
            Tuple of (start_date, end_date) as strings in YYYY-MM-DD format
        """
        try:
            # Use DuckDB to get date ranges efficiently
            from ..shared.adapter_selector import USE_DUCKDB
            
            if USE_DUCKDB:
                # Get date ranges from DuckDB service
                from data_service.shared_data_duckdb import duckdb_data_service
                date_ranges = duckdb_data_service.get_date_ranges()
                
                if 'prices' in date_ranges and 'prices30' in date_ranges:
                    # Use 30-minute data range as it has the full historical data
                    start_date = date_ranges['prices30']['start'].strftime('%Y-%m-%d')
                    end_date = date_ranges['prices30']['end'].strftime('%Y-%m-%d')
                    logger.info(f"Full available price date range: {start_date} to {end_date}")
                    return start_date, end_date
            else:
                # Fallback to loading a small sample
                from ..shared.resolution_manager import resolution_manager
                
                # Get the full price data range by loading a small sample
                # Use 30-minute data for efficiency when checking date ranges
                price_file = resolution_manager.get_file_path('price', '30min')
                logger.debug(f"Checking date range from price file: {price_file}")
                
                # Read just the date column to check range efficiently
                import pandas as pd
                df = pd.read_parquet(price_file, columns=['settlementdate'])
                
                if len(df) == 0:
                    logger.warning("No price data found in source file")
                    return None, None
                    
                start_date = df['settlementdate'].min().strftime('%Y-%m-%d')
                end_date = df['settlementdate'].max().strftime('%Y-%m-%d')
                
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
        
        # Check what columns are available
        if 'Fuel' in available_columns and 'Region' in available_columns:
            # Multi-level aggregations (without DUID for proper grouping)
            hierarchies['Fuel → Region'] = ['Fuel', 'Region']
            hierarchies['Region → Fuel'] = ['Region', 'Fuel']
            
        if 'Owner' in available_columns:
            hierarchies['Owner → Fuel'] = ['Owner', 'Fuel']
            hierarchies['Fuel → Owner'] = ['Fuel', 'Owner']
        
        # Single-level aggregations
        for col in ['Fuel', 'Region', 'Owner']:
            if col in available_columns:
                hierarchies[col] = [col]
        
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
            
            # Group by full hierarchy including DUID for details
            grouped = data.groupby(detail_hierarchy).agg({
                'scadavalue': 'sum',        # Total generation (sum of MW readings across 5min intervals)
                'revenue_5min': 'sum',      # Total revenue ($)
                'settlementdate': ['min', 'max', 'count']  # Date range and record count
            }).round(2)
            
            # Flatten column names
            grouped.columns = ['total_generation_sum', 'total_revenue_dollars', 'start_date', 'end_date', 'record_count']
            
            # Calculate actual MWh generation
            # scadavalue sum = sum of MW readings across intervals
            # Convert to MWh: sum(MW) × (5min / 60min/hr) = sum(MW) × (1/12) = MWh
            grouped['generation_mwh'] = grouped['total_generation_sum'] * (5.0 / 60.0)
            grouped['average_price_per_mwh'] = np.where(
                grouped['generation_mwh'] > 0,
                grouped['total_revenue_dollars'] / grouped['generation_mwh'],
                0
            )
            
            # Add capacity factor information if we can
            if 'Capacity(MW)' in data.columns:
                capacity_info = data.groupby(detail_hierarchy)['Capacity(MW)'].first()
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
                    filtered_data = filtered_data[filtered_data['Region'].isin(region_filters)]
                    if logger.isEnabledFor(logging.DEBUG):
                        logger.debug(f"After region filter: {len(filtered_data)} records")
                
                # Apply fuel filter  
                if fuel_filters:
                    filtered_data = filtered_data[filtered_data['Fuel'].isin(fuel_filters)]
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
            
            # Filter the DUID data to only include display columns
            filtered_duid_data = duid_data[display_columns].copy()
            
            # Add Station Name and Owner columns from the original integrated data
            if not filtered_duid_data.empty:
                # Get unique DUIDs from the filtered data
                duids = filtered_duid_data['duid'].unique()
                
                # Extract Station Name (Site Name) and Owner info for these DUIDs
                duid_info = original_data.groupby('duid').agg({
                    'Site Name': 'first',
                    'Owner': 'first'
                }).reset_index()
                
                # Merge this info into the filtered data
                filtered_duid_data = filtered_duid_data.merge(
                    duid_info[duid_info['duid'].isin(duids)],
                    on='duid',
                    how='left'
                )
                
                # Rename for user-friendly display
                if 'Site Name' in filtered_duid_data.columns:
                    filtered_duid_data['station_name'] = filtered_duid_data['Site Name']
                    filtered_duid_data = filtered_duid_data.drop('Site Name', axis=1)
                
                if 'Owner' in filtered_duid_data.columns:
                    filtered_duid_data['owner'] = filtered_duid_data['Owner']
                    filtered_duid_data = filtered_duid_data.drop('Owner', axis=1)
            
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
        
        # Test standardization
        if motor.standardize_columns():
            print("✓ Columns standardized")
            
            # Test integration
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