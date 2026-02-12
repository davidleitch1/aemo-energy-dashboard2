"""
Station Analysis Motor - Core calculation engine for individual station/DUID analysis.

This module provides data integration and calculation functions for detailed 
station-level performance analysis including time series, rankings, and statistics.
"""

import pandas as pd
import numpy as np
from pathlib import Path
from typing import Dict, List, Tuple, Optional, Union
import pickle
from datetime import datetime, timedelta
from ..shared.config import config
from ..shared.logging_config import get_logger

logger = get_logger(__name__)

class StationAnalysisMotor:
    """Core calculation engine for station analysis"""
    
    def __init__(self):
        """Initialize the station analysis motor"""
        self.gen_data = None
        self.price_data = None 
        self.duid_mapping = None
        self.integrated_data = None
        self.station_data = None  # Filtered data for selected station
        logger.info("Station Analysis Motor initialized")
    
    def load_data(self) -> bool:
        """
        Load DUID mapping only. Data will be loaded on-demand based on user requests.
        
        Returns:
            bool: True if mapping loaded successfully
        """
        try:
            logger.info("Loading DUID mapping...")
            with open(config.gen_info_file, 'rb') as f:
                self.duid_mapping = pickle.load(f)
            logger.info(f"Loaded {len(self.duid_mapping)} DUID mappings")
            
            # Initialize data containers but don't load data yet
            self.gen_data = None
            self.price_data = None
            logger.info("Station analysis motor initialized - data will be loaded on demand")
            
            return True
            
        except Exception as e:
            logger.error(f"Error loading DUID mapping: {e}")
            return False
    
    def load_data_for_date_range(self, start_date: datetime, end_date: datetime) -> bool:
        """
        Load generation and price data for specific date range on-demand.
        
        Args:
            start_date: Start of requested date range
            end_date: End of requested date range
            
        Returns:
            bool: True if data loaded successfully
        """
        try:
            logger.info(f"Loading data on-demand for {start_date.date()} to {end_date.date()}")
            
            # Reset integrated data since we're loading new raw data
            self.integrated_data = None
            self.station_data = None
            
            # Use intelligent fallback to handle data gaps
            from ..shared.resolution_manager import resolution_manager
            from ..shared.adapter_selector import load_generation_data, load_price_data
            
            # Get optimal resolution strategy with fallback
            generation_strategy = resolution_manager.get_optimal_resolution_with_fallback(
                start_date, end_date, 'generation'
            )
            price_strategy = resolution_manager.get_optimal_resolution_with_fallback(
                start_date, end_date, 'price'
            )
            
            logger.info(f"Generation strategy: {generation_strategy['reasoning']}")
            logger.info(f"Price strategy: {price_strategy['reasoning']}")
            
            # Load generation data with fallback
            self.gen_data = load_generation_data(
                start_date=start_date,
                end_date=end_date,
                resolution='auto'  # Uses fallback automatically
            )
            logger.info(f"Loaded {len(self.gen_data):,} generation records for requested period")
            
            # Load price data with fallback
            self.price_data = load_price_data(
                start_date=start_date,
                end_date=end_date,
                resolution='auto'  # Uses fallback automatically
            )
            logger.info(f"Loaded {len(self.price_data):,} price records for requested period")
            
            return True
            
        except Exception as e:
            logger.error(f"Error loading data for date range: {e}")
            return False
    
    def standardize_columns(self) -> bool:
        """
        Standardize column names and data types across datasets.
        
        Returns:
            bool: True if standardization successful
        """
        try:
            # Skip if data not loaded yet
            if self.gen_data is None or self.price_data is None:
                logger.info("Data not loaded yet, skipping column standardization")
                return True
                
            logger.info("Standardizing column names...")
            
            # Standardize generation data - lowercase (match price analysis motor)
            gen_columns_before = list(self.gen_data.columns)
            self.gen_data.columns = self.gen_data.columns.str.lower()
            logger.info(f"Generation columns: {gen_columns_before} -> {list(self.gen_data.columns)}")
            
            # Price data already has proper structure - index is SETTLEMENTDATE
            # Reset index to make SETTLEMENTDATE a column
            if isinstance(self.price_data.index, pd.DatetimeIndex):
                self.price_data = self.price_data.reset_index()
                logger.info(f"Price data: Reset datetime index to column")
            
            logger.info(f"Price columns: {list(self.price_data.columns)}")
            
            # Ensure generation data has proper datetime
            if self.gen_data['settlementdate'].dtype == 'object':
                self.gen_data['settlementdate'] = pd.to_datetime(self.gen_data['settlementdate'])
            
            # Ensure price data has proper datetime
            if 'SETTLEMENTDATE' in self.price_data.columns and self.price_data['SETTLEMENTDATE'].dtype == 'object':
                self.price_data['SETTLEMENTDATE'] = pd.to_datetime(self.price_data['SETTLEMENTDATE'])
                
            logger.info("Column standardization completed")
            return True
            
        except Exception as e:
            logger.error(f"Error standardizing columns: {e}")
            return False
    
    def integrate_data(self) -> bool:
        """
        Integrate generation and price data for analysis.
        
        Returns:
            bool: True if integration successful
        """
        try:
            # Ensure data is available
            if self.gen_data is None or self.price_data is None:
                logger.error("Generation or price data not loaded. Cannot integrate.")
                return False
                
            # Standardize columns first
            if not self.standardize_columns():
                logger.error("Failed to standardize columns")
                return False
                
            logger.info("Starting data integration...")
            
            # First, prepare DUID mapping DataFrame
            if isinstance(self.duid_mapping, pd.DataFrame):
                duid_df = self.duid_mapping.copy()
                # The DataFrame should have DUID as column, transpose if needed
                if 'DUID' not in duid_df.columns:
                    duid_df = duid_df.T
                    duid_df = duid_df.reset_index()
                    duid_df.columns = ['DUID'] + list(duid_df.columns[1:])
            else:
                # Handle dictionary format as fallback
                duid_df = pd.DataFrame(self.duid_mapping).T.reset_index()
                duid_df.columns = ['DUID'] + list(duid_df.columns[1:])
            
            # Rename columns to expected names if they exist
            column_renames = {
                'Site Name': 'Station Name',
                'Capacity(MW)': 'Nameplate Capacity (MW)'
            }
            for old_name, new_name in column_renames.items():
                if old_name in duid_df.columns:
                    duid_df[new_name] = duid_df[old_name]
            
            logger.info("Joining generation data with DUID mapping...")
            # First merge generation with DUID mapping to get Region
            gen_with_mapping = self.gen_data.merge(
                duid_df,
                left_on='duid',
                right_on='DUID',
                how='left'
            )
            
            logger.info(f"Generation+DUID data shape: {gen_with_mapping.shape}")
            
            # Check for missing DUIDs
            missing_duids = gen_with_mapping[gen_with_mapping['Region'].isna()]['duid'].unique()
            if len(missing_duids) > 0:
                logger.warning(f"Found {len(missing_duids)} DUIDs without mapping: {missing_duids[:10]}...")
            
            logger.info("Joining with price data...")
            # Handle different price data formats
            if 'SETTLEMENTDATE' in self.price_data.columns:
                # SETTLEMENTDATE is a column
                right_on_cols = ['SETTLEMENTDATE', 'REGIONID']
            else:
                # SETTLEMENTDATE might be the index, reset it first
                if isinstance(self.price_data.index, pd.DatetimeIndex):
                    self.price_data = self.price_data.reset_index()
                    logger.info(f"Price data: Reset datetime index to column in merge")
                right_on_cols = ['SETTLEMENTDATE', 'REGIONID']
            
            # Then merge with price data using Region
            self.integrated_data = gen_with_mapping.merge(
                self.price_data,
                left_on=['settlementdate', 'Region'],
                right_on=right_on_cols,
                how='inner'  # Only keep records where we have both generation and price
            )
            
            logger.info(f"Integrated data shape: {self.integrated_data.shape}")
            
            # Calculate revenue for each 5-minute interval
            # Use the correct column name (lowercase from the data)
            scada_col = 'scadavalue' if 'scadavalue' in self.integrated_data.columns else 'SCADAVALUE'
            self.integrated_data['revenue_5min'] = (
                self.integrated_data[scada_col] * self.integrated_data['RRP'] * (5/60)  # 5 minutes in hours
            )
            
            # Clean column names - ensure scadavalue is properly named
            rename_dict = {
                'RRP': 'price',
                'REGIONID': 'region',
                'Station Name': 'station_name',
                'Owner': 'owner',
                'Nameplate Capacity (MW)': 'capacity_mw'
            }
            
            # Add scadavalue column name fix if needed
            if 'SCADAVALUE' in self.integrated_data.columns and 'scadavalue' not in self.integrated_data.columns:
                rename_dict['SCADAVALUE'] = 'scadavalue'
            
            self.integrated_data.rename(columns=rename_dict, inplace=True)
            
            logger.info(f"Data integration completed. {len(self.integrated_data):,} records available")
            return True
            
        except Exception as e:
            logger.error(f"Error integrating data: {e}")
            return False
    
    def filter_station_data(self, duid_or_duids: Union[str, List[str]], start_date: Optional[datetime] = None, 
                           end_date: Optional[datetime] = None) -> bool:
        """
        Filter data for a specific station/DUID(s) and date range.
        
        Args:
            duid_or_duids: Single DUID string or list of DUIDs for station aggregation
            start_date: Start date for filtering (optional)
            end_date: End date for filtering (optional)
            
        Returns:
            bool: True if filtering successful
        """
        try:
            # Always reload data for the specific date range requested
            if start_date and end_date:
                logger.info(f"Loading data for date range: {start_date.date()} to {end_date.date()}")
                if not self.load_data_for_date_range(start_date, end_date):
                    logger.error("Failed to load data for date range")
                    return False
            else:
                # Default to last 30 days if no date range specified
                from datetime import datetime, timedelta
                end_date = datetime.now()
                start_date = end_date - timedelta(days=30)
                logger.info(f"No date range specified, loading last 30 days: {start_date.date()} to {end_date.date()}")
                if not self.load_data_for_date_range(start_date, end_date):
                    logger.error("Failed to load default data range")
                    return False
            
            # Ensure data is integrated
            if self.integrated_data is None:
                logger.info("Integrating data...")
                if not self.integrate_data():
                    logger.error("Failed to integrate data")
                    return False
            
            # Handle both single DUID and multiple DUIDs for station aggregation
            if isinstance(duid_or_duids, str):
                duids = [duid_or_duids]
                filter_description = duid_or_duids
            else:
                duids = duid_or_duids
                filter_description = f"station with {len(duids)} units: {', '.join(duids)}"
            
            # Filter by DUID(s) (use lowercase column name from generation data)
            station_filter = self.integrated_data['duid'].isin(duids)
            
            # Apply date filters if provided
            if start_date:
                station_filter &= self.integrated_data['settlementdate'] >= start_date
            if end_date:
                station_filter &= self.integrated_data['settlementdate'] <= end_date
            
            filtered_data = self.integrated_data[station_filter].copy()
            
            if len(filtered_data) == 0:
                logger.warning(f"No data found for {filter_description}")
                return False
            
            # If multiple DUIDs (station mode), aggregate by time period
            if len(duids) > 1:
                logger.info(f"Aggregating data for {len(duids)} units in station mode")
                
                # Group by settlementdate and aggregate
                # Sum generation and revenue, mean for price, sum for capacity
                agg_dict = {
                    'scadavalue': 'sum',        # Sum generation across all units
                    'revenue_5min': 'sum',      # Sum revenue across all units 
                    'price': 'mean',            # Price should be same for all units in region
                    'capacity_mw': 'sum'        # Sum capacity across all units
                }
                
                # Add other columns that should be preserved (take first value)
                other_cols = ['region', 'station_name', 'owner', 'Fuel']
                for col in other_cols:
                    if col in filtered_data.columns:
                        agg_dict[col] = 'first'
                
                # Aggregate the data
                self.station_data = filtered_data.groupby('settlementdate').agg(agg_dict).reset_index()
                
                logger.info(f"Aggregated to {len(self.station_data):,} time periods for station with {len(duids)} units")
                logger.info(f"Station total capacity: {self.station_data['capacity_mw'].iloc[0]:.1f} MW")
                logger.info(f"Peak station generation: {self.station_data['scadavalue'].max():.1f} MW")
                
            else:
                # Single DUID mode - no aggregation needed
                self.station_data = filtered_data
                logger.info(f"Filtered {len(self.station_data):,} records for single DUID")
            
            return True
            
        except Exception as e:
            logger.error(f"Error filtering station data: {e}")
            return False
    
    def calculate_time_of_day_averages(self) -> pd.DataFrame:
        """
        Calculate average performance metrics by hour of day.
        
        Returns:
            DataFrame with hourly statistics
        """
        if self.station_data is None or len(self.station_data) == 0:
            return pd.DataFrame()
        
        try:
            # Extract hour from datetime
            hourly_data = self.station_data.copy()
            hourly_data['hour'] = hourly_data['settlementdate'].dt.hour
            
            # Group by hour and calculate means
            hourly_stats = hourly_data.groupby('hour').agg({
                'scadavalue': 'mean',           # Average generation by hour
                'revenue_5min': 'mean',         # Average revenue by hour  
                'price': 'mean'                 # Average price by hour
            }).reset_index()
            
            logger.info(f"Calculated time-of-day averages for {len(hourly_stats)} hours")
            return hourly_stats
            
        except Exception as e:
            logger.error(f"Error calculating time-of-day averages: {e}")
            return pd.DataFrame()
    
    def calculate_performance_metrics(self) -> Dict:
        """
        Calculate comprehensive performance metrics for the station.
        
        Returns:
            Dictionary containing performance statistics
        """
        if self.station_data is None or len(self.station_data) == 0:
            return {}
        
        try:
            data = self.station_data
            
            # Basic metrics
            total_generation_mwh = data['scadavalue'].sum() * (5/60)  # Convert to MWh
            total_revenue = data['revenue_5min'].sum()
            avg_price = (data['scadavalue'] * data['price']).sum() / data['scadavalue'].sum() if data['scadavalue'].sum() > 0 else 0
            
            # Capacity and utilization
            capacity_mw = data['capacity_mw'].iloc[0] if not data['capacity_mw'].isna().all() else 0
            hours_in_period = len(data) * (5/60)  # Convert 5-min intervals to hours
            capacity_factor = (total_generation_mwh / (capacity_mw * hours_in_period) * 100) if capacity_mw > 0 and hours_in_period > 0 else 0
            
            # Peak and operational statistics
            peak_generation = data['scadavalue'].max()
            peak_revenue_5min = data['revenue_5min'].max()
            best_price = data['price'].max()
            
            # Operating hours (periods with generation > 0)
            operating_periods = (data['scadavalue'] > 0).sum()
            operating_hours = operating_periods * (5/60)
            zero_generation_periods = (data['scadavalue'] == 0).sum()
            zero_generation_hours = zero_generation_periods * (5/60)
            
            # Time period info
            period_start = data['settlementdate'].min()
            period_end = data['settlementdate'].max()
            
            # Get fuel type - check multiple possible column names
            fuel_type = 'Unknown'
            for fuel_col in ['Fuel', 'fuel', 'FUEL']:
                if fuel_col in data.columns and not data[fuel_col].isna().all():
                    fuel_type = data[fuel_col].iloc[0]
                    break
            
            # Get station name - check multiple possible column names  
            station_name = 'Unknown'
            for name_col in ['Site Name', 'Station Name', 'station_name']:
                if name_col in data.columns and not data[name_col].isna().all():
                    station_name = data[name_col].iloc[0]
                    break
            
            # Get owner - check multiple possible column names
            owner = 'Unknown'
            for owner_col in ['Owner', 'owner', 'OWNER']:
                if owner_col in data.columns and not data[owner_col].isna().all():
                    owner = data[owner_col].iloc[0]
                    break
            
            # Get region - check multiple possible column names
            region = 'Unknown'
            for region_col in ['Region', 'region', 'REGION']:
                if region_col in data.columns and not data[region_col].isna().all():
                    region = data[region_col].iloc[0]
                    break
            
            metrics = {
                'total_generation_gwh': total_generation_mwh / 1000,  # Convert to GWh
                'total_revenue_millions': total_revenue / 1_000_000,  # Convert to millions
                'average_price': avg_price,
                'capacity_factor': capacity_factor,
                'capacity_mw': capacity_mw,
                'peak_generation': peak_generation,
                'peak_revenue_5min': peak_revenue_5min,
                'best_price': best_price,
                'operating_hours': operating_hours,
                'zero_generation_hours': zero_generation_hours,
                'total_periods': len(data),
                'period_start': period_start,
                'period_end': period_end,
                'station_name': station_name,
                'owner': owner,
                'fuel_type': fuel_type,
                'region': region
            }
            
            logger.info(f"Calculated performance metrics for station")
            return metrics
            
        except Exception as e:
            logger.error(f"Error calculating performance metrics: {e}")
            return {}
    
    def get_available_duids(self) -> List[str]:
        """
        Get list of all available DUIDs from the mapping.
        
        Returns:
            List of DUID strings
        """
        if self.duid_mapping is None:
            return []
        
        return list(self.duid_mapping.keys())
    
    def get_station_info(self, duid: str) -> Dict:
        """
        Get station information for a specific DUID.
        
        Args:
            duid: The DUID to look up
            
        Returns:
            Dictionary with station information
        """
        if self.duid_mapping is None or duid not in self.duid_mapping:
            return {}
        
        station_info = self.duid_mapping[duid].copy()
        station_info['duid'] = duid
        return station_info