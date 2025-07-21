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
from ..shared.hybrid_query_manager import HybridQueryManager
from ..shared.duckdb_views import view_manager

logger = get_logger(__name__)

class StationAnalysisMotor:
    """Core calculation engine for station analysis"""
    
    def __init__(self):
        """Initialize the station analysis motor with hybrid query manager"""
        self.query_manager = HybridQueryManager(cache_size_mb=100, cache_ttl=300)
        self.duid_mapping = None
        self.station_data = None  # Filtered data for selected station
        self.data_available = False
        
        # Ensure DuckDB views are created
        view_manager.create_all_views()
        
        logger.info("Station Analysis Motor initialized with hybrid query manager")
    
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
            
            # Mark data as available
            self.data_available = True
            logger.info("Station analysis motor initialized - data will be loaded on demand")
            
            return True
            
        except Exception as e:
            logger.error(f"Error loading DUID mapping: {e}")
            return False
    
    def get_available_date_range(self) -> Tuple[Optional[datetime], Optional[datetime]]:
        """
        Get available date range from DuckDB.
        
        Returns:
            Tuple of (start_date, end_date) as datetime objects
        """
        try:
            date_ranges = self.query_manager.get_date_ranges()
            if 'generation' in date_ranges and 'prices' in date_ranges:
                # Use intersection of generation and price data
                gen_start = date_ranges['generation']['start']
                gen_end = date_ranges['generation']['end']
                price_start = date_ranges['prices']['start']
                price_end = date_ranges['prices']['end']
                
                start_date = max(gen_start, price_start)
                end_date = min(gen_end, price_end)
                
                return start_date, end_date
            return None, None
        except Exception as e:
            logger.error(f"Error getting available date range: {e}")
            return None, None
    
    def filter_station_data(self, duid_or_duids: Union[str, List[str]], start_date: Optional[datetime] = None, 
                           end_date: Optional[datetime] = None) -> bool:
        """
        Filter data for a specific station/DUID(s) and date range using DuckDB.
        
        Args:
            duid_or_duids: Single DUID string or list of DUIDs for station aggregation
            start_date: Start date for filtering (optional)
            end_date: End date for filtering (optional)
            
        Returns:
            bool: True if filtering successful
        """
        try:
            # Handle both single DUID and multiple DUIDs
            if isinstance(duid_or_duids, str):
                duids = [duid_or_duids]
                filter_description = duid_or_duids
            else:
                duids = duid_or_duids
                filter_description = f"station with {len(duids)} units: {', '.join(duids)}"
            
            # Set default date range if not provided
            if not start_date or not end_date:
                end_date = datetime.now()
                start_date = end_date - timedelta(days=30)
                logger.info(f"Using default date range: last 30 days")
            
            logger.info(f"Loading data for {filter_description} from {start_date.date()} to {end_date.date()}")
            
            # Determine resolution based on date range
            days_diff = (end_date - start_date).days
            if days_diff <= 7:
                view_name = "station_time_series_5min"
                revenue_col = "revenue_5min"
                interval_hours = 0.0833  # 5 minutes
            else:
                view_name = "station_time_series_30min"
                revenue_col = "revenue_30min"
                interval_hours = 0.5  # 30 minutes
            
            # Build query for specific DUIDs
            placeholders = ','.join(['?' for _ in duids])
            query = f"""
            SELECT 
                settlementdate,
                duid,
                scadavalue,
                price,
                {revenue_col} as revenue,
                station_name,
                owner,
                region,
                fuel_type,
                capacity_mw
            FROM {view_name}
            WHERE duid IN ({placeholders})
            AND settlementdate >= ?
            AND settlementdate <= ?
            ORDER BY settlementdate
            """
            
            # Execute query - need to format it with actual values since DuckDB doesn't support ? parameters in Python API
            formatted_duids = ','.join([f"'{d}'" for d in duids])
            formatted_query = f"""
            SELECT 
                settlementdate,
                duid,
                scadavalue,
                price,
                {revenue_col} as revenue,
                station_name,
                owner,
                region,
                fuel_type,
                capacity_mw
            FROM {view_name}
            WHERE duid IN ({formatted_duids})
            AND settlementdate >= '{start_date.strftime('%Y-%m-%d %H:%M:%S')}'
            AND settlementdate <= '{end_date.strftime('%Y-%m-%d %H:%M:%S')}'
            ORDER BY settlementdate
            """
            
            filtered_data = self.query_manager.query_with_progress(formatted_query)
            
            if len(filtered_data) == 0:
                logger.warning(f"No data found for {filter_description}")
                self.station_data = pd.DataFrame()
                return False
            
            # If multiple DUIDs (station mode), aggregate by time period
            if len(duids) > 1:
                logger.info(f"Aggregating data for {len(duids)} units in station mode")
                
                # Group by settlementdate and aggregate
                agg_dict = {
                    'scadavalue': 'sum',        # Sum generation across all units
                    'revenue': 'sum',           # Sum revenue across all units 
                    'price': 'mean',            # Price should be same for all units in region
                    'capacity_mw': 'sum'        # Sum capacity across all units
                }
                
                # Add other columns that should be preserved (take first value)
                other_cols = ['region', 'station_name', 'owner', 'fuel_type']
                for col in other_cols:
                    if col in filtered_data.columns:
                        agg_dict[col] = 'first'
                
                # Aggregate the data
                self.station_data = filtered_data.groupby('settlementdate').agg(agg_dict).reset_index()
                
                # Rename revenue column to standard name
                self.station_data['revenue_5min'] = self.station_data['revenue']
                
                logger.info(f"Aggregated to {len(self.station_data):,} time periods for station with {len(duids)} units")
                logger.info(f"Station total capacity: {self.station_data['capacity_mw'].iloc[0]:.1f} MW")
                logger.info(f"Peak station generation: {self.station_data['scadavalue'].max():.1f} MW")
                
            else:
                # Single DUID mode - no aggregation needed
                self.station_data = filtered_data
                # Rename revenue column to standard name
                self.station_data['revenue_5min'] = self.station_data['revenue']
                logger.info(f"Loaded {len(self.station_data):,} records for single DUID")
            
            return True
            
        except Exception as e:
            logger.error(f"Error filtering station data: {e}")
            self.station_data = pd.DataFrame()
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
            # Check if we have enough data for the selected DUID(s)
            duids = self.station_data['duid'].unique() if 'duid' in self.station_data.columns else []
            
            if len(duids) == 0:
                # For aggregated station data, calculate directly
                hourly_data = self.station_data.copy()
                hourly_data['hour'] = hourly_data['settlementdate'].dt.hour
                
                # Group by hour and calculate means
                hourly_stats = hourly_data.groupby('hour').agg({
                    'scadavalue': 'mean',
                    'revenue_5min': 'mean',
                    'price': 'mean'
                }).reset_index()
                
                logger.info(f"Calculated time-of-day averages for aggregated station data")
                return hourly_stats
            
            # For single DUID, we could use the pre-computed view but it's simpler to calculate here
            hourly_data = self.station_data.copy()
            hourly_data['hour'] = hourly_data['settlementdate'].dt.hour
            
            # Group by hour and calculate means
            hourly_stats = hourly_data.groupby('hour').agg({
                'scadavalue': 'mean',
                'revenue_5min': 'mean',
                'price': 'mean'
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
            
            # Determine interval hours based on data frequency
            if len(data) > 1:
                time_diff = (data['settlementdate'].iloc[1] - data['settlementdate'].iloc[0]).total_seconds() / 3600
                if time_diff < 0.1:  # ~5 minutes
                    interval_hours = 0.0833
                else:  # ~30 minutes
                    interval_hours = 0.5
            else:
                interval_hours = 0.0833  # Default to 5 minutes
            
            # Basic metrics
            total_generation_mwh = data['scadavalue'].sum() * interval_hours
            total_revenue = data['revenue_5min'].sum()
            avg_price = (data['scadavalue'] * data['price']).sum() / data['scadavalue'].sum() if data['scadavalue'].sum() > 0 else 0
            
            # Capacity and utilization
            capacity_mw = data['capacity_mw'].iloc[0] if not data['capacity_mw'].isna().all() else 0
            
            # Time-based calculations
            date_range = data['settlementdate'].max() - data['settlementdate'].min()
            hours_in_period = date_range.total_seconds() / 3600
            
            if capacity_mw > 0 and hours_in_period > 0:
                capacity_factor = (total_generation_mwh / (capacity_mw * hours_in_period)) * 100
            else:
                capacity_factor = 0
            
            # Generation statistics
            intervals_generating = (data['scadavalue'] > 0).sum()
            total_intervals = len(data)
            generation_availability = (intervals_generating / total_intervals * 100) if total_intervals > 0 else 0
            
            metrics = {
                'total_generation_mwh': round(total_generation_mwh, 2),
                'total_revenue': round(total_revenue, 2),
                'avg_generation_mw': round(data['scadavalue'].mean(), 2),
                'max_generation_mw': round(data['scadavalue'].max(), 2),
                'min_generation_mw': round(data['scadavalue'].min(), 2),
                'std_generation_mw': round(data['scadavalue'].std(), 2),
                'avg_price_per_mwh': round(avg_price, 2),
                'capacity_mw': round(capacity_mw, 1),
                'capacity_factor_pct': round(capacity_factor, 1),
                'generation_availability_pct': round(generation_availability, 1),
                'data_start': data['settlementdate'].min().strftime('%Y-%m-%d %H:%M'),
                'data_end': data['settlementdate'].max().strftime('%Y-%m-%d %H:%M'),
                'intervals': total_intervals,
                'intervals_generating': intervals_generating
            }
            
            logger.info(f"Calculated performance metrics: {list(metrics.keys())}")
            return metrics
            
        except Exception as e:
            logger.error(f"Error calculating performance metrics: {e}")
            return {}
    
    def get_available_duids(self) -> List[str]:
        """
        Get list of all available DUIDs from the DUID mapping.
        
        Returns:
            List of DUID strings
        """
        if self.duid_mapping is None:
            return []
        
        if isinstance(self.duid_mapping, pd.DataFrame):
            if 'DUID' in self.duid_mapping.columns:
                return self.duid_mapping['DUID'].tolist()
            else:
                # If DUID is the index
                return self.duid_mapping.index.tolist()
        else:
            # If it's a dictionary
            return list(self.duid_mapping.keys())
    
    def get_station_info(self, duid: str) -> Dict:
        """
        Get detailed information about a specific DUID/station.
        
        Args:
            duid: DUID to query
            
        Returns:
            Dictionary with station information
        """
        if self.duid_mapping is None:
            return {}
        
        try:
            if isinstance(self.duid_mapping, pd.DataFrame):
                if 'DUID' in self.duid_mapping.columns:
                    station_info = self.duid_mapping[self.duid_mapping['DUID'] == duid]
                else:
                    # DUID is the index
                    station_info = self.duid_mapping.loc[[duid]]
                
                if len(station_info) > 0:
                    info = station_info.iloc[0].to_dict()
                else:
                    info = {}
            else:
                # Dictionary format
                info = self.duid_mapping.get(duid, {})
            
            return info
            
        except Exception as e:
            logger.error(f"Error getting station info for {duid}: {e}")
            return {}

# Example usage and testing
if __name__ == "__main__":
    motor = StationAnalysisMotor()
    
    # Test data loading
    if motor.load_data():
        print("✓ Data loaded successfully")
        
        # Test date range
        start_date, end_date = motor.get_available_date_range()
        if start_date and end_date:
            print(f"✓ Available date range: {start_date} to {end_date}")
        
        # Get available DUIDs
        duids = motor.get_available_duids()
        print(f"✓ Available DUIDs: {len(duids)}")
        
        if duids:
            # Test with first DUID
            test_duid = duids[0]
            print(f"\nTesting with DUID: {test_duid}")
            
            # Get station info
            info = motor.get_station_info(test_duid)
            print(f"Station info: {info}")
            
            # Filter data
            if motor.filter_station_data(test_duid):
                print(f"✓ Filtered data: {len(motor.station_data)} records")
                
                # Calculate metrics
                metrics = motor.calculate_performance_metrics()
                print(f"✓ Performance metrics: {metrics}")
                
                # Time of day analysis
                tod = motor.calculate_time_of_day_averages()
                print(f"✓ Time of day averages: {len(tod)} hours")