"""
Shared Data Service - Core data loading and management

This singleton class loads all AEMO data once at startup and provides
efficient in-memory access for all dashboard users.
"""

import pandas as pd
import numpy as np
import pickle
import logging
from pathlib import Path
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any, Tuple
from functools import lru_cache

from aemo_dashboard.shared.config import config
from aemo_dashboard.shared.logging_config import get_logger
from aemo_dashboard.shared.performance_logging import PerformanceLogger

logger = get_logger(__name__)
perf_logger = PerformanceLogger(__name__)


class SharedDataService:
    """
    Singleton service that loads and manages all AEMO data in memory.
    
    This class is instantiated once when the FastAPI server starts,
    loading all data into memory for shared access across all users.
    """
    
    _instance = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._initialized = False
        return cls._instance
    
    def __init__(self):
        """Initialize the data service (only runs once)"""
        if self._initialized:
            return
            
        logger.info("Initializing Shared Data Service...")
        
        with perf_logger.timer("total_data_service_init", threshold=5.0):
            # Load all data files
            self._load_generation_data()
            self._load_price_data()
            self._load_transmission_data()
            self._load_rooftop_data()
            self._load_duid_mapping()
            
            # Create enriched datasets
            self._create_enriched_data()
            
            # Pre-calculate common aggregations
            self._precalculate_aggregations()
        
        self._initialized = True
        logger.info(f"Data Service initialized. Memory usage: {self.get_memory_usage():.1f} MB")
    
    def _load_generation_data(self):
        """Load generation data files"""
        with perf_logger.timer("load_generation_data", threshold=1.0):
            try:
                # Load 30-minute data as primary
                gen_30_path = str(config.gen_output_file).replace('scada5.parquet', 'scada30.parquet')
                self.generation_30min = pd.read_parquet(gen_30_path)
                logger.info(f"Loaded {len(self.generation_30min):,} 30-minute generation records")
                
                # Load 5-minute data for recent periods
                self.generation_5min = pd.read_parquet(config.gen_output_file)
                logger.info(f"Loaded {len(self.generation_5min):,} 5-minute generation records")
                
                # Ensure datetime columns
                for df in [self.generation_30min, self.generation_5min]:
                    if 'settlementdate' in df.columns:
                        df['settlementdate'] = pd.to_datetime(df['settlementdate'])
                        
            except Exception as e:
                logger.error(f"Error loading generation data: {e}")
                self.generation_30min = pd.DataFrame()
                self.generation_5min = pd.DataFrame()
    
    def _load_price_data(self):
        """Load price data files"""
        with perf_logger.timer("load_price_data", threshold=1.0):
            try:
                # Load 30-minute prices
                price_30_path = str(config.spot_hist_file).replace('prices5.parquet', 'prices30.parquet')
                self.price_30min = pd.read_parquet(price_30_path)
                logger.info(f"Loaded {len(self.price_30min):,} 30-minute price records")
                
                # Load 5-minute prices
                self.price_5min = pd.read_parquet(config.spot_hist_file)
                logger.info(f"Loaded {len(self.price_5min):,} 5-minute price records")
                
                # Standardize column names
                for df in [self.price_30min, self.price_5min]:
                    if 'settlementdate' in df.columns:
                        df['settlementdate'] = pd.to_datetime(df['settlementdate'])
                    # Ensure consistent column names
                    df.rename(columns={
                        'SETTLEMENTDATE': 'settlementdate',
                        'REGIONID': 'regionid',
                        'RRP': 'rrp'
                    }, inplace=True)
                    
            except Exception as e:
                logger.error(f"Error loading price data: {e}")
                self.price_30min = pd.DataFrame()
                self.price_5min = pd.DataFrame()
    
    def _load_transmission_data(self):
        """Load transmission flow data"""
        with perf_logger.timer("load_transmission_data", threshold=1.0):
            try:
                # Load 30-minute transmission
                trans_30_path = str(config.transmission_output_file).replace('transmission5.parquet', 'transmission30.parquet')
                self.transmission_30min = pd.read_parquet(trans_30_path)
                logger.info(f"Loaded {len(self.transmission_30min):,} 30-minute transmission records")
                
                # Ensure datetime column
                if 'settlementdate' in self.transmission_30min.columns:
                    self.transmission_30min['settlementdate'] = pd.to_datetime(self.transmission_30min['settlementdate'])
                    
            except Exception as e:
                logger.error(f"Error loading transmission data: {e}")
                self.transmission_30min = pd.DataFrame()
    
    def _load_rooftop_data(self):
        """Load rooftop solar data"""
        with perf_logger.timer("load_rooftop_data", threshold=0.5):
            try:
                self.rooftop_solar = pd.read_parquet(config.rooftop_solar_file)
                logger.info(f"Loaded {len(self.rooftop_solar):,} rooftop solar records")
                
                if 'settlementdate' in self.rooftop_solar.columns:
                    self.rooftop_solar['settlementdate'] = pd.to_datetime(self.rooftop_solar['settlementdate'])
                    
            except Exception as e:
                logger.error(f"Error loading rooftop data: {e}")
                self.rooftop_solar = pd.DataFrame()
    
    def _load_duid_mapping(self):
        """Load DUID to station mapping"""
        with perf_logger.timer("load_duid_mapping", threshold=0.5):
            try:
                with open(config.gen_info_file, 'rb') as f:
                    self.duid_mapping = pickle.load(f)
                logger.info(f"Loaded {len(self.duid_mapping)} DUID mappings")
                
                # Ensure it's a DataFrame
                if not isinstance(self.duid_mapping, pd.DataFrame):
                    self.duid_mapping = pd.DataFrame(self.duid_mapping)
                    
            except Exception as e:
                logger.error(f"Error loading DUID mapping: {e}")
                self.duid_mapping = pd.DataFrame()
    
    def _create_enriched_data(self):
        """Create enriched datasets by joining with DUID mapping"""
        with perf_logger.timer("create_enriched_data", threshold=2.0):
            try:
                # Enrich generation data with station info
                self.generation_enriched = self.generation_30min.merge(
                    self.duid_mapping,
                    left_on='duid',
                    right_on='DUID',
                    how='left'
                )
                
                # Add derived columns
                if 'Fuel' in self.generation_enriched.columns:
                    # Standardize fuel types
                    self.generation_enriched['fuel_type'] = self.generation_enriched['Fuel'].fillna('Unknown')
                
                if 'Region' in self.generation_enriched.columns:
                    self.generation_enriched['region'] = self.generation_enriched['Region']
                    
                logger.info("Created enriched generation dataset")
                
            except Exception as e:
                logger.error(f"Error creating enriched data: {e}")
                self.generation_enriched = self.generation_30min.copy()
    
    def _precalculate_aggregations(self):
        """Pre-calculate common aggregations for faster access"""
        with perf_logger.timer("precalculate_aggregations", threshold=2.0):
            try:
                # Daily generation by fuel type
                self.daily_fuel_generation = self.generation_enriched.groupby([
                    pd.Grouper(key='settlementdate', freq='D'),
                    'fuel_type'
                ])['scadavalue'].sum().reset_index()
                
                # Hourly average prices by region
                self.hourly_region_prices = self.price_30min.groupby([
                    pd.Grouper(key='settlementdate', freq='h'),
                    'regionid'
                ])['rrp'].mean().reset_index()
                
                logger.info("Pre-calculated common aggregations")
                
            except Exception as e:
                logger.error(f"Error pre-calculating aggregations: {e}")
    
    def get_memory_usage(self) -> float:
        """Calculate total memory usage of loaded data in MB"""
        total_bytes = 0
        
        # Add up memory usage of all DataFrames
        for attr_name in dir(self):
            attr = getattr(self, attr_name)
            if isinstance(attr, pd.DataFrame) and not attr_name.startswith('_'):
                total_bytes += attr.memory_usage(deep=True).sum()
        
        return total_bytes / 1024 / 1024  # Convert to MB
    
    def get_date_ranges(self) -> Dict[str, Dict[str, Any]]:
        """Get available date ranges for all data types"""
        ranges = {}
        
        if not self.generation_30min.empty:
            ranges['generation'] = {
                'start': self.generation_30min['settlementdate'].min(),
                'end': self.generation_30min['settlementdate'].max(),
                'records': len(self.generation_30min)
            }
        
        if not self.price_30min.empty:
            ranges['prices'] = {
                'start': self.price_30min['settlementdate'].min(),
                'end': self.price_30min['settlementdate'].max(),
                'records': len(self.price_30min)
            }
        
        if not self.transmission_30min.empty:
            ranges['transmission'] = {
                'start': self.transmission_30min['settlementdate'].min(),
                'end': self.transmission_30min['settlementdate'].max(),
                'records': len(self.transmission_30min)
            }
        
        return ranges
    
    def get_regions(self) -> List[str]:
        """Get list of available regions"""
        if 'Region' in self.duid_mapping.columns:
            return sorted(self.duid_mapping['Region'].dropna().unique().tolist())
        return []
    
    def get_fuel_types(self) -> List[str]:
        """Get list of available fuel types"""
        if 'Fuel' in self.duid_mapping.columns:
            return sorted(self.duid_mapping['Fuel'].dropna().unique().tolist())
        return []
    
    @lru_cache(maxsize=128)
    def get_generation_by_fuel(
        self,
        start_date: datetime,
        end_date: datetime,
        regions: Optional[Tuple[str]] = None,
        resolution: str = '30min'
    ) -> pd.DataFrame:
        """
        Get generation data aggregated by fuel type.
        
        Args:
            start_date: Start of date range
            end_date: End of date range
            regions: Optional tuple of regions to filter
            resolution: Data resolution ('5min', '30min', 'hourly', 'daily')
            
        Returns:
            DataFrame with columns: settlementdate, fuel_type, scadavalue
        """
        # Select appropriate dataset
        if resolution == '5min' and not self.generation_5min.empty:
            data = self.generation_enriched  # Would need enriched 5min data
        else:
            data = self.generation_enriched
        
        # Filter by date range
        mask = (
            (data['settlementdate'] >= start_date) &
            (data['settlementdate'] <= end_date)
        )
        filtered = data[mask]
        
        # Filter by regions if specified
        if regions and 'region' in filtered.columns:
            filtered = filtered[filtered['region'].isin(regions)]
        
        # Aggregate based on resolution
        if resolution == 'hourly':
            result = filtered.groupby([
                pd.Grouper(key='settlementdate', freq='h'),
                'fuel_type'
            ])['scadavalue'].sum().reset_index()
        elif resolution == 'daily':
            result = filtered.groupby([
                pd.Grouper(key='settlementdate', freq='D'),
                'fuel_type'
            ])['scadavalue'].sum().reset_index()
        else:
            # 30min or 5min - already at that resolution
            result = filtered.groupby(['settlementdate', 'fuel_type'])['scadavalue'].sum().reset_index()
        
        return result
    
    def get_regional_prices(
        self,
        start_date: datetime,
        end_date: datetime,
        regions: Optional[List[str]] = None,
        resolution: str = '30min'
    ) -> pd.DataFrame:
        """
        Get price data by region.
        
        Args:
            start_date: Start of date range
            end_date: End of date range
            regions: Optional list of regions to filter
            resolution: Data resolution
            
        Returns:
            DataFrame with columns: settlementdate, regionid, rrp
        """
        # Select appropriate dataset
        data = self.price_5min if resolution == '5min' and not self.price_5min.empty else self.price_30min
        
        # Filter by date range
        mask = (
            (data['settlementdate'] >= start_date) &
            (data['settlementdate'] <= end_date)
        )
        filtered = data[mask]
        
        # Filter by regions if specified
        if regions:
            filtered = filtered[filtered['regionid'].isin(regions)]
        
        return filtered[['settlementdate', 'regionid', 'rrp']].copy()
    
    def calculate_revenue(
        self,
        start_date: datetime,
        end_date: datetime,
        group_by: List[str]
    ) -> pd.DataFrame:
        """
        Calculate revenue analysis with custom grouping.
        
        Args:
            start_date: Start of date range
            end_date: End of date range
            group_by: List of columns to group by
            
        Returns:
            DataFrame with aggregated revenue data
        """
        # Filter generation data
        gen_mask = (
            (self.generation_enriched['settlementdate'] >= start_date) &
            (self.generation_enriched['settlementdate'] <= end_date)
        )
        gen_filtered = self.generation_enriched[gen_mask]
        
        # Merge with prices
        revenue_data = gen_filtered.merge(
            self.price_30min,
            left_on=['settlementdate', 'region'],
            right_on=['settlementdate', 'regionid'],
            how='inner'
        )
        
        # Calculate revenue (MW * $/MWh / 2 for 30-minute periods)
        revenue_data['revenue'] = revenue_data['scadavalue'] * revenue_data['rrp'] / 2
        
        # Ensure all group_by columns exist
        valid_group_by = [col for col in group_by if col in revenue_data.columns]
        if not valid_group_by:
            logger.warning(f"No valid grouping columns found in {group_by}")
            return pd.DataFrame()
        
        # Aggregate
        result = revenue_data.groupby(valid_group_by).agg({
            'scadavalue': 'sum',
            'revenue': 'sum',
            'rrp': 'mean'
        }).round(2).reset_index()
        
        return result


# Create singleton instance
data_service = SharedDataService()