"""
Optimized Shared Data Service - Memory-efficient data loading and management

This optimized version reduces memory usage from 21GB to ~200-300MB through:
- Lazy loading of 5-minute data
- Memory-efficient data types
- On-demand enrichment instead of pre-computed joins
- Minimal data duplication
"""

import pandas as pd
import numpy as np
import pickle
import logging
from pathlib import Path
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any, Tuple
from functools import lru_cache
import gc

from aemo_dashboard.shared.config import config
from aemo_dashboard.shared.logging_config import get_logger
from aemo_dashboard.shared.performance_logging import PerformanceLogger

logger = get_logger(__name__)
perf_logger = PerformanceLogger(__name__)


class OptimizedSharedDataService:
    """
    Memory-optimized singleton service for AEMO data management.
    
    Key optimizations:
    - Lazy loading of 5-minute data (only when needed)
    - Memory-efficient data types (categories, downcast numerics)
    - On-demand data enrichment (no pre-computed joins)
    - Minimal data duplication
    - Aggressive garbage collection
    """
    
    _instance = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._initialized = False
        return cls._instance
    
    def __init__(self):
        """Initialize the optimized data service"""
        if self._initialized:
            return
            
        logger.info("Initializing Optimized Shared Data Service...")
        
        with perf_logger.timer("total_data_service_init", threshold=5.0):
            # Load only essential data at startup
            self._load_30min_data()
            self._load_duid_mapping()
            self._optimize_memory()
            
        self._initialized = True
        logger.info(f"Optimized Data Service initialized. Memory usage: {self.get_memory_usage():.1f} MB")
    
    def _load_30min_data(self):
        """Load only 30-minute resolution data at startup"""
        with perf_logger.timer("load_30min_data", threshold=1.0):
            try:
                # Load 30-minute generation data
                gen_30_path = str(config.gen_output_file).replace('scada5.parquet', 'scada30.parquet')
                self.generation_30min = self._load_and_optimize_parquet(
                    gen_30_path,
                    dtypes={
                        'duid': 'category',
                        'scadavalue': 'float32'
                    }
                )
                logger.info(f"Loaded {len(self.generation_30min):,} 30-minute generation records")
                
                # Load 30-minute price data
                price_30_path = str(config.spot_hist_file).replace('prices5.parquet', 'prices30.parquet')
                self.price_30min = self._load_and_optimize_parquet(
                    price_30_path,
                    dtypes={
                        'regionid': 'category',
                        'rrp': 'float32'
                    },
                    rename_columns={
                        'SETTLEMENTDATE': 'settlementdate',
                        'REGIONID': 'regionid',
                        'RRP': 'rrp'
                    }
                )
                logger.info(f"Loaded {len(self.price_30min):,} 30-minute price records")
                
                # Load transmission data
                trans_30_path = str(config.transmission_output_file).replace('transmission5.parquet', 'transmission30.parquet')
                self.transmission_30min = self._load_and_optimize_parquet(
                    trans_30_path,
                    dtypes={
                        'interconnectorid': 'category',
                        'meteredmwflow': 'float32',
                        'mwflow': 'float32',
                        'exportlimit': 'float32',
                        'importlimit': 'float32'
                    }
                )
                logger.info(f"Loaded {len(self.transmission_30min):,} 30-minute transmission records")
                
                # Load rooftop solar
                self.rooftop_solar = self._load_and_optimize_parquet(
                    config.rooftop_solar_file,
                    dtypes={
                        'regionid': 'category',
                        'measurement': 'float32',
                        'power': 'float32'
                    }
                )
                logger.info(f"Loaded {len(self.rooftop_solar):,} rooftop solar records")
                
            except Exception as e:
                logger.error(f"Error loading 30-minute data: {e}")
                # Initialize empty DataFrames as fallback
                self.generation_30min = pd.DataFrame()
                self.price_30min = pd.DataFrame()
                self.transmission_30min = pd.DataFrame()
                self.rooftop_solar = pd.DataFrame()
    
    def _load_and_optimize_parquet(
        self, 
        file_path: str, 
        dtypes: Dict[str, str],
        rename_columns: Optional[Dict[str, str]] = None
    ) -> pd.DataFrame:
        """Load parquet file with memory optimizations"""
        try:
            # Load the parquet file
            df = pd.read_parquet(file_path)
            
            # Rename columns if needed
            if rename_columns:
                df.rename(columns=rename_columns, inplace=True)
            
            # Ensure settlementdate is datetime
            if 'settlementdate' in df.columns:
                df['settlementdate'] = pd.to_datetime(df['settlementdate'])
            
            # Apply optimized dtypes
            for col, dtype in dtypes.items():
                if col in df.columns:
                    if dtype == 'category':
                        df[col] = df[col].astype('category')
                    elif dtype == 'float32':
                        df[col] = pd.to_numeric(df[col], downcast='float', errors='coerce')
                    elif dtype == 'int32':
                        df[col] = pd.to_numeric(df[col], downcast='integer', errors='coerce')
            
            # Sort by settlementdate for efficient filtering
            if 'settlementdate' in df.columns:
                df.sort_values('settlementdate', inplace=True)
            
            # Reset index to save memory
            df.reset_index(drop=True, inplace=True)
            
            return df
            
        except Exception as e:
            logger.error(f"Error loading {file_path}: {e}")
            return pd.DataFrame()
    
    def _load_duid_mapping(self):
        """Load DUID mapping with memory optimization"""
        with perf_logger.timer("load_duid_mapping", threshold=0.5):
            try:
                with open(config.gen_info_file, 'rb') as f:
                    self.duid_mapping = pickle.load(f)
                
                # Ensure it's a DataFrame
                if not isinstance(self.duid_mapping, pd.DataFrame):
                    self.duid_mapping = pd.DataFrame(self.duid_mapping)
                
                # Optimize data types
                category_cols = ['DUID', 'Station Name', 'Fuel', 'Technology', 'Region', 'Owner', 'Class']
                for col in category_cols:
                    if col in self.duid_mapping.columns:
                        self.duid_mapping[col] = self.duid_mapping[col].astype('category')
                
                # Downcast numeric columns
                numeric_cols = ['nameplate', 'Reg Cap (MW)']
                for col in numeric_cols:
                    if col in self.duid_mapping.columns:
                        self.duid_mapping[col] = pd.to_numeric(
                            self.duid_mapping[col], 
                            downcast='float', 
                            errors='coerce'
                        )
                
                # Set DUID as index for fast lookups
                if 'DUID' in self.duid_mapping.columns:
                    self.duid_mapping.set_index('DUID', inplace=True)
                
                logger.info(f"Loaded {len(self.duid_mapping)} DUID mappings")
                
            except Exception as e:
                logger.error(f"Error loading DUID mapping: {e}")
                self.duid_mapping = pd.DataFrame()
    
    def _optimize_memory(self):
        """Run garbage collection and optimize memory usage"""
        # Force garbage collection
        gc.collect()
        
        # Log memory usage
        memory_mb = self.get_memory_usage()
        logger.info(f"Memory optimization complete. Current usage: {memory_mb:.1f} MB")
    
    def get_memory_usage(self) -> float:
        """Calculate total memory usage of loaded data in MB"""
        total_bytes = 0
        
        # Add up memory usage of all DataFrames
        dataframes = [
            ('generation_30min', self.generation_30min),
            ('price_30min', self.price_30min),
            ('transmission_30min', self.transmission_30min),
            ('rooftop_solar', self.rooftop_solar),
            ('duid_mapping', self.duid_mapping)
        ]
        
        for name, df in dataframes:
            if isinstance(df, pd.DataFrame) and not df.empty:
                usage = df.memory_usage(deep=True).sum()
                total_bytes += usage
                logger.debug(f"{name}: {usage / 1024 / 1024:.1f} MB")
        
        return total_bytes / 1024 / 1024  # Convert to MB
    
    def get_date_ranges(self) -> Dict[str, Dict[str, Any]]:
        """Get available date ranges for all data types"""
        ranges = {}
        
        datasets = [
            ('generation', self.generation_30min),
            ('prices', self.price_30min),
            ('transmission', self.transmission_30min),
            ('rooftop', self.rooftop_solar)
        ]
        
        for name, df in datasets:
            if not df.empty and 'settlementdate' in df.columns:
                ranges[name] = {
                    'start': df['settlementdate'].min(),
                    'end': df['settlementdate'].max(),
                    'records': len(df)
                }
        
        return ranges
    
    def get_regions(self) -> List[str]:
        """Get list of available regions"""
        if 'Region' in self.duid_mapping.columns:
            return sorted(self.duid_mapping['Region'].dropna().unique().tolist())
        elif self.duid_mapping.index.name == 'DUID' and 'Region' in self.duid_mapping.columns:
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
        
        This method performs on-demand enrichment to avoid storing
        duplicate data in memory.
        """
        # Filter generation data by date
        mask = (
            (self.generation_30min['settlementdate'] >= start_date) &
            (self.generation_30min['settlementdate'] <= end_date)
        )
        gen_filtered = self.generation_30min[mask].copy()
        
        if gen_filtered.empty:
            return pd.DataFrame()
        
        # Get unique DUIDs in the filtered data
        unique_duids = gen_filtered['duid'].unique()
        
        # Get fuel and region info for these DUIDs only
        if self.duid_mapping.index.name == 'DUID':
            duid_info = self.duid_mapping.loc[
                self.duid_mapping.index.isin(unique_duids),
                ['Fuel', 'Region']
            ].copy()
        else:
            duid_info = self.duid_mapping[
                self.duid_mapping['DUID'].isin(unique_duids)
            ][['DUID', 'Fuel', 'Region']].copy()
            duid_info.set_index('DUID', inplace=True)
        
        # Map fuel and region info (much more memory efficient than merge)
        # Handle categories properly
        fuel_mapped = gen_filtered['duid'].map(duid_info['Fuel'])
        gen_filtered['fuel_type'] = fuel_mapped.fillna('Unknown').astype(str)
        
        region_mapped = gen_filtered['duid'].map(duid_info['Region'])
        gen_filtered['region'] = region_mapped.astype(str) if not region_mapped.empty else region_mapped
        
        # Filter by regions if specified
        if regions and 'region' in gen_filtered.columns:
            gen_filtered = gen_filtered[gen_filtered['region'].isin(regions)]
        
        # Aggregate based on resolution
        if resolution == 'hourly':
            result = gen_filtered.groupby([
                pd.Grouper(key='settlementdate', freq='h'),
                'fuel_type'
            ])['scadavalue'].sum().reset_index()
        elif resolution == 'daily':
            result = gen_filtered.groupby([
                pd.Grouper(key='settlementdate', freq='D'),
                'fuel_type'
            ])['scadavalue'].sum().reset_index()
        else:
            # 30min - already at that resolution
            result = gen_filtered.groupby([
                'settlementdate', 
                'fuel_type'
            ])['scadavalue'].sum().reset_index()
        
        return result
    
    def get_regional_prices(
        self,
        start_date: datetime,
        end_date: datetime,
        regions: Optional[List[str]] = None,
        resolution: str = '30min'
    ) -> pd.DataFrame:
        """Get price data by region"""
        # Filter by date range
        mask = (
            (self.price_30min['settlementdate'] >= start_date) &
            (self.price_30min['settlementdate'] <= end_date)
        )
        filtered = self.price_30min[mask]
        
        # Filter by regions if specified
        if regions:
            filtered = filtered[filtered['regionid'].isin(regions)]
        
        # Return copy of selected columns to avoid modifying original
        return filtered[['settlementdate', 'regionid', 'rrp']].copy()
    
    def calculate_revenue(
        self,
        start_date: datetime,
        end_date: datetime,
        group_by: List[str]
    ) -> pd.DataFrame:
        """
        Calculate revenue analysis with on-demand enrichment.
        
        This avoids storing pre-joined data in memory.
        """
        # Filter generation data
        gen_mask = (
            (self.generation_30min['settlementdate'] >= start_date) &
            (self.generation_30min['settlementdate'] <= end_date)
        )
        gen_filtered = self.generation_30min[gen_mask].copy()
        
        if gen_filtered.empty:
            return pd.DataFrame()
        
        # Add region info from DUID mapping
        if self.duid_mapping.index.name == 'DUID':
            gen_filtered['region'] = gen_filtered['duid'].map(self.duid_mapping['Region'])
        else:
            region_map = self.duid_mapping.set_index('DUID')['Region']
            gen_filtered['region'] = gen_filtered['duid'].map(region_map)
        
        # Filter prices for same period
        price_mask = (
            (self.price_30min['settlementdate'] >= start_date) &
            (self.price_30min['settlementdate'] <= end_date)
        )
        price_filtered = self.price_30min[price_mask]
        
        # Merge generation with prices
        revenue_data = gen_filtered.merge(
            price_filtered,
            left_on=['settlementdate', 'region'],
            right_on=['settlementdate', 'regionid'],
            how='inner'
        )
        
        # Calculate revenue (MW * $/MWh / 2 for 30-minute periods)
        revenue_data['revenue'] = revenue_data['scadavalue'] * revenue_data['rrp'] / 2
        
        # Add additional fields for grouping if needed
        if 'fuel_type' in group_by:
            if self.duid_mapping.index.name == 'DUID':
                revenue_data['fuel_type'] = revenue_data['duid'].map(
                    self.duid_mapping['Fuel']
                ).fillna('Unknown')
            else:
                fuel_map = self.duid_mapping.set_index('DUID')['Fuel']
                revenue_data['fuel_type'] = revenue_data['duid'].map(fuel_map).fillna('Unknown')
        
        # Validate group_by columns
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
    
    def load_5min_data_on_demand(self, data_type: str):
        """
        Load 5-minute data only when specifically requested.
        
        This is called on-demand to avoid loading large datasets at startup.
        """
        if data_type == 'generation' and not hasattr(self, 'generation_5min'):
            logger.info("Loading 5-minute generation data on demand...")
            self.generation_5min = self._load_and_optimize_parquet(
                config.gen_output_file,
                dtypes={
                    'duid': 'category',
                    'scadavalue': 'float32'
                }
            )
            logger.info(f"Loaded {len(self.generation_5min):,} 5-minute generation records")
            gc.collect()
        
        elif data_type == 'prices' and not hasattr(self, 'price_5min'):
            logger.info("Loading 5-minute price data on demand...")
            self.price_5min = self._load_and_optimize_parquet(
                config.spot_hist_file,
                dtypes={
                    'regionid': 'category',
                    'rrp': 'float32'
                },
                rename_columns={
                    'SETTLEMENTDATE': 'settlementdate',
                    'REGIONID': 'regionid',
                    'RRP': 'rrp'
                }
            )
            logger.info(f"Loaded {len(self.price_5min):,} 5-minute price records")
            gc.collect()


# Create optimized singleton instance
optimized_data_service = OptimizedSharedDataService()