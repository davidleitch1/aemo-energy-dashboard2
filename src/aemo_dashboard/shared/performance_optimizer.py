"""
Performance Optimizer for Multi-Year Data Visualization

This module provides intelligent data resampling and optimization strategies
for rendering large time series datasets efficiently.
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Optional, Dict, Any, Tuple
from .logging_config import get_logger

logger = get_logger(__name__)

class PerformanceOptimizer:
    """Intelligent data resampling for optimal visualization performance"""
    
    # Performance thresholds for different time ranges
    RESAMPLING_RULES = {
        'realtime': {
            'max_days': 0.5,  # 12 hours
            'frequency': '5min',
            'description': 'Real-time 5-minute data'
        },
        'recent': {
            'max_days': 7,
            'frequency': '15min',  # Slight aggregation for week view
            'description': 'Recent 15-minute data'
        },
        'short_term': {
            'max_days': 30,
            'frequency': '1h',
            'description': 'Short-term hourly data'
        },
        'medium_term': {
            'max_days': 90,
            'frequency': '4h',
            'description': 'Medium-term 4-hourly data'
        },
        'long_term': {
            'max_days': 365,
            'frequency': '1D',
            'description': 'Long-term daily data'
        },
        'historical': {
            'max_days': float('inf'),
            'frequency': '1W',
            'description': 'Historical weekly data'
        }
    }
    
    @classmethod
    def get_optimal_frequency(cls, start_date: datetime, end_date: datetime, 
                             plot_type: str = 'generation') -> Dict[str, Any]:
        """
        Determine optimal resampling frequency based on date range and plot type
        
        Args:
            start_date: Start of time range
            end_date: End of time range
            plot_type: Type of plot ('generation', 'price', 'station')
            
        Returns:
            Dict with frequency, rule name, and metadata
        """
        days_span = (end_date - start_date).total_seconds() / (24 * 3600)
        
        # Special handling for time-of-day analysis
        if plot_type == 'time_of_day':
            return cls._get_time_of_day_strategy(days_span)
        
        # Find appropriate resampling rule
        for rule_name, rule in cls.RESAMPLING_RULES.items():
            if days_span <= rule['max_days']:
                return {
                    'frequency': rule['frequency'],
                    'rule_name': rule_name,
                    'description': rule['description'],
                    'days_span': days_span,
                    'estimated_points': cls._estimate_data_points(days_span, rule['frequency'])
                }
        
        # Default to historical for very long ranges
        rule = cls.RESAMPLING_RULES['historical']
        return {
            'frequency': rule['frequency'],
            'rule_name': 'historical',
            'description': rule['description'],
            'days_span': days_span,
            'estimated_points': cls._estimate_data_points(days_span, rule['frequency'])
        }
    
    @classmethod
    def _get_time_of_day_strategy(cls, days_span: float) -> Dict[str, Any]:
        """Special strategy for time-of-day analysis that preserves hourly patterns"""
        if days_span <= 30:
            # For short ranges, use all data
            freq = '5min'
            desc = 'Full resolution for time-of-day analysis'
        elif days_span <= 365:
            # For medium ranges, sample representative days
            freq = 'sample_days'
            desc = 'Sampled days preserving weekly patterns'
        else:
            # For long ranges, use monthly samples
            freq = 'sample_months'
            desc = 'Monthly samples for long-term time-of-day patterns'
        
        return {
            'frequency': freq,
            'rule_name': 'time_of_day',
            'description': desc,
            'days_span': days_span,
            'special_handling': True
        }
    
    @classmethod
    def _estimate_data_points(cls, days_span: float, frequency: str) -> int:
        """Estimate number of data points after resampling"""
        freq_map = {
            '5min': 288,    # 288 points per day
            '15min': 96,    # 96 points per day
            '1h': 24,       # 24 points per day
            '4h': 6,        # 6 points per day  
            '1D': 1,        # 1 point per day
            '1W': 1/7,      # 1 point per week
        }
        
        points_per_day = freq_map.get(frequency, 288)
        return int(days_span * points_per_day)
    
    @classmethod
    def resample_generation_data(cls, df: pd.DataFrame, frequency: str, 
                                start_date: datetime, end_date: datetime) -> pd.DataFrame:
        """
        Resample generation data with appropriate aggregation method
        
        Args:
            df: DataFrame with generation data
            frequency: Pandas frequency string ('1h', '1D', etc.)
            start_date: Start date for filtering
            end_date: End date for filtering
            
        Returns:
            Resampled DataFrame
        """
        if df.empty:
            return df
        
        logger.info(f"Resampling generation data to {frequency} frequency")
        logger.info(f"Input shape: {df.shape}")
        
        # Ensure datetime index
        if not isinstance(df.index, pd.DatetimeIndex):
            # Check for various datetime column names
            datetime_col = None
            for col in ['settlementdate', 'SETTLEMENTDATE', 'datetime', 'timestamp']:
                if col in df.columns:
                    datetime_col = col
                    break
            
            if datetime_col:
                df = df.set_index(datetime_col)
                logger.info(f"Set datetime index using column: {datetime_col}")
            else:
                logger.warning(f"No datetime column found in {list(df.columns)}, returning original data")
                return df
        
        # Filter to date range
        df_filtered = df.loc[start_date:end_date]
        
        # Handle special frequencies
        if frequency == 'sample_days':
            return cls._sample_representative_days(df_filtered)
        elif frequency == 'sample_months':
            return cls._sample_representative_months(df_filtered)
        
        # Standard resampling
        try:
            # Group by DUID and resample
            if 'duid' in df_filtered.columns:
                # For generation data with DUIDs
                agg_dict = {}
                
                # Define aggregation based on available columns
                if 'scadavalue' in df_filtered.columns:
                    agg_dict['scadavalue'] = 'mean'
                if 'mw' in df_filtered.columns:
                    agg_dict['mw'] = 'mean'
                
                # Preserve categorical columns
                for col in ['fuel_type', 'station', 'region']:
                    if col in df_filtered.columns:
                        agg_dict[col] = 'first'
                
                if agg_dict:
                    resampled = (df_filtered.groupby('duid')
                               .resample(frequency)
                               .agg(agg_dict)
                               .reset_index())
                else:
                    # Fallback: just resample numeric columns
                    numeric_cols = df_filtered.select_dtypes(include=[np.number]).columns
                    resampled = (df_filtered.groupby('duid')
                               .resample(frequency)[numeric_cols]
                               .mean()
                               .reset_index())
            else:
                # For other data types (transmission, price, etc.)
                numeric_cols = df_filtered.select_dtypes(include=[np.number]).columns
                non_numeric_cols = df_filtered.select_dtypes(exclude=[np.number]).columns
                
                # Check if we have categorical grouping columns (like interconnectorid)
                group_cols = []
                potential_group_cols = ['interconnectorid', 'regionid', 'REGIONID', 'region']
                for col in potential_group_cols:
                    if col in df_filtered.columns:
                        group_cols.append(col)
                
                if group_cols:
                    # Group by categorical columns first, then resample
                    logger.info(f"Grouping by {group_cols} before resampling")
                    
                    # For grouped resampling, we need to use a different approach
                    # because the groupby columns are already in the data
                    grouped_resampled = (df_filtered.groupby(group_cols)
                                       .resample(frequency)[numeric_cols]
                                       .mean()
                                       .reset_index())
                    
                    # The result should now have the correct columns
                    resampled = grouped_resampled
                    
                    logger.info(f"Grouped resampling completed: {resampled.shape}")
                    logger.info(f"Resampled columns: {list(resampled.columns)}")
                else:
                    # No grouping columns, simple resampling
                    resampled = df_filtered.resample(frequency)[numeric_cols].mean()
                    
                    # Reset index to convert datetime index back to column
                    resampled = resampled.reset_index()
                    
                    # Preserve non-numeric columns by taking first value in each group
                    if len(non_numeric_cols) > 0:
                        # Resample non-numeric columns separately
                        non_numeric_resampled = df_filtered.resample(frequency)[non_numeric_cols].first().reset_index()
                        
                        # Merge numeric and non-numeric data
                        datetime_col = resampled.columns[0]  # First column should be datetime
                        resampled = resampled.merge(non_numeric_resampled, on=datetime_col, how='left')
                        
                        logger.info(f"Preserved non-numeric columns: {list(non_numeric_cols)}")
                
                # Ensure datetime column has the expected name - but only if it's not already correct
                if not resampled.empty and 'settlementdate' not in resampled.columns:
                    # Find the datetime column and rename it
                    datetime_cols = [col for col in resampled.columns if 'date' in col.lower() or col == resampled.columns[0]]
                    if datetime_cols:
                        resampled = resampled.rename(columns={datetime_cols[0]: 'settlementdate'})
                        logger.info(f"Renamed datetime column '{datetime_cols[0]}' to 'settlementdate'")
                
                # Remove any duplicate columns
                resampled = resampled.loc[:, ~resampled.columns.duplicated()]
                logger.info(f"Final columns after deduplication: {list(resampled.columns)}")
                
                logger.info(f"Resampled data columns: {list(resampled.columns)}")
                logger.info(f"Group columns found: {group_cols}")
                
            logger.info(f"Resampled shape: {resampled.shape}")
            return resampled
            
        except Exception as e:
            logger.error(f"Resampling failed: {e}")
            return df_filtered
    
    @classmethod
    def _sample_representative_days(cls, df: pd.DataFrame, 
                                   sample_rate: float = 0.1) -> pd.DataFrame:
        """Sample representative days preserving weekly patterns"""
        if df.empty:
            return df
            
        # Get unique dates
        dates = df.index.date
        unique_dates = pd.Series(dates).unique()
        
        # Sample dates, ensuring we get different days of week
        n_samples = max(1, int(len(unique_dates) * sample_rate))
        sampled_dates = np.random.choice(unique_dates, size=n_samples, replace=False)
        
        # Filter to sampled dates
        mask = pd.Series(dates).isin(sampled_dates)
        return df[mask.values]
    
    @classmethod
    def _sample_representative_months(cls, df: pd.DataFrame) -> pd.DataFrame:
        """Sample representative months for very long time series"""
        if df.empty:
            return df
            
        # Group by year-month and sample one week per month
        monthly_samples = []
        for (year, month), group in df.groupby([df.index.year, df.index.month]):
            # Take middle week of each month
            start_week = group.index.min() + timedelta(days=7)
            end_week = start_week + timedelta(days=7)
            week_data = group.loc[start_week:end_week]
            if not week_data.empty:
                monthly_samples.append(week_data)
        
        if monthly_samples:
            return pd.concat(monthly_samples)
        return df
    
    @classmethod
    def optimize_for_plotting(cls, df: pd.DataFrame, start_date: datetime, 
                             end_date: datetime, plot_type: str = 'generation') -> Tuple[pd.DataFrame, Dict[str, Any]]:
        """
        Main optimization method that applies appropriate strategy
        
        Args:
            df: Input DataFrame
            start_date: Start date
            end_date: End date
            plot_type: Type of plot being generated
            
        Returns:
            Tuple of (optimized_dataframe, optimization_metadata)
        """
        # Get optimization strategy
        strategy = cls.get_optimal_frequency(start_date, end_date, plot_type)
        
        # Apply optimization
        if strategy.get('special_handling'):
            # Handle special cases like time-of-day analysis
            optimized_df = cls.resample_generation_data(
                df, strategy['frequency'], start_date, end_date
            )
        else:
            # Standard resampling
            optimized_df = cls.resample_generation_data(
                df, strategy['frequency'], start_date, end_date
            )
        
        # Add metadata about optimization
        metadata = {
            **strategy,
            'original_points': len(df),
            'optimized_points': len(optimized_df),
            'reduction_ratio': len(optimized_df) / len(df) if len(df) > 0 else 1,
            'optimization_applied': True
        }
        
        logger.info(f"Performance optimization applied: {strategy['description']}")
        logger.info(f"Data points reduced from {metadata['original_points']:,} to {metadata['optimized_points']:,} "
                   f"({metadata['reduction_ratio']:.1%} of original)")
        
        return optimized_df, metadata