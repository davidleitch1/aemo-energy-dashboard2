"""
Data Resolution Manager for Adaptive Performance Optimization

Intelligently selects between 5-minute and 30-minute data based on:
- Date range duration
- Memory usage estimates  
- User preferences
- Data type characteristics
"""

import os
import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path
from typing import Tuple, Optional, Dict, Any
from ..shared.logging_config import get_logger

logger = get_logger(__name__)

class DataResolutionManager:
    """
    Central service for intelligent data resolution selection
    """
    
    # Performance thresholds based on data volume analysis
    PERFORMANCE_THRESHOLDS = {
        'memory_limit_mb': 512,        # Max memory for single dataset  
        'critical_days': 14,           # Force 30-min after 2 weeks
        'performance_days': 7,         # Recommend 30-min after 1 week
        'realtime_hours': 24,          # Always use 5-min for last 24h
        'max_records_5min': 1000000,   # Max 5-min records before switching
    }
    
    # Data type characteristics (bytes per record estimates)
    DATA_TYPE_SPECS = {
        'generation': {
            'record_size_bytes': 50,
            'typical_duids': 500,
            'file_patterns': {
                '5min': 'scada5.parquet',
                '30min': 'scada30.parquet'
            }
        },
        'price': {
            'record_size_bytes': 40, 
            'typical_regions': 5,
            'file_patterns': {
                '5min': 'prices5.parquet',
                '30min': 'prices30.parquet'
            }
        },
        'transmission': {
            'record_size_bytes': 60,
            'typical_interconnectors': 40,
            'file_patterns': {
                '5min': 'transmission5.parquet', 
                '30min': 'transmission30.parquet'
            }
        },
        'rooftop': {
            'record_size_bytes': 45,
            'typical_regions': 10,
            'file_patterns': {
                '5min': 'rooftop5.parquet',    # Would be converted
                '30min': 'rooftop30.parquet'   # Current source
            }
        }
    }
    
    def __init__(self):
        self.user_preferences = {}
        
    def get_optimal_resolution(
        self, 
        start_date: datetime, 
        end_date: datetime, 
        data_type: str, 
        user_preference: str = 'auto',
        region_filter: Optional[str] = None
    ) -> str:
        """
        Determine optimal data resolution based on performance analysis
        
        Args:
            start_date: Start of date range
            end_date: End of date range  
            data_type: 'generation', 'price', 'transmission', 'rooftop'
            user_preference: 'auto', '5min', '30min', 'performance'
            region_filter: Optional region filter (affects data volume)
            
        Returns:
            '5min' or '30min'
        """
        
        # Handle user preference overrides
        if user_preference == '5min':
            logger.info(f"User forced 5-minute resolution for {data_type}")
            return '5min'
        elif user_preference == '30min':
            logger.info(f"User forced 30-minute resolution for {data_type}")  
            return '30min'
        elif user_preference == 'performance':
            logger.info(f"User requested performance mode for {data_type}")
            return '30min'
            
        # Add detailed logging for date type debugging
        logger.info(f"get_optimal_resolution called - start: {start_date} (type: {type(start_date)}), end: {end_date} (type: {type(end_date)})")
        
        # Calculate date range characteristics with error handling
        try:
            duration = end_date - start_date
            duration_days = duration.total_seconds() / (24 * 3600)
        except TypeError as e:
            logger.error(f"Date type error: {e}. start_date type: {type(start_date)}, end_date type: {type(end_date)}")
            # Try to convert if needed
            if hasattr(start_date, 'date') and hasattr(end_date, 'date'):
                # Already datetime objects
                raise
            else:
                # Convert date to datetime
                from datetime import datetime as dt
                if not hasattr(start_date, 'hour'):
                    start_date = dt.combine(start_date, dt.min.time())
                if not hasattr(end_date, 'hour'):
                    end_date = dt.combine(end_date, dt.max.time())
                duration = end_date - start_date
                duration_days = duration.total_seconds() / (24 * 3600)
                logger.info(f"Converted dates and recalculated duration: {duration_days} days")
        
        # Check if we're querying recent data (always use 5-min for last 24h)
        now = datetime.now()
        try:
            hours_from_now = abs((now - end_date).total_seconds() / 3600)
        except TypeError as e:
            logger.error(f"Date comparison error: {e}")
            # Default to 30min for safety
            return '30min'
        if hours_from_now <= self.PERFORMANCE_THRESHOLDS['realtime_hours'] and duration_days <= 7:
            logger.info(f"Recent data range detected ({hours_from_now:.1f}h from now), using 5-minute resolution for {data_type}")
            return '5min'
            
        # Check critical threshold (force 30-min)
        if duration_days > self.PERFORMANCE_THRESHOLDS['critical_days']:
            logger.info(f"Long range ({duration_days:.1f} days) detected, using 30-minute resolution for {data_type}")
            return '30min'
            
        # Estimate memory usage for 5-minute data
        estimated_memory_mb = self.estimate_memory_usage(
            start_date, end_date, '5min', data_type, region_filter
        )
        
        # Check memory threshold
        if estimated_memory_mb > self.PERFORMANCE_THRESHOLDS['memory_limit_mb']:
            logger.info(f"Memory estimate ({estimated_memory_mb:.1f}MB) exceeds limit, using 30-minute resolution for {data_type}")
            return '30min'
            
        # Check performance recommendation threshold
        if duration_days > self.PERFORMANCE_THRESHOLDS['performance_days']:
            logger.info(f"Performance range ({duration_days:.1f} days) detected, recommending 30-minute resolution for {data_type}")
            return '30min'
            
        # Default to 5-minute for short ranges
        logger.info(f"Short range ({duration_days:.1f} days) detected, using 5-minute resolution for {data_type}")
        return '5min'
    
    def estimate_memory_usage(
        self,
        start_date: datetime,
        end_date: datetime, 
        resolution: str,
        data_type: str,
        region_filter: Optional[str] = None
    ) -> float:
        """
        Estimate memory usage in MB for given parameters
        
        Args:
            start_date, end_date: Date range
            resolution: '5min' or '30min'
            data_type: Type of data
            region_filter: Optional region filter
            
        Returns:
            Estimated memory usage in MB
        """
        
        if data_type not in self.DATA_TYPE_SPECS:
            logger.warning(f"Unknown data type: {data_type}, using default estimate")
            return 100.0  # Conservative default
            
        spec = self.DATA_TYPE_SPECS[data_type]
        
        # Calculate number of time intervals
        duration = end_date - start_date
        if resolution == '5min':
            intervals = duration.total_seconds() / (5 * 60)
        else:  # 30min
            intervals = duration.total_seconds() / (30 * 60)
            
        # Calculate number of entities (DUIDs, regions, etc.)
        if data_type == 'generation':
            entities = spec['typical_duids']
            if region_filter and region_filter != 'NEM':
                entities = entities // 5  # Rough regional split
        elif data_type == 'price':
            entities = spec['typical_regions']
            if region_filter and region_filter != 'NEM':
                entities = 1  # Single region
        elif data_type == 'transmission':
            entities = spec['typical_interconnectors']
            if region_filter and region_filter != 'NEM':
                entities = entities // 3  # Rough regional interconnectors
        else:  # rooftop
            entities = spec['typical_regions']
            if region_filter and region_filter != 'NEM':
                entities = 1
                
        # Calculate total memory
        total_records = intervals * entities
        memory_bytes = total_records * spec['record_size_bytes']
        memory_mb = memory_bytes / (1024 * 1024)
        
        # Add overhead for pandas DataFrame (roughly 2x)
        memory_mb *= 2.0
        
        logger.debug(f"Memory estimate for {data_type} ({resolution}): {memory_mb:.1f}MB "
                    f"({int(total_records):,} records)")
        
        return memory_mb
    
    def get_file_path(self, data_type: str, resolution: str, base_path: Optional[str] = None) -> str:
        """
        Get appropriate file path for data type and resolution
        
        Args:
            data_type: Type of data
            resolution: '5min' or '30min'  
            base_path: Optional base directory path
            
        Returns:
            Full file path
        """
        
        if data_type not in self.DATA_TYPE_SPECS:
            raise ValueError(f"Unknown data type: {data_type}")
            
        if resolution not in ['5min', '30min']:
            raise ValueError(f"Invalid resolution: {resolution}")
            
        pattern = self.DATA_TYPE_SPECS[data_type]['file_patterns'][resolution]
        
        if base_path:
            return str(Path(base_path) / pattern)
        else:
            # Point to aemo-data-updater directory where multi-resolution files are located
            # Use environment variable for data path, with fallback to default
            data_path_env = os.getenv('AEMO_DATA_PATH')
            if data_path_env:
                aemo_data_path = Path(data_path_env)
            else:
                # Default to production path
                aemo_data_path = Path("/Users/davidleitch/aemo_production/data")
            file_path = aemo_data_path / pattern
            
            # Check if file exists, otherwise fallback to legacy config paths
            if file_path.exists():
                logger.debug(f"Using multi-resolution file: {file_path}")
                return str(file_path)
            else:
                # Fallback to legacy config-based paths
                logger.warning(f"Multi-resolution file {file_path} not found, falling back to legacy config")
                from ..shared.config import config
                if data_type == 'generation':
                    return config.gen_output_file
                elif data_type == 'price':
                    return config.spot_hist_file
                elif data_type == 'transmission':
                    return config.transmission_output_file
                elif data_type == 'rooftop':
                    return config.rooftop_solar_file
                else:
                    raise ValueError(f"No fallback path for data type: {data_type}")
    
    def get_optimal_resolution_with_fallback(
        self,
        start_date: datetime,
        end_date: datetime,
        data_type: str,
        user_preference: str = 'auto'
    ) -> Dict[str, Any]:
        """
        Enhanced resolution selection with automatic fallback when data unavailable
        
        This method implements intelligent fallback to handle data collection gaps:
        1. Determines optimal resolution using standard logic
        2. Checks if data actually exists for that resolution
        3. Falls back to alternative resolution if primary unavailable
        4. Returns strategy for seamless data loading
        
        Args:
            start_date: Start of date range
            end_date: End of date range
            data_type: 'generation', 'price', 'transmission', 'rooftop'
            user_preference: 'auto', '5min', '30min', 'performance'
            
        Returns:
            Dictionary with:
            - primary_resolution: Optimal resolution
            - fallback_resolution: Alternative if primary fails
            - strategy: 'primary_only', 'fallback_only', or 'hybrid'
            - reasoning: Explanation of choices
        """
        
        # Get primary resolution recommendation
        primary_resolution = self.get_optimal_resolution(
            start_date, end_date, data_type, user_preference
        )
        
        # Determine fallback resolution
        fallback_resolution = '30min' if primary_resolution == '5min' else '5min'
        
        # Define data availability periods based on analysis
        data_availability = self._get_data_availability_periods(data_type)
        
        # Check if we need fallback for specific periods
        needs_fallback = self._check_needs_fallback(
            start_date, end_date, primary_resolution, data_availability
        )
        
        if needs_fallback:
            if primary_resolution == '5min':
                # 5-minute data missing, use 30-minute fallback
                strategy = 'hybrid'
                reasoning = f"Using hybrid strategy: 30min for periods where 5min unavailable, 5min for other periods"
                logger.info(f"Fallback needed: {reasoning}")
            else:
                # 30-minute primary has gaps, fall back to 5-minute where available
                strategy = 'hybrid'
                reasoning = f"Using hybrid strategy: 5min for periods where 30min has gaps, 30min for other periods"
                logger.info(f"Fallback needed: {reasoning}")
        else:
            strategy = 'primary_only'
            reasoning = f"Using {primary_resolution} resolution, full data availability"
        
        return {
            'primary_resolution': primary_resolution,
            'fallback_resolution': fallback_resolution,
            'strategy': strategy,
            'reasoning': reasoning,
            'data_availability': data_availability
        }
    
    def _get_data_availability_periods(self, data_type: str) -> Dict[str, Any]:
        """Get known data availability periods for different resolutions"""
        
        # Based on actual historical data analysis
        availability = {
            'generation': {
                '5min': {
                    'available_from': datetime(2024, 8, 1),  # When 5-minute SCADA became available
                    'gaps': []  # Assume minimal gaps for now
                },
                '30min': {
                    'available_from': datetime(2020, 2, 1),  # Historical data starts here
                    'gaps': []  # Assume minimal gaps for now
                }
            },
            'price': {
                '5min': {
                    'available_from': datetime(2024, 8, 1),  # When 5-minute price data became available
                    'gaps': []
                },
                '30min': {
                    'available_from': datetime(2020, 1, 1),  # Historical price data starts here
                    'gaps': []
                }
            }
        }
        
        return availability.get(data_type, {})
    
    def _check_needs_fallback(
        self, 
        start_date: datetime, 
        end_date: datetime, 
        primary_resolution: str,
        data_availability: Dict[str, Any]
    ) -> bool:
        """Check if fallback is needed for the requested date range"""
        
        if not data_availability or primary_resolution not in data_availability:
            return False
            
        primary_info = data_availability[primary_resolution]
        available_from = primary_info.get('available_from')
        
        # Check if request period overlaps with unavailable period
        if available_from and start_date < available_from:
            # Request includes period before data collection started
            return True
            
        # Check for gaps in requested period
        gaps = primary_info.get('gaps', [])
        for gap_start, gap_end in gaps:
            if (start_date <= gap_end and end_date >= gap_start):
                # Request overlaps with known gap
                return True
                
        return False

    def get_performance_recommendation(
        self,
        start_date: datetime,
        end_date: datetime,
        data_type: str
    ) -> Dict[str, Any]:
        """
        Get detailed performance recommendation with explanations
        
        Returns:
            Dictionary with recommendation details
        """
        
        duration = end_date - start_date
        duration_days = duration.total_seconds() / (24 * 3600)
        
        # Get memory estimates for both resolutions
        memory_5min = self.estimate_memory_usage(start_date, end_date, '5min', data_type)
        memory_30min = self.estimate_memory_usage(start_date, end_date, '30min', data_type)
        
        # Get optimal resolution
        optimal = self.get_optimal_resolution(start_date, end_date, data_type)
        
        recommendation = {
            'optimal_resolution': optimal,
            'duration_days': duration_days,
            'memory_estimates': {
                '5min': memory_5min,
                '30min': memory_30min
            },
            'performance_factors': [],
            'explanation': '',
            'load_time_estimate': self._estimate_load_time(memory_5min if optimal == '5min' else memory_30min)
        }
        
        # Build explanation based on factors
        factors = []
        
        if duration_days <= 1:
            factors.append("Short time range - high resolution recommended")
        elif duration_days <= 7:
            factors.append("Medium time range - balanced resolution")
        else:
            factors.append("Long time range - performance resolution recommended")
            
        if memory_5min > self.PERFORMANCE_THRESHOLDS['memory_limit_mb']:
            factors.append(f"High memory usage ({memory_5min:.0f}MB) - lower resolution beneficial")
            
        recommendation['performance_factors'] = factors
        recommendation['explanation'] = self._build_explanation(optimal, factors)
        
        return recommendation
    
    def _estimate_load_time(self, memory_mb: float) -> float:
        """
        Estimate load time based on memory usage
        (Rough estimate: 100MB = 1 second)
        """
        return max(0.5, memory_mb / 100.0)
    
    def _build_explanation(self, resolution: str, factors: list) -> str:
        """Build human-readable explanation"""
        
        if resolution == '5min':
            base = "High resolution (5-minute) data recommended for detailed analysis."
        else:
            base = "Performance resolution (30-minute) data recommended for faster loading."
            
        if factors:
            base += f" Factors: {'; '.join(factors)}."
            
        return base
    
    def set_user_preference(self, preference: str, data_type: str = 'all'):
        """Set user preference for resolution selection"""
        if data_type == 'all':
            self.user_preferences['default'] = preference
        else:
            self.user_preferences[data_type] = preference
            
        logger.info(f"Set user preference for {data_type}: {preference}")
    
    def get_user_preference(self, data_type: str) -> str:
        """Get user preference for data type"""
        return self.user_preferences.get(data_type, 
                                       self.user_preferences.get('default', 'auto'))

# Global instance for use across dashboard
resolution_manager = DataResolutionManager()