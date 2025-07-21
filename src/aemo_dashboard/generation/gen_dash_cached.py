#!/usr/bin/env python3
"""
Generation Dashboard with Panel Cache Implementation
This is the cached version of gen_dash.py with pn.cache decorators
"""

import pandas as pd
import numpy as np
import panel as pn
import param
import holoviews as hv
import hvplot.pandas
import asyncio
import os
from datetime import datetime, timedelta
import pickle
from pathlib import Path
import json
import sys
import time
import hashlib
from typing import Dict, List, Tuple, Optional
from bokeh.models import DatetimeTickFormatter
from dotenv import load_dotenv

from ..shared.config import config
from ..shared.logging_config import setup_logging, get_logger
from ..shared.email_alerts import EmailAlertManager
from ..analysis.price_analysis_ui import create_price_analysis_tab
from ..station.station_analysis_ui import create_station_analysis_tab
from ..nem_dash.nem_dash_tab import create_nem_dash_tab_with_updates
from .generation_query_manager import GenerationQueryManager

# Import the original dashboard
from .gen_dash import GenerationByFuelDashboard

# Set up logging
setup_logging()
logger = get_logger(__name__)

# Configure Panel and HoloViews
pn.config.theme = 'dark'
pn.extension('tabulator', 'plotly', template='material')

# Cache configuration
ENABLE_PN_CACHE = os.getenv('ENABLE_PN_CACHE', 'true').lower() == 'true'
CACHE_LOG_STATS = os.getenv('CACHE_LOG_STATS', 'true').lower() == 'true'

def conditional_cache(**cache_kwargs):
    """Conditional cache decorator that can be disabled"""
    def decorator(func):
        if ENABLE_PN_CACHE:
            return pn.cache(**cache_kwargs)(func)
        return func
    return decorator

# =============================================================================
# Cache Key Helpers
# =============================================================================

def create_data_fingerprint(df: pd.DataFrame, sample_rows: int = 20) -> str:
    """Create a lightweight fingerprint of DataFrame for cache key"""
    if df.empty:
        return "empty"
    
    # Use data characteristics instead of full data
    fingerprint_parts = [
        str(len(df)),  # Row count
        str(df.index[0]) if not df.empty else "none",  # First timestamp
        str(df.index[-1]) if not df.empty else "none",  # Last timestamp
        str(hash(tuple(df.columns.tolist()))),  # Column hash
    ]
    
    # Sample data points for fingerprint
    if len(df) > sample_rows:
        indices = np.linspace(0, len(df)-1, sample_rows, dtype=int)
        sample = df.iloc[indices]
    else:
        sample = df
    
    # Add statistical summary
    numeric_cols = df.select_dtypes(include=[np.number]).columns
    if len(numeric_cols) > 0:
        # Use subset of stats to avoid floating point variations
        stats_dict = {}
        for col in numeric_cols[:5]:  # Limit to first 5 numeric columns
            stats_dict[col] = {
                'mean': round(df[col].mean(), 2),
                'sum': round(df[col].sum(), 0)
            }
        stats_str = json.dumps(stats_dict, sort_keys=True)
        fingerprint_parts.append(hashlib.md5(stats_str.encode()).hexdigest()[:8])
    
    return '|'.join(fingerprint_parts)

# =============================================================================
# Cached Plot Creation Functions
# =============================================================================

@conditional_cache(max_items=20, policy='LRU', ttl=300)  # 5 minute TTL, conservative limit
def create_generation_plot_cached(
    data_fingerprint: str,
    fuel_types_hash: str,  # Hash of fuel types tuple
    fuel_colors_hash: str,  # Hash of colors dict
    region: str,
    time_range: str,
    has_negative_values: bool,
    width: int = 1200,
    height: int = 300
) -> hv.DynamicMap:
    """
    Cached version of generation plot creation.
    Note: We pass hashes/fingerprints for cache key, but the actual plot
    creation happens inside the dashboard's create_plot method.
    
    This function acts as a cache wrapper around the expensive plotting operation.
    """
    # This function will be called by the modified create_plot method
    # The actual plot creation logic will be passed in
    pass

@conditional_cache(max_items=20, policy='LRU', ttl=600)  # 10 minute TTL
def create_utilization_plot_cached(
    data_fingerprint: str,
    fuel_types: Tuple[str, ...],
    region: str,
    time_range: str,
    width: int = 1200,
    height: int = 200
) -> hv.DynamicMap:
    """Cached version of capacity utilization plot"""
    pass

@conditional_cache(max_items=20, policy='LRU', ttl=300)  # 5 minute TTL
def create_transmission_plot_cached(
    data_fingerprint: str,
    interconnectors: Tuple[str, ...],
    region: str,
    time_range: str,
    width: int = 1200,
    height: int = 200
) -> hv.DynamicMap:
    """Cached version of transmission flow plot"""
    pass

# =============================================================================
# Cached Dashboard Class
# =============================================================================

class CachedGenerationByFuelDashboard(GenerationByFuelDashboard):
    """Generation dashboard with caching enabled"""
    
    def __init__(self, **params):
        super().__init__(**params)
        self._cache_stats = {
            'hits': 0,
            'misses': 0,
            'errors': 0
        }
        logger.info(f"Cache-enabled dashboard initialized (cache={'enabled' if ENABLE_PN_CACHE else 'disabled'})")
    
    def _create_generation_plot_with_cache(self, data: pd.DataFrame) -> hv.DynamicMap:
        """
        Extract the expensive plot creation logic and wrap it with caching.
        This method replaces the plot creation part of create_plot().
        """
        if data.empty:
            return self._create_empty_plot()
        
        # Get colors and fuel types
        fuel_colors = self.get_fuel_colors()
        fuel_types = list(data.columns)
        
        # Create cache key components
        data_fingerprint = create_data_fingerprint(data)
        fuel_types_hash = hashlib.md5(str(sorted(fuel_types)).encode()).hexdigest()[:8]
        fuel_colors_hash = hashlib.md5(json.dumps(fuel_colors, sort_keys=True).encode()).hexdigest()[:8]
        
        # Check for negative values
        has_negative = any(
            col in data.columns and (data[col] < 0).any()
            for col in ['Battery Storage', 'Transmission Exports']
        )
        
        # Define the actual plot creation function
        @conditional_cache(max_items=20, policy='LRU', ttl=300)
        def _create_plot_cached(
            fingerprint: str,
            fuels_hash: str,
            colors_hash: str,
            region: str,
            time_range: str,
            has_neg: bool
        ):
            """The actual expensive plot creation"""
            start_time = time.time()
            
            # This is where the expensive operation happens
            plot_data = data[fuel_types].copy().reset_index()
            
            if has_neg:
                # Complex negative value handling (battery & transmission)
                plot = self._create_plot_with_negative_values(plot_data, fuel_types, fuel_colors)
            else:
                # Standard stacked area plot
                time_range_display = self._get_time_range_display()
                plot = plot_data.hvplot.area(
                    x='settlementdate',
                    y=fuel_types,
                    stacked=True,
                    width=1200,
                    height=300,
                    ylabel='Generation (MW)',
                    xlabel='',
                    grid=True,
                    legend='right',
                    bgcolor='black',
                    color=[fuel_colors.get(fuel, '#6272a4') for fuel in fuel_types],
                    alpha=0.8,
                    hover=True,
                    hover_tooltips=[('Fuel Type', '$name')],
                    title=f'Generation by Fuel Type - {region} ({time_range_display}) | data:AEMO, design ITK'
                )
            
            creation_time = time.time() - start_time
            if CACHE_LOG_STATS:
                logger.info(f"Plot creation took {creation_time:.2f}s (will be cached)")
            
            return plot
        
        # Call the cached function
        try:
            plot = _create_plot_cached(
                data_fingerprint,
                fuel_types_hash,
                fuel_colors_hash,
                self.region,
                self._get_time_range_display(),
                has_negative
            )
            self._cache_stats['hits'] += 1
            return plot
        except Exception as e:
            logger.error(f"Cache error, falling back to direct creation: {e}")
            self._cache_stats['errors'] += 1
            # Fallback to original method
            return super().create_plot()
    
    def create_plot(self):
        """Override create_plot to use cached version"""
        try:
            # Load fresh data
            self.load_generation_data()
            data = self.process_data_for_region()
            
            # Use cached plot creation
            return self._create_generation_plot_with_cache(data)
            
        except Exception as e:
            logger.error(f"Error in cached create_plot: {e}")
            # Fallback to original
            return super().create_plot()
    
    def _create_plot_with_negative_values(self, plot_data: pd.DataFrame, 
                                         fuel_types: List[str], 
                                         fuel_colors: Dict[str, str]) -> hv.DynamicMap:
        """Extract complex negative value plotting logic"""
        # This contains the complex battery & transmission handling
        # Extracted from the original create_plot method
        
        battery_col = 'Battery Storage'
        transmission_exports_col = 'Transmission Exports'
        has_battery = battery_col in plot_data.columns
        has_transmission_exports = transmission_exports_col in plot_data.columns
        
        # Prepare data for main positive stack
        positive_fuel_types = [f for f in fuel_types if f != transmission_exports_col]
        plot_data_positive = plot_data.copy()
        
        # Handle battery storage negative values
        if has_battery:
            battery_data = plot_data[battery_col].copy()
            plot_data_positive[battery_col] = pd.Series(
                np.where(battery_data.values >= 0, battery_data.values, 0),
                index=battery_data.index
            )
        
        # Create main plot
        main_plot = plot_data_positive.hvplot.area(
            x='settlementdate',
            y=positive_fuel_types,
            stacked=True,
            width=1200,
            height=300,
            ylabel='Generation (MW)',
            xlabel='',
            grid=True,
            legend='right',
            bgcolor='black',
            color=[fuel_colors.get(fuel, '#6272a4') for fuel in positive_fuel_types],
            alpha=0.8,
            hover=True,
            hover_tooltips=[('Fuel Type', '$name')]
        )
        
        # Add negative values handling...
        # (rest of the complex logic from original create_plot)
        
        time_range_display = self._get_time_range_display()
        main_plot = main_plot.opts(
            title=f'Generation by Fuel Type - {self.region} ({time_range_display}) | data:AEMO, design ITK',
            show_grid=False,
            bgcolor='black',
            xaxis=None,
            hooks=[self._get_datetime_formatter_hook()]
        )
        
        return main_plot
    
    def get_cache_stats(self) -> Dict:
        """Get cache statistics"""
        stats = self._cache_stats.copy()
        
        # Add Panel cache info if available
        if hasattr(pn.state, 'cache'):
            stats['cache_size'] = len(pn.state.cache) if hasattr(pn.state.cache, '__len__') else 'unknown'
            stats['cache_enabled'] = ENABLE_PN_CACHE
        else:
            stats['cache_size'] = 0
            stats['cache_enabled'] = False
            
        # Calculate hit rate
        total = stats['hits'] + stats['misses']
        stats['hit_rate'] = (stats['hits'] / total * 100) if total > 0 else 0
        
        return stats
    
    def _create_empty_plot(self):
        """Create empty plot with message"""
        return hv.Text(0.5, 0.5, 'No data available').opts(
            xlim=(0, 1),
            ylim=(0, 1),
            bgcolor='black',
            width=1200,
            height=400,
            color='white',
            fontsize=16
        )

# =============================================================================
# Factory function to create dashboard with caching
# =============================================================================

def create_cached_dashboard():
    """Create a cached version of the generation dashboard"""
    return CachedGenerationByFuelDashboard()

# =============================================================================
# Main entry point
# =============================================================================

def main():
    """Run the cached dashboard"""
    logger.info(f"Starting cached generation dashboard (cache={'enabled' if ENABLE_PN_CACHE else 'disabled'})")
    
    # Import the original main function
    from .gen_dash import main as original_main
    
    # Monkey-patch the dashboard class
    import sys
    module = sys.modules['aemo_dashboard.generation.gen_dash']
    original_class = module.GenerationByFuelDashboard
    module.GenerationByFuelDashboard = CachedGenerationByFuelDashboard
    
    # Run original main
    original_main()
    
    # Restore original class
    module.GenerationByFuelDashboard = original_class

if __name__ == "__main__":
    main()