#!/usr/bin/env python3
"""
Example implementation of pn.cache for the generation dashboard
This shows how to refactor the expensive plot creation with caching
"""

import panel as pn
import pandas as pd
import numpy as np
import hashlib
import json
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional

# Enable Panel extensions
pn.extension('tabulator')

# Configuration for caching
ENABLE_PN_CACHE = True  # Can be controlled via environment variable

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
        str(df.index[0]),  # First timestamp
        str(df.index[-1]),  # Last timestamp
        str(hash(tuple(df.columns))),  # Column hash
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
        stats = df[numeric_cols].agg(['mean', 'std', 'sum']).to_json()
        fingerprint_parts.append(hashlib.md5(stats.encode()).hexdigest()[:8])
    
    return '|'.join(fingerprint_parts)


# =============================================================================
# Cached Plot Creation Functions
# =============================================================================

@conditional_cache(max_items=50, policy='LRU', ttl=300)  # 5 minute TTL
def create_generation_plot_cached(
    data_fingerprint: str,
    data_json: str,
    fuel_types: Tuple[str, ...],
    fuel_colors_json: str,
    region: str,
    time_range: str,
    width: int = 1200,
    height: int = 300
):
    """
    Cached version of generation plot creation.
    
    Args:
        data_fingerprint: Lightweight fingerprint of the data
        data_json: JSON serialized DataFrame (only used if cache miss)
        fuel_types: Tuple of fuel types to plot
        fuel_colors_json: JSON serialized color mapping
        region: Region name
        time_range: Time range description
        width: Plot width
        height: Plot height
    
    Returns:
        HvPlot area chart
    """
    import hvplot.pandas
    
    # Deserialize data (only happens on cache miss)
    data = pd.read_json(data_json)
    data['settlementdate'] = pd.to_datetime(data['settlementdate'])
    data.set_index('settlementdate', inplace=True)
    
    # Deserialize colors
    fuel_colors = json.loads(fuel_colors_json)
    
    # Create the plot (expensive operation)
    plot = data[list(fuel_types)].hvplot.area(
        x='settlementdate',
        y=list(fuel_types),
        stacked=True,
        width=width,
        height=height,
        ylabel='Generation (MW)',
        xlabel='',
        grid=True,
        legend='right',
        bgcolor='black',
        color=[fuel_colors.get(fuel, '#6272a4') for fuel in fuel_types],
        alpha=0.8,
        hover=True,
        hover_tooltips=[('Fuel Type', '$name')],
        title=f'Generation by Fuel Type - {region} ({time_range})'
    )
    
    return plot


@conditional_cache(max_items=30, policy='LRU', ttl=600)  # 10 minute TTL
def create_capacity_utilization_cached(
    data_fingerprint: str,
    utilization_json: str,
    region: str,
    time_range: str,
    width: int = 1200,
    height: int = 200
):
    """Cached version of capacity utilization plot"""
    import hvplot.pandas
    
    # Deserialize data
    utilization_data = pd.read_json(utilization_json)
    utilization_data['settlementdate'] = pd.to_datetime(utilization_data['settlementdate'])
    utilization_data.set_index('settlementdate', inplace=True)
    
    # Create line plot
    plot = utilization_data.hvplot.line(
        y=['Coal', 'CCGT', 'Solar', 'Wind'],
        width=width,
        height=height,
        ylabel='Capacity Utilization (%)',
        xlabel='',
        grid=True,
        legend='right',
        bgcolor='black',
        alpha=0.8,
        hover=True,
        title=f'Capacity Utilization - {region} ({time_range})'
    )
    
    return plot


@conditional_cache(max_items=30, policy='LRU', ttl=300)  # 5 minute TTL
def create_transmission_plot_cached(
    data_fingerprint: str,
    flow_data_json: str,
    limit_data_json: str,
    region: str,
    time_range: str,
    width: int = 1200,
    height: int = 200
):
    """Cached version of transmission flow plot"""
    import hvplot.pandas
    import holoviews as hv
    
    # Deserialize data
    flow_data = pd.read_json(flow_data_json)
    flow_data['settlementdate'] = pd.to_datetime(flow_data['settlementdate'])
    
    limit_data = json.loads(limit_data_json)
    
    # Create complex transmission visualization
    # ... (implementation details)
    
    return plot


# =============================================================================
# Refactored Dashboard Class Methods
# =============================================================================

class GenerationDashboardCached:
    """Example of how to integrate caching into the existing dashboard"""
    
    def create_plot(self):
        """Refactored plot creation using cache"""
        try:
            # Load fresh data (fast with DuckDB)
            self.load_generation_data()
            data = self.process_data_for_region()
            
            if data.empty:
                return self._create_empty_plot()
            
            # Prepare cache-friendly parameters
            data_fingerprint = create_data_fingerprint(data)
            data_json = data.reset_index().to_json(date_format='iso')
            fuel_types = tuple(data.columns)  # Convert to hashable tuple
            fuel_colors_json = json.dumps(self.get_fuel_colors())
            
            # Call cached function
            plot = create_generation_plot_cached(
                data_fingerprint=data_fingerprint,
                data_json=data_json,
                fuel_types=fuel_types,
                fuel_colors_json=fuel_colors_json,
                region=self.region_selector.value,
                time_range=self._get_time_range_display(),
                width=1200,
                height=300
            )
            
            return plot
            
        except Exception as e:
            logger.error(f"Error creating plot: {e}")
            return self._create_error_plot(str(e))
    
    def create_utilization_plot(self):
        """Refactored utilization plot using cache"""
        try:
            utilization_data = self.calculate_capacity_utilization()
            
            if utilization_data.empty:
                return self._create_empty_plot()
            
            # Prepare for caching
            data_fingerprint = create_data_fingerprint(utilization_data)
            utilization_json = utilization_data.reset_index().to_json(date_format='iso')
            
            # Call cached function
            plot = create_capacity_utilization_cached(
                data_fingerprint=data_fingerprint,
                utilization_json=utilization_json,
                region=self.region_selector.value,
                time_range=self._get_time_range_display(),
                width=1200,
                height=200
            )
            
            return plot
            
        except Exception as e:
            logger.error(f"Error creating utilization plot: {e}")
            return self._create_error_plot(str(e))


# =============================================================================
# Cache Monitoring and Management
# =============================================================================

def get_cache_stats() -> Dict:
    """Get current cache statistics"""
    if hasattr(pn.state, 'cache'):
        cache = pn.state.cache
        return {
            'enabled': ENABLE_PN_CACHE,
            'size': len(cache) if hasattr(cache, '__len__') else 'unknown',
            'policy': getattr(cache, 'policy', 'unknown'),
            'max_items': getattr(cache, 'max_items', 'unknown')
        }
    return {'enabled': ENABLE_PN_CACHE, 'size': 0}


def clear_cache():
    """Clear all cached items"""
    if hasattr(pn.state, 'cache'):
        pn.state.cache.clear()
        print("Cache cleared")


# =============================================================================
# Testing the Cache
# =============================================================================

if __name__ == "__main__":
    import time
    
    # Create sample data
    dates = pd.date_range(start='2024-01-01', end='2024-01-02', freq='5min')
    sample_data = pd.DataFrame({
        'settlementdate': dates,
        'Coal': np.random.uniform(1000, 2000, len(dates)),
        'Solar': np.random.uniform(0, 1000, len(dates)),
        'Wind': np.random.uniform(500, 1500, len(dates))
    })
    
    # Test cache performance
    print("Testing cache performance...")
    
    # First call - cache miss
    start = time.time()
    plot1 = create_generation_plot_cached(
        data_fingerprint=create_data_fingerprint(sample_data),
        data_json=sample_data.to_json(date_format='iso'),
        fuel_types=('Coal', 'Solar', 'Wind'),
        fuel_colors_json=json.dumps({'Coal': '#4a4a4a', 'Solar': '#ffd700', 'Wind': '#00ff7f'}),
        region='NSW1',
        time_range='Last 24 hours',
        width=1200,
        height=300
    )
    time1 = time.time() - start
    print(f"First call (cache miss): {time1:.3f} seconds")
    
    # Second call - cache hit
    start = time.time()
    plot2 = create_generation_plot_cached(
        data_fingerprint=create_data_fingerprint(sample_data),
        data_json=sample_data.to_json(date_format='iso'),
        fuel_types=('Coal', 'Solar', 'Wind'),
        fuel_colors_json=json.dumps({'Coal': '#4a4a4a', 'Solar': '#ffd700', 'Wind': '#00ff7f'}),
        region='NSW1',
        time_range='Last 24 hours',
        width=1200,
        height=300
    )
    time2 = time.time() - start
    print(f"Second call (cache hit): {time2:.3f} seconds")
    print(f"Speedup: {time1/time2:.1f}x")
    
    # Show cache stats
    print("\nCache stats:", get_cache_stats())