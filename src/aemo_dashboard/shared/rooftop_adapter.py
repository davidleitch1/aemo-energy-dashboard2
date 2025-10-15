"""
Rooftop solar data adapter
Converts 30-minute long format data to 5-minute wide format with Henderson smoothing
"""

import pandas as pd
import numpy as np
from pathlib import Path
import os
from datetime import timedelta
from .fuel_categories import MAIN_ROOFTOP_REGIONS

# Henderson 7-term filter weights
HENDERSON_7 = np.array([-0.059, 0.059, 0.294, 0.412, 0.294, 0.059, -0.059])

def henderson_smooth(data, weights=HENDERSON_7):
    """
    Apply Henderson filter to smooth data
    
    Args:
        data: 1D array of values to smooth
        weights: Henderson filter weights (default 7-term)
    
    Returns:
        Smoothed data array
    """
    n = len(weights)
    half_n = n // 2
    
    # Handle NaN values
    valid_mask = ~np.isnan(data)
    if not valid_mask.any():
        return data
    
    # Pad data to handle edges
    # Use linear extrapolation for padding instead of edge repeat
    first_valid = np.where(valid_mask)[0][0]
    last_valid = np.where(valid_mask)[0][-1]
    
    # Pad with linear extrapolation
    padded = np.pad(data, (half_n, half_n), mode='edge')
    
    # Apply convolution
    smoothed = np.convolve(padded, weights, mode='valid')
    
    # Ensure non-negative values for solar data
    smoothed = np.maximum(smoothed, 0)
    
    return smoothed

def interpolate_and_smooth(series_30min, target_index):
    """
    Interpolate 30-minute data to 5-minute and apply Henderson smoothing
    
    Args:
        series_30min: Pandas Series with 30-minute data
        target_index: DatetimeIndex with 5-minute frequency
    
    Returns:
        Smoothed 5-minute series
    """
    # First, do linear interpolation
    series_5min = series_30min.reindex(target_index).interpolate(method='linear')
    
    # Fill any remaining NaN values at the edges
    series_5min = series_5min.fillna(method='ffill').fillna(method='bfill').fillna(0)
    
    # Apply Henderson smoothing
    values = series_5min.values
    smoothed = henderson_smooth(values)
    
    return pd.Series(smoothed, index=target_index)

def handle_future_projection(df_5min, last_30min_time, decay_factor=0.985):
    """
    Handle projection beyond last 30-minute observation
    
    For solar data, we apply a decay factor since generation typically
    decreases after the last observation (usually in the evening)
    
    Args:
        df_5min: DataFrame with 5-minute data
        last_30min_time: Last timestamp in 30-minute data
        decay_factor: Factor to decay values (default 0.985 = 1.5% per interval)
    
    Returns:
        DataFrame with projected values
    """
    future_mask = df_5min.index > last_30min_time
    
    if future_mask.any():
        # Get the last known values
        last_idx = df_5min.index[~future_mask][-1]
        last_values = df_5min.loc[last_idx]
        
        # Apply decay for future timestamps (up to 25 minutes)
        future_indices = df_5min.index[future_mask][:5]  # Max 5 intervals
        
        for i, idx in enumerate(future_indices):
            # Apply exponential decay
            df_5min.loc[idx] = last_values * (decay_factor ** (i + 1))
    
    return df_5min

def load_rooftop_data(
    start_date=None,
    end_date=None, 
    file_path=None,
    resolution='5min'  # For compatibility, rooftop is always converted to 5min
):
    """
    Load rooftop solar data with automatic format adaptation
    
    Converts from 30-minute long format to 5-minute wide format
    matching the old dashboard structure
    
    Args:
        start_date: Start date for filtering (optional)
        end_date: End date for filtering (optional) 
        file_path: Path to rooftop parquet file (uses config if not provided)
        resolution: For compatibility (rooftop always converts to 5min)
    
    Returns:
        DataFrame in wide format with 5-minute data
    """
    if file_path is None:
        from ..shared.config import config
        file_path = config.rooftop_solar_file
    
    df = pd.read_parquet(file_path)
    
    # Apply date filtering if provided
    if start_date is not None or end_date is not None:
        # Ensure datetime type
        if not pd.api.types.is_datetime64_any_dtype(df['settlementdate']):
            df['settlementdate'] = pd.to_datetime(df['settlementdate'])
        
        if start_date is not None:
            df = df[df['settlementdate'] >= start_date]
        if end_date is not None:
            df = df[df['settlementdate'] <= end_date]
    
    # Check if this is the new long format
    if 'regionid' in df.columns:
        print(f"Converting rooftop data from 30-min long to 5-min wide format...")

        # Convert to wide format first
        df_wide = df.pivot(
            index='settlementdate',
            columns='regionid',
            values='power'
        )

        # Filter to main regions only (exclude sub-regions to avoid double-counting)
        # This prevents Queensland (QLDN+QLDS+QLDC) and Tasmania (TASN+TASS) from being counted twice
        available_regions = [r for r in MAIN_ROOFTOP_REGIONS if r in df_wide.columns]
        if len(available_regions) < len(MAIN_ROOFTOP_REGIONS):
            missing = set(MAIN_ROOFTOP_REGIONS) - set(available_regions)
            print(f"⚠️  Warning: Missing main rooftop regions: {missing}")

        # Keep only the 5 main regions
        df_wide = df_wide[available_regions]
        print(f"✅ Filtered to {len(available_regions)} main regions (excluded sub-regions)")
        print(f"   Main regions: {available_regions}")

        # Create 5-minute index spanning the data range
        start_time = df_wide.index.min()
        end_time = df_wide.index.max()
        
        # Extend end time by 25 minutes to handle SCADA lead
        end_time_extended = end_time + timedelta(minutes=25)
        
        index_5min = pd.date_range(
            start=start_time,
            end=end_time_extended,
            freq='5min'
        )
        
        # Convert each region to 5-minute with smoothing
        result_data = {}
        
        for region in df_wide.columns:
            # Get 30-minute series for this region
            series_30min = df_wide[region]
            
            # Interpolate and smooth
            series_5min = interpolate_and_smooth(series_30min, index_5min)
            
            result_data[region] = series_5min
        
        # Create result DataFrame
        df_5min = pd.DataFrame(result_data)
        
        # Handle future projections
        df_5min = handle_future_projection(df_5min, end_time)
        
        # Reset index to match old format (settlementdate as column)
        df_5min = df_5min.reset_index()
        df_5min = df_5min.rename(columns={'index': 'settlementdate'})
        
        print(f"✅ Converted {len(df_wide)} 30-min records to {len(df_5min)} 5-min records")
        print(f"   Regions: {list(df_5min.columns[1:])}")
        
        return df_5min
    
    else:
        # Already in correct format (old wide format)
        return df

def get_rooftop_at_time(df_rooftop, target_time, region=None):
    """
    Get rooftop solar value at specific time
    
    Args:
        df_rooftop: Rooftop DataFrame from load_rooftop_data()
        target_time: Timestamp to query
        region: Optional region code (returns all if None)
    
    Returns:
        Series of values by region or single value
    """
    # Find closest time (within 5 minutes)
    time_diff = abs(df_rooftop['settlementdate'] - target_time)
    closest_idx = time_diff.idxmin()
    
    if time_diff[closest_idx] > timedelta(minutes=5):
        # No data within 5 minutes
        if region:
            return 0.0
        else:
            return pd.Series(0.0, index=df_rooftop.columns[1:])
    
    row = df_rooftop.iloc[closest_idx]
    
    if region:
        return row.get(region, 0.0)
    else:
        return row.drop('settlementdate')

# Backward compatibility function
def smooth_rooftop_data(df_rooftop):
    """
    Legacy function for compatibility
    The new load_rooftop_data already returns smoothed data
    """
    return df_rooftop