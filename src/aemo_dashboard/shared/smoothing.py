"""
Smoothing utilities for time series data in the AEMO dashboard.
"""
import pandas as pd
import numpy as np
from typing import Union, Optional


def apply_ewm_smoothing(
    data: pd.Series, 
    span: int = 30,
    min_periods: Optional[int] = None
) -> pd.Series:
    """
    Apply Exponential Weighted Moving average smoothing to a time series.
    
    Parameters
    ----------
    data : pd.Series
        Time series data to smooth
    span : int, default=30
        Span for the EWM calculation (equivalent to window size)
    min_periods : int, optional
        Minimum number of observations required to have a value
        
    Returns
    -------
    pd.Series
        Smoothed time series
    """
    if min_periods is None:
        min_periods = span // 2
        
    return data.ewm(span=span, min_periods=min_periods, adjust=False).mean()


def apply_centered_ma(
    data: pd.Series,
    window: int = 30,
    min_periods: Optional[int] = None
) -> pd.Series:
    """
    Apply centered moving average smoothing.
    
    Parameters
    ----------
    data : pd.Series
        Time series data to smooth
    window : int, default=30
        Window size for the moving average
    min_periods : int, optional
        Minimum number of observations required
        
    Returns
    -------
    pd.Series
        Smoothed time series
    """
    if min_periods is None:
        min_periods = window // 2
        
    return data.rolling(window=window, center=True, min_periods=min_periods).mean()