"""
Station Analysis Module

This module provides individual station/DUID analysis capabilities including:
- Station search with fuzzy matching
- Time series performance charts  
- Time-of-day analysis
- Performance statistics and rankings
"""

from .station_analysis_ui import create_station_analysis_tab

__all__ = ['create_station_analysis_tab']