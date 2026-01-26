"""
Constants for AEMO Energy Dashboard

This module defines standard constants used throughout the dashboard,
particularly for revenue calculations and time conversions.
"""

# Time conversion factors for revenue calculations
# Revenue = Power (MW) × Price ($/MWh) × Time (hours)

# 5-minute interval to hours
MINUTES_5_TO_HOURS = 5.0 / 60.0  # 0.0833... hours

# 30-minute interval to hours
MINUTES_30_TO_HOURS = 0.5  # 0.5 hours

# Intervals per hour for each resolution
INTERVALS_PER_HOUR_5MIN = 12   # 12 × 5-minute intervals = 60 minutes
INTERVALS_PER_HOUR_30MIN = 2   # 2 × 30-minute intervals = 60 minutes

# Derived constants for energy calculations
# Energy (MWh) = Power (MW) × Time (hours)
ENERGY_FACTOR_5MIN = MINUTES_5_TO_HOURS    # MW × 0.0833 = MWh per 5-min interval
ENERGY_FACTOR_30MIN = MINUTES_30_TO_HOURS  # MW × 0.5 = MWh per 30-min interval

# Resolution names
RESOLUTION_5MIN = '5min'
RESOLUTION_30MIN = '30min'

# =============================================================================
# Standard Time Range Options for Date Selectors
# =============================================================================
# Use these constants to ensure consistency across all dashboard tabs.
# Values represent number of days (as strings) for compatibility with param.Selector

STANDARD_TIME_RANGES = ['1', '7', '30', '90', '365', 'All']

# Human-readable labels for time range options
STANDARD_TIME_RANGE_LABELS = {
    '1': '1 day',
    '7': '7 days',
    '30': '30 days',
    '90': '90 days',
    '365': '1 year',
    'All': 'All data'
}

# Default time range for different contexts
DEFAULT_TIME_RANGE_REALTIME = '1'    # For real-time views (Today tab)
DEFAULT_TIME_RANGE_ANALYSIS = '7'    # For analysis views
DEFAULT_TIME_RANGE_TRENDS = '30'     # For trend views
DEFAULT_TIME_RANGE_HISTORICAL = '365'  # For historical views

# Example usage in tabs:
#
# from aemo_dashboard.shared.constants import STANDARD_TIME_RANGES, DEFAULT_TIME_RANGE_ANALYSIS
#
# class MyTab(param.Parameterized):
#     time_range = param.Selector(
#         default=DEFAULT_TIME_RANGE_ANALYSIS,
#         objects=STANDARD_TIME_RANGES
#     )
#

# Example usage in revenue calculations:
#
# For 5-minute data:
#   revenue_5min = power_mw * price_per_mwh * MINUTES_5_TO_HOURS
#   # OR equivalently:
#   revenue_5min = power_mw * price_per_mwh * 0.0833
#
# For 30-minute data:
#   revenue_30min = power_mw * price_per_mwh * MINUTES_30_TO_HOURS
#   # OR equivalently:
#   revenue_30min = power_mw * price_per_mwh * 0.5
#
# IMPORTANT: Never use division (/ 2) for revenue calculations.
# Always use multiplication with the appropriate time factor.
