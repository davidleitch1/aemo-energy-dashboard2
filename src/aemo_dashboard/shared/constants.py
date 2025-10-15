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
