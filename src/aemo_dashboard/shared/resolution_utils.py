"""
Resolution Detection Utilities

Provides functions to detect data resolution dynamically from timestamp series
and calculate period counts based on detected resolution.

This fixes CRITICAL ISSUE #6: Hardcoded 5-minute resolution assumptions throughout codebase.
"""

import pandas as pd
import numpy as np
from typing import Union
from ..shared.logging_config import get_logger

logger = get_logger(__name__)


def detect_resolution_minutes(timestamps: Union[pd.DatetimeIndex, pd.Series]) -> int:
    """
    Detect data resolution in minutes from timestamp series.

    Calculates the most common time difference between consecutive timestamps
    to determine the data resolution (5 minutes, 30 minutes, 60 minutes, etc.).

    Args:
        timestamps: DatetimeIndex or Series of datetime values

    Returns:
        Resolution in minutes (e.g., 5, 30, 60)

    Raises:
        ValueError: If timestamps is empty or resolution cannot be detected

    Examples:
        >>> # 5-minute data
        >>> times = pd.date_range('2025-01-01', periods=100, freq='5min')
        >>> detect_resolution_minutes(times)
        5

        >>> # 30-minute data
        >>> times = pd.date_range('2025-01-01', periods=100, freq='30min')
        >>> detect_resolution_minutes(times)
        30
    """
    if timestamps is None or len(timestamps) == 0:
        raise ValueError("Timestamps cannot be empty")

    # Convert Series to DatetimeIndex if needed
    if isinstance(timestamps, pd.Series):
        timestamps = pd.DatetimeIndex(timestamps)

    # Need at least 2 timestamps to calculate differences
    if len(timestamps) < 2:
        raise ValueError("Need at least 2 timestamps to detect resolution")

    # Calculate differences between consecutive timestamps
    time_diffs = timestamps[1:] - timestamps[:-1]

    # Convert to minutes
    diff_minutes = time_diffs.total_seconds() / 60

    # Find the most common difference (mode)
    # Round to nearest integer to handle slight timing variations
    diff_minutes_rounded = np.round(diff_minutes).astype(int)

    # Get mode (most common value)
    # Use value_counts to find most frequent
    value_counts = pd.Series(diff_minutes_rounded).value_counts()

    if len(value_counts) == 0:
        raise ValueError("Could not determine resolution from timestamps")

    resolution_minutes = int(value_counts.index[0])

    # Log detection
    logger.info(f"Detected data resolution: {resolution_minutes} minutes")
    logger.debug(f"Time difference distribution: {value_counts.head().to_dict()}")

    # Validate resolution is reasonable
    valid_resolutions = [1, 5, 10, 15, 30, 60]
    if resolution_minutes not in valid_resolutions:
        logger.warning(f"Unusual resolution detected: {resolution_minutes} minutes. "
                      f"Expected one of {valid_resolutions}")

    return resolution_minutes


def periods_for_hours(hours: Union[int, float], resolution_minutes: int) -> int:
    """
    Calculate number of periods for given hours based on data resolution.

    Args:
        hours: Number of hours to convert
        resolution_minutes: Data resolution in minutes (e.g., 5, 30, 60)

    Returns:
        Number of periods (rounded to nearest integer)

    Examples:
        >>> # 24 hours of 5-minute data
        >>> periods_for_hours(24, 5)
        288

        >>> # 24 hours of 30-minute data
        >>> periods_for_hours(24, 30)
        48

        >>> # 2 hours of 5-minute data
        >>> periods_for_hours(2, 5)
        24

        >>> # 2 hours of 30-minute data
        >>> periods_for_hours(2, 30)
        4
    """
    if resolution_minutes <= 0:
        raise ValueError(f"Resolution must be positive, got {resolution_minutes}")

    if hours < 0:
        raise ValueError(f"Hours must be non-negative, got {hours}")

    # Calculate periods
    total_minutes = hours * 60
    periods = int(round(total_minutes / resolution_minutes))

    logger.debug(f"{hours} hours at {resolution_minutes}-minute resolution = {periods} periods")

    return periods


def periods_for_days(days: Union[int, float], resolution_minutes: int) -> int:
    """
    Calculate number of periods for given days based on data resolution.

    Args:
        days: Number of days to convert
        resolution_minutes: Data resolution in minutes (e.g., 5, 30, 60)

    Returns:
        Number of periods (rounded to nearest integer)

    Examples:
        >>> # 1 day of 5-minute data
        >>> periods_for_days(1, 5)
        288

        >>> # 1 day of 30-minute data
        >>> periods_for_days(1, 30)
        48
    """
    return periods_for_hours(days * 24, resolution_minutes)


def detect_and_calculate_periods(timestamps: Union[pd.DatetimeIndex, pd.Series],
                                  hours: Union[int, float]) -> int:
    """
    Convenience function that detects resolution and calculates periods in one call.

    Args:
        timestamps: DatetimeIndex or Series of datetime values
        hours: Number of hours to convert to periods

    Returns:
        Number of periods for the given hours at detected resolution

    Examples:
        >>> # Detect 5-minute resolution and calculate 24-hour periods
        >>> times = pd.date_range('2025-01-01', periods=100, freq='5min')
        >>> detect_and_calculate_periods(times, 24)
        288
    """
    resolution = detect_resolution_minutes(timestamps)
    return periods_for_hours(hours, resolution)


def get_decay_rate_per_period(hours_halflife: float, resolution_minutes: int) -> float:
    """
    Calculate decay rate per period for exponential decay based on desired half-life.

    For exponential decay with half-life, we want: value(t) = initial_value * (decay_rate ^ periods)
    At half-life: 0.5 = decay_rate ^ periods_at_halflife
    Therefore: decay_rate = 0.5 ^ (1 / periods_at_halflife)

    Args:
        hours_halflife: Desired half-life in hours (time for value to decay to 50%)
        resolution_minutes: Data resolution in minutes

    Returns:
        Decay rate per period (between 0 and 1)

    Examples:
        >>> # 2% decay per period means 98% retention
        >>> # For 5-minute data, what half-life does this give?
        >>> decay_rate = 0.98
        >>> # Find half-life: 0.5 = 0.98^n -> n = log(0.5)/log(0.98) ≈ 34.3 periods
        >>> # 34.3 periods * 5 min = 171.5 minutes = 2.86 hours

        >>> # If we want 2-hour half-life for 5-minute data:
        >>> get_decay_rate_per_period(2.0, 5)
        0.9716...

        >>> # If we want 2-hour half-life for 30-minute data:
        >>> get_decay_rate_per_period(2.0, 30)
        0.8409...
    """
    if hours_halflife <= 0:
        raise ValueError(f"Half-life must be positive, got {hours_halflife}")

    if resolution_minutes <= 0:
        raise ValueError(f"Resolution must be positive, got {resolution_minutes}")

    # Calculate number of periods in half-life
    periods_at_halflife = periods_for_hours(hours_halflife, resolution_minutes)

    # Calculate decay rate: 0.5 = decay_rate ^ periods_at_halflife
    # Therefore: decay_rate = 0.5 ^ (1 / periods_at_halflife)
    decay_rate = 0.5 ** (1.0 / periods_at_halflife)

    logger.debug(f"Decay rate for {hours_halflife}h half-life at {resolution_minutes}-min resolution: "
                f"{decay_rate:.4f} ({100*(1-decay_rate):.2f}% decay per period)")

    return decay_rate


if __name__ == "__main__":
    # Example usage and tests
    print("Resolution Detection Utilities - Examples")
    print("=" * 50)

    # Example 1: 5-minute data
    print("\nExample 1: 5-minute data")
    times_5min = pd.date_range('2025-01-01', periods=100, freq='5min')
    resolution = detect_resolution_minutes(times_5min)
    print(f"Detected resolution: {resolution} minutes")

    periods_24h = periods_for_hours(24, resolution)
    print(f"Periods in 24 hours: {periods_24h}")

    periods_2h = periods_for_hours(2, resolution)
    print(f"Periods in 2 hours: {periods_2h}")

    # Example 2: 30-minute data
    print("\nExample 2: 30-minute data")
    times_30min = pd.date_range('2025-01-01', periods=100, freq='30min')
    resolution = detect_resolution_minutes(times_30min)
    print(f"Detected resolution: {resolution} minutes")

    periods_24h = periods_for_hours(24, resolution)
    print(f"Periods in 24 hours: {periods_24h}")

    periods_2h = periods_for_hours(2, resolution)
    print(f"Periods in 2 hours: {periods_2h}")

    # Example 3: Decay rate calculation
    print("\nExample 3: Decay rate for 2-hour half-life")
    decay_5min = get_decay_rate_per_period(2.0, 5)
    print(f"5-minute data: {decay_5min:.4f} ({100*(1-decay_5min):.2f}% decay per period)")

    decay_30min = get_decay_rate_per_period(2.0, 30)
    print(f"30-minute data: {decay_30min:.4f} ({100*(1-decay_30min):.2f}% decay per period)")

    # Verify half-life calculation
    print("\nVerifying half-life calculation:")
    periods_2h_5min = periods_for_hours(2, 5)
    value_after_halflife = decay_5min ** periods_2h_5min
    print(f"Value after 2 hours (5-min): {value_after_halflife:.4f} (should be ~0.5)")

    periods_2h_30min = periods_for_hours(2, 30)
    value_after_halflife = decay_30min ** periods_2h_30min
    print(f"Value after 2 hours (30-min): {value_after_halflife:.4f} (should be ~0.5)")
