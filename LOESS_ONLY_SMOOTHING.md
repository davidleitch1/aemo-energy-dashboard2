# LOESS-Only Smoothing Configuration

## Date: July 23, 2025

### Changes Made

The smoothing menu has been simplified to show only LOESS options, as requested. The original smoothing methods (Moving Average, Exponential, Savitzky-Golay) remain in the code but are hidden from the menu.

### Available LOESS Options

1. **LOESS (3 hours, frac=0.01)** - Ultra-short term smoothing
   - Designed for 5-minute data over 1-3 days
   - ~36 points for 3 hours of 5-minute data
   - Minimal smoothing, preserves most features
   - Best for: Intraday analysis, spike detection

2. **LOESS (1 day, frac=0.02)** - Short term smoothing
   - ~288 points for 1 day of 5-minute data
   - Light smoothing while maintaining daily patterns
   - Best for: Daily volatility analysis

3. **LOESS (7 days, frac=0.05)** - Medium term smoothing
   - Balances detail preservation with noise reduction
   - Best for: Weekly patterns and trends

4. **LOESS (30 days, frac=0.1)** - Long term smoothing
   - Good balance for monthly analysis
   - Best for: Monthly trends, seasonal patterns

5. **LOESS (90 days, frac=0.15)** - Very long term smoothing
   - Strong smoothing for long-term trends
   - Best for: Quarterly/yearly trend analysis

### Parameter Rationale

- **frac=0.01**: For ~1000 points (3 days of 5-min data), uses ~10 points per window
- **frac=0.02**: For ~288 points (1 day of 5-min data), uses ~6 points per window
- **frac=0.05**: Standard for weekly analysis
- **frac=0.1**: Standard for monthly analysis
- **frac=0.15**: Standard for quarterly analysis

### Hidden Methods (Still in Code)

The following methods are hidden but can be re-enabled by updating the options list:
- Moving Avg (7 periods)
- Moving Avg (30 periods)
- Exponential (α=0.3)
- Savitzky-Golay (7 days)
- Savitzky-Golay (30 days)
- Savitzky-Golay (90 days)

### To Restore Other Methods

Simply edit the `options` list in `gen_dash.py` around line 2547 to include any of the hidden methods.

### Performance Notes

- The 3-hour and 1-day options are optimized for high-frequency (5-minute) data
- Larger frac values provide more smoothing but may be slower on very large datasets
- LOESS adapts well to volatile electricity price data patterns