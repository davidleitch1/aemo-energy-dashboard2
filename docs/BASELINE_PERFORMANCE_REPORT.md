# Dashboard Performance Baseline Report

**Date**: July 21, 2025, 8:30 PM AEST  
**After**: Tab name preservation fix

## Executive Summary

Based on analysis of recent dashboard logs, here are the current performance baselines:

### Tab Loading Times (Current Baseline)

| Tab Name | Average Load Time | Most Recent | Min | Max |
|----------|------------------|-------------|-----|-----|
| **Today** | 5.40s | 5.40s | 5.40s | 5.40s |
| **Generation mix** | 0.60s | 0.30s | 0.30s | 0.76s |
| **Pivot table** | 0.10s | 0.05s | 0.05s | 0.15s |
| **Station Analysis** | 0.29s | 0.28s | 0.26s | 0.31s |
| **Penetration** | 3.68s | 3.35s | 3.35s | 3.87s |

**Total time to load all tabs**: ~9.38s (most recent session)

### Key Observations

1. **"Today" tab** (5.40s) - This loads at startup and is the slowest
   - Contains NEM dashboard with multiple real-time components
   - Not lazy loaded (always loads immediately)

2. **"Penetration" tab** (3.35-3.87s) - Consistently the slowest lazy-loaded tab
   - Contains complex year-over-year VRE analysis charts
   - Multiple hvplot visualizations with large datasets

3. **"Pivot table" tab** (0.05-0.15s) - Fastest tab
   - Price analysis but well-optimized
   - Benefits from DuckDB query efficiency

4. **"Generation mix" tab** (0.30-0.76s) - Moderate speed
   - Main generation dashboard
   - Performance varies based on data range

5. **"Station Analysis" tab** (0.26-0.31s) - Very consistent
   - Complex analysis but well-optimized
   - Stable performance across sessions

### Comparison to Previous State

Before implementing lazy loading:
- Dashboard startup included loading ALL tabs
- Total startup time was 15-20 seconds

After lazy loading + tab name fix:
- Only "Today" tab loads at startup (~5.4s)
- Other tabs load on-demand
- Total time reduced by ~50% for typical usage

### Performance Concerns

1. **Tab Name Fix Impact**: The fix to preserve tab names (`tabs.objects[index]` instead of `tabs[index]`) appears to have minimal performance impact based on the timing data.

2. **Startup Time**: The "Today" tab taking 5.4s at startup is the main bottleneck for initial dashboard load.

3. **Penetration Tab**: At 3.35-3.87s, this tab could benefit from optimization.

### Recommendations

1. **Optimize "Today" Tab**: 
   - Consider making it truly lazy-loaded
   - Or optimize the NEM dashboard components

2. **Cache Penetration Charts**:
   - Apply pn.cache to the VRE analysis charts
   - Pre-calculate year-over-year comparisons

3. **Monitor Performance**:
   - Continue tracking these metrics
   - Alert if any tab exceeds 2x baseline

## Test Methodology

Data collected from `logs/aemo_dashboard.log` analyzing:
- 6 dashboard sessions from July 21, 2025
- Tab loading times logged by the application
- Both individual and aggregate metrics

## Conclusion

The current implementation with lazy loading and the tab name fix provides reasonable performance:
- Initial load: ~5.4 seconds (Today tab only)
- Full dashboard access: ~10 seconds total
- Individual tab response: 0.05s - 3.87s

This establishes the baseline for future optimization efforts.