# Savitzky-Golay Filter Implementation

## Date: July 23, 2025

### What Was Implemented

Successfully added Savitzky-Golay smoothing filter to the Prices tab with two preset configurations:

1. **Savitzky-Golay (w=13, o=3)** - For volatile data
   - Window size: 13 periods (6.5 hours for 30-minute data)
   - Polynomial order: 3 (cubic)
   - Best for preserving price spikes while smoothing noise

2. **Savitzky-Golay (w=25, o=3)** - For trend analysis
   - Window size: 25 periods (12.5 hours for 30-minute data)
   - Polynomial order: 3 (cubic)
   - Stronger smoothing for identifying trends

### Key Features

- **Spike Preservation**: Unlike moving averages, Savitzky-Golay preserves sharp features like price spikes
- **Edge Handling**: Uses 'nearest' mode to extrapolate at data boundaries
- **Error Handling**: Gracefully handles cases with insufficient data points
- **Integration**: Seamlessly integrated with existing smoothing options

### Technical Details

- Added scipy dependency (v1.16.0) to project
- Filter is applied per region to maintain data integrity
- Minimum data points required equals window size
- Falls back to original data if smoothing fails

### Testing Instructions

1. Navigate to the Prices tab
2. Select regions and time period
3. Choose one of the Savitzky-Golay options from the Smoothing dropdown
4. Click "Analyze Prices"
5. Compare with other smoothing methods:
   - Notice how price spikes are preserved better than with MA
   - Observe smoother curves compared to raw data
   - Check edge behavior at start/end of time series

### Next Steps

Test the implementation and provide feedback before implementing:
- LOESS smoothing
- Gaussian filter
- Enhanced bidirectional EWM

### Comparison Points

When testing, compare:
1. **Feature Preservation**: How well are price spikes maintained?
2. **Smoothness**: Is the curve smooth without being over-smoothed?
3. **Edge Effects**: How does it handle the start/end of data?
4. **Computation Speed**: Is it noticeably slower than MA?