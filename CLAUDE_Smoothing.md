# Time Series Smoothing Methods for Electricity Price Data

## Overview

This document provides a comprehensive analysis of time series smoothing methods suitable for volatile electricity price data, including implementation guidelines and parameter recommendations.

## Smoothing Methods Comparison

### 1. Savitzky-Golay Filter

**Description**: A polynomial smoothing filter that fits a polynomial to a window of data points using least squares.

**Strengths:**
- Preserves sharp features like peaks and troughs
- Excellent for locating maxima and minima in price spikes
- Maintains signal tendency without distortion
- Can simultaneously smooth and calculate derivatives
- Better feature preservation than moving averages

**Weaknesses:**
- Poor high-frequency noise suppression
- Artifacts near data boundaries
- Can create overshoots at discontinuities
- Not ideal for extremely noisy data

**Recommended Parameters for Electricity Prices:**
- **Window size**: 13-25 points (6.5-12.5 hours for 30min data)
- **Polynomial order**: 3 (cubic) - balances smoothness with feature preservation
- **For volatile data**: Use smaller windows (13 points) to preserve price spikes
- **For trend analysis**: Use larger windows (25 points)

### 2. LOESS (Locally Estimated Scatterplot Smoothing)

**Description**: Non-parametric method that fits local polynomials to subsets of data.

**Strengths:**
- Adapts to local patterns and nonlinear trends
- No assumptions about data distribution
- Handles varying volatility well
- Excellent for complex seasonal patterns

**Weaknesses:**
- Computationally intensive (slow for large datasets)
- Complex parameter tuning
- Can overfit with small bandwidths
- Edge effects at data boundaries

**Recommended Parameters for Electricity Prices:**
- **Bandwidth (frac)**: 0.05-0.15 (5-15% of data)
- **Degree**: 1 (linear) for volatile data, 2 (quadratic) for smoother trends
- **For price spikes**: Use frac=0.05 to preserve local features
- **For seasonal trends**: Use frac=0.15 for broader smoothing

### 3. Gaussian Filter

**Description**: Weighted moving average using Gaussian (normal) distribution weights.

**Strengths:**
- Smooth, continuous output
- No phase shift when properly implemented
- Good general-purpose filter
- Simple to understand and implement
- Excellent high-frequency noise suppression

**Weaknesses:**
- Rounds sharp features (price spikes)
- May over-smooth volatile periods
- Fixed kernel doesn't adapt to local volatility

**Recommended Parameters for Electricity Prices:**
- **Sigma (std dev)**: 2-4 for 30min data
- **Window size**: 4-6 × sigma (truncate at 3-4 standard deviations)
- **For volatile periods**: sigma=2 (preserves more detail)
- **For stable periods**: sigma=4 (stronger smoothing)

### 4. Exponentially Weighted Moving Average (EWM)

**Description**: Weighted average giving exponentially decreasing weights to older observations.

**Strengths:**
- Very fast computation
- Adapts quickly to recent changes
- Memory efficient (no window needed)
- Natural for real-time/streaming data
- Good for short-term forecasting

**Weaknesses:**
- Poor frequency response
- Can introduce lag
- Less effective at preserving features
- Asymmetric (causal) by default

**Recommended Parameters for Electricity Prices:**
- **Span**: 12-48 periods (6-24 hours for 30min data)
- **Alpha**: 0.05-0.2 (alternatively to span)
- **For volatile markets**: span=12 (alpha≈0.16)
- **For stable periods**: span=48 (alpha≈0.04)
- **Use bidirectional**: Apply forward then backward to reduce lag

## Implementation Strategy

### Phase 1: Basic Implementation

Each smoothing method will be implemented with:
1. Default parameters optimized for electricity prices
2. Validation for edge cases (missing data, boundaries)
3. Performance metrics (computation time, smoothness measure)

### Phase 2: Parameter Controls

Add UI controls for:
- Method-specific parameters (window size, polynomial order, etc.)
- Adaptive smoothing based on local volatility
- Comparison mode (overlay multiple methods)

### Phase 3: Advanced Features

1. **Hybrid approach**: Switch methods based on market conditions
2. **Volatility-adaptive smoothing**: Adjust parameters based on local price variance
3. **Spike preservation**: Special handling for extreme price events

## Recommended Window Sizes for Electricity Data

Based on electricity market characteristics:

| Time Frame | Window Size | Use Case |
|------------|-------------|----------|
| Ultra-short | 3-6 periods (1.5-3 hours) | Intraday trading, spike detection |
| Short | 13-25 periods (6.5-12.5 hours) | Daily patterns, operational planning |
| Medium | 48-96 periods (1-2 days) | Weekly patterns, removing noise |
| Long | 336-672 periods (7-14 days) | Monthly trends, seasonal analysis |

## Implementation Task List

### Task 1: Implement Savitzky-Golay Filter ✅
```python
def savitzky_golay_smooth(price_data, window_size=13, poly_order=3):
    """
    Apply Savitzky-Golay filter to price data
    
    Parameters:
    - window_size: odd number, typically 13-25 for 30min data
    - poly_order: polynomial order, typically 3 for prices
    """
    from scipy.signal import savgol_filter
    
    # Ensure window size is odd
    if window_size % 2 == 0:
        window_size += 1
    
    # Apply filter to each region separately
    smoothed_data = price_data.copy()
    for region in price_data['REGIONID'].unique():
        mask = price_data['REGIONID'] == region
        smoothed_data.loc[mask, 'RRP_smooth'] = savgol_filter(
            price_data.loc[mask, 'RRP'],
            window_size,
            poly_order
        )
    
    return smoothed_data
```

**Testing Points:**
- Verify spike preservation
- Check boundary handling
- Compare with current MA implementation
- Test different window sizes on volatile periods

### Task 2: Implement LOESS Smoothing 📋
```python
def loess_smooth(price_data, frac=0.1, degree=1):
    """
    Apply LOESS smoothing to price data
    
    Parameters:
    - frac: fraction of data for local regression (0.05-0.15)
    - degree: polynomial degree (1 or 2)
    """
    from statsmodels.nonparametric.smoothers_lowess import lowess
    
    smoothed_data = price_data.copy()
    for region in price_data['REGIONID'].unique():
        mask = price_data['REGIONID'] == region
        region_data = price_data.loc[mask]
        
        # Convert datetime to numeric for LOESS
        x = np.arange(len(region_data))
        y = region_data['RRP'].values
        
        # Apply LOESS
        smoothed = lowess(y, x, frac=frac, degree=degree)
        smoothed_data.loc[mask, 'RRP_smooth'] = smoothed[:, 1]
    
    return smoothed_data
```

**Testing Points:**
- Performance on large datasets
- Parameter sensitivity analysis
- Comparison with Savitzky-Golay
- Edge behavior validation

### Task 3: Implement Gaussian Filter 📋
```python
def gaussian_smooth(price_data, sigma=3):
    """
    Apply Gaussian filter to price data
    
    Parameters:
    - sigma: standard deviation of Gaussian kernel (2-4 typical)
    """
    from scipy.ndimage import gaussian_filter1d
    
    smoothed_data = price_data.copy()
    for region in price_data['REGIONID'].unique():
        mask = price_data['REGIONID'] == region
        smoothed_data.loc[mask, 'RRP_smooth'] = gaussian_filter1d(
            price_data.loc[mask, 'RRP'],
            sigma=sigma,
            mode='nearest'  # Handle boundaries
        )
    
    return smoothed_data
```

**Testing Points:**
- Smoothness vs feature preservation
- Boundary mode comparison
- Sigma parameter optimization
- Visual quality assessment

### Task 4: Implement Enhanced EWM (Bidirectional) 📋
```python
def ewm_smooth_bidirectional(price_data, span=24):
    """
    Apply bidirectional EWM to reduce lag
    
    Parameters:
    - span: number of periods for EWM (12-48 typical)
    """
    smoothed_data = price_data.copy()
    
    for region in price_data['REGIONID'].unique():
        mask = price_data['REGIONID'] == region
        region_prices = price_data.loc[mask, 'RRP']
        
        # Forward pass
        forward_smooth = region_prices.ewm(span=span, adjust=False).mean()
        
        # Backward pass
        backward_smooth = region_prices.iloc[::-1].ewm(span=span, adjust=False).mean().iloc[::-1]
        
        # Average forward and backward to reduce lag
        smoothed_data.loc[mask, 'RRP_smooth'] = (forward_smooth + backward_smooth) / 2
    
    return smoothed_data
```

**Testing Points:**
- Lag reduction effectiveness
- Comparison with standard EWM
- Real-time simulation (forward only)
- Parameter sensitivity

### Task 5: Add Parameter Controls 📋

Update the smoothing selector to include:
1. Method selection dropdown
2. Method-specific parameter inputs
3. Preview of smoothing effect
4. Performance metrics display

### Task 6: Performance Testing & Comparison 📋

Create comprehensive test suite:
1. Load test data with known volatile periods
2. Apply all methods with various parameters
3. Calculate metrics:
   - Smoothness (sum of squared differences)
   - Feature preservation (correlation with original)
   - Computation time
   - Visual quality score
4. Generate comparison report

## Best Practices for Volatile Electricity Prices

1. **Preserve Price Spikes**: Use Savitzky-Golay or small-window LOESS
2. **Remove High-Frequency Noise**: Use Gaussian or larger-window methods
3. **Real-time Analysis**: Use EWM or causal filters only
4. **Seasonal Decomposition**: Apply smoothing after removing known patterns
5. **Adaptive Smoothing**: Adjust parameters based on local volatility:
   - High volatility periods: Reduce smoothing strength
   - Stable periods: Increase smoothing for cleaner trends

## Validation Criteria

Each implementation should be tested against:
1. **Synthetic data** with known properties
2. **Historical price spikes** (e.g., market events)
3. **Normal trading periods**
4. **Transition periods** (volatility changes)
5. **Missing data scenarios**

## Next Steps

1. Implement Savitzky-Golay filter first (best balance of features)
2. Test on current dashboard data
3. Add parameter controls to UI
4. Implement remaining methods based on test results
5. Create automated parameter selection based on data characteristics