# Testing axiswise=True Solution

## Current Implementation
Based on research findings, we're using:
```python
combined_plot = (plot1 + plot2).cols(1).opts(
    hv.opts.Layout(
        shared_axes=True,  # Link axes within this layout
        axiswise=True      # But keep them independent from other tabs
    )
)
```

## What axiswise=True Does
- Makes each plot get its own independent x-axis and y-axis
- Prevents axis linking across different layouts/tabs
- Should maintain linking within a layout but isolate from others

## Test Procedure
1. Open dashboard at http://localhost:5008
2. Go to Batteries tab
3. Select NSW1 and Waratah Super Battery
4. Set dates to 2025-08-25 to 2025-09-01
5. Verify x-axes are linked between power and price plots (zoom one, both should zoom)
6. Switch to Prices tab
7. Interact with price charts (zoom, pan)
8. Switch back to Batteries tab
9. Check if dates still show 2025-08-25 to 2025-09-01

## Expected Result
- Within Batteries tab: Power and Price plots should have linked x-axes
- Within Prices tab: Generation and Price plots should have linked x-axes
- Between tabs: No interference - each tab maintains independent axis ranges

## Alternative Solutions If This Doesn't Work
1. Use different dimension names for each tab's plots
2. Wrap plots in pn.pane.HoloViews(linked_axes=False)
3. Create explicit x_range objects for each tab instance