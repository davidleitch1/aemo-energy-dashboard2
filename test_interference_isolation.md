# Test Plan: X-Axis Interference Isolation

## Current Setup
- **Prices tab**: `shared_axes=False` 
- **Batteries tab**: `shared_axes=False`
- **Expected**: No axis linking within tabs, but also no cross-tab interference

## Test Steps
1. Open dashboard at http://localhost:5008
2. Go to Batteries tab first
3. Select NSW1 region and Waratah Super Battery
4. Set dates to 2025-08-25 to 2025-09-01
5. Note the x-axis range shown (should match the selected dates)
6. Switch to Prices tab
7. Interact with the price charts (zoom, pan, etc.)
8. Switch back to Batteries tab
9. Check if the x-axis still shows 2025-08-25 to 2025-09-01

## Expected Result
- Battery plot x-axis should still show the correct date range (2025-08-25 to 2025-09-01)
- No interference from the Prices tab

## If This Works
If disabling shared_axes stops the interference, then we know the issue is with HoloViews' axis sharing mechanism. We would then need to find an alternative way to link axes within tabs without causing cross-tab interference.

## Possible Solutions If Isolation Works
1. **Manual synchronization**: Use Panel callbacks to manually sync zoom/pan within a tab
2. **Tab-specific contexts**: Create separate plot contexts for each tab
3. **RangeXY with explicit bounds**: Set explicit x_range bounds for each tab
4. **Accept no linking**: Keep shared_axes=False as the solution

## Note
The downside of shared_axes=False is that zooming/panning one plot won't affect the other plot in the same tab. But this is better than cross-tab interference.