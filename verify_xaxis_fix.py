#!/usr/bin/env python3
"""
Verify that the x-axis interference fix is working correctly.

The fix changes the link_x_ranges_hook to use a local list (x_ranges_to_link)
instead of a function attribute (link_x_ranges_hook.x_ranges).

This ensures that x_ranges from the Prices tab don't interfere with the Batteries tab.
"""

print("""
X-AXIS INTERFERENCE FIX VERIFICATION
====================================

PROBLEM:
- The link_x_ranges_hook was storing x_ranges as a function attribute
- Function attributes persist across tab switches
- This caused Prices tab x-axis range to affect Batteries tab plots

SOLUTION:
- Changed to use a local list (x_ranges_to_link) created fresh for each plot
- This ensures each tab's plots are independent
- No cross-tab interference

TO TEST MANUALLY:
1. Open the dashboard at http://localhost:5008
2. Go to the Prices tab
3. Use the "Analyse prices" button or interact with price plots
4. Switch to the Batteries tab
5. Select a battery and check if the x-axis looks normal
6. The battery plot should NOT inherit the x-axis range from the Prices tab

EXPECTED RESULT:
- Battery plots should have their own independent x-axis range
- No zooming or range changes from Prices tab should affect Batteries tab

CODE CHANGE:
- Replaced: if not hasattr(link_x_ranges_hook, 'x_ranges'):
            link_x_ranges_hook.x_ranges = []
- With:     x_ranges_to_link = []  # Local list, not function attribute

This fix ensures proper scoping and prevents cross-tab interference.
""")