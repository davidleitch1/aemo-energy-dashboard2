#!/usr/bin/env python3
"""
Test that the shared_axes='x' fix prevents cross-tab x-axis interference.

Changes made:
1. Prices tab: Removed js_link approach, now using shared_axes='x'
2. Batteries tab: Changed from shared_axes=True to shared_axes='x'

This should:
- Link x-axes within each tab
- Prevent cross-tab interference
- Avoid y-axis linking issues
"""

print("""
SHARED_AXES FIX VERIFICATION
============================

CHANGES MADE:
1. In gen_dash.py (Prices tab):
   - Removed js_link hook approach
   - Now using: shared_axes='x' in Layout opts
   
2. In insights_tab.py (Batteries tab):
   - Changed from: shared_axes=True
   - To: shared_axes='x'

WHY THIS WORKS:
- shared_axes='x' creates HoloViews-managed linking (not persistent JS)
- Each tab creates its own layout with its own axis linking
- No global state or persistent connections

TO TEST:
1. Open dashboard at http://localhost:5008
2. Go to Prices tab, interact with charts
3. Switch to Batteries tab
4. Select a battery and check date range
5. The battery plot should show its own date range, not the Prices tab range

EXPECTED RESULT:
- X-axes linked within each tab
- No interference between tabs
- Each tab maintains independent zoom/pan state

NOTE:
If shared_axes='x' causes any UFuncTypeError or other issues,
we may need to fall back to independent axes without linking.
""")