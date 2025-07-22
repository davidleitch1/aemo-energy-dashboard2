# Dashboard Tab Renaming Complete

*Date: July 19, 2025, 9:30 PM AEST*

## Changes Made

### 1. Tab Names Updated

**Old Names → New Names:**
- "Nem-dash" → **"Today"**
- "Generation by Fuel" → **"Generation mix"**
- "Average Price Analysis" → **"Pivot table"**
- "Station Analysis" → **"Station Analysis"** (unchanged)

### 2. New Tab Added

Added a new top-level tab called **"Penetration"** that will show renewable energy penetration metrics and analysis.

Currently displays:
- Title: "Penetration Analysis"
- Message: "This tab is under development"
- Description: "Will show renewable energy penetration metrics and analysis."

## Technical Details

**File Modified**: `src/aemo_dashboard/generation/gen_dash.py`

**Changes at lines 2263-2281:**
1. Created placeholder content for the Penetration tab
2. Updated the tab names in the `pn.Tabs()` constructor
3. Added the Penetration tab as the 5th tab

## Result

The dashboard now has 5 tabs with clearer, more descriptive names:
1. **Today** - Current market overview (was Nem-dash)
2. **Generation mix** - Generation by fuel type analysis
3. **Pivot table** - Price analysis with flexible aggregation
4. **Station Analysis** - Individual station performance
5. **Penetration** - Renewable penetration metrics (new, placeholder)

## Next Steps

The Penetration tab is ready for content to be added. It could include:
- Renewable energy percentage over time
- Penetration by region
- Peak vs off-peak renewable contribution
- Curtailment analysis
- Grid stability metrics with high renewable penetration