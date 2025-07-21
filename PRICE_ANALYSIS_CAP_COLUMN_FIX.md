# Price Analysis Cap (MW) Column Fix Complete

*Date: July 19, 2025, 8:55 PM AEST*

## Issue Resolved

The Cap (MW) column was not appearing in the Average Price Analysis table when selected by the user. This has now been fixed.

## Root Cause

The issue was in the UI layer (`price_analysis_ui.py`), not in the data processing:

1. The data processing correctly included `capacity_mw` in all stages:
   - ✓ Integrated data included `nameplate_capacity` 
   - ✓ Aggregated data calculated `capacity_mw` sums
   - ✓ DUID details included `capacity_mw` values
   - ✓ Hierarchical data passed `capacity_mw` through

2. The UI issue was that `self.selected_columns` was storing formatted column names ('generation_gwh', 'revenue_millions') instead of display names ('Gen (GWh)', 'Rev ($M)', 'Cap (MW)')

3. When building the display columns, the code defaulted to the wrong column names when the attribute wasn't set properly

## Fix Applied

### File: `src/aemo_dashboard/analysis/price_analysis_ui.py`

1. **Line 764**: Changed default column names from internal format to display format:
   ```python
   # Before:
   user_selected = getattr(self, 'selected_columns', ['generation_gwh', 'revenue_millions', 'avg_price'])
   
   # After:
   user_selected = getattr(self, 'selected_columns', ['Gen (GWh)', 'Rev ($M)', 'Price ($/MWh)'])
   ```

2. **Lines 478, 481, 599, 601**: Updated default selected columns to use display names:
   ```python
   # Before:
   self.selected_columns = ['generation_gwh', 'revenue_millions', 'avg_price']
   
   # After:
   self.selected_columns = ['Gen (GWh)', 'Rev ($M)', 'Price ($/MWh)']
   ```

### File: `src/aemo_dashboard/analysis/price_analysis.py`

3. **Lines 455-464**: Made capacity column detection more robust:
   ```python
   # Now checks for both possible column names:
   if 'Capacity(MW)' in data.columns:
       capacity_col = 'Capacity(MW)'
   elif 'nameplate_capacity' in data.columns:
       capacity_col = 'nameplate_capacity'
   ```

## Verification

Created and ran test scripts that confirmed:
- ✓ 'Cap (MW)' is available in the column selector checkboxes
- ✓ capacity_mw data flows through all processing stages
- ✓ The column can now be selected and displayed in the table

## Summary

All requested fixes have been completed:
1. ✅ Number formatting: Values ≥ 10 show 0 decimal places, values < 10 show 1 decimal place
2. ✅ Cap (MW) column: Now appears in the table when selected by the user

The Average Price Analysis table now correctly:
- Shows only selected fuel types (e.g., just "Wind" when selected)
- Displays individual DUIDs when groups are expanded
- Includes capacity information when Cap (MW) is selected
- Formats all numbers according to the smart rounding rules