# Curtailment Rate Calculation Issue

## Problem

The curtailment rates shown for individual units are unrealistically high (46-70%) compared to regional averages (7-9%).

## Root Cause

The current calculation counts ALL intervals where:
- `SEMIDISPATCHCAP = 1` (AEMO has control)
- `AVAILABILITY > TOTALCLEARED`

However, analysis shows that during 86% of these "curtailment" intervals, the unit was NOT actually generating (SCADA = 0 MW).

### Example: MUWAWF1 (Oct 8-15, 2025)

```
Period: 7 days
Curtailment records: 1,112
SCADA > 1 MW: 100 (9%)
SCADA ≈ 0 MW: 959 (86%)
```

Sample data:
```
Timestamp            Avail   Dispatch  Curtail   SCADA
2025-10-08 08:00:00   27.9      0.0     27.9     0.0   ← Wind not blowing
2025-10-08 08:05:00   27.9      0.0     27.9     0.0   ← Wind not blowing
2025-10-08 08:10:00   23.9      0.0     23.9     0.0   ← Wind not blowing
```

##  Issue

AVAILABILITY appears to be a **forecast** or **bid**, not actual real-time wind conditions. When wind dies unexpectedly, we still count it as "curtailment" even though the unit couldn't have generated anyway.

## Current Formula (Incorrect)

### Curtailment MWh
```sql
SUM(CURTAILMENT) / 12  -- All intervals where AVAIL > DISPATCH
```

### Rate
```sql
curtailment / (curtailment + actual_generation) × 100
```

### Result
- MW based on theoretical availability
- Not actual lost generation
- Rates: 46-70% (too high)

## Proposed Fix

Only count curtailment when unit was actually capable of generating.

### Option 1: SCADA Threshold Filter
Only include intervals where SCADA > threshold (e.g., 1 MW)

```sql
-- Curtailment: only when actually generating
SUM(CASE WHEN scada > 1.0 THEN curtailment ELSE 0 END) / 12 as curtailed_mwh

-- Actual: same filter
SUM(CASE WHEN scada > 1.0 THEN scada ELSE 0 END) * 0.5 as actual_mwh

-- Rate: curtailed / (curtailed + actual)
```

### Option 2: Use SCADA as Proof of Capability
If unit wasn't generating, assume AVAILABILITY was wrong

```sql
-- Effective curtailment: limited by what unit could actually do
CASE
  WHEN scada > 0 THEN curtailment
  WHEN scada = 0 AND dispatchcap = 0 THEN 0  -- Dispatch = 0, not generating = no curtailment
  ELSE curtailment  -- Keep for cases where we don't have SCADA
END
```

### Option 3: Conservative Approach
Use minimum of curtailment and actual generation as upper bound

```sql
-- Curtailment can't exceed what unit actually did
LEAST(curtailment, scada + curtailment) as effective_curtailment
```

## Recommendation

**Use Option 1** - Only count intervals where unit was actively generating (SCADA > 1 MW).

**Rationale:**
- If wind wasn't blowing (SCADA ≈ 0), it's not meaningful curtailment
- Aligns with industry definition: "prevented from generating when capable"
- Will bring unit-level rates closer to regional averages
- More accurate representation of actual lost renewable energy

## Implementation

Modify `query_top_curtailed_units()` and potentially `query_region_summary()`:

```sql
-- Filter to intervals where unit was capable
WHERE scada > 1.0  -- or similar threshold

-- Or adjust in aggregation
SUM(CASE WHEN scada > 1.0 THEN curtailment ELSE 0 END)
```

## Impact

With SCADA filtering, MUWAWF1 rates would be:
- Current: 70.4% (based on 12,816 MWh curtailed)
- Corrected: ~15-20% (based on ~100 intervals with SCADA > 1)
- More realistic and aligned with regional averages

## References

- Curtailment documentation: curtailment.md
- Test results: test_curtailment_simple.py, test_old_vs_new_formula.py
