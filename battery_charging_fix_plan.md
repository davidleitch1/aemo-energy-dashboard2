# Battery Charging Data Integration Plan

## Problem Statement
AEMO SCADA data only reports generation (positive values) for batteries, not consumption/charging (negative values). This makes it impossible to accurately visualize battery storage operations in stacked area charts or calculate true net generation.

## Phase 1: Identify AEMO Data Source for Battery Charging

### Potential Data Sources

1. **DISPATCHIS Reports** (Most Likely)
   - URL Pattern: `http://nemweb.com.au/Reports/Current/DispatchIS_Reports/`
   - Contains: Unit dispatch targets including both generation and load
   - Files: `PUBLIC_DISPATCHIS_YYYYMMDD_HHMMSS.zip`
   - Table needed: `DISPATCH_UNIT_SOLUTION` or `DISPATCH_UNIT_SCADA`
   - Fields: `DUID`, `SETTLEMENTDATE`, `TOTALCLEARED` (can be negative for charging)

2. **Next Day Dispatch Reports**
   - URL: `http://nemweb.com.au/Reports/Current/Next_Day_Dispatch/`
   - Contains: Actual dispatch outcomes including battery charging
   - Table: `NEXT_DAY_DISPATCH`

3. **MMSDM Data** (Most Comprehensive but Delayed)
   - URL: `https://nemweb.com.au/Data_Archive/Wholesale_Electricity/MMSDM/`
   - Table: `DISPATCH_UNIT_SOLUTION`
   - Delay: 6+ months
   - Contains: Complete dispatch data including negative values

### Investigation Tasks
- [ ] Download sample DISPATCHIS report
- [ ] Check if `DISPATCH_UNIT_SOLUTION` contains negative values for known batteries
- [ ] Verify data completeness and timing alignment with SCADA

## Phase 2: Download Battery Charging Data

### New Data Collection Script
Create `collect_battery_charging.py`:

```python
# Key components:
1. Download DISPATCHIS reports (5-minute intervals)
2. Extract DISPATCH_UNIT_SOLUTION table
3. Filter for battery storage DUIDs
4. Extract negative values (charging)
5. Save to parquet: battery_charging5.parquet, battery_charging30.parquet
```

### Data Structure
```
battery_charging5.parquet:
- settlementdate: datetime
- duid: str
- charging_mw: float (negative values only)
- dispatch_type: str (DISPATCH/PRE-DISPATCH)

battery_charging30.parquet:
- settlementdate: datetime  
- duid: str
- charging_mw: float (30-min aggregated)
```

### Backfill Historical Data
- [ ] Create `backfill_battery_charging.py`
- [ ] Download historical DISPATCHIS from archive
- [ ] Process last 12 months of data
- [ ] Handle data gaps and missing files

## Phase 3: Merge Charging Data into SCADA Files

### Merging Strategy

1. **Create backup of existing files**
   ```bash
   cp scada5.parquet scada5_backup_20250820.parquet
   cp scada30.parquet scada30_backup_20250820.parquet
   ```

2. **Merge Process**
   ```python
   # Pseudocode for merge_battery_charging.py
   
   # Load existing SCADA data
   scada5 = pd.read_parquet('scada5.parquet')
   charging5 = pd.read_parquet('battery_charging5.parquet')
   
   # For battery DUIDs with charging data:
   # - When charging exists: use negative charging value
   # - When only discharge exists: keep positive SCADA value
   # - Handle overlaps carefully
   
   # Merge logic:
   merged = scada5.merge(
       charging5, 
       on=['settlementdate', 'duid'], 
       how='outer',
       suffixes=('_scada', '_charging')
   )
   
   # Combine values:
   # If charging_mw exists and < 0: use charging_mw
   # Else: use scadavalue
   merged['scadavalue'] = np.where(
       merged['charging_mw'].notna() & (merged['charging_mw'] < 0),
       merged['charging_mw'],
       merged['scadavalue_scada']
   )
   ```

3. **Validation Steps**
   - [ ] Verify battery DUIDs now have both positive and negative values
   - [ ] Check daily charge/discharge cycles make sense
   - [ ] Ensure non-battery DUIDs unchanged
   - [ ] Validate total energy balance (discharge ≈ charge * efficiency)

## Phase 4: Update Data Collectors

### Modify Unified Collector

1. **Update `unified_collector.py`**
   ```python
   # Add new collection step:
   def collect_battery_charging(self):
       """Collect battery charging data from DISPATCHIS"""
       # Download latest DISPATCHIS file
       # Extract charging data
       # Merge with SCADA data before saving
   ```

2. **Integration Points**
   - Modify `process_scada_data()` to include charging merge
   - Update `calculate_30min_aggregates()` to handle negative values
   - Ensure atomic updates (don't corrupt data mid-update)

3. **New Configuration**
   ```python
   # Add to collector config:
   COLLECT_BATTERY_CHARGING = True
   BATTERY_DUIDS = load_battery_duids()  # From gen_info
   DISPATCHIS_URL = "http://nemweb.com.au/Reports/Current/DispatchIS_Reports/"
   ```

### Testing Plan

1. **Unit Tests**
   - [ ] Test charging data extraction from DISPATCHIS
   - [ ] Test merge logic with various scenarios
   - [ ] Test aggregation with negative values

2. **Integration Tests**
   - [ ] Run collector in test mode
   - [ ] Verify merged data integrity
   - [ ] Check dashboard displays correctly

3. **Rollback Plan**
   - Keep backups of original SCADA files
   - Version control for collector changes
   - Ability to disable charging collection via config

## Phase 5: Dashboard Updates

### Required Changes

1. **Generation Stack Plot**
   - Handle negative values in stacked area
   - Consider split positive/negative display for batteries
   - Update tooltips to show charge/discharge

2. **Calculations**
   - Net generation = sum(all positive) + sum(all negative)
   - Battery round-trip efficiency metrics
   - Peak charging/discharging times

3. **New Visualizations**
   - Battery state of charge estimation
   - Charge/discharge patterns
   - Arbitrage opportunity analysis

## Implementation Timeline

### Week 1: Investigation & Design
- Day 1-2: Investigate DISPATCHIS format, confirm negative values
- Day 3-4: Design data schema and merge strategy
- Day 5: Create proof of concept with one battery

### Week 2: Implementation
- Day 1-2: Build charging data collector
- Day 3-4: Implement merge process
- Day 5: Backfill historical data

### Week 3: Integration & Testing
- Day 1-2: Update unified collector
- Day 3-4: Test and validate
- Day 5: Deploy to production

## Risk Mitigation

1. **Data Quality Issues**
   - DISPATCHIS might not have all charging data
   - Timing misalignment between SCADA and DISPATCH
   - Missing data for some batteries

2. **Performance Impact**
   - Additional data collection overhead
   - Larger parquet files
   - More complex merge operations

3. **Backwards Compatibility**
   - Existing dashboards expect only positive values
   - Historical analysis might break
   - Need to handle transition period

## Success Criteria

- [ ] All battery DUIDs show both positive and negative values
- [ ] Daily charge/discharge cycles visible in data
- [ ] Dashboard correctly displays battery operations
- [ ] No data corruption or loss
- [ ] Collector runs within performance targets
- [ ] Historical data successfully backfilled

## Notes and Considerations

1. **Battery Efficiency**: Charging will typically be 10-15% more than discharge due to round-trip losses
2. **Frequency Control**: Some charging/discharging is for frequency control (FCAS) not energy arbitrage
3. **State of Charge**: Without initial SOC, we can only show relative changes
4. **Data Licensing**: Ensure DISPATCHIS data usage complies with AEMO terms

## Next Steps

1. Start with Phase 1: Download and analyze sample DISPATCHIS file
2. Confirm negative values present for known battery DUIDs
3. Create proof of concept merge for one day of data
4. Review plan with team before full implementation