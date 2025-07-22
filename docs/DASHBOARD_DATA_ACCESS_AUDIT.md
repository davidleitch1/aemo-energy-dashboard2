# AEMO Dashboard Data Access Audit

*Generated: July 19, 2025*

This document audits every plot, table, and data visualization in the AEMO Energy Dashboard to identify which components are using the proper DuckDB/hybrid calls versus legacy methods.

## Summary Table

| Tab | Component | Type | Data Source | Access Method | Status |
|-----|-----------|------|-------------|---------------|---------|
| **NEM Dash** | | | | | |
| | Current Spot Prices | Table | prices5.parquet | pd.read_parquet (legacy) | ❌ Needs migration |
| | Renewable Gauge | Gauge | scada5.parquet | pd.read_parquet (legacy) | ❌ Needs migration |
| | Generation Overview | Area Chart | scada5.parquet | pd.read_parquet (legacy) | ❌ Needs migration |
| | Fuel Columns | Bar Charts | scada5.parquet | pd.read_parquet (legacy) | ❌ Needs migration |
| | Transmission Flows | Bar Chart | transmission5.parquet | pd.read_parquet (legacy) | ❌ Needs migration |
| **Generation by Fuel** | | | | | |
| | Generation Stack | Area Chart | scada5/30.parquet | GenerationQueryManager → HybridQueryManager | ✅ Using DuckDB |
| | Capacity Utilization | Line Chart | scada5/30.parquet | GenerationQueryManager → HybridQueryManager | ✅ Using DuckDB |
| | Date Range Selector | Controls | - | - | N/A |
| | Region Selector | Controls | - | - | N/A |
| **Average Price Analysis** | | | | | |
| | Main Analysis Table | Tabulator | Multiple parquets | PriceAnalysisMotor → HybridQueryManager | ✅ Using DuckDB |
| | Group By Controls | Controls | - | - | N/A |
| | Date Range Controls | Controls | - | - | N/A |
| **Station Analysis** | | | | | |
| | Station Search | AutoComplete | gen_info.pickle | Direct pickle load | ⚠️ Acceptable |
| | Time Series Plot | Line Chart | Multiple parquets | StationAnalysisMotor → HybridQueryManager | ✅ Using DuckDB |
| | Time of Day Plot | Box Plot | Multiple parquets | StationAnalysisMotor → HybridQueryManager | ✅ Using DuckDB |
| | Statistics Table | Table | Multiple parquets | StationAnalysisMotor → HybridQueryManager | ✅ Using DuckDB |
| | Revenue Analysis | Table | Multiple parquets | StationAnalysisMotor → HybridQueryManager | ✅ Using DuckDB |

## Detailed Analysis by Tab

### 1. NEM Dash Tab (nem_dash/)

This is the main dashboard view showing real-time market status.

#### Components Needing Migration:

1. **Current Spot Prices Table** (`price_components.py`)
   - Current: `pd.read_parquet(spot_hist_file)`
   - Should use: `HybridQueryManager.query_prices()`
   - Status: ❌ Legacy direct read

2. **Renewable Gauge** (`generation_overview.py`)
   - Current: `pd.read_parquet(gen_output_file)`
   - Should use: `GenerationQueryManager.query_generation_by_fuel()`
   - Status: ❌ Legacy direct read

3. **Generation Overview Chart** (`generation_overview.py`)
   - Current: `pd.read_parquet(gen_output_file)` + manual filtering
   - Should use: `GenerationQueryManager.query_generation_by_fuel()`
   - Status: ❌ Legacy direct read

4. **Fuel Type Bar Charts** (`generation_overview.py`)
   - Current: Direct DataFrame operations on loaded data
   - Should use: Pre-aggregated DuckDB views
   - Status: ❌ Legacy approach

5. **Transmission Flows** (`generation_overview.py`)
   - Current: `pd.read_parquet(transmission_output_file)`
   - Should use: `TransmissionAdapter` or `HybridQueryManager`
   - Status: ❌ Legacy direct read

### 2. Generation by Fuel Tab ✅

**Status: FULLY MIGRATED**

All components in this tab have been successfully migrated to use the DuckDB/hybrid approach:

- Uses `GenerationQueryManager` for all data queries
- Implements smart caching with 5-minute TTL
- Pre-aggregated views reduce data by 99.5%
- Memory usage < 500MB even for 5+ years of data

### 3. Average Price Analysis Tab ✅

**Status: FULLY MIGRATED**

This tab successfully uses the hybrid approach:

- `PriceAnalysisMotor` class uses `HybridQueryManager`
- Supports both DuckDB and DataFrame modes
- Efficient aggregation pushed to DuckDB when possible

### 4. Station Analysis Tab ✅

**Status: FULLY MIGRATED**

This tab is properly implemented:

- `StationAnalysisMotor` uses `HybridQueryManager`
- All queries go through the hybrid layer
- Only the station list (gen_info.pickle) uses direct load, which is acceptable

## Migration Priority

Based on this analysis, the components that need migration are all in the **NEM Dash tab**:

### High Priority (Real-time data):
1. **Current Spot Prices Table** - Shows live market prices
2. **Generation Overview Chart** - Shows current generation mix
3. **Renewable Gauge** - Shows renewable percentage

### Medium Priority:
4. **Fuel Type Bar Charts** - Detailed fuel breakdown
5. **Transmission Flows** - Interconnector flows

## Migration Approach

For each component that needs migration:

1. **Replace direct parquet reads** with appropriate query manager calls
2. **Use pre-aggregated views** where possible
3. **Implement caching** for frequently accessed data
4. **Maintain exact same UI/UX** - users shouldn't notice the change

### Example Migration Pattern:

**Before (Legacy):**
```python
def load_current_prices():
    df = pd.read_parquet(spot_hist_file)
    latest = df.groupby('REGIONID').last()
    return latest
```

**After (Hybrid):**
```python
def load_current_prices():
    end_date = datetime.now()
    start_date = end_date - timedelta(hours=1)
    
    prices = self.query_manager.query_prices(
        start_date=start_date,
        end_date=end_date,
        resolution='5min'
    )
    latest = prices.groupby('REGIONID').last()
    return latest
```

## Benefits of Migration

1. **Memory Efficiency**: Reduce memory usage from 21GB to <1GB
2. **Performance**: Faster queries through pre-aggregation
3. **Scalability**: Can handle years of data without memory issues
4. **Consistency**: All components use the same data access layer
5. **Caching**: Automatic caching reduces redundant queries

## Next Steps

1. Create a new branch for NEM dash migration
2. Migrate one component at a time, starting with Current Spot Prices
3. Test each component thoroughly
4. Ensure UI remains exactly the same
5. Monitor memory usage and performance improvements