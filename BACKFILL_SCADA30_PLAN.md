# Backfill Plan for SCADA30 Missing Data

## Missing Data Periods
1. **December 2020**: 2020-12-01 00:30:00 to 2020-12-31 23:30:00 (30 days, 23.5 hours)
2. **October 2021**: 2021-10-01 00:30:00 to 2021-10-31 23:30:00 (30 days, 23.5 hours)  
3. **June 2022**: 2022-06-01 00:30:00 to 2022-06-30 23:30:00 (29 days, 23.5 hours)

## Data Source: MMSDM Archives

### Location
- Base URL: `https://nemweb.com.au/Data_Archive/Wholesale_Electricity/MMSDM/{year}/`
- File format: `MMSDM_{year}_{month}.zip`

### Archive Structure
1. **Single ZIP file per month** (very large: 4-8 GB each)
2. Contains CSV files in AEMO MMS format
3. **DISPATCH_UNIT_SCADA** table contains the 5-minute generation data
4. Multiple CSV files per day within the archive

### Data Extraction Process

#### Step 1: Download MMSDM Archives
- December 2020: `MMSDM_2020_12.zip` (4.4 GB)
- October 2021: `MMSDM_2021_10.zip` (6.2 GB)
- June 2022: `MMSDM_2022_06.zip` (7.7 GB)

#### Step 2: Extract DISPATCH_UNIT_SCADA Files
Within each monthly ZIP:
- Look for files matching pattern: `PUBLIC_DVD_DISPATCH_UNIT_SCADA_YYYYMMDD_*.CSV`
- Each CSV contains all 5-minute intervals for that day
- CSV format: AEMO MMS format with header rows marked with 'I' and data rows with 'D'

#### Step 3: Parse CSV Data
MMS CSV structure:
```
C,comment...
I,DISPATCH,UNIT_SCADA,1,SETTLEMENTDATE,DUID,SCADAVALUE,...
D,DISPATCH,UNIT_SCADA,1,2020/12/01 00:05:00,UNIT1,123.45,...
D,DISPATCH,UNIT_SCADA,1,2020/12/01 00:05:00,UNIT2,456.78,...
```

#### Step 4: Convert to Parquet Format
Transform data to match existing schema:
- `settlementdate`: datetime
- `duid`: string  
- `scadavalue`: float

#### Step 5: Calculate 30-minute Aggregates
- Group by settlementdate (rounded to 30-min) and duid
- Calculate mean of scadavalue (not sum/2)
- This fixes the existing bug in scada30 calculation

## Implementation Strategy

### Option 1: Full Archive Download (Reliable but Slow)
1. Download entire monthly archives
2. Extract all DISPATCH_UNIT_SCADA files
3. Process and save to temporary parquet files
4. Merge with existing scada5.parquet and scada30.parquet

### Option 2: Streaming Download (Memory Efficient)
1. Stream download the ZIP file
2. Process DISPATCH_UNIT_SCADA files as encountered
3. Accumulate data in batches
4. Save directly to parquet format

### Option 3: Cloud Processing (Recommended for Production)
1. Use cloud compute instance with high bandwidth
2. Download archives in parallel
3. Process using distributed computing (Dask/Ray)
4. Upload results to production storage

## Storage Requirements
- Temporary space needed: ~20 GB for archives
- Processing memory: ~8 GB RAM
- Final data size: ~500 MB additional for scada30.parquet

## Time Estimate
- Download time: 30-60 minutes (depends on bandwidth)
- Processing time: 30-45 minutes
- Total: 1-2 hours per month

## Next Steps
1. Create `backfill_scada30_mmsdm.py` script
2. Test with December 2020 first (smallest archive)
3. Process all three months
4. Verify data integrity
5. Update both development and production systems