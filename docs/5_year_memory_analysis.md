# 5-Year Memory Requirements Analysis

## Executive Summary

For a 5-year dataset, the AEMO Energy Dashboard will require:
- **Storage**: ~1.1 GB for parquet files
- **RAM**: 7-10 GB minimum for full dataset loading
- **Records**: ~244 million generation records

## Detailed Analysis

### 1. Storage Requirements (Parquet Files)

| Data Type | Current (22 days) | 5 Years | Growth Factor |
|-----------|------------------|---------|---------------|
| Generation | 12.9 MB | 1,072 MB | 83x |
| Prices | 0.3 MB | 23 MB | 77x |
| Rooftop Solar | 0.07 MB | 9 MB | 129x |
| Transmission | 0.11 MB | 9 MB | 82x |
| **Total** | **13.4 MB** | **1,113 MB (1.1 GB)** | **83x** |

### 2. Memory Requirements (RAM)

When loaded into pandas DataFrames, memory usage multiplies due to:
- Data type overhead (float64 vs compressed storage)
- Index structures
- String interning for categorical data
- Pandas metadata

| Component | 5-Year Size | Memory Multiplier | RAM Usage |
|-----------|-------------|-------------------|-----------|
| Generation Data | 1,072 MB | 4x | 4,288 MB |
| Price Data | 23 MB | 3x | 70 MB |
| Other Data | 18 MB | 3x | 56 MB |
| Dashboard Overhead | - | - | 500 MB |
| **Total Base** | | | **4,915 MB** |
| **With 50% Safety** | | | **7.4 GB** |
| **Recommended** | | | **10 GB** |

### 3. Data Volume Projections

- **Generation Records**: 244 million (133,593/day × 1,826 days)
- **Price Records**: 2.7 million (1,458/day × 1,826 days)
- **Unique DUIDs**: ~500-600 (growing slowly)
- **5-minute intervals**: 525,600 per year

## Recommendations

### Option 1: Full In-Memory Loading (Current Approach)

**Pros:**
- Fastest query performance
- Simple implementation
- No code changes needed
- Excellent user experience

**Cons:**
- Requires 8-16 GB RAM dedicated to dashboard
- Slow initial load time (10-30 seconds)
- Memory pressure on smaller systems

**Recommended if:**
- Server has 16+ GB RAM
- Fast response times are critical
- User base is small (<10 concurrent users)

### Option 2: Time-Window Loading

Load only recent data (e.g., last 90 days) by default:

```python
def load_generation_data(days_back=90):
    cutoff_date = datetime.now() - timedelta(days=days_back)
    df = pd.read_parquet('gen_output.parquet', 
                        filters=[('settlementdate', '>=', cutoff_date)])
    return df
```

**Pros:**
- Reduces RAM to ~1-2 GB
- Fast initial load
- Covers most use cases

**Cons:**
- Historical analysis requires reload
- Code changes needed
- User experience changes

### Option 3: Lazy Loading with Dask

Replace pandas with Dask for out-of-core computation:

```python
import dask.dataframe as dd

# Load as Dask DataFrame - doesn't load into memory
df = dd.read_parquet('gen_output.parquet')

# Compute only what's needed
recent_data = df[df.settlementdate > cutoff].compute()
```

**Pros:**
- Handles any data size
- Minimal memory usage
- Scales to decades of data

**Cons:**
- Slower operations
- Complex implementation
- Requires code refactoring

### Option 4: Database Backend

Migrate to PostgreSQL/TimescaleDB:

```sql
CREATE TABLE generation (
    settlementdate TIMESTAMP,
    duid VARCHAR(20),
    scadavalue FLOAT,
    PRIMARY KEY (settlementdate, duid)
);

-- Hypertable for time-series optimization
SELECT create_hypertable('generation', 'settlementdate');
```

**Pros:**
- Unlimited data size
- Multiple users
- Advanced queries
- Data integrity

**Cons:**
- Major refactoring
- Database maintenance
- Deployment complexity

## Recommended Implementation Strategy

### Phase 1: Immediate (0-6 months)
- Continue with full loading
- Monitor memory usage
- Add memory alerts at 80% usage

### Phase 2: Short-term (6-12 months)
- Implement time-window loading
- Add date range selector to UI
- Default to last 90 days
- "Load All" button for full dataset

### Phase 3: Long-term (1-2 years)
- Evaluate actual growth rate
- If exceeding 16GB RAM, implement Dask
- Consider database if multiple users

## Code Changes for Time-Window Loading

### 1. Update data loading in gen_dash.py:

```python
class EnergyDashboard(param.Parameterized):
    data_range = param.Selector(
        default="Last 90 Days",
        objects=["Last 7 Days", "Last 30 Days", "Last 90 Days", "Last Year", "All Data"]
    )
    
    def load_data(self):
        # Map selection to days
        days_map = {
            "Last 7 Days": 7,
            "Last 30 Days": 30,
            "Last 90 Days": 90,
            "Last Year": 365,
            "All Data": None
        }
        
        days = days_map[self.data_range]
        
        if days:
            cutoff = pd.Timestamp.now() - pd.Timedelta(days=days)
            # Use pyarrow for efficient filtering
            self.gen_df = pd.read_parquet(
                self.gen_output_file,
                filters=[('settlementdate', '>=', cutoff)]
            )
        else:
            self.gen_df = pd.read_parquet(self.gen_output_file)
```

### 2. Add UI control:

```python
def create_dashboard():
    dashboard = EnergyDashboard()
    
    # Add data range selector
    data_range_select = pn.widgets.Select(
        name='Data Range',
        value=dashboard.data_range,
        options=dashboard.param.data_range.objects
    )
    
    data_range_select.param.watch(
        lambda event: dashboard.update_data_range(event.new),
        'value'
    )
```

## Monitoring Script

Create `monitor_memory.py`:

```python
import psutil
import pandas as pd
from pathlib import Path

def check_dashboard_memory():
    # Get process memory
    process = psutil.Process()
    mem_info = process.memory_info()
    
    # Get data sizes
    gen_file = Path('gen_output.parquet')
    gen_size = gen_file.stat().st_size / (1024**3)  # GB
    
    # Check system memory
    sys_mem = psutil.virtual_memory()
    
    print(f"Dashboard Memory Usage:")
    print(f"  Process RAM: {mem_info.rss / (1024**3):.1f} GB")
    print(f"  Generation file: {gen_size:.1f} GB")
    print(f"  System RAM available: {sys_mem.available / (1024**3):.1f} GB")
    print(f"  System RAM percent: {sys_mem.percent}%")
    
    if sys_mem.percent > 80:
        print("WARNING: High memory usage!")
        
    # Estimate days until problem
    growth_rate_gb_per_day = 0.0006  # From analysis
    days_until_16gb = (16 - gen_size) / growth_rate_gb_per_day
    print(f"  Days until 16GB: {days_until_16gb:.0f}")
```

## Conclusion

For a 5-year dataset:

1. **Storage is not a concern** - only 1.1 GB needed
2. **Memory is manageable** - 8-10 GB RAM handles it well  
3. **Plan for time-window loading** - Implement when data exceeds 2 years
4. **Monitor growth** - Real growth may differ from projections

The current architecture can handle 5 years of data on any modern system with 16GB+ RAM. Implement time-window loading as a user feature rather than a necessity, allowing users to choose between performance and memory usage.