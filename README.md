# AEMO Energy Dashboard

Comprehensive Australian Energy Market (AEMO) electricity analysis platform with real-time data collection, multi-tab visualization, and advanced station analysis capabilities.

## 🎯 **Dashboard Overview**

**"Nem Analysis"** - A professional energy market analysis platform featuring:

- **4 Main Analysis Tabs**: Generation by Fuel, Average Price Analysis, Station Analysis, plus planned extensions
- **Real-time Data Integration**: 5-minute AEMO generation and price data with 2.7M+ integrated records
- **Advanced Station Analysis**: Individual unit or whole station aggregation with dual-axis charts
- **Professional UI**: Material Design dark theme with interactive visualizations

## ✨ **Core Features**

### **🔥 Generation by Fuel Analysis**
- **Interactive Stacked Charts**: Real-time generation by fuel type (Coal, Gas, Solar, Wind, etc.)
- **Capacity Utilization**: Percentage utilization by fuel type with AEMO-verified capacity data
- **Regional Analysis**: Filter by NEM regions (NSW1, QLD1, SA1, TAS1, VIC1, or whole NEM)
- **Dual Layout**: Region selector on left, chart subtabs on right

### **💰 Average Price Analysis** 
- **Custom Pivot Builder**: User-driven grouping by Region and Fuel combinations
- **Hierarchical Tables**: Expandable fuel groups showing individual generation units (DUIDs)
- **Multi-Column Analysis**: Generation (GWh), Revenue ($M), Price ($/MWh), Utilization (%), Capacity (MW)
- **Flexible Filtering**: Date ranges, region selection, fuel type filtering
- **Professional Tabulator**: Dark theme with sorting and expandable rows

### **🏭 Station Analysis - Advanced Features**
- **Station vs DUID Toggle**: Analyze whole power stations or individual generating units
- **Smart Station Discovery**: Automatic grouping using DUID naming patterns (ER01,ER02,ER03,ER04 → "Eraring")
- **Multi-Unit Aggregation**: Combines data from all units (e.g., Eraring 4×720MW = 2,880MW total)
- **Dual-Axis Charts**: Generation (MW) on left axis, Price ($/MWh) on right axis
- **Time Series Analysis**: Smart hourly resampling for optimal performance
- **Time-of-Day Patterns**: 24-hour performance profiles with dual-axis visualization
- **Performance Metrics**: Revenue, capacity factor, operating hours, peak values
- **Fuzzy Search**: Find stations by name or DUID with auto-suggestions

### **📊 Data & Performance**
- **Real-time Updates**: Auto-refresh every 4.5 minutes
- **Efficient Storage**: Parquet files for historical data with optimal compression
- **Smart Resampling**: Automatic data aggregation for optimal chart performance
- **Robust Integration**: Links generation, price, and station metadata across 528 DUIDs

## 🚀 **Quick Start**

### Prerequisites
- Python 3.10 or higher
- [uv package manager](https://github.com/astral-sh/uv)

### Installation

1. **Clone and setup**
   ```bash
   git clone https://github.com/davidleitch1/aemo-energy-dashboard.git
   cd aemo-energy-dashboard
   uv sync
   ```

2. **Configure environment**
   ```bash
   cp .env.sample .env
   # Edit .env with your email credentials and data paths
   ```

3. **Start the dashboard**
   ```bash
   cd src
   uv run python -m aemo_dashboard.generation.gen_dash
   ```

4. **Access dashboard**
   ```
   http://localhost:5010
   ```

## 📋 **Dashboard Tabs**

### **Tab 1: Generation by Fuel**
- **Purpose**: Real-time generation mix analysis by fuel type
- **Layout**: Region selector (left) + Chart subtabs (right)
- **Subtabs**: Generation Stack, Capacity Utilization
- **Data**: 5-minute SCADA generation data

### **Tab 2: Average Price Analysis**  
- **Purpose**: Custom pivot table analysis of generation and revenue
- **Features**: Hierarchical grouping, multi-column selection, date filtering
- **Use Cases**: Fuel type comparison, regional analysis, revenue insights

### **Tab 3: Station Analysis**
- **Purpose**: Individual power station performance analysis
- **Modes**: Station aggregation (multiple DUIDs) or individual unit analysis
- **Charts**: Time series (dual-axis), Time-of-day patterns, Summary statistics
- **Examples**: Eraring (4 units, 2,880MW) vs ER01 (single unit, 720MW)

### **Tab 4+: Planned Extensions**
- **Transmission Tab**: Interconnector flow analysis
- **Comparison Tab**: Side-by-side station/region comparison
- **Summary Tab**: High-level dashboard with key trends

## 🛠 **Configuration**

### Data File Paths
```bash
# Main data directory
DATA_DIR=/path/to/your/aemo/data

# Required data files
GEN_INFO_FILE=/path/to/gen_info.pkl       # DUID mapping (528 stations)
GEN_OUTPUT_FILE=/path/to/gen_output.parquet  # Generation time series
SPOT_HIST_FILE=/path/to/spot_hist.parquet    # Price time series
```

### Email Alert System
```bash
# Email configuration for DUID alerts
ALERT_EMAIL=your-email@icloud.com
ALERT_PASSWORD=your-app-specific-password
SMTP_SERVER=smtp.mail.me.com
SMTP_PORT=587

# Alert behavior
ENABLE_EMAIL_ALERTS=true
ALERT_COOLDOWN_HOURS=24
```

### Dashboard Settings
```bash
# Dashboard configuration
DEFAULT_REGION=NEM
DASHBOARD_PORT=5010
UPDATE_INTERVAL_MINUTES=4.5
LOG_LEVEL=INFO
```

## 📁 **Project Structure**

```
aemo-energy-dashboard/
├── src/aemo_dashboard/
│   ├── generation/              # Main dashboard and generation analysis
│   │   └── gen_dash.py         # Primary dashboard application
│   ├── analysis/               # Price analysis components
│   │   └── price_analysis_ui.py # Custom pivot table builder
│   ├── station/                # Station analysis module
│   │   ├── station_analysis.py    # Data processing engine
│   │   ├── station_analysis_ui.py # UI components
│   │   └── station_search.py      # Fuzzy search functionality
│   ├── spot_prices/            # Price monitoring
│   │   ├── update_spot.py      # Price data collection
│   │   └── display_spot.py     # Price dashboard
│   └── shared/                 # Common utilities
│       ├── config.py           # Configuration management
│       ├── logging_config.py   # Unified logging
│       └── email_alerts.py     # Alert system
├── data/                       # Data files (gitignored)
├── logs/                       # Application logs
└── pyproject.toml             # Dependencies and project config
```

## 🔧 **Advanced Usage**

### Station vs DUID Analysis

**Station Mode** (Multi-unit aggregation):
```python
# Example: Eraring Power Station
- DUIDs: ER01, ER02, ER03, ER04
- Total Capacity: 4 × 720MW = 2,880MW
- Analysis: Combined generation, revenue, and performance
```

**DUID Mode** (Individual unit):
```python
# Example: Eraring Unit 1
- DUID: ER01
- Capacity: 720MW
- Analysis: Single unit performance and economics
```

### Search Functionality
- **Fuzzy Matching**: Type "erag" to find "Eraring"
- **DUID Search**: Direct lookup by unit identifier
- **Popular Stations**: Quick access to major generators
- **Auto-suggestions**: Real-time dropdown with matching results

### Chart Interactions
- **Dual-Axis**: Generation (MW) and Price ($/MWh) on same chart
- **Zoom & Pan**: Interactive chart navigation
- **Hover Tooltips**: Detailed data on mouse hover
- **Legend Toggle**: Show/hide data series
- **Date Controls**: Last 7/30 days, custom ranges

## 📈 **Data Sources & Integration**

### AEMO NEM Web
- **Dispatch Data**: 5-minute generation and price data
- **Market Data**: Regional reference prices (RRP)
- **Station Metadata**: DUID mappings, capacities, fuel types

### Data Pipeline
1. **Collection**: Automated download from AEMO every 5 minutes
2. **Processing**: Data cleaning, validation, and integration
3. **Storage**: Efficient parquet format for time series data
4. **Analysis**: Real-time aggregation and visualization

### Performance Metrics
- **Data Volume**: 2.7M+ integrated generation+price records
- **Stations**: 528 DUIDs across 420+ power stations
- **Update Frequency**: 5-minute real-time data with 4.5-minute refresh
- **Response Time**: Sub-second chart updates with smart caching

## 🎨 **User Interface**

### Design System
- **Theme**: Material Design dark theme
- **Typography**: Professional contrast with white titles
- **Layout**: Responsive design with left controls + right charts
- **Interactions**: Smooth transitions and real-time updates

### Accessibility
- **Color Coding**: Distinct colors for different data series
- **Tooltips**: Detailed information on hover
- **Navigation**: Intuitive tab structure and controls
- **Performance**: Optimized for large datasets

## 🔍 **Troubleshooting**

### Dashboard Issues
```bash
# Check if dashboard is running
lsof -i:5010

# Restart dashboard
pkill -f "python -m aemo_dashboard"
cd src && uv run python -m aemo_dashboard.generation.gen_dash

# Check logs
tail -f logs/aemo_dashboard.log
```

### Data Issues
- **Missing stations**: Check gen_info.pkl has latest DUID mappings
- **No price data**: Verify spot_hist.parquet file exists and is current
- **Empty charts**: Ensure date range has available data

### Performance
- **Slow loading**: Check data file sizes and available memory
- **Chart lag**: Reduce date range or enable smart resampling
- **Search issues**: Verify fuzzy search dependencies (fuzzywuzzy)

## 🎯 **Planned Features**

### Generation Tab Enhancements
- **Rooftop Solar**: 30-minute distributed generation data
- **Transmission Flows**: Interconnector flows as virtual "fuel" type

### New Tabs
- **Transmission Analysis**: Dedicated interconnector flow visualization
- **Station Comparison**: Side-by-side performance analysis
- **Market Summary**: High-level trends and key metrics

### Data Sources
- **DISPATCHINTERCONNECTORRES**: 5-minute transmission flow data
- **30-minute Rooftop Solar**: AEMO distributed generation
- **Forecast Accuracy**: Generation vs forecast analysis

## 🤝 **Contributing**

1. Fork the repository
2. Create feature branch: `git checkout -b feature/amazing-feature`
3. Commit changes: `git commit -m 'Add amazing feature'`
4. Push to branch: `git push origin feature/amazing-feature`
5. Open Pull Request

## 📄 **License**

MIT License - see LICENSE file for details.

## 📧 **Support**

- **Issues**: [GitHub Issues](https://github.com/davidleitch1/aemo-energy-dashboard/issues)
- **Discussions**: [GitHub Discussions](https://github.com/davidleitch1/aemo-energy-dashboard/discussions)
- **Documentation**: See CLAUDE.md for development notes