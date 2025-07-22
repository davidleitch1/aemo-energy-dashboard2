# AEMO Dashboard: Shared Data Service Architecture

## Overview

This document outlines the architectural transformation of the AEMO Energy Dashboard from a file-based, per-user data loading system to a shared data service architecture using FastAPI and Panel's native integration.

## Current Architecture Issues

1. **Per-User Memory Usage**: Each dashboard instance loads ~200MB+ of data
2. **Slow Initial Load**: 10-15 seconds to load all parquet files
3. **No Data Sharing**: Multiple browser tabs = multiple data copies
4. **Limited Scalability**: RAM usage grows linearly with users

## Proposed Architecture

### Core Components

```
┌─────────────────────────────────────┐
│    FastAPI + Panel Application      │
│                                     │
│  ┌─────────────────────────────┐   │
│  │   Shared Data Service        │   │
│  │   (Singleton Pattern)        │   │
│  │                              │   │
│  │  • Loads data ONCE at startup│   │
│  │  • ~200MB in memory          │   │
│  │  • Serves all users          │   │
│  └──────────┬──────────────────┘   │
│             │                       │
│  ┌──────────┴──────────────────┐   │
│  │      FastAPI Endpoints       │   │
│  │  /api/generation/by-fuel     │   │
│  │  /api/prices/regional        │   │
│  │  /api/analysis/revenue       │   │
│  └──────────┬──────────────────┘   │
│             │                       │
│  ┌──────────┴──────────────────┐   │
│  │    Panel Dashboards          │   │
│  │  /dashboard/generation       │   │
│  │  /dashboard/prices          │   │
│  │  /dashboard/station         │   │
│  └─────────────────────────────┘   │
└─────────────────────────────────────┘
```

### Key Benefits

1. **Memory Efficiency**: 200MB total vs 200MB per user
2. **Fast Response**: Data already in memory, no file I/O
3. **Shared Resources**: All users share same data instance
4. **Native Integration**: Panel runs on same FastAPI server

## Implementation Plan

### Phase 1: Core Data Service (2-3 days)

1. **Create Shared Data Service Class**
   ```python
   class SharedDataService:
       def __init__(self):
           # Load all parquet files once
           self.generation_data = pd.read_parquet('scada30.parquet')
           self.price_data = pd.read_parquet('prices30.parquet')
           self.duid_mapping = pickle.load('gen_info.pkl')
   ```

2. **Implement FastAPI Endpoints**
   - `/api/generation/by-fuel` - Aggregated generation data
   - `/api/prices/regional` - Regional price data
   - `/api/analysis/revenue` - Revenue calculations
   - `/api/metadata` - Data availability info

3. **Add Data Filtering & Aggregation**
   - Date range filtering in memory
   - Smart aggregation for large ranges
   - Efficient pandas operations

### Phase 2: Panel Integration (2-3 days)

1. **Convert Generation Dashboard**
   - Use `@add_application` decorator
   - Replace file loading with shared data service
   - Maintain existing functionality

2. **Convert Price Analysis Dashboard**
   - Adapt to use API endpoints
   - Keep existing UI components
   - Add error handling

3. **Convert Station Analysis**
   - Most complex due to data integration
   - May need custom endpoints

### Phase 3: Testing & Optimization (1-2 days)

1. **Performance Testing**
   - Multiple concurrent users
   - Memory usage monitoring
   - Response time benchmarks

2. **Add Caching Layer**
   - Cache common queries
   - TTL-based invalidation
   - Memory-efficient caching

3. **Error Handling**
   - Graceful degradation
   - User-friendly error messages
   - Automatic retries

## Technical Decisions

### Why FastAPI + Panel Native Integration?

1. **Single Process**: No separate servers to manage
2. **Shared Memory**: Direct access to data structures
3. **Modern Stack**: Both actively developed
4. **Auto Documentation**: FastAPI provides /docs automatically
5. **Type Safety**: Pydantic validation built-in

### Data Loading Strategy

1. **Startup Loading**: Load all data when service starts
2. **In-Memory Operations**: All filtering/aggregation in pandas
3. **No Lazy Loading Initially**: Keep it simple for now
4. **Pre-Aggregation**: Calculate common views at startup

### API Design Principles

1. **RESTful Endpoints**: Standard HTTP verbs and patterns
2. **Query Parameters**: For filtering and aggregation
3. **JSON Responses**: Easy to consume
4. **Pagination Ready**: Structure supports future pagination

## Migration Strategy

### Step 1: Parallel Development
- Build new service alongside existing dashboard
- No changes to current dashboard initially
- Test with subset of functionality

### Step 2: Gradual Migration
- Start with Generation tab
- Move to Price Analysis
- Finally, complex Station Analysis

### Step 3: Deployment
- Single service to run
- Simple deployment: `uvicorn app:app`
- Works on local machine or server

## Code Structure

```
aemo-energy-dashboard/
├── src/
│   ├── data_service/
│   │   ├── __init__.py
│   │   ├── shared_data.py      # SharedDataService class
│   │   ├── api_endpoints.py    # FastAPI routes
│   │   └── panel_apps.py       # Panel dashboards
│   │
│   ├── aemo_dashboard/         # Existing code (gradual migration)
│   │   └── ...
│   │
│   └── main.py                 # FastAPI app entry point
│
├── tests/
│   ├── test_data_service.py
│   └── test_endpoints.py
│
└── requirements.txt            # Add: fastapi, uvicorn
```

## Performance Expectations

### Current System
- Initial Load: 10-15 seconds
- Per User RAM: 200MB+
- Concurrent Users: Limited by RAM

### New System
- Initial Load: < 1 second (after service starts)
- Total RAM: ~250MB (including overhead)
- Concurrent Users: 100s possible

## Security Considerations

1. **Read-Only Access**: No data modification endpoints
2. **Rate Limiting**: Prevent abuse (optional)
3. **CORS Configuration**: For browser access
4. **No Authentication**: Not needed for personal use

## Future Enhancements

1. **WebSocket Support**: Real-time data updates
2. **Data Refresh**: Periodic reload from files
3. **Query Optimization**: Database backend for complex queries
4. **Deployment Options**: Docker, cloud services

## Next Steps

1. Review and approve architecture
2. Set up FastAPI development environment
3. Implement Phase 1 (Core Data Service)
4. Test with simple endpoints
5. Begin Panel integration

This architecture provides a solid foundation for current needs while being extensible for future requirements. The shared data service pattern is a standard solution for data-intensive web applications.