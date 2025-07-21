"""
Optimized FastAPI endpoints for AEMO data service

Provides RESTful API access to the optimized shared data service.
"""

from fastapi import APIRouter, Query, HTTPException
from fastapi.responses import JSONResponse
from datetime import datetime, timedelta
from typing import List, Optional, Dict, Any
from pydantic import BaseModel, Field

from .shared_data_optimized import optimized_data_service
from aemo_dashboard.shared.logging_config import get_logger

logger = get_logger(__name__)

# Create API router
router = APIRouter(prefix="/api", tags=["data"])


# Pydantic models for request/response validation
class DateRangeParams(BaseModel):
    start_date: datetime = Field(..., description="Start date for data range")
    end_date: datetime = Field(..., description="End date for data range")


class GenerationResponse(BaseModel):
    data: List[Dict[str, Any]]
    count: int
    resolution: str
    aggregation: Optional[str] = None


class PriceResponse(BaseModel):
    data: List[Dict[str, Any]]
    count: int
    regions: List[str]


class RevenueRequest(BaseModel):
    start_date: datetime
    end_date: datetime
    group_by: List[str] = Field(default=['fuel_type', 'region'])


class RevenueResponse(BaseModel):
    data: List[Dict[str, Any]]
    group_by: List[str]
    total_revenue: float
    total_generation_mwh: float


# API Endpoints

@router.get("/health")
async def health_check():
    """Check service health and memory usage"""
    return {
        "status": "healthy",
        "memory_usage_mb": optimized_data_service.get_memory_usage(),
        "optimization": "enabled",
        "data_loaded": {
            "generation_30min": not optimized_data_service.generation_30min.empty,
            "prices_30min": not optimized_data_service.price_30min.empty,
            "transmission_30min": not optimized_data_service.transmission_30min.empty,
            "generation_5min": hasattr(optimized_data_service, 'generation_5min'),
            "prices_5min": hasattr(optimized_data_service, 'price_5min')
        }
    }


@router.get("/metadata")
async def get_metadata():
    """Get metadata about available data"""
    return {
        "date_ranges": optimized_data_service.get_date_ranges(),
        "regions": optimized_data_service.get_regions(),
        "fuel_types": optimized_data_service.get_fuel_types(),
        "update_frequency": "30 minutes",
        "memory_usage_mb": optimized_data_service.get_memory_usage(),
        "optimization_features": {
            "lazy_5min_loading": True,
            "memory_efficient_types": True,
            "on_demand_enrichment": True
        }
    }


@router.get("/generation/by-fuel", response_model=GenerationResponse)
async def get_generation_by_fuel(
    start_date: datetime = Query(..., description="Start date"),
    end_date: datetime = Query(..., description="End date"),
    regions: Optional[List[str]] = Query(None, description="Filter by regions"),
    resolution: str = Query("30min", description="Data resolution: 5min, 30min, hourly, daily")
):
    """
    Get generation data aggregated by fuel type.
    
    Returns generation values grouped by fuel type for the specified
    date range and regions. Uses on-demand enrichment for memory efficiency.
    """
    try:
        # Validate date range
        if end_date < start_date:
            raise HTTPException(400, "End date must be after start date")
        
        # Load 5-minute data if requested and short time range
        if resolution == "5min":
            date_diff = (end_date - start_date).days
            if date_diff > 7:
                logger.warning(f"5-minute data requested for {date_diff} days, using 30-minute instead")
                resolution = "30min"
            else:
                # Load 5-minute data on demand
                optimized_data_service.load_5min_data_on_demand('generation')
        
        # Convert regions list to tuple for caching
        regions_tuple = tuple(regions) if regions else None
        
        # Get data from optimized service
        data = optimized_data_service.get_generation_by_fuel(
            start_date=start_date,
            end_date=end_date,
            regions=regions_tuple,
            resolution=resolution
        )
        
        # Convert to response format
        records = data.to_dict('records')
        
        return GenerationResponse(
            data=records,
            count=len(records),
            resolution=resolution,
            aggregation="fuel_type"
        )
        
    except Exception as e:
        logger.error(f"Error in generation endpoint: {e}")
        raise HTTPException(500, str(e))


@router.get("/prices/regional", response_model=PriceResponse)
async def get_regional_prices(
    start_date: datetime = Query(..., description="Start date"),
    end_date: datetime = Query(..., description="End date"),
    regions: Optional[List[str]] = Query(None, description="Filter by regions")
):
    """
    Get regional electricity prices.
    
    Returns price data for specified regions and time range.
    """
    try:
        # Validate date range
        if end_date < start_date:
            raise HTTPException(400, "End date must be after start date")
        
        # Determine if we need 5-minute data
        date_diff = (end_date - start_date).days
        if date_diff < 1:
            # Load 5-minute prices on demand for short ranges
            optimized_data_service.load_5min_data_on_demand('prices')
            
        # Get data
        data = optimized_data_service.get_regional_prices(
            start_date=start_date,
            end_date=end_date,
            regions=regions,
            resolution='5min' if date_diff < 1 else '30min'
        )
        
        # Convert to response format
        records = data.to_dict('records')
        
        # Get unique regions in the data
        unique_regions = data['regionid'].unique().tolist() if not data.empty else []
        
        return PriceResponse(
            data=records,
            count=len(records),
            regions=unique_regions
        )
        
    except Exception as e:
        logger.error(f"Error in prices endpoint: {e}")
        raise HTTPException(500, str(e))


@router.post("/analysis/revenue", response_model=RevenueResponse)
async def analyze_revenue(request: RevenueRequest):
    """
    Calculate revenue analysis with custom grouping.
    
    Uses on-demand data enrichment to minimize memory usage.
    """
    try:
        # Get revenue data
        data = optimized_data_service.calculate_revenue(
            start_date=request.start_date,
            end_date=request.end_date,
            group_by=request.group_by
        )
        
        if data.empty:
            return RevenueResponse(
                data=[],
                group_by=request.group_by,
                total_revenue=0.0,
                total_generation_mwh=0.0
            )
        
        # Calculate totals
        total_revenue = data['revenue'].sum()
        total_generation_mwh = data['scadavalue'].sum()
        
        # Convert to response format
        records = data.to_dict('records')
        
        return RevenueResponse(
            data=records,
            group_by=request.group_by,
            total_revenue=float(total_revenue),
            total_generation_mwh=float(total_generation_mwh)
        )
        
    except Exception as e:
        logger.error(f"Error in revenue analysis: {e}")
        raise HTTPException(500, str(e))


@router.get("/generation/stations")
async def get_station_generation(
    start_date: datetime = Query(..., description="Start date"),
    end_date: datetime = Query(..., description="End date"),
    station_name: Optional[str] = Query(None, description="Filter by station name"),
    fuel_type: Optional[str] = Query(None, description="Filter by fuel type"),
    region: Optional[str] = Query(None, description="Filter by region")
):
    """
    Get generation data by power station.
    
    This endpoint demonstrates efficient on-demand data filtering
    without pre-computed joins.
    """
    try:
        # Filter generation data
        mask = (
            (optimized_data_service.generation_30min['settlementdate'] >= start_date) &
            (optimized_data_service.generation_30min['settlementdate'] <= end_date)
        )
        data = optimized_data_service.generation_30min[mask].copy()
        
        if data.empty:
            return {"data": [], "count": 0}
        
        # Add station info on-demand
        if optimized_data_service.duid_mapping.index.name == 'DUID':
            data['station_name'] = data['duid'].map(
                optimized_data_service.duid_mapping['Station Name']
            )
            data['fuel_type'] = data['duid'].map(
                optimized_data_service.duid_mapping['Fuel']
            )
            data['region'] = data['duid'].map(
                optimized_data_service.duid_mapping['Region']
            )
        
        # Apply filters
        if station_name:
            data = data[data['station_name'] == station_name]
        if fuel_type:
            data = data[data['fuel_type'] == fuel_type]
        if region:
            data = data[data['region'] == region]
        
        # Aggregate by station
        result = data.groupby(['settlementdate', 'station_name', 'fuel_type', 'region'])[
            'scadavalue'
        ].sum().reset_index()
        
        records = result.to_dict('records')
        
        return {
            "data": records,
            "count": len(records),
            "filters": {
                "station_name": station_name,
                "fuel_type": fuel_type,
                "region": region
            }
        }
        
    except Exception as e:
        logger.error(f"Error in station generation endpoint: {e}")
        raise HTTPException(500, str(e))


@router.get("/transmission/flows")
async def get_transmission_flows(
    start_date: datetime = Query(..., description="Start date"),
    end_date: datetime = Query(..., description="End date"),
    interconnector_id: Optional[str] = Query(None, description="Filter by interconnector")
):
    """Get transmission flow data"""
    try:
        # Filter transmission data
        mask = (
            (optimized_data_service.transmission_30min['settlementdate'] >= start_date) &
            (optimized_data_service.transmission_30min['settlementdate'] <= end_date)
        )
        data = optimized_data_service.transmission_30min[mask]
        
        # Apply interconnector filter if specified
        if interconnector_id:
            data = data[data['interconnectorid'] == interconnector_id]
        
        # Select relevant columns
        columns = ['settlementdate', 'interconnectorid', 'meteredmwflow', 
                  'mwflow', 'exportlimit', 'importlimit']
        result = data[columns].copy()
        
        records = result.to_dict('records')
        
        return {
            "data": records,
            "count": len(records),
            "interconnector_id": interconnector_id
        }
        
    except Exception as e:
        logger.error(f"Error in transmission flows endpoint: {e}")
        raise HTTPException(500, str(e))


# Export router
__all__ = ['router']