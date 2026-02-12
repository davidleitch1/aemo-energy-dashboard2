"""
FastAPI endpoints for AEMO data service

Provides RESTful API access to the shared data service.
"""

from fastapi import APIRouter, Query, HTTPException
from fastapi.responses import JSONResponse
from datetime import datetime, timedelta
from typing import List, Optional, Dict, Any
from pydantic import BaseModel, Field

from .shared_data import data_service
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
        "memory_usage_mb": data_service.get_memory_usage(),
        "data_loaded": {
            "generation": not data_service.generation_30min.empty,
            "prices": not data_service.price_30min.empty,
            "transmission": not data_service.transmission_30min.empty,
        }
    }


@router.get("/metadata")
async def get_metadata():
    """Get metadata about available data"""
    return {
        "date_ranges": data_service.get_date_ranges(),
        "regions": data_service.get_regions(),
        "fuel_types": data_service.get_fuel_types(),
        "update_frequency": "30 minutes",
        "memory_usage_mb": data_service.get_memory_usage()
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
    date range and regions.
    """
    try:
        # Validate date range
        if end_date < start_date:
            raise HTTPException(400, "End date must be after start date")
        
        # Convert regions list to tuple for caching
        regions_tuple = tuple(regions) if regions else None
        
        # Get data from service
        data = data_service.get_generation_by_fuel(
            start_date, end_date, regions_tuple, resolution
        )
        
        # Convert DataFrame to dict for JSON response
        records = data.to_dict(orient='records')
        
        # Convert timestamps to ISO format
        for record in records:
            if 'settlementdate' in record:
                record['settlementdate'] = record['settlementdate'].isoformat()
        
        return GenerationResponse(
            data=records,
            count=len(records),
            resolution=resolution,
            aggregation="fuel_type"
        )
        
    except Exception as e:
        logger.error(f"Error in get_generation_by_fuel: {e}")
        raise HTTPException(500, f"Internal server error: {str(e)}")


@router.get("/prices/regional", response_model=PriceResponse)
async def get_regional_prices(
    start_date: datetime = Query(..., description="Start date"),
    end_date: datetime = Query(..., description="End date"),
    regions: Optional[List[str]] = Query(None, description="Filter by regions"),
    resolution: str = Query("30min", description="Data resolution: 5min, 30min")
):
    """
    Get regional electricity prices.
    
    Returns RRP (Regional Reference Price) data for specified regions
    and date range.
    """
    try:
        # Validate date range
        if end_date < start_date:
            raise HTTPException(400, "End date must be after start date")
        
        # Get data from service
        data = data_service.get_regional_prices(
            start_date, end_date, regions, resolution
        )
        
        # Convert DataFrame to dict
        records = data.to_dict(orient='records')
        
        # Convert timestamps to ISO format
        for record in records:
            if 'settlementdate' in record:
                record['settlementdate'] = record['settlementdate'].isoformat()
        
        # Get actual regions in response
        response_regions = data['regionid'].unique().tolist() if not data.empty else []
        
        return PriceResponse(
            data=records,
            count=len(records),
            regions=response_regions
        )
        
    except Exception as e:
        logger.error(f"Error in get_regional_prices: {e}")
        raise HTTPException(500, f"Internal server error: {str(e)}")


@router.post("/analysis/revenue", response_model=RevenueResponse)
async def calculate_revenue(request: RevenueRequest):
    """
    Calculate revenue analysis with custom grouping.
    
    This endpoint performs complex calculations joining generation
    and price data to calculate revenues.
    """
    try:
        # Validate date range
        if request.end_date < request.start_date:
            raise HTTPException(400, "End date must be after start date")
        
        # Calculate revenue
        data = data_service.calculate_revenue(
            request.start_date,
            request.end_date,
            request.group_by
        )
        
        if data.empty:
            return RevenueResponse(
                data=[],
                group_by=request.group_by,
                total_revenue=0.0,
                total_generation_mwh=0.0
            )
        
        # Convert DataFrame to dict
        records = data.to_dict(orient='records')
        
        # Calculate totals
        total_revenue = float(data['revenue'].sum())
        total_generation_mwh = float(data['scadavalue'].sum() / 2)  # Convert to MWh
        
        return RevenueResponse(
            data=records,
            group_by=request.group_by,
            total_revenue=total_revenue,
            total_generation_mwh=total_generation_mwh
        )
        
    except Exception as e:
        logger.error(f"Error in calculate_revenue: {e}")
        raise HTTPException(500, f"Internal server error: {str(e)}")


@router.get("/generation/stations")
async def get_station_generation(
    start_date: datetime = Query(..., description="Start date"),
    end_date: datetime = Query(..., description="End date"),
    fuel_type: Optional[str] = Query(None, description="Filter by fuel type"),
    region: Optional[str] = Query(None, description="Filter by region"),
    limit: int = Query(50, description="Maximum number of stations to return")
):
    """
    Get generation by individual stations (DUIDs).
    
    Returns top generating stations for the specified criteria.
    """
    try:
        # Filter enriched generation data
        mask = (
            (data_service.generation_enriched['settlementdate'] >= start_date) &
            (data_service.generation_enriched['settlementdate'] <= end_date)
        )
        filtered = data_service.generation_enriched[mask]
        
        # Apply additional filters
        if fuel_type and 'fuel_type' in filtered.columns:
            filtered = filtered[filtered['fuel_type'] == fuel_type]
        
        if region and 'region' in filtered.columns:
            filtered = filtered[filtered['region'] == region]
        
        # Aggregate by station
        station_gen = filtered.groupby(['duid', 'Site Name', 'fuel_type', 'region']).agg({
            'scadavalue': 'sum',
            'Capacity(MW)': 'first'
        }).round(2)
        
        # Sort by total generation and limit
        station_gen = station_gen.sort_values('scadavalue', ascending=False).head(limit)
        
        # Reset index and convert to dict
        result = station_gen.reset_index()
        records = result.to_dict(orient='records')
        
        return {
            "data": records,
            "count": len(records),
            "filters": {
                "fuel_type": fuel_type,
                "region": region,
                "date_range": f"{start_date.date()} to {end_date.date()}"
            }
        }
        
    except Exception as e:
        logger.error(f"Error in get_station_generation: {e}")
        raise HTTPException(500, f"Internal server error: {str(e)}")


@router.get("/transmission/flows")
async def get_transmission_flows(
    start_date: datetime = Query(..., description="Start date"),
    end_date: datetime = Query(..., description="End date"),
    interconnector: Optional[str] = Query(None, description="Filter by interconnector")
):
    """Get transmission flow data for interconnectors"""
    try:
        # Filter transmission data
        mask = (
            (data_service.transmission_30min['settlementdate'] >= start_date) &
            (data_service.transmission_30min['settlementdate'] <= end_date)
        )
        filtered = data_service.transmission_30min[mask]
        
        if interconnector:
            filtered = filtered[filtered['interconnectorid'] == interconnector]
        
        # Convert to dict
        records = filtered.to_dict(orient='records')
        
        # Convert timestamps
        for record in records:
            if 'settlementdate' in record:
                record['settlementdate'] = record['settlementdate'].isoformat()
        
        return {
            "data": records,
            "count": len(records),
            "interconnectors": filtered['interconnectorid'].unique().tolist() if not filtered.empty else []
        }
        
    except Exception as e:
        logger.error(f"Error in get_transmission_flows: {e}")
        raise HTTPException(500, f"Internal server error: {str(e)}")