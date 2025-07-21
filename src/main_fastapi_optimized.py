"""
AEMO Energy Dashboard - Optimized FastAPI Application

Main entry point for the FastAPI server with optimized memory usage.
"""

import os
import sys
from pathlib import Path

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent))

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import RedirectResponse
import panel as pn

# Import optimized data service components
from data_service.shared_data_optimized import optimized_data_service
from data_service.api_endpoints_optimized import router as api_router

# Import logging
from aemo_dashboard.shared.logging_config import get_logger

logger = get_logger(__name__)

# Enable Panel extensions
pn.extension('tabulator', 'bokeh')

# Create FastAPI app
app = FastAPI(
    title="AEMO Energy Dashboard API (Optimized)",
    description="Memory-optimized RESTful API for Australian Energy Market data",
    version="2.0.1"
)

# Add CORS middleware for development
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Configure appropriately for production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include API routes
app.include_router(api_router)

# Initialize data service (happens once at startup)
@app.on_event("startup")
async def startup_event():
    """Initialize optimized shared data service on startup"""
    logger.info("Starting AEMO Dashboard Optimized FastAPI server...")
    # Force initialization of singleton
    data_service = optimized_data_service
    logger.info(f"Optimized data service initialized. Memory: {data_service.get_memory_usage():.1f} MB")

# Root endpoint
@app.get("/")
async def root():
    """Redirect to API documentation"""
    return RedirectResponse(url="/docs")

# API information endpoint
@app.get("/info")
async def api_info():
    """Get API and dashboard information"""
    return {
        "title": "AEMO Energy Dashboard (Optimized)",
        "version": "2.0.1",
        "description": "Memory-optimized shared data service architecture",
        "memory_usage_mb": optimized_data_service.get_memory_usage(),
        "endpoints": {
            "api_docs": "/docs",
            "api_redoc": "/redoc",
            "health": "/api/health",
            "metadata": "/api/metadata"
        },
        "data_endpoints": {
            "generation_by_fuel": "/api/generation/by-fuel",
            "regional_prices": "/api/prices/regional",
            "revenue_analysis": "/api/analysis/revenue",
            "station_generation": "/api/generation/stations",
            "transmission_flows": "/api/transmission/flows"
        },
        "optimizations": {
            "lazy_loading": "5-minute data loaded only when needed",
            "memory_efficient_types": "float32, categories used",
            "on_demand_enrichment": "No pre-computed joins",
            "expected_memory": "200-300 MB (vs 21 GB original)"
        }
    }


# For development - run with: python main_fastapi_optimized.py
if __name__ == "__main__":
    import uvicorn
    
    # Get port from environment or use default
    port = int(os.getenv("FASTAPI_PORT", "8001"))  # Different port for testing
    host = os.getenv("FASTAPI_HOST", "0.0.0.0")
    
    logger.info(f"Starting optimized server on {host}:{port}")
    logger.info("API docs available at: http://localhost:8001/docs")
    
    # Run the server
    uvicorn.run(
        app,  # Pass the app directly, not as string
        host=host,
        port=port,
        reload=False,  # Disable auto-reload for testing
        log_level="info"
    )