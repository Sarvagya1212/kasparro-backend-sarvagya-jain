
"""
FastAPI application initialization
"""

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from api.routes import health, data, stats
from core.config import settings
import logging
from api.middleware import RequestContextMiddleware
from ingestion.scheduler import ETLScheduler

# Configure logging
logging.basicConfig(
    level=getattr(logging, settings.LOG_LEVEL),
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)

logger = logging.getLogger(__name__)

# Create FastAPI app
app = FastAPI(
    title="Kasparro ETL Backend API",
    description="Backend service for ETL data ingestion and retrieval",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc"
)

# Add CORS middleware
app.add_middleware(RequestContextMiddleware)

# Initialize Scheduler
scheduler = ETLScheduler()


# Include routers
app.include_router(health.router)
app.include_router(data.router)
app.include_router(stats.router)


@app.on_event("startup")
async def startup_event():
    """Application startup event"""
    logger.info("Starting Kasparro ETL Backend API")
    logger.info(f"Environment: {settings.ENVIRONMENT}")
    logger.info(f"Database: {settings.DATABASE_URL.split('@')[1] if '@' in settings.DATABASE_URL else 'configured'}")
    
    # Start Scheduler
    scheduler.start()


@app.on_event("shutdown")
async def shutdown_event():
    """Application shutdown event"""
    logger.info("Shutting down Kasparro ETL Backend API")
    scheduler.stop()


@app.get("/")
async def root():
    """Root endpoint"""
    return {
        "message": "Kasparro ETL Backend API",
        "version": "1.0.0",
        "docs": "/docs",
        "health": "/health",
        "endpoints": {
            "data": "/data",
            "stats": "/stats"
        }
    }
