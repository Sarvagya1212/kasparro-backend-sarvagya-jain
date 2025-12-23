"""
Health check endpoint with database and ETL status
"""

from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, text
from api.dependencies import get_db
from schemas.api import HealthCheckResponse, ETLCheckpointInfo
from models.checkpoint import ETLCheckpoint
from datetime import datetime
import time
import uuid
import logging

logger = logging.getLogger(__name__)
router = APIRouter(tags=["Health"])


@router.get("/health", response_model=HealthCheckResponse)
async def health_check(db: AsyncSession = Depends(get_db)):
    """
    Health check endpoint.
    
    Returns:
    - Database connectivity status
    - ETL checkpoint status for all sources
    - Request metadata
    """
    
    # Check database connectivity
    db_connected = False
    
    try:
        await db.execute(text("SELECT 1"))
        db_connected = True
    except Exception as e:
        logger.error(f"Database connection failed: {str(e)}")
    
    # Get ETL checkpoint status
    etl_checkpoints = []
    total_sources = 0
    successful_sources = 0
    failed_sources = 0
    
    try:
        result = await db.execute(select(ETLCheckpoint))
        checkpoints = result.scalars().all()
        total_sources = len(checkpoints)
        
        for checkpoint in checkpoints:
            status_val = checkpoint.status.value
            if status_val == "failed":
                failed_sources += 1
            elif status_val == "success":
                successful_sources += 1
                
            etl_checkpoints.append(ETLCheckpointInfo(
                source_type=checkpoint.source_type.value,
                source_name=checkpoint.source_name,
                status=status_val,
                last_run_at=checkpoint.last_run_at,
                last_success_at=checkpoint.last_success_at,
                last_failure_at=checkpoint.last_failure_at,
                total_records_processed=checkpoint.total_records_processed,
                last_records_processed=checkpoint.last_records_processed,
                checkpoint_value=checkpoint.checkpoint_value,
                error_message=checkpoint.error_message
            ))
    except Exception as e:
        logger.error(f"Failed to fetch ETL checkpoints: {str(e)}")
    
    # Status calculation is handled by the validator in HealthCheckResponse
    # but we pass the raw values
    
    return HealthCheckResponse(
        status="healthy", # Placeholder, validator will update
        timestamp=datetime.utcnow(),
        database_connected=db_connected,
        etl_checkpoints=etl_checkpoints,
        total_sources=total_sources,
        successful_sources=successful_sources,
        failed_sources=failed_sources
    )

