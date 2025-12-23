"""
ETL statistics and metrics endpoint
"""
from datetime import datetime
from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func, and_
from api.dependencies import get_db
from schemas.api import StatsResponse, SourceStatistics, ETLRunSummary
from models.checkpoint import ETLCheckpoint
from models.etl_run import ETLRun, ETLStatus
from models.normalized_data import UnifiedItem
from typing import Optional
import uuid
import logging

logger = logging.getLogger(__name__)
router = APIRouter(tags=["Statistics"])


@router.get("/stats", response_model=StatsResponse)
async def get_stats(
    limit: int = Query(10, ge=1, le=100, description="Number of recent runs to return"),
    db: AsyncSession = Depends(get_db)
):
    """
    Get ETL statistics and metrics.
    
    Returns:
    - Overall summary (total records, runs, etc.)
    - Per-source statistics
    - Recent ETL run history
    """
    request_id = f"req_{uuid.uuid4().hex[:12]}"
    
    logger.info(f"[{request_id}] GET /stats")
    
    # ========== Overall Summary ==========
    
    # Total records
    total_records_result = await db.execute(
        select(func.count()).select_from(UnifiedItem)
    )
    total_records = total_records_result.scalar()
    
    # Total sources
    total_sources_result = await db.execute(
        select(func.count()).select_from(ETLCheckpoint)
    )
    total_sources = total_sources_result.scalar()
    
    # Last success/failure across all sources
    checkpoints_result = await db.execute(select(ETLCheckpoint))
    checkpoints = checkpoints_result.scalars().all()
    
    last_success = None
    last_failure = None
    
    for checkpoint in checkpoints:
        if checkpoint.last_success_at:
            if not last_success or checkpoint.last_success_at > last_success:
                last_success = checkpoint.last_success_at
        if checkpoint.last_failure_at:
            if not last_failure or checkpoint.last_failure_at > last_failure:
                last_failure = checkpoint.last_failure_at
    
    # Total ETL runs
    total_runs_result = await db.execute(
        select(func.count()).select_from(ETLRun)
    )
    total_runs = total_runs_result.scalar()
    
    # Average run duration
    avg_duration_result = await db.execute(
        select(func.avg(ETLRun.duration_seconds)).where(
            and_(
                ETLRun.status == ETLStatus.SUCCESS,
                ETLRun.duration_seconds.isnot(None)
            )
        )
    )
    avg_duration = avg_duration_result.scalar()
    
    summary = {
        "total_records": total_records,
        "total_sources": total_sources,
        "last_success": last_success,
        "last_failure": last_failure,
        "total_etl_runs": total_runs,
        "average_run_duration_seconds": round(avg_duration, 2) if avg_duration else None
    }
    
    # ========== Per-Source Statistics ==========
    
    sources = []
    
    for checkpoint in checkpoints:
        # Count records for this source
        record_count_result = await db.execute(
            select(func.count()).select_from(UnifiedItem).where(
                and_(
                    UnifiedItem.source_type == checkpoint.source_type,
                    UnifiedItem.source_name == checkpoint.source_name
                )
            )
        )
        record_count = record_count_result.scalar()
        
        # Calculate success rate
        if checkpoint.total_runs > 0:
            runs_result = await db.execute(
                select(func.count()).select_from(ETLRun).where(
                    and_(
                        ETLRun.source_type == checkpoint.source_type,
                        ETLRun.source_name == checkpoint.source_name,
                        ETLRun.status == ETLStatus.SUCCESS
                    )
                )
            )
            success_count = runs_result.scalar()
            success_rate = success_count / checkpoint.total_runs
        else:
            success_rate = None
        
        # Average duration for this source
        avg_dur_result = await db.execute(
            select(func.avg(ETLRun.duration_seconds)).where(
                and_(
                    ETLRun.source_type == checkpoint.source_type,
                    ETLRun.source_name == checkpoint.source_name,
                    ETLRun.status == ETLStatus.SUCCESS,
                    ETLRun.duration_seconds.isnot(None)
                )
            )
        )
        avg_source_duration = avg_dur_result.scalar()
        
        sources.append(SourceStatistics(
            source_type=checkpoint.source_type.value,
            source_name=checkpoint.source_name,
            total_records=record_count if record_count else 0,
            active_records=record_count if record_count else 0, # Assuming all are active for now
            last_run_at=checkpoint.last_run_at,
            last_success_at=checkpoint.last_success_at,
            last_failure_at=checkpoint.last_failure_at,
            total_runs=checkpoint.total_runs,
            success_rate=round(success_rate * 100, 1) if success_rate is not None else 0.0, # Convert to percentage
            avg_records_per_run=round(checkpoint.total_records_processed / checkpoint.total_runs, 1) if checkpoint.total_runs > 0 else 0.0
        ))
    
    # ========== Recent ETL Runs ==========
    
    recent_runs_result = await db.execute(
        select(ETLRun)
        .order_by(ETLRun.started_at.desc())
        .limit(limit)
    )
    recent_runs = recent_runs_result.scalars().all()
    
    recent_runs_list = [
        ETLRunSummary(
            run_id=str(run.run_id),
            source_type=run.source_type.value if run.source_type else None,
            completed_at=run.completed_at,
            records_extracted=run.records_extracted or 0,
            records_loaded=run.records_loaded or 0,
            records_failed=run.records_failed or 0
        )
        for run in recent_runs
    ]
    
    logger.info(
        f"[{request_id}] Stats: {total_records} records, "
        f"{total_sources} sources, {total_runs} runs"
    )
    
    return StatsResponse(
        timestamp=datetime.utcnow(),
        total_records=total_records if total_records else 0,
        total_sources=total_sources if total_sources else 0,
        active_sources=total_sources if total_sources else 0, # Assuming active
        records_by_source_type={}, # Placeholder as logic was not implemented
        records_by_category={}, # Placeholder
        source_statistics=sources,
        recent_runs=recent_runs_list,
        last_etl_success=last_success,
        last_etl_failure=last_failure,
        avg_etl_duration_seconds=round(avg_duration, 2) if avg_duration else None,
        request_id=request_id
    )