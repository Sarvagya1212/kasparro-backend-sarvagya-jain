"""
Data retrieval endpoint with pagination and filtering
"""

from fastapi import APIRouter, Depends, Query, Request
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func, and_, or_
from api.dependencies import get_db
from schemas.api import DataResponse, UnifiedItemResponse, PaginationMetadata, APIResponse
from models.normalized_data import UnifiedItem
from models.raw_data import SourceType
from models.normalized_data import ItemStatus
from typing import Optional
from datetime import datetime
import time
import uuid
import math
import logging

logger = logging.getLogger(__name__)
router = APIRouter(tags=["Data"])


@router.get("/data", response_model=DataResponse)
async def get_data(
    request: Request,
    page: int = Query(1, ge=1, description="Page number"),
    page_size: int = Query(50, ge=1, le=1000, description="Items per page"),
    source_type: Optional[SourceType] = Query(None, description="Filter by source type"),
    source_name: Optional[str] = Query(None, description="Filter by source name"),
    category: Optional[str] = Query(None, description="Filter by category"),
    status: Optional[ItemStatus] = Query(None, description="Filter by status"),
    search: Optional[str] = Query(None, description="Search in title and description"),
    min_amount: Optional[float] = Query(None, ge=0, description="Minimum amount"),
    max_amount: Optional[float] = Query(None, ge=0, description="Maximum amount"),
    published_after: Optional[datetime] = Query(None, description="Published after date"),
    published_before: Optional[datetime] = Query(None, description="Published before date"),
    db: AsyncSession = Depends(get_db)
):
    """
    Retrieve paginated and filtered data from UnifiedItem table.
    
    Features:
    - Pagination
    - Multiple filter options
    - Full-text search
    - Performance metrics
    """
    start_time = time.time()
    request_id = getattr(request.state, "request_id", f"req_{uuid.uuid4().hex[:12]}")
    
    logger.info(
        f"[{request_id}] GET /data - page={page}, page_size={page_size}, "
        f"filters: source_type={source_type}, category={category}, search={search}"
    )
    
    # Build query with filters
    query = select(UnifiedItem)
    filters = []
    filters_applied = []
    
    if source_type:
        filters.append(UnifiedItem.source_type == source_type)
        filters_applied.append("source_type")
    
    if source_name:
        filters.append(UnifiedItem.source_name == source_name)
        filters_applied.append("source_name")
    
    if category:
        filters.append(UnifiedItem.category == category)
        filters_applied.append("category")
    
    if status:
        filters.append(UnifiedItem.status == status)
        filters_applied.append("status")
    
    if search:
        search_filter = or_(
            UnifiedItem.title.ilike(f"%{search}%"),
            UnifiedItem.description.ilike(f"%{search}%")
        )
        filters.append(search_filter)
        filters_applied.append("search")
    
    if min_amount is not None:
        filters.append(UnifiedItem.amount >= min_amount)
        filters_applied.append("min_amount")
    
    if max_amount is not None:
        filters.append(UnifiedItem.amount <= max_amount)
        filters_applied.append("max_amount")
    
    if published_after:
        filters.append(UnifiedItem.published_at >= published_after)
        filters_applied.append("published_after")
    
    if published_before:
        filters.append(UnifiedItem.published_at <= published_before)
        filters_applied.append("published_before")
    
    # Apply filters
    if filters:
        query = query.where(and_(*filters))
    
    # Get total count
    count_query = select(func.count()).select_from(UnifiedItem)
    if filters:
        count_query = count_query.where(and_(*filters))
    
    count_result = await db.execute(count_query)
    total_items = count_result.scalar()
    
    # Calculate pagination
    total_pages = math.ceil(total_items / page_size) if total_items > 0 else 0
    offset = (page - 1) * page_size
    
    # Apply pagination and ordering
    query = query.order_by(UnifiedItem.created_at.desc())
    query = query.offset(offset).limit(page_size)
    
    # Execute query
    query_start = time.time()
    result = await db.execute(query)
    items = result.scalars().all()
    query_time_ms = (time.time() - query_start) * 1000
    
    # Convert to response models
    data = [UnifiedItemResponse.from_orm(item) for item in items]
    
    # Calculate API latency
    api_latency_ms = (time.time() - start_time) * 1000
    
    logger.info(
        f"[{request_id}] Returned {len(data)} items "
        f"(query: {query_time_ms:.2f}ms, total: {api_latency_ms:.2f}ms)"
    )
    
    return DataResponse(
        items=data,
        pagination=PaginationMetadata(
            current_page=page,
            page_size=page_size,
            total_items=total_items,
            total_pages=total_pages,
            has_next=page < total_pages,
            has_previous=page > 1
        ),
        filters_applied={k: v for k, v in {
            "source_type": source_type,
            "source_name": source_name,
            "category": category,
            "status": status,
            "search": search,
            "min_amount": min_amount,
            "max_amount": max_amount,
            "published_after": published_after,
            "published_before": published_before
        }.items() if v is not None}
    )