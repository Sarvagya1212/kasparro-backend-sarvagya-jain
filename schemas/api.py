"""
Pydantic schemas for API request/response models
"""

from pydantic import BaseModel, Field, validator
from typing import Optional, List, Dict, Any, Generic, TypeVar
from datetime import datetime
from models.raw_data import SourceType
from models.normalized_data import ItemStatus
from models.etl_run import ETLStatus
from uuid import UUID

T = TypeVar("T")


class APIResponse(BaseModel, Generic[T]):
    request_id: str
    api_latency_ms: int
    data: T

# ============================================================================
# Health Check Schemas
# ============================================================================

class ETLCheckpointInfo(BaseModel):
    """ETL checkpoint information for health check"""
    source_type: SourceType
    source_name: str
    status: ETLStatus
    last_run_at: Optional[datetime]
    last_success_at: Optional[datetime]
    last_failure_at: Optional[datetime]
    checkpoint_value: Optional[str]
    total_records_processed: int
    last_records_processed: int
    error_message: Optional[str] = None
    
    class Config:
        from_attributes = True
        use_enum_values = True


class HealthCheckResponse(BaseModel):
    """Health check response model"""
    status: str = Field(..., description="Overall system status: healthy, degraded, unhealthy")
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    database_connected: bool
    etl_checkpoints: List[ETLCheckpointInfo] = Field(default_factory=list)
    total_sources: int = 0
    successful_sources: int = 0
    failed_sources: int = 0
    
    @validator("status", pre=True, always=True)
    def determine_status(cls, v, values):
        """Determine overall health status"""
        if not values.get("database_connected", False):
            return "unhealthy"
        
        failed = values.get("failed_sources", 0)
        total = values.get("total_sources", 0)
        
        if total == 0:
            return "healthy"  # No ETL configured yet
        
        if failed == 0:
            return "healthy"
        elif failed < total:
            return "degraded"
        else:
            return "unhealthy"
    
    class Config:
        json_schema_extra = {
            "example": {
                "status": "healthy",
                "timestamp": "2024-01-15T10:30:00Z",
                "database_connected": True,
                "total_sources": 3,
                "successful_sources": 3,
                "failed_sources": 0,
                "etl_checkpoints": [
                    {
                        "source_type": "api",
                        "source_name": "products_api",
                        "status": "success",
                        "last_run_at": "2024-01-15T10:00:00Z",
                        "last_success_at": "2024-01-15T10:00:00Z",
                        "checkpoint_value": "2024-01-15T10:00:00Z",
                        "total_records_processed": 1500,
                        "last_records_processed": 25
                    }
                ]
            }
        }

# ============================================================================
# Data Query Schemas
# ============================================================================

class DataQueryParams(BaseModel):
    """Query parameters for data endpoint"""
    page: int = Field(default=1, ge=1, description="Page number (1-indexed)")
    page_size: int = Field(default=50, ge=1, le=1000, description="Items per page")
    
    # Filtering
    source_type: Optional[SourceType] = Field(None, description="Filter by source type")
    source_name: Optional[str] = Field(None, description="Filter by source name")
    category: Optional[str] = Field(None, description="Filter by category")
    status: Optional[ItemStatus] = Field(None, description="Filter by status")
    
    # Search
    search: Optional[str] = Field(None, description="Search in title and description")
    
    # Date range
    published_after: Optional[datetime] = Field(None, description="Filter items published after this date")
    published_before: Optional[datetime] = Field(None, description="Filter items published before this date")
    
    # Sorting
    sort_by: str = Field(default="created_at", description="Sort field: created_at, published_at, title, amount")
    sort_order: str = Field(default="desc", description="Sort order: asc or desc")
    
    @validator("sort_order")
    def validate_sort_order(cls, v):
        if v.lower() not in ["asc", "desc"]:
            raise ValueError("sort_order must be 'asc' or 'desc'")
        return v.lower()
    
    @validator("sort_by")
    def validate_sort_by(cls, v):
        allowed_fields = ["created_at", "published_at", "title", "amount", "rating", "updated_at"]
        if v not in allowed_fields:
            raise ValueError(f"sort_by must be one of: {', '.join(allowed_fields)}")
        return v


class UnifiedItemResponse(BaseModel):
    """Response model for unified item"""
    id: int
    uuid: str
    source_type: SourceType
    source_name: str
    external_id: str
    
    title: str
    description: Optional[str]
    category: Optional[str]
    url: Optional[str]
    image_url: Optional[str]
    author: Optional[str]
    
    amount: Optional[float]
    quantity: Optional[int]
    rating: Optional[float]
    
    tags: Optional[List[str]]
    extra_metadata: Optional[Dict[str, Any]]
    
    status: ItemStatus
    published_at: Optional[datetime]
    created_at: datetime
    updated_at: datetime
    
    @classmethod
    def from_orm(cls, item):
        """Custom from_orm to explicitly convert UUID to string"""
        return cls(
            id=item.id,
            uuid=str(item.uuid),  # Explicit UUID → str conversion
            source_type=item.source_type,
            source_name=item.source_name,
            external_id=item.external_id,
            title=item.title,
            description=item.description,
            category=item.category,
            url=item.url,
            image_url=item.image_url,
            author=item.author,
            amount=item.amount,
            quantity=item.quantity,
            rating=item.rating,
            tags=item.tags,
            extra_metadata=item.extra_metadata,
            status=item.status,
            published_at=item.published_at,
            created_at=item.created_at,
            updated_at=item.updated_at,
        )
    
    class Config:
        use_enum_values = True
        json_schema_extra = {
            "example": {
                "id": 1,
                "uuid": "550e8400-e29b-41d4-a716-446655440000",
                "source_type": "api",
                "source_name": "products_api",
                "external_id": "prod_12345",
                "title": "Premium Wireless Headphones",
                "description": "High-quality noise-cancelling headphones",
                "category": "Electronics",
                "url": "https://example.com/products/12345",
                "amount": 299.99,
                "quantity": 50,
                "rating": 4.5,
                "tags": ["audio", "wireless", "premium"],
                "status": "active",
                "published_at": "2024-01-15T10:00:00Z",
                "created_at": "2024-01-15T10:30:00Z",
                "updated_at": "2024-01-15T10:30:00Z"
            }
        }


class PaginationMetadata(BaseModel):
    """Pagination metadata"""
    total_items: int
    total_pages: int
    current_page: int
    page_size: int
    has_next: bool
    has_previous: bool


class DataResponse(BaseModel):
    """Paginated data response"""
    items: List[UnifiedItemResponse]
    pagination: PaginationMetadata
    filters_applied: Dict[str, Any] = Field(default_factory=dict)
    
    class Config:
        json_schema_extra = {
            "example": {
                "items": [
                    {
                        "id": 1,
                        "uuid": "550e8400-e29b-41d4-a716-446655440000",
                        "source_type": "api",
                        "source_name": "products_api",
                        "title": "Sample Product",
                        "category": "Electronics",
                        "status": "active"
                    }
                ],
                "pagination": {
                    "total_items": 150,
                    "total_pages": 3,
                    "current_page": 1,
                    "page_size": 50,
                    "has_next": True,
                    "has_previous": False
                },
                "filters_applied": {
                    "source_type": "api",
                    "category": "Electronics"
                }
            }
        }


# ============================================================================
# Statistics Schemas
# ============================================================================

class SourceStatistics(BaseModel):
    """Statistics for a single data source"""
    source_type: SourceType
    source_name: str
    total_records: int
    active_records: int
    last_success_at: Optional[datetime]
    last_failure_at: Optional[datetime]
    total_runs: int
    success_rate: float = Field(..., ge=0, le=100, description="Success rate percentage")
    avg_records_per_run: float
    
    class Config:
        use_enum_values = True


class ETLRunSummary(BaseModel):
    run_id: str
    source_type: Optional[str] = None
    completed_at: Optional[datetime] = None
    records_extracted: int = 0
    records_loaded: int = 0
    records_failed: int = 0
    
    class Config:
        from_attributes = True
        use_enum_values = True


class StatsResponse(BaseModel):
    """Statistics response model"""
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    
    # Overall statistics
    total_records: int
    total_sources: int
    active_sources: int
    
    # By source type
    records_by_source_type: Dict[str, int]
    records_by_category: Dict[str, int]
    
    # Source-level stats
    source_statistics: List[SourceStatistics]
    
    # Recent ETL runs
    recent_runs: List[ETLRunSummary] = Field(default_factory=list)
    
    # Time-based stats
    last_etl_success: Optional[datetime]
    last_etl_failure: Optional[datetime]
    avg_etl_duration_seconds: Optional[float]
    
    class Config:
        json_schema_extra = {
            "example": {
                "timestamp": "2024-01-15T10:30:00Z",
                "total_records": 5000,
                "total_sources": 3,
                "active_sources": 3,
                "records_by_source_type": {
                    "api": 2500,
                    "csv": 1500,
                    "rss": 1000
                },
                "records_by_category": {
                    "Electronics": 1200,
                    "Books": 800,
                    "Clothing": 600
                },
                "last_etl_success": "2024-01-15T10:00:00Z",
                "avg_etl_duration_seconds": 45.2
            }
        }


# ============================================================================
# Error Response Schema
# ============================================================================

class ErrorResponse(BaseModel):
    """Standard error response"""
    error: str
    detail: Optional[str] = None
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    
    class Config:
        json_schema_extra = {
            "example": {
                "error": "Resource not found",
                "detail": "The requested item does not exist",
                "timestamp": "2024-01-15T10:30:00Z"
            }
        }
