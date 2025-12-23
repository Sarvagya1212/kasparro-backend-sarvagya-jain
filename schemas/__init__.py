"""
Pydantic schemas for data validation and serialization.

This package defines Pydantic models for request/response validation
and data serialization throughout the ETL pipeline:

Schemas:
    base: Base schemas and shared validators
    raw: Raw data schemas for API requests/responses
    normalized: Unified item schemas for normalized data
    etl: ETL run and checkpoint schemas
    api: API endpoint request/response schemas

Features:
    - Automatic data validation
    - Type coercion and conversion
    - JSON serialization/deserialization
    - OpenAPI schema generation for FastAPI

Usage:
    from schemas import UnifiedItemCreate, ETLRunResponse
    from schemas.api import DataQueryParams, HealthResponse

Example:
    # Validate and create normalized item
    item = UnifiedItemCreate(
        source_type=SourceType.API,
        source_name="my_api",
        external_id="123",
        title="Example Item",
        raw_data_id=456
    )
    
    # Pydantic automatically validates types and required fields
    assert item.source_type == SourceType.API
    assert item.title == "Example Item"

Validation:
    All schemas use Pydantic validators for:
    - Required field checking
    - Type validation and coercion
    - Custom business logic validation
    - Default value assignment
"""

__all__ = [
    "UnifiedItemCreate",
    "UnifiedItemResponse",
    "ETLRunResponse",
    "ETLCheckpointResponse",
    "DataQueryParams",
    "HealthResponse",
    "StatsResponse",
]
