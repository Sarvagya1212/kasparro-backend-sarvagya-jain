"""
Pydantic schemas for unified normalized data with validation
"""

from pydantic import BaseModel, Field, validator
from typing import Optional, List, Dict, Any
from datetime import datetime
from models.raw_data import SourceType
from models.normalized_data import ItemStatus


class UnifiedItemCreate(BaseModel):
    """
    Schema for creating unified items with validation.
    
    Ensures:
    - Required fields are present
    - Types are correct
    - Data is cleaned and normalized
    """
    
    # Source tracking (required)
    source_type: SourceType
    source_name: str = Field(..., min_length=1, max_length=100)
    external_id: str = Field(..., min_length=1, max_length=255)
    raw_data_id: int
    
    # Core fields
    title: str = Field(..., min_length=1, max_length=500)
    description: Optional[str] = None
    category: Optional[str] = Field(None, max_length=200)
    
    # Optional fields
    url: Optional[str] = Field(None, max_length=2048)
    image_url: Optional[str] = Field(None, max_length=2048)
    author: Optional[str] = Field(None, max_length=200)
    
    # Numeric fields
    amount: Optional[float] = Field(None, ge=0)
    quantity: Optional[int] = Field(None, ge=0)
    rating: Optional[float] = Field(None, ge=0, le=5)
    
    # Flexible fields
    tags: Optional[List[str]] = Field(default_factory=list)
    extra_metadata: Optional[Dict[str, Any]] = Field(default_factory=dict, alias="metadata")
    
    # Status
    status: ItemStatus = ItemStatus.ACTIVE
    
    # Timestamps
    published_at: Optional[datetime] = None
    
    @validator("title")
    def clean_title(cls, v):
        """Clean and normalize title"""
        if v:
            v = v.strip()
            if not v:
                raise ValueError("Title cannot be empty after stripping")
        return v
    
    @validator("tags", pre=True)
    def clean_tags(cls, v):
        """Ensure tags is a list"""
        if v is None:
            return []
        if isinstance(v, str):
            return [t.strip() for t in v.split(",") if t.strip()]
        if isinstance(v, list):
            return [str(t).strip() for t in v if str(t).strip()]
        return []
    
    @validator("extra_metadata", pre=True)
    def clean_extra_metadata(cls, v):
        """Ensure metadata is a dict"""
        if v is None:
            return {}
        if not isinstance(v, dict):
            return {}
        return v
    
    class Config:
        use_enum_values = True


class UnifiedItemResponse(UnifiedItemCreate):
    """Schema for API responses"""
    id: int
    uuid: str
    created_at: datetime
    updated_at: datetime
    
    class Config:
        from_attributes = True
