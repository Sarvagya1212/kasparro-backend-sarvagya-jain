from sqlalchemy import Column, String, BigInteger, Enum, Text, Float, Integer, DateTime, ForeignKey, Index
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import relationship
from datetime import datetime
import uuid
from models.base import Base, SourceType, ItemStatus

class UnifiedItem(Base):
    """
    Unified, normalized table for all data sources.
    
    Schema Design Philosophy:
    - Common fields across all sources (id, title, description, timestamps)
    - Flexible fields for source-specific data (category, tags, metadata)
    - JSONB for truly variable/nested data
    - Denormalized for query performance
    
    Field Mapping Strategy:
    
    Source A (API):
    - item_id -> external_id
    - name -> title
    - description -> description
    - category -> category
    - price -> amount
    - created_date -> published_at
    - tags -> tags (array)
    - api_metadata -> metadata (JSONB)
    
    Source B (CSV):
    - row_id -> external_id
    - product_name -> title
    - details -> description
    - category -> category
    - cost -> amount
    - date -> published_at
    - keywords -> tags (comma-separated to array)
    
    Source C (RSS/Feed):
    - guid -> external_id
    - title -> title
    - summary -> description
    - category -> category
    - link -> url
    - pubDate -> published_at
    - tags -> tags
    """
    __tablename__ = "unified_items"
    
    id = Column(BigInteger, primary_key=True, autoincrement=True)
    uuid = Column(UUID(as_uuid=True), default=uuid.uuid4, unique=True, nullable=False, index=True)
    
    # Source tracking
    source_type = Column(Enum(SourceType), nullable=False, index=True)
    source_name = Column(String(100), nullable=False, index=True)
    external_id = Column(String(255), nullable=False, index=True)  # Original ID from source
    raw_data_id = Column(BigInteger, ForeignKey("raw_data.id"), nullable=True)
    
    # Core fields (common across all sources)
    title = Column(String(500), nullable=False, index=True)
    description = Column(Text, nullable=True)
    category = Column(String(200), nullable=True, index=True)
    
    # Optional common fields
    url = Column(String(2048), nullable=True)
    image_url = Column(String(2048), nullable=True)
    author = Column(String(200), nullable=True)
    
    # Numeric fields
    amount = Column(Float, nullable=True)  # Price, cost, value, etc.
    quantity = Column(Integer, nullable=True)
    rating = Column(Float, nullable=True)
    
    # Flexible fields
    tags = Column(JSONB, nullable=True)  # Array of tags/keywords
    extra_metadata = Column("metadata", JSONB, nullable=True)  # Source-specific nested data
    
    # Status and lifecycle
    status = Column(Enum(ItemStatus), default=ItemStatus.ACTIVE, nullable=False, index=True)
    
    # Timestamps
    published_at = Column(DateTime, nullable=True, index=True)  # Original publish date
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow, index=True)
    updated_at = Column(DateTime, nullable=False, default=datetime.utcnow, onupdate=datetime.utcnow)
    deleted_at = Column(DateTime, nullable=True)
    
    # ETL tracking
    etl_run_id = Column(BigInteger, ForeignKey("etl_runs.id"), nullable=True, index=True)
    
    # Full-text search (for PostgreSQL)
    search_vector = Column(Text, nullable=True)  # Will be populated via trigger
    
    # Relationships
    raw_data = relationship("RawData", foreign_keys=[raw_data_id])
    etl_run = relationship("ETLRun", back_populates="unified_items")
    
    # Indexes for performance
    __table_args__ = (
        Index("idx_unified_source_external", "source_type", "source_name", "external_id", unique=True),
        Index("idx_unified_category_status", "category", "status"),
        Index("idx_unified_published", "published_at", "status"),
        Index("idx_unified_created", "created_at"),
        Index("idx_unified_search", "title", "description", postgresql_using="gin"),  # Full-text
    )
