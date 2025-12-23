from sqlalchemy import Column, String, BigInteger, Enum, Text, DateTime, ForeignKey, Boolean, Index
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import relationship
from datetime import datetime
from models.base import Base, SourceType

class RawData(Base):
    """
    Stores raw, unprocessed data from all sources.
    
    Purpose:
    - Immutable audit trail
    - Reprocessing capability
    - Debugging and data lineage
    
    Design Decisions:
    - JSONB for efficient querying and indexing
    - source_id allows tracking original record identifiers
    - ingested_at captures exact ingestion timestamp
    """
    __tablename__ = "raw_data"
    
    id = Column(BigInteger, primary_key=True, autoincrement=True)
    
    # Source identification
    source_type = Column(Enum(SourceType), nullable=False, index=True)
    source_name = Column(String(100), nullable=False, index=True)
    source_id = Column(String(255), nullable=True, index=True)  # Original record ID
    
    # Raw data storage
    raw_payload = Column(JSONB, nullable=False)  # JSONB for efficient querying
    raw_text = Column(Text, nullable=True)  # For CSV rows or text content
    
    # Metadata
    ingested_at = Column(DateTime, nullable=False, default=datetime.utcnow, index=True)
    file_path = Column(String(500), nullable=True)  # For file-based sources
    content_hash = Column(String(64), nullable=True, index=True)  # SHA-256 hash for deduplication
    
    # Processing tracking
    processed = Column(Boolean, default=False, index=True)
    processed_at = Column(DateTime, nullable=True)
    etl_run_id = Column(BigInteger, ForeignKey("etl_runs.id"), nullable=True, index=True)
    
    # Relationships
    etl_run = relationship("ETLRun", back_populates="raw_data_records")
    
    # Indexes
    __table_args__ = (
        Index("idx_raw_source_ingested", "source_type", "source_name", "ingested_at"),
        Index("idx_raw_unprocessed", "processed", "ingested_at"),
        Index("idx_raw_content_hash", "content_hash"),  # For deduplication
    )
