from sqlalchemy import Column, BigInteger, String, Enum, DateTime, Float, Integer, Text, Index
from sqlalchemy.dialects.postgresql import UUID, JSONB
from sqlalchemy.orm import relationship
from datetime import datetime
import uuid
from models.base import Base, SourceType, ETLStatus

class ETLRun(Base):
    """
    Tracks metadata for each ETL execution.
    
    Purpose:
    - Audit trail of all ETL runs
    - Performance monitoring
    - Error tracking and debugging
    - Run comparison and anomaly detection (P2)
    """
    __tablename__ = "etl_runs"
    
    id = Column(BigInteger, primary_key=True, autoincrement=True)
    run_id = Column(UUID(as_uuid=True), default=uuid.uuid4, unique=True, nullable=False, index=True)
    
    # Source identification
    source_type = Column(Enum(SourceType), nullable=False, index=True)
    source_name = Column(String(100), nullable=False, index=True)
    
    # Run metadata
    status = Column(Enum(ETLStatus), default=ETLStatus.PENDING, nullable=False, index=True)
    
    # Timestamps
    started_at = Column(DateTime, nullable=False, default=datetime.utcnow, index=True)
    completed_at = Column(DateTime, nullable=True)
    duration_seconds = Column(Float, nullable=True)
    
    # Statistics
    records_extracted = Column(Integer, default=0)
    records_transformed = Column(Integer, default=0)
    records_loaded = Column(Integer, default=0)
    records_failed = Column(Integer, default=0)
    records_skipped = Column(Integer, default=0)
    
    # Error tracking
    error_message = Column(Text, nullable=True)
    error_details = Column(JSONB, nullable=True)
    
    # Configuration snapshot
    config_snapshot = Column(JSONB, nullable=True)  # ETL config at run time
    
    # Checkpoint info
    checkpoint_before = Column(String(255), nullable=True)
    checkpoint_after = Column(String(255), nullable=True)
    
    # Performance metrics
    avg_extraction_time_ms = Column(Float, nullable=True)
    avg_transformation_time_ms = Column(Float, nullable=True)
    avg_loading_time_ms = Column(Float, nullable=True)
    
    # Additional metadata
    run_metadata = Column("metadata", JSONB, nullable=True)  # Flexible field for custom metrics
    
    # Relationships
    raw_data_records = relationship("RawData", back_populates="etl_run")
    unified_items = relationship("UnifiedItem", back_populates="etl_run")
    
    # Indexes
    __table_args__ = (
        Index("idx_etl_run_source_started", "source_type", "source_name", "started_at"),
        Index("idx_etl_run_status", "status", "started_at"),
    )
