from sqlalchemy import Column, Integer, String, Enum, DateTime, Text, Index, BigInteger
from sqlalchemy.dialects.postgresql import JSONB
from datetime import datetime
from models.base import Base, SourceType, ETLStatus

class ETLCheckpoint(Base):
    """
    Tracks incremental ingestion state per source.
    
    Purpose:
    - Resume ETL from last successful point
    - Avoid reprocessing old data
    - Track watermarks (timestamps, IDs, offsets)
    
    Design:
    - One row per source
    - checkpoint_value stores the watermark (timestamp, ID, page number, etc.)
    - Supports both timestamp-based and ID-based incremental loading
    """
    __tablename__ = "etl_checkpoints"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    
    # Source identification
    source_type = Column(Enum(SourceType), nullable=False)
    source_name = Column(String(100), nullable=False)
    
    # Checkpoint data
    checkpoint_type = Column(String(50), nullable=False)  # "timestamp", "id", "page", "offset"
    checkpoint_value = Column(String(255), nullable=True)  # Last processed value
    checkpoint_data = Column(JSONB, nullable=True)  # Additional checkpoint metadata
    
    # Statistics
    last_run_at = Column(DateTime, nullable=True, index=True)
    last_success_at = Column(DateTime, nullable=True)
    last_failure_at = Column(DateTime, nullable=True)
    
    total_runs = Column(Integer, default=0)
    total_records_processed = Column(BigInteger, default=0)
    last_records_processed = Column(Integer, default=0)
    
    # Status
    status = Column(Enum(ETLStatus), default=ETLStatus.PENDING, nullable=False)
    error_message = Column(Text, nullable=True)
    
    # Timestamps
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    updated_at = Column(DateTime, nullable=False, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # Constraints
    __table_args__ = (
        Index("idx_checkpoint_source", "source_type", "source_name", unique=True),
    )
