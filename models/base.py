from datetime import datetime
from typing import Optional
from sqlalchemy import (
    Column, String, Integer, DateTime, JSON, Text, 
    Boolean, Float, Enum, Index, ForeignKey, BigInteger
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from sqlalchemy.dialects.postgresql import JSONB, UUID
import enum
import uuid

Base = declarative_base()


# ============================================================================
# ENUMS
# ============================================================================

class SourceType(str, enum.Enum):
    """Data source types"""
    API = "api"
    CSV = "csv"
    RSS = "rss"
    FEED = "feed"


class ETLStatus(str, enum.Enum):
    """ETL run status"""
    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    PARTIAL = "partial"


class ItemStatus(str, enum.Enum):
    """Unified item status"""
    ACTIVE = "active"
    INACTIVE = "inactive"
    DELETED = "deleted"
    PENDING = "pending"
