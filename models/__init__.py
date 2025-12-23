"""
SQLAlchemy ORM models for database tables.

This package defines the database schema using SQLAlchemy ORM models:

Models:
    base: Base declarative class and shared enums (SourceType, ETLStatus, ItemStatus)
    raw_data: Raw data storage with source metadata
    normalized_data: Unified, normalized data items
    etl_run: ETL execution tracking and metrics
    checkpoint: Checkpoint management for resume-on-failure

Database Schema:
    All models inherit from the Base declarative class and use
    PostgreSQL-specific features like JSONB for flexible metadata storage.

Usage:
    from models import RawData, UnifiedItem, ETLRun, ETLCheckpoint
    from models.base import SourceType, ETLStatus

Example:
    # Create a raw data record
    raw = RawData(
        source_type=SourceType.API,
        source_name="my_api",
        source_id="123",
        raw_payload={"data": "value"}
    )
    session.add(raw)
    await session.commit()

Relationships:
    - RawData → UnifiedItem (one-to-one normalization)
    - ETLRun → RawData (one-to-many tracking)
    - ETLCheckpoint → ETLRun (one-to-many management)
"""

__all__ = [
    "Base",
    "SourceType",
    "ETLStatus",
    "ItemStatus",
    "RawData",
    "UnifiedItem",
    "ETLRun",
    "ETLCheckpoint",
]
