"""
Core utilities and configuration for the Kasparro ETL system.

This package provides foundational components used throughout the ETL pipeline:

Modules:
    config: Application configuration and environment variable management
    database: Database connection and session management
    exceptions: Custom exception hierarchy for error handling
    logging: Logging configuration and utilities
    security: Security utilities (API key management, etc.)

Usage:
    from core.config import settings
    from core.database import get_db_session
    from core.exceptions import APIExtractionError, NetworkError
    from core.logging import setup_logging

Example:
    # Initialize logging
    setup_logging()
    
    # Get database session
    async with get_db_session() as session:
        # Perform database operations
        pass
"""

__all__ = [
    "settings",
    "get_db_session",
    "setup_logging",
    # Exceptions
    "ETLException",
    "ExtractionError",
    "APIExtractionError",
    "CSVExtractionError",
    "RSSExtractionError",
    "TransformationError",
    "ValidationError",
    "NormalizationError",
    "LoadError",
    "DatabaseError",
    "UpsertError",
    "CheckpointError",
    "RetryableError",
    "NonRetryableError",
    "NetworkError",
    "RateLimitError",
    "DatabaseConnectionError",
    "DeadlockError",
    "AuthenticationError",
    "SchemaValidationError",
    "DataFormatError",
    "ResourceNotFoundError",
]
