"""
ETL pipeline components for data ingestion and processing.

This package contains all components for the Extract-Transform-Load pipeline:

Modules:
    base: Abstract base class for data sources with checkpoint management
    runner: ETL orchestrator that coordinates extract, transform, and load phases
    scheduler: APScheduler integration for automated ETL job execution
    checkpoint: Checkpoint management utilities

Subpackages:
    extractors: Data source extractors (API, CSV, RSS)
    transformers: Data normalization and validation
    loaders: Database loaders with idempotent upsert operations

Architecture:
    The ETL pipeline follows a three-phase approach:
    
    1. Extract - Fetch data from external sources with retry logic
    2. Transform - Normalize data to unified schema with validation
    3. Load - Upsert data to database with idempotency guarantees
    
    Each phase is independent and can handle partial failures gracefully.

Usage:
    from ingestion.extractors import APIExtractor, CSVExtractor
    from ingestion.transformers import DataNormalizer
    from ingestion.loaders import PostgresLoader
    from ingestion.runner import ETLRunner

Example:
    # Create extractor
    extractor = APIExtractor(
        db_session=session,
        source_name="my_api",
        api_url="https://api.example.com/data"
    )
    
    # Run ETL pipeline
    runner = ETLRunner(session)
    result = await runner.run(extractor)
    
    print(f"Loaded {result['records_loaded']} records")

Error Handling:
    All components use custom exceptions from core.exceptions for
    structured error handling with retry logic and circuit breakers.
    See ARCHITECTURE.md for detailed error handling documentation.
"""

__all__ = [
    "DataSource",
    "ETLRunner",
    "APIExtractor",
    "CSVExtractor",
    "RSSExtractor",
    "DataNormalizer",
    "PostgresLoader",
]
