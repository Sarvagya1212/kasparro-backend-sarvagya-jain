"""
Custom exceptions for ETL pipeline with structured error context.

This module provides a comprehensive exception hierarchy for handling
errors throughout the ETL pipeline. Each exception includes context
information for debugging and monitoring.

Exception Hierarchy:
    ETLException (base)
    ├── ExtractionError
    │   ├── APIExtractionError
    │   ├── CSVExtractionError
    │   └── RSSExtractionError
    ├── TransformationError
    │   ├── ValidationError
    │   └── NormalizationError
    ├── LoadError
    │   ├── DatabaseError
    │   └── UpsertError
    ├── CheckpointError
    └── RetryableError / NonRetryableError (mixins)
"""

from typing import Optional, Dict, Any
from datetime import datetime


class ETLException(Exception):
    """
    Base exception for all ETL-related errors.
    
    Attributes:
        message: Human-readable error message
        context: Additional context information (source, timestamp, etc.)
        original_exception: The original exception that was caught (if any)
    """
    
    def __init__(
        self,
        message: str,
        context: Optional[Dict[str, Any]] = None,
        original_exception: Optional[Exception] = None
    ):
        self.message = message
        self.context = context or {}
        self.original_exception = original_exception
        self.timestamp = datetime.utcnow()
        
        # Add timestamp to context
        self.context["error_timestamp"] = self.timestamp.isoformat()
        
        # Chain original exception if provided
        super().__init__(message)
        if original_exception:
            self.__cause__ = original_exception
    
    def __str__(self) -> str:
        """Format error message with context."""
        base_msg = f"{self.__class__.__name__}: {self.message}"
        
        if self.context:
            context_str = ", ".join(f"{k}={v}" for k, v in self.context.items())
            base_msg += f" | Context: {context_str}"
        
        if self.original_exception:
            base_msg += f" | Caused by: {type(self.original_exception).__name__}: {str(self.original_exception)}"
        
        return base_msg
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert exception to dictionary for logging/storage."""
        return {
            "error_type": self.__class__.__name__,
            "message": self.message,
            "context": self.context,
            "timestamp": self.timestamp.isoformat(),
            "original_error": str(self.original_exception) if self.original_exception else None
        }


# ============================================================================
# Extraction Errors
# ============================================================================

class ExtractionError(ETLException):
    """Base exception for data extraction failures."""
    pass


class APIExtractionError(ExtractionError):
    """
    Exception raised when API data extraction fails.
    
    Context should include:
        - api_url: The API endpoint that failed
        - status_code: HTTP status code (if applicable)
        - response_body: Response body (truncated if large)
        - retry_count: Number of retries attempted
    """
    pass


class CSVExtractionError(ExtractionError):
    """
    Exception raised when CSV file extraction fails.
    
    Context should include:
        - file_path: Path to the CSV file
        - line_number: Line number where error occurred (if applicable)
        - column_name: Column name that caused the error (if applicable)
    """
    pass


class RSSExtractionError(ExtractionError):
    """
    Exception raised when RSS feed extraction fails.
    
    Context should include:
        - feed_url: URL of the RSS feed
        - entry_index: Index of the entry that failed (if applicable)
    """
    pass


# ============================================================================
# Transformation Errors
# ============================================================================

class TransformationError(ETLException):
    """Base exception for data transformation failures."""
    pass


class ValidationError(TransformationError):
    """
    Exception raised when data validation fails.
    
    Context should include:
        - field_name: Name of the field that failed validation
        - field_value: Value that failed validation
        - validation_rule: The validation rule that was violated
        - raw_data_id: ID of the raw data record
    """
    pass


class NormalizationError(TransformationError):
    """
    Exception raised when data normalization fails.
    
    Context should include:
        - source_type: Type of data source (API, CSV, RSS)
        - source_name: Name of the data source
        - raw_data_id: ID of the raw data record
        - field_errors: Dictionary of field-level errors
    """
    pass


# ============================================================================
# Load Errors
# ============================================================================

class LoadError(ETLException):
    """Base exception for data loading failures."""
    pass


class DatabaseError(LoadError):
    """
    Exception raised when database operations fail.
    
    Context should include:
        - operation: Type of database operation (INSERT, UPDATE, UPSERT)
        - table_name: Name of the table
        - error_code: Database error code (if available)
        - constraint_name: Name of violated constraint (if applicable)
    """
    pass


class UpsertError(LoadError):
    """
    Exception raised when upsert operation fails.
    
    Context should include:
        - record_id: ID of the record being upserted
        - conflict_fields: Fields that caused the conflict
        - batch_index: Index in the batch (if batch operation)
    """
    pass


# ============================================================================
# Checkpoint Errors
# ============================================================================

class CheckpointError(ETLException):
    """
    Exception raised when checkpoint management fails.
    
    Context should include:
        - source_type: Type of data source
        - source_name: Name of the data source
        - checkpoint_value: The checkpoint value that failed
        - operation: Operation that failed (read, write, update)
    """
    pass


# ============================================================================
# Retry Strategy Mixins
# ============================================================================

class RetryableError(ETLException):
    """
    Mixin for errors that should trigger retry logic.
    
    Use this for transient errors like:
    - Network timeouts
    - Rate limiting (HTTP 429)
    - Temporary database connection issues
    - Service unavailable (HTTP 503)
    """
    
    def __init__(
        self,
        message: str,
        context: Optional[Dict[str, Any]] = None,
        original_exception: Optional[Exception] = None,
        max_retries: int = 3,
        retry_delay: float = 1.0
    ):
        super().__init__(message, context, original_exception)
        self.max_retries = max_retries
        self.retry_delay = retry_delay


class NonRetryableError(ETLException):
    """
    Mixin for errors that should NOT trigger retry logic.
    
    Use this for permanent errors like:
    - Authentication failures (HTTP 401, 403)
    - Invalid data format
    - Schema validation errors
    - Resource not found (HTTP 404)
    """
    pass


# ============================================================================
# Specific Retryable Errors
# ============================================================================

class NetworkError(RetryableError, APIExtractionError):
    """Network-related errors that should be retried."""
    pass


class RateLimitError(RetryableError, APIExtractionError):
    """Rate limiting errors (HTTP 429) that should be retried with backoff."""
    
    def __init__(
        self,
        message: str,
        context: Optional[Dict[str, Any]] = None,
        original_exception: Optional[Exception] = None,
        retry_after: Optional[int] = None
    ):
        super().__init__(message, context, original_exception)
        self.retry_after = retry_after  # Seconds to wait before retry
        if retry_after:
            self.context["retry_after"] = retry_after


class DatabaseConnectionError(RetryableError, DatabaseError):
    """Database connection errors that should be retried."""
    pass


class DeadlockError(RetryableError, DatabaseError):
    """Database deadlock errors that should be retried."""
    pass


# ============================================================================
# Specific Non-Retryable Errors
# ============================================================================

class AuthenticationError(NonRetryableError, APIExtractionError):
    """Authentication failures (HTTP 401, 403) that should not be retried."""
    pass


class SchemaValidationError(NonRetryableError, ValidationError):
    """Schema validation errors that should not be retried."""
    pass


class DataFormatError(NonRetryableError, TransformationError):
    """Data format errors that should not be retried."""
    pass


class ResourceNotFoundError(NonRetryableError, ExtractionError):
    """Resource not found errors (HTTP 404) that should not be retried."""
    pass
