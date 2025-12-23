"""
API data source extractor with authentication, rate limiting, and retry logic.

This module provides robust API extraction with:
- Exponential backoff retry logic for transient failures
- Circuit breaker pattern to prevent cascading failures
- Rate limiting protection
- Comprehensive error handling with custom exceptions
- Timeout handling with configurable limits
"""

import httpx
import asyncio
from typing import List, Dict, Any, Optional
from datetime import datetime, timedelta
from ingestion.base import DataSource
from models.raw_data import SourceType
from core.config import settings
from core.exceptions import (
    APIExtractionError,
    NetworkError,
    RateLimitError,
    AuthenticationError,
    ResourceNotFoundError,
    RetryableError
)
import logging

logger = logging.getLogger(__name__)


class APIExtractor(DataSource):
    """
    Extract data from REST API with authentication and resilience patterns.
    
    Features:
    - Bearer token authentication
    - Pagination support
    - Retry logic with exponential backoff
    - Circuit breaker pattern
    - Rate limiting protection
    - Incremental loading via timestamp
    - Comprehensive error handling
    
    Attributes:
        max_retries: Maximum number of retry attempts (default: 3)
        retry_delay: Initial retry delay in seconds (default: 1.0)
        timeout: Request timeout in seconds (default: 30.0)
        circuit_breaker_threshold: Failures before circuit opens (default: 5)
        circuit_breaker_timeout: Seconds before circuit reset (default: 60)
    """
    
    def __init__(
        self,
        db_session,
        source_name: str,
        api_url: str,
        api_key: Optional[str] = None,
        timestamp_field: str = "created_at",
        id_field: str = "id",
        max_retries: int = 3,
        retry_delay: float = 1.0,
        timeout: float = 30.0
    ):
        super().__init__(
            db_session=db_session,
            source_type=SourceType.API,
            source_name=source_name,
            checkpoint_type="timestamp"
        )
        self.api_url = api_url
        self.api_key = api_key or settings.API_KEY
        self.timestamp_field = timestamp_field
        self.id_field = id_field
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.timeout = timeout
        
        # Circuit breaker state
        self._circuit_breaker_failures = 0
        self._circuit_breaker_threshold = 5
        self._circuit_breaker_open_until: Optional[datetime] = None
        self._circuit_breaker_timeout = 60  # seconds
    
    def _is_circuit_open(self) -> bool:
        """Check if circuit breaker is open."""
        if self._circuit_breaker_open_until is None:
            return False
        
        if datetime.utcnow() >= self._circuit_breaker_open_until:
            # Circuit breaker timeout expired, reset
            logger.info(f"Circuit breaker reset for {self.source_name}")
            self._circuit_breaker_failures = 0
            self._circuit_breaker_open_until = None
            return False
        
        return True
    
    def _record_failure(self):
        """Record a failure and potentially open circuit breaker."""
        self._circuit_breaker_failures += 1
        
        if self._circuit_breaker_failures >= self._circuit_breaker_threshold:
            self._circuit_breaker_open_until = datetime.utcnow() + timedelta(
                seconds=self._circuit_breaker_timeout
            )
            logger.warning(
                f"Circuit breaker opened for {self.source_name}. "
                f"Will retry after {self._circuit_breaker_timeout} seconds."
            )
    
    def _record_success(self):
        """Record a successful request."""
        self._circuit_breaker_failures = 0
        self._circuit_breaker_open_until = None
    
    async def _make_request_with_retry(
        self,
        client: httpx.AsyncClient,
        url: str,
        headers: Dict[str, str],
        params: Dict[str, Any]
    ) -> httpx.Response:
        """
        Make HTTP request with retry logic and exponential backoff.
        
        Args:
            client: HTTP client
            url: Request URL
            headers: Request headers
            params: Query parameters
        
        Returns:
            HTTP response
        
        Raises:
            APIExtractionError: For non-retryable errors
            NetworkError: For retryable network errors after max retries
        """
        # Check circuit breaker
        if self._is_circuit_open():
            raise APIExtractionError(
                f"Circuit breaker is open for {self.source_name}",
                context={
                    "source_name": self.source_name,
                    "api_url": url,
                    "open_until": self._circuit_breaker_open_until.isoformat()
                }
            )
        
        last_exception = None
        
        for attempt in range(self.max_retries):
            try:
                logger.debug(f"Request attempt {attempt + 1}/{self.max_retries} to {url}")
                
                response = await client.get(
                    url,
                    headers=headers,
                    params=params,
                    timeout=self.timeout
                )
                
                # Handle HTTP errors
                if response.status_code == 401 or response.status_code == 403:
                    self._record_failure()
                    raise AuthenticationError(
                        f"Authentication failed for {url}",
                        context={
                            "status_code": response.status_code,
                            "api_url": url,
                            "source_name": self.source_name
                        }
                    )
                
                if response.status_code == 404:
                    self._record_failure()
                    raise ResourceNotFoundError(
                        f"Resource not found: {url}",
                        context={
                            "status_code": 404,
                            "api_url": url,
                            "source_name": self.source_name
                        }
                    )
                
                if response.status_code == 429:
                    # Rate limiting
                    retry_after = int(response.headers.get("Retry-After", self.retry_delay * (2 ** attempt)))
                    logger.warning(f"Rate limited. Retrying after {retry_after} seconds")
                    
                    if attempt < self.max_retries - 1:
                        await asyncio.sleep(retry_after)
                        continue
                    else:
                        self._record_failure()
                        raise RateLimitError(
                            f"Rate limit exceeded for {url}",
                            context={
                                "status_code": 429,
                                "api_url": url,
                                "source_name": self.source_name,
                                "retry_count": attempt + 1
                            },
                            retry_after=retry_after
                        )
                
                if response.status_code >= 500:
                    # Server error - retryable
                    if attempt < self.max_retries - 1:
                        delay = self.retry_delay * (2 ** attempt)  # Exponential backoff
                        logger.warning(
                            f"Server error {response.status_code}. "
                            f"Retrying in {delay} seconds (attempt {attempt + 1}/{self.max_retries})"
                        )
                        await asyncio.sleep(delay)
                        continue
                    else:
                        self._record_failure()
                        raise NetworkError(
                            f"Server error after {self.max_retries} retries",
                            context={
                                "status_code": response.status_code,
                                "api_url": url,
                                "source_name": self.source_name,
                                "retry_count": attempt + 1,
                                "response_body": response.text[:500]  # Truncate
                            }
                        )
                
                # Success
                response.raise_for_status()
                self._record_success()
                return response
            
            except httpx.TimeoutException as e:
                last_exception = e
                if attempt < self.max_retries - 1:
                    delay = self.retry_delay * (2 ** attempt)
                    logger.warning(f"Request timeout. Retrying in {delay} seconds")
                    await asyncio.sleep(delay)
                else:
                    self._record_failure()
                    raise NetworkError(
                        f"Request timeout after {self.max_retries} retries",
                        context={
                            "api_url": url,
                            "source_name": self.source_name,
                            "timeout": self.timeout,
                            "retry_count": attempt + 1
                        },
                        original_exception=e
                    )
            
            except httpx.NetworkError as e:
                last_exception = e
                if attempt < self.max_retries - 1:
                    delay = self.retry_delay * (2 ** attempt)
                    logger.warning(f"Network error. Retrying in {delay} seconds")
                    await asyncio.sleep(delay)
                else:
                    self._record_failure()
                    raise NetworkError(
                        f"Network error after {self.max_retries} retries",
                        context={
                            "api_url": url,
                            "source_name": self.source_name,
                            "retry_count": attempt + 1
                        },
                        original_exception=e
                    )
            
            except (AuthenticationError, ResourceNotFoundError):
                # Non-retryable errors - re-raise immediately
                raise
            
            except Exception as e:
                last_exception = e
                if attempt < self.max_retries - 1:
                    delay = self.retry_delay * (2 ** attempt)
                    logger.warning(f"Unexpected error. Retrying in {delay} seconds: {str(e)}")
                    await asyncio.sleep(delay)
                else:
                    self._record_failure()
                    raise APIExtractionError(
                        f"Unexpected error after {self.max_retries} retries",
                        context={
                            "api_url": url,
                            "source_name": self.source_name,
                            "retry_count": attempt + 1
                        },
                        original_exception=e
                    )
        
        # Should never reach here, but just in case
        raise APIExtractionError(
            "Max retries exceeded",
            context={"api_url": url, "source_name": self.source_name},
            original_exception=last_exception
        )
    
    async def fetch_data(self, checkpoint_value: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Fetch data from API with incremental loading, retry logic, and error handling.
        
        Args:
            checkpoint_value: ISO timestamp of last successful fetch
        
        Returns:
            List of records fetched from the API
        
        Raises:
            APIExtractionError: For API-related errors
            NetworkError: For network-related errors
            AuthenticationError: For authentication failures
        """
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json"
        }
        
        params = {}
        if checkpoint_value:
            # Only fetch records newer than checkpoint
            params["since"] = checkpoint_value
        
        all_records = []
        page = 1
        
        try:
            async with httpx.AsyncClient(timeout=self.timeout) as client:
                while True:
                    params["page"] = page
                    
                    logger.info(f"Fetching page {page} from {self.api_url}")
                    
                    # Make request with retry logic
                    response = await self._make_request_with_retry(
                        client, self.api_url, headers, params
                    )
                    
                    try:
                        data = response.json()
                    except Exception as e:
                        raise APIExtractionError(
                            "Failed to parse JSON response",
                            context={
                                "api_url": self.api_url,
                                "source_name": self.source_name,
                                "page": page,
                                "response_body": response.text[:500]
                            },
                            original_exception=e
                        )
                    
                    # Handle different API response formats
                    if isinstance(data, list):
                        records = data
                    elif isinstance(data, dict):
                        records = data.get("data", data.get("results", []))
                    else:
                        records = []
                    
                    if not records:
                        break
                    
                    all_records.extend(records)
                    logger.debug(f"Fetched {len(records)} records from page {page}")
                    
                    # Check if there are more pages
                    if isinstance(data, dict):
                        has_next = data.get("has_next", False)
                        if not has_next:
                            break
                    else:
                        # If response is a list and less than expected, assume last page
                        if len(records) < 100:  # Assuming default page size
                            break
                    
                    page += 1
            
            logger.info(
                f"Successfully fetched {len(all_records)} records from {self.source_name} "
                f"({page} pages)"
            )
            return all_records
        
        except (APIExtractionError, NetworkError, AuthenticationError, ResourceNotFoundError):
            # Re-raise our custom exceptions
            raise
        
        except Exception as e:
            # Catch any unexpected errors
            raise APIExtractionError(
                f"Unexpected error during data fetch",
                context={
                    "api_url": self.api_url,
                    "source_name": self.source_name,
                    "page": page,
                    "records_fetched": len(all_records)
                },
                original_exception=e
            )
    
    def extract_record_id(self, record: Dict[str, Any]) -> str:
        """Extract ID from API record"""
        return str(record.get(self.id_field, ""))
    
    def extract_timestamp(self, record: Dict[str, Any]) -> Optional[datetime]:
        """Extract timestamp from API record"""
        timestamp_str = record.get(self.timestamp_field)
        if timestamp_str:
            try:
                return datetime.fromisoformat(timestamp_str.replace("Z", "+00:00"))
            except:
                pass
        return None

