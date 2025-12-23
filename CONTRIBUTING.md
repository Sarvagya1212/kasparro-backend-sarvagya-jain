# Contributing to Kasparro ETL

Thank you for considering contributing to the Kasparro ETL project! This document provides guidelines and best practices for maintaining code quality and consistency.

## Code Style Guidelines

### Python Style

- Follow [PEP 8](https://pep8.org/) style guide
- Use 4 spaces for indentation (no tabs)
- Maximum line length: 100 characters
- Use descriptive variable names (avoid single letters except in loops)

### Naming Conventions

- **Classes**: PascalCase (e.g., `APIExtractor`, `DataNormalizer`)
- **Functions/Methods**: snake_case (e.g., `fetch_data`, `normalize_record`)
- **Constants**: UPPER_SNAKE_CASE (e.g., `MAX_RETRIES`, `DEFAULT_TIMEOUT`)
- **Private methods**: Prefix with underscore (e.g., `_make_request`, `_parse_response`)

### Import Organization

```python
# Standard library imports
import asyncio
from typing import List, Dict, Any, Optional
from datetime import datetime

# Third-party imports
import httpx
from sqlalchemy import select
from pydantic import BaseModel

# Local application imports
from core.exceptions import APIExtractionError
from models.raw_data import RawData
from ingestion.base import DataSource
```

## Documentation Standards

### Docstrings

All public functions, classes, and methods must have docstrings following Google style:

```python
def fetch_data(self, checkpoint_value: Optional[str] = None) -> List[Dict[str, Any]]:
    """
    Fetch data from API with incremental loading.
    
    This method implements retry logic with exponential backoff and
    respects rate limiting from the API server.
    
    Args:
        checkpoint_value: ISO timestamp of last successful fetch.
            If None, fetches all available data.
    
    Returns:
        List of records fetched from the API. Each record is a dictionary
        containing the raw API response data.
    
    Raises:
        APIExtractionError: If API request fails after all retries
        NetworkError: If network connectivity issues occur
        AuthenticationError: If API authentication fails
    
    Example:
        >>> extractor = APIExtractor(db, "my_api", "https://api.example.com")
        >>> records = await extractor.fetch_data("2025-01-01T00:00:00Z")
        >>> print(f"Fetched {len(records)} records")
    """
```

### Type Hints

- Use type hints for all function parameters and return values
- Use `Optional[T]` for nullable types
- Use `List[T]`, `Dict[K, V]` for collections
- Import types from `typing` module

```python
from typing import List, Dict, Any, Optional

async def process_records(
    records: List[Dict[str, Any]],
    batch_size: int = 100
) -> Optional[int]:
    """Process records in batches."""
    ...
```

### Inline Comments

- Use comments to explain **why**, not **what**
- Place comments above the code they describe
- Keep comments concise and up-to-date

```python
# Good: Explains reasoning
# Use exponential backoff to avoid overwhelming the API server
delay = base_delay * (2 ** attempt)

# Bad: States the obvious
# Multiply base_delay by 2 to the power of attempt
delay = base_delay * (2 ** attempt)
```

## Error Handling Best Practices

### Use Custom Exceptions

Always use custom exceptions from `core/exceptions.py` instead of generic exceptions:

```python
# Good
from core.exceptions import APIExtractionError

raise APIExtractionError(
    "Failed to fetch data from API",
    context={
        "api_url": self.api_url,
        "status_code": response.status_code
    },
    original_exception=e
)

# Bad
raise Exception(f"API error: {str(e)}")
```

### Provide Context

Include relevant context in exception messages:

```python
raise DatabaseError(
    "Failed to load normalized data",
    context={
        "source_type": extractor.source_type.value,
        "source_name": extractor.source_name,
        "records_to_load": len(normalized_items),
        "operation": "UPSERT",
        "table_name": "unified_items"
    },
    original_exception=e
)
```

### Classify Errors Correctly

- Use `RetryableError` for transient failures (network, rate limits)
- Use `NonRetryableError` for permanent failures (auth, validation)

```python
# Retryable
if response.status_code >= 500:
    raise NetworkError("Server error", ...)

# Non-retryable
if response.status_code == 401:
    raise AuthenticationError("Invalid API key", ...)
```

### Log with Structured Context

```python
logger.error(
    f"Normalization failed for raw_data_id={raw.id}",
    extra={
        "error_context": {
            "raw_data_id": raw.id,
            "source_name": self.source_name,
            "error_type": type(e).__name__
        }
    }
)
```

## Testing Requirements

### Unit Tests

- Test individual functions and methods in isolation
- Mock external dependencies (database, API calls)
- Aim for >80% code coverage

```python
import pytest
from unittest.mock import AsyncMock, patch

@pytest.mark.asyncio
async def test_api_extractor_retry_logic():
    """Test that API extractor retries on server errors."""
    extractor = APIExtractor(db_session, "test", "https://api.test.com")
    
    with patch("httpx.AsyncClient.get") as mock_get:
        # Simulate server error then success
        mock_get.side_effect = [
            httpx.Response(500, text="Server Error"),
            httpx.Response(200, json=[{"id": 1}])
        ]
        
        records = await extractor.fetch_data()
        assert len(records) == 1
        assert mock_get.call_count == 2
```

### Integration Tests

- Test complete workflows (extract → transform → load)
- Use test database or transactions with rollback
- Test error scenarios and recovery

### Test Organization

```
tests/
├── unit/
│   ├── test_extractors.py
│   ├── test_transformers.py
│   └── test_loaders.py
├── integration/
│   ├── test_etl_pipeline.py
│   └── test_api_endpoints.py
└── conftest.py  # Shared fixtures
```

## Database Migrations

### Using Alembic

1. Create migration:
   ```bash
   alembic revision --autogenerate -m "Add new column"
   ```

2. Review generated migration file

3. Apply migration:
   ```bash
   alembic upgrade head
   ```

### Migration Best Practices

- Always review auto-generated migrations
- Test migrations on development database first
- Include both upgrade and downgrade paths
- Add comments explaining complex migrations

## Pull Request Checklist

Before submitting a pull request, ensure:

- [ ] Code follows style guidelines
- [ ] All functions have docstrings with type hints
- [ ] Custom exceptions used for error handling
- [ ] Unit tests added for new functionality
- [ ] Integration tests pass
- [ ] No linting errors (`flake8`, `mypy`)
- [ ] Documentation updated (README, ARCHITECTURE.md)
- [ ] Commit messages are descriptive
- [ ] No sensitive data (API keys, passwords) in code

## Commit Message Format

Use conventional commit format:

```
<type>(<scope>): <subject>

<body>

<footer>
```

**Types**:
- `feat`: New feature
- `fix`: Bug fix
- `docs`: Documentation changes
- `refactor`: Code refactoring
- `test`: Adding tests
- `chore`: Maintenance tasks

**Examples**:
```
feat(extractors): Add retry logic with exponential backoff

Implemented retry mechanism for API extractor with configurable
max retries and exponential backoff delay. Includes circuit breaker
pattern to prevent cascading failures.

Closes #123
```

```
fix(loader): Handle deadlock errors with retry

Added deadlock detection and automatic retry for database upsert
operations. Deadlocks are now classified as retryable errors.

Fixes #456
```

## Code Review Guidelines

### As a Reviewer

- Be constructive and respectful
- Focus on code quality, not personal preferences
- Suggest improvements with examples
- Approve if code meets standards, even if you would have done it differently

### As an Author

- Respond to all comments
- Explain your reasoning for design decisions
- Be open to feedback and suggestions
- Make requested changes or discuss alternatives

## Performance Considerations

### Async/Await

- Use `async`/`await` for all I/O operations
- Don't block the event loop with CPU-intensive tasks
- Use `asyncio.gather()` for concurrent operations

```python
# Good: Concurrent API calls
results = await asyncio.gather(
    fetch_from_api_1(),
    fetch_from_api_2(),
    fetch_from_api_3()
)

# Bad: Sequential API calls
result1 = await fetch_from_api_1()
result2 = await fetch_from_api_2()
result3 = await fetch_from_api_3()
```

### Database Queries

- Use batch operations instead of individual inserts
- Limit query results to avoid memory issues
- Use indexes for frequently queried columns

```python
# Good: Batch upsert
await loader.load_batch(items, batch_size=500)

# Bad: Individual inserts
for item in items:
    await loader.load([item])
```

### Memory Management

- Process large files in chunks/streams
- Limit batch sizes (1000 records max)
- Clean up resources in `finally` blocks

## Security Best Practices

1. **Never commit secrets**
   - Use environment variables for API keys
   - Add `.env` to `.gitignore`
   - Use `.env.example` for documentation

2. **Validate all inputs**
   - Use Pydantic schemas for validation
   - Sanitize user inputs
   - Validate file paths and URLs

3. **Use parameterized queries**
   - SQLAlchemy ORM prevents SQL injection
   - Never concatenate SQL strings

4. **Truncate sensitive data in logs**
   ```python
   logger.error(
       "API error",
       extra={"response": response.text[:500]}  # Truncate
   )
   ```

## Getting Help

- Check existing documentation (README, ARCHITECTURE.md)
- Search existing issues on GitHub
- Ask questions in pull request comments
- Reach out to maintainers

## License

By contributing, you agree that your contributions will be licensed under the MIT License.
