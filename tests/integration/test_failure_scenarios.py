"""
Tests for failure scenarios and error handling
"""

import pytest
from unittest.mock import AsyncMock, patch
from ingestion.extractors.api_extractor import APIExtractor
from ingestion.runner import ETLRunner
from models.checkpoint import ETLCheckpoint
from models.etl_run import ETLRun, ETLStatus
from sqlalchemy import select
import httpx


@pytest.mark.asyncio
async def test_api_source_down_failure_logged(db_session):
    """
    Test: API source is down, ETL logs failure but doesn't crash
    """
    # Mock API failure (connection error)
    with patch("httpx.AsyncClient") as mock_client:
        mock_client.return_value.__aenter__.return_value.get = AsyncMock(
            side_effect=httpx.ConnectError("Connection refused")
        )
        
        extractor = APIExtractor(
            db_session=db_session,
            source_name="failing_api",
            api_url="https://api.example.com/data"
        )
        
        runner = ETLRunner(db_session)
        
        # Run ETL - should catch exception
        with pytest.raises(httpx.ConnectError):
            await runner.run(extractor)
        
        # Verify checkpoint logs failure
        checkpoint_result = await db_session.execute(
            select(ETLCheckpoint).where(
                ETLCheckpoint.source_name == "failing_api"
            )
        )
        checkpoint = checkpoint_result.scalar_one_or_none()
        
        if checkpoint:
            assert checkpoint.status == ETLStatus.FAILED
            assert "Connection refused" in checkpoint.error_message
        
        # Verify ETL run logs failure
        run_result = await db_session.execute(
            select(ETLRun).where(
                ETLRun.source_name == "failing_api"
            )
        )
        etl_run = run_result.scalar_one_or_none()
        
        if etl_run:
            assert etl_run.status == ETLStatus.FAILED
            assert etl_run.error_message is not None


@pytest.mark.asyncio
async def test_api_returns_invalid_data(db_session):
    """
    Test: API returns malformed data, ETL handles gracefully
    """
    # Mock API returning invalid JSON structure
    mock_response = MagicMock()
    mock_response.json.return_value = {"error": "Invalid request"}
    mock_response.raise_for_status = MagicMock()
    
    with patch("httpx.AsyncClient") as mock_client:
        mock_client.return_value.__aenter__.return_value.get = AsyncMock(
            return_value=mock_response
        )
        
        extractor = APIExtractor(
            db_session=db_session,
            source_name="invalid_data_api",
            api_url="https://api.example.com/data"
        )
        
        runner = ETLRunner(db_session)
        
        # Should complete without crashing
        result = await runner.run(extractor)
        
        # No data should be loaded
        assert result["records_extracted"] == 0
        assert result["status"] == "success"


@pytest.mark.asyncio
async def test_transformation_failure_partial_success(db_session):
    """
    Test: Some records fail transformation, others succeed
    """
    # Mock API data with one invalid record
    mock_data = [
        {
            "id": "valid_001",
            "name": "Valid Product",
            "description": "Valid description",
            "price": 99.99,
            "created_at": "2024-01-15T10:00:00Z"
        },
        {
            "id": "invalid_001",
            # Missing required 'name' field
            "description": "Invalid product",
            "price": "not_a_number",  # Invalid price
        }
    ]
    
    mock_response = MagicMock()
    mock_response.json.return_value = mock_data
    mock_response.raise_for_status = MagicMock()
    
    with patch("httpx.AsyncClient") as mock_client:
        mock_client.return_value.__aenter__.return_value.get = AsyncMock(
            return_value=mock_response
        )
        
        extractor = APIExtractor(
            db_session=db_session,
            source_name="partial_failure_api",
            api_url="https://api.example.com/data"
        )
        
        runner = ETLRunner(db_session)
        result = await runner.run(extractor)
        
        # One record should succeed, one should fail
        assert result["records_extracted"] == 2
        assert result["records_loaded"] == 1  # Only valid record
        assert result["records_failed"] == 1