"""
Integration tests for complete ETL pipeline
"""

import pytest
from unittest.mock import AsyncMock, patch, MagicMock
from ingestion.extractors.api_extractor import APIExtractor
from ingestion.runner import ETLRunner
from models.normalized_data import UnifiedItem
from models.raw_data import RawData
from models.checkpoint import ETLCheckpoint
from models.etl_run import ETLRun, ETLStatus
from sqlalchemy import select


@pytest.mark.asyncio
async def test_full_etl_pipeline_integration(db_session, mock_api_data):
    """
    Integration test: Extract → Transform → Load → Verify
    """
    # Mock API response
    mock_response = MagicMock()
    mock_response.json.return_value = mock_api_data
    mock_response.raise_for_status = MagicMock()
    
    with patch("httpx.AsyncClient") as mock_client:
        mock_client.return_value.__aenter__.return_value.get = AsyncMock(
            return_value=mock_response
        )
        
        # Create extractor
        extractor = APIExtractor(
            db_session=db_session,
            source_name="integration_test_api",
            api_url="https://api.example.com/data",
            api_key="test_key"
        )
        
        # Create ETL runner
        runner = ETLRunner(db_session)
        
        # Run ETL pipeline
        result = await runner.run(extractor)
        
        # Verify result
        assert result["status"] == "success"
        assert result["records_extracted"] == 2
        assert result["records_loaded"] == 2
        assert result["records_failed"] == 0
        
        # Verify raw data was saved
        raw_result = await db_session.execute(select(RawData))
        raw_records = raw_result.scalars().all()
        assert len(raw_records) == 2
        
        # Verify normalized data was loaded
        unified_result = await db_session.execute(select(UnifiedItem))
        unified_items = unified_result.scalars().all()
        assert len(unified_items) == 2
        assert unified_items[0].title == "Test Product 1"
        assert unified_items[0].amount == 99.99
        assert unified_items[1].title == "Test Product 2"
        
        # Verify checkpoint was created
        checkpoint_result = await db_session.execute(
            select(ETLCheckpoint).where(
                ETLCheckpoint.source_name == "integration_test_api"
            )
        )
        checkpoint = checkpoint_result.scalar_one()
        assert checkpoint.status == ETLStatus.SUCCESS
        assert checkpoint.total_records_processed == 2
        
        # Verify ETL run was logged
        run_result = await db_session.execute(
            select(ETLRun).where(
                ETLRun.source_name == "integration_test_api"
            )
        )
        etl_run = run_result.scalar_one()
        assert etl_run.status == ETLStatus.SUCCESS
        assert etl_run.records_loaded == 2


@pytest.mark.asyncio
async def test_incremental_etl_no_duplicates(db_session, mock_api_data):
    """
    Integration test: Running ETL twice should not create duplicates
    """
    mock_response = MagicMock()
    mock_response.json.return_value = mock_api_data
    mock_response.raise_for_status = MagicMock()
    
    with patch("httpx.AsyncClient") as mock_client:
        mock_client.return_value.__aenter__.return_value.get = AsyncMock(
            return_value=mock_response
        )
        
        extractor = APIExtractor(
            db_session=db_session,
            source_name="incremental_test",
            api_url="https://api.example.com/data"
        )
        
        runner = ETLRunner(db_session)
        
        # First run
        result1 = await runner.run(extractor)
        assert result1["records_loaded"] == 2
        
        # Second run (should upsert, not duplicate)
        result2 = await runner.run(extractor)
        assert result2["records_loaded"] == 2
        
        # Verify only 2 records exist (no duplicates)
        unified_result = await db_session.execute(select(UnifiedItem))
        unified_items = unified_result.scalars().all()
        assert len(unified_items) == 2

