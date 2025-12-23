# ============================================================================
# File: tests/integration/test_etl_failure_recovery.py
# ============================================================================

import pytest
from unittest.mock import AsyncMock, patch, MagicMock
from sqlalchemy import select

from ingestion.extractors.api_extractor import APIExtractor
from ingestion.runner import ETLRunner
from models.raw_data import RawData
from models.normalized_data import UnifiedItem
from models.checkpoint import ETLCheckpoint
from models.etl_run import ETLStatus


@pytest.mark.asyncio
async def test_etl_failure_recovery_resume(db_session, mock_api_data):
    """
    Failure Recovery Test:
    1. ETL fails halfway during LOAD
    2. Checkpoint must NOT advance
    3. Restart ETL
    4. No duplicate unified records
    """

    # -------------------------------------------------------
    # STEP 1: Mock API Response
    # -------------------------------------------------------
    mock_response = MagicMock()
    mock_response.json.return_value = mock_api_data
    mock_response.raise_for_status = MagicMock()

    with patch("httpx.AsyncClient") as mock_client:
        mock_client.return_value.__aenter__.return_value.get = AsyncMock(
            return_value=mock_response
        )

        extractor = APIExtractor(
            db_session=db_session,
            source_name="recovery_test_api",
            api_url="https://api.example.com/data",
            api_key="test_key"
        )

        runner = ETLRunner(db_session)

        # -------------------------------------------------------
        # STEP 2: Force Loader Failure on First Run
        # -------------------------------------------------------
        with patch(
            "ingestion.loaders.postgres_loader.PostgresLoader.load",
            side_effect=Exception("Simulated DB failure during load")
        ):

            with pytest.raises(Exception):
                await runner.run(extractor)

        # -------------------------------------------------------
        # STEP 3: Validate FAILURE State
        # -------------------------------------------------------

        # Raw data SHOULD exist (extracted before failure)
        raw_result = await db_session.execute(select(RawData))
        raw_records = raw_result.scalars().all()
        assert len(raw_records) == 2
        assert all(r.processed is False for r in raw_records)

        # Unified data SHOULD NOT exist
        unified_result = await db_session.execute(select(UnifiedItem))
        assert unified_result.scalars().all() == []

        # Checkpoint SHOULD exist but NOT be advanced
        checkpoint_result = await db_session.execute(
            select(ETLCheckpoint).where(
                ETLCheckpoint.source_name == "recovery_test_api"
            )
        )
        checkpoint = checkpoint_result.scalar_one()
        assert checkpoint.status == ETLStatus.FAILED
        failed_checkpoint_value = checkpoint.checkpoint_value

        # -------------------------------------------------------
        # STEP 4: Restart ETL (No Failure)
        # -------------------------------------------------------
        result = await runner.run(extractor)

        # -------------------------------------------------------
        # STEP 5: Validate SUCCESSFUL RECOVERY
        # -------------------------------------------------------
        assert result["status"] == "success"
        assert result["records_loaded"] == 2
        assert result["records_failed"] == 0

        # Raw data should now be marked processed
        raw_result = await db_session.execute(select(RawData))
        raw_records = raw_result.scalars().all()
        assert all(r.processed is True for r in raw_records)

        # Unified items should exist EXACTLY once
        unified_result = await db_session.execute(select(UnifiedItem))
        unified_items = unified_result.scalars().all()

        assert len(unified_items) == 2
        assert len({u.external_id for u in unified_items}) == 2  # No duplicates

        # Checkpoint SHOULD NOW advance
        checkpoint_result = await db_session.execute(
            select(ETLCheckpoint).where(
                ETLCheckpoint.source_name == "recovery_test_api"
            )
        )
        checkpoint_after = checkpoint_result.scalar_one()

        assert checkpoint_after.status == ETLStatus.SUCCESS
        assert checkpoint_after.checkpoint_value != failed_checkpoint_value
