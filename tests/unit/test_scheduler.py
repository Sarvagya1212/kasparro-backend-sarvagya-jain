import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from ingestion.scheduler import ETLScheduler

@pytest.mark.asyncio
async def test_scheduler_initialization():
    scheduler = ETLScheduler()
    assert scheduler.scheduler is not None
    assert scheduler.engine is not None

@pytest.mark.asyncio
async def test_scheduler_job_execution():
    with patch("ingestion.scheduler.ETLRunner") as mock_runner_cls:
        mock_runner = AsyncMock()
        mock_runner_cls.return_value = mock_runner
        
        # Mock database session
        mock_session = AsyncMock()
        
        with patch("ingestion.scheduler.async_sessionmaker") as mock_maker:
            mock_maker.return_value = MagicMock()
            mock_maker.return_value.__aenter__.return_value = mock_session
            
            scheduler = ETLScheduler()
            # Inject mocked session maker
            scheduler.SessionLocal = mock_maker
            
            await scheduler.run_etl_job()
            
            # Verify runner was called for sources
            assert mock_runner.run.called
