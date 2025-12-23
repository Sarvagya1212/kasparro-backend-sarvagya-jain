"""
Unit tests for data loaders
"""

import pytest
from unittest.mock import AsyncMock, Mock
from ingestion.loaders.postgres_loader import PostgresLoader
from schemas.normalized import UnifiedItemCreate
from models.raw_data import SourceType
from models.normalized_data import ItemStatus


class TestPostgresLoader:
    """Test PostgreSQL loader functionality"""
    
    @pytest.mark.asyncio
    async def test_load_single_item(self):
        """Test loading a single item"""
        # Mock database session
        mock_session = AsyncMock()
        mock_session.execute = AsyncMock()
        mock_session.commit = AsyncMock()
        
        loader = PostgresLoader(mock_session)
        
        # Create test item
        item = UnifiedItemCreate(
            source_type=SourceType.API,
            source_name="test_source",
            external_id="test_001",
            title="Test Item",
            description="Test description",
            category="test_category",
            status=ItemStatus.ACTIVE,
            raw_data_id=1
        )
        
        # Load item
        result = await loader.load([item])
        
        # Assertions
        assert result == 1
        mock_session.execute.assert_called_once()
        mock_session.commit.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_load_multiple_items(self):
        """Test loading multiple items"""
        mock_session = AsyncMock()
        mock_session.execute = AsyncMock()
        mock_session.commit = AsyncMock()
        
        loader = PostgresLoader(mock_session)
        
        # Create test items
        items = [
            UnifiedItemCreate(
                source_type=SourceType.API,
                source_name="test_source",
                external_id=f"test_{i:03d}",
                title=f"Test Item {i}",
                status=ItemStatus.ACTIVE,
                raw_data_id=i
            )
            for i in range(1, 6)
        ]
        
        # Load items
        result = await loader.load(items)
        
        # Assertions
        assert result == 5
        assert mock_session.execute.call_count == 5
        mock_session.commit.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_load_empty_list(self):
        """Test loading empty list"""
        mock_session = AsyncMock()
        loader = PostgresLoader(mock_session)
        
        result = await loader.load([])
        
        assert result == 0
        mock_session.execute.assert_not_called()
    
    @pytest.mark.asyncio
    async def test_load_handles_duplicates(self):
        """Test that loader handles duplicate external_ids (UPSERT)"""
        mock_session = AsyncMock()
        mock_session.execute = AsyncMock()
        mock_session.commit = AsyncMock()
        
        loader = PostgresLoader(mock_session)
        
        # Create duplicate items (same external_id)
        item1 = UnifiedItemCreate(
            source_type=SourceType.API,
            source_name="test_source",
            external_id="duplicate_001",
            title="Original Title",
            status=ItemStatus.ACTIVE,
            raw_data_id=1
        )
        
        item2 = UnifiedItemCreate(
            source_type=SourceType.API,
            source_name="test_source",
            external_id="duplicate_001",
            title="Updated Title",
            status=ItemStatus.ACTIVE,
            raw_data_id=2
        )
        
        # Load both items
        result = await loader.load([item1, item2])
        
        # Should still process both (UPSERT handles duplicates)
        assert result == 2
        assert mock_session.execute.call_count == 2
