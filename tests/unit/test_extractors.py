"""
Unit tests for data extractors
"""

import pytest
from unittest.mock import Mock, AsyncMock, patch
from ingestion.extractors.api_extractor import APIExtractor
from ingestion.extractors.csv_extractor import CSVExtractor
from models.raw_data import SourceType


class TestAPIExtractor:
    """Test API extractor functionality"""
    
    @pytest.mark.asyncio
    async def test_fetch_data_success(self):
        """Test successful API data fetch"""
        # Mock database session
        mock_session = AsyncMock()
        
        # Create extractor
        extractor = APIExtractor(
            db_session=mock_session,
            source_name="test_api",
            api_url="https://api.example.com/data",
            api_key="test_key"
        )
        
        # Mock HTTP response
        mock_response_data = [
            {"id": "1", "name": "Item 1", "value": 100},
            {"id": "2", "name": "Item 2", "value": 200}
        ]
        
        with patch('httpx.AsyncClient') as mock_client:
            mock_client.return_value.__aenter__.return_value.get.return_value.json.return_value = mock_response_data
            mock_client.return_value.__aenter__.return_value.get.return_value.raise_for_status = Mock()
            
            # Fetch data
            result = await extractor.fetch_data()
            
            # Assertions
            assert len(result) == 2
            assert result[0]["id"] == "1"
            assert result[1]["name"] == "Item 2"
    
    def test_extract_record_id(self):
        """Test record ID extraction"""
        mock_session = Mock()
        extractor = APIExtractor(
            db_session=mock_session,
            source_name="test_api",
            api_url="https://api.example.com/data"
        )
        
        record = {"id": "test_123", "name": "Test"}
        record_id = extractor.extract_record_id(record)
        
        assert record_id == "test_123"
    
    def test_extract_timestamp(self):
        """Test timestamp extraction"""
        from datetime import datetime
        
        mock_session = Mock()
        extractor = APIExtractor(
            db_session=mock_session,
            source_name="test_api",
            api_url="https://api.example.com/data"
        )
        
        record = {
            "id": "1",
            "created_at": "2024-01-15T10:00:00Z"
        }
        
        timestamp = extractor.extract_timestamp(record)
        
        assert timestamp is not None
        assert isinstance(timestamp, datetime)


class TestCSVExtractor:
    """Test CSV extractor functionality"""
    
    @pytest.mark.asyncio
    async def test_fetch_data_from_csv(self, tmp_path):
        """Test CSV data extraction"""
        # Create temporary CSV file
        csv_file = tmp_path / "test.csv"
        csv_content = """id,name,price
1,Product A,10.99
2,Product B,20.99
3,Product C,30.99"""
        csv_file.write_text(csv_content)
        
        # Mock database session
        mock_session = AsyncMock()
        
        # Create extractor
        extractor = CSVExtractor(
            db_session=mock_session,
            source_name="test_csv",
            file_path=str(csv_file)
        )
        
        # Fetch data
        result = await extractor.fetch_data()
        
        # Assertions
        assert len(result) == 3
        assert result[0]["id"] == "1"
        assert result[1]["name"] == "Product B"
        assert result[2]["price"] == "30.99"
    
    def test_extract_record_id_from_csv(self):
        """Test CSV record ID extraction"""
        mock_session = Mock()
        extractor = CSVExtractor(
            db_session=mock_session,
            source_name="test_csv",
            file_path="/test/file.csv"
        )
        
        record = {"id": "csv_001", "name": "Test"}
        record_id = extractor.extract_record_id(record)
        
        assert record_id == "csv_001"
