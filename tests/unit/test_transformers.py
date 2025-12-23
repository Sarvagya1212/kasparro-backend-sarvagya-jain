"""
Unit tests for data transformers
"""

import pytest
from datetime import datetime
from ingestion.transformers.normalizer import Normalizer
from models.raw_data import SourceType


class TestNormalizer:
    """Test data normalization functionality"""
    
    def test_normalize_api_data(self):
        """Test API data normalization"""
        normalizer = Normalizer(
            source_type=SourceType.API,
            source_name="test_api"
        )
        
        raw_record = {
            "id": "api_001",
            "title": "Test Product",
            "description": "A test product description",
            "category": "electronics",
            "price": 99.99,
            "url": "https://example.com/product/1",
            "created_at": "2024-01-15T10:00:00Z"
        }
        
        result = normalizer.normalize(raw_record, raw_data_id=1)
        
        # Assertions
        assert result.external_id == "api_001"
        assert result.title == "Test Product"
        assert result.description == "A test product description"
        assert result.category == "electronics"
        assert result.amount == 99.99
        assert result.url == "https://example.com/product/1"
        assert result.source_type == SourceType.API
        assert result.source_name == "test_api"
    
    def test_normalize_csv_data(self):
        """Test CSV data normalization"""
        normalizer = Normalizer(
            source_type=SourceType.CSV,
            source_name="test_csv"
        )
        
        raw_record = {
            "id": "csv_001",
            "name": "CSV Product",
            "details": "Product details",
            "category": "home",
            "cost": "49.99",
            "date": "2024-01-15"
        }
        
        result = normalizer.normalize(raw_record, raw_data_id=2)
        
        # Assertions
        assert result.external_id == "csv_001"
        assert result.title == "CSV Product"
        assert result.description == "Product details"
        assert result.category == "home"
        assert result.amount == 49.99
        assert result.source_type == SourceType.CSV
    
    def test_normalize_handles_missing_fields(self):
        """Test normalization with missing optional fields"""
        normalizer = Normalizer(
            source_type=SourceType.API,
            source_name="test_api"
        )
        
        raw_record = {
            "id": "minimal_001",
            "title": "Minimal Product"
        }
        
        result = normalizer.normalize(raw_record, raw_data_id=3)
        
        # Assertions
        assert result.external_id == "minimal_001"
        assert result.title == "Minimal Product"
        assert result.description is None
        assert result.category is None
        assert result.amount is None
    
    def test_normalize_extracts_metadata(self):
        """Test metadata extraction"""
        normalizer = Normalizer(
            source_type=SourceType.API,
            source_name="test_api"
        )
        
        raw_record = {
            "id": "meta_001",
            "title": "Product with Metadata",
            "custom_field": "custom_value",
            "rating": 4.5,
            "reviews_count": 100
        }
        
        result = normalizer.normalize(raw_record, raw_data_id=4)
        
        # Assertions
        assert result.extra_metadata is not None
        assert "custom_field" in result.extra_metadata
        assert result.extra_metadata["custom_field"] == "custom_value"