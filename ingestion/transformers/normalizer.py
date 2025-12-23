
"""
Transform raw data into normalized unified schema with Pydantic validation
"""

from typing import Dict, Any, Optional, List
from datetime import datetime
from pydantic import BaseModel, Field, validator
from schemas.normalized import UnifiedItemCreate
from models.raw_data import SourceType
import logging

logger = logging.getLogger(__name__)


class DataNormalizer:
    """
    Normalize data from different sources into unified schema.
    
    Handles:
    - Schema mapping
    - Type conversion
    - Data validation
    - Default values
    """
    
    def __init__(self, source_type: SourceType, source_name: str):
        self.source_type = source_type
        self.source_name = source_name
    
    def normalize(self, raw_record: Dict[str, Any], raw_data_id: int) -> UnifiedItemCreate:
        """
        Normalize a raw record into unified schema.
        
        Returns:
            Validated UnifiedItemCreate Pydantic model
        """
        if self.source_type == SourceType.API:
            return self._normalize_api(raw_record, raw_data_id)
        elif self.source_type == SourceType.CSV:
            return self._normalize_csv(raw_record, raw_data_id)
        elif self.source_type == SourceType.RSS:
            return self._normalize_rss(raw_record, raw_data_id)
        else:
            raise ValueError(f"Unknown source type: {self.source_type}")
    
    def _normalize_api(self, record: Dict[str, Any], raw_data_id: int) -> UnifiedItemCreate:
        """Normalize API data"""
        return UnifiedItemCreate(
            source_type=self.source_type,
            source_name=self.source_name,
            external_id=str(record.get("id", record.get("item_id", ""))),
            raw_data_id=raw_data_id,
            title=record.get("name", record.get("title", "Untitled")),
            description=record.get("description", ""),
            category=record.get("category"),
            url=record.get("url"),
            image_url=record.get("image_url", record.get("thumbnail")),
            author=record.get("author"),
            amount=self._parse_float(record.get("price", record.get("cost"))),
            quantity=self._parse_int(record.get("quantity", record.get("stock"))),
            rating=self._parse_float(record.get("rating")),
            tags=record.get("tags", []) if isinstance(record.get("tags"), list) else [],
            extra_metadata={
                k: v for k, v in record.items() 
                if k not in ["id", "name", "title", "description", "category"]
            },
            published_at=self._parse_datetime(record.get("created_date", record.get("created_at"))),
        )
    
    def _normalize_csv(self, record: Dict[str, Any], raw_data_id: int) -> UnifiedItemCreate:
        """Normalize CSV data"""
        # Parse tags from comma-separated string
        tags = []
        if record.get("keywords") or record.get("tags"):
            tags_str = record.get("keywords") or record.get("tags")
            if isinstance(tags_str, str):
                tags = [t.strip() for t in tags_str.split(",") if t.strip()]
        
        return UnifiedItemCreate(
            source_type=self.source_type,
            source_name=self.source_name,
            external_id=str(record.get("id", record.get("row_id", ""))),
            raw_data_id=raw_data_id,
            title=record.get("product_name", record.get("name", record.get("title", "Untitled"))),
            description=record.get("details", record.get("description", "")),
            category=record.get("category"),
            url=record.get("url", record.get("link")),
            amount=self._parse_float(record.get("cost", record.get("price"))),
            quantity=self._parse_int(record.get("quantity", record.get("qty"))),
            tags=tags,
            extra_metadata={
                k: v for k, v in record.items()
                if k not in ["id", "row_id", "product_name", "name", "details", "category"]
            },
            published_at=self._parse_datetime(record.get("date", record.get("created_at"))),
        )
    
    def _normalize_rss(self, record: Dict[str, Any], raw_data_id: int) -> UnifiedItemCreate:
        """Normalize RSS/Feed data"""
        return UnifiedItemCreate(
            source_type=self.source_type,
            source_name=self.source_name,
            external_id=str(record.get("id", record.get("guid", ""))),
            raw_data_id=raw_data_id,
            title=record.get("title", "Untitled"),
            description=record.get("summary", record.get("description", "")),
            category=record.get("category"),
            url=record.get("link"),
            author=record.get("author"),
            tags=record.get("tags", []),
            extra_metadata={
                "content": record.get("content", ""),
                **{k: v for k, v in record.items() if k not in ["id", "title", "summary", "link"]}
            },
            published_at=self._parse_datetime(record.get("published")),
        )
    
    @staticmethod
    def _parse_float(value: Any) -> Optional[float]:
        """Safely parse float value"""
        if value is None or value == "":
            return None
        try:
            return float(value)
        except (ValueError, TypeError):
            return None
    
    @staticmethod
    def _parse_int(value: Any) -> Optional[int]:
        """Safely parse int value"""
        if value is None or value == "":
            return None
        try:
            return int(float(value))  # Handle "10.0" strings
        except (ValueError, TypeError):
            return None
    
    @staticmethod
    def _parse_datetime(value: Any) -> Optional[datetime]:
        """Safely parse datetime value"""
        if value is None or value == "":
            return None
        if isinstance(value, datetime):
            return value
        try:
            return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        except:
            return None