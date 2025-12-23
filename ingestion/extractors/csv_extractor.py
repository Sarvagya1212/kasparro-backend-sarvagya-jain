"""
CSV file extractor with incremental loading
"""

import pandas as pd
from typing import List, Dict, Any, Optional
from datetime import datetime
from pathlib import Path
from ingestion.base import DataSource
from models.raw_data import SourceType
import logging

logger = logging.getLogger(__name__)


class CSVExtractor(DataSource):
    """
    Extract data from CSV files.
    
    Supports:
    - Incremental loading via row ID or timestamp
    - Type inference
    - Header normalization
    """
    
    def __init__(
        self,
        db_session,
        source_name: str,
        file_path: str,
        timestamp_column: Optional[str] = None,
        id_column: str = "id"
    ):
        checkpoint_type = "timestamp" if timestamp_column else "id"
        super().__init__(
            db_session=db_session,
            source_type=SourceType.CSV,
            source_name=source_name,
            checkpoint_type=checkpoint_type
        )
        self.file_path = Path(file_path)
        self.timestamp_column = timestamp_column
        self.id_column = id_column
    
    async def fetch_data(self, checkpoint_value: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Read CSV file with incremental loading.
        
        Args:
            checkpoint_value: Last processed ID or timestamp
        """
        if not self.file_path.exists():
            logger.warning(f"CSV file not found: {self.file_path}")
            return []
        
        logger.info(f"Reading CSV from {self.file_path}")
        
        # Read CSV with pandas
        df = pd.read_csv(self.file_path)
        
        # Normalize column names (strip whitespace, lowercase)
        df.columns = df.columns.str.strip().str.lower().str.replace(' ', '_')
        
        # Apply incremental filter
        if checkpoint_value:
            if self.checkpoint_type == "timestamp" and self.timestamp_column:
                df[self.timestamp_column] = pd.to_datetime(df[self.timestamp_column])
                checkpoint_dt = datetime.fromisoformat(checkpoint_value)
                df = df[df[self.timestamp_column] > checkpoint_dt]
            elif self.checkpoint_type == "id":
                df = df[df[self.id_column] > checkpoint_value]
        
        # Convert to list of dicts
        records = df.to_dict(orient="records")
        
        logger.info(f"Read {len(records)} records from CSV")
        return records
    
    def extract_record_id(self, record: Dict[str, Any]) -> str:
        """Extract ID from CSV record"""
        return str(record.get(self.id_column, ""))
    
    def extract_timestamp(self, record: Dict[str, Any]) -> Optional[datetime]:
        """Extract timestamp from CSV record"""
        if self.timestamp_column:
            timestamp_val = record.get(self.timestamp_column)
            if timestamp_val:
                try:
                    if isinstance(timestamp_val, str):
                        return datetime.fromisoformat(timestamp_val)
                    elif isinstance(timestamp_val, pd.Timestamp):
                        return timestamp_val.to_pydatetime()
                except:
                    pass
        return None