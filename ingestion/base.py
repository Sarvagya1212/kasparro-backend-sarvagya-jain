"""
Abstract base class for data sources with checkpoint management
"""

from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional
from datetime import datetime
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, and_
from models.checkpoint import ETLCheckpoint
from models.etl_run import ETLRun, ETLStatus
from models.raw_data import RawData, SourceType
import logging
import uuid

logger = logging.getLogger(__name__)


class DataSource(ABC):
    """
    Abstract base class for all data sources.
    
    Responsibilities:
    - Checkpoint management (incremental ingestion)
    - Raw data storage
    - ETL run tracking
    """
    
    def __init__(
        self, 
        db_session: AsyncSession,
        source_type: SourceType,
        source_name: str,
        checkpoint_type: str = "timestamp"
    ):
        self.db = db_session
        self.source_type = source_type
        self.source_name = source_name
        self.checkpoint_type = checkpoint_type
        self.etl_run: Optional[ETLRun] = None
        
    @abstractmethod
    async def fetch_data(self, checkpoint_value: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Fetch data from the source.
        
        Args:
            checkpoint_value: Last checkpoint (timestamp, ID, page, etc.)
            
        Returns:
            List of raw data dictionaries
        """
        pass
    
    @abstractmethod
    def extract_record_id(self, record: Dict[str, Any]) -> str:
        """Extract unique identifier from a record"""
        pass
    
    @abstractmethod
    def extract_timestamp(self, record: Dict[str, Any]) -> Optional[datetime]:
        """Extract timestamp from a record for checkpoint"""
        pass
    
    async def get_checkpoint(self) -> Optional[ETLCheckpoint]:
        """Retrieve checkpoint for this source"""
        result = await self.db.execute(
            select(ETLCheckpoint).where(
                and_(
                    ETLCheckpoint.source_type == self.source_type,
                    ETLCheckpoint.source_name == self.source_name
                )
            )
        )
        return result.scalar_one_or_none()
    
    async def create_or_update_checkpoint(
        self, 
        checkpoint_value: str,
        status: ETLStatus,
        records_processed: int = 0,
        error_message: Optional[str] = None
    ):
        """Create or update checkpoint"""
        checkpoint = await self.get_checkpoint()
        
        if checkpoint is None:
            # Create new checkpoint
            checkpoint = ETLCheckpoint(
                source_type=self.source_type,
                source_name=self.source_name,
                checkpoint_type=self.checkpoint_type,
                checkpoint_value=checkpoint_value,
                status=status,
                last_run_at=datetime.utcnow(),
                total_runs=1,
                total_records_processed=records_processed,
                last_records_processed=records_processed,
                error_message=error_message
            )
            
            if status == ETLStatus.SUCCESS:
                checkpoint.last_success_at = datetime.utcnow()
            elif status == ETLStatus.FAILED:
                checkpoint.last_failure_at = datetime.utcnow()
                
            self.db.add(checkpoint)
        else:
            # Update existing checkpoint
            checkpoint.checkpoint_value = checkpoint_value
            checkpoint.status = status
            checkpoint.last_run_at = datetime.utcnow()
            checkpoint.total_runs += 1
            checkpoint.total_records_processed += records_processed
            checkpoint.last_records_processed = records_processed
            checkpoint.error_message = error_message
            checkpoint.updated_at = datetime.utcnow()
            
            if status == ETLStatus.SUCCESS:
                checkpoint.last_success_at = datetime.utcnow()
            elif status == ETLStatus.FAILED:
                checkpoint.last_failure_at = datetime.utcnow()
        
        await self.db.commit()
        return checkpoint
    
    async def start_etl_run(self, checkpoint_before: Optional[str] = None) -> ETLRun:
        """Create ETL run record"""
        self.etl_run = ETLRun(
            run_id=uuid.uuid4(),
            source_type=self.source_type,
            source_name=self.source_name,
            status=ETLStatus.RUNNING,
            started_at=datetime.utcnow(),
            checkpoint_before=checkpoint_before
        )
        self.db.add(self.etl_run)
        await self.db.commit()
        await self.db.refresh(self.etl_run)
        return self.etl_run
    
    async def complete_etl_run(
        self,
        status: ETLStatus,
        records_extracted: int = 0,
        records_loaded: int = 0,
        records_failed: int = 0,
        checkpoint_after: Optional[str] = None,
        error_message: Optional[str] = None
    ):
        """Complete ETL run with statistics"""
        if self.etl_run:
            self.etl_run.status = status
            self.etl_run.completed_at = datetime.utcnow()
            self.etl_run.duration_seconds = (
                self.etl_run.completed_at - self.etl_run.started_at
            ).total_seconds()
            self.etl_run.records_extracted = records_extracted
            self.etl_run.records_loaded = records_loaded
            self.etl_run.records_failed = records_failed
            self.etl_run.checkpoint_after = checkpoint_after
            self.etl_run.error_message = error_message
            
            await self.db.commit()
    
    async def save_raw_data(self, records: List[Dict[str, Any]]) -> List[RawData]:
        """Save raw data to database"""
        raw_records = []
        
        for record in records:
            raw_data = RawData(
                source_type=self.source_type,
                source_name=self.source_name,
                source_id=self.extract_record_id(record),
                raw_payload=record,
                ingested_at=datetime.utcnow(),
                etl_run_id=self.etl_run.id if self.etl_run else None
            )
            raw_records.append(raw_data)
            self.db.add(raw_data)
        
        await self.db.commit()
        
        # Refresh to get IDs
        for raw_data in raw_records:
            await self.db.refresh(raw_data)
            
        return raw_records
    
    async def run_incremental(self) -> Dict[str, Any]:
        """
        Execute incremental ETL with checkpoint management.
        
        Returns:
            Dictionary with run statistics
        """
        try:
            # Get checkpoint
            checkpoint = await self.get_checkpoint()
            checkpoint_value = ""
            
            if checkpoint:
                checkpoint_value = checkpoint.checkpoint_value or ""
            
            logger.info(
                f"Starting ETL for {self.source_name} "
                f"(checkpoint: {checkpoint_value})"
            )
            
            # Start ETL run
            await self.start_etl_run(checkpoint_before=checkpoint_value)
            
            # Fetch data
            records = await self.fetch_data(checkpoint_value)
            records_extracted = len(records)
            
            logger.info(f"Extracted {records_extracted} records")
            
            if records_extracted == 0:
                # No new data
                await self.complete_etl_run(
                    status=ETLStatus.SUCCESS,
                    records_extracted=0,
                    checkpoint_after=checkpoint_value
                )
                await self.create_or_update_checkpoint(
                    checkpoint_value=checkpoint_value or "",
                    status=ETLStatus.SUCCESS,
                    records_processed=0
                )
                return {
                    "status": "success",
                    "records_extracted": 0,
                    "message": "No new data"
                }
            
            # Save raw data
            raw_records = await self.save_raw_data(records)
            
            # Calculate new checkpoint
            new_checkpoint = self._calculate_checkpoint(records)
            
            # Complete ETL run
            await self.complete_etl_run(
                status=ETLStatus.SUCCESS,
                records_extracted=records_extracted,
                records_loaded=records_extracted,
                checkpoint_after=new_checkpoint
            )
            
            # Update checkpoint
            await self.create_or_update_checkpoint(
                checkpoint_value=new_checkpoint,
                status=ETLStatus.SUCCESS,
                records_processed=records_extracted
            )
            
            logger.info(
                f"ETL completed for {self.source_name}. "
                f"Records: {records_extracted}, New checkpoint: {new_checkpoint}"
            )
            
            return {
                "status": "success",
                "records_extracted": records_extracted,
                "records_loaded": records_extracted,
                "checkpoint": new_checkpoint,
                "raw_data_ids": [r.id for r in raw_records]
            }
            
        except Exception as e:
            logger.error(f"ETL failed for {self.source_name}: {str(e)}")
            
            # Complete ETL run with failure
            if self.etl_run:
                await self.complete_etl_run(
                    status=ETLStatus.FAILED,
                    error_message=str(e)
                )
            
            # Update checkpoint with failure
            await self.create_or_update_checkpoint(
                checkpoint_value=checkpoint_value or "",
                status=ETLStatus.FAILED,
                error_message=str(e)
            )
            
            raise
    
    def _calculate_checkpoint(self, records: List[Dict[str, Any]]) -> str:
        """Calculate new checkpoint value from records"""
        if self.checkpoint_type == "timestamp":
            # Get max timestamp
            timestamps = [
                self.extract_timestamp(r) 
                for r in records 
                if self.extract_timestamp(r)
            ]
            if timestamps:
                return max(timestamps).isoformat()
        elif self.checkpoint_type == "id":
            # Get max ID
            ids = [self.extract_record_id(r) for r in records]
            if ids:
                return max(ids)
        
        return datetime.utcnow().isoformat()