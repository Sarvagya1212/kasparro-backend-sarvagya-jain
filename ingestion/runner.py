# ============================================================================
# File: ingestion/runner.py
# Description: Production-grade ETL orchestrator with comprehensive error handling
# ============================================================================
"""
ETL Runner - Orchestrates Extract, Transform, Load pipeline.

This module provides robust ETL orchestration with:
- Comprehensive error handling and recovery
- Partial failure support (continue processing on individual record failures)
- Detailed error context and logging
- Transaction safety with proper rollback
- Accurate ETL run metrics tracking
"""

from typing import Dict, Any, List, Optional
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
import logging

from ingestion.base import DataSource
from ingestion.transformers.normalizer import DataNormalizer
from ingestion.loaders.postgres_loader import PostgresLoader
from models.raw_data import RawData
from models.etl_run import ETLStatus
from core.exceptions import (
    ETLException,
    ExtractionError,
    TransformationError,
    LoadError,
    NormalizationError,
    DatabaseError
)

logger = logging.getLogger(__name__)


class ETLRunner:
    """
    Production-grade ETL Orchestrator

    Responsibilities:
    - Orchestrate Extract → Transform → Load
    - Ensure idempotency
    - Handle partial failures safely
    - Control checkpoint advancement
    - Record accurate ETL run metrics
    """

    def __init__(self, db_session: AsyncSession):
        self.db = db_session

    async def run(self, extractor: DataSource) -> Dict[str, Any]:
        """
        Run full ETL pipeline for a given extractor with comprehensive error handling.
        
        Pipeline phases:
        1. Extract - Fetch data from source
        2. Transform - Normalize data to unified schema
        3. Load - Upsert data to database
        4. Checkpoint - Update progress tracking
        
        Args:
            extractor: Data source extractor instance
        
        Returns:
            Dictionary with run statistics:
            - status: "success" or "partial_success" or "failed"
            - records_extracted: Number of records extracted
            - records_loaded: Number of records successfully loaded
            - records_failed: Number of records that failed processing
            - error_details: List of error details (if any)
        
        Raises:
            ExtractionError: If extraction phase fails completely
            LoadError: If load phase fails completely
            ETLException: For other ETL-related errors
        """
        records_extracted = 0
        records_loaded = 0
        records_failed = 0
        error_details = []

        try:
            # --------------------------------------------------
            # PHASE 1: EXTRACTION
            # --------------------------------------------------
            logger.info(f"Starting extraction for {extractor.source_name}")
            
            try:
                extract_result = await extractor.run_incremental()

                if extract_result["status"] != "success":
                    raise ExtractionError(
                        "Extraction phase failed",
                        context={
                            "source_type": extractor.source_type.value,
                            "source_name": extractor.source_name,
                            "extract_result": extract_result
                        }
                    )

                records_extracted = extract_result.get("records_extracted", 0)
                logger.info(f"Extracted {records_extracted} records")

                if records_extracted == 0:
                    logger.info("No new data to process")
                    return {
                        "status": "success",
                        "records_extracted": 0,
                        "records_loaded": 0,
                        "records_failed": 0,
                        "message": "No new data"
                    }
            
            except ExtractionError:
                raise  # Re-raise extraction errors
            
            except Exception as e:
                raise ExtractionError(
                    "Unexpected error during extraction",
                    context={
                        "source_type": extractor.source_type.value,
                        "source_name": extractor.source_name
                    },
                    original_exception=e
                )

            # --------------------------------------------------
            # PHASE 2: FETCH UNPROCESSED RAW DATA
            # --------------------------------------------------
            # Fetch ALL unprocessed data for this source to support resume-on-failure
            logger.info(f"Fetching unprocessed raw data for {extractor.source_name}")
            
            try:
                raw_result = await self.db.execute(
                    select(RawData).where(
                        RawData.source_type == extractor.source_type,
                        RawData.source_name == extractor.source_name,
                        RawData.processed.is_(False)
                    ).limit(1000)  # Process in batches to avoid OOM
                )
                raw_records: List[RawData] = raw_result.scalars().all()
                logger.info(f"Found {len(raw_records)} unprocessed records")
            
            except Exception as e:
                raise DatabaseError(
                    "Failed to fetch unprocessed raw data",
                    context={
                        "source_type": extractor.source_type.value,
                        "source_name": extractor.source_name,
                        "operation": "SELECT",
                        "table_name": "raw_data"
                    },
                    original_exception=e
                )

            # --------------------------------------------------
            # PHASE 3: TRANSFORMATION (NORMALIZATION)
            # --------------------------------------------------
            logger.info(f"Starting normalization for {len(raw_records)} records")
            
            normalizer = DataNormalizer(
                source_type=extractor.source_type,
                source_name=extractor.source_name
            )

            normalized_items = []
            failed_raw_ids = []

            for raw in raw_records:
                try:
                    normalized = normalizer.normalize(
                        raw_record=raw.raw_payload,
                        raw_data_id=raw.id
                    )
                    normalized_items.append(normalized)
                
                except Exception as e:
                    records_failed += 1
                    failed_raw_ids.append(raw.id)
                    
                    error_detail = {
                        "phase": "normalization",
                        "raw_data_id": raw.id,
                        "error_type": type(e).__name__,
                        "error_message": str(e)
                    }
                    error_details.append(error_detail)
                    
                    logger.error(
                        f"Normalization failed for raw_data_id={raw.id}: {str(e)}",
                        extra={"error_context": error_detail}
                    )
            
            logger.info(
                f"Normalization complete: {len(normalized_items)} succeeded, "
                f"{records_failed} failed"
            )

            # --------------------------------------------------
            # PHASE 4: LOAD (IDEMPOTENT UPSERT)
            # --------------------------------------------------
            if normalized_items:
                logger.info(f"Starting load for {len(normalized_items)} normalized records")
                
                loader = PostgresLoader(self.db)

                try:
                    records_loaded = await loader.load(normalized_items)
                    logger.info(f"Successfully loaded {records_loaded} records")
                
                except Exception as e:
                    logger.error(f"Load phase failed: {str(e)}")
                    await self.db.rollback()
                    
                    raise LoadError(
                        "Failed to load normalized data",
                        context={
                            "source_type": extractor.source_type.value,
                            "source_name": extractor.source_name,
                            "records_to_load": len(normalized_items),
                            "operation": "UPSERT",
                            "table_name": "unified_items"
                        },
                        original_exception=e
                    )
            else:
                logger.warning("No records to load (all normalization failed)")

            # --------------------------------------------------
            # PHASE 5: MARK RAW DATA AS PROCESSED
            # --------------------------------------------------
            logger.info("Marking raw data as processed")
            
            try:
                # Only mark successfully normalized records as processed
                successfully_processed_ids = [
                    raw.id for raw in raw_records 
                    if raw.id not in failed_raw_ids
                ]
                
                for raw in raw_records:
                    if raw.id in successfully_processed_ids:
                        raw.processed = True
                        raw.processed_at = extractor.etl_run.completed_at if extractor.etl_run else None

                await self.db.commit()
                logger.info(f"Marked {len(successfully_processed_ids)} records as processed")
            
            except Exception as e:
                logger.error(f"Failed to mark records as processed: {str(e)}")
                await self.db.rollback()
                
                raise DatabaseError(
                    "Failed to update raw data processed status",
                    context={
                        "source_type": extractor.source_type.value,
                        "source_name": extractor.source_name,
                        "operation": "UPDATE",
                        "table_name": "raw_data"
                    },
                    original_exception=e
                )

            # --------------------------------------------------
            # PHASE 6: FINALIZE ETL RUN
            # --------------------------------------------------
            status = ETLStatus.SUCCESS if records_failed == 0 else ETLStatus.PARTIAL
            
            await extractor.complete_etl_run(
                status=status,
                records_extracted=records_extracted,
                records_loaded=records_loaded,
                records_failed=records_failed,
                checkpoint_after=extract_result.get("checkpoint"),
                error_message=f"{records_failed} records failed" if records_failed > 0 else None
            )

            result = {
                "status": "success" if records_failed == 0 else "partial_success",
                "records_extracted": records_extracted,
                "records_loaded": records_loaded,
                "records_failed": records_failed
            }
            
            if error_details:
                result["error_details"] = error_details
            
            logger.info(
                f"ETL run completed: {result['status']} - "
                f"Extracted: {records_extracted}, Loaded: {records_loaded}, Failed: {records_failed}"
            )
            
            return result

        except (ExtractionError, LoadError, DatabaseError) as e:
            # Known ETL errors - log with context and fail gracefully
            logger.error(
                f"ETL pipeline failed: {e.message}",
                extra={"error_context": e.to_dict()}
            )
            
            await self.db.rollback()

            # Mark ETL run as FAILED
            if extractor.etl_run:
                await extractor.complete_etl_run(
                    status=ETLStatus.FAILED,
                    records_extracted=records_extracted,
                    records_loaded=records_loaded,
                    records_failed=records_failed,
                    error_message=e.message
                )

            raise
        
        except Exception as e:
            # Unexpected errors - log and wrap in ETLException
            logger.exception("Unexpected error in ETL pipeline")

            await self.db.rollback()

            # Mark ETL run as FAILED
            if extractor.etl_run:
                await extractor.complete_etl_run(
                    status=ETLStatus.FAILED,
                    records_extracted=records_extracted,
                    records_loaded=records_loaded,
                    records_failed=records_failed,
                    error_message=str(e)
                )

            raise ETLException(
                "Unexpected error in ETL pipeline",
                context={
                    "source_type": extractor.source_type.value,
                    "source_name": extractor.source_name,
                    "records_extracted": records_extracted,
                    "records_loaded": records_loaded,
                    "records_failed": records_failed
                },
                original_exception=e
            )
