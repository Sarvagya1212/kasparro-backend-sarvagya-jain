"""
Load normalized data into PostgreSQL with upsert logic (idempotency)
"""

from typing import List, Optional
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from models.normalized_data import UnifiedItem
from schemas.normalized import UnifiedItemCreate
import logging

logger = logging.getLogger(__name__)


class PostgresLoader:
    """
    Load data into PostgreSQL with idempotent upsert operations.
    
    Ensures:
    - No duplicate rows on repeated runs
    - Updates existing records if source data changes
    - Atomic transactions
    """
    
    def __init__(self, db_session: AsyncSession):
        self.db = db_session
    
    async def load(
        self, 
        items: List[UnifiedItemCreate],
        etl_run_id: Optional[int] = None
    ) -> int:
        """
        Load items with upsert logic (INSERT ON CONFLICT UPDATE).
        
        Args:
            items: List of validated UnifiedItemCreate models
            etl_run_id: ETL run ID for tracking
            
        Returns:
            Number of records loaded
        """
        if not items:
            return 0
        
        loaded_count = 0
        
        for item in items:
            # Convert Pydantic model to dict
            item_dict = item.dict(exclude_unset=True)
            if etl_run_id:
                item_dict["etl_run_id"] = etl_run_id
            
            # PostgreSQL INSERT ... ON CONFLICT (upsert)
            stmt = insert(UnifiedItem).values(**item_dict)
            
            # Define unique constraint: (source_type, source_name, external_id)
            stmt = stmt.on_conflict_do_update(
                index_elements=["source_type", "source_name", "external_id"],
                set_={
                    "title": stmt.excluded.title,
                    "description": stmt.excluded.description,
                    "category": stmt.excluded.category,
                    "url": stmt.excluded.url,
                    "image_url": stmt.excluded.image_url,
                    "author": stmt.excluded.author,
                    "amount": stmt.excluded.amount,
                    "quantity": stmt.excluded.quantity,
                    "rating": stmt.excluded.rating,
                    "tags": stmt.excluded.tags,
                    "metadata": stmt.excluded.metadata,
                    "status": stmt.excluded.status,
                    "published_at": stmt.excluded.published_at,
                    "updated_at": stmt.excluded.updated_at,
                    "etl_run_id": stmt.excluded.etl_run_id,
                }
            )
            
            await self.db.execute(stmt)
            loaded_count += 1
        
        await self.db.commit()
        
        logger.info(f"Loaded {loaded_count} items into UnifiedItem table")
        return loaded_count
    
    async def load_batch(
        self,
        items: List[UnifiedItemCreate],
        etl_run_id: Optional[int] = None,
        batch_size: int = 500
    ) -> int:
        """
        Load items in batches for better performance.
        
        Args:
            items: List of validated items
            etl_run_id: ETL run ID
            batch_size: Number of items per batch
            
        Returns:
            Total number of records loaded
        """
        total_loaded = 0
        
        for i in range(0, len(items), batch_size):
            batch = items[i:i + batch_size]
            count = await self.load(batch, etl_run_id)
            total_loaded += count
            
            logger.info(f"Batch {i//batch_size + 1}: Loaded {count} items")
        
        return total_loaded
