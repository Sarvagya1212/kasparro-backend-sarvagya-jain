import asyncio
import logging
import sys
import os

# Add current directory to path to allow imports from core, models, etc.
sys.path.append(os.getcwd())

from sqlalchemy.ext.asyncio import create_async_engine
from core.config import settings
from models.base import Base
# Import all models to ensure they are registered
from models.raw_data import RawData
from models.normalized_data import UnifiedItem
from models.etl_run import ETLRun
from models.checkpoint import ETLCheckpoint

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def init_database():
    logger.info(f"Connecting to database...")
    # Create engine
    engine = create_async_engine(settings.DATABASE_URL, echo=True)
    
    async with engine.begin() as conn:
        logger.info("Creating tables...")
        # Create all tables defined in models
        await conn.run_sync(Base.metadata.create_all)
        logger.info("Tables created successfully.")
        
    await engine.dispose()

if __name__ == "__main__":
    asyncio.run(init_database())
