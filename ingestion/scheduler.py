import logging
import asyncio
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine, async_sessionmaker
from core.config import settings
from ingestion.runner import ETLRunner
from ingestion.extractors.api_extractor import APIExtractor
# from ingestion.extractors.csv_extractor import CSVExtractor

logger = logging.getLogger(__name__)

class ETLScheduler:
    def __init__(self):
        self.scheduler = AsyncIOScheduler()
        self.engine = create_async_engine(settings.DATABASE_URL, echo=False)
        self.SessionLocal = async_sessionmaker(
            self.engine, class_=AsyncSession, expire_on_commit=False
        )

    async def run_etl_job(self):
        """Job to run ETL pipeline"""
        logger.info("Scheduler: Starting ETL job")
        async with self.SessionLocal() as session:
            try:
                runner = ETLRunner(session)
                # Define sources - In a real app, these should come from config/DB
                sources = [
                     APIExtractor(
                        db_session=session,
                        source_name="api_source_1",
                        api_url="https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd&order=market_cap_desc&per_page=100&page=1&sparkline=false", # Example public API
                        id_field="id",
                        timestamp_field="last_updated" 
                    )
                ]
                
                for source in sources:
                    await runner.run(source)
                    
            except Exception as e:
                logger.error(f"Scheduler: ETL job failed - {e}")

    def start(self):
        """Start the scheduler"""
        self.scheduler.add_job(
            self.run_etl_job,
            trigger=IntervalTrigger(minutes=30),  # Run every 30 minutes
            id="etl_job",
            replace_existing=True
        )
        self.scheduler.start()
        logger.info("ETL Scheduler started")

    def stop(self):
        self.scheduler.shutdown()
        logger.info("ETL Scheduler stopped")
