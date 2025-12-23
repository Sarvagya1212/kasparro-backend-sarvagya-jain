"""
Script to run ETL pipeline for all configured sources
"""

import asyncio
import sys
import os
import logging

# Add current directory to path to allow imports from core, models, etc.
sys.path.append(os.getcwd())

from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession
from core.config import settings
from ingestion.extractors.api_extractor import APIExtractor
from ingestion.extractors.csv_extractor import CSVExtractor
from ingestion.runner import ETLRunner
from ingestion.extractors.rss_extractor import RSSExtractor

# Configure logging
logging.basicConfig(
    level=getattr(logging, settings.LOG_LEVEL),
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


async def run_etl():
    """Run ETL for all configured sources"""
    
    # Create database engine
    engine = create_async_engine(
        settings.DATABASE_URL,
        echo=False,
    )
    
    AsyncSessionLocal = async_sessionmaker(
        engine,
        class_=AsyncSession,
        expire_on_commit=False,
    )
    
    try:
        async with AsyncSessionLocal() as session:
            runner = ETLRunner(session)
            
            # Configure your data sources here
            sources = [
            APIExtractor(
                db_session=session,
                source_name="coingecko_markets",
                api_url=(
            "https://api.coingecko.com/api/v3/coins/markets"
            "?vs_currency=usd"
            "&order=market_cap_desc"
            "&per_page=10"
            "&page=1"
                )
            ),

            CSVExtractor(
                db_session=session,
                source_name="sample_products",
                file_path="/app/data/sample_products.csv"
            ),
            
            RSSExtractor(
                db_session=session,
                source_name="bbc_news",
                feed_url="http://feeds.bbci.co.uk/news/rss.xml"
            ),
        ]
                
            if not sources:
                logger.warning("No data sources configured. Skipping ETL.")
                return
            
            # Run ETL for each source
            for source in sources:
                try:
                    logger.info(f"Running ETL for source: {source.source_name}")
                    result = await runner.run(source)
                    logger.info(
                        f"ETL completed for {source.source_name}: "
                        f"Extracted={result['records_extracted']}, "
                        f"Loaded={result['records_loaded']}"
                    )
                except Exception as e:
                    logger.error(f"ETL failed for {source.source_name}: {str(e)}")
                    continue
            
            logger.info("All ETL jobs completed")
            
    except Exception as e:
        logger.error(f"ETL pipeline error: {str(e)}")
        sys.exit(1)
    finally:
        await engine.dispose()


if __name__ == "__main__":
    asyncio.run(run_etl())