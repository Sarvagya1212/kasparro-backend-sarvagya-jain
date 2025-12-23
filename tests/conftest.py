"""
Pytest configuration and fixtures
"""

import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine, async_sessionmaker
from sqlalchemy.pool import NullPool
from models.base import Base
from core.config import settings
import asyncio
from typing import AsyncGenerator

# Test database URL
TEST_DATABASE_URL = "postgresql+asyncpg://etl_user:etl_password@localhost:5432/etl_test_db"


@pytest.fixture(scope="session")
def event_loop():
    """Create event loop for async tests"""
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()


@pytest_asyncio.fixture(scope="function")
async def test_engine():
    """Create test database engine"""
    engine = create_async_engine(
        TEST_DATABASE_URL,
        echo=False,
        poolclass=NullPool,  # Disable connection pooling for tests
    )
    
    # Create all tables
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    
    yield engine
    
    # Drop all tables after test
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
    
    await engine.dispose()


@pytest_asyncio.fixture(scope="function")
async def db_session(test_engine) -> AsyncGenerator[AsyncSession, None]:
    """Create database session for tests"""
    async_session_maker = async_sessionmaker(
        test_engine,
        class_=AsyncSession,
        expire_on_commit=False,
        autocommit=False,
        autoflush=False,
    )
    
    async with async_session_maker() as session:
        yield session
        await session.rollback()


@pytest.fixture
def mock_api_data():
    """Mock API response data"""
    return [
        {
            "id": "api_001",
            "name": "Test Product 1",
            "description": "This is a test product",
            "category": "electronics",
            "price": 99.99,
            "created_at": "2024-01-15T10:00:00Z",
            "tags": ["new", "featured"]
        },
        {
            "id": "api_002",
            "name": "Test Product 2",
            "description": "Another test product",
            "category": "books",
            "price": 19.99,
            "created_at": "2024-01-15T11:00:00Z",
            "tags": ["bestseller"]
        }
    ]


@pytest.fixture
def mock_csv_data():
    """Mock CSV data"""
    return [
        {
            "id": "csv_001",
            "product_name": "CSV Product 1",
            "details": "CSV product description",
            "category": "home",
            "cost": 49.99,
            "date": "2024-01-15T12:00:00Z",
            "keywords": "home, furniture"
        },
        {
            "id": "csv_002",
            "product_name": "CSV Product 2",
            "details": "Another CSV product",
            "category": "garden",
            "cost": 29.99,
            "date": "2024-01-15T13:00:00Z",
            "keywords": "outdoor, plants"
        }
    ]
