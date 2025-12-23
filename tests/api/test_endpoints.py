"""
API endpoint tests
"""

import pytest
from fastapi.testclient import TestClient
from api.main import app
from api.dependencies import get_db
from models.normalized_data import UnifiedItem
from models.checkpoint import ETLCheckpoint
from models.raw_data import SourceType
from models.etl_run import ETLStatus
from datetime import datetime


@pytest.fixture
def client(db_session):
    """Create test client with database override"""
    
    async def override_get_db():
        yield db_session
    
    app.dependency_overrides[get_db] = override_get_db
    
    with TestClient(app) as test_client:
        yield test_client
    
    app.dependency_overrides.clear()


def test_health_endpoint_database_connected(client, db_session):
    """Test health endpoint returns database status"""
    response = client.get("/health")
    
    assert response.status_code == 200
    data = response.json()
    
    assert "status" in data
    assert "database" in data
    assert "etl_status" in data
    assert "request_id" in data
    assert data["database"]["connected"] is True


@pytest.mark.asyncio
async def test_data_endpoint_pagination(client, db_session):
    """Test data endpoint returns paginated results"""
    # Insert test data
    item1 = UnifiedItem(
        source_type=SourceType.API,
        source_name="test",
        external_id="test_001",
        title="Test Item 1",
        category="electronics",
        amount=99.99
    )
    item2 = UnifiedItem(
        source_type=SourceType.API,
        source_name="test",
        external_id="test_002",
        title="Test Item 2",
        category="books",
        amount=19.99
    )
    
    db_session.add_all([item1, item2])
    await db_session.commit()
    
    # Test pagination
    response = client.get("/data?page=1&page_size=1")
    
    assert response.status_code == 200
    data = response.json()
    
    assert len(data["data"]) == 1
    assert data["pagination"]["total_items"] == 2
    assert data["pagination"]["total_pages"] == 2
    assert data["pagination"]["has_next"] is True
