"""Pytest configuration and fixtures."""

import asyncio
from unittest.mock import AsyncMock, MagicMock

import pytest


@pytest.fixture(scope="session")
def event_loop():
    """Create an event loop for the test session."""
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()


@pytest.fixture
def mock_sql_driver():
    """Create a mock SQL driver for testing."""
    driver = AsyncMock()
    driver.execute_query = AsyncMock(return_value=[])
    return driver


@pytest.fixture
def mock_hypopg_service():
    """Create a mock HypoPG service for testing."""
    service = AsyncMock()
    service.check_hypopg_available = AsyncMock(return_value=True)
    service.create_hypothetical_index = AsyncMock(return_value={
        "success": True,
        "index_name": "hypo_idx_test",
        "index_oid": 12345,
        "estimated_size": "8192 bytes"
    })
    service.list_hypothetical_indexes = AsyncMock(return_value=[])
    service.drop_hypothetical_index = AsyncMock(return_value={"success": True})
    service.reset_hypothetical_indexes = AsyncMock(return_value={"success": True})
    service.get_index_size = AsyncMock(return_value="8 kB")
    return service


@pytest.fixture
def mock_index_advisor(mock_sql_driver, mock_hypopg_service):
    """Create a mock Index Advisor for testing."""
    advisor = AsyncMock()
    advisor.sql_driver = mock_sql_driver
    advisor.hypopg_service = mock_hypopg_service
    advisor.analyze_workload = AsyncMock(return_value=[])
    advisor.analyze_query = AsyncMock(return_value=[])
    return advisor


@pytest.fixture
def mock_db_pool():
    """Create a mock database connection pool."""
    pool = AsyncMock()
    pool.connect = AsyncMock()
    pool.close = AsyncMock()
    pool.get_pool = MagicMock(return_value=AsyncMock())
    return pool
