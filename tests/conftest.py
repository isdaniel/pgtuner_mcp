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
    from pgtuner_mcp.services.hypopg_service import HypoPGStatus, HypotheticalIndex

    service = AsyncMock()
    # check_status returns HypoPGStatus
    service.check_status = AsyncMock(return_value=HypoPGStatus(
        is_installed=True,
        is_available=True,
        version="1.3.1",
        message="HypoPG extension is installed and ready."
    ))
    # create_index returns HypotheticalIndex
    service.create_index = AsyncMock(return_value=HypotheticalIndex(
        indexrelid=12345,
        index_name="hypo_idx_test",
        table_name="users",
        schema_name="public",
        am_name="btree",
        definition="CREATE INDEX ON public.users USING btree (email)",
        estimated_size=8192
    ))
    # list_indexes returns list of HypotheticalIndex
    service.list_indexes = AsyncMock(return_value=[])
    service.drop_index = AsyncMock(return_value=True)
    service.reset = AsyncMock(return_value=True)
    service.get_index_size = AsyncMock(return_value=8192)
    return service


@pytest.fixture
def mock_index_advisor(mock_sql_driver, mock_hypopg_service):
    """Create a mock Index Advisor for testing."""
    from pgtuner_mcp.services.index_advisor import WorkloadAnalysisResult, IndexRecommendation

    advisor = AsyncMock()
    advisor.driver = mock_sql_driver
    advisor.hypopg = mock_hypopg_service
    # analyze_workload returns WorkloadAnalysisResult
    advisor.analyze_workload = AsyncMock(return_value=WorkloadAnalysisResult(
        recommendations=[],
        analyzed_queries=0,
        total_improvement=None,
        error=None
    ))
    # analyze_queries returns WorkloadAnalysisResult
    advisor.analyze_queries = AsyncMock(return_value=WorkloadAnalysisResult(
        recommendations=[],
        analyzed_queries=0,
        total_improvement=None,
        error=None
    ))
    # analyze_query returns WorkloadAnalysisResult
    advisor.analyze_query = AsyncMock(return_value=WorkloadAnalysisResult(
        recommendations=[],
        analyzed_queries=1,
        total_improvement=None,
        error=None
    ))
    return advisor


@pytest.fixture
def mock_db_pool():
    """Create a mock database connection pool."""
    pool = AsyncMock()
    pool.connect = AsyncMock()
    pool.close = AsyncMock()
    pool.get_pool = MagicMock(return_value=AsyncMock())
    return pool
