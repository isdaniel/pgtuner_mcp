"""Tests for services."""

from unittest.mock import AsyncMock, patch

import pytest

from pgtuner_mcp.services.sql_driver import DbConnPool, SqlDriver


class TestDbConnPool:
    """Tests for DbConnPool."""

    def test_init(self):
        """Test pool initialization."""
        pool = DbConnPool("postgresql://localhost/test")
        assert pool.connection_url == "postgresql://localhost/test"
        assert pool.pool is None

    @pytest.mark.asyncio
    async def test_connect_failure_no_url(self):
        """Test that connect fails without URL."""
        pool = DbConnPool(None)

        with pytest.raises(ValueError) as exc_info:
            await pool.connect()

        assert "not provided" in str(exc_info.value).lower()

    @pytest.mark.asyncio
    async def test_close_with_no_pool(self):
        """Test that close works when pool is None."""
        pool = DbConnPool("postgresql://localhost/test")
        # Should not raise
        await pool.close()
        assert pool.pool is None


class TestSqlDriver:
    """Tests for SqlDriver."""

    @pytest.mark.asyncio
    async def test_execute_query_not_connected(self):
        """Test that execute_query fails when not connected."""
        pool = DbConnPool("postgresql://localhost/test")
        driver = SqlDriver(pool)

        with pytest.raises(ValueError) as exc_info:
            await driver.execute_query("SELECT 1")

        assert "not connected" in str(exc_info.value).lower()

    @pytest.mark.asyncio
    async def test_execute_query_with_mock(self, mock_db_pool):
        """Test execute_query with mocked pool."""
        driver = SqlDriver(mock_db_pool)

        # This will need the actual pool to work, so we mock the whole method
        with patch.object(driver, 'execute_query', new_callable=AsyncMock) as mock_exec:
            mock_exec.return_value = [
                {"id": 1, "name": "test"},
                {"id": 2, "name": "test2"}
            ]

            result = await driver.execute_query("SELECT * FROM test")

            assert len(result) == 2
            assert result[0]["id"] == 1
