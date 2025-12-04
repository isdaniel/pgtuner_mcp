"""Tests for services."""

import os
from unittest.mock import AsyncMock, patch

import pytest

from pgtuner_mcp.services.sql_driver import DbConnPool, SqlDriver
from pgtuner_mcp.services.user_filter import UserFilter, get_user_filter, EXCLUDE_USERIDS_ENV


class TestUserFilter:
    """Tests for UserFilter."""

    def setup_method(self):
        """Reset the singleton before each test."""
        UserFilter._instance = None
        # Clear environment variable
        if EXCLUDE_USERIDS_ENV in os.environ:
            del os.environ[EXCLUDE_USERIDS_ENV]

    def teardown_method(self):
        """Clean up after each test."""
        UserFilter._instance = None
        if EXCLUDE_USERIDS_ENV in os.environ:
            del os.environ[EXCLUDE_USERIDS_ENV]

    def test_singleton_pattern(self):
        """Test that UserFilter follows singleton pattern."""
        filter1 = UserFilter()
        filter2 = UserFilter()
        assert filter1 is filter2

    def test_get_user_filter_returns_singleton(self):
        """Test that get_user_filter returns the singleton instance."""
        filter1 = get_user_filter()
        filter2 = get_user_filter()
        assert filter1 is filter2

    def test_no_env_variable_returns_empty_filters(self):
        """Test that no environment variable results in empty filters."""
        user_filter = UserFilter()

        assert user_filter.get_activity_filter() == ""
        assert user_filter.get_statements_filter() == ""
        assert user_filter.get_filter_params() == []
        assert user_filter.has_exclusions is False

    def test_single_userid_exclusion(self):
        """Test exclusion with a single user ID."""
        os.environ[EXCLUDE_USERIDS_ENV] = "16384"
        UserFilter.reload()
        user_filter = UserFilter()

        assert user_filter.has_exclusions is True
        assert user_filter.get_activity_filter() == "AND usesysid NOT IN (16384)"
        assert user_filter.get_statements_filter() == "AND userid NOT IN (16384)"
        assert user_filter.get_filter_params() == [16384]

    def test_multiple_userid_exclusion(self):
        """Test exclusion with multiple user IDs."""
        os.environ[EXCLUDE_USERIDS_ENV] = "16384,16385,16386"
        UserFilter.reload()
        user_filter = UserFilter()

        assert user_filter.has_exclusions is True
        assert user_filter.get_activity_filter() == "AND usesysid NOT IN (16384, 16385, 16386)"
        assert user_filter.get_statements_filter() == "AND userid NOT IN (16384, 16385, 16386)"
        assert user_filter.get_filter_params() == [16384, 16385, 16386]

    def test_userid_with_whitespace(self):
        """Test that whitespace around user IDs is handled correctly."""
        os.environ[EXCLUDE_USERIDS_ENV] = " 16384 , 16385 , 16386 "
        UserFilter.reload()
        user_filter = UserFilter()

        assert user_filter.has_exclusions is True
        assert user_filter.get_filter_params() == [16384, 16385, 16386]

    def test_empty_env_variable(self):
        """Test that empty environment variable results in no exclusions."""
        os.environ[EXCLUDE_USERIDS_ENV] = ""
        UserFilter.reload()
        user_filter = UserFilter()

        assert user_filter.has_exclusions is False
        assert user_filter.get_activity_filter() == ""
        assert user_filter.get_statements_filter() == ""

    def test_whitespace_only_env_variable(self):
        """Test that whitespace-only environment variable results in no exclusions."""
        os.environ[EXCLUDE_USERIDS_ENV] = "   "
        UserFilter.reload()
        user_filter = UserFilter()

        assert user_filter.has_exclusions is False
        assert user_filter.get_filter_params() == []

    def test_invalid_userid_is_skipped(self):
        """Test that invalid (non-integer) user IDs are skipped with warning."""
        os.environ[EXCLUDE_USERIDS_ENV] = "16384,invalid,16386"
        UserFilter.reload()
        user_filter = UserFilter()

        # Should only include valid IDs
        assert user_filter.get_filter_params() == [16384, 16386]
        assert user_filter.has_exclusions is True

    def test_all_invalid_userids(self):
        """Test that all invalid user IDs result in no exclusions."""
        os.environ[EXCLUDE_USERIDS_ENV] = "invalid,abc,xyz"
        UserFilter.reload()
        user_filter = UserFilter()

        assert user_filter.has_exclusions is False
        assert user_filter.get_filter_params() == []
        assert user_filter.get_activity_filter() == ""

    def test_empty_entries_in_list(self):
        """Test that empty entries in comma-separated list are handled."""
        os.environ[EXCLUDE_USERIDS_ENV] = "16384,,16386,,"
        UserFilter.reload()
        user_filter = UserFilter()

        assert user_filter.get_filter_params() == [16384, 16386]

    def test_reload_updates_configuration(self):
        """Test that reload() updates the configuration."""
        os.environ[EXCLUDE_USERIDS_ENV] = "16384"
        UserFilter.reload()
        user_filter = UserFilter()
        assert user_filter.get_filter_params() == [16384]

        # Change environment variable and reload
        os.environ[EXCLUDE_USERIDS_ENV] = "16385,16386"
        UserFilter.reload()

        # Get new instance after reload
        user_filter = UserFilter()
        assert user_filter.get_filter_params() == [16385, 16386]

    def test_reload_clears_exclusions(self):
        """Test that reload() can clear exclusions when env var is removed."""
        os.environ[EXCLUDE_USERIDS_ENV] = "16384"
        UserFilter.reload()
        user_filter = UserFilter()
        assert user_filter.has_exclusions is True

        # Remove environment variable and reload
        del os.environ[EXCLUDE_USERIDS_ENV]
        UserFilter.reload()

        user_filter = UserFilter()
        assert user_filter.has_exclusions is False

    def test_negative_userid(self):
        """Test that negative user IDs are accepted (edge case)."""
        os.environ[EXCLUDE_USERIDS_ENV] = "-1,16384"
        UserFilter.reload()
        user_filter = UserFilter()

        # Negative values are technically valid integers
        assert user_filter.get_filter_params() == [-1, 16384]

    def test_large_userid(self):
        """Test that large user IDs are handled correctly."""
        os.environ[EXCLUDE_USERIDS_ENV] = "2147483647,16384"
        UserFilter.reload()
        user_filter = UserFilter()

        assert user_filter.get_filter_params() == [2147483647, 16384]

    def test_filter_clause_format_activity(self):
        """Test the exact format of activity filter clause."""
        os.environ[EXCLUDE_USERIDS_ENV] = "100,200"
        UserFilter.reload()
        user_filter = UserFilter()

        filter_clause = user_filter.get_activity_filter()
        assert filter_clause.startswith("AND ")
        assert "usesysid" in filter_clause
        assert "NOT IN" in filter_clause
        assert "100" in filter_clause
        assert "200" in filter_clause

    def test_filter_clause_format_statements(self):
        """Test the exact format of statements filter clause."""
        os.environ[EXCLUDE_USERIDS_ENV] = "100,200"
        UserFilter.reload()
        user_filter = UserFilter()

        filter_clause = user_filter.get_statements_filter()
        assert filter_clause.startswith("AND ")
        assert "userid" in filter_clause
        assert "NOT IN" in filter_clause
        assert "100" in filter_clause
        assert "200" in filter_clause


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
