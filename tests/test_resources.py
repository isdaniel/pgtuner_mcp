"""Tests for resource handlers and JSON serialization.

These tests ensure that database query results with Decimal, datetime,
and other non-JSON-serializable types are properly handled.
"""

import json
from decimal import Decimal
from datetime import datetime, timedelta
from unittest.mock import AsyncMock, patch

import pytest


class TestHealthResourceJsonSerialization:
    """Tests for _get_health_resource JSON serialization.

    These tests verify that database results containing Decimal types
    are properly serialized to JSON without errors.
    """

    @pytest.mark.asyncio
    async def test_connections_check_with_decimal_values(self):
        """Test that connections check handles Decimal values from database."""
        from pgtuner_mcp.server import _get_health_resource

        # Mock the SQL driver
        mock_driver = AsyncMock()

        # Simulate database returning Decimal types (common with COUNT aggregates)
        mock_driver.execute_query = AsyncMock(return_value=[{
            "total_connections": Decimal("15"),
            "active": Decimal("5"),
            "idle": Decimal("8"),
            "idle_in_transaction": Decimal("2"),
            "waiting": Decimal("0"),
            "max_connections": Decimal("100"),
        }])

        with patch('pgtuner_mcp.server.get_sql_driver', return_value=mock_driver):

            result = await _get_health_resource("connections")

            # Should not raise "Object of type Decimal is not JSON serializable"
            # Verify it's valid JSON
            parsed = json.loads(result)

            assert parsed["check_type"] == "connections"
            assert "max_connections" in parsed
            assert "current_connections" in parsed
            assert "usage_percentage" in parsed
            assert "breakdown" in parsed
            assert "status" in parsed

    @pytest.mark.asyncio
    async def test_cache_check_with_decimal_values(self):
        """Test that cache check handles Decimal values from SUM aggregates."""
        from pgtuner_mcp.server import _get_health_resource

        mock_driver = AsyncMock()

        # Simulate database returning Decimal types from SUM aggregates
        mock_driver.execute_query = AsyncMock(return_value=[{
            "heap_read": Decimal("1000"),
            "heap_hit": Decimal("99000"),
            "idx_read": Decimal("500"),
            "idx_hit": Decimal("49500"),
        }])

        with patch('pgtuner_mcp.server.get_sql_driver', return_value=mock_driver):

            result = await _get_health_resource("cache")

            # Should not raise serialization error
            parsed = json.loads(result)

            assert parsed["check_type"] == "cache"
            assert "table_cache_hit_ratio" in parsed
            assert "index_cache_hit_ratio" in parsed
            assert "status" in parsed

    @pytest.mark.asyncio
    async def test_cache_check_with_none_values(self):
        """Test that cache check handles None values (empty tables)."""
        from pgtuner_mcp.server import _get_health_resource

        mock_driver = AsyncMock()

        # Simulate database returning None (no tables yet)
        mock_driver.execute_query = AsyncMock(return_value=[{
            "heap_read": None,
            "heap_hit": None,
            "idx_read": None,
            "idx_hit": None,
        }])

        with patch('pgtuner_mcp.server.get_sql_driver', return_value=mock_driver):

            result = await _get_health_resource("cache")

            parsed = json.loads(result)

            assert parsed["check_type"] == "cache"
            # Should handle division by zero gracefully
            assert parsed["table_cache_hit_ratio"] == 0
            assert parsed["index_cache_hit_ratio"] == 0

    @pytest.mark.asyncio
    async def test_locks_check_with_decimal_values(self):
        """Test that locks check handles Decimal values from COUNT aggregates."""
        from pgtuner_mcp.server import _get_health_resource

        mock_driver = AsyncMock()

        # Simulate database returning Decimal types
        mock_driver.execute_query = AsyncMock(return_value=[{
            "total_locks": Decimal("25"),
            "waiting_locks": Decimal("3"),
            "exclusive_locks": Decimal("5"),
        }])

        with patch('pgtuner_mcp.server.get_sql_driver', return_value=mock_driver):

            result = await _get_health_resource("locks")

            parsed = json.loads(result)

            assert parsed["check_type"] == "locks"
            assert "total_locks" in parsed
            assert "waiting_locks" in parsed
            assert "exclusive_locks" in parsed
            assert "status" in parsed

    @pytest.mark.asyncio
    async def test_bloat_check_with_decimal_values(self):
        """Test that bloat check handles Decimal values from database."""
        from pgtuner_mcp.server import _get_health_resource

        mock_driver = AsyncMock()

        # Simulate database returning Decimal types
        mock_driver.execute_query = AsyncMock(return_value=[
            {
                "schemaname": "public",
                "table_name": "users",
                "dead_tuples": Decimal("5000"),
                "live_tuples": Decimal("100000"),
                "dead_tuple_ratio": Decimal("5.00"),
            },
            {
                "schemaname": "public",
                "table_name": "orders",
                "dead_tuples": Decimal("2000"),
                "live_tuples": Decimal("50000"),
                "dead_tuple_ratio": Decimal("4.00"),
            },
        ])

        with patch('pgtuner_mcp.server.get_sql_driver', return_value=mock_driver):

            result = await _get_health_resource("bloat")

            parsed = json.loads(result)

            assert parsed["check_type"] == "bloat"
            assert parsed["bloated_table_count"] == 2
            assert len(parsed["tables"]) == 2
            assert parsed["tables"][0]["table"] == "users"

    @pytest.mark.asyncio
    async def test_replication_check_with_decimal_lag(self):
        """Test that replication check handles Decimal lag values."""
        from pgtuner_mcp.server import _get_health_resource

        mock_driver = AsyncMock()

        # Simulate database returning Decimal and special types
        mock_driver.execute_query = AsyncMock(return_value=[
            {
                "client_addr": "192.168.1.100",
                "state": "streaming",
                "sent_lsn": "0/3000000",
                "write_lsn": "0/3000000",
                "flush_lsn": "0/3000000",
                "replay_lsn": "0/2F00000",
                "replication_lag_bytes": Decimal("1048576"),
            },
        ])

        with patch('pgtuner_mcp.server.get_sql_driver', return_value=mock_driver):

            result = await _get_health_resource("replication")

            parsed = json.loads(result)

            assert parsed["check_type"] == "replication"
            assert parsed["replica_count"] == 1
            assert len(parsed["replicas"]) == 1

    @pytest.mark.asyncio
    async def test_all_check_aggregates_properly(self):
        """Test that 'all' check type properly aggregates all checks."""
        from pgtuner_mcp.server import _get_health_resource

        mock_driver = AsyncMock()

        # Create different return values for different queries
        call_count = 0
        async def mock_execute_query(*args, **kwargs):
            nonlocal call_count
            call_count += 1

            if call_count == 1:  # connections
                return [{
                    "total_connections": Decimal("10"),
                    "active": Decimal("5"),
                    "idle": Decimal("5"),
                    "idle_in_transaction": Decimal("0"),
                    "waiting": Decimal("0"),
                    "max_connections": Decimal("100"),
                }]
            elif call_count == 2:  # cache
                return [{
                    "heap_read": Decimal("1000"),
                    "heap_hit": Decimal("99000"),
                    "idx_read": Decimal("500"),
                    "idx_hit": Decimal("49500"),
                }]
            elif call_count == 3:  # locks
                return [{
                    "total_locks": Decimal("10"),
                    "waiting_locks": Decimal("0"),
                    "exclusive_locks": Decimal("2"),
                }]
            else:  # bloat
                return []

        mock_driver.execute_query = mock_execute_query

        with patch('pgtuner_mcp.server.get_sql_driver', return_value=mock_driver):

            result = await _get_health_resource("all")

            parsed = json.loads(result)

            assert parsed["check_type"] == "all"
            assert "overall_status" in parsed
            assert "checks" in parsed
            assert "connections" in parsed["checks"]
            assert "cache" in parsed["checks"]
            assert "locks" in parsed["checks"]
            assert "bloat" in parsed["checks"]

    @pytest.mark.asyncio
    async def test_unknown_check_type_returns_error(self):
        """Test that unknown check type returns proper error."""
        from pgtuner_mcp.server import _get_health_resource

        mock_driver = AsyncMock()

        with patch('pgtuner_mcp.server.get_sql_driver', return_value=mock_driver):
            result = await _get_health_resource("unknown_type")

            parsed = json.loads(result)

            assert "error" in parsed
            assert "unknown_type" in parsed["error"].lower()
            assert "valid_types" in parsed


class TestTableStatsResourceJsonSerialization:
    """Tests for _get_table_stats_resource JSON serialization."""

    @pytest.mark.asyncio
    async def test_table_stats_with_decimal_values(self):
        """Test that table stats handles Decimal values."""
        from pgtuner_mcp.server import _get_table_stats_resource

        mock_driver = AsyncMock()

        # First call: table stats
        # Second call: size info
        mock_driver.execute_query = AsyncMock(side_effect=[
            [{
                "schemaname": "public",
                "table_name": "users",
                "live_rows": Decimal("100000"),
                "dead_rows": Decimal("5000"),
                "modifications_since_analyze": Decimal("1000"),
                "last_vacuum": datetime.now() - timedelta(days=1),
                "last_autovacuum": datetime.now() - timedelta(hours=6),
                "last_analyze": datetime.now() - timedelta(days=2),
                "last_autoanalyze": datetime.now() - timedelta(hours=12),
                "vacuum_count": Decimal("10"),
                "autovacuum_count": Decimal("50"),
                "analyze_count": Decimal("5"),
                "autoanalyze_count": Decimal("25"),
                "seq_scan": Decimal("1000"),
                "seq_tup_read": Decimal("10000000"),
                "idx_scan": Decimal("50000"),
                "idx_tup_fetch": Decimal("500000"),
                "inserts": Decimal("10000"),
                "updates": Decimal("5000"),
                "deletes": Decimal("1000"),
                "hot_updates": Decimal("2500"),
            }],
            [{
                "total_size": "1 GB",
                "table_size": "800 MB",
                "indexes_size": "200 MB",
            }],
        ])

        with patch('pgtuner_mcp.server.get_sql_driver', return_value=mock_driver):

            result = await _get_table_stats_resource("public", "users")

            # Should not raise serialization error
            parsed = json.loads(result)

            assert parsed["schema"] == "public"
            assert parsed["table_name"] == "users"
            assert "row_counts" in parsed
            assert "size" in parsed
            assert "maintenance" in parsed
            assert "access_patterns" in parsed
            assert "modifications" in parsed

    @pytest.mark.asyncio
    async def test_table_stats_not_found(self):
        """Test that missing table returns proper error."""
        from pgtuner_mcp.server import _get_table_stats_resource

        mock_driver = AsyncMock()
        mock_driver.execute_query = AsyncMock(return_value=[])

        with patch('pgtuner_mcp.server.get_sql_driver', return_value=mock_driver):

            result = await _get_table_stats_resource("public", "nonexistent")

            parsed = json.loads(result)

            assert "error" in parsed
            assert "nonexistent" in parsed["error"]


class TestQueryStatsResourceJsonSerialization:
    """Tests for _get_query_stats_resource JSON serialization."""

    @pytest.mark.asyncio
    async def test_query_stats_with_decimal_values(self):
        """Test that query stats handles Decimal values from pg_stat_statements."""
        from pgtuner_mcp.server import _get_query_stats_resource

        mock_driver = AsyncMock()

        mock_driver.execute_query = AsyncMock(return_value=[{
            "queryid": 12345678901234567,
            "query": "SELECT * FROM users WHERE id = $1",
            "calls": Decimal("10000"),
            "total_exec_time": Decimal("5000.50"),
            "mean_exec_time": Decimal("0.50"),
            "min_exec_time": Decimal("0.01"),
            "max_exec_time": Decimal("100.00"),
            "stddev_exec_time": Decimal("5.25"),
            "rows": Decimal("10000"),
            "shared_blks_hit": Decimal("50000"),
            "shared_blks_read": Decimal("1000"),
            "shared_blks_dirtied": Decimal("100"),
            "shared_blks_written": Decimal("50"),
            "local_blks_hit": Decimal("0"),
            "local_blks_read": Decimal("0"),
            "temp_blks_read": Decimal("0"),
            "temp_blks_written": Decimal("0"),
        }])

        with patch('pgtuner_mcp.server.get_sql_driver', return_value=mock_driver):

            result = await _get_query_stats_resource("12345678901234567")

            # Should not raise serialization error
            parsed = json.loads(result)

            assert "query_id" in parsed
            assert "query" in parsed
            assert "execution" in parsed
            assert "buffer_usage" in parsed
            assert "cache_hit_ratio" in parsed["buffer_usage"]


class TestSettingsResourceJsonSerialization:
    """Tests for _get_settings_resource JSON serialization."""

    @pytest.mark.asyncio
    async def test_settings_with_various_types(self):
        """Test that settings resource handles various value types."""
        from pgtuner_mcp.server import _get_settings_resource

        mock_driver = AsyncMock()

        mock_driver.execute_query = AsyncMock(return_value=[
            {
                "name": "shared_buffers",
                "setting": "128MB",
                "unit": "8kB",
                "context": "postmaster",
                "short_desc": "Sets the number of shared memory buffers",
                "boot_val": "128MB",
                "reset_val": "128MB",
            },
            {
                "name": "work_mem",
                "setting": "4MB",
                "unit": "kB",
                "context": "user",
                "short_desc": "Sets the maximum memory for query workspaces",
                "boot_val": "4MB",
                "reset_val": "4MB",
            },
        ])

        with patch('pgtuner_mcp.server.get_sql_driver', return_value=mock_driver):

            result = await _get_settings_resource("memory")

            parsed = json.loads(result)

            assert parsed["category"] == "memory"
            assert "setting_count" in parsed
            assert "settings" in parsed

    @pytest.mark.asyncio
    async def test_settings_unknown_category(self):
        """Test that unknown category returns proper error."""
        from pgtuner_mcp.server import _get_settings_resource

        mock_driver = AsyncMock()

        with patch('pgtuner_mcp.server.get_sql_driver', return_value=mock_driver):
            result = await _get_settings_resource("invalid_category")

            parsed = json.loads(result)

            assert "error" in parsed
            assert "invalid_category" in parsed["error"]
            assert "valid_categories" in parsed


class TestJsonSerializerHelper:
    """Tests to verify json.dumps with default=str works for various types."""

    def test_decimal_serialization(self):
        """Test that Decimal values are serialized properly."""
        data = {
            "count": Decimal("100"),
            "ratio": Decimal("99.95"),
            "negative": Decimal("-50.5"),
        }

        result = json.dumps(data, default=str)
        parsed = json.loads(result)

        assert parsed["count"] == "100"
        assert parsed["ratio"] == "99.95"
        assert parsed["negative"] == "-50.5"

    def test_datetime_serialization(self):
        """Test that datetime values are serialized properly."""
        now = datetime.now()
        data = {
            "timestamp": now,
            "date": now.date(),
        }

        result = json.dumps(data, default=str)
        parsed = json.loads(result)

        assert str(now) in parsed["timestamp"]

    def test_mixed_types_serialization(self):
        """Test that mixed types including None are handled."""
        data = {
            "string": "test",
            "int": 42,
            "float": 3.14,
            "decimal": Decimal("99.99"),
            "none": None,
            "list": [1, Decimal("2"), "three"],
            "nested": {
                "decimal": Decimal("123.456"),
            },
        }

        result = json.dumps(data, default=str)
        parsed = json.loads(result)

        assert parsed["string"] == "test"
        assert parsed["int"] == 42
        assert parsed["float"] == 3.14
        assert parsed["decimal"] == "99.99"
        assert parsed["none"] is None
        assert parsed["nested"]["decimal"] == "123.456"
