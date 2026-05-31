"""Unit tests for buffer cache + TOAST handlers."""

import json
from unittest.mock import AsyncMock

import pytest

from pgtuner_mcp.tools.tools_buffercache import (
    AnalyzeBufferCacheHandler,
    AnalyzeToastStorageHandler,
)


class TestAnalyzeBufferCacheHandler:
    def test_definition(self, mock_sql_driver):
        handler = AnalyzeBufferCacheHandler(mock_sql_driver)
        tool = handler.get_tool_definition()
        assert tool.name == "analyze_buffer_cache"
        assert tool.inputSchema["properties"]["top_n"]["maximum"] == 100

    @pytest.mark.asyncio
    async def test_top_n_clamps_to_100(self, mock_sql_driver):
        mock_sql_driver.execute_query = AsyncMock(side_effect=[
            [{"1": 1}],          # check_extension_installed (returns non-empty)
            [],                  # buffercache query
            [{"mb": 128}],       # shared_buffers query
        ])
        handler = AnalyzeBufferCacheHandler(mock_sql_driver)
        await handler.run_tool({"top_n": 5000})
        bc_call = mock_sql_driver.execute_query.call_args_list[1]
        assert bc_call.args[1][1] == 100

    @pytest.mark.asyncio
    async def test_extension_absent_returns_remediation(self, mock_sql_driver):
        mock_sql_driver.execute_query = AsyncMock(side_effect=[[]])
        handler = AnalyzeBufferCacheHandler(mock_sql_driver)
        result = await handler.run_tool({})
        assert "pg_buffercache" in result[0].text
        assert "CREATE EXTENSION" in result[0].text

    @pytest.mark.asyncio
    async def test_buffer_cache_dirty_threshold_triggers_recommendation(self, mock_sql_driver):
        mock_sql_driver.execute_query = AsyncMock(side_effect=[
            [{"1": 1}],
            [{"relation": "users", "schema": "public", "kind": "table",
              "pages": 10000, "dirty_pages": 5000, "avg_usagecount": 3.0,
              "mb_cached": 78.12, "pct_of_shared_buffers": 50.0}],
            [{"mb": 128}],
        ])
        handler = AnalyzeBufferCacheHandler(mock_sql_driver)
        result = await handler.run_tool({})
        payload = json.loads(result[0].text)
        assert payload["dirty_summary"]["total_dirty_pages"] == 5000
        assert any("checkpoint" in r for r in payload["recommendations"])

    @pytest.mark.asyncio
    async def test_buffer_cache_error_returns_format_error(self, mock_sql_driver):
        # First call (extension check) succeeds; second call (main query) raises.
        # Note: check_extension_installed swallows exceptions, so we must let it
        # succeed and trigger the error on the subsequent query.
        mock_sql_driver.execute_query = AsyncMock(side_effect=[
            [{"1": 1}],
            Exception("kaboom"),
        ])
        handler = AnalyzeBufferCacheHandler(mock_sql_driver)
        result = await handler.run_tool({})
        assert "Error" in result[0].text
        assert "kaboom" in result[0].text


class TestAnalyzeToastStorageHandler:
    def test_definition(self, mock_sql_driver):
        handler = AnalyzeToastStorageHandler(mock_sql_driver)
        tool = handler.get_tool_definition()
        assert tool.name == "analyze_toast_storage"

    @pytest.mark.asyncio
    async def test_toast_recommends_lz4_on_pg14(self, mock_sql_driver):
        mock_sql_driver.execute_query = AsyncMock(side_effect=[
            [{"server_version": "16.2"}],
            [{"reloid": 1, "table_name": "docs", "schema": "public",
              "toast_oid": 2, "toast_size_mb": 50}],
            [{"name": "body", "type": "text", "storage": "x", "compression": "p"}],
        ])
        handler = AnalyzeToastStorageHandler(mock_sql_driver)
        result = await handler.run_tool({})
        payload = json.loads(result[0].text)
        assert any("SET COMPRESSION lz4" in r for r in payload["recommendations"])

    @pytest.mark.asyncio
    async def test_toast_no_lz4_recommendation_on_pg13(self, mock_sql_driver):
        mock_sql_driver.execute_query = AsyncMock(side_effect=[
            [{"server_version": "13.10"}],
            [{"reloid": 1, "table_name": "docs", "schema": "public",
              "toast_oid": 2, "toast_size_mb": 50}],
            [{"name": "body", "type": "text", "storage": "x", "compression": " "}],
        ])
        handler = AnalyzeToastStorageHandler(mock_sql_driver)
        result = await handler.run_tool({})
        payload = json.loads(result[0].text)
        assert payload["recommendations"] == []

    @pytest.mark.asyncio
    async def test_toast_no_varlena_columns_skips_table(self, mock_sql_driver):
        mock_sql_driver.execute_query = AsyncMock(side_effect=[
            [{"server_version": "16.2"}],
            [{"reloid": 1, "table_name": "ints_only", "schema": "public",
              "toast_oid": 0, "toast_size_mb": 0}],
            [],  # no varlena columns
        ])
        handler = AnalyzeToastStorageHandler(mock_sql_driver)
        result = await handler.run_tool({})
        payload = json.loads(result[0].text)
        assert payload["tables"] == []  # skipped because col_payload empty

    @pytest.mark.asyncio
    async def test_toast_recommends_lz4_for_default_compression(self, mock_sql_driver):
        """PG14+ default attcompression marker (' ') means PGLZ — recommend LZ4."""
        mock_sql_driver.execute_query = AsyncMock(side_effect=[
            [{"server_version": "16.2"}],
            [{"reloid": 1, "table_name": "docs", "schema": "public",
              "toast_oid": 2, "toast_size_mb": 50}],
            [{"name": "body", "type": "text", "storage": "x", "compression": " "}],
        ])
        handler = AnalyzeToastStorageHandler(mock_sql_driver)
        result = await handler.run_tool({})
        payload = json.loads(result[0].text)
        assert any("SET COMPRESSION lz4" in r for r in payload["recommendations"])

    @pytest.mark.asyncio
    async def test_toast_no_recommendation_when_already_lz4(self, mock_sql_driver):
        """Columns already using LZ4 should NOT be flagged."""
        mock_sql_driver.execute_query = AsyncMock(side_effect=[
            [{"server_version": "16.2"}],
            [{"reloid": 1, "table_name": "docs", "schema": "public",
              "toast_oid": 2, "toast_size_mb": 50}],
            [{"name": "body", "type": "text", "storage": "x", "compression": "l"}],
        ])
        handler = AnalyzeToastStorageHandler(mock_sql_driver)
        result = await handler.run_tool({})
        payload = json.loads(result[0].text)
        assert payload["recommendations"] == []
