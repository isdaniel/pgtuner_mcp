"""Tests for tool handlers."""

from unittest.mock import AsyncMock

import pytest

from pgtuner_mcp.tools.tools_health import (
    ActiveQueriesToolHandler,
    DatabaseHealthToolHandler,
)
from pgtuner_mcp.tools.tools_index import (
    HypoPGToolHandler,
    IndexAdvisorToolHandler,
    UnusedIndexesToolHandler,
)
from pgtuner_mcp.tools.tools_performance import (
    AnalyzeQueryToolHandler,
    GetSlowQueriesToolHandler,
)


class TestToolHandlerBase:
    """Tests for the base ToolHandler class."""

    def test_validate_required_args_success(self, mock_sql_driver):
        """Test that validation passes with all required args."""
        handler = GetSlowQueriesToolHandler(mock_sql_driver)
        # Should not raise
        handler.validate_required_args({"query": "SELECT 1"}, ["query"])

    def test_validate_required_args_missing(self, mock_sql_driver):
        """Test that validation fails with missing required args."""
        handler = GetSlowQueriesToolHandler(mock_sql_driver)
        with pytest.raises(ValueError) as exc_info:
            handler.validate_required_args({}, ["query"])
        assert "Missing required arguments" in str(exc_info.value)

    def test_format_result(self, mock_sql_driver):
        """Test that format_result returns proper TextContent."""
        handler = GetSlowQueriesToolHandler(mock_sql_driver)
        result = handler.format_result("Test message")
        assert len(result) == 1
        assert result[0].type == "text"
        assert result[0].text == "Test message"

    def test_format_error(self, mock_sql_driver):
        """Test that format_error returns proper error message."""
        handler = GetSlowQueriesToolHandler(mock_sql_driver)
        result = handler.format_error(Exception("Test error"))
        assert len(result) == 1
        assert "Error: Test error" in result[0].text

    def test_format_json_result(self, mock_sql_driver):
        """Test that format_json_result returns proper JSON."""
        handler = GetSlowQueriesToolHandler(mock_sql_driver)
        result = handler.format_json_result({"key": "value"})
        assert len(result) == 1
        assert '"key": "value"' in result[0].text


class TestGetSlowQueriesToolHandler:
    """Tests for GetSlowQueriesToolHandler."""

    def test_tool_definition(self, mock_sql_driver):
        """Test that tool definition is properly formed."""
        handler = GetSlowQueriesToolHandler(mock_sql_driver)
        tool_def = handler.get_tool_definition()

        assert tool_def.name == "get_slow_queries"
        assert "slow queries" in tool_def.description.lower()
        assert "properties" in tool_def.inputSchema
        assert "limit" in tool_def.inputSchema["properties"]

    @pytest.mark.asyncio
    async def test_run_tool_no_extension(self, mock_sql_driver):
        """Test behavior when pg_stat_statements is not available."""
        mock_sql_driver.execute_query = AsyncMock(
            return_value=[{"available": False}]
        )

        handler = GetSlowQueriesToolHandler(mock_sql_driver)
        result = await handler.run_tool({})

        assert len(result) == 1
        assert "pg_stat_statements" in result[0].text
        assert "not installed" in result[0].text.lower()

    @pytest.mark.asyncio
    async def test_run_tool_with_results(self, mock_sql_driver):
        """Test behavior with slow queries returned."""
        mock_sql_driver.execute_query = AsyncMock(side_effect=[
            [{"available": True}],  # Extension check
            [  # Slow queries
                {
                    "queryid": 123,
                    "query_text": "SELECT * FROM users",
                    "total_time_ms": 1000.0,
                    "calls": 100,
                    "mean_time_ms": 10.0,
                    "rows": 1000,
                }
            ]
        ])

        handler = GetSlowQueriesToolHandler(mock_sql_driver)
        result = await handler.run_tool({"limit": 5})

        assert len(result) == 1
        assert "slow_queries" in result[0].text


class TestAnalyzeQueryToolHandler:
    """Tests for AnalyzeQueryToolHandler."""

    def test_tool_definition(self, mock_sql_driver):
        """Test that tool definition is properly formed."""
        handler = AnalyzeQueryToolHandler(mock_sql_driver)
        tool_def = handler.get_tool_definition()

        assert tool_def.name == "analyze_query"
        assert "query" in tool_def.inputSchema["required"]

    @pytest.mark.asyncio
    async def test_run_tool_missing_query(self, mock_sql_driver):
        """Test behavior when query is missing."""
        handler = AnalyzeQueryToolHandler(mock_sql_driver)
        result = await handler.run_tool({})

        assert "Error" in result[0].text
        assert "Missing required arguments" in result[0].text

    @pytest.mark.asyncio
    async def test_run_tool_with_query(self, mock_sql_driver):
        """Test EXPLAIN with a query."""
        mock_sql_driver.execute_query = AsyncMock(return_value=[
            {"QUERY PLAN": [{"Plan": {"Node Type": "Seq Scan", "Total Cost": 100}}]}
        ])

        handler = AnalyzeQueryToolHandler(mock_sql_driver)
        result = await handler.run_tool({
            "query": "SELECT * FROM users",
            "analyze": False
        })

        assert len(result) == 1
        assert "execution_plan" in result[0].text or "Seq Scan" in result[0].text


class TestHypoPGToolHandler:
    """Tests for HypoPGToolHandler."""

    def test_tool_definition(self, mock_hypopg_service):
        """Test that tool definition is properly formed."""
        handler = HypoPGToolHandler(mock_hypopg_service)
        tool_def = handler.get_tool_definition()

        assert tool_def.name == "manage_hypothetical_indexes"
        assert "action" in tool_def.inputSchema["required"]
        assert "properties" in tool_def.inputSchema
        assert "action" in tool_def.inputSchema["properties"]

    @pytest.mark.asyncio
    async def test_check_action(self, mock_hypopg_service):
        """Test the check action."""
        handler = HypoPGToolHandler(mock_hypopg_service)
        result = await handler.run_tool({"action": "check"})

        assert "hypopg_available" in result[0].text
        mock_hypopg_service.check_hypopg_available.assert_called_once()

    @pytest.mark.asyncio
    async def test_create_action(self, mock_hypopg_service):
        """Test the create action."""
        handler = HypoPGToolHandler(mock_hypopg_service)
        result = await handler.run_tool({
            "action": "create",
            "table": "users",
            "columns": ["email"]
        })

        assert "success" in result[0].text.lower() or "index_name" in result[0].text
        mock_hypopg_service.create_hypothetical_index.assert_called_once()

    @pytest.mark.asyncio
    async def test_create_action_missing_args(self, mock_hypopg_service):
        """Test create action with missing arguments."""
        handler = HypoPGToolHandler(mock_hypopg_service)
        result = await handler.run_tool({"action": "create"})

        assert "Error" in result[0].text
        assert "Missing required arguments" in result[0].text

    @pytest.mark.asyncio
    async def test_list_action(self, mock_hypopg_service):
        """Test the list action."""
        mock_hypopg_service.list_hypothetical_indexes = AsyncMock(return_value=[
            {"index_name": "hypo_idx_1", "table": "users"}
        ])

        handler = HypoPGToolHandler(mock_hypopg_service)
        result = await handler.run_tool({"action": "list"})

        assert "hypothetical_indexes" in result[0].text
        mock_hypopg_service.list_hypothetical_indexes.assert_called_once()

    @pytest.mark.asyncio
    async def test_reset_action(self, mock_hypopg_service):
        """Test the reset action."""
        handler = HypoPGToolHandler(mock_hypopg_service)
        await handler.run_tool({"action": "reset"})

        mock_hypopg_service.reset_hypothetical_indexes.assert_called_once()


class TestIndexAdvisorToolHandler:
    """Tests for IndexAdvisorToolHandler."""

    def test_tool_definition(self, mock_index_advisor):
        """Test that tool definition is properly formed."""
        handler = IndexAdvisorToolHandler(mock_index_advisor)
        tool_def = handler.get_tool_definition()

        assert tool_def.name == "get_index_recommendations"
        assert "AI-powered" in tool_def.description or "index recommendations" in tool_def.description.lower()

    @pytest.mark.asyncio
    async def test_run_tool_from_workload(self, mock_index_advisor):
        """Test getting recommendations from workload analysis."""
        mock_index_advisor.analyze_workload = AsyncMock(return_value=[
            {
                "table": "users",
                "columns": ["email"],
                "index_type": "btree",
                "estimated_improvement_percent": 50,
                "create_statement": "CREATE INDEX idx_users_email ON users(email)"
            }
        ])

        handler = IndexAdvisorToolHandler(mock_index_advisor)
        result = await handler.run_tool({})

        assert "recommendations" in result[0].text
        mock_index_advisor.analyze_workload.assert_called_once()

    @pytest.mark.asyncio
    async def test_run_tool_with_queries(self, mock_index_advisor):
        """Test getting recommendations from specific queries."""
        mock_index_advisor.analyze_query = AsyncMock(return_value=[
            {
                "table": "orders",
                "columns": ["status"],
                "estimated_improvement_percent": 30,
                "create_statement": "CREATE INDEX idx_orders_status ON orders(status)"
            }
        ])

        handler = IndexAdvisorToolHandler(mock_index_advisor)
        result = await handler.run_tool({
            "workload_queries": ["SELECT * FROM orders WHERE status = 'pending'"]
        })

        assert "recommendations" in result[0].text
        mock_index_advisor.analyze_query.assert_called_once()


class TestDatabaseHealthToolHandler:
    """Tests for DatabaseHealthToolHandler."""

    def test_tool_definition(self, mock_sql_driver):
        """Test that tool definition is properly formed."""
        handler = DatabaseHealthToolHandler(mock_sql_driver)
        tool_def = handler.get_tool_definition()

        assert tool_def.name == "check_database_health"
        assert "health" in tool_def.description.lower()

    @pytest.mark.asyncio
    async def test_run_tool(self, mock_sql_driver):
        """Test health check execution."""
        # Mock all the health check queries
        mock_sql_driver.execute_query = AsyncMock(return_value=[{
            "max_conn": 100,
            "used": 10,
            "res_for_super": 3,
            "used_pct": 10.0
        }])

        handler = DatabaseHealthToolHandler(mock_sql_driver)
        result = await handler.run_tool({})

        assert "overall_score" in result[0].text or "checks" in result[0].text


class TestActiveQueriesToolHandler:
    """Tests for ActiveQueriesToolHandler."""

    def test_tool_definition(self, mock_sql_driver):
        """Test that tool definition is properly formed."""
        handler = ActiveQueriesToolHandler(mock_sql_driver)
        tool_def = handler.get_tool_definition()

        assert tool_def.name == "get_active_queries"

    @pytest.mark.asyncio
    async def test_run_tool_empty(self, mock_sql_driver):
        """Test with no active queries."""
        mock_sql_driver.execute_query = AsyncMock(side_effect=[
            [],  # Active queries
            [{"state": "idle", "count": 5}],  # Summary
            []  # Blocked queries
        ])

        handler = ActiveQueriesToolHandler(mock_sql_driver)
        result = await handler.run_tool({})

        assert "summary" in result[0].text or "active_queries" in result[0].text


class TestUnusedIndexesToolHandler:
    """Tests for UnusedIndexesToolHandler."""

    def test_tool_definition(self, mock_sql_driver):
        """Test that tool definition is properly formed."""
        handler = UnusedIndexesToolHandler(mock_sql_driver)
        tool_def = handler.get_tool_definition()

        assert tool_def.name == "find_unused_indexes"
        assert "unused" in tool_def.description.lower()

    @pytest.mark.asyncio
    async def test_run_tool(self, mock_sql_driver):
        """Test finding unused indexes."""
        mock_sql_driver.execute_query = AsyncMock(side_effect=[
            [  # Unused indexes
                {
                    "index_name": "idx_unused",
                    "table_name": "users",
                    "scans": 0,
                    "size": "1 MB"
                }
            ],
            []  # Duplicate indexes
        ])

        handler = UnusedIndexesToolHandler(mock_sql_driver)
        result = await handler.run_tool({})

        assert "unused_indexes" in result[0].text
