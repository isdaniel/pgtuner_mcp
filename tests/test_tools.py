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
        mock_hypopg_service.check_status.assert_called_once()

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
        mock_hypopg_service.create_index.assert_called_once()

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
        from pgtuner_mcp.services.hypopg_service import HypotheticalIndex
        mock_hypopg_service.list_indexes = AsyncMock(return_value=[
            HypotheticalIndex(
                indexrelid=12345,
                index_name="hypo_idx_1",
                table_name="users",
                schema_name="public",
                am_name="btree",
                definition="CREATE INDEX ON public.users USING btree (email)",
                estimated_size=8192
            )
        ])

        handler = HypoPGToolHandler(mock_hypopg_service)
        result = await handler.run_tool({"action": "list"})

        assert "hypothetical_indexes" in result[0].text
        mock_hypopg_service.list_indexes.assert_called_once()

    @pytest.mark.asyncio
    async def test_reset_action(self, mock_hypopg_service):
        """Test the reset action."""
        handler = HypoPGToolHandler(mock_hypopg_service)
        await handler.run_tool({"action": "reset"})

        mock_hypopg_service.reset.assert_called_once()

    @pytest.mark.asyncio
    async def test_hide_action(self, mock_hypopg_service):
        """Test the hide action."""
        mock_hypopg_service.hide_index = AsyncMock(return_value=True)

        handler = HypoPGToolHandler(mock_hypopg_service)
        result = await handler.run_tool({
            "action": "hide",
            "index_id": 12345
        })

        assert "success" in result[0].text.lower()
        assert "hidden" in result[0].text.lower()
        mock_hypopg_service.hide_index.assert_called_once_with(12345)

    @pytest.mark.asyncio
    async def test_hide_action_missing_args(self, mock_hypopg_service):
        """Test hide action with missing arguments."""
        handler = HypoPGToolHandler(mock_hypopg_service)
        result = await handler.run_tool({"action": "hide"})

        assert "Error" in result[0].text
        assert "Missing required arguments" in result[0].text

    @pytest.mark.asyncio
    async def test_unhide_action(self, mock_hypopg_service):
        """Test the unhide action."""
        mock_hypopg_service.unhide_index = AsyncMock(return_value=True)

        handler = HypoPGToolHandler(mock_hypopg_service)
        result = await handler.run_tool({
            "action": "unhide",
            "index_id": 12345
        })

        assert "success" in result[0].text.lower()
        mock_hypopg_service.unhide_index.assert_called_once_with(12345)

    @pytest.mark.asyncio
    async def test_unhide_action_missing_args(self, mock_hypopg_service):
        """Test unhide action with missing arguments."""
        handler = HypoPGToolHandler(mock_hypopg_service)
        result = await handler.run_tool({"action": "unhide"})

        assert "Error" in result[0].text
        assert "Missing required arguments" in result[0].text

    @pytest.mark.asyncio
    async def test_list_hidden_action(self, mock_hypopg_service):
        """Test the list_hidden action."""
        mock_hypopg_service.list_hidden_indexes = AsyncMock(return_value=[
            {"indexrelid": 12345, "index_name": "idx_users_email"}
        ])

        handler = HypoPGToolHandler(mock_hypopg_service)
        result = await handler.run_tool({"action": "list_hidden"})

        assert "hidden_indexes" in result[0].text
        assert "count" in result[0].text
        mock_hypopg_service.list_hidden_indexes.assert_called_once()

    @pytest.mark.asyncio
    async def test_explain_with_index_action(self, mock_hypopg_service):
        """Test the explain_with_index action."""
        mock_hypopg_service.explain_with_hypothetical_index = AsyncMock(return_value={
            "hypothetical_index": {
                "indexrelid": 12345,
                "name": "hypo_idx_test",
                "definition": "CREATE INDEX ON users USING btree (email)",
                "estimated_size": 8192
            },
            "before": {
                "plan": {},
                "total_cost": 100.0
            },
            "after": {
                "plan": {},
                "total_cost": 50.0
            },
            "improvement_percentage": 50.0,
            "would_use_index": True
        })

        handler = HypoPGToolHandler(mock_hypopg_service)
        result = await handler.run_tool({
            "action": "explain_with_index",
            "query": "SELECT * FROM users WHERE email = 'test@example.com'",
            "table": "users",
            "columns": ["email"]
        })

        assert "improvement_percentage" in result[0].text
        mock_hypopg_service.explain_with_hypothetical_index.assert_called_once()

    @pytest.mark.asyncio
    async def test_explain_with_index_action_missing_args(self, mock_hypopg_service):
        """Test explain_with_index action with missing arguments."""
        handler = HypoPGToolHandler(mock_hypopg_service)
        result = await handler.run_tool({
            "action": "explain_with_index",
            "query": "SELECT * FROM users"
            # Missing table and columns
        })

        assert "Error" in result[0].text
        assert "Missing required arguments" in result[0].text

    @pytest.mark.asyncio
    async def test_drop_action(self, mock_hypopg_service):
        """Test the drop action."""
        handler = HypoPGToolHandler(mock_hypopg_service)
        result = await handler.run_tool({
            "action": "drop",
            "index_id": 12345
        })

        assert "success" in result[0].text.lower()
        mock_hypopg_service.drop_index.assert_called_once_with(12345)

    @pytest.mark.asyncio
    async def test_drop_action_missing_args(self, mock_hypopg_service):
        """Test drop action with missing arguments."""
        handler = HypoPGToolHandler(mock_hypopg_service)
        result = await handler.run_tool({"action": "drop"})

        assert "Error" in result[0].text
        assert "Missing required arguments" in result[0].text

    @pytest.mark.asyncio
    async def test_create_action_with_schema_and_options(self, mock_hypopg_service):
        """Test create action with schema, where, and include options."""
        from pgtuner_mcp.services.hypopg_service import HypotheticalIndex
        mock_hypopg_service.create_index = AsyncMock(return_value=HypotheticalIndex(
            indexrelid=12345,
            index_name="hypo_idx_test",
            table_name="users",
            schema_name="myschema",
            am_name="btree",
            definition="CREATE INDEX ON myschema.users USING btree (email) INCLUDE (name) WHERE active = true",
            estimated_size=8192
        ))

        handler = HypoPGToolHandler(mock_hypopg_service)
        result = await handler.run_tool({
            "action": "create",
            "table": "users",
            "columns": ["email"],
            "schema": "myschema",
            "where": "active = true",
            "include": ["name"]
        })

        assert "success" in result[0].text.lower()
        mock_hypopg_service.create_index.assert_called_once_with(
            table="users",
            columns=["email"],
            using="btree",
            schema="myschema",
            where="active = true",
            include=["name"]
        )

    @pytest.mark.asyncio
    async def test_unknown_action(self, mock_hypopg_service):
        """Test unknown action returns error message."""
        handler = HypoPGToolHandler(mock_hypopg_service)
        result = await handler.run_tool({"action": "invalid_action"})

        assert "Unknown action" in result[0].text


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
        from pgtuner_mcp.services.index_advisor import WorkloadAnalysisResult, IndexRecommendation

        mock_index_advisor.analyze_workload = AsyncMock(return_value=WorkloadAnalysisResult(
            recommendations=[
                IndexRecommendation(
                    table="users",
                    columns=["email"],
                    using="btree",
                    estimated_improvement=50.0,
                    reason="Improves query performance",
                    create_statement="CREATE INDEX idx_users_email ON users(email)"
                )
            ],
            analyzed_queries=10,
            total_improvement=50.0,
            error=None
        ))

        handler = IndexAdvisorToolHandler(mock_index_advisor)
        result = await handler.run_tool({})

        assert "recommendations" in result[0].text
        mock_index_advisor.analyze_workload.assert_called_once()

    @pytest.mark.asyncio
    async def test_run_tool_with_queries(self, mock_index_advisor):
        """Test getting recommendations from specific queries."""
        from pgtuner_mcp.services.index_advisor import WorkloadAnalysisResult, IndexRecommendation

        mock_index_advisor.analyze_queries = AsyncMock(return_value=WorkloadAnalysisResult(
            recommendations=[
                IndexRecommendation(
                    table="orders",
                    columns=["status"],
                    using="btree",
                    estimated_improvement=30.0,
                    reason="Improves query performance",
                    create_statement="CREATE INDEX idx_orders_status ON orders(status)"
                )
            ],
            analyzed_queries=1,
            total_improvement=30.0,
            error=None
        ))

        handler = IndexAdvisorToolHandler(mock_index_advisor)
        result = await handler.run_tool({
            "workload_queries": ["SELECT * FROM orders WHERE status = 'pending'"]
        })

        assert "recommendations" in result[0].text
        mock_index_advisor.analyze_queries.assert_called_once()


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
