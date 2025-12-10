"""Tests for tool handlers."""

from unittest.mock import AsyncMock

import pytest

from pgtuner_mcp.tools.tools_bloat import (
    DatabaseBloatSummaryToolHandler,
    IndexBloatToolHandler,
    TableBloatToolHandler,
)
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


class TestTableBloatToolHandler:
    """Tests for TableBloatToolHandler."""

    def test_tool_definition(self, mock_sql_driver):
        """Test that tool definition is properly formed."""
        handler = TableBloatToolHandler(mock_sql_driver)
        tool_def = handler.get_tool_definition()

        assert tool_def.name == "analyze_table_bloat"
        assert "bloat" in tool_def.description.lower() or "pgstattuple" in tool_def.description.lower()
        assert tool_def.inputSchema is not None

    @pytest.mark.asyncio
    async def test_extension_not_installed(self, mock_sql_driver):
        """Test handling when pgstattuple extension is not installed."""
        mock_sql_driver.execute_query = AsyncMock(return_value=[])

        handler = TableBloatToolHandler(mock_sql_driver)
        result = await handler.run_tool({})

        assert "not installed" in result[0].text.lower() or "extension" in result[0].text.lower()

    @pytest.mark.asyncio
    async def test_single_table_exact_mode(self, mock_sql_driver):
        """Test analyzing a single table with exact mode."""
        mock_sql_driver.execute_query = AsyncMock(side_effect=[
            # Extension check - returns available: True
            [{"available": True, "version": "1.5"}],
            # Size query result
            [{"total_size": 10000000, "table_size": 8192000, "indexes_size": 1808000}],
            # pgstattuple result
            [{
                "table_len": 8192000,
                "tuple_count": 10000,
                "tuple_len": 5000000,
                "tuple_percent": 61.0,
                "dead_tuple_count": 500,
                "dead_tuple_len": 250000,
                "dead_tuple_percent": 3.05,
                "free_space": 2000000,
                "free_percent": 24.4
            }]
        ])

        handler = TableBloatToolHandler(mock_sql_driver)
        result = await handler.run_tool({
            "table_name": "users",
            "schema_name": "public",
            "use_approx": False
        })

        result_text = result[0].text
        assert "users" in result_text or "bloat" in result_text.lower()

    @pytest.mark.asyncio
    async def test_single_table_approx_mode(self, mock_sql_driver):
        """Test analyzing a single table with approximate mode."""
        mock_sql_driver.execute_query = AsyncMock(side_effect=[
            # Extension check - returns available: True
            [{"available": True, "version": "1.5"}],
            # Size query result
            [{"total_size": 10000000, "table_size": 8192000, "indexes_size": 1808000}],
            # pgstattuple_approx result
            [{
                "table_len": 8192000,
                "scanned_percent": 100.0,
                "approx_tuple_count": 10000,
                "approx_tuple_len": 5000000,
                "approx_tuple_percent": 61.0,
                "dead_tuple_count": 500,
                "dead_tuple_len": 250000,
                "dead_tuple_percent": 3.05,
                "approx_free_space": 2000000,
                "approx_free_percent": 24.4
            }]
        ])

        handler = TableBloatToolHandler(mock_sql_driver)
        result = await handler.run_tool({
            "table_name": "users",
            "schema_name": "public",
            "use_approx": True
        })

        result_text = result[0].text
        assert "users" in result_text or "bloat" in result_text.lower() or "approx" in result_text.lower()

    @pytest.mark.asyncio
    async def test_schema_wide_analysis(self, mock_sql_driver):
        """Test analyzing all tables in a schema."""
        mock_sql_driver.execute_query = AsyncMock(side_effect=[
            # Extension check - returns available: True
            [{"available": True, "version": "1.5"}],
            # Get tables in schema (returns table_name column)
            [
                {"table_name": "users", "table_size": 10000000},
                {"table_name": "orders", "table_size": 5000000}
            ],
            # Size query for users
            [{"total_size": 10000000, "table_size": 8192000, "indexes_size": 1808000}],
            # pgstattuple_approx for users
            [{
                "table_len": 8192000,
                "scanned_percent": 100.0,
                "approx_tuple_count": 10000,
                "approx_tuple_len": 5000000,
                "approx_tuple_percent": 61.0,
                "dead_tuple_count": 500,
                "dead_tuple_len": 250000,
                "dead_tuple_percent": 3.05,
                "approx_free_space": 2000000,
                "approx_free_percent": 24.4
            }],
            # Size query for orders
            [{"total_size": 5000000, "table_size": 4096000, "indexes_size": 904000}],
            # pgstattuple_approx for orders
            [{
                "table_len": 4096000,
                "scanned_percent": 100.0,
                "approx_tuple_count": 5000,
                "approx_tuple_len": 2500000,
                "approx_tuple_percent": 61.0,
                "dead_tuple_count": 200,
                "dead_tuple_len": 100000,
                "dead_tuple_percent": 2.44,
                "approx_free_space": 1000000,
                "approx_free_percent": 24.4
            }]
        ])

        handler = TableBloatToolHandler(mock_sql_driver)
        result = await handler.run_tool({
            "schema_name": "public",
            "use_approx": True
        })

        result_text = result[0].text
        # Should contain results for schema analysis
        assert "bloat" in result_text.lower() or "tables" in result_text.lower()


class TestIndexBloatToolHandler:
    """Tests for IndexBloatToolHandler."""

    def test_tool_definition(self, mock_sql_driver):
        """Test that tool definition is properly formed."""
        handler = IndexBloatToolHandler(mock_sql_driver)
        tool_def = handler.get_tool_definition()

        assert tool_def.name == "analyze_index_bloat"
        assert "bloat" in tool_def.description.lower() or "index" in tool_def.description.lower()
        assert tool_def.inputSchema is not None

    @pytest.mark.asyncio
    async def test_extension_not_installed(self, mock_sql_driver):
        """Test handling when pgstattuple extension is not installed."""
        mock_sql_driver.execute_query = AsyncMock(return_value=[])

        handler = IndexBloatToolHandler(mock_sql_driver)
        result = await handler.run_tool({})

        assert "not installed" in result[0].text.lower() or "extension" in result[0].text.lower()

    @pytest.mark.asyncio
    async def test_single_btree_index_analysis(self, mock_sql_driver):
        """Test analyzing a single B-tree index."""
        mock_sql_driver.execute_query = AsyncMock(side_effect=[
            # Extension check - returns available: True
            [{"available": True}],
            # Get index info
            [{
                "index_name": "idx_users_email",
                "table_name": "users",
                "index_type": "btree",
                "index_size": 8192000,
                "is_unique": True,
                "is_primary": False,
                "definition": "CREATE INDEX idx_users_email ON users(email)"
            }],
            # pgstatindex result
            [{
                "version": 4,
                "tree_level": 2,
                "index_size": 8192000,
                "root_block_no": 3,
                "internal_pages": 10,
                "leaf_pages": 100,
                "empty_pages": 5,
                "deleted_pages": 2,
                "avg_leaf_density": 85.5,
                "leaf_fragmentation": 10.2
            }]
        ])

        handler = IndexBloatToolHandler(mock_sql_driver)
        result = await handler.run_tool({
            "index_name": "idx_users_email",
            "schema_name": "public"
        })

        result_text = result[0].text
        assert "idx_users_email" in result_text or "btree" in result_text.lower() or "bloat" in result_text.lower()

    @pytest.mark.asyncio
    async def test_table_indexes_analysis(self, mock_sql_driver):
        """Test analyzing all indexes for a table."""
        mock_sql_driver.execute_query = AsyncMock(side_effect=[
            # Extension check - returns available: True
            [{"available": True}],
            # Get indexes for table
            [
                {
                    "index_name": "idx_users_email",
                    "index_type": "btree",
                    "index_size": 8192000,
                    "is_unique": True,
                    "is_primary": False
                },
                {
                    "index_name": "idx_users_name",
                    "index_type": "btree",
                    "index_size": 4096000,
                    "is_unique": False,
                    "is_primary": False
                }
            ],
            # pgstatindex for idx_users_email
            [{
                "version": 4,
                "tree_level": 2,
                "index_size": 8192000,
                "root_block_no": 3,
                "internal_pages": 10,
                "leaf_pages": 100,
                "empty_pages": 5,
                "deleted_pages": 2,
                "avg_leaf_density": 85.5,
                "leaf_fragmentation": 10.2
            }],
            # pgstatindex for idx_users_name
            [{
                "version": 4,
                "tree_level": 1,
                "index_size": 4096000,
                "root_block_no": 2,
                "internal_pages": 5,
                "leaf_pages": 50,
                "empty_pages": 1,
                "deleted_pages": 0,
                "avg_leaf_density": 90.0,
                "leaf_fragmentation": 5.0
            }]
        ])

        handler = IndexBloatToolHandler(mock_sql_driver)
        result = await handler.run_tool({
            "table_name": "users",
            "schema_name": "public"
        })

        result_text = result[0].text
        assert "indexes" in result_text.lower() or "bloat" in result_text.lower()

    @pytest.mark.asyncio
    async def test_schema_indexes_analysis(self, mock_sql_driver):
        """Test analyzing all indexes in a schema."""
        mock_sql_driver.execute_query = AsyncMock(side_effect=[
            # Extension check - returns available: True
            [{"available": True}],
            # Get all indexes in schema
            [{
                "index_name": "idx_users_email",
                "table_name": "users",
                "index_type": "btree",
                "index_size": 8192000,
                "is_unique": True,
                "is_primary": False
            }],
            # pgstatindex result
            [{
                "version": 4,
                "tree_level": 2,
                "index_size": 8192000,
                "root_block_no": 3,
                "internal_pages": 10,
                "leaf_pages": 100,
                "empty_pages": 5,
                "deleted_pages": 2,
                "avg_leaf_density": 85.5,
                "leaf_fragmentation": 10.2
            }]
        ])

        handler = IndexBloatToolHandler(mock_sql_driver)
        result = await handler.run_tool({
            "schema_name": "public"
        })

        result_text = result[0].text
        assert "index" in result_text.lower() or "bloat" in result_text.lower()


class TestDatabaseBloatSummaryToolHandler:
    """Tests for DatabaseBloatSummaryToolHandler."""

    def test_tool_definition(self, mock_sql_driver):
        """Test that tool definition is properly formed."""
        handler = DatabaseBloatSummaryToolHandler(mock_sql_driver)
        tool_def = handler.get_tool_definition()

        assert tool_def.name == "get_bloat_summary"
        assert "bloat" in tool_def.description.lower() or "summary" in tool_def.description.lower()
        assert tool_def.inputSchema is not None

    @pytest.mark.asyncio
    async def test_extension_not_installed(self, mock_sql_driver):
        """Test handling when pgstattuple extension is not installed."""
        mock_sql_driver.execute_query = AsyncMock(return_value=[])

        handler = DatabaseBloatSummaryToolHandler(mock_sql_driver)
        result = await handler.run_tool({})

        assert "not installed" in result[0].text.lower() or "extension" in result[0].text.lower()

    @pytest.mark.asyncio
    async def test_full_bloat_summary(self, mock_sql_driver):
        """Test generating full bloat summary for database."""
        mock_sql_driver.execute_query = AsyncMock(side_effect=[
            # Extension check - returns available: True
            [{"available": True}],
            # Get tables in schema for table bloat summary
            [
                {"table_name": "users", "table_size": 81920000},
                {"table_name": "orders", "table_size": 40960000}
            ],
            # pgstattuple_approx for users
            [{
                "table_len": 81920000,
                "scanned_percent": 100.0,
                "approx_tuple_count": 100000,
                "approx_tuple_len": 50000000,
                "approx_tuple_percent": 61.0,
                "dead_tuple_count": 5000,
                "dead_tuple_len": 2500000,
                "dead_tuple_percent": 3.05,
                "approx_free_space": 20000000,
                "approx_free_percent": 24.4
            }],
            # pgstattuple_approx for orders
            [{
                "table_len": 40960000,
                "scanned_percent": 100.0,
                "approx_tuple_count": 50000,
                "approx_tuple_len": 25000000,
                "approx_tuple_percent": 61.0,
                "dead_tuple_count": 2000,
                "dead_tuple_len": 1000000,
                "dead_tuple_percent": 2.44,
                "approx_free_space": 10000000,
                "approx_free_percent": 24.4
            }],
            # Get indexes in schema for index bloat summary
            [{
                "index_name": "idx_users_email",
                "table_name": "users",
                "index_type": "btree",
                "index_size": 8192000
            }],
            # pgstatindex for idx_users_email
            [{
                "version": 4,
                "tree_level": 2,
                "index_size": 8192000,
                "root_block_no": 3,
                "internal_pages": 10,
                "leaf_pages": 100,
                "empty_pages": 5,
                "deleted_pages": 2,
                "avg_leaf_density": 85.5,
                "leaf_fragmentation": 10.2
            }]
        ])

        handler = DatabaseBloatSummaryToolHandler(mock_sql_driver)
        result = await handler.run_tool({
            "schema_name": "public"
        })

        result_text = result[0].text
        # Should contain summary information
        assert "bloat" in result_text.lower() or "summary" in result_text.lower() or "total" in result_text.lower()

    @pytest.mark.asyncio
    async def test_tables_only_summary(self, mock_sql_driver):
        """Test generating bloat summary with tables only."""
        mock_sql_driver.execute_query = AsyncMock(side_effect=[
            # Extension check - returns available: True
            [{"available": True}],
            # Get tables in schema
            [{"table_name": "users", "table_size": 81920000}],
            # pgstattuple_approx for users
            [{
                "table_len": 81920000,
                "scanned_percent": 100.0,
                "approx_tuple_count": 100000,
                "approx_tuple_len": 50000000,
                "approx_tuple_percent": 61.0,
                "dead_tuple_count": 5000,
                "dead_tuple_len": 2500000,
                "dead_tuple_percent": 3.05,
                "approx_free_space": 20000000,
                "approx_free_percent": 24.4
            }],
            # Get indexes in schema (empty for this test)
            []
        ])

        handler = DatabaseBloatSummaryToolHandler(mock_sql_driver)
        result = await handler.run_tool({
            "schema_name": "public"
        })

        result_text = result[0].text
        assert "bloat" in result_text.lower() or "table" in result_text.lower() or "summary" in result_text.lower()

    @pytest.mark.asyncio
    async def test_empty_schema(self, mock_sql_driver):
        """Test generating bloat summary for empty schema."""
        mock_sql_driver.execute_query = AsyncMock(side_effect=[
            # Extension check
            [{"extname": "pgstattuple"}],
            # Get tables in schema - empty
            [],
            # Get indexes in schema - empty
            []
        ])

        handler = DatabaseBloatSummaryToolHandler(mock_sql_driver)
        result = await handler.run_tool({
            "schema_name": "empty_schema",
            "include_tables": True,
            "include_indexes": True
        })

        result_text = result[0].text
        # Should handle empty schema gracefully
        assert "bloat" in result_text.lower() or "no" in result_text.lower() or "empty" in result_text.lower() or "summary" in result_text.lower()


# Import new tool handlers for testing
from pgtuner_mcp.tools.tools_query_history import (
    QueryPlanHistoryToolHandler,
    clear_plan_history,
    get_plan_history,
)
from pgtuner_mcp.tools.tools_query_rewrite import (
    QueryRewriteSuggestionsToolHandler,
)
from pgtuner_mcp.tools.tools_vacuum import (
    VacuumProgressToolHandler,
)


class TestQueryPlanHistoryToolHandler:
    """Tests for QueryPlanHistoryToolHandler."""

    def test_tool_definition(self, mock_sql_driver):
        """Test that tool definition is properly formed."""
        handler = QueryPlanHistoryToolHandler(mock_sql_driver)
        tool_def = handler.get_tool_definition()

        assert tool_def.name == "manage_query_plan_history"
        assert "action" in tool_def.inputSchema["required"]
        assert "store" in str(tool_def.inputSchema)
        assert "compare" in str(tool_def.inputSchema)

    @pytest.mark.asyncio
    async def test_store_action(self, mock_sql_driver):
        """Test storing a query plan."""
        # Clear history first
        clear_plan_history()

        mock_sql_driver.execute_query = AsyncMock(return_value=[
            {"QUERY PLAN": [{"Plan": {"Node Type": "Seq Scan", "Total Cost": 100, "Plan Rows": 1000}}]}
        ])

        handler = QueryPlanHistoryToolHandler(mock_sql_driver)
        result = await handler.run_tool({
            "action": "store",
            "query": "SELECT * FROM users",
            "label": "test_plan"
        })

        result_text = result[0].text
        assert "success" in result_text.lower()
        assert "query_hash" in result_text

    @pytest.mark.asyncio
    async def test_store_action_missing_query(self, mock_sql_driver):
        """Test store action with missing query."""
        handler = QueryPlanHistoryToolHandler(mock_sql_driver)
        result = await handler.run_tool({"action": "store"})

        assert "Error" in result[0].text or "Missing" in result[0].text

    @pytest.mark.asyncio
    async def test_get_action(self, mock_sql_driver):
        """Test retrieving stored plans."""
        # Clear and store a plan first
        clear_plan_history()

        mock_sql_driver.execute_query = AsyncMock(return_value=[
            {"QUERY PLAN": [{"Plan": {"Node Type": "Seq Scan", "Total Cost": 100}}]}
        ])

        handler = QueryPlanHistoryToolHandler(mock_sql_driver)
        # Store first
        await handler.run_tool({
            "action": "store",
            "query": "SELECT * FROM users",
            "label": "test"
        })

        # Get plans
        result = await handler.run_tool({
            "action": "get",
            "query": "SELECT * FROM users"
        })

        result_text = result[0].text
        assert "plans" in result_text.lower()
        assert "total_plans" in result_text

    @pytest.mark.asyncio
    async def test_get_action_no_plans(self, mock_sql_driver):
        """Test get action when no plans exist."""
        clear_plan_history()

        handler = QueryPlanHistoryToolHandler(mock_sql_driver)
        result = await handler.run_tool({
            "action": "get",
            "query_hash": "nonexistent"
        })

        assert "No stored plans" in result[0].text

    @pytest.mark.asyncio
    async def test_compare_action(self, mock_sql_driver):
        """Test comparing two plans."""
        clear_plan_history()

        # Mock different execution plans
        mock_sql_driver.execute_query = AsyncMock(side_effect=[
            [{"QUERY PLAN": [{"Plan": {"Node Type": "Seq Scan", "Total Cost": 100}, "Execution Time": 10.5, "Planning Time": 0.5}]}],
            [{"QUERY PLAN": [{"Plan": {"Node Type": "Index Scan", "Total Cost": 50}, "Execution Time": 5.2, "Planning Time": 0.3}]}],
        ])

        handler = QueryPlanHistoryToolHandler(mock_sql_driver)

        # Store two plans
        await handler.run_tool({
            "action": "store",
            "query": "SELECT * FROM users WHERE id = 1",
            "label": "before_index"
        })
        await handler.run_tool({
            "action": "store",
            "query": "SELECT * FROM users WHERE id = 1",
            "label": "after_index"
        })

        # Compare
        result = await handler.run_tool({
            "action": "compare",
            "query": "SELECT * FROM users WHERE id = 1",
            "plan_index_1": 0,
            "plan_index_2": 1
        })

        result_text = result[0].text
        assert "comparison" in result_text.lower()
        assert "plan_1" in result_text
        assert "plan_2" in result_text

    @pytest.mark.asyncio
    async def test_compare_action_insufficient_plans(self, mock_sql_driver):
        """Test compare action with insufficient plans."""
        clear_plan_history()

        mock_sql_driver.execute_query = AsyncMock(return_value=[
            {"QUERY PLAN": [{"Plan": {"Node Type": "Seq Scan"}}]}
        ])

        handler = QueryPlanHistoryToolHandler(mock_sql_driver)
        # Store only one plan
        await handler.run_tool({
            "action": "store",
            "query": "SELECT 1",
            "label": "single"
        })

        result = await handler.run_tool({
            "action": "compare",
            "query": "SELECT 1"
        })

        assert "At least 2" in result[0].text or "insufficient" in result[0].text.lower()

    @pytest.mark.asyncio
    async def test_list_action(self, mock_sql_driver):
        """Test listing all stored query hashes."""
        clear_plan_history()

        mock_sql_driver.execute_query = AsyncMock(return_value=[
            {"QUERY PLAN": [{"Plan": {"Node Type": "Seq Scan"}}]}
        ])

        handler = QueryPlanHistoryToolHandler(mock_sql_driver)
        # Store some plans
        await handler.run_tool({
            "action": "store",
            "query": "SELECT * FROM users"
        })
        await handler.run_tool({
            "action": "store",
            "query": "SELECT * FROM orders"
        })

        result = await handler.run_tool({"action": "list"})

        result_text = result[0].text
        assert "total_queries" in result_text
        assert "queries" in result_text

    @pytest.mark.asyncio
    async def test_list_action_empty(self, mock_sql_driver):
        """Test list action with no stored plans."""
        clear_plan_history()

        handler = QueryPlanHistoryToolHandler(mock_sql_driver)
        result = await handler.run_tool({"action": "list"})

        assert "No stored plans" in result[0].text

    @pytest.mark.asyncio
    async def test_clear_action_specific_query(self, mock_sql_driver):
        """Test clearing history for a specific query."""
        clear_plan_history()

        mock_sql_driver.execute_query = AsyncMock(return_value=[
            {"QUERY PLAN": [{"Plan": {"Node Type": "Seq Scan"}}]}
        ])

        handler = QueryPlanHistoryToolHandler(mock_sql_driver)
        await handler.run_tool({
            "action": "store",
            "query": "SELECT * FROM users"
        })

        result = await handler.run_tool({
            "action": "clear",
            "query": "SELECT * FROM users"
        })

        result_text = result[0].text
        assert "success" in result_text.lower()
        assert "Cleared" in result_text

    @pytest.mark.asyncio
    async def test_clear_action_all(self, mock_sql_driver):
        """Test clearing all history."""
        clear_plan_history()

        mock_sql_driver.execute_query = AsyncMock(return_value=[
            {"QUERY PLAN": [{"Plan": {"Node Type": "Seq Scan"}}]}
        ])

        handler = QueryPlanHistoryToolHandler(mock_sql_driver)
        await handler.run_tool({
            "action": "store",
            "query": "SELECT 1"
        })

        result = await handler.run_tool({"action": "clear"})

        result_text = result[0].text
        assert "success" in result_text.lower()
        assert "Cleared all" in result_text or "Cleared" in result_text


class TestVacuumProgressToolHandler:
    """Tests for VacuumProgressToolHandler."""

    def test_tool_definition(self, mock_sql_driver):
        """Test that tool definition is properly formed."""
        handler = VacuumProgressToolHandler(mock_sql_driver)
        tool_def = handler.get_tool_definition()

        assert tool_def.name == "monitor_vacuum_progress"
        assert "vacuum" in tool_def.description.lower()
        assert "action" in tool_def.inputSchema["properties"]

    @pytest.mark.asyncio
    async def test_progress_action_no_vacuum(self, mock_sql_driver):
        """Test progress action when no vacuum is running."""
        mock_sql_driver.execute_query = AsyncMock(side_effect=[
            [],  # pg_stat_progress_vacuum
            [],  # pg_stat_progress_cluster
            []   # autovacuum workers
        ])

        handler = VacuumProgressToolHandler(mock_sql_driver)
        result = await handler.run_tool({"action": "progress"})

        result_text = result[0].text
        assert "vacuum_operations" in result_text or "No vacuum" in result_text

    @pytest.mark.asyncio
    async def test_progress_action_with_vacuum(self, mock_sql_driver):
        """Test progress action with active vacuum."""
        mock_sql_driver.execute_query = AsyncMock(side_effect=[
            [{  # pg_stat_progress_vacuum
                "pid": 12345,
                "database": "testdb",
                "schema_name": "public",
                "table_name": "users",
                "phase": "scanning heap",
                "heap_blks_total": 1000,
                "heap_blks_scanned": 500,
                "heap_blks_vacuumed": 400,
                "index_vacuum_count": 1,
                "max_dead_tuples": 10000,
                "num_dead_tuples": 500,
                "scan_progress_pct": 50.0,
                "vacuum_progress_pct": 40.0,
                "query": "autovacuum: VACUUM public.users",
                "state": "active",
                "duration_seconds": 120
            }],
            [],  # pg_stat_progress_cluster
            []   # autovacuum workers
        ])

        handler = VacuumProgressToolHandler(mock_sql_driver)
        result = await handler.run_tool({"action": "progress"})

        result_text = result[0].text
        assert "vacuum_operations" in result_text
        assert "users" in result_text or "scanning" in result_text

    @pytest.mark.asyncio
    async def test_needs_vacuum_action(self, mock_sql_driver):
        """Test needs_vacuum action."""
        mock_sql_driver.execute_query = AsyncMock(side_effect=[
            [{  # Tables needing vacuum
                "schema_name": "public",
                "table_name": "users",
                "n_live_tup": 100000,
                "n_dead_tup": 25000,
                "dead_tuple_ratio": 25.0,
                "last_vacuum": None,
                "last_autovacuum": None,
                "exceeds_threshold": True
            }],
            []  # wraparound query
        ])

        handler = VacuumProgressToolHandler(mock_sql_driver)
        result = await handler.run_tool({
            "action": "needs_vacuum",
            "min_dead_tuples": 1000
        })

        result_text = result[0].text
        assert "tables_needing_vacuum" in result_text
        assert "users" in result_text or "summary" in result_text

    @pytest.mark.asyncio
    async def test_autovacuum_status_action(self, mock_sql_driver):
        """Test autovacuum_status action."""
        mock_sql_driver.execute_query = AsyncMock(side_effect=[
            [  # autovacuum settings
                {"name": "autovacuum", "setting": "on", "unit": None, "short_desc": "..."},
                {"name": "autovacuum_max_workers", "setting": "3", "unit": None, "short_desc": "..."},
                {"name": "autovacuum_naptime", "setting": "60", "unit": "s", "short_desc": "..."}
            ],
            [],  # active workers
            [{"pid": 1234, "state": "active", "uptime_seconds": 3600}]  # launcher
        ])

        handler = VacuumProgressToolHandler(mock_sql_driver)
        result = await handler.run_tool({"action": "autovacuum_status"})

        result_text = result[0].text
        assert "autovacuum_enabled" in result_text
        assert "settings" in result_text
        assert "summary" in result_text

    @pytest.mark.asyncio
    async def test_recent_activity_action(self, mock_sql_driver):
        """Test recent_activity action."""
        mock_sql_driver.execute_query = AsyncMock(return_value=[
            {
                "schema_name": "public",
                "table_name": "users",
                "last_vacuum": "2024-01-01T10:00:00",
                "last_autovacuum": "2024-01-01T12:00:00",
                "vacuum_count": 10,
                "autovacuum_count": 50,
                "n_live_tup": 10000,
                "n_dead_tup": 100,
                "vacuum_status": "fresh"
            }
        ])

        handler = VacuumProgressToolHandler(mock_sql_driver)
        result = await handler.run_tool({"action": "recent_activity"})

        result_text = result[0].text
        assert "recent_activity" in result_text
        assert "summary" in result_text


class TestQueryRewriteSuggestionsToolHandler:
    """Tests for QueryRewriteSuggestionsToolHandler."""

    def test_tool_definition(self, mock_sql_driver):
        """Test that tool definition is properly formed."""
        handler = QueryRewriteSuggestionsToolHandler(mock_sql_driver)
        tool_def = handler.get_tool_definition()

        assert tool_def.name == "get_query_rewrite_suggestions"
        assert "query" in tool_def.inputSchema["required"]
        assert "anti-pattern" in tool_def.description.lower() or "rewrite" in tool_def.description.lower()

    @pytest.mark.asyncio
    async def test_select_star_detection(self, mock_sql_driver):
        """Test detection of SELECT * anti-pattern."""
        handler = QueryRewriteSuggestionsToolHandler(mock_sql_driver)
        result = await handler.run_tool({
            "query": "SELECT * FROM users WHERE id = 1",
            "table_info": False
        })

        result_text = result[0].text
        assert "SELECT *" in result_text
        assert "suggestions" in result_text

    @pytest.mark.asyncio
    async def test_function_on_column_detection(self, mock_sql_driver):
        """Test detection of function on column anti-pattern."""
        handler = QueryRewriteSuggestionsToolHandler(mock_sql_driver)
        result = await handler.run_tool({
            "query": "SELECT * FROM users WHERE UPPER(email) = 'TEST@EXAMPLE.COM'",
            "table_info": False
        })

        result_text = result[0].text
        assert "UPPER" in result_text or "function" in result_text.lower()
        assert "index" in result_text.lower()

    @pytest.mark.asyncio
    async def test_not_in_subquery_detection(self, mock_sql_driver):
        """Test detection of NOT IN subquery anti-pattern."""
        handler = QueryRewriteSuggestionsToolHandler(mock_sql_driver)
        result = await handler.run_tool({
            "query": "SELECT * FROM users WHERE id NOT IN (SELECT user_id FROM banned_users)",
            "table_info": False
        })

        result_text = result[0].text
        assert "NOT IN" in result_text or "NOT EXISTS" in result_text

    @pytest.mark.asyncio
    async def test_like_wildcard_prefix_detection(self, mock_sql_driver):
        """Test detection of LIKE with leading wildcard."""
        handler = QueryRewriteSuggestionsToolHandler(mock_sql_driver)
        result = await handler.run_tool({
            "query": "SELECT * FROM users WHERE email LIKE '%@gmail.com'",
            "table_info": False
        })

        result_text = result[0].text
        assert "LIKE" in result_text or "wildcard" in result_text.lower()

    @pytest.mark.asyncio
    async def test_null_comparison_detection(self, mock_sql_driver):
        """Test detection of incorrect NULL comparison."""
        handler = QueryRewriteSuggestionsToolHandler(mock_sql_driver)
        result = await handler.run_tool({
            "query": "SELECT * FROM users WHERE deleted_at = NULL",
            "table_info": False
        })

        result_text = result[0].text
        assert "NULL" in result_text
        assert "IS NULL" in result_text or "critical" in result_text.lower()

    @pytest.mark.asyncio
    async def test_count_for_existence_detection(self, mock_sql_driver):
        """Test detection of COUNT(*) for existence check."""
        handler = QueryRewriteSuggestionsToolHandler(mock_sql_driver)
        result = await handler.run_tool({
            "query": "SELECT * FROM orders WHERE (SELECT COUNT(*) FROM items WHERE items.order_id = orders.id) > 0",
            "table_info": False
        })

        result_text = result[0].text
        assert "COUNT" in result_text or "EXISTS" in result_text

    @pytest.mark.asyncio
    async def test_union_without_all_detection(self, mock_sql_driver):
        """Test detection of UNION without ALL."""
        handler = QueryRewriteSuggestionsToolHandler(mock_sql_driver)
        result = await handler.run_tool({
            "query": "SELECT id FROM users UNION SELECT id FROM admins",
            "table_info": False
        })

        result_text = result[0].text
        assert "UNION" in result_text

    @pytest.mark.asyncio
    async def test_multiple_or_conditions_detection(self, mock_sql_driver):
        """Test detection of multiple OR conditions."""
        handler = QueryRewriteSuggestionsToolHandler(mock_sql_driver)
        result = await handler.run_tool({
            "query": "SELECT * FROM users WHERE status = 'a' OR status = 'b' OR status = 'c' OR status = 'd' OR status = 'e'",
            "table_info": False
        })

        result_text = result[0].text
        assert "OR" in result_text or "IN" in result_text

    @pytest.mark.asyncio
    async def test_no_issues_found(self, mock_sql_driver):
        """Test query with no obvious anti-patterns."""
        handler = QueryRewriteSuggestionsToolHandler(mock_sql_driver)
        result = await handler.run_tool({
            "query": "SELECT id, name, email FROM users WHERE id = 1",
            "table_info": False
        })

        result_text = result[0].text
        assert "suggestions" in result_text

    @pytest.mark.asyncio
    async def test_missing_query_argument(self, mock_sql_driver):
        """Test error when query argument is missing."""
        handler = QueryRewriteSuggestionsToolHandler(mock_sql_driver)
        result = await handler.run_tool({})

        assert "Error" in result[0].text or "Missing" in result[0].text

    @pytest.mark.asyncio
    async def test_summary_generation(self, mock_sql_driver):
        """Test that summary is generated correctly."""
        handler = QueryRewriteSuggestionsToolHandler(mock_sql_driver)
        result = await handler.run_tool({
            "query": "SELECT * FROM users WHERE deleted_at = NULL",
            "table_info": False
        })

        result_text = result[0].text
        assert "summary" in result_text
        assert "overall_status" in result_text or "severity_counts" in result_text

    @pytest.mark.asyncio
    async def test_with_explain_option(self, mock_sql_driver):
        """Test with include_explain option enabled."""
        mock_sql_driver.execute_query = AsyncMock(return_value=[
            {"QUERY PLAN": [{"Plan": {"Node Type": "Seq Scan", "Relation Name": "users", "Plan Rows": 50000}}]}
        ])

        handler = QueryRewriteSuggestionsToolHandler(mock_sql_driver)
        result = await handler.run_tool({
            "query": "SELECT id, name FROM users WHERE status = 'active'",
            "include_explain": True,
            "table_info": False
        })

        result_text = result[0].text
        assert "suggestions" in result_text

    @pytest.mark.asyncio
    async def test_cross_join_detection(self, mock_sql_driver):
        """Test detection of CROSS JOIN."""
        handler = QueryRewriteSuggestionsToolHandler(mock_sql_driver)
        result = await handler.run_tool({
            "query": "SELECT * FROM users CROSS JOIN orders",
            "table_info": False
        })

        result_text = result[0].text
        assert "CROSS JOIN" in result_text or "Cartesian" in result_text

    @pytest.mark.asyncio
    async def test_order_by_limit_detection(self, mock_sql_driver):
        """Test detection of ORDER BY with LIMIT."""
        handler = QueryRewriteSuggestionsToolHandler(mock_sql_driver)
        result = await handler.run_tool({
            "query": "SELECT * FROM users ORDER BY created_at DESC LIMIT 10",
            "table_info": False
        })

        result_text = result[0].text
        assert "ORDER BY" in result_text or "LIMIT" in result_text
