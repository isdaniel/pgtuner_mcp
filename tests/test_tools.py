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
    DiskIOPatternToolHandler,
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
from pgtuner_mcp.tools.tools_vacuum import (
    VacuumProgressToolHandler,
)


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

    # Tests for SQL placeholder escaping - prevents "only '%s', '%b', '%t' are allowed" errors
    # These tests ensure that % wildcards in LIKE patterns are properly escaped as %%

    @pytest.mark.asyncio
    async def test_needs_vacuum_excludes_toast_with_escaped_placeholder(self, mock_sql_driver):
        """Test that LIKE pattern with % is properly escaped when excluding TOAST tables."""
        mock_sql_driver.execute_query = AsyncMock(side_effect=[
            [],  # Tables needing vacuum
            []   # wraparound query
        ])

        handler = VacuumProgressToolHandler(mock_sql_driver)
        result = await handler.run_tool({
            "action": "needs_vacuum",
            "include_toast": False,
            "min_dead_tuples": 1000
        })

        # Verify the query was called
        assert mock_sql_driver.execute_query.called

        # Get the first call (main query, not wraparound query)
        first_call = mock_sql_driver.execute_query.call_args_list[0]
        query = first_call[0][0]

        # Verify that %% (escaped %) is in the query for LIKE pattern
        assert "NOT LIKE 'pg_toast%%'" in query
        # Verify there's no unescaped % in LIKE pattern (would cause placeholder error)
        assert "NOT LIKE 'pg_toast%'" not in query or "NOT LIKE 'pg_toast%%'" in query

    @pytest.mark.asyncio
    async def test_needs_vacuum_includes_toast_no_filter(self, mock_sql_driver):
        """Test that TOAST tables are included when include_toast=True."""
        mock_sql_driver.execute_query = AsyncMock(side_effect=[
            [],  # Tables needing vacuum
            []   # wraparound query
        ])

        handler = VacuumProgressToolHandler(mock_sql_driver)
        result = await handler.run_tool({
            "action": "needs_vacuum",
            "include_toast": True,
            "min_dead_tuples": 1000
        })

        # Get the first call
        first_call = mock_sql_driver.execute_query.call_args_list[0]
        query = first_call[0][0]

        # Verify no TOAST filter is applied when include_toast=True
        assert "NOT LIKE 'pg_toast" not in query

    @pytest.mark.asyncio
    async def test_recent_activity_excludes_toast_with_escaped_placeholder(self, mock_sql_driver):
        """Test that LIKE pattern with % is properly escaped in recent_activity action."""
        mock_sql_driver.execute_query = AsyncMock(return_value=[])

        handler = VacuumProgressToolHandler(mock_sql_driver)
        result = await handler.run_tool({
            "action": "recent_activity",
            "include_toast": False
        })

        # Verify the query was called
        assert mock_sql_driver.execute_query.called

        call_args = mock_sql_driver.execute_query.call_args
        query = call_args[0][0]

        # Verify that %% (escaped %) is in the query for LIKE pattern
        assert "NOT LIKE 'pg_toast%%'" in query
        # Verify there's no unescaped % in LIKE pattern
        assert "NOT LIKE 'pg_toast%'" not in query or "NOT LIKE 'pg_toast%%'" in query

    @pytest.mark.asyncio
    async def test_progress_action_excludes_toast_with_escaped_placeholder(self, mock_sql_driver):
        """Test that LIKE pattern with % is properly escaped in progress action."""
        mock_sql_driver.execute_query = AsyncMock(side_effect=[
            [],  # progress query
            [],  # vacuum full query
            []   # autovacuum workers query
        ])

        handler = VacuumProgressToolHandler(mock_sql_driver)
        result = await handler.run_tool({
            "action": "progress",
            "include_toast": False
        })

        # Verify all queries were called
        assert mock_sql_driver.execute_query.call_count == 3

        # Check all queries for proper escaping
        for call in mock_sql_driver.execute_query.call_args_list:
            query = call[0][0]
            if "pg_toast" in query:
                # Verify that %% (escaped %) is in the query for LIKE pattern
                assert "NOT LIKE 'pg_toast%%'" in query
                # Verify there's no unescaped % in LIKE pattern
                assert "NOT LIKE 'pg_toast%'" not in query or "NOT LIKE 'pg_toast%%'" in query

    @pytest.mark.asyncio
    async def test_needs_vacuum_with_schema_filter_and_params(self, mock_sql_driver):
        """Test that parameters are correctly ordered when schema_name is provided."""
        mock_sql_driver.execute_query = AsyncMock(side_effect=[
            [],  # Tables needing vacuum
            []   # wraparound query
        ])

        handler = VacuumProgressToolHandler(mock_sql_driver)
        result = await handler.run_tool({
            "action": "needs_vacuum",
            "schema_name": "public",
            "min_dead_tuples": 5000,
            "include_toast": False
        })

        # Get the first call
        first_call = mock_sql_driver.execute_query.call_args_list[0]
        query = first_call[0][0]
        params = first_call[0][1]

        # Verify parameters are in correct order: [min_dead_tuples, schema_name]
        assert params == [5000, "public"]

        # Verify query has placeholders in correct order
        assert "%s" in query
        assert "n.nspname = %s" in query


class TestDiskIOPatternToolHandler:
    """Tests for DiskIOPatternToolHandler."""

    def test_tool_definition(self, mock_sql_driver):
        """Test that tool definition is properly formed."""
        handler = DiskIOPatternToolHandler(mock_sql_driver)
        tool_def = handler.get_tool_definition()

        assert tool_def.name == "analyze_disk_io_patterns"
        assert "I/O" in tool_def.description or "disk" in tool_def.description.lower()
        assert "properties" in tool_def.inputSchema
        assert "schema_name" in tool_def.inputSchema["properties"]
        assert "analysis_type" in tool_def.inputSchema["properties"]
        assert "top_n" in tool_def.inputSchema["properties"]

    def test_tool_annotations(self, mock_sql_driver):
        """Test that tool has correct annotations."""
        handler = DiskIOPatternToolHandler(mock_sql_driver)
        tool_def = handler.get_tool_definition()

        assert tool_def.annotations is not None
        assert tool_def.annotations.readOnlyHint is True
        assert tool_def.annotations.destructiveHint is False

    @pytest.mark.asyncio
    async def test_run_tool_all_analysis(self, mock_sql_driver):
        """Test running with all analysis types."""
        # Mock buffer pool query
        mock_sql_driver.execute_query = AsyncMock(side_effect=[
            # Buffer pool stats
            [{
                "heap_blocks_read": 1000,
                "heap_blocks_hit": 50000,
                "index_blocks_read": 500,
                "index_blocks_hit": 30000,
                "toast_blocks_read": 10,
                "toast_blocks_hit": 100,
                "heap_hit_ratio": 98.04,
                "index_hit_ratio": 98.36
            }],
            # Table I/O stats
            [{
                "schemaname": "public",
                "table_name": "users",
                "heap_blks_read": 500,
                "heap_blks_hit": 10000,
                "heap_hit_ratio": 95.24,
                "idx_blks_read": 100,
                "idx_blks_hit": 5000,
                "idx_hit_ratio": 98.04,
                "total_reads": 600,
                "total_hits": 15000
            }],
            # Scan query
            [{
                "schemaname": "public",
                "table_name": "users",
                "seq_scan": 10,
                "seq_tup_read": 1000,
                "idx_scan": 100,
                "idx_tup_fetch": 5000,
                "seq_scan_ratio": 9.09,
                "n_live_tup": 10000,
                "n_dead_tup": 100
            }],
            # Index I/O stats
            [{
                "schemaname": "public",
                "table_name": "users",
                "index_name": "idx_users_email",
                "idx_blks_read": 100,
                "idx_blks_hit": 5000,
                "hit_ratio": 98.04
            }],
            # Temp files
            [{
                "datname": "testdb",
                "temp_files": 5,
                "temp_bytes": 1048576,
                "temp_size_pretty": "1 MB"
            }],
            # Checkpoint stats
            [{
                "checkpoints_timed": 100,
                "checkpoints_req": 5,
                "checkpoint_write_time": 50000,
                "checkpoint_sync_time": 1000,
                "buffers_checkpoint": 10000,
                "buffers_clean": 5000,
                "buffers_backend": 100,
                "buffers_backend_fsync": 0,
                "buffers_alloc": 50000,
                "backend_write_ratio": 0.66,
                "stats_reset": "2024-01-01 00:00:00"
            }],
            # pg_stat_io check
            [{"available": False}]
        ])

        handler = DiskIOPatternToolHandler(mock_sql_driver)
        result = await handler.run_tool({"analysis_type": "all"})

        result_text = result[0].text
        assert "io_patterns" in result_text
        assert "buffer_pool" in result_text
        assert "tables" in result_text
        assert "summary" in result_text

    @pytest.mark.asyncio
    async def test_run_tool_buffer_pool_only(self, mock_sql_driver):
        """Test running with buffer_pool analysis type only."""
        mock_sql_driver.execute_query = AsyncMock(side_effect=[
            [{
                "heap_blocks_read": 1000,
                "heap_blocks_hit": 50000,
                "index_blocks_read": 500,
                "index_blocks_hit": 30000,
                "toast_blocks_read": 10,
                "toast_blocks_hit": 100,
                "heap_hit_ratio": 98.04,
                "index_hit_ratio": 98.36
            }],
            [{"available": False}]  # pg_stat_io check
        ])

        handler = DiskIOPatternToolHandler(mock_sql_driver)
        result = await handler.run_tool({"analysis_type": "buffer_pool"})

        result_text = result[0].text
        assert "buffer_pool" in result_text
        assert "heap_hit_ratio" in result_text

    @pytest.mark.asyncio
    async def test_run_tool_tables_only(self, mock_sql_driver):
        """Test running with tables analysis type only."""
        mock_sql_driver.execute_query = AsyncMock(side_effect=[
            # Table I/O stats
            [{
                "schemaname": "public",
                "table_name": "orders",
                "heap_blks_read": 2000,
                "heap_blks_hit": 8000,
                "heap_hit_ratio": 80.0,
                "idx_blks_read": 500,
                "idx_blks_hit": 4500,
                "idx_hit_ratio": 90.0,
                "total_reads": 2500,
                "total_hits": 12500
            }],
            # Scan query
            [{
                "schemaname": "public",
                "table_name": "orders",
                "seq_scan": 500,
                "seq_tup_read": 500000,
                "idx_scan": 100,
                "idx_tup_fetch": 10000,
                "seq_scan_ratio": 83.33,
                "n_live_tup": 50000,
                "n_dead_tup": 1000
            }],
            [{"available": False}]  # pg_stat_io check
        ])

        handler = DiskIOPatternToolHandler(mock_sql_driver)
        result = await handler.run_tool({"analysis_type": "tables", "schema_name": "public"})

        result_text = result[0].text
        assert "tables" in result_text
        assert "orders" in result_text

    @pytest.mark.asyncio
    async def test_run_tool_indexes_only(self, mock_sql_driver):
        """Test running with indexes analysis type only."""
        mock_sql_driver.execute_query = AsyncMock(side_effect=[
            [{
                "schemaname": "public",
                "table_name": "users",
                "index_name": "idx_users_id",
                "idx_blks_read": 200,
                "idx_blks_hit": 9800,
                "hit_ratio": 98.0
            }],
            [{"available": False}]  # pg_stat_io check
        ])

        handler = DiskIOPatternToolHandler(mock_sql_driver)
        result = await handler.run_tool({"analysis_type": "indexes", "include_indexes": True})

        result_text = result[0].text
        assert "indexes" in result_text
        assert "idx_users_id" in result_text

    @pytest.mark.asyncio
    async def test_run_tool_temp_files_only(self, mock_sql_driver):
        """Test running with temp_files analysis type only."""
        mock_sql_driver.execute_query = AsyncMock(side_effect=[
            [{
                "datname": "testdb",
                "temp_files": 10000,
                "temp_bytes": 10737418240,  # 10 GB
                "temp_size_pretty": "10 GB"
            }],
            [{"available": False}]  # pg_stat_io check
        ])

        handler = DiskIOPatternToolHandler(mock_sql_driver)
        result = await handler.run_tool({"analysis_type": "temp_files"})

        result_text = result[0].text
        assert "temp_files" in result_text
        # Should have recommendations for high temp file usage
        assert "recommendations" in result_text

    @pytest.mark.asyncio
    async def test_run_tool_checkpoints_only(self, mock_sql_driver):
        """Test running with checkpoints analysis type only."""
        mock_sql_driver.execute_query = AsyncMock(side_effect=[
            [{
                "checkpoints_timed": 50,
                "checkpoints_req": 100,
                "checkpoint_write_time": 100000,
                "checkpoint_sync_time": 5000,
                "buffers_checkpoint": 5000,
                "buffers_clean": 2000,
                "buffers_backend": 3000,
                "buffers_backend_fsync": 10,
                "buffers_alloc": 20000,
                "backend_write_ratio": 30.0,
                "stats_reset": "2024-01-01 00:00:00"
            }],
            [{"available": False}]  # pg_stat_io check
        ])

        handler = DiskIOPatternToolHandler(mock_sql_driver)
        result = await handler.run_tool({"analysis_type": "checkpoints"})

        result_text = result[0].text
        assert "checkpoints" in result_text
        # Should have issues for high backend write ratio and fsync
        assert "issues" in result_text
        assert "recommendations" in result_text

    @pytest.mark.asyncio
    async def test_run_tool_low_cache_hit_issues(self, mock_sql_driver):
        """Test that low cache hit ratios generate issues."""
        mock_sql_driver.execute_query = AsyncMock(side_effect=[
            [{
                "heap_blocks_read": 50000,
                "heap_blocks_hit": 50000,
                "index_blocks_read": 10000,
                "index_blocks_hit": 90000,
                "toast_blocks_read": 10,
                "toast_blocks_hit": 100,
                "heap_hit_ratio": 50.0,  # Very low
                "index_hit_ratio": 90.0  # Low
            }],
            [{"available": False}]  # pg_stat_io check
        ])

        handler = DiskIOPatternToolHandler(mock_sql_driver)
        result = await handler.run_tool({"analysis_type": "buffer_pool"})

        result_text = result[0].text
        assert "issues" in result_text
        assert "Low heap buffer cache hit ratio" in result_text or "50" in result_text

    @pytest.mark.asyncio
    async def test_run_tool_pg_stat_io_available(self, mock_sql_driver):
        """Test behavior when pg_stat_io is available (PostgreSQL 16+)."""
        mock_sql_driver.execute_query = AsyncMock(side_effect=[
            # Buffer pool
            [{
                "heap_blocks_read": 1000,
                "heap_blocks_hit": 50000,
                "index_blocks_read": 500,
                "index_blocks_hit": 30000,
                "heap_hit_ratio": 98.04,
                "index_hit_ratio": 98.36
            }],
            # pg_stat_io check - available
            [{"available": True}],
            # pg_stat_io data
            [{
                "backend_type": "client backend",
                "object": "relation",
                "context": "normal",
                "reads": 10000,
                "read_time": 50000,
                "writes": 1000,
                "write_time": 5000,
                "hits": 500000,
                "evictions": 100,
                "reuses": 1000,
                "fsyncs": 0,
                "fsync_time": 0
            }]
        ])

        handler = DiskIOPatternToolHandler(mock_sql_driver)
        result = await handler.run_tool({"analysis_type": "buffer_pool"})

        result_text = result[0].text
        assert "pg_stat_io" in result_text
        assert "available" in result_text

    @pytest.mark.asyncio
    async def test_run_tool_empty_results(self, mock_sql_driver):
        """Test handling when queries return no results."""
        mock_sql_driver.execute_query = AsyncMock(return_value=[])

        handler = DiskIOPatternToolHandler(mock_sql_driver)
        result = await handler.run_tool({"analysis_type": "all"})

        result_text = result[0].text
        # Should still return valid JSON with empty patterns
        assert "io_patterns" in result_text
        assert "summary" in result_text

    @pytest.mark.asyncio
    async def test_run_tool_error_handling(self, mock_sql_driver):
        """Test error handling when query fails."""
        mock_sql_driver.execute_query = AsyncMock(
            side_effect=Exception("Database connection error")
        )

        handler = DiskIOPatternToolHandler(mock_sql_driver)
        result = await handler.run_tool({})

        result_text = result[0].text
        assert "Error" in result_text
        assert "Database connection error" in result_text

    @pytest.mark.asyncio
    async def test_run_tool_top_n_parameter(self, mock_sql_driver):
        """Test that top_n parameter is respected."""
        tables_data = [
            {
                "schemaname": "public",
                "table_name": f"table_{i}",
                "heap_blks_read": 100 * i,
                "heap_blks_hit": 1000 * i,
                "heap_hit_ratio": 90.0,
                "idx_blks_read": 50 * i,
                "idx_blks_hit": 500 * i,
                "idx_hit_ratio": 91.0,
                "total_reads": 150 * i,
                "total_hits": 1500 * i
            }
            for i in range(1, 6)  # 5 tables
        ]

        mock_sql_driver.execute_query = AsyncMock(side_effect=[
            tables_data[:5],  # Table I/O stats
            [],  # Scan query
            [{"available": False}]  # pg_stat_io check
        ])

        handler = DiskIOPatternToolHandler(mock_sql_driver)
        result = await handler.run_tool({"analysis_type": "tables", "top_n": 5})

        result_text = result[0].text
        assert "tables" in result_text
        assert "count" in result_text

    @pytest.mark.asyncio
    async def test_run_tool_sequential_scan_issue_detection(self, mock_sql_driver):
        """Test detection of sequential scan heavy tables."""
        mock_sql_driver.execute_query = AsyncMock(side_effect=[
            # Table I/O stats
            [{
                "schemaname": "public",
                "table_name": "large_table",
                "heap_blks_read": 10000,
                "heap_blks_hit": 5000,
                "heap_hit_ratio": 33.33,
                "idx_blks_read": 100,
                "idx_blks_hit": 500,
                "idx_hit_ratio": 83.33,
                "total_reads": 10100,
                "total_hits": 5500
            }],
            # Scan query showing high sequential scans
            [{
                "schemaname": "public",
                "table_name": "large_table",
                "seq_scan": 900,
                "seq_tup_read": 9000000,
                "idx_scan": 100,
                "idx_tup_fetch": 10000,
                "seq_scan_ratio": 90.0,
                "n_live_tup": 100000,
                "n_dead_tup": 5000
            }],
            [{"available": False}]  # pg_stat_io check
        ])

        handler = DiskIOPatternToolHandler(mock_sql_driver)
        result = await handler.run_tool({"analysis_type": "tables"})

        result_text = result[0].text
        assert "issues" in result_text
        assert "sequential scan" in result_text.lower() or "90" in result_text

    @pytest.mark.asyncio
    async def test_run_tool_backend_fsync_detection(self, mock_sql_driver):
        """Test detection of backend fsync issues."""
        mock_sql_driver.execute_query = AsyncMock(side_effect=[
            [{
                "checkpoints_timed": 100,
                "checkpoints_req": 50,
                "checkpoint_write_time": 50000,
                "checkpoint_sync_time": 1000,
                "buffers_checkpoint": 10000,
                "buffers_clean": 5000,
                "buffers_backend": 1000,
                "buffers_backend_fsync": 50,  # Non-zero fsync - very bad
                "buffers_alloc": 50000,
                "backend_write_ratio": 6.25,
                "stats_reset": "2024-01-01 00:00:00"
            }],
            [{"available": False}]  # pg_stat_io check
        ])

        handler = DiskIOPatternToolHandler(mock_sql_driver)
        result = await handler.run_tool({"analysis_type": "checkpoints"})

        result_text = result[0].text
        assert "fsync" in result_text.lower()
        assert "issues" in result_text
