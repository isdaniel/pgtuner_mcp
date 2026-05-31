"""Unit tests for lint_query and lint_workload handlers."""

import json
from unittest.mock import AsyncMock

import pytest

from pgtuner_mcp.tools.tools_linter import (
    LintQueryHandler,
    LintWorkloadHandler,
)


class TestLintQueryHandler:
    def test_definition(self, mock_sql_driver):
        h = LintQueryHandler()
        tool = h.get_tool_definition()
        assert tool.name == "lint_query"
        assert "query" in tool.inputSchema["required"]

    @pytest.mark.asyncio
    async def test_lint_returns_findings(self):
        h = LintQueryHandler()
        result = await h.run_tool({"query": "SELECT * FROM users"})
        payload = json.loads(result[0].text)
        assert payload["summary"]["total"] >= 1
        assert any(f["rule"] == "select-star" for f in payload["findings"])

    @pytest.mark.asyncio
    async def test_malformed_sql_returns_structured_error(self):
        h = LintQueryHandler()
        result = await h.run_tool({"query": "not sql"})
        payload = json.loads(result[0].text)
        assert any(f["rule"] == "parse-error" for f in payload["findings"])


    @pytest.mark.asyncio
    async def test_lint_query_severity_threshold_filters(self):
        h = LintQueryHandler()
        # SELECT * is WARNING; with threshold=error it should be filtered
        result = await h.run_tool({"query": "SELECT * FROM users",
                                     "severity_threshold": "error"})
        payload = json.loads(result[0].text)
        assert not any(f["rule"] == "select-star" for f in payload["findings"])


class TestLintWorkloadHandler:
    def test_definition(self, mock_sql_driver):
        h = LintWorkloadHandler(mock_sql_driver)
        tool = h.get_tool_definition()
        assert tool.name == "lint_workload"

    @pytest.mark.asyncio
    async def test_lint_workload_aggregates(self, mock_sql_driver):
        mock_sql_driver.execute_query = AsyncMock(side_effect=[
            [{"exists": True}],  # pg_stat_statements present
            [
                {"query_text": "SELECT * FROM big", "calls": 5000,
                 "mean_time_ms": 10.0, "queryid": "1"},
                {"query_text": "SELECT id FROM small LIMIT 1", "calls": 100,
                 "mean_time_ms": 1.0, "queryid": "2"},
            ],
        ])
        h = LintWorkloadHandler(mock_sql_driver)
        result = await h.run_tool({"limit": 10, "min_calls": 0})
        payload = json.loads(result[0].text)
        assert payload["ranked_findings"][0]["query"].startswith("SELECT * FROM big")

    @pytest.mark.asyncio
    async def test_lint_workload_queryid_passthrough(self, mock_sql_driver):
        # Mock check_extension_installed returning True (single-row result)
        mock_sql_driver.execute_query = AsyncMock(side_effect=[
            [{"exists": True}],  # extension check
            [{"query_text": "SELECT * FROM users", "calls": 1,
              "mean_time_ms": 1.0, "queryid": "qid_abc123"}],
        ])
        h = LintWorkloadHandler(mock_sql_driver)
        result = await h.run_tool({"limit": 5, "min_calls": 0})
        payload = json.loads(result[0].text)
        assert payload["ranked_findings"][0]["queryid"] == "qid_abc123"

    @pytest.mark.asyncio
    async def test_lint_workload_extension_absent(self, mock_sql_driver):
        mock_sql_driver.execute_query = AsyncMock(side_effect=[[]])
        h = LintWorkloadHandler(mock_sql_driver)
        result = await h.run_tool({})
        assert "pg_stat_statements" in result[0].text
        assert "extension" in result[0].text.lower()

    @pytest.mark.asyncio
    async def test_lint_workload_no_findings_when_query_is_clean(self, mock_sql_driver):
        mock_sql_driver.execute_query = AsyncMock(side_effect=[
            [{"exists": True}],
            [{"query_text": "SELECT id, name FROM users LIMIT 10",
              "calls": 100, "mean_time_ms": 5.0, "queryid": "q1"}],
        ])
        h = LintWorkloadHandler(mock_sql_driver)
        result = await h.run_tool({"min_calls": 0, "severity_threshold": "warning"})
        payload = json.loads(result[0].text)
        assert payload["queries_with_findings"] == 0
        assert payload["ranked_findings"] == []

    @pytest.mark.asyncio
    async def test_lint_workload_db_error_returns_format_error(self, mock_sql_driver):
        mock_sql_driver.execute_query = AsyncMock(side_effect=[
            [{"exists": True}],     # extension check passes
            Exception("db down"),    # actual query fails
        ])
        h = LintWorkloadHandler(mock_sql_driver)
        result = await h.run_tool({})
        assert "Error" in result[0].text
        assert "db down" in result[0].text

    @pytest.mark.asyncio
    async def test_lint_workload_handles_long_query_text(self, mock_sql_driver):
        """build_slow_query_sql must return the FULL query text so the linter
        can parse it. SQL-side truncation (LEFT(query, 500)) would corrupt
        the AST for any query longer than 500 chars and produce parse-error."""
        long_query = (
            "SELECT * FROM users WHERE "
            + " OR ".join(f"email = 'user{i:05d}@example.com'" for i in range(40))
        )
        assert len(long_query) > 500
        mock_sql_driver.execute_query = AsyncMock(side_effect=[
            [{"exists": True}],
            [{"query_text": long_query, "calls": 100, "mean_time_ms": 10.0,
              "queryid": "long_q"}],
        ])
        h = LintWorkloadHandler(mock_sql_driver)
        result = await h.run_tool({"min_calls": 0, "severity_threshold": "info"})
        payload = json.loads(result[0].text)
        # Linter parsed the full query and produced findings; no parse-error.
        all_rules = {f["rule"] for r in payload["ranked_findings"] for f in r["findings"]}
        assert "parse-error" not in all_rules
        assert "select-star" in all_rules
