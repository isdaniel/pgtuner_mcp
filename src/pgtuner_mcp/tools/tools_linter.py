"""Query anti-pattern linter MCP tools."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

from mcp.types import TextContent, Tool

from pgtuner_mcp.services.query_linter import QueryLinter, Severity
from pgtuner_mcp.services.sql_driver import SqlDriver, check_extension_installed
from pgtuner_mcp.services.user_filter import get_user_filter
from pgtuner_mcp.tools.toolhandler import ToolHandler
from pgtuner_mcp.tools.tools_performance import build_slow_query_sql


_SEVERITY_VALUES = ["info", "warning", "error"]
_SEVERITY_RANK = {"info": 0, "warning": 1, "error": 2}


class LintQueryHandler(ToolHandler):
    name = "lint_query"
    description = (
        "Statically lint a SQL query for anti-patterns using a pglast AST visitor. "
        "Pure-static, no DB call. Detects SELECT *, implicit casts, OR-of-equals, "
        "non-sargable LIKE, unbounded SELECT, NOT IN nullable, function-on-indexed-col."
    )
    title = "Lint SQL Query"
    read_only_hint = True

    def get_tool_definition(self) -> Tool:
        return Tool(
            name=self.name,
            description=self.description,
            inputSchema={
                "type": "object",
                "properties": {
                    "query": {"type": "string", "description": "SQL to lint"},
                    "severity_threshold": {
                        "type": "string",
                        "enum": _SEVERITY_VALUES,
                        "default": "info",
                    },
                },
                "required": ["query"],
            },
            annotations=self.get_annotations(),
        )

    async def run_tool(self, arguments: dict[str, Any]) -> Sequence[TextContent]:
        self.validate_required_args(arguments, ["query"])
        query = arguments["query"]
        threshold = Severity(arguments.get("severity_threshold", "info"))
        linter = QueryLinter()
        findings = linter.lint(query, threshold=threshold)
        by_sev: dict[str, int] = {"info": 0, "warning": 0, "error": 0}
        for f in findings:
            by_sev[f.severity.value] = by_sev.get(f.severity.value, 0) + 1
        return self.format_json_result({
            "findings": [f.to_dict() for f in findings],
            "summary": {"total": len(findings), "by_severity": by_sev},
        })


class LintWorkloadHandler(ToolHandler):
    name = "lint_workload"
    description = (
        "Pull top-N queries from pg_stat_statements and lint each. "
        "Findings are ranked by severity x calls x mean_time so the "
        "worst-offending pattern appears first."
    )
    title = "Lint Workload"
    read_only_hint = True

    def __init__(self, driver: SqlDriver):
        self.driver = driver

    def get_tool_definition(self) -> Tool:
        return Tool(
            name=self.name,
            description=self.description,
            inputSchema={
                "type": "object",
                "properties": {
                    "limit": {"type": "integer", "default": 20, "minimum": 1, "maximum": 200},
                    "min_calls": {"type": "integer", "default": 100, "minimum": 0},
                    "min_mean_time_ms": {"type": ["number", "null"], "default": None},
                    "severity_threshold": {
                        "type": "string",
                        "enum": _SEVERITY_VALUES,
                        "default": "warning",
                    },
                },
            },
            annotations=self.get_annotations(),
        )

    async def run_tool(self, arguments: dict[str, Any]) -> Sequence[TextContent]:
        limit = int(arguments.get("limit", 20))
        min_calls = int(arguments.get("min_calls", 100))
        min_mean_time = arguments.get("min_mean_time_ms")
        threshold = Severity(arguments.get("severity_threshold", "warning"))

        # Check pg_stat_statements is installed; bail out with a structured error
        # rather than crashing on an undefined relation.
        if not await check_extension_installed(self.driver, "pg_stat_statements"):
            return self.format_json_result({
                "error": "pg_stat_statements extension not installed",
                "remediation": (
                    "Add pg_stat_statements to shared_preload_libraries and "
                    "CREATE EXTENSION pg_stat_statements;"
                ),
            })

        user_filter = get_user_filter()
        statements_filter = user_filter.get_statements_filter()

        # Reuse the slow-query SQL builder so we get system-schema filters for free.
        sql, params = build_slow_query_sql(
            min_calls=min_calls,
            min_mean_time_ms=min_mean_time,
            limit=limit,
            order_by="mean_time",
            statements_filter=statements_filter,
        )
        try:
            rows = await self.driver.execute_query(sql, params) or []
        except Exception as e:
            return self.format_error(e)

        linter = QueryLinter()
        ranked: list[dict[str, Any]] = []
        for r in rows:
            # Column names come straight from build_slow_query_sql's SELECT list.
            query_text = r["query_text"]
            mean_time = r["mean_time_ms"]
            queryid = r["queryid"]
            calls = r["calls"]

            findings = linter.lint(query_text, threshold=threshold)
            if not findings:
                continue
            top_severity_rank = max(_SEVERITY_RANK[f.severity.value] for f in findings)
            score = (top_severity_rank + 1) * float(calls) * float(mean_time)
            ranked.append({
                "query": query_text[:500],
                "calls": calls,
                "mean_exec_time_ms": float(mean_time),
                "queryid": str(queryid) if queryid is not None else None,
                "findings": [f.to_dict() for f in findings],
                "score": score,
            })
        ranked.sort(key=lambda x: x["score"], reverse=True)

        return self.format_json_result({
            "analyzed_queries": len(rows),
            "queries_with_findings": len(ranked),
            "ranked_findings": ranked,
        })
