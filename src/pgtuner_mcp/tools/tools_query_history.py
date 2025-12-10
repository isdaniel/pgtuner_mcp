"""Query plan history and comparison tool handlers."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Sequence
from datetime import datetime, timezone
from typing import Any

from mcp.types import TextContent, Tool

from ..services import SqlDriver
from .toolhandler import ToolHandler


# In-memory storage for query plan history
# In production, this could be persisted to a file or database
_plan_history: dict[str, list[dict[str, Any]]] = {}


def _generate_query_hash(query: str) -> str:
    """Generate a hash for a query to use as an identifier."""
    # Normalize whitespace for consistent hashing
    normalized = " ".join(query.split())
    return hashlib.sha256(normalized.encode()).hexdigest()[:16]


class QueryPlanHistoryToolHandler(ToolHandler):
    """Tool handler for storing and comparing execution plans over time."""

    name = "manage_query_plan_history"
    title = "Query Plan History Manager"
    read_only_hint = False  # Can modify history storage
    destructive_hint = False
    idempotent_hint = False
    open_world_hint = False
    description = """Store and compare execution plans over time.

This tool allows you to:
- Store execution plans for queries to track changes
- Compare plans before and after index changes or configuration updates
- Identify performance regressions or improvements
- View historical plan evolution for specific queries

Actions:
- store: Capture and store the current execution plan for a query
- get: Retrieve stored plans for a query
- compare: Compare two stored plans for the same query
- list: List all stored query hashes with summaries
- clear: Clear history for a specific query or all queries

This is useful for:
- Tracking query performance over time
- Validating the impact of index changes
- Detecting performance regressions after upgrades
- A/B testing different query approaches"""

    def __init__(self, sql_driver: SqlDriver):
        self.sql_driver = sql_driver

    def get_tool_definition(self) -> Tool:
        return Tool(
            name=self.name,
            description=self.description,
            inputSchema={
                "type": "object",
                "properties": {
                    "action": {
                        "type": "string",
                        "description": "Action to perform",
                        "enum": ["store", "get", "compare", "list", "clear"]
                    },
                    "query": {
                        "type": "string",
                        "description": "SQL query to analyze (required for store action)"
                    },
                    "query_hash": {
                        "type": "string",
                        "description": "Query hash identifier (for get/compare/clear actions)"
                    },
                    "label": {
                        "type": "string",
                        "description": "Optional label for the stored plan (e.g., 'before_index', 'after_optimization')"
                    },
                    "plan_index_1": {
                        "type": "integer",
                        "description": "First plan index for comparison (0-based)",
                        "default": 0
                    },
                    "plan_index_2": {
                        "type": "integer",
                        "description": "Second plan index for comparison (0-based, -1 for latest)",
                        "default": -1
                    },
                    "analyze": {
                        "type": "boolean",
                        "description": "Whether to actually execute the query when storing (EXPLAIN ANALYZE)",
                        "default": True
                    }
                },
                "required": ["action"]
            },
            annotations=self.get_annotations()
        )

    async def run_tool(self, arguments: dict[str, Any]) -> Sequence[TextContent]:
        try:
            action = arguments.get("action")
            self.validate_required_args(arguments, ["action"])

            if action == "store":
                return await self._store_plan(arguments)
            elif action == "get":
                return await self._get_plans(arguments)
            elif action == "compare":
                return await self._compare_plans(arguments)
            elif action == "list":
                return await self._list_plans(arguments)
            elif action == "clear":
                return await self._clear_plans(arguments)
            else:
                return self.format_result(f"Unknown action: {action}")

        except Exception as e:
            return self.format_error(e)

    async def _store_plan(self, arguments: dict[str, Any]) -> Sequence[TextContent]:
        """Store a new execution plan for a query."""
        query = arguments.get("query")
        if not query:
            self.validate_required_args(arguments, ["query"])

        label = arguments.get("label", "")
        analyze = arguments.get("analyze", True)

        # Generate query hash
        query_hash = _generate_query_hash(query)

        # Build EXPLAIN options
        options = ["BUFFERS", "FORMAT JSON"]
        if analyze:
            options.append("ANALYZE")

        options_str = ", ".join(options)
        explain_query = f"EXPLAIN ({options_str}) {query}"

        # Execute EXPLAIN
        results = await self.sql_driver.execute_query(explain_query)

        if not results:
            return self.format_result("No execution plan returned")

        # Parse plan data
        plan_data = results[0].get("QUERY PLAN", results)
        if isinstance(plan_data, str):
            plan_data = json.loads(plan_data)

        # Extract key metrics
        metrics = self._extract_plan_metrics(plan_data, analyze)

        # Create plan entry
        plan_entry = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "label": label,
            "analyzed": analyze,
            "query": query,
            "plan": plan_data,
            "metrics": metrics
        }

        # Store in history
        if query_hash not in _plan_history:
            _plan_history[query_hash] = []
        _plan_history[query_hash].append(plan_entry)

        output = {
            "success": True,
            "message": "Execution plan stored successfully",
            "query_hash": query_hash,
            "plan_index": len(_plan_history[query_hash]) - 1,
            "label": label,
            "metrics": metrics
        }

        return self.format_json_result(output)

    async def _get_plans(self, arguments: dict[str, Any]) -> Sequence[TextContent]:
        """Retrieve stored plans for a query."""
        query_hash = arguments.get("query_hash")
        query = arguments.get("query")

        if not query_hash and query:
            query_hash = _generate_query_hash(query)

        if not query_hash:
            return self.format_result(
                "Error: Either query_hash or query is required for get action"
            )

        if query_hash not in _plan_history:
            return self.format_result(
                f"No stored plans found for query hash: {query_hash}"
            )

        plans = _plan_history[query_hash]
        output = {
            "query_hash": query_hash,
            "total_plans": len(plans),
            "plans": [
                {
                    "index": i,
                    "timestamp": p["timestamp"],
                    "label": p["label"],
                    "analyzed": p["analyzed"],
                    "metrics": p["metrics"]
                }
                for i, p in enumerate(plans)
            ]
        }

        return self.format_json_result(output)

    async def _compare_plans(self, arguments: dict[str, Any]) -> Sequence[TextContent]:
        """Compare two stored plans for the same query."""
        query_hash = arguments.get("query_hash")
        query = arguments.get("query")
        plan_index_1 = arguments.get("plan_index_1", 0)
        plan_index_2 = arguments.get("plan_index_2", -1)

        if not query_hash and query:
            query_hash = _generate_query_hash(query)

        if not query_hash:
            return self.format_result(
                "Error: Either query_hash or query is required for compare action"
            )

        if query_hash not in _plan_history:
            return self.format_result(
                f"No stored plans found for query hash: {query_hash}"
            )

        plans = _plan_history[query_hash]
        if len(plans) < 2:
            return self.format_result(
                "At least 2 stored plans are required for comparison"
            )

        # Handle negative indices
        if plan_index_2 < 0:
            plan_index_2 = len(plans) + plan_index_2

        if plan_index_1 < 0 or plan_index_1 >= len(plans):
            return self.format_result(f"Invalid plan_index_1: {plan_index_1}")
        if plan_index_2 < 0 or plan_index_2 >= len(plans):
            return self.format_result(f"Invalid plan_index_2: {plan_index_2}")

        plan1 = plans[plan_index_1]
        plan2 = plans[plan_index_2]

        # Compare metrics
        comparison = self._compare_metrics(plan1["metrics"], plan2["metrics"])

        output = {
            "query_hash": query_hash,
            "plan_1": {
                "index": plan_index_1,
                "timestamp": plan1["timestamp"],
                "label": plan1["label"],
                "metrics": plan1["metrics"]
            },
            "plan_2": {
                "index": plan_index_2,
                "timestamp": plan2["timestamp"],
                "label": plan2["label"],
                "metrics": plan2["metrics"]
            },
            "comparison": comparison
        }

        return self.format_json_result(output)

    async def _list_plans(self, arguments: dict[str, Any]) -> Sequence[TextContent]:
        """List all stored query hashes with summaries."""
        if not _plan_history:
            return self.format_result("No stored plans in history")

        summaries = []
        for query_hash, plans in _plan_history.items():
            latest = plans[-1] if plans else None
            # Truncate query for summary
            query_preview = latest["query"][:100] + "..." if latest and len(latest["query"]) > 100 else (latest["query"] if latest else "")

            summaries.append({
                "query_hash": query_hash,
                "plan_count": len(plans),
                "query_preview": query_preview,
                "latest_timestamp": latest["timestamp"] if latest else None,
                "labels": [p["label"] for p in plans if p["label"]]
            })

        output = {
            "total_queries": len(_plan_history),
            "queries": summaries
        }

        return self.format_json_result(output)

    async def _clear_plans(self, arguments: dict[str, Any]) -> Sequence[TextContent]:
        """Clear history for a specific query or all queries."""
        query_hash = arguments.get("query_hash")
        query = arguments.get("query")

        if query and not query_hash:
            query_hash = _generate_query_hash(query)

        if query_hash:
            if query_hash in _plan_history:
                count = len(_plan_history[query_hash])
                del _plan_history[query_hash]
                return self.format_json_result({
                    "success": True,
                    "message": f"Cleared {count} plans for query hash: {query_hash}"
                })
            else:
                return self.format_result(
                    f"No stored plans found for query hash: {query_hash}"
                )
        else:
            # Clear all
            total = sum(len(plans) for plans in _plan_history.values())
            _plan_history.clear()
            return self.format_json_result({
                "success": True,
                "message": f"Cleared all plan history ({total} plans)"
            })

    def _extract_plan_metrics(self, plan_data: Any, was_analyzed: bool) -> dict[str, Any]:
        """Extract key metrics from an execution plan."""
        metrics = {
            "was_analyzed": was_analyzed
        }

        if not plan_data:
            return metrics

        # Handle the plan structure
        if isinstance(plan_data, list) and len(plan_data) > 0:
            root = plan_data[0]
        else:
            root = plan_data

        plan = root.get("Plan", root) if isinstance(root, dict) else root

        # Extract timing information (only available with ANALYZE)
        if was_analyzed:
            if isinstance(root, dict):
                metrics["execution_time_ms"] = root.get("Execution Time", 0)
                metrics["planning_time_ms"] = root.get("Planning Time", 0)

        # Extract plan costs
        if isinstance(plan, dict):
            metrics["total_cost"] = plan.get("Total Cost", 0)
            metrics["startup_cost"] = plan.get("Startup Cost", 0)
            metrics["plan_rows"] = plan.get("Plan Rows", 0)
            metrics["plan_width"] = plan.get("Plan Width", 0)

            if was_analyzed:
                metrics["actual_rows"] = plan.get("Actual Rows", 0)
                metrics["actual_loops"] = plan.get("Actual Loops", 1)

            # Count node types
            node_types = self._count_node_types(plan)
            metrics["node_types"] = node_types

            # Check for sequential scans
            metrics["has_seq_scan"] = "Seq Scan" in node_types
            metrics["seq_scan_count"] = node_types.get("Seq Scan", 0)

        return metrics

    def _count_node_types(self, plan: dict[str, Any]) -> dict[str, int]:
        """Count occurrences of each node type in the plan."""
        counts: dict[str, int] = {}

        def count_recursive(node: dict[str, Any]) -> None:
            if not isinstance(node, dict):
                return
            node_type = node.get("Node Type")
            if node_type:
                counts[node_type] = counts.get(node_type, 0) + 1
            for child in node.get("Plans", []):
                count_recursive(child)

        count_recursive(plan)
        return counts

    def _compare_metrics(
        self,
        metrics1: dict[str, Any],
        metrics2: dict[str, Any]
    ) -> dict[str, Any]:
        """Compare two sets of metrics and identify improvements/regressions."""
        comparison = {
            "changes": [],
            "summary": "",
            "improved": False,
            "regressed": False
        }

        # Compare execution time
        time1 = metrics1.get("execution_time_ms", 0)
        time2 = metrics2.get("execution_time_ms", 0)
        if time1 and time2:
            time_diff = time2 - time1
            time_pct = ((time2 - time1) / time1 * 100) if time1 != 0 else 0
            comparison["changes"].append({
                "metric": "execution_time_ms",
                "before": time1,
                "after": time2,
                "difference": time_diff,
                "percent_change": round(time_pct, 2),
                "status": "improved" if time_diff < 0 else ("regressed" if time_diff > 0 else "unchanged")
            })
            if time_diff < 0:
                comparison["improved"] = True
            elif time_diff > 0:
                comparison["regressed"] = True

        # Compare total cost
        cost1 = metrics1.get("total_cost", 0)
        cost2 = metrics2.get("total_cost", 0)
        if cost1 and cost2:
            cost_diff = cost2 - cost1
            cost_pct = ((cost2 - cost1) / cost1 * 100) if cost1 != 0 else 0
            comparison["changes"].append({
                "metric": "total_cost",
                "before": cost1,
                "after": cost2,
                "difference": cost_diff,
                "percent_change": round(cost_pct, 2),
                "status": "improved" if cost_diff < 0 else ("regressed" if cost_diff > 0 else "unchanged")
            })
            if cost_diff < 0 and not comparison["improved"]:
                comparison["improved"] = True
            elif cost_diff > 0:
                comparison["regressed"] = True

        # Compare sequential scan usage
        seq1 = metrics1.get("seq_scan_count", 0)
        seq2 = metrics2.get("seq_scan_count", 0)
        if seq1 != seq2:
            comparison["changes"].append({
                "metric": "seq_scan_count",
                "before": seq1,
                "after": seq2,
                "difference": seq2 - seq1,
                "status": "improved" if seq2 < seq1 else "regressed"
            })

        # Generate summary
        if comparison["improved"] and not comparison["regressed"]:
            comparison["summary"] = "Query performance improved"
        elif comparison["regressed"] and not comparison["improved"]:
            comparison["summary"] = "Query performance regressed - investigate changes"
        elif comparison["improved"] and comparison["regressed"]:
            comparison["summary"] = "Mixed results - some metrics improved, others regressed"
        else:
            comparison["summary"] = "No significant changes detected"

        return comparison


def get_plan_history() -> dict[str, list[dict[str, Any]]]:
    """Get the current plan history (for testing purposes)."""
    return _plan_history


def clear_plan_history() -> None:
    """Clear all plan history (for testing purposes)."""
    _plan_history.clear()
