"""Index management and analysis tool handlers."""

from __future__ import annotations

import json
from collections.abc import Sequence
from typing import Any

from mcp.types import TextContent, Tool

from ..services import HypoPGService, IndexAdvisor, SqlDriver
from .toolhandler import ToolHandler


class IndexAdvisorToolHandler(ToolHandler):
    """Tool handler for AI-powered index recommendations."""

    name = "get_index_recommendations"
    title = "Index Recommendation Engine"
    read_only_hint = True
    destructive_hint = False
    idempotent_hint = True
    open_world_hint = False
    description = """Get AI-powered index recommendations for your database.

Analyzes your query workload (from pg_stat_statements) and recommends indexes
that would improve performance. Uses a sophisticated analysis algorithm that:

1. Identifies slow queries and their access patterns
2. Extracts columns used in WHERE, JOIN, ORDER BY, and GROUP BY clauses
3. Generates candidate indexes (single-column and composite)
4. If HypoPG is available, tests indexes without creating them
5. Uses a greedy optimization algorithm to select the best index set

The recommendations consider:
- Query frequency and total execution time
- Estimated improvement from each index
- Index size and maintenance overhead
- Avoiding redundant indexes"""

    def __init__(self, index_advisor: IndexAdvisor):
        self.index_advisor = index_advisor

    def get_tool_definition(self) -> Tool:
        return Tool(
            name=self.name,
            description=self.description,
            inputSchema={
                "type": "object",
                "properties": {
                    "workload_queries": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Optional list of specific queries to analyze. If not provided, uses pg_stat_statements."
                    },
                    "max_recommendations": {
                        "type": "integer",
                        "description": "Maximum number of index recommendations to return",
                        "default": 10,
                        "minimum": 1,
                        "maximum": 50
                    },
                    "min_improvement_percent": {
                        "type": "number",
                        "description": "Minimum improvement percentage for a recommendation to be included",
                        "default": 10.0
                    },
                    "include_hypothetical_testing": {
                        "type": "boolean",
                        "description": "Whether to test indexes using HypoPG (if available)",
                        "default": True
                    },
                    "target_tables": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Optional list of tables to focus on"
                    }
                },
                "required": []
            },
            annotations=self.get_annotations()
        )

    async def run_tool(self, arguments: dict[str, Any]) -> Sequence[TextContent]:
        try:
            workload_queries = arguments.get("workload_queries")
            max_recommendations = arguments.get("max_recommendations", 10)
            min_improvement = arguments.get("min_improvement_percent", 10.0)
            use_hypopg = arguments.get("include_hypothetical_testing", True)
            target_tables = arguments.get("target_tables")

            if workload_queries:
                # Analyze specific queries
                all_recommendations = []
                for query in workload_queries:
                    recs = await self.index_advisor.analyze_query(
                        query,
                        test_with_hypopg=use_hypopg
                    )
                    all_recommendations.extend(recs)
            else:
                # Analyze workload from pg_stat_statements
                all_recommendations = await self.index_advisor.analyze_workload(
                    limit=50,
                    min_calls=5
                )

            # Filter by target tables if specified
            if target_tables:
                target_set = {t.lower() for t in target_tables}
                all_recommendations = [
                    r for r in all_recommendations
                    if r.get("table", "").lower() in target_set
                ]

            # Filter by minimum improvement
            recommendations = [
                r for r in all_recommendations
                if r.get("estimated_improvement_percent", 0) >= min_improvement
            ]

            # Sort by improvement and limit
            recommendations.sort(
                key=lambda x: x.get("estimated_improvement_percent", 0),
                reverse=True
            )
            recommendations = recommendations[:max_recommendations]

            # Check HypoPG availability
            hypopg_available = await self.index_advisor.hypopg_service.check_hypopg_available()

            output = {
                "summary": {
                    "total_recommendations": len(recommendations),
                    "hypopg_available": hypopg_available,
                    "analysis_source": "provided_queries" if workload_queries else "pg_stat_statements"
                },
                "recommendations": recommendations,
                "create_statements": [
                    r.get("create_statement") for r in recommendations
                    if r.get("create_statement")
                ]
            }

            if not recommendations:
                output["message"] = (
                    "No index recommendations found. This could mean:\n"
                    "- Your database already has optimal indexes\n"
                    "- Not enough query data in pg_stat_statements\n"
                    "- The improvement threshold is too high"
                )

            return self.format_json_result(output)

        except Exception as e:
            return self.format_error(e)


class ExplainQueryToolHandler(ToolHandler):
    """Tool handler for EXPLAIN with hypothetical index testing."""

    name = "explain_with_indexes"
    title = "Query Plan Analyzer"
    read_only_hint = True
    destructive_hint = False
    idempotent_hint = True
    open_world_hint = False
    description = """Run EXPLAIN on a query, optionally with hypothetical indexes.

This tool allows you to see how a query would perform with proposed indexes
WITHOUT actually creating them. Requires HypoPG extension for hypothetical testing.

Use this to:
- Compare execution plans with and without specific indexes
- Test if a proposed index would be used
- Estimate the performance impact of new indexes

Returns both the original and hypothetical execution plans for comparison."""

    def __init__(self, sql_driver: SqlDriver, hypopg_service: HypoPGService):
        self.sql_driver = sql_driver
        self.hypopg_service = hypopg_service

    def get_tool_definition(self) -> Tool:
        return Tool(
            name=self.name,
            description=self.description,
            inputSchema={
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": "The SQL query to explain"
                    },
                    "hypothetical_indexes": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "table": {
                                    "type": "string",
                                    "description": "Table name"
                                },
                                "columns": {
                                    "type": "array",
                                    "items": {"type": "string"},
                                    "description": "Columns for the index"
                                },
                                "index_type": {
                                    "type": "string",
                                    "enum": ["btree", "hash", "gin", "gist", "brin"],
                                    "default": "btree"
                                },
                                "unique": {
                                    "type": "boolean",
                                    "default": False
                                }
                            },
                            "required": ["table", "columns"]
                        },
                        "description": "List of hypothetical indexes to test"
                    },
                    "analyze": {
                        "type": "boolean",
                        "description": "Whether to use EXPLAIN ANALYZE (executes the query)",
                        "default": False
                    }
                },
                "required": ["query"]
            },
            annotations=self.get_annotations()
        )

    async def run_tool(self, arguments: dict[str, Any]) -> Sequence[TextContent]:
        try:
            self.validate_required_args(arguments, ["query"])

            query = arguments["query"]
            hypothetical_indexes = arguments.get("hypothetical_indexes", [])
            analyze = arguments.get("analyze", False)

            # Get the original execution plan
            explain_opts = "ANALYZE, " if analyze else ""
            original_explain = f"EXPLAIN ({explain_opts}FORMAT JSON) {query}"
            original_result = await self.sql_driver.execute_query(original_explain)
            original_plan = self._extract_plan(original_result)

            output = {
                "query": query,
                "original_plan": original_plan,
                "original_cost": self._extract_cost(original_plan)
            }

            # Test with hypothetical indexes if provided
            if hypothetical_indexes:
                hypopg_available = await self.hypopg_service.check_hypopg_available()

                if not hypopg_available:
                    output["error"] = (
                        "HypoPG extension is not available. "
                        "Install it with: CREATE EXTENSION hypopg;"
                    )
                    return self.format_json_result(output)

                # Create hypothetical indexes
                created_indexes = []
                try:
                    for idx_spec in hypothetical_indexes:
                        result = await self.hypopg_service.create_hypothetical_index(
                            table=idx_spec["table"],
                            columns=idx_spec["columns"],
                            index_type=idx_spec.get("index_type", "btree"),
                            unique=idx_spec.get("unique", False)
                        )
                        if result.get("success"):
                            created_indexes.append({
                                **idx_spec,
                                "index_name": result.get("index_name"),
                                "estimated_size": result.get("estimated_size")
                            })

                    # Get the plan with hypothetical indexes
                    hypo_result = await self.sql_driver.execute_query(original_explain)
                    hypo_plan = self._extract_plan(hypo_result)

                    output["hypothetical_indexes"] = created_indexes
                    output["plan_with_indexes"] = hypo_plan
                    output["cost_with_indexes"] = self._extract_cost(hypo_plan)

                    # Calculate improvement
                    original_cost = output["original_cost"]
                    new_cost = output["cost_with_indexes"]
                    if original_cost > 0:
                        improvement = ((original_cost - new_cost) / original_cost) * 100
                        output["estimated_improvement_percent"] = round(improvement, 2)

                    # Check which hypothetical indexes were used
                    output["indexes_used"] = self._find_used_indexes(hypo_plan, created_indexes)

                finally:
                    # Clean up hypothetical indexes
                    await self.hypopg_service.reset_hypothetical_indexes()

            return self.format_json_result(output)

        except Exception as e:
            return self.format_error(e)

    def _extract_plan(self, result: list[dict]) -> dict:
        """Extract the execution plan from EXPLAIN result."""
        if not result:
            return {}

        plan_data = result[0].get("QUERY PLAN", result[0])
        if isinstance(plan_data, str):
            plan_data = json.loads(plan_data)

        if isinstance(plan_data, list) and len(plan_data) > 0:
            return plan_data[0]
        return plan_data

    def _extract_cost(self, plan: dict) -> float:
        """Extract total cost from execution plan."""
        if not plan:
            return 0.0

        plan_node = plan.get("Plan", plan)
        return plan_node.get("Total Cost", 0.0)

    def _find_used_indexes(
        self,
        plan: dict,
        created_indexes: list[dict]
    ) -> list[dict]:
        """Find which hypothetical indexes were used in the plan."""
        used = []
        index_names = {idx["index_name"] for idx in created_indexes if "index_name" in idx}

        def check_node(node: dict):
            if not isinstance(node, dict):
                return

            node_type = node.get("Node Type", "")
            if "Index" in node_type:
                idx_name = node.get("Index Name", "")
                if any(name in idx_name for name in index_names):
                    used.append({
                        "index_name": idx_name,
                        "scan_type": node_type,
                        "startup_cost": node.get("Startup Cost"),
                        "total_cost": node.get("Total Cost")
                    })

            for child in node.get("Plans", []):
                check_node(child)

        check_node(plan.get("Plan", plan))
        return used


class HypoPGToolHandler(ToolHandler):
    """Tool handler for direct HypoPG hypothetical index management."""

    name = "manage_hypothetical_indexes"
    title = "Hypothetical Index Manager"
    read_only_hint = False  # Can create/drop hypothetical indexes
    destructive_hint = False  # Hypothetical indexes don't affect real data
    idempotent_hint = False  # Creating indexes multiple times creates multiple
    open_world_hint = False
    description = """Manage HypoPG hypothetical indexes for testing.

HypoPG allows you to create "hypothetical" indexes that exist only in memory
and can be used to test query plans without the overhead of creating real indexes.

Actions:
- create: Create a new hypothetical index
- list: List all current hypothetical indexes
- drop: Drop a specific hypothetical index
- reset: Drop all hypothetical indexes
- estimate_size: Estimate the size of a hypothetical index

This is useful for:
- Testing if an index would improve a query
- Comparing different index strategies
- Estimating index storage requirements"""

    def __init__(self, hypopg_service: HypoPGService):
        self.hypopg_service = hypopg_service

    def get_tool_definition(self) -> Tool:
        return Tool(
            name=self.name,
            description=self.description,
            inputSchema={
                "type": "object",
                "properties": {
                    "action": {
                        "type": "string",
                        "enum": ["create", "list", "drop", "reset", "estimate_size", "check"],
                        "description": "Action to perform"
                    },
                    "table": {
                        "type": "string",
                        "description": "Table name (required for create, estimate_size)"
                    },
                    "columns": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Column names for the index (required for create, estimate_size)"
                    },
                    "index_type": {
                        "type": "string",
                        "enum": ["btree", "hash", "gin", "gist", "brin"],
                        "default": "btree",
                        "description": "Type of index to create"
                    },
                    "unique": {
                        "type": "boolean",
                        "default": False,
                        "description": "Whether the index should be unique"
                    },
                    "index_id": {
                        "type": "integer",
                        "description": "Index OID (required for drop)"
                    }
                },
                "required": ["action"]
            },
            annotations=self.get_annotations()
        )

    async def run_tool(self, arguments: dict[str, Any]) -> Sequence[TextContent]:
        try:
            action = arguments.get("action")

            if action == "check":
                available = await self.hypopg_service.check_hypopg_available()
                return self.format_json_result({
                    "hypopg_available": available,
                    "message": "HypoPG extension is available" if available
                              else "HypoPG extension is NOT installed. Install with: CREATE EXTENSION hypopg;"
                })

            if action == "create":
                self.validate_required_args(arguments, ["table", "columns"])
                result = await self.hypopg_service.create_hypothetical_index(
                    table=arguments["table"],
                    columns=arguments["columns"],
                    index_type=arguments.get("index_type", "btree"),
                    unique=arguments.get("unique", False)
                )
                return self.format_json_result(result)

            elif action == "list":
                indexes = await self.hypopg_service.list_hypothetical_indexes()
                return self.format_json_result({
                    "count": len(indexes),
                    "hypothetical_indexes": indexes
                })

            elif action == "drop":
                self.validate_required_args(arguments, ["index_id"])
                result = await self.hypopg_service.drop_hypothetical_index(
                    arguments["index_id"]
                )
                return self.format_json_result(result)

            elif action == "reset":
                result = await self.hypopg_service.reset_hypothetical_indexes()
                return self.format_json_result(result)

            elif action == "estimate_size":
                self.validate_required_args(arguments, ["table", "columns"])
                # Create temporarily to get size estimate
                create_result = await self.hypopg_service.create_hypothetical_index(
                    table=arguments["table"],
                    columns=arguments["columns"],
                    index_type=arguments.get("index_type", "btree"),
                    unique=arguments.get("unique", False)
                )

                if create_result.get("success"):
                    size = await self.hypopg_service.get_index_size(
                        create_result["index_oid"]
                    )
                    # Clean up
                    await self.hypopg_service.drop_hypothetical_index(
                        create_result["index_oid"]
                    )
                    return self.format_json_result({
                        "table": arguments["table"],
                        "columns": arguments["columns"],
                        "index_type": arguments.get("index_type", "btree"),
                        "estimated_size": size,
                        "estimated_size_bytes": create_result.get("estimated_size_bytes")
                    })
                else:
                    return self.format_json_result(create_result)

            else:
                return self.format_result(f"Unknown action: {action}")

        except Exception as e:
            return self.format_error(e)


class UnusedIndexesToolHandler(ToolHandler):
    """Tool handler for identifying unused or duplicate indexes."""

    name = "find_unused_indexes"
    title = "Unused Index Finder"
    read_only_hint = True
    destructive_hint = False
    idempotent_hint = True
    open_world_hint = False
    description = """Find indexes that are not being used or are duplicates.

Identifies:
- Indexes with zero or very few scans since last stats reset
- Duplicate indexes (same columns in same order)
- Overlapping indexes (one index is a prefix of another)

Removing unused indexes can:
- Reduce storage space
- Speed up INSERT/UPDATE/DELETE operations
- Reduce vacuum and maintenance overhead"""

    def __init__(self, sql_driver: SqlDriver):
        self.sql_driver = sql_driver

    def get_tool_definition(self) -> Tool:
        return Tool(
            name=self.name,
            description=self.description,
            inputSchema={
                "type": "object",
                "properties": {
                    "schema_name": {
                        "type": "string",
                        "description": "Schema to analyze (default: public)",
                        "default": "public"
                    },
                    "min_size_mb": {
                        "type": "number",
                        "description": "Minimum index size in MB to include",
                        "default": 0
                    },
                    "max_scan_ratio": {
                        "type": "number",
                        "description": "Maximum scan ratio (scans/rows) to consider an index unused",
                        "default": 0.01
                    },
                    "include_duplicates": {
                        "type": "boolean",
                        "description": "Include analysis of duplicate/overlapping indexes",
                        "default": True
                    }
                },
                "required": []
            },
            annotations=self.get_annotations()
        )

    async def run_tool(self, arguments: dict[str, Any]) -> Sequence[TextContent]:
        try:
            schema_name = arguments.get("schema_name", "public")
            min_size_mb = arguments.get("min_size_mb", 0)
            arguments.get("max_scan_ratio", 0.01)
            include_duplicates = arguments.get("include_duplicates", True)

            # Find unused indexes
            unused_query = """
                SELECT
                    s.schemaname,
                    s.relname as table_name,
                    s.indexrelname as index_name,
                    s.idx_scan as scans,
                    s.idx_tup_read as tuples_read,
                    s.idx_tup_fetch as tuples_fetched,
                    pg_size_pretty(pg_relation_size(s.indexrelid)) as size,
                    pg_relation_size(s.indexrelid) as size_bytes,
                    pg_get_indexdef(s.indexrelid) as definition,
                    t.n_live_tup as table_rows
                FROM pg_stat_user_indexes s
                JOIN pg_stat_user_tables t ON s.relid = t.relid
                WHERE s.schemaname = %s
                  AND pg_relation_size(s.indexrelid) >= %s * 1024 * 1024
                  AND s.idx_scan = 0
                  AND s.indexrelname NOT LIKE '%%_pkey'
                ORDER BY pg_relation_size(s.indexrelid) DESC
            """

            unused_results = await self.sql_driver.execute_query(
                unused_query,
                [schema_name, min_size_mb]
            )

            output = {
                "schema": schema_name,
                "unused_indexes": unused_results,
                "unused_count": len(unused_results),
                "potential_savings_bytes": sum(
                    r.get("size_bytes", 0) for r in unused_results
                )
            }

            # Find duplicate/overlapping indexes
            if include_duplicates:
                duplicate_query = """
                    WITH index_cols AS (
                        SELECT
                            n.nspname as schema_name,
                            t.relname as table_name,
                            i.relname as index_name,
                            pg_get_indexdef(i.oid) as definition,
                            array_agg(a.attname ORDER BY k.n) as columns,
                            pg_relation_size(i.oid) as size_bytes
                        FROM pg_index x
                        JOIN pg_class t ON t.oid = x.indrelid
                        JOIN pg_class i ON i.oid = x.indexrelid
                        JOIN pg_namespace n ON n.oid = t.relnamespace
                        CROSS JOIN unnest(x.indkey) WITH ORDINALITY AS k(attnum, n)
                        JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = k.attnum
                        WHERE n.nspname = %s
                        GROUP BY n.nspname, t.relname, i.relname, i.oid
                    )
                    SELECT
                        a.table_name,
                        a.index_name as index1,
                        a.columns as columns1,
                        a.definition as definition1,
                        a.size_bytes as size1,
                        b.index_name as index2,
                        b.columns as columns2,
                        b.definition as definition2,
                        b.size_bytes as size2,
                        CASE
                            WHEN a.columns = b.columns THEN 'duplicate'
                            WHEN a.columns[1:array_length(b.columns, 1)] = b.columns THEN 'overlapping'
                            ELSE 'related'
                        END as relationship
                    FROM index_cols a
                    JOIN index_cols b ON a.table_name = b.table_name
                        AND a.index_name < b.index_name
                    WHERE a.columns = b.columns
                       OR a.columns[1:array_length(b.columns, 1)] = b.columns
                """

                duplicate_results = await self.sql_driver.execute_query(
                    duplicate_query,
                    [schema_name]
                )

                output["duplicate_indexes"] = duplicate_results
                output["duplicate_count"] = len(duplicate_results)

            # Generate recommendations
            recommendations = []

            for idx in unused_results[:5]:
                size = idx.get("size", "unknown")
                recommendations.append(
                    f"DROP INDEX {schema_name}.{idx['index_name']}; -- {size}, 0 scans"
                )

            if include_duplicates:
                for dup in duplicate_results[:5]:
                    if dup["relationship"] == "duplicate":
                        # Recommend dropping the larger one
                        if dup["size1"] > dup["size2"]:
                            recommendations.append(
                                f"DROP INDEX {schema_name}.{dup['index1']}; -- duplicate of {dup['index2']}"
                            )
                        else:
                            recommendations.append(
                                f"DROP INDEX {schema_name}.{dup['index2']}; -- duplicate of {dup['index1']}"
                            )

            output["recommendations"] = recommendations

            return self.format_json_result(output)

        except Exception as e:
            return self.format_error(e)
