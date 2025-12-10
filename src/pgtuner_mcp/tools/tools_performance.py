"""Performance analysis tool handlers."""

from __future__ import annotations

import json
from collections.abc import Sequence
from typing import Any

from mcp.types import TextContent, Tool

from ..services import SqlDriver, get_user_filter
from .toolhandler import ToolHandler

class GetSlowQueriesToolHandler(ToolHandler):
    """Tool handler for retrieving slow queries from pg_stat_statements."""

    name = "get_slow_queries"
    title = "Slow Query Analyzer"
    read_only_hint = True
    destructive_hint = False
    idempotent_hint = True
    open_world_hint = False
    description = """Retrieve slow queries from PostgreSQL using pg_stat_statements.

Returns the top N slowest queries ordered by mean (average) execution time.
Requires the pg_stat_statements extension to be enabled.

Note: This tool focuses on user/application queries only. System catalog
queries (pg_catalog, information_schema, pg_toast) are automatically excluded.

The results include:
- Query text (normalized)
- Number of calls
- Mean execution time (average per call)
- Min/Max execution time
- Rows returned
- Shared buffer hits/reads for cache analysis"""

    def __init__(self, sql_driver: SqlDriver):
        self.sql_driver = sql_driver

    def get_tool_definition(self) -> Tool:
        return Tool(
            name=self.name,
            description=self.description,
            inputSchema={
                "type": "object",
                "properties": {
                    "limit": {
                        "type": "integer",
                        "description": "Maximum number of slow queries to return (default: 10)",
                        "default": 10,
                        "minimum": 1,
                        "maximum": 100
                    },
                    "min_calls": {
                        "type": "integer",
                        "description": "Minimum number of calls for a query to be included (default: 1)",
                        "default": 1,
                        "minimum": 1
                    },
                    "min_mean_time_ms": {
                        "type": "number",
                        "description": "Minimum mean (average) execution time in milliseconds (default: 0)",
                        "default": 0
                    },
                    "order_by": {
                        "type": "string",
                        "description": "Column to order results by",
                        "enum": ["mean_time", "calls", "rows"],
                        "default": "mean_time"
                    }
                },
                "required": []
            },
            annotations=self.get_annotations()
        )

    async def run_tool(self, arguments: dict[str, Any]) -> Sequence[TextContent]:
        try:
            limit = arguments.get("limit", 10)
            min_calls = arguments.get("min_calls", 1)
            min_mean_time_ms = arguments.get("min_mean_time_ms", 0)
            order_by = arguments.get("order_by", "mean_time")

            # Map order_by to actual column names (whitelist for SQL injection protection)
            order_map = {
                "mean_time": "mean_exec_time",
                "calls": "calls",
                "rows": "rows"
            }
            # Validate order_by against whitelist to prevent SQL injection
            if order_by not in order_map:
                order_by = "mean_time"
            order_column = order_map[order_by]

            # Check if pg_stat_statements is available
            check_query = """
                SELECT EXISTS (
                    SELECT 1 FROM pg_extension WHERE extname = 'pg_stat_statements'
                ) as available
            """
            check_result = await self.sql_driver.execute_query(check_query)

            if not check_result or not check_result[0].get("available"):
                return self.format_result(
                    "Error: pg_stat_statements extension is not installed.\n"
                    "Install it with: CREATE EXTENSION pg_stat_statements;\n"
                    "Note: You may need to add it to shared_preload_libraries in postgresql.conf"
                )

            # Get user filter for excluding specific user IDs
            user_filter = get_user_filter()
            statements_filter = user_filter.get_statements_filter()

            # Query pg_stat_statements for slow queries
            # Using pg_stat_statements columns available in PostgreSQL 13+
            # Excludes system catalog queries to focus on user/application queries
            query = f"""
                SELECT
                    queryid,
                    LEFT(query, 500) as query_text,
                    calls,
                    ROUND(mean_exec_time::numeric, 2) as mean_time_ms,
                    ROUND(min_exec_time::numeric, 2) as min_time_ms,
                    ROUND(max_exec_time::numeric, 2) as max_time_ms,
                    ROUND(stddev_exec_time::numeric, 2) as stddev_time_ms,
                    rows,
                    shared_blks_hit,
                    shared_blks_read,
                    CASE
                        WHEN shared_blks_hit + shared_blks_read > 0
                        THEN ROUND(100.0 * shared_blks_hit / (shared_blks_hit + shared_blks_read), 2)
                        ELSE 100
                    END as cache_hit_ratio,
                    temp_blks_read,
                    temp_blks_written
                FROM pg_stat_statements
                WHERE calls >= %s
                  AND mean_exec_time >= %s
                  AND query NOT LIKE '%%pg_stat_statements%%'
                  AND query NOT LIKE '%%pg_catalog%%'
                  AND query NOT LIKE '%%information_schema%%'
                  AND query NOT LIKE '%%pg_toast%%'
                  {statements_filter}
                ORDER BY {order_column} DESC
                LIMIT %s
            """

            results = await self.sql_driver.execute_query(
                query,
                [min_calls, min_mean_time_ms, limit]
            )

            if not results:
                return self.format_result(
                    "No slow queries found matching the criteria.\n"
                    "This could mean:\n"
                    "- pg_stat_statements has been recently reset\n"
                    "- No queries exceed the minimum thresholds\n"
                    "- The database has low query activity"
                )

            # Format results
            output = {
                "summary": {
                    "total_queries_returned": len(results),
                    "filters_applied": {
                        "min_calls": min_calls,
                        "min_mean_time_ms": min_mean_time_ms,
                        "order_by": order_by
                    }
                },
                "slow_queries": results
            }

            return self.format_json_result(output)

        except Exception as e:
            return self.format_error(e)


class AnalyzeQueryToolHandler(ToolHandler):
    """Tool handler for analyzing a query's execution plan and performance."""

    name = "analyze_query"
    title = "Query Execution Analyzer"
    read_only_hint = False  # EXPLAIN ANALYZE actually executes the query
    destructive_hint = False  # Read queries are safe, but DML could be destructive
    idempotent_hint = True
    open_world_hint = False
    description = """Analyze a SQL query's execution plan and performance characteristics.

Uses EXPLAIN ANALYZE to execute the query and capture detailed timing information.
Provides analysis of:
- Execution plan with actual vs estimated rows
- Timing breakdown by operation
- Buffer usage and I/O statistics
- Potential performance issues and recommendations

WARNING: This actually executes the query! For SELECT queries this is safe,
but be careful with INSERT/UPDATE/DELETE - use analyze_only=false for those."""

    def __init__(self, sql_driver: SqlDriver):
        self.sql_driver = sql_driver

    def get_tool_definition(self) -> Tool:
        return Tool(
            name=self.name,
            description=self.description,
            inputSchema={
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": "The SQL query to analyze"
                    },
                    "analyze": {
                        "type": "boolean",
                        "description": "Whether to actually execute the query (EXPLAIN ANALYZE vs EXPLAIN)",
                        "default": True
                    },
                    "buffers": {
                        "type": "boolean",
                        "description": "Include buffer usage statistics",
                        "default": True
                    },
                    "verbose": {
                        "type": "boolean",
                        "description": "Include verbose output with additional details",
                        "default": False
                    },
                    "format": {
                        "type": "string",
                        "description": "Output format for the execution plan",
                        "enum": ["text", "json", "yaml", "xml"],
                        "default": "json"
                    },
                    "settings": {
                        "type": "boolean",
                        "description": "Include information about configuration parameters",
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
            analyze = arguments.get("analyze", True)
            buffers = arguments.get("buffers", True)
            verbose = arguments.get("verbose", False)
            output_format = arguments.get("format", "json")
            settings = arguments.get("settings", False)

            # Build EXPLAIN options
            options = []
            if analyze:
                options.append("ANALYZE")
            if buffers:
                options.append("BUFFERS")
            if verbose:
                options.append("VERBOSE")
            if settings:
                options.append("SETTINGS")
            options.append(f"FORMAT {output_format.upper()}")

            options_str = ", ".join(options)
            explain_query = f"EXPLAIN ({options_str}) {query}"

            # Execute EXPLAIN
            results = await self.sql_driver.execute_query(explain_query)

            if not results:
                return self.format_result("No execution plan returned")

            # For JSON format, parse and analyze the plan
            if output_format == "json":
                # The result comes as a list with QUERY PLAN column
                plan_data = results[0].get("QUERY PLAN", results)

                # If it's a string, parse it
                if isinstance(plan_data, str):
                    plan_data = json.loads(plan_data)

                analysis = self._analyze_plan(plan_data, analyze)

                output = {
                    "query": query,
                    "explain_options": {
                        "analyze": analyze,
                        "buffers": buffers,
                        "verbose": verbose,
                        "format": output_format
                    },
                    "execution_plan": plan_data,
                    "analysis": analysis
                }

                return self.format_json_result(output)
            else:
                # For text/yaml/xml, return as-is
                plan_text = "\n".join(
                    str(row.get("QUERY PLAN", row))
                    for row in results
                )
                return self.format_result(f"Query: {query}\n\nExecution Plan:\n{plan_text}")

        except Exception as e:
            return self.format_error(e)

    def _analyze_plan(self, plan_data: Any, was_analyzed: bool) -> dict[str, Any]:
        """Analyze an execution plan and extract insights."""
        analysis = {
            "warnings": [],
            "recommendations": [],
            "statistics": {}
        }

        if not plan_data:
            return analysis

        # Handle the plan structure (it's usually a list with one element)
        if isinstance(plan_data, list) and len(plan_data) > 0:
            plan = plan_data[0].get("Plan", plan_data[0])
        else:
            plan = plan_data.get("Plan", plan_data)

        # Extract top-level statistics
        if was_analyzed:
            if "Execution Time" in plan_data[0] if isinstance(plan_data, list) else plan_data:
                exec_time = (plan_data[0] if isinstance(plan_data, list) else plan_data).get("Execution Time", 0)
                analysis["statistics"]["execution_time_ms"] = exec_time

            if "Planning Time" in (plan_data[0] if isinstance(plan_data, list) else plan_data):
                plan_time = (plan_data[0] if isinstance(plan_data, list) else plan_data).get("Planning Time", 0)
                analysis["statistics"]["planning_time_ms"] = plan_time

        # Analyze the plan recursively
        self._analyze_node(plan, analysis)

        return analysis

    def _analyze_node(self, node: dict[str, Any], analysis: dict[str, Any], depth: int = 0) -> None:
        """Recursively analyze plan nodes for issues."""
        if not isinstance(node, dict):
            return

        node_type = node.get("Node Type", "Unknown")

        # Check for sequential scans on large tables
        if node_type == "Seq Scan":
            rows = node.get("Actual Rows", node.get("Plan Rows", 0))
            if rows > 10000:
                table = node.get("Relation Name", "unknown")
                analysis["warnings"].append(
                    f"Sequential scan on '{table}' returned {rows} rows - consider adding an index"
                )
                filter_cond = node.get("Filter")
                if filter_cond:
                    analysis["recommendations"].append(
                        f"Consider creating an index for filter condition: {filter_cond}"
                    )

        # Check for row estimate mismatches
        actual_rows = node.get("Actual Rows")
        plan_rows = node.get("Plan Rows")
        if actual_rows is not None and plan_rows is not None and plan_rows > 0:
            ratio = actual_rows / plan_rows
            if ratio > 10 or ratio < 0.1:
                analysis["warnings"].append(
                    f"{node_type}: Row estimate mismatch - planned {plan_rows}, actual {actual_rows} "
                    f"(ratio: {ratio:.2f}). Consider running ANALYZE on the table."
                )

        # Check for hash operations with high memory usage
        if "Hash" in node_type:
            batches = node.get("Hash Batches", 1)
            if batches > 1:
                analysis["warnings"].append(
                    f"{node_type} spilled to disk ({batches} batches). "
                    "Consider increasing work_mem or optimizing the query."
                )

        # Check for sorts that spill to disk
        if node_type == "Sort":
            sort_method = node.get("Sort Method", "")
            if "external" in sort_method.lower():
                analysis["warnings"].append(
                    f"Sort operation spilled to disk ({sort_method}). "
                    "Consider increasing work_mem."
                )

        # Check for nested loops with many iterations
        if node_type == "Nested Loop":
            actual_loops = node.get("Actual Loops", 1)
            if actual_loops > 1000:
                analysis["warnings"].append(
                    f"Nested Loop executed {actual_loops} times - consider using a different join strategy"
                )

        # Recursively analyze child nodes
        for child in node.get("Plans", []):
            self._analyze_node(child, analysis, depth + 1)


class TableStatsToolHandler(ToolHandler):
    """Tool handler for retrieving table statistics and health metrics."""

    name = "get_table_stats"
    title = "Table Statistics Analyzer"
    read_only_hint = True
    destructive_hint = False
    idempotent_hint = True
    open_world_hint = False
    description = """Get detailed statistics for user/client database tables.

Note: This tool analyzes only user-created tables and excludes PostgreSQL
system tables (pg_catalog, information_schema, pg_toast). This focuses
the analysis on your application's custom tables.

Returns information about:
- Table size (data, indexes, total)
- Row counts and dead tuple ratio
- Last vacuum and analyze times
- Sequential vs index scan ratios
- Cache hit ratios

This helps identify tables that may need maintenance (VACUUM, ANALYZE)
or have performance issues."""

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
                    "table_name": {
                        "type": "string",
                        "description": "Specific table to analyze (optional, analyzes all tables if not provided)"
                    },
                    "include_indexes": {
                        "type": "boolean",
                        "description": "Include index statistics",
                        "default": True
                    },
                    "order_by": {
                        "type": "string",
                        "description": "Order results by this metric",
                        "enum": ["size", "rows", "dead_tuples", "seq_scans", "last_vacuum"],
                        "default": "size"
                    }
                },
                "required": []
            },
            annotations=self.get_annotations()
        )

    async def run_tool(self, arguments: dict[str, Any]) -> Sequence[TextContent]:
        try:
            schema_name = arguments.get("schema_name", "public")
            table_name = arguments.get("table_name")
            include_indexes = arguments.get("include_indexes", True)
            order_by = arguments.get("order_by", "size")

            # Build the query with whitelist-validated order clause
            order_map = {
                "size": "total_size DESC",
                "rows": "n_live_tup DESC",
                "dead_tuples": "n_dead_tup DESC",
                "seq_scans": "seq_scan DESC",
                "last_vacuum": "last_vacuum DESC NULLS LAST"
            }
            # Validate order_by against whitelist to prevent SQL injection
            if order_by not in order_map:
                order_by = "size"
            order_clause = order_map[order_by]

            table_filter = ""
            params = [schema_name]
            if table_name:
                table_filter = "AND c.relname ILIKE %s"
                params.append(table_name)

            # Query only user tables, explicitly excluding system schemas
            query = f"""
                SELECT
                    c.relname as table_name,
                    n.nspname as schema_name,
                    pg_size_pretty(pg_table_size(c.oid)) as table_size,
                    pg_size_pretty(pg_indexes_size(c.oid)) as indexes_size,
                    pg_size_pretty(pg_total_relation_size(c.oid)) as total_size,
                    pg_total_relation_size(c.oid) as total_size_bytes,
                    s.n_live_tup,
                    s.n_dead_tup,
                    CASE
                        WHEN s.n_live_tup > 0
                        THEN ROUND(100.0 * s.n_dead_tup / s.n_live_tup, 2)
                        ELSE 0
                    END as dead_tuple_ratio,
                    s.seq_scan,
                    s.seq_tup_read,
                    s.idx_scan,
                    s.idx_tup_fetch,
                    CASE
                        WHEN s.seq_scan + COALESCE(s.idx_scan, 0) > 0
                        THEN ROUND(100.0 * COALESCE(s.idx_scan, 0) / (s.seq_scan + COALESCE(s.idx_scan, 0)), 2)
                        ELSE 0
                    END as index_scan_ratio,
                    s.last_vacuum,
                    s.last_autovacuum,
                    s.last_analyze,
                    s.last_autoanalyze,
                    s.vacuum_count,
                    s.autovacuum_count,
                    s.analyze_count,
                    s.autoanalyze_count
                FROM pg_class c
                JOIN pg_namespace n ON n.oid = c.relnamespace
                LEFT JOIN pg_stat_user_tables s ON s.relid = c.oid
                WHERE c.relkind = 'r'
                  AND n.nspname = %s
                  AND n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
                  {table_filter}
                ORDER BY {order_clause}
            """

            results = await self.sql_driver.execute_query(query, params)

            if not results:
                return self.format_result(f"No tables found in schema '{schema_name}'")

            output = {
                "schema": schema_name,
                "table_count": len(results),
                "tables": results
            }

            # Add index statistics if requested
            if include_indexes and table_name:
                index_query = """
                    SELECT
                        i.indexrelname as index_name,
                        i.idx_scan as scans,
                        i.idx_tup_read as tuples_read,
                        i.idx_tup_fetch as tuples_fetched,
                        pg_size_pretty(pg_relation_size(i.indexrelid)) as size,
                        pg_relation_size(i.indexrelid) as size_bytes,
                        pg_get_indexdef(i.indexrelid) as definition
                    FROM pg_stat_user_indexes i
                    JOIN pg_class c ON c.oid = i.relid
                    JOIN pg_namespace n ON n.oid = c.relnamespace
                    WHERE n.nspname = %s AND c.relname = %s
                    ORDER BY i.idx_scan DESC
                """
                index_results = await self.sql_driver.execute_query(
                    index_query,
                    [schema_name, table_name]
                )
                output["indexes"] = index_results

            # Add analysis and recommendations
            output["analysis"] = self._analyze_stats(results)

            return self.format_json_result(output)

        except Exception as e:
            return self.format_error(e)

    def _analyze_stats(self, tables: list[dict]) -> dict[str, Any]:
        """Analyze table stats and generate recommendations."""
        analysis = {
            "needs_vacuum": [],
            "needs_analyze": [],
            "low_index_usage": [],
            "recommendations": []
        }

        for table in tables:
            table_name = table.get("table_name", "unknown")

            # Check dead tuple ratio
            dead_ratio = table.get("dead_tuple_ratio", 0) or 0
            if dead_ratio > 10:
                analysis["needs_vacuum"].append({
                    "table": table_name,
                    "dead_tuple_ratio": dead_ratio,
                    "dead_tuples": table.get("n_dead_tup", 0)
                })

            # Check if analyze is needed
            last_analyze = table.get("last_analyze") or table.get("last_autoanalyze")
            n_live = table.get("n_live_tup", 0) or 0
            if n_live > 1000 and not last_analyze:
                analysis["needs_analyze"].append(table_name)

            # Check index usage
            idx_ratio = table.get("index_scan_ratio", 0) or 0
            seq_scans = table.get("seq_scan", 0) or 0
            if seq_scans > 100 and idx_ratio < 50 and n_live > 10000:
                analysis["low_index_usage"].append({
                    "table": table_name,
                    "index_scan_ratio": idx_ratio,
                    "seq_scans": seq_scans,
                    "rows": n_live
                })

        # Generate recommendations
        if analysis["needs_vacuum"]:
            tables_list = ", ".join(t["table"] for t in analysis["needs_vacuum"][:5])
            analysis["recommendations"].append(
                f"Run VACUUM on tables with high dead tuple ratios: {tables_list}"
            )

        if analysis["needs_analyze"]:
            tables_list = ", ".join(analysis["needs_analyze"][:5])
            analysis["recommendations"].append(
                f"Run ANALYZE on tables that haven't been analyzed: {tables_list}"
            )

        if analysis["low_index_usage"]:
            for item in analysis["low_index_usage"][:3]:
                analysis["recommendations"].append(
                    f"Table '{item['table']}' has low index usage ({item['index_scan_ratio']}% index scans). "
                    "Consider adding indexes for frequently filtered columns."
                )

        return analysis
