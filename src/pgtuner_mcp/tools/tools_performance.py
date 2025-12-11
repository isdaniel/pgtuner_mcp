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

            # Validate output_format against whitelist (defense in depth)
            valid_formats = {"text", "json", "yaml", "xml"}
            if output_format.lower() not in valid_formats:
                output_format = "json"

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


class DiskIOPatternToolHandler(ToolHandler):
    """Tool handler for analyzing disk I/O patterns and identifying bottlenecks."""

    name = "analyze_disk_io_patterns"
    title = "Disk I/O Pattern Analyzer"
    read_only_hint = True
    destructive_hint = False
    idempotent_hint = True
    open_world_hint = False
    description = """Analyze disk I/O read/write patterns in PostgreSQL.

Note: This tool focuses on user/client tables and excludes PostgreSQL
system tables (pg_catalog, information_schema, pg_toast) from analysis.

This tool provides comprehensive I/O analysis including:
- Buffer pool I/O statistics (hits vs reads)
- Table and index I/O patterns (sequential vs random reads)
- Backend vs background writer I/O distribution
- Temporary file I/O usage
- Checkpointer I/O statistics
- Per-table read/write hotspots

For PostgreSQL 16+, additional pg_stat_io metrics are available.

Use this to identify:
- Tables with high I/O activity (hot tables)
- I/O bottlenecks and cache inefficiencies
- Sequential scan heavy workloads
- Temporary file spills indicating work_mem issues"""

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
                    "include_indexes": {
                        "type": "boolean",
                        "description": "Include index I/O statistics",
                        "default": True
                    },
                    "top_n": {
                        "type": "integer",
                        "description": "Number of top tables to return by I/O activity",
                        "default": 20,
                        "minimum": 1,
                        "maximum": 100
                    },
                    "analysis_type": {
                        "type": "string",
                        "description": "Type of I/O analysis to perform",
                        "enum": ["all", "tables", "indexes", "buffer_pool", "temp_files", "checkpoints"],
                        "default": "all"
                    },
                    "min_size_gb": {
                        "type": "number",
                        "description": "Minimum table/index size in GB to include in analysis (default: 1)",
                        "default": 1,
                        "minimum": 0
                    }
                },
                "required": []
            },
            annotations=self.get_annotations()
        )

    async def run_tool(self, arguments: dict[str, Any]) -> Sequence[TextContent]:
        try:
            schema_name = arguments.get("schema_name", "public")
            include_indexes = arguments.get("include_indexes", True)
            top_n = arguments.get("top_n", 20)
            analysis_type = arguments.get("analysis_type", "all")
            min_size_gb = arguments.get("min_size_gb", 1)

            output: dict[str, Any] = {
                "schema": schema_name,
                "analysis_type": analysis_type,
                "io_patterns": {},
                "analysis": {
                    "issues": [],
                    "recommendations": []
                }
            }

            # Collect different types of I/O statistics based on analysis_type
            if analysis_type in ("all", "buffer_pool"):
                await self._analyze_buffer_pool(output)

            if analysis_type in ("all", "tables"):
                await self._analyze_table_io(output, schema_name, top_n, min_size_gb)

            if analysis_type in ("all", "indexes") and include_indexes:
                await self._analyze_index_io(output, schema_name, top_n, min_size_gb)

            if analysis_type in ("all", "temp_files"):
                await self._analyze_temp_files(output)

            if analysis_type in ("all", "checkpoints"):
                await self._analyze_checkpoint_io(output)

            # Check for pg_stat_io availability (PostgreSQL 16+)
            await self._analyze_pg_stat_io(output)

            # Generate summary and recommendations
            self._generate_io_recommendations(output)

            return self.format_json_result(output)

        except Exception as e:
            return self.format_error(e)

    async def _analyze_buffer_pool(self, output: dict[str, Any]) -> None:
        """Analyze buffer pool I/O statistics."""
        query = """
            SELECT
                sum(heap_blks_read) as heap_blocks_read,
                sum(heap_blks_hit) as heap_blocks_hit,
                sum(idx_blks_read) as index_blocks_read,
                sum(idx_blks_hit) as index_blocks_hit,
                sum(toast_blks_read) as toast_blocks_read,
                sum(toast_blks_hit) as toast_blocks_hit,
                sum(tidx_blks_read) as toast_index_blocks_read,
                sum(tidx_blks_hit) as toast_index_blocks_hit,
                CASE
                    WHEN sum(heap_blks_hit) + sum(heap_blks_read) > 0
                    THEN ROUND(100.0 * sum(heap_blks_hit) / (sum(heap_blks_hit) + sum(heap_blks_read)), 2)
                    ELSE 100
                END as heap_hit_ratio,
                CASE
                    WHEN sum(idx_blks_hit) + sum(idx_blks_read) > 0
                    THEN ROUND(100.0 * sum(idx_blks_hit) / (sum(idx_blks_hit) + sum(idx_blks_read)), 2)
                    ELSE 100
                END as index_hit_ratio
            FROM pg_statio_user_tables
        """
        result = await self.sql_driver.execute_query(query)

        if result:
            row = result[0]
            heap_hit = row.get("heap_hit_ratio") or 100
            idx_hit = row.get("index_hit_ratio") or 100

            output["io_patterns"]["buffer_pool"] = {
                "heap_blocks_read": row.get("heap_blocks_read") or 0,
                "heap_blocks_hit": row.get("heap_blocks_hit") or 0,
                "heap_hit_ratio": heap_hit,
                "index_blocks_read": row.get("index_blocks_read") or 0,
                "index_blocks_hit": row.get("index_blocks_hit") or 0,
                "index_hit_ratio": idx_hit,
                "toast_blocks_read": row.get("toast_blocks_read") or 0,
                "toast_blocks_hit": row.get("toast_blocks_hit") or 0
            }

            # Check for cache issues
            if heap_hit < 90:
                output["analysis"]["issues"].append(
                    f"Low heap buffer cache hit ratio: {heap_hit}%"
                )
                output["analysis"]["recommendations"].append(
                    "Consider increasing shared_buffers to improve cache hit ratio"
                )
            if idx_hit < 95:
                output["analysis"]["issues"].append(
                    f"Low index buffer cache hit ratio: {idx_hit}%"
                )
                output["analysis"]["recommendations"].append(
                    "Ensure frequently accessed indexes fit in buffer cache"
                )

    async def _analyze_table_io(self, output: dict[str, Any], schema_name: str, top_n: int, min_size_gb: float = 1) -> None:
        """Analyze table-level I/O patterns."""
        min_size_bytes = int(min_size_gb * 1024 * 1024 * 1024)
        query = """
            SELECT
                s.schemaname,
                s.relname as table_name,
                s.heap_blks_read,
                s.heap_blks_hit,
                CASE
                    WHEN s.heap_blks_hit + s.heap_blks_read > 0
                    THEN ROUND(100.0 * s.heap_blks_hit / (s.heap_blks_hit + s.heap_blks_read), 2)
                    ELSE 100
                END as heap_hit_ratio,
                s.idx_blks_read,
                s.idx_blks_hit,
                CASE
                    WHEN s.idx_blks_hit + s.idx_blks_read > 0
                    THEN ROUND(100.0 * s.idx_blks_hit / (s.idx_blks_hit + s.idx_blks_read), 2)
                    ELSE 100
                END as idx_hit_ratio,
                s.heap_blks_read + COALESCE(s.idx_blks_read, 0) as total_reads,
                s.heap_blks_hit + COALESCE(s.idx_blks_hit, 0) as total_hits,
                pg_total_relation_size(c.oid) as table_size_bytes
            FROM pg_statio_user_tables s
            JOIN pg_class c ON c.oid = s.relid
            JOIN pg_namespace n ON n.oid = c.relnamespace AND n.nspname = s.schemaname
            WHERE s.schemaname = %s
              AND s.schemaname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
              AND pg_total_relation_size(c.oid) >= %s
            ORDER BY (s.heap_blks_read + COALESCE(s.idx_blks_read, 0)) DESC
            LIMIT %s
        """
        result = await self.sql_driver.execute_query(query, [schema_name, min_size_bytes, top_n])

        # Also get sequential vs index scan patterns
        scan_query = """
            SELECT
                s.schemaname,
                s.relname as table_name,
                s.seq_scan,
                s.seq_tup_read,
                s.idx_scan,
                s.idx_tup_fetch,
                CASE
                    WHEN s.seq_scan + COALESCE(s.idx_scan, 0) > 0
                    THEN ROUND(100.0 * s.seq_scan / (s.seq_scan + COALESCE(s.idx_scan, 0)), 2)
                    ELSE 0
                END as seq_scan_ratio,
                s.n_live_tup,
                s.n_dead_tup
            FROM pg_stat_user_tables s
            JOIN pg_class c ON c.oid = s.relid
            JOIN pg_namespace n ON n.oid = c.relnamespace AND n.nspname = s.schemaname
            WHERE s.schemaname = %s
              AND s.schemaname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
              AND pg_total_relation_size(c.oid) >= %s
            ORDER BY s.seq_tup_read DESC
            LIMIT %s
        """
        scan_result = await self.sql_driver.execute_query(scan_query, [schema_name, min_size_bytes, top_n])

        # Combine results
        tables = []
        scan_data = {r["table_name"]: r for r in scan_result} if scan_result else {}

        if result:
            for row in result:
                table_name = row.get("table_name")
                scan_info = scan_data.get(table_name, {})

                tables.append({
                    "table_name": table_name,
                    "heap_blocks_read": row.get("heap_blks_read") or 0,
                    "heap_blocks_hit": row.get("heap_blks_hit") or 0,
                    "heap_hit_ratio": row.get("heap_hit_ratio") or 100,
                    "index_blocks_read": row.get("idx_blks_read") or 0,
                    "index_blocks_hit": row.get("idx_blks_hit") or 0,
                    "index_hit_ratio": row.get("idx_hit_ratio") or 100,
                    "total_physical_reads": row.get("total_reads") or 0,
                    "seq_scans": scan_info.get("seq_scan") or 0,
                    "seq_tuples_read": scan_info.get("seq_tup_read") or 0,
                    "idx_scans": scan_info.get("idx_scan") or 0,
                    "idx_tuples_fetched": scan_info.get("idx_tup_fetch") or 0,
                    "seq_scan_ratio": scan_info.get("seq_scan_ratio") or 0,
                    "live_tuples": scan_info.get("n_live_tup") or 0
                })

                # Identify hot tables with low cache hit
                heap_hit = row.get("heap_hit_ratio") or 100
                total_reads = row.get("total_reads") or 0
                if total_reads > 1000 and heap_hit < 85:
                    output["analysis"]["issues"].append(
                        f"Table '{table_name}' has high I/O ({total_reads} reads) with low cache hit ({heap_hit}%)"
                    )

                # Identify sequential scan heavy tables
                seq_ratio = scan_info.get("seq_scan_ratio") or 0
                live_tuples = scan_info.get("n_live_tup") or 0
                if seq_ratio > 80 and live_tuples > 10000:
                    output["analysis"]["issues"].append(
                        f"Table '{table_name}' has {seq_ratio}% sequential scans with {live_tuples} rows"
                    )
                    output["analysis"]["recommendations"].append(
                        f"Consider adding indexes to table '{table_name}' to reduce sequential scans"
                    )

        output["io_patterns"]["tables"] = {
            "count": len(tables),
            "top_tables_by_io": tables
        }

    async def _analyze_index_io(self, output: dict[str, Any], schema_name: str, top_n: int, min_size_gb: float = 1) -> None:
        """Analyze index-level I/O patterns."""
        min_size_bytes = int(min_size_gb * 1024 * 1024 * 1024)
        query = """
            SELECT
                s.schemaname,
                s.relname as table_name,
                s.indexrelname as index_name,
                s.idx_blks_read,
                s.idx_blks_hit,
                CASE
                    WHEN s.idx_blks_hit + s.idx_blks_read > 0
                    THEN ROUND(100.0 * s.idx_blks_hit / (s.idx_blks_hit + s.idx_blks_read), 2)
                    ELSE 100
                END as hit_ratio,
                pg_relation_size(s.indexrelid) as index_size_bytes
            FROM pg_statio_user_indexes s
            JOIN pg_class i ON i.oid = s.indexrelid
            JOIN pg_namespace n ON n.oid = i.relnamespace AND n.nspname = s.schemaname
            WHERE s.schemaname = %s
              AND s.schemaname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
              AND pg_relation_size(s.indexrelid) >= %s
            ORDER BY s.idx_blks_read DESC
            LIMIT %s
        """
        result = await self.sql_driver.execute_query(query, [schema_name, min_size_bytes, top_n])

        if result:
            indexes = []
            for row in result:
                indexes.append({
                    "index_name": row.get("index_name"),
                    "table_name": row.get("table_name"),
                    "blocks_read": row.get("idx_blks_read") or 0,
                    "blocks_hit": row.get("idx_blks_hit") or 0,
                    "hit_ratio": row.get("hit_ratio") or 100
                })

                # Check for indexes with poor cache hit
                hit_ratio = row.get("hit_ratio") or 100
                blocks_read = row.get("idx_blks_read") or 0
                if blocks_read > 1000 and hit_ratio < 90:
                    output["analysis"]["issues"].append(
                        f"Index '{row.get('index_name')}' has high I/O with low cache hit ({hit_ratio}%)"
                    )

            output["io_patterns"]["indexes"] = {
                "count": len(indexes),
                "top_indexes_by_io": indexes
            }

    async def _analyze_temp_files(self, output: dict[str, Any]) -> None:
        """Analyze temporary file I/O usage."""
        query = """
            SELECT
                datname,
                temp_files,
                temp_bytes,
                pg_size_pretty(temp_bytes) as temp_size_pretty
            FROM pg_stat_database
            WHERE datname = current_database()
        """
        result = await self.sql_driver.execute_query(query)

        if result:
            row = result[0]
            temp_files = row.get("temp_files") or 0
            temp_bytes = row.get("temp_bytes") or 0

            output["io_patterns"]["temp_files"] = {
                "temp_files_created": temp_files,
                "temp_bytes_written": temp_bytes,
                "temp_size_pretty": row.get("temp_size_pretty") or "0 bytes"
            }

            # Check for excessive temp file usage
            if temp_files > 1000:
                output["analysis"]["issues"].append(
                    f"High temporary file usage: {temp_files} files created"
                )
                output["analysis"]["recommendations"].append(
                    "Consider increasing work_mem to reduce temporary file spills"
                )
            if temp_bytes > 1024 * 1024 * 1024:  # > 1GB
                output["analysis"]["issues"].append(
                    f"Large temporary file I/O: {row.get('temp_size_pretty')}"
                )
                output["analysis"]["recommendations"].append(
                    "Review queries using sorts, hashes, or CTEs that may spill to disk"
                )

    async def _analyze_checkpoint_io(self, output: dict[str, Any]) -> None:
        """Analyze checkpoint and background writer I/O."""
        query = """
            SELECT
                checkpoints_timed,
                checkpoints_req,
                checkpoint_write_time,
                checkpoint_sync_time,
                buffers_checkpoint,
                buffers_clean,
                buffers_backend,
                buffers_backend_fsync,
                buffers_alloc,
                CASE
                    WHEN buffers_checkpoint + buffers_clean + buffers_backend > 0
                    THEN ROUND(100.0 * buffers_backend / (buffers_checkpoint + buffers_clean + buffers_backend), 2)
                    ELSE 0
                END as backend_write_ratio,
                stats_reset
            FROM pg_stat_bgwriter
        """
        result = await self.sql_driver.execute_query(query)

        if result:
            row = result[0]
            backend_ratio = row.get("backend_write_ratio") or 0
            buffers_backend_fsync = row.get("buffers_backend_fsync") or 0
            checkpoints_req = row.get("checkpoints_req") or 0
            checkpoints_timed = row.get("checkpoints_timed") or 0

            output["io_patterns"]["checkpoints"] = {
                "checkpoints_timed": checkpoints_timed,
                "checkpoints_requested": checkpoints_req,
                "checkpoint_write_time_ms": row.get("checkpoint_write_time") or 0,
                "checkpoint_sync_time_ms": row.get("checkpoint_sync_time") or 0,
                "buffers_written_by_checkpoint": row.get("buffers_checkpoint") or 0,
                "buffers_written_by_bgwriter": row.get("buffers_clean") or 0,
                "buffers_written_by_backend": row.get("buffers_backend") or 0,
                "backend_fsync_count": buffers_backend_fsync,
                "buffers_allocated": row.get("buffers_alloc") or 0,
                "backend_write_ratio": backend_ratio,
                "stats_reset": str(row.get("stats_reset")) if row.get("stats_reset") else None
            }

            # Check for backend doing too much writing
            if backend_ratio > 20:
                output["analysis"]["issues"].append(
                    f"Backend processes writing {backend_ratio}% of buffers (should be near 0)"
                )
                output["analysis"]["recommendations"].append(
                    "Increase shared_buffers and bgwriter_lru_maxpages to reduce backend writes"
                )

            # Check for backend fsyncs (very bad for performance)
            if buffers_backend_fsync > 0:
                output["analysis"]["issues"].append(
                    f"Backend processes performed {buffers_backend_fsync} fsync calls (should be 0)"
                )
                output["analysis"]["recommendations"].append(
                    "This indicates severe I/O performance issues - check storage and increase checkpointing"
                )

            # Check for too many requested checkpoints
            total_checkpoints = checkpoints_timed + checkpoints_req
            if total_checkpoints > 0 and checkpoints_req > checkpoints_timed:
                output["analysis"]["issues"].append(
                    f"More requested checkpoints ({checkpoints_req}) than timed ({checkpoints_timed})"
                )
                output["analysis"]["recommendations"].append(
                    "Increase max_wal_size and checkpoint_timeout to reduce checkpoint frequency"
                )

    async def _analyze_pg_stat_io(self, output: dict[str, Any]) -> None:
        """Analyze pg_stat_io for PostgreSQL 16+ detailed I/O statistics."""
        # Check if pg_stat_io exists (PostgreSQL 16+)
        check_query = """
            SELECT EXISTS (
                SELECT 1 FROM pg_catalog.pg_class c
                JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
                WHERE n.nspname = 'pg_catalog' AND c.relname = 'pg_stat_io'
            ) as available
        """
        check_result = await self.sql_driver.execute_query(check_query)

        if not check_result or not check_result[0].get("available"):
            output["io_patterns"]["pg_stat_io"] = {
                "available": False,
                "message": "pg_stat_io is available in PostgreSQL 16+ for more detailed I/O statistics"
            }
            return

        # Query pg_stat_io for detailed breakdown
        query = """
            SELECT
                backend_type,
                object,
                context,
                reads,
                read_time,
                writes,
                write_time,
                writebacks,
                writeback_time,
                extends,
                extend_time,
                hits,
                evictions,
                reuses,
                fsyncs,
                fsync_time
            FROM pg_stat_io
            WHERE reads > 0 OR writes > 0
            ORDER BY reads + writes DESC
            LIMIT 20
        """
        result = await self.sql_driver.execute_query(query)

        if result:
            output["io_patterns"]["pg_stat_io"] = {
                "available": True,
                "io_by_backend_and_object": result
            }

            # Analyze for issues
            for row in result:
                backend = row.get("backend_type", "unknown")
                reads = row.get("reads") or 0
                writes = row.get("writes") or 0
                read_time = row.get("read_time") or 0
                write_time = row.get("write_time") or 0

                # Check for slow I/O
                if reads > 0 and read_time / reads > 10:  # > 10ms average
                    avg_time = read_time / reads
                    output["analysis"]["issues"].append(
                        f"Slow reads for {backend}: {avg_time:.2f}ms average"
                    )

                if writes > 0 and write_time / writes > 10:  # > 10ms average
                    avg_time = write_time / writes
                    output["analysis"]["issues"].append(
                        f"Slow writes for {backend}: {avg_time:.2f}ms average"
                    )

    def _generate_io_recommendations(self, output: dict[str, Any]) -> None:
        """Generate overall I/O recommendations based on analysis."""
        issues = output["analysis"]["issues"]

        # Remove duplicates while preserving order
        seen_issues = set()
        unique_issues = []
        for issue in issues:
            if issue not in seen_issues:
                seen_issues.add(issue)
                unique_issues.append(issue)
        output["analysis"]["issues"] = unique_issues

        # Remove duplicate recommendations
        seen_recs = set()
        unique_recs = []
        for rec in output["analysis"]["recommendations"]:
            if rec not in seen_recs:
                seen_recs.add(rec)
                unique_recs.append(rec)
        output["analysis"]["recommendations"] = unique_recs

        # Add summary
        io_patterns = output["io_patterns"]
        summary = {
            "total_issues": len(unique_issues),
            "total_recommendations": len(unique_recs)
        }

        if "buffer_pool" in io_patterns:
            bp = io_patterns["buffer_pool"]
            summary["heap_cache_hit_ratio"] = bp.get("heap_hit_ratio")
            summary["index_cache_hit_ratio"] = bp.get("index_hit_ratio")

        if "temp_files" in io_patterns:
            tf = io_patterns["temp_files"]
            summary["temp_files_created"] = tf.get("temp_files_created")

        if "checkpoints" in io_patterns:
            cp = io_patterns["checkpoints"]
            summary["backend_write_ratio"] = cp.get("backend_write_ratio")

        output["summary"] = summary
