"""Bloat detection and analysis tool handlers using pgstattuple extension."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

from mcp.types import TextContent, Tool

from ..services import SqlDriver
from .toolhandler import ToolHandler



class TableBloatToolHandler(ToolHandler):
    """Tool handler for analyzing table bloat using pgstattuple."""

    name = "analyze_table_bloat"
    title = "Table Bloat Analyzer"
    read_only_hint = True
    destructive_hint = False
    idempotent_hint = True
    open_world_hint = False
    description = """Analyze table bloat using the pgstattuple extension.

Note: This tool analyzes only user/client tables and excludes PostgreSQL
system tables (pg_catalog, information_schema, pg_toast). This focuses
the analysis on your application's custom tables.

Uses pgstattuple to get accurate tuple-level statistics including:
- Dead tuple count and percentage
- Free space within the table
- Physical vs logical table size

This helps identify tables that:
- Need VACUUM to reclaim space
- Need VACUUM FULL to reclaim disk space
- Have high bloat affecting performance

Requires the pgstattuple extension to be installed:
CREATE EXTENSION IF NOT EXISTS pgstattuple;

Note: pgstattuple performs a full table scan, so use with caution on large tables.
For large tables, consider using pgstattuple_approx instead (use_approx=true)."""

    def __init__(self, sql_driver: SqlDriver):
        self.sql_driver = sql_driver

    def get_tool_definition(self) -> Tool:
        return Tool(
            name=self.name,
            description=self.description,
            inputSchema={
                "type": "object",
                "properties": {
                    "table_name": {
                        "type": "string",
                        "description": "Name of the table to analyze (required if not using schema-wide scan)"
                    },
                    "schema_name": {
                        "type": "string",
                        "description": "Schema name (default: public)",
                        "default": "public"
                    },
                    "use_approx": {
                        "type": "boolean",
                        "description": "Use pgstattuple_approx for faster but approximate results (recommended for large tables)",
                        "default": False
                    },
                    "min_table_size_gb": {
                        "type": "number",
                        "description": "Minimum table size in GB to include in schema-wide scan (default: 5)",
                        "default": 5
                    },
                    "include_toast": {
                        "type": "boolean",
                        "description": "Include TOAST table analysis if applicable",
                        "default": False
                    }
                },
                "required": []
            },
            annotations=self.get_annotations()
        )

    async def run_tool(self, arguments: dict[str, Any]) -> Sequence[TextContent]:
        try:
            table_name = arguments.get("table_name")
            schema_name = arguments.get("schema_name", "public")
            use_approx = arguments.get("use_approx", False)
            min_size_gb = arguments.get("min_table_size_gb", 5)
            include_toast = arguments.get("include_toast", False)

            # Check if pgstattuple extension is available
            ext_check = await self._check_extension()
            if not ext_check["available"]:
                return self.format_result(
                    "Error: pgstattuple extension is not installed.\n"
                    "Install it with: CREATE EXTENSION IF NOT EXISTS pgstattuple;\n\n"
                    "Note: You may need superuser privileges or the pg_stat_scan_tables role."
                )

            if table_name:
                # Analyze specific table
                result = await self._analyze_single_table(
                    schema_name, table_name, use_approx, include_toast
                )
            else:
                # Analyze all tables in schema
                result = await self._analyze_schema_tables(
                    schema_name, use_approx, min_size_gb
                )

            return self.format_json_result(result)

        except Exception as e:
            return self.format_error(e)

    async def _check_extension(self) -> dict[str, Any]:
        """Check if pgstattuple extension is available."""
        query = """
            SELECT EXISTS (
                SELECT 1 FROM pg_extension WHERE extname = 'pgstattuple'
            ) as available,
            (SELECT extversion FROM pg_extension WHERE extname = 'pgstattuple') as version
        """
        result = await self.sql_driver.execute_query(query)
        if result:
            return {
                "available": result[0].get("available", False),
                "version": result[0].get("version")
            }
        return {"available": False, "version": None}

    async def _analyze_single_table(
        self,
        schema_name: str,
        table_name: str,
        use_approx: bool,
        include_toast: bool
    ) -> dict[str, Any]:
        """Analyze bloat for a single table."""

        # Get table size first
        size_query = """
            SELECT
                pg_total_relation_size(quote_ident(%s) || '.' || quote_ident(%s)) as total_size,
                pg_table_size(quote_ident(%s) || '.' || quote_ident(%s)) as table_size,
                pg_indexes_size(quote_ident(%s) || '.' || quote_ident(%s)) as indexes_size
        """
        size_result = await self.sql_driver.execute_query(
            size_query,
            (schema_name, table_name, schema_name, table_name, schema_name, table_name)
        )

        table_size = size_result[0] if size_result else {}

        # Use pgstattuple or pgstattuple_approx
        if use_approx:
            stats_query = """
                SELECT * FROM pgstattuple_approx(quote_ident(%s) || '.' || quote_ident(%s))
            """
        else:
            stats_query = """
                SELECT * FROM pgstattuple(quote_ident(%s) || '.' || quote_ident(%s))
            """

        stats_result = await self.sql_driver.execute_query(
            stats_query, (schema_name, table_name)
        )

        if not stats_result:
            return {
                "error": f"Could not analyze table {schema_name}.{table_name}",
                "hint": "Make sure the table exists and you have permissions to access it"
            }

        stats = stats_result[0]

        # Build result based on whether we used approx or exact
        if use_approx:
            result = self._build_approx_result(schema_name, table_name, stats, table_size)
        else:
            result = self._build_exact_result(schema_name, table_name, stats, table_size)

        # Add recommendations
        result["recommendations"] = self._generate_recommendations(result)

        # Analyze TOAST table if requested
        if include_toast:
            toast_result = await self._analyze_toast_table(schema_name, table_name, use_approx)
            if toast_result:
                result["toast_table"] = toast_result

        return result

    def _build_exact_result(
        self,
        schema_name: str,
        table_name: str,
        stats: dict,
        table_size: dict
    ) -> dict[str, Any]:
        """Build result from exact pgstattuple output."""
        table_len = stats.get("table_len", 0) or 0
        tuple_len = stats.get("tuple_len", 0) or 0
        dead_tuple_len = stats.get("dead_tuple_len", 0) or 0
        free_space = stats.get("free_space", 0) or 0

        # Get percentages directly from pgstattuple output
        tuple_percent = stats.get("tuple_percent", 0) or 0
        dead_tuple_percent = stats.get("dead_tuple_percent", 0) or 0
        free_percent = stats.get("free_percent", 0) or 0

        # Calculate wasted space (dead tuples + free space)
        wasted_space = dead_tuple_len + free_space
        wasted_percent = round(100.0 * wasted_space / table_len, 2) if table_len > 0 else 0

        # Get comprehensive bloat analysis based on the three key rules
        bloat_analysis = self._get_bloat_severity(dead_tuple_percent, free_percent, tuple_percent)

        return {
            "schema": schema_name,
            "table_name": table_name,
            "analysis_type": "exact",
            "size": {
                "table_len_bytes": table_len,
                "table_len_pretty": self._format_bytes(table_len),
                "total_relation_size": table_size.get("total_size"),
                "total_relation_size_pretty": self._format_bytes(table_size.get("total_size", 0)),
                "indexes_size": table_size.get("indexes_size"),
                "indexes_size_pretty": self._format_bytes(table_size.get("indexes_size", 0))
            },
            "tuples": {
                "live_tuple_count": stats.get("tuple_count", 0),
                "live_tuple_len": tuple_len,
                "live_tuple_percent": tuple_percent,
                "dead_tuple_count": stats.get("dead_tuple_count", 0),
                "dead_tuple_len": dead_tuple_len,
                "dead_tuple_percent": dead_tuple_percent
            },
            "free_space": {
                "free_space_bytes": free_space,
                "free_space_pretty": self._format_bytes(free_space),
                "free_percent": free_percent
            },
            "bloat": {
                "wasted_space_bytes": wasted_space,
                "wasted_space_pretty": self._format_bytes(wasted_space),
                "wasted_percent": wasted_percent,
                "bloat_severity": bloat_analysis["overall_severity"],
                "dead_tuple_status": bloat_analysis["dead_tuple_status"],
                "free_space_status": bloat_analysis["free_space_status"],
                "tuple_density_status": bloat_analysis["tuple_density_status"],
                "issues": bloat_analysis["issues"]
            }
        }

    def _build_approx_result(
        self,
        schema_name: str,
        table_name: str,
        stats: dict,
        table_size: dict
    ) -> dict[str, Any]:
        """Build result from approximate pgstattuple_approx output."""
        table_len = stats.get("table_len", 0) or 0
        approx_tuple_len = stats.get("approx_tuple_len", 0) or 0
        dead_tuple_len = stats.get("dead_tuple_len", 0) or 0
        approx_free_space = stats.get("approx_free_space", 0) or 0

        # Get percentages directly from pgstattuple_approx output
        approx_tuple_percent = stats.get("approx_tuple_percent", 0) or 0
        dead_tuple_percent = stats.get("dead_tuple_percent", 0) or 0
        approx_free_percent = stats.get("approx_free_percent", 0) or 0

        # Calculate wasted space
        wasted_space = dead_tuple_len + approx_free_space
        wasted_percent = round(100.0 * wasted_space / table_len, 2) if table_len > 0 else 0

        # Get comprehensive bloat analysis based on the three key rules
        bloat_analysis = self._get_bloat_severity(dead_tuple_percent, approx_free_percent, approx_tuple_percent)

        return {
            "schema": schema_name,
            "table_name": table_name,
            "analysis_type": "approximate",
            "scanned_percent": stats.get("scanned_percent", 0),
            "size": {
                "table_len_bytes": table_len,
                "table_len_pretty": self._format_bytes(table_len),
                "total_relation_size": table_size.get("total_size"),
                "total_relation_size_pretty": self._format_bytes(table_size.get("total_size", 0)),
                "indexes_size": table_size.get("indexes_size"),
                "indexes_size_pretty": self._format_bytes(table_size.get("indexes_size", 0))
            },
            "tuples": {
                "approx_live_tuple_count": stats.get("approx_tuple_count", 0),
                "approx_live_tuple_len": approx_tuple_len,
                "approx_live_tuple_percent": approx_tuple_percent,
                "dead_tuple_count": stats.get("dead_tuple_count", 0),
                "dead_tuple_len": dead_tuple_len,
                "dead_tuple_percent": dead_tuple_percent
            },
            "free_space": {
                "approx_free_space_bytes": approx_free_space,
                "approx_free_space_pretty": self._format_bytes(approx_free_space),
                "approx_free_percent": approx_free_percent
            },
            "bloat": {
                "wasted_space_bytes": wasted_space,
                "wasted_space_pretty": self._format_bytes(wasted_space),
                "wasted_percent": wasted_percent,
                "bloat_severity": bloat_analysis["overall_severity"],
                "dead_tuple_status": bloat_analysis["dead_tuple_status"],
                "free_space_status": bloat_analysis["free_space_status"],
                "tuple_density_status": bloat_analysis["tuple_density_status"],
                "issues": bloat_analysis["issues"]
            }
        }

    async def _analyze_schema_tables(
        self,
        schema_name: str,
        use_approx: bool,
        min_size_gb: float
    ) -> dict[str, Any]:
        """Analyze all tables in a schema."""

        # Convert GB to bytes (use bigint cast to avoid integer overflow)
        min_size_bytes = int(min_size_gb * 1024 * 1024 * 1024)

        # Get list of user tables meeting size criteria (exclude system schemas)
        tables_query = """
            SELECT
                c.relname as table_name,
                pg_table_size(c.oid) as table_size
            FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE c.relkind = 'r'
              AND n.nspname = %s
              AND n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
              AND pg_table_size(c.oid) >= %s::bigint
            ORDER BY pg_table_size(c.oid) DESC
        """

        tables = await self.sql_driver.execute_query(
            tables_query, (schema_name, min_size_bytes)
        )

        if not tables:
            return {
                "schema": schema_name,
                "message": f"No tables found with size >= {min_size_gb}GB",
                "tables": []
            }

        # Analyze each table
        results = []
        total_wasted = 0
        total_size = 0

        for table in tables:
            table_name = table["table_name"]
            try:
                table_result = await self._analyze_single_table(
                    schema_name, table_name, use_approx, include_toast=False
                )
                if "error" not in table_result:
                    # Extract key metrics for summary
                    bloat_info = table_result.get("bloat", {})
                    tuples_info = table_result.get("tuples", {})
                    free_space_info = table_result.get("free_space", {})

                    # Get tuple percent (exact vs approx)
                    tuple_percent = tuples_info.get("live_tuple_percent", 0) or tuples_info.get("approx_live_tuple_percent", 0)
                    # Get free percent (exact vs approx)
                    free_percent = free_space_info.get("free_percent", 0) or free_space_info.get("approx_free_percent", 0)

                    results.append({
                        "table_name": table_name,
                        "table_size": table_result.get("size", {}).get("table_len_bytes", 0),
                        "table_size_pretty": table_result.get("size", {}).get("table_len_pretty", "0"),
                        "dead_tuple_percent": tuples_info.get("dead_tuple_percent", 0),
                        "free_percent": free_percent,
                        "tuple_percent": tuple_percent,
                        "wasted_space_bytes": bloat_info.get("wasted_space_bytes", 0),
                        "wasted_space_pretty": bloat_info.get("wasted_space_pretty", "0"),
                        "wasted_percent": bloat_info.get("wasted_percent", 0),
                        "bloat_severity": bloat_info.get("bloat_severity", "low")
                    })
                    total_wasted += bloat_info.get("wasted_space_bytes", 0)
                    total_size += table_result.get("size", {}).get("table_len_bytes", 0)
            except Exception as e:
                results.append({
                    "table_name": table_name,
                    "error": str(e)
                })

        # Sort by wasted space
        results.sort(key=lambda x: x.get("wasted_space_bytes", 0), reverse=True)

        return {
            "schema": schema_name,
            "analysis_type": "approximate" if use_approx else "exact",
            "tables_analyzed": len(results),
            "summary": {
                "total_table_size": total_size,
                "total_table_size_pretty": self._format_bytes(total_size),
                "total_wasted_space": total_wasted,
                "total_wasted_space_pretty": self._format_bytes(total_wasted),
                "overall_wasted_percent": round(100.0 * total_wasted / total_size, 2) if total_size > 0 else 0
            },
            "tables": results,
            "recommendations": self._generate_schema_recommendations(results)
        }

    async def _analyze_toast_table(
        self,
        schema_name: str,
        table_name: str,
        use_approx: bool
    ) -> dict[str, Any] | None:
        """Analyze the TOAST table associated with a table."""

        # Get TOAST table name
        toast_query = """
            SELECT
                t.relname as toast_table_name,
                pg_relation_size(t.oid) as toast_size
            FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            LEFT JOIN pg_class t ON t.oid = c.reltoastrelid
            WHERE c.relname = %s AND n.nspname = %s AND t.relname IS NOT NULL
        """

        toast_result = await self.sql_driver.execute_query(
            toast_query, (table_name, schema_name)
        )

        if not toast_result or not toast_result[0].get("toast_table_name"):
            return None

        toast_table = toast_result[0]["toast_table_name"]

        # Analyze TOAST table
        if use_approx:
            stats_query = """
                SELECT * FROM pgstattuple_approx(%s::regclass)
            """
        else:
            stats_query = """
                SELECT * FROM pgstattuple(%s::regclass)
            """

        try:
            stats = await self.sql_driver.execute_query(
                stats_query, (f"pg_toast.{toast_table}",)
            )
            if stats:
                return {
                    "toast_table_name": toast_table,
                    "toast_size": toast_result[0]["toast_size"],
                    "toast_size_pretty": self._format_bytes(toast_result[0]["toast_size"]),
                    "stats": stats[0]
                }
        except Exception:
            pass  # TOAST table analysis is optional

        return None

    def _get_bloat_severity(
        self,
        dead_tuple_percent: float,
        free_percent: float,
        tuple_percent: float
    ) -> dict[str, Any]:
        """
        Determine bloat severity based on pgstattuple metrics.

        Rules based on pgstattuple best practices:
        - dead_tuple_percent > 10% → Autovacuum is lagging
        - free_percent > 20% → Page fragmentation (consider VACUUM FULL/CLUSTER)
        - tuple_percent < 70% → Heavy table bloat (VACUUM FULL likely needed)
        """
        severity_result = {
            "overall_severity": "minimal",
            "dead_tuple_status": "normal",
            "free_space_status": "normal",
            "tuple_density_status": "normal",
            "issues": []
        }

        severity_score = 0

        # Rule 1: Dead tuple percentage check (autovacuum lag indicator)
        if dead_tuple_percent > 30:
            severity_result["dead_tuple_status"] = "critical"
            severity_result["issues"].append(
                f"Dead tuple percent ({dead_tuple_percent:.1f}%) is critical (>30%). "
                "Manual VACUUM recommended."
            )
            severity_score += 3
        elif dead_tuple_percent > 10:
            severity_result["dead_tuple_status"] = "warning"
            severity_result["issues"].append(
                f"Dead tuple percent ({dead_tuple_percent:.1f}%) indicates autovacuum lag (>10%). "
                "Tune autovacuum settings."
            )
            severity_score += 2

        # Rule 2: Free space percentage check (fragmentation indicator)
        if free_percent > 30:
            severity_result["free_space_status"] = "critical"
            severity_result["issues"].append(
                f"Free space ({free_percent:.1f}%) is very high (>30%). "
                "Consider VACUUM FULL, CLUSTER, or pg_repack."
            )
            severity_score += 3
        elif free_percent > 20:
            severity_result["free_space_status"] = "warning"
            severity_result["issues"].append(
                f"Free space ({free_percent:.1f}%) indicates page fragmentation (>20%). "
                "Consider VACUUM FULL or CLUSTER."
            )
            severity_score += 2

        # Rule 3: Tuple percent check (live data density)
        if tuple_percent < 50:
            severity_result["tuple_density_status"] = "critical"
            severity_result["issues"].append(
                f"Tuple density ({tuple_percent:.1f}%) is critically low (<50%). "
                "Only {:.1f}% of table contains real data. VACUUM FULL strongly recommended.".format(tuple_percent)
            )
            severity_score += 3
        elif tuple_percent < 70:
            severity_result["tuple_density_status"] = "warning"
            severity_result["issues"].append(
                f"Tuple density ({tuple_percent:.1f}%) is low (<70%). "
                "Heavy bloat detected. VACUUM FULL likely needed."
            )
            severity_score += 2

        # Determine overall severity
        if severity_score >= 6:
            severity_result["overall_severity"] = "critical"
        elif severity_score >= 4:
            severity_result["overall_severity"] = "high"
        elif severity_score >= 2:
            severity_result["overall_severity"] = "moderate"
        elif severity_score >= 1:
            severity_result["overall_severity"] = "low"
        else:
            severity_result["overall_severity"] = "minimal"

        return severity_result

    def _generate_recommendations(self, result: dict) -> list[str]:
        """
        Generate recommendations based on bloat analysis.

        Based on pgstattuple best practices:
        - dead_tuple_percent < 10%: Normal, no action
        - dead_tuple_percent 10-30%: Tune autovacuum
        - dead_tuple_percent > 30%: Manual VACUUM recommended
        - free_percent > 20%: Consider CLUSTER / VACUUM FULL
        - tuple_percent < 70%: Bloat serious, VACUUM FULL likely needed
        """
        recommendations = []

        bloat = result.get("bloat", {})
        tuples = result.get("tuples", {})
        free_space = result.get("free_space", {})

        table_name = result.get("table_name", "")
        schema = result.get("schema", "public")
        full_name = f"{schema}.{table_name}"

        # Get key metrics
        dead_percent = tuples.get("dead_tuple_percent", 0) or tuples.get("dead_tuple_percent", 0)
        free_percent = free_space.get("free_percent", 0) or free_space.get("approx_free_percent", 0)
        tuple_percent = tuples.get("live_tuple_percent", 0) or tuples.get("approx_live_tuple_percent", 0)
        severity = bloat.get("bloat_severity", "minimal")

        # Rule 1: Dead tuple percentage recommendations
        if dead_percent > 30:
            recommendations.append(
                f"CRITICAL: Dead tuple percent ({dead_percent:.1f}%) is very high. "
                f"Manual VACUUM recommended: VACUUM ANALYZE {full_name};"
            )
            recommendations.append(
                "Consider tuning autovacuum settings:\n"
                f"  ALTER TABLE {full_name} SET (\n"
                "    autovacuum_vacuum_scale_factor = 0.05,\n"
                "    autovacuum_vacuum_threshold = 50\n"
                "  );"
            )
        elif dead_percent > 10:
            recommendations.append(
                f"WARNING: Dead tuple percent ({dead_percent:.1f}%) indicates autovacuum lag. "
                "Tune autovacuum settings:\n"
                f"  ALTER TABLE {full_name} SET (autovacuum_vacuum_scale_factor = 0.1);"
            )
            recommendations.append(
                "Consider increasing autovacuum_vacuum_cost_limit for faster cleanup."
            )

        # Rule 2: Free space percentage recommendations (fragmentation)
        if free_percent > 30:
            recommendations.append(
                f"CRITICAL: Free space ({free_percent:.1f}%) indicates severe page fragmentation. "
                f"Strongly recommend one of:\n"
                f"  - VACUUM FULL {full_name}; (requires exclusive lock)\n"
                f"  - CLUSTER {full_name} USING <primary_key>; (requires exclusive lock)\n"
                f"  - pg_repack -t {full_name} (online, no locks)"
            )
        elif free_percent > 20:
            recommendations.append(
                f"WARNING: Free space ({free_percent:.1f}%) indicates page fragmentation. "
                f"Consider during maintenance window:\n"
                f"  - VACUUM FULL {full_name};\n"
                f"  - Or use pg_repack for online compaction: pg_repack -t {full_name}"
            )

        # Rule 3: Tuple percent recommendations (live data density)
        if tuple_percent > 0 and tuple_percent < 50:
            recommendations.append(
                f"CRITICAL: Only {tuple_percent:.1f}% of table contains live data. "
                f"~{100 - tuple_percent:.1f}% of space is wasted. "
                f"VACUUM FULL strongly recommended:\n"
                f"  VACUUM FULL {full_name};"
            )
        elif tuple_percent > 0 and tuple_percent < 70:
            recommendations.append(
                f"WARNING: Tuple density ({tuple_percent:.1f}%) is low. "
                f"~{100 - tuple_percent:.1f}% of space is wasted (dead tuples + free space). "
                f"VACUUM FULL likely needed."
            )

        # If no issues found, add positive feedback
        if not recommendations:
            if dead_percent < 10 and free_percent <= 20 and tuple_percent >= 70:
                recommendations.append(
                    f"Table {full_name} is healthy. No bloat concerns detected."
                )
            else:
                recommendations.append(
                    f"Table {full_name} has minimal bloat. Continue monitoring."
                )

        return recommendations

    def _generate_schema_recommendations(self, tables: list[dict]) -> list[str]:
        """
        Generate schema-wide recommendations based on bloat analysis.

        Uses the pgstattuple best practice thresholds:
        - dead_tuple_percent > 10%: Autovacuum issues
        - free_percent > 20%: Fragmentation issues
        - tuple_percent < 70%: Heavy bloat
        """
        recommendations = []

        critical_tables = [t for t in tables if t.get("bloat_severity") == "critical"]
        high_tables = [t for t in tables if t.get("bloat_severity") == "high"]

        if critical_tables:
            table_list = ", ".join(t["table_name"] for t in critical_tables[:5])
            recommendations.append(
                f"CRITICAL: {len(critical_tables)} tables have critical bloat levels. "
                f"Priority tables: {table_list}"
            )
            recommendations.append(
                "Schedule VACUUM FULL or pg_repack for critical tables during maintenance window."
            )

        if high_tables:
            table_list = ", ".join(t["table_name"] for t in high_tables[:5])
            recommendations.append(
                f"HIGH: {len(high_tables)} tables have high bloat levels. "
                f"Tables: {table_list}"
            )

        # Check for autovacuum issues (dead_tuple_percent > 10%)
        high_dead_tables = [t for t in tables if t.get("dead_tuple_percent", 0) > 10]
        if high_dead_tables:
            recommendations.append(
                f"{len(high_dead_tables)} tables have >10% dead tuples (autovacuum lag). "
                "Review autovacuum settings and ensure it's running properly. "
                "Consider lowering autovacuum_vacuum_scale_factor."
            )

        # Check for fragmentation issues (free_percent > 20%)
        fragmented_tables = [t for t in tables if t.get("free_percent", 0) > 20]
        if fragmented_tables:
            table_list = ", ".join(t["table_name"] for t in fragmented_tables[:5])
            recommendations.append(
                f"{len(fragmented_tables)} tables have >20% free space (page fragmentation). "
                f"Tables: {table_list}. "
                "Consider VACUUM FULL or pg_repack to compact pages."
            )

        # Check for low tuple density (tuple_percent < 70%)
        low_density_tables = [t for t in tables if 0 < t.get("tuple_percent", 100) < 70]
        if low_density_tables:
            table_list = ", ".join(t["table_name"] for t in low_density_tables[:5])
            recommendations.append(
                f"{len(low_density_tables)} tables have <70% tuple density (heavy bloat). "
                f"Tables: {table_list}. "
                "VACUUM FULL strongly recommended for these tables."
            )

        return recommendations

    def _format_bytes(self, size: int | None) -> str:
        """Format bytes to human-readable string."""
        if size is None:
            return "0 B"
        for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
            if abs(size) < 1024.0:
                return f"{size:.2f} {unit}"
            size /= 1024.0
        return f"{size:.2f} PB"


class IndexBloatToolHandler(ToolHandler):
    """Tool handler for analyzing index bloat using pgstatindex."""

    name = "analyze_index_bloat"
    title = "Index Bloat Analyzer"
    read_only_hint = True
    destructive_hint = False
    idempotent_hint = True
    open_world_hint = False
    description = """Analyze index bloat using pgstatindex from pgstattuple extension.

Note: This tool analyzes only user/client indexes and excludes PostgreSQL
system indexes (pg_catalog, information_schema, pg_toast). This focuses
the analysis on your application's custom indexes.

Uses pgstatindex to get B-tree index statistics including:
- Leaf page density (avg_leaf_density) - lower values indicate more bloat
- Fragmentation percentage
- Empty and deleted pages

Helps identify indexes that:
- Need REINDEX to improve performance
- Have high fragmentation
- Are wasting storage space

Requires the pgstattuple extension:
CREATE EXTENSION IF NOT EXISTS pgstattuple;

Note: Also supports GIN indexes (pgstatginindex) and Hash indexes (pgstathashindex)."""

    def __init__(self, sql_driver: SqlDriver):
        self.sql_driver = sql_driver

    def get_tool_definition(self) -> Tool:
        return Tool(
            name=self.name,
            description=self.description,
            inputSchema={
                "type": "object",
                "properties": {
                    "index_name": {
                        "type": "string",
                        "description": "Name of a specific index to analyze"
                    },
                    "table_name": {
                        "type": "string",
                        "description": "Analyze all indexes on this table"
                    },
                    "schema_name": {
                        "type": "string",
                        "description": "Schema name (default: public)",
                        "default": "public"
                    },
                    "min_index_size_gb": {
                        "type": "number",
                        "description": "Minimum index size in GB to include (default: 5)",
                        "default": 5
                    },
                    "min_bloat_percent": {
                        "type": "number",
                        "description": "Only show indexes with bloat above this percentage (default: 20)",
                        "default": 20
                    }
                },
                "required": []
            },
            annotations=self.get_annotations()
        )

    async def run_tool(self, arguments: dict[str, Any]) -> Sequence[TextContent]:
        try:
            index_name = arguments.get("index_name")
            table_name = arguments.get("table_name")
            schema_name = arguments.get("schema_name", "public")
            min_size_gb = arguments.get("min_index_size_gb", 5)
            min_bloat_percent = arguments.get("min_bloat_percent", 20)

            # Check if pgstattuple extension is available
            ext_check = await self._check_extension()
            if not ext_check:
                return self.format_result(
                    "Error: pgstattuple extension is not installed.\n"
                    "Install it with: CREATE EXTENSION IF NOT EXISTS pgstattuple;"
                )

            if index_name:
                # Analyze specific index
                result = await self._analyze_single_index(schema_name, index_name)
            elif table_name:
                # Analyze all indexes on a table
                result = await self._analyze_table_indexes(
                    schema_name, table_name, min_size_gb, min_bloat_percent
                )
            else:
                # Analyze all indexes in schema
                result = await self._analyze_schema_indexes(
                    schema_name, min_size_gb, min_bloat_percent
                )

            return self.format_json_result(result)

        except Exception as e:
            return self.format_error(e)

    async def _check_extension(self) -> bool:
        """Check if pgstattuple extension is available."""
        query = """
            SELECT EXISTS (
                SELECT 1 FROM pg_extension WHERE extname = 'pgstattuple'
            ) as available
        """
        result = await self.sql_driver.execute_query(query)
        return result[0].get("available", False) if result else False

    async def _analyze_single_index(
        self,
        schema_name: str,
        index_name: str
    ) -> dict[str, Any]:
        """Analyze a single index."""

        # Get index info including type
        info_query = """
            SELECT
                i.relname as index_name,
                t.relname as table_name,
                am.amname as index_type,
                pg_relation_size(i.oid) as index_size,
                idx.indisunique as is_unique,
                idx.indisprimary as is_primary,
                pg_get_indexdef(i.oid) as definition
            FROM pg_class i
            JOIN pg_namespace n ON n.oid = i.relnamespace
            JOIN pg_am am ON am.oid = i.relam
            JOIN pg_index idx ON idx.indexrelid = i.oid
            JOIN pg_class t ON t.oid = idx.indrelid
            WHERE i.relname = %s AND n.nspname = %s
        """

        info_result = await self.sql_driver.execute_query(
            info_query, (index_name, schema_name)
        )

        if not info_result:
            return {
                "error": f"Index {schema_name}.{index_name} not found"
            }

        info = info_result[0]
        index_type = info["index_type"]

        # Call appropriate function based on index type
        if index_type == "btree":
            stats = await self._get_btree_stats(schema_name, index_name)
        elif index_type == "gin":
            stats = await self._get_gin_stats(schema_name, index_name)
        elif index_type == "hash":
            stats = await self._get_hash_stats(schema_name, index_name)
        else:
            stats = {"note": f"pgstattuple does not support {index_type} indexes directly"}

        return {
            "schema": schema_name,
            "index_name": index_name,
            "table_name": info["table_name"],
            "index_type": index_type,
            "is_unique": info["is_unique"],
            "is_primary": info["is_primary"],
            "size": {
                "bytes": info["index_size"],
                "pretty": self._format_bytes(info["index_size"])
            },
            "definition": info["definition"],
            "statistics": stats,
            "recommendations": self._generate_index_recommendations(stats, index_type, info)
        }

    async def _get_btree_stats(
        self,
        schema_name: str,
        index_name: str
    ) -> dict[str, Any]:
        """Get B-tree index statistics using pgstatindex."""
        query = """
            SELECT * FROM pgstatindex(quote_ident(%s) || '.' || quote_ident(%s))
        """
        result = await self.sql_driver.execute_query(
            query, (schema_name, index_name)
        )

        if not result:
            return {"error": "Could not get index statistics"}

        stats = result[0]

        # Get key metrics for bloat analysis
        avg_density = stats.get("avg_leaf_density", 90) or 90
        # Calculate free_percent based on empty/deleted pages vs total
        leaf_pages = stats.get("leaf_pages", 1) or 1
        empty_pages = stats.get("empty_pages", 0) or 0
        deleted_pages = stats.get("deleted_pages", 0) or 0
        free_percent = round(100.0 * (empty_pages + deleted_pages) / leaf_pages, 2) if leaf_pages > 0 else 0

        # Get comprehensive bloat analysis
        bloat_analysis = self._get_index_bloat_severity(avg_density, free_percent)

        return {
            "version": stats.get("version"),
            "tree_level": stats.get("tree_level"),
            "index_size": stats.get("index_size"),
            "root_block_no": stats.get("root_block_no"),
            "internal_pages": stats.get("internal_pages"),
            "leaf_pages": leaf_pages,
            "empty_pages": empty_pages,
            "deleted_pages": deleted_pages,
            "avg_leaf_density": avg_density,
            "leaf_fragmentation": stats.get("leaf_fragmentation"),
            "free_percent": free_percent,
            "estimated_bloat_percent": bloat_analysis["estimated_bloat_percent"],
            "bloat_severity": bloat_analysis["overall_severity"],
            "density_status": bloat_analysis["density_status"],
            "issues": bloat_analysis["issues"]
        }

    async def _get_gin_stats(
        self,
        schema_name: str,
        index_name: str
    ) -> dict[str, Any]:
        """Get GIN index statistics using pgstatginindex."""
        query = """
            SELECT * FROM pgstatginindex(quote_ident(%s) || '.' || quote_ident(%s))
        """
        result = await self.sql_driver.execute_query(
            query, (schema_name, index_name)
        )

        if not result:
            return {"error": "Could not get GIN index statistics"}

        stats = result[0]
        return {
            "version": stats.get("version"),
            "pending_pages": stats.get("pending_pages"),
            "pending_tuples": stats.get("pending_tuples"),
            "note": "GIN indexes with many pending tuples may need VACUUM to merge pending entries"
        }

    async def _get_hash_stats(
        self,
        schema_name: str,
        index_name: str
    ) -> dict[str, Any]:
        """Get Hash index statistics using pgstathashindex."""
        query = """
            SELECT * FROM pgstathashindex(quote_ident(%s) || '.' || quote_ident(%s))
        """
        result = await self.sql_driver.execute_query(
            query, (schema_name, index_name)
        )

        if not result:
            return {"error": "Could not get Hash index statistics"}

        stats = result[0]
        return {
            "version": stats.get("version"),
            "bucket_pages": stats.get("bucket_pages"),
            "overflow_pages": stats.get("overflow_pages"),
            "bitmap_pages": stats.get("bitmap_pages"),
            "unused_pages": stats.get("unused_pages"),
            "live_items": stats.get("live_items"),
            "dead_items": stats.get("dead_items"),
            "free_percent": stats.get("free_percent")
        }

    async def _analyze_table_indexes(
        self,
        schema_name: str,
        table_name: str,
        min_size_gb: float,
        min_bloat_percent: float
    ) -> dict[str, Any]:
        """Analyze all indexes on a specific table."""

        # Convert GB to bytes (use bigint cast to avoid integer overflow)
        min_size_bytes = int(min_size_gb * 1024 * 1024 * 1024)

        # Get all indexes on the table
        indexes_query = """
            SELECT
                i.relname as index_name,
                am.amname as index_type,
                pg_relation_size(i.oid) as index_size
            FROM pg_class i
            JOIN pg_namespace n ON n.oid = i.relnamespace
            JOIN pg_am am ON am.oid = i.relam
            JOIN pg_index idx ON idx.indexrelid = i.oid
            JOIN pg_class t ON t.oid = idx.indrelid
            WHERE t.relname = %s
              AND n.nspname = %s
              AND pg_relation_size(i.oid) >= %s::bigint
            ORDER BY pg_relation_size(i.oid) DESC
        """

        indexes = await self.sql_driver.execute_query(
            indexes_query, (table_name, schema_name, min_size_bytes)
        )

        if not indexes:
            return {
                "schema": schema_name,
                "table_name": table_name,
                "message": f"No indexes found with size >= {min_size_gb}GB",
                "indexes": []
            }

        results = []
        for idx in indexes:
            try:
                idx_result = await self._analyze_single_index(
                    schema_name, idx["index_name"]
                )
                if "error" not in idx_result:
                    stats = idx_result.get("statistics", {})
                    bloat_pct = stats.get("estimated_bloat_percent", 0)
                    if bloat_pct >= min_bloat_percent or idx["index_type"] != "btree":
                        results.append(idx_result)
            except Exception as e:
                results.append({
                    "index_name": idx["index_name"],
                    "error": str(e)
                })

        return {
            "schema": schema_name,
            "table_name": table_name,
            "indexes_analyzed": len(indexes),
            "indexes_with_bloat": len(results),
            "min_bloat_threshold": min_bloat_percent,
            "indexes": results
        }

    async def _analyze_schema_indexes(
        self,
        schema_name: str,
        min_size_gb: float,
        min_bloat_percent: float
    ) -> dict[str, Any]:
        """Analyze all indexes in a schema."""

        # Convert GB to bytes (use bigint cast to avoid integer overflow)
        min_size_bytes = int(min_size_gb * 1024 * 1024 * 1024)

        # Get all B-tree user indexes in schema (only B-tree for bloat analysis, exclude system schemas)
        indexes_query = """
            SELECT
                i.relname as index_name,
                t.relname as table_name,
                am.amname as index_type,
                pg_relation_size(i.oid) as index_size
            FROM pg_class i
            JOIN pg_namespace n ON n.oid = i.relnamespace
            JOIN pg_am am ON am.oid = i.relam
            JOIN pg_index idx ON idx.indexrelid = i.oid
            JOIN pg_class t ON t.oid = idx.indrelid
            WHERE n.nspname = %s
              AND n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
              AND am.amname = 'btree'
              AND pg_relation_size(i.oid) >= %s::bigint
            ORDER BY pg_relation_size(i.oid) DESC
            LIMIT 50
        """

        indexes = await self.sql_driver.execute_query(
            indexes_query, (schema_name, min_size_bytes)
        )

        if not indexes:
            return {
                "schema": schema_name,
                "message": f"No B-tree indexes found with size >= {min_size_gb}GB",
                "indexes": []
            }

        results = []
        total_size = 0
        total_bloated_size = 0

        for idx in indexes:
            try:
                stats = await self._get_btree_stats(schema_name, idx["index_name"])
                if "error" not in stats:
                    bloat_pct = stats.get("estimated_bloat_percent", 0)
                    idx_size = idx["index_size"]
                    total_size += idx_size

                    if bloat_pct >= min_bloat_percent:
                        bloated_size = int(idx_size * bloat_pct / 100)
                        total_bloated_size += bloated_size
                        results.append({
                            "index_name": idx["index_name"],
                            "table_name": idx["table_name"],
                            "index_size": idx_size,
                            "index_size_pretty": self._format_bytes(idx_size),
                            "avg_leaf_density": stats.get("avg_leaf_density"),
                            "leaf_fragmentation": stats.get("leaf_fragmentation"),
                            "estimated_bloat_percent": bloat_pct,
                            "bloat_severity": stats.get("bloat_severity"),
                            "estimated_wasted_space": self._format_bytes(bloated_size)
                        })
            except Exception as e:
                pass  # Skip indexes that fail

        # Sort by bloat percent
        results.sort(key=lambda x: x.get("estimated_bloat_percent", 0), reverse=True)

        return {
            "schema": schema_name,
            "indexes_analyzed": len(indexes),
            "indexes_with_bloat": len(results),
            "min_bloat_threshold": min_bloat_percent,
            "summary": {
                "total_index_size": self._format_bytes(total_size),
                "estimated_bloated_space": self._format_bytes(total_bloated_size)
            },
            "indexes": results,
            "recommendations": self._generate_schema_index_recommendations(results)
        }

    def _get_index_bloat_severity(self, avg_leaf_density: float, free_percent: float = 0) -> dict[str, Any]:
        """
        Determine index bloat severity based on pgstatindex metrics.

        Rules based on pgstattuple best practices for indexes:
        - avg_leaf_density < 70%: Index page fragmentation (needs REINDEX)
        - free_space > 20%: Too many empty index pages (needs REINDEX)
        - leaf_pages grows over time: Index bloat accumulating
        """
        severity_result = {
            "overall_severity": "low",
            "density_status": "normal",
            "issues": []
        }

        severity_score = 0

        # Calculate estimated bloat from density (ideal is ~90%)
        estimated_bloat = max(0, 90 - avg_leaf_density)

        # Rule: avg_leaf_density < 70% = Index page fragmentation
        if avg_leaf_density < 50:
            severity_result["density_status"] = "critical"
            severity_result["issues"].append(
                f"Leaf density ({avg_leaf_density:.1f}%) is critically low (<50%). "
                "Index is heavily fragmented. REINDEX required."
            )
            severity_score += 3
        elif avg_leaf_density < 70:
            severity_result["density_status"] = "warning"
            severity_result["issues"].append(
                f"Leaf density ({avg_leaf_density:.1f}%) indicates fragmentation (<70%). "
                "Consider REINDEX to improve performance."
            )
            severity_score += 2

        # Rule: free_space > 20% = Too many empty index pages
        if free_percent > 30:
            severity_result["issues"].append(
                f"Free space ({free_percent:.1f}%) is very high (>30%). "
                "Many empty index pages. REINDEX recommended."
            )
            severity_score += 2
        elif free_percent > 20:
            severity_result["issues"].append(
                f"Free space ({free_percent:.1f}%) is elevated (>20%). "
                "Index may benefit from REINDEX."
            )
            severity_score += 1

        # Determine overall severity
        if severity_score >= 4 or estimated_bloat >= 40:
            severity_result["overall_severity"] = "critical"
        elif severity_score >= 3 or estimated_bloat >= 30:
            severity_result["overall_severity"] = "high"
        elif severity_score >= 2 or estimated_bloat >= 20:
            severity_result["overall_severity"] = "moderate"
        else:
            severity_result["overall_severity"] = "low"

        severity_result["estimated_bloat_percent"] = round(estimated_bloat, 2)

        return severity_result

    def _generate_index_recommendations(
        self,
        stats: dict,
        index_type: str,
        info: dict
    ) -> list[str]:
        """
        Generate recommendations for a single index.

        Based on pgstatindex best practices:
        - avg_leaf_density < 70%: Index page fragmentation → REINDEX
        - free_space > 20%: Too many empty index pages → REINDEX
        - leaf_pages grows over time: Index bloat accumulating
        """
        recommendations = []

        if index_type == "btree":
            avg_density = stats.get("avg_leaf_density", 90)
            free_percent = stats.get("free_percent", 0)
            bloat_pct = stats.get("estimated_bloat_percent", 0)
            severity = stats.get("bloat_severity", "low")
            index_name = info.get("index_name", "")
            schema = info.get("schema", "public") if "schema" in info else "public"
            full_name = f"{schema}.{index_name}" if schema else index_name

            # Rule: avg_leaf_density < 70% = fragmentation
            if avg_density < 50:
                recommendations.append(
                    f"CRITICAL: Leaf density ({avg_density:.1f}%) is very low (<50%). "
                    f"Index is heavily fragmented. Run:\n"
                    f"  REINDEX INDEX CONCURRENTLY {full_name};"
                )
            elif avg_density < 70:
                recommendations.append(
                    f"WARNING: Leaf density ({avg_density:.1f}%) indicates fragmentation (<70%). "
                    f"Consider:\n"
                    f"  REINDEX INDEX CONCURRENTLY {full_name};"
                )

            # Rule: free_space > 20% = too many empty pages
            if free_percent > 30:
                recommendations.append(
                    f"CRITICAL: Free space ({free_percent:.1f}%) is very high (>30%). "
                    "Many empty index pages. REINDEX strongly recommended."
                )
            elif free_percent > 20:
                recommendations.append(
                    f"WARNING: Free space ({free_percent:.1f}%) is elevated (>20%). "
                    "Index may benefit from REINDEX."
                )

            frag = stats.get("leaf_fragmentation", 0)
            if frag and frag > 30:
                recommendations.append(
                    f"Index has {frag:.1f}% leaf fragmentation. "
                    "This can slow sequential index scans. Consider REINDEX."
                )

            deleted_pages = stats.get("deleted_pages", 0)
            if deleted_pages and deleted_pages > 10:
                recommendations.append(
                    f"Index has {deleted_pages} deleted pages. "
                    "These will be reclaimed by future index operations or REINDEX."
                )

            # If no issues, provide positive feedback
            if not recommendations:
                if avg_density >= 70 and free_percent <= 20:
                    recommendations.append(
                        f"Index {full_name} is healthy. Leaf density ({avg_density:.1f}%) is good."
                    )

        elif index_type == "gin":
            pending = stats.get("pending_tuples", 0)
            if pending and pending > 1000:
                recommendations.append(
                    f"GIN index has {pending} pending tuples. "
                    "Run VACUUM to merge pending entries into main index."
                )
            elif pending and pending > 100:
                recommendations.append(
                    f"GIN index has {pending} pending tuples. "
                    "Consider running VACUUM if this continues to grow."
                )

        elif index_type == "hash":
            dead_items = stats.get("dead_items", 0)
            if dead_items and dead_items > 100:
                recommendations.append(
                    f"Hash index has {dead_items} dead items. "
                    "Run VACUUM to clean up dead entries."
                )

        return recommendations

    def _generate_schema_index_recommendations(self, indexes: list[dict]) -> list[str]:
        """
        Generate schema-wide index recommendations.

        Based on pgstatindex best practices:
        - avg_leaf_density < 70%: Index page fragmentation
        - free_space > 20%: Too many empty index pages
        """
        recommendations = []

        critical = [i for i in indexes if i.get("bloat_severity") == "critical"]
        high = [i for i in indexes if i.get("bloat_severity") == "high"]

        if critical:
            idx_list = ", ".join(i["index_name"] for i in critical[:5])
            recommendations.append(
                f"CRITICAL: {len(critical)} indexes have critical bloat. "
                f"Priority indexes: {idx_list}"
            )
            recommendations.append(
                "Run REINDEX INDEX CONCURRENTLY for these indexes to reclaim space."
            )

        if high:
            idx_list = ", ".join(i["index_name"] for i in high[:5])
            recommendations.append(
                f"HIGH: {len(high)} indexes have high bloat levels. "
                f"Indexes: {idx_list}"
            )

        # Check for low density indexes (< 70%)
        low_density_indexes = [i for i in indexes if i.get("avg_leaf_density", 100) < 70]
        if low_density_indexes:
            idx_list = ", ".join(i["index_name"] for i in low_density_indexes[:5])
            recommendations.append(
                f"{len(low_density_indexes)} indexes have leaf density <70% (fragmented). "
                f"Indexes: {idx_list}. Consider REINDEX."
            )

        return recommendations

    def _format_bytes(self, size: int | None) -> str:
        """Format bytes to human-readable string."""
        if size is None:
            return "0 B"
        for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
            if abs(size) < 1024.0:
                return f"{size:.2f} {unit}"
            size /= 1024.0
        return f"{size:.2f} PB"


class DatabaseBloatSummaryToolHandler(ToolHandler):
    """Tool handler for getting a comprehensive database bloat summary."""

    name = "get_bloat_summary"
    title = "Database Bloat Summary"
    read_only_hint = True
    destructive_hint = False
    idempotent_hint = True
    open_world_hint = False
    description = """Get a comprehensive summary of database bloat across tables and indexes.

Note: This tool analyzes only user/client tables and indexes, excluding
PostgreSQL system objects (pg_catalog, information_schema, pg_toast).
This focuses the analysis on your application's custom objects.

Provides a high-level overview of:
- Top bloated tables by wasted space
- Top bloated indexes by estimated bloat
- Total reclaimable space estimates
- Priority maintenance recommendations

Uses pgstattuple_approx for tables (faster) and pgstatindex for B-tree indexes.
Requires the pgstattuple extension to be installed.

Best for: Quick assessment of database bloat and maintenance priorities."""

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
                    "top_n": {
                        "type": "integer",
                        "description": "Number of top bloated objects to show (default: 10)",
                        "default": 10
                    },
                    "min_size_gb": {
                        "type": "number",
                        "description": "Minimum object size in GB to include (default: 5)",
                        "default": 5
                    }
                },
                "required": []
            },
            annotations=self.get_annotations()
        )

    async def run_tool(self, arguments: dict[str, Any]) -> Sequence[TextContent]:
        try:
            schema_name = arguments.get("schema_name", "public")
            top_n = arguments.get("top_n", 10)
            min_size_gb = arguments.get("min_size_gb", 5)

            # Check extension
            ext_query = """
                SELECT EXISTS (
                    SELECT 1 FROM pg_extension WHERE extname = 'pgstattuple'
                ) as available
            """
            ext_result = await self.sql_driver.execute_query(ext_query)

            if not ext_result or not ext_result[0].get("available"):
                return self.format_result(
                    "Error: pgstattuple extension is not installed.\n"
                    "Install it with: CREATE EXTENSION IF NOT EXISTS pgstattuple;"
                )

            # Get table bloat summary
            table_bloat = await self._get_table_bloat_summary(
                schema_name, top_n, min_size_gb
            )

            # Get index bloat summary
            index_bloat = await self._get_index_bloat_summary(
                schema_name, top_n, min_size_gb
            )

            # Calculate totals
            total_table_wasted = sum(
                t.get("wasted_bytes", 0) for t in table_bloat.get("tables", [])
            )
            total_index_wasted = sum(
                i.get("estimated_wasted_bytes", 0) for i in index_bloat.get("indexes", [])
            )

            result = {
                "schema": schema_name,
                "summary": {
                    "tables_analyzed": table_bloat.get("tables_analyzed", 0),
                    "indexes_analyzed": index_bloat.get("indexes_analyzed", 0),
                    "total_table_wasted_space": self._format_bytes(total_table_wasted),
                    "total_index_wasted_space": self._format_bytes(total_index_wasted),
                    "total_reclaimable": self._format_bytes(total_table_wasted + total_index_wasted)
                },
                "top_bloated_tables": table_bloat.get("tables", []),
                "top_bloated_indexes": index_bloat.get("indexes", []),
                "maintenance_priority": self._generate_priority_actions(
                    table_bloat.get("tables", []),
                    index_bloat.get("indexes", [])
                )
            }

            return self.format_json_result(result)

        except Exception as e:
            return self.format_error(e)

    async def _get_table_bloat_summary(
        self,
        schema_name: str,
        top_n: int,
        min_size_gb: float
    ) -> dict[str, Any]:
        """
        Get summary of table bloat using pgstattuple_approx.

        Analyzes tables based on the key bloat indicators:
        - dead_tuple_percent > 10%: Autovacuum lag
        - free_percent > 20%: Page fragmentation
        - tuple_percent < 70%: Heavy bloat
        """

        # Convert GB to bytes (use bigint cast to avoid integer overflow)
        min_size_bytes = int(min_size_gb * 1024 * 1024 * 1024)

        # Get user tables to analyze (exclude system schemas)
        tables_query = """
            SELECT
                c.relname as table_name,
                pg_table_size(c.oid) as table_size
            FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE c.relkind = 'r'
              AND n.nspname = %s
              AND n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
              AND pg_table_size(c.oid) >= %s::bigint
            ORDER BY pg_table_size(c.oid) DESC
            LIMIT 100
        """

        tables = await self.sql_driver.execute_query(
            tables_query, (schema_name, min_size_bytes)
        )

        results = []
        for table in tables:
            try:
                stats_query = """
                    SELECT * FROM pgstattuple_approx(quote_ident(%s) || '.' || quote_ident(%s))
                """
                stats_result = await self.sql_driver.execute_query(
                    stats_query, (schema_name, table["table_name"])
                )

                if stats_result:
                    stats = stats_result[0]
                    table_len = stats.get("table_len", 0) or 0
                    dead_tuple_len = stats.get("dead_tuple_len", 0) or 0
                    free_space = stats.get("approx_free_space", 0) or 0
                    wasted = dead_tuple_len + free_space
                    wasted_pct = round(100.0 * wasted / table_len, 2) if table_len > 0 else 0

                    # Get key metrics for bloat analysis
                    dead_tuple_percent = stats.get("dead_tuple_percent", 0) or 0
                    free_percent = stats.get("approx_free_percent", 0) or 0
                    tuple_percent = stats.get("approx_tuple_percent", 0) or 0

                    # Determine bloat severity based on rules
                    bloat_severity = "minimal"
                    if dead_tuple_percent > 30 or free_percent > 30 or (tuple_percent > 0 and tuple_percent < 50):
                        bloat_severity = "critical"
                    elif dead_tuple_percent > 10 or free_percent > 20 or (tuple_percent > 0 and tuple_percent < 70):
                        bloat_severity = "high"
                    elif dead_tuple_percent > 5 or free_percent > 10:
                        bloat_severity = "moderate"

                    results.append({
                        "table_name": table["table_name"],
                        "table_size": self._format_bytes(table_len),
                        "table_size_bytes": table_len,
                        "dead_tuple_percent": dead_tuple_percent,
                        "free_percent": free_percent,
                        "tuple_percent": tuple_percent,
                        "wasted_bytes": wasted,
                        "wasted_space": self._format_bytes(wasted),
                        "wasted_percent": wasted_pct,
                        "bloat_severity": bloat_severity
                    })
            except Exception:
                pass

        # Sort by wasted space and take top N
        results.sort(key=lambda x: x.get("wasted_bytes", 0), reverse=True)

        return {
            "tables_analyzed": len(tables) if tables else 0,
            "tables": results[:top_n]
        }

    async def _get_index_bloat_summary(
        self,
        schema_name: str,
        top_n: int,
        min_size_gb: float
    ) -> dict[str, Any]:
        """
        Get summary of index bloat using pgstatindex.

        Analyzes indexes based on the key bloat indicators:
        - avg_leaf_density < 70%: Index page fragmentation
        - free_space > 20%: Too many empty index pages
        """

        # Convert GB to bytes (use bigint cast to avoid integer overflow)
        min_size_bytes = int(min_size_gb * 1024 * 1024 * 1024)

        # Get B-tree user indexes (exclude system schemas)
        indexes_query = """
            SELECT
                i.relname as index_name,
                t.relname as table_name,
                pg_relation_size(i.oid) as index_size
            FROM pg_class i
            JOIN pg_namespace n ON n.oid = i.relnamespace
            JOIN pg_am am ON am.oid = i.relam
            JOIN pg_index idx ON idx.indexrelid = i.oid
            JOIN pg_class t ON t.oid = idx.indrelid
            WHERE n.nspname = %s
              AND n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
              AND am.amname = 'btree'
              AND pg_relation_size(i.oid) >= %s::bigint
            ORDER BY pg_relation_size(i.oid) DESC
            LIMIT 100
        """

        indexes = await self.sql_driver.execute_query(
            indexes_query, (schema_name, min_size_bytes)
        )

        results = []
        for idx in indexes:
            try:
                stats_query = """
                    SELECT * FROM pgstatindex(quote_ident(%s) || '.' || quote_ident(%s))
                """
                stats_result = await self.sql_driver.execute_query(
                    stats_query, (schema_name, idx["index_name"])
                )

                if stats_result:
                    stats = stats_result[0]
                    avg_density = stats.get("avg_leaf_density", 90) or 90
                    bloat_pct = max(0, 90 - avg_density)
                    idx_size = idx["index_size"]
                    wasted = int(idx_size * bloat_pct / 100)

                    # Calculate free percent from empty/deleted pages
                    leaf_pages = stats.get("leaf_pages", 1) or 1
                    empty_pages = stats.get("empty_pages", 0) or 0
                    deleted_pages = stats.get("deleted_pages", 0) or 0
                    free_percent = round(100.0 * (empty_pages + deleted_pages) / leaf_pages, 2) if leaf_pages > 0 else 0

                    # Determine bloat severity based on rules
                    bloat_severity = "low"
                    if avg_density < 50 or free_percent > 30:
                        bloat_severity = "critical"
                    elif avg_density < 70 or free_percent > 20:
                        bloat_severity = "high"
                    elif bloat_pct >= 20:
                        bloat_severity = "moderate"

                    results.append({
                        "index_name": idx["index_name"],
                        "table_name": idx["table_name"],
                        "index_size": self._format_bytes(idx_size),
                        "index_size_bytes": idx_size,
                        "avg_leaf_density": avg_density,
                        "free_percent": free_percent,
                        "estimated_bloat_percent": round(bloat_pct, 2),
                        "estimated_wasted_bytes": wasted,
                        "estimated_wasted_space": self._format_bytes(wasted),
                        "bloat_severity": bloat_severity
                    })
            except Exception:
                pass

        # Sort by bloat percent and take top N
        results.sort(key=lambda x: x.get("estimated_bloat_percent", 0), reverse=True)

        return {
            "indexes_analyzed": len(indexes) if indexes else 0,
            "indexes": results[:top_n]
        }

    def _generate_priority_actions(
        self,
        tables: list[dict],
        indexes: list[dict]
    ) -> list[dict]:
        """
        Generate prioritized maintenance actions based on bloat analysis.

        Uses pgstattuple best practice thresholds:
        - Tables: dead_tuple_percent > 10%, free_percent > 20%, tuple_percent < 70%
        - Indexes: avg_leaf_density < 70%, free_space > 20%
        """
        actions = []

        # High-priority table maintenance based on the new rules
        for t in tables:
            dead_pct = t.get("dead_tuple_percent", 0)
            free_pct = t.get("free_percent", 0)
            tuple_pct = t.get("tuple_percent", 100)
            wasted_pct = t.get("wasted_percent", 0)
            severity = t.get("bloat_severity", "minimal")

            issues = []
            priority = "low"

            # Check dead tuple percent (autovacuum lag indicator)
            if dead_pct > 30:
                issues.append(f"dead tuples {dead_pct:.1f}% (critical)")
                priority = "high"
            elif dead_pct > 10:
                issues.append(f"dead tuples {dead_pct:.1f}% (autovacuum lag)")
                priority = "medium" if priority != "high" else priority

            # Check free space percent (fragmentation indicator)
            if free_pct > 30:
                issues.append(f"free space {free_pct:.1f}% (severe fragmentation)")
                priority = "high"
            elif free_pct > 20:
                issues.append(f"free space {free_pct:.1f}% (fragmentation)")
                priority = "medium" if priority != "high" else priority

            # Check tuple percent (live data density)
            if tuple_pct > 0 and tuple_pct < 50:
                issues.append(f"tuple density {tuple_pct:.1f}% (critical bloat)")
                priority = "high"
            elif tuple_pct > 0 and tuple_pct < 70:
                issues.append(f"tuple density {tuple_pct:.1f}% (heavy bloat)")
                priority = "medium" if priority != "high" else priority

            if issues:
                action = f"VACUUM ANALYZE {t['table_name']}"
                alternative = None
                if priority == "high" or tuple_pct < 70:
                    alternative = f"VACUUM FULL {t['table_name']} (requires exclusive lock) or pg_repack"

                actions.append({
                    "priority": priority,
                    "type": "table",
                    "object": t["table_name"],
                    "issue": "; ".join(issues),
                    "action": action,
                    "alternative": alternative
                })

        # High-priority index maintenance based on the new rules
        for i in indexes:
            avg_density = i.get("avg_leaf_density", 90)
            free_pct = i.get("free_percent", 0)
            bloat_pct = i.get("estimated_bloat_percent", 0)
            severity = i.get("bloat_severity", "low")

            issues = []
            priority = "low"

            # Check leaf density (fragmentation indicator)
            if avg_density < 50:
                issues.append(f"leaf density {avg_density:.1f}% (critical fragmentation)")
                priority = "high"
            elif avg_density < 70:
                issues.append(f"leaf density {avg_density:.1f}% (fragmentation)")
                priority = "medium"

            # Check free percent
            if free_pct > 30:
                issues.append(f"free space {free_pct:.1f}% (many empty pages)")
                priority = "high"
            elif free_pct > 20:
                issues.append(f"free space {free_pct:.1f}% (elevated)")
                priority = "medium" if priority != "high" else priority

            if issues:
                actions.append({
                    "priority": priority,
                    "type": "index",
                    "object": i["index_name"],
                    "table": i["table_name"],
                    "issue": "; ".join(issues),
                    "action": f"REINDEX INDEX CONCURRENTLY {i['index_name']}"
                })

        # Sort by priority
        priority_order = {"high": 0, "medium": 1, "low": 2}
        actions.sort(key=lambda x: priority_order.get(x.get("priority", "low"), 2))

        return actions[:10]  # Top 10 actions

    def _format_bytes(self, size: int | None) -> str:
        """Format bytes to human-readable string."""
        if size is None:
            return "0 B"
        for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
            if abs(size) < 1024.0:
                return f"{size:.2f} {unit}"
            size /= 1024.0
        return f"{size:.2f} PB"
