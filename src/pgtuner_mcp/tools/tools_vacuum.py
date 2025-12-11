"""Vacuum progress monitoring tool handlers."""

from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime, timezone
from typing import Any

from mcp.types import TextContent, Tool

from ..services import SqlDriver, get_user_filter
from .toolhandler import ToolHandler


class VacuumProgressToolHandler(ToolHandler):
    """Tool handler for monitoring vacuum operations progress."""

    name = "monitor_vacuum_progress"
    title = "Vacuum Progress Monitor"
    read_only_hint = True
    destructive_hint = False
    idempotent_hint = True
    open_world_hint = False
    description = """Track long-running VACUUM operations in PostgreSQL.

This tool monitors the progress of:
- Manual VACUUM operations
- VACUUM FULL operations
- Autovacuum processes

Shows:
- Current phase of the vacuum operation
- Progress percentage (heap scanned, indexes vacuumed)
- Dead tuples collected
- Index vacuum rounds
- Estimated time remaining

Also provides information about:
- Autovacuum configuration
- Tables that need vacuuming
- Recent vacuum activity

Useful for:
- Monitoring long-running maintenance operations
- Identifying tables with vacuum backlog
- Troubleshooting autovacuum issues"""

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
                        "enum": [
                            "progress",      # Show current vacuum progress
                            "needs_vacuum",  # List tables that need vacuuming
                            "autovacuum_status",  # Show autovacuum configuration and status
                            "recent_activity"  # Show recent vacuum activity
                        ],
                        "default": "needs_vacuum"
                    },
                    "schema_name": {
                        "type": "string",
                        "description": "Schema to filter by (default: all schemas)",
                        "default": "public"
                    },
                    "include_toast": {
                        "type": "boolean",
                        "description": "Include TOAST tables in results",
                        "default": False
                    },
                    "min_dead_tuples": {
                        "type": "integer",
                        "description": "Minimum dead tuples for needs_vacuum action",
                        "default": 1000
                    }
                },
                "required": []
            },
            annotations=self.get_annotations()
        )

    async def run_tool(self, arguments: dict[str, Any]) -> Sequence[TextContent]:
        try:
            action = arguments.get("action", "progress")

            if action == "progress":
                return await self._get_vacuum_progress(arguments)
            elif action == "needs_vacuum":
                return await self._get_tables_needing_vacuum(arguments)
            elif action == "autovacuum_status":
                return await self._get_autovacuum_status(arguments)
            elif action == "recent_activity":
                return await self._get_recent_vacuum_activity(arguments)
            else:
                return self.format_result(f"Unknown action: {action}")

        except Exception as e:
            return self.format_error(e)

    async def _get_vacuum_progress(self, arguments: dict[str, Any]) -> Sequence[TextContent]:
        """Get current vacuum operation progress."""
        include_toast = arguments.get("include_toast", False)

        # Get user filter
        user_filter = get_user_filter()
        activity_filter = user_filter.get_activity_filter()

        toast_filter = "" if include_toast else "AND c.relname NOT LIKE 'pg_toast%%'"

        # Query pg_stat_progress_vacuum for vacuum progress
        progress_query = f"""
            SELECT
                p.pid,
                p.datname as database,
                n.nspname as schema_name,
                c.relname as table_name,
                p.phase,
                p.heap_blks_total,
                p.heap_blks_scanned,
                p.heap_blks_vacuumed,
                p.index_vacuum_count,
                p.max_dead_tuples,
                p.num_dead_tuples,
                CASE
                    WHEN p.heap_blks_total > 0
                    THEN ROUND(100.0 * p.heap_blks_scanned / p.heap_blks_total, 2)
                    ELSE 0
                END as scan_progress_pct,
                CASE
                    WHEN p.heap_blks_total > 0
                    THEN ROUND(100.0 * p.heap_blks_vacuumed / p.heap_blks_total, 2)
                    ELSE 0
                END as vacuum_progress_pct,
                a.query,
                a.state,
                EXTRACT(epoch FROM now() - a.xact_start)::integer as duration_seconds,
                a.wait_event_type,
                a.wait_event
            FROM pg_stat_progress_vacuum p
            JOIN pg_class c ON c.oid = p.relid
            JOIN pg_namespace n ON n.oid = c.relnamespace
            LEFT JOIN pg_stat_activity a ON a.pid = p.pid
            WHERE n.nspname NOT IN ('pg_catalog', 'information_schema')
              {toast_filter}
              {activity_filter.replace('usesysid', 'a.usesysid') if activity_filter else ''}
            ORDER BY p.heap_blks_total DESC
        """

        progress = await self.sql_driver.execute_query(progress_query)

        # Also check for VACUUM FULL operations (uses pg_stat_progress_cluster)
        cluster_query = f"""
            SELECT
                p.pid,
                p.datname as database,
                n.nspname as schema_name,
                c.relname as table_name,
                p.phase,
                p.heap_blks_total,
                p.heap_blks_scanned,
                p.heap_tuples_scanned,
                p.heap_tuples_written,
                CASE
                    WHEN p.heap_blks_total > 0
                    THEN ROUND(100.0 * p.heap_blks_scanned / p.heap_blks_total, 2)
                    ELSE 0
                END as progress_pct,
                a.query,
                EXTRACT(epoch FROM now() - a.xact_start)::integer as duration_seconds
            FROM pg_stat_progress_cluster p
            JOIN pg_class c ON c.oid = p.relid
            JOIN pg_namespace n ON n.oid = c.relnamespace
            LEFT JOIN pg_stat_activity a ON a.pid = p.pid
            WHERE p.command = 'VACUUM FULL'
              AND n.nspname NOT IN ('pg_catalog', 'information_schema')
              {toast_filter}
              {activity_filter.replace('usesysid', 'a.usesysid') if activity_filter else ''}
        """

        vacuum_full = await self.sql_driver.execute_query(cluster_query)

        # Check for active autovacuum workers
        autovacuum_query = f"""
            SELECT
                pid,
                datname as database,
                query,
                state,
                EXTRACT(epoch FROM now() - xact_start)::integer as duration_seconds,
                wait_event_type,
                wait_event
            FROM pg_stat_activity
            WHERE backend_type = 'autovacuum worker'
              {activity_filter}
        """

        autovacuum_workers = await self.sql_driver.execute_query(autovacuum_query)

        output = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "vacuum_operations": progress if progress else [],
            "vacuum_full_operations": vacuum_full if vacuum_full else [],
            "autovacuum_workers": autovacuum_workers if autovacuum_workers else [],
            "summary": {
                "active_vacuum_count": len(progress) if progress else 0,
                "active_vacuum_full_count": len(vacuum_full) if vacuum_full else 0,
                "active_autovacuum_workers": len(autovacuum_workers) if autovacuum_workers else 0
            }
        }

        if not progress and not vacuum_full and not autovacuum_workers:
            output["message"] = "No vacuum operations currently in progress"

        return self.format_json_result(output)

    async def _get_tables_needing_vacuum(self, arguments: dict[str, Any]) -> Sequence[TextContent]:
        """Get tables that need vacuuming based on dead tuple count."""
        schema_name = arguments.get("schema_name")
        min_dead_tuples = arguments.get("min_dead_tuples", 1000)
        include_toast = arguments.get("include_toast", False)

        toast_filter = "" if include_toast else "AND c.relname NOT LIKE 'pg_toast%%'"

        if schema_name:
            schema_filter = "AND n.nspname = %s"
            params: list[Any] = [min_dead_tuples, schema_name]
        else:
            schema_filter = ""
            params = [min_dead_tuples]

        query = f"""
            SELECT
                n.nspname as schema_name,
                c.relname as table_name,
                s.n_live_tup,
                s.n_dead_tup,
                CASE
                    WHEN s.n_live_tup > 0
                    THEN ROUND(100.0 * s.n_dead_tup / s.n_live_tup, 2)
                    ELSE 0
                END as dead_tuple_ratio,
                s.last_vacuum,
                s.last_autovacuum,
                s.vacuum_count,
                s.autovacuum_count,
                pg_size_pretty(pg_table_size(c.oid)) as table_size,
                pg_table_size(c.oid) as table_size_bytes,
                -- Calculate if autovacuum threshold is exceeded
                COALESCE(
                    (SELECT setting::integer FROM pg_settings WHERE name = 'autovacuum_vacuum_threshold'),
                    50
                ) +
                COALESCE(
                    (SELECT setting::float FROM pg_settings WHERE name = 'autovacuum_vacuum_scale_factor'),
                    0.2
                ) * s.n_live_tup as autovacuum_threshold,
                CASE
                    WHEN s.n_dead_tup > (
                        COALESCE((SELECT setting::integer FROM pg_settings WHERE name = 'autovacuum_vacuum_threshold'), 50) +
                        COALESCE((SELECT setting::float FROM pg_settings WHERE name = 'autovacuum_vacuum_scale_factor'), 0.2) * s.n_live_tup
                    )
                    THEN true
                    ELSE false
                END as exceeds_threshold
            FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            LEFT JOIN pg_stat_user_tables s ON s.relid = c.oid
            WHERE c.relkind = 'r'
              AND n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
              AND s.n_dead_tup >= %s
              {schema_filter}
              {toast_filter}
            ORDER BY s.n_dead_tup DESC
            LIMIT 50
        """

        results = await self.sql_driver.execute_query(query, params)

        # Get tables with transaction ID wraparound risk
        wraparound_query = """
            SELECT
                n.nspname as schema_name,
                c.relname as table_name,
                age(c.relfrozenxid) as xid_age,
                pg_size_pretty(pg_table_size(c.oid)) as table_size,
                ROUND(100.0 * age(c.relfrozenxid) / 2147483647, 2) as pct_towards_wraparound
            FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE c.relkind = 'r'
              AND n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
              AND age(c.relfrozenxid) > 100000000  -- > 100 million transactions
            ORDER BY age(c.relfrozenxid) DESC
            LIMIT 20
        """

        wraparound_results = await self.sql_driver.execute_query(wraparound_query)

        output = {
            "tables_needing_vacuum": results if results else [],
            "tables_with_wraparound_risk": wraparound_results if wraparound_results else [],
            "summary": {
                "tables_with_dead_tuples": len(results) if results else 0,
                "tables_exceeding_threshold": sum(1 for r in results if r.get("exceeds_threshold")) if results else 0,
                "tables_with_wraparound_risk": len(wraparound_results) if wraparound_results else 0
            },
            "recommendations": []
        }

        # Generate recommendations
        if results:
            high_dead_ratio = [r for r in results if (r.get("dead_tuple_ratio") or 0) > 20]
            if high_dead_ratio:
                output["recommendations"].append({
                    "severity": "high",
                    "message": f"{len(high_dead_ratio)} tables have > 20% dead tuple ratio",
                    "action": "Consider running VACUUM on these tables"
                })

        if wraparound_results:
            critical = [r for r in wraparound_results if (r.get("pct_towards_wraparound") or 0) > 50]
            if critical:
                output["recommendations"].append({
                    "severity": "critical",
                    "message": f"{len(critical)} tables are > 50% towards transaction wraparound",
                    "action": "Urgently run VACUUM FREEZE on these tables"
                })

        return self.format_json_result(output)

    async def _get_autovacuum_status(self, arguments: dict[str, Any]) -> Sequence[TextContent]:
        """Get autovacuum configuration and status."""
        # Get autovacuum settings
        settings_query = """
            SELECT name, setting, unit, short_desc
            FROM pg_settings
            WHERE name LIKE 'autovacuum%'
            ORDER BY name
        """

        settings = await self.sql_driver.execute_query(settings_query)

        # Get user filter
        user_filter = get_user_filter()
        activity_filter = user_filter.get_activity_filter()

        # Get current autovacuum workers
        workers_query = f"""
            SELECT
                pid,
                datname as database,
                SUBSTRING(query FROM 'autovacuum: (.*)') as operation,
                state,
                EXTRACT(epoch FROM now() - xact_start)::integer as duration_seconds,
                wait_event_type,
                wait_event
            FROM pg_stat_activity
            WHERE backend_type = 'autovacuum worker'
              {activity_filter}
        """

        workers = await self.sql_driver.execute_query(workers_query)

        # Get autovacuum launcher status
        launcher_query = f"""
            SELECT
                pid,
                state,
                EXTRACT(epoch FROM now() - backend_start)::integer as uptime_seconds
            FROM pg_stat_activity
            WHERE backend_type = 'autovacuum launcher'
              {activity_filter}
        """

        launcher = await self.sql_driver.execute_query(launcher_query)

        # Calculate effective settings
        max_workers = 3  # default
        naptime = 60  # default (seconds)
        for s in settings or []:
            if s["name"] == "autovacuum_max_workers":
                max_workers = int(s["setting"])
            elif s["name"] == "autovacuum_naptime":
                naptime = int(s["setting"])

        output = {
            "autovacuum_enabled": any(s["setting"] == "on" for s in settings if s["name"] == "autovacuum") if settings else True,
            "settings": settings if settings else [],
            "launcher": launcher[0] if launcher else None,
            "active_workers": workers if workers else [],
            "summary": {
                "max_workers": max_workers,
                "active_worker_count": len(workers) if workers else 0,
                "naptime_seconds": naptime,
                "workers_available": max_workers - (len(workers) if workers else 0)
            }
        }

        # Add warnings
        if output["summary"]["workers_available"] == 0:
            output["warning"] = "All autovacuum workers are busy - consider increasing autovacuum_max_workers"

        return self.format_json_result(output)

    async def _get_recent_vacuum_activity(self, arguments: dict[str, Any]) -> Sequence[TextContent]:
        """Get recent vacuum activity from statistics."""
        schema_name = arguments.get("schema_name")
        include_toast = arguments.get("include_toast", False)

        toast_filter = "" if include_toast else "AND c.relname NOT LIKE 'pg_toast%%'"
        schema_filter = "AND n.nspname = %s" if schema_name else ""

        query = f"""
            SELECT
                n.nspname as schema_name,
                c.relname as table_name,
                s.last_vacuum,
                s.last_autovacuum,
                s.last_analyze,
                s.last_autoanalyze,
                s.vacuum_count,
                s.autovacuum_count,
                s.analyze_count,
                s.autoanalyze_count,
                s.n_live_tup,
                s.n_dead_tup,
                s.n_mod_since_analyze,
                pg_size_pretty(pg_table_size(c.oid)) as table_size,
                GREATEST(s.last_vacuum, s.last_autovacuum) as last_vacuumed,
                CASE
                    WHEN GREATEST(s.last_vacuum, s.last_autovacuum) IS NULL THEN 'never'
                    WHEN GREATEST(s.last_vacuum, s.last_autovacuum) < now() - interval '7 days' THEN 'stale'
                    WHEN GREATEST(s.last_vacuum, s.last_autovacuum) < now() - interval '1 day' THEN 'recent'
                    ELSE 'fresh'
                END as vacuum_status
            FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            LEFT JOIN pg_stat_user_tables s ON s.relid = c.oid
            WHERE c.relkind = 'r'
              AND n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
              {schema_filter}
              {toast_filter}
            ORDER BY GREATEST(s.last_vacuum, s.last_autovacuum) DESC NULLS LAST
            LIMIT 50
        """

        # Build parameters list
        params: list[Any] = []
        if schema_name:
            params.append(schema_name)

        results = await self.sql_driver.execute_query(query, params if params else None)

        if not results:
            return self.format_result(f"No tables found in schema: {schema_name or 'all'}")

        # Categorize tables
        never_vacuumed = [r for r in results if r.get("vacuum_status") == "never"]
        stale = [r for r in results if r.get("vacuum_status") == "stale"]
        recent = [r for r in results if r.get("vacuum_status") == "recent"]
        fresh = [r for r in results if r.get("vacuum_status") == "fresh"]

        output = {
            "recent_activity": results,
            "summary": {
                "total_tables": len(results),
                "never_vacuumed": len(never_vacuumed),
                "stale_vacuum": len(stale),
                "recent_vacuum": len(recent),
                "fresh_vacuum": len(fresh)
            },
            "recommendations": []
        }

        if never_vacuumed:
            output["recommendations"].append({
                "severity": "warning",
                "message": f"{len(never_vacuumed)} tables have never been vacuumed",
                "tables": [t["table_name"] for t in never_vacuumed[:5]]
            })

        if stale:
            output["recommendations"].append({
                "severity": "info",
                "message": f"{len(stale)} tables haven't been vacuumed in over 7 days",
                "tables": [t["table_name"] for t in stale[:5]]
            })

        return self.format_json_result(output)
