"""Database health and diagnostics tool handlers."""

from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime, timezone
from typing import Any

from mcp.types import TextContent, Tool

from ..services import SqlDriver, get_user_filter
from .toolhandler import ToolHandler


class DatabaseHealthToolHandler(ToolHandler):
    """Tool handler for overall database health assessment."""

    name = "check_database_health"
    title = "PostgreSQL Health Check"
    read_only_hint = True
    destructive_hint = False
    idempotent_hint = True
    open_world_hint = False
    description = """Perform a comprehensive database health check.

Note: This tool focuses on user/client tables and excludes PostgreSQL
system tables (pg_catalog, information_schema, pg_toast) from analysis.

Analyzes multiple aspects of PostgreSQL health:
- Connection statistics and pool usage
- Cache hit ratios (buffer and index)
- Lock contention and blocking queries
- Replication status (if configured)
- Transaction wraparound risk
- Disk space usage
- Background writer statistics
- Checkpoint frequency

Returns a health score with detailed breakdown and recommendations."""

    def __init__(self, sql_driver: SqlDriver):
        self.sql_driver = sql_driver

    def get_tool_definition(self) -> Tool:
        return Tool(
            name=self.name,
            description=self.description,
            inputSchema={
                "type": "object",
                "properties": {
                    "include_recommendations": {
                        "type": "boolean",
                        "description": "Include actionable recommendations",
                        "default": True
                    },
                    "verbose": {
                        "type": "boolean",
                        "description": "Include detailed statistics",
                        "default": False
                    }
                },
                "required": []
            },
            annotations=self.get_annotations()
        )

    async def run_tool(self, arguments: dict[str, Any]) -> Sequence[TextContent]:
        try:
            include_recommendations = arguments.get("include_recommendations", True)
            verbose = arguments.get("verbose", False)

            health = {
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "checks": {},
                "overall_score": 0,
                "issues": [],
                "recommendations": []
            }

            # Run all health checks
            await self._check_connections(health)
            await self._check_cache_ratios(health)
            await self._check_locks(health)
            await self._check_replication(health)
            await self._check_wraparound(health)
            await self._check_disk_usage(health)
            await self._check_bgwriter(health)
            await self._check_checkpoints(health)

            # Calculate overall score
            scores = [
                check.get("score", 100)
                for check in health["checks"].values()
            ]
            health["overall_score"] = round(sum(scores) / len(scores), 1) if scores else 0

            # Determine health status
            if health["overall_score"] >= 90:
                health["status"] = "healthy"
            elif health["overall_score"] >= 70:
                health["status"] = "warning"
            else:
                health["status"] = "critical"

            if not include_recommendations:
                health.pop("recommendations", None)

            if not verbose:
                # Remove detailed stats from each check
                for check in health["checks"].values():
                    check.pop("details", None)

            return self.format_json_result(health)

        except Exception as e:
            return self.format_error(e)

    async def _check_connections(self, health: dict) -> None:
        """Check connection statistics."""
        user_filter = get_user_filter()
        activity_filter = user_filter.get_activity_filter()

        query = f"""
            SELECT
                max_conn,
                used,
                res_for_super,
                ROUND(100.0 * used / max_conn, 1) as used_pct
            FROM (
                SELECT
                    setting::int as max_conn
                FROM pg_settings WHERE name = 'max_connections'
            ) m,
            (
                SELECT
                    COUNT(*) as used
                FROM pg_stat_activity
                WHERE 1=1 {activity_filter}
            ) u,
            (
                SELECT
                    setting::int as res_for_super
                FROM pg_settings WHERE name = 'superuser_reserved_connections'
            ) r
        """
        result = await self.sql_driver.execute_query(query)

        if result:
            row = result[0]
            used_pct = row.get("used_pct", 0)

            score = 100
            if used_pct > 90:
                score = 30
                health["issues"].append("Critical: Connection usage above 90%")
                health["recommendations"].append("Increase max_connections or use connection pooling (pgbouncer)")
            elif used_pct > 75:
                score = 70
                health["issues"].append("Warning: Connection usage above 75%")

            health["checks"]["connections"] = {
                "score": score,
                "used_percent": used_pct,
                "details": row
            }

    async def _check_cache_ratios(self, health: dict) -> None:
        """Check buffer and index cache hit ratios."""
        query = """
            SELECT
                ROUND(100.0 * sum(heap_blks_hit) / nullif(sum(heap_blks_hit) + sum(heap_blks_read), 0), 2) as buffer_hit_ratio,
                ROUND(100.0 * sum(idx_blks_hit) / nullif(sum(idx_blks_hit) + sum(idx_blks_read), 0), 2) as index_hit_ratio
            FROM pg_statio_user_tables
        """
        result = await self.sql_driver.execute_query(query)

        if result:
            row = result[0]
            buffer_ratio = row.get("buffer_hit_ratio") or 0
            index_ratio = row.get("index_hit_ratio") or 0

            score = 100
            if buffer_ratio < 90:
                score -= 30
                health["issues"].append(f"Buffer cache hit ratio is low: {buffer_ratio}%")
                health["recommendations"].append("Consider increasing shared_buffers")
            if index_ratio < 95:
                score -= 20
                health["issues"].append(f"Index cache hit ratio is low: {index_ratio}%")

            health["checks"]["cache"] = {
                "score": max(0, score),
                "buffer_hit_ratio": buffer_ratio,
                "index_hit_ratio": index_ratio,
                "details": row
            }

    async def _check_locks(self, health: dict) -> None:
        """Check for lock contention."""
        query = """
            SELECT
                COUNT(*) as total_locks,
                COUNT(*) FILTER (WHERE NOT granted) as waiting_locks,
                COUNT(DISTINCT pid) FILTER (WHERE NOT granted) as waiting_processes
            FROM pg_locks
        """
        result = await self.sql_driver.execute_query(query)

        # Check for blocking queries
        user_filter = get_user_filter()
        activity_filter = user_filter.get_activity_filter()

        blocking_query = f"""
            SELECT COUNT(*) as blocking_count
            FROM pg_stat_activity
            WHERE wait_event_type = 'Lock'
              AND state = 'active'
              {activity_filter}
        """
        blocking_result = await self.sql_driver.execute_query(blocking_query)

        if result:
            row = result[0]
            waiting = row.get("waiting_locks", 0) or 0
            blocking = blocking_result[0].get("blocking_count", 0) if blocking_result else 0

            score = 100
            if waiting > 10:
                score -= 40
                health["issues"].append(f"High lock contention: {waiting} locks waiting")
            elif waiting > 5:
                score -= 20

            if blocking > 0:
                score -= 30
                health["issues"].append(f"{blocking} queries blocked by locks")
                health["recommendations"].append("Investigate blocking queries using pg_blocking_pids()")

            health["checks"]["locks"] = {
                "score": max(0, score),
                "waiting_locks": waiting,
                "blocking_queries": blocking,
                "details": row
            }

    async def _check_replication(self, health: dict) -> None:
        """Check replication status if configured."""
        query = """
            SELECT
                client_addr,
                state,
                sent_lsn,
                write_lsn,
                flush_lsn,
                replay_lsn,
                pg_wal_lsn_diff(sent_lsn, replay_lsn) as replication_lag_bytes
            FROM pg_stat_replication
        """
        result = await self.sql_driver.execute_query(query)

        score = 100
        if result:
            max_lag = max(r.get("replication_lag_bytes", 0) or 0 for r in result)

            if max_lag > 100 * 1024 * 1024:  # 100MB
                score = 50
                health["issues"].append(f"High replication lag: {max_lag / 1024 / 1024:.1f}MB")
            elif max_lag > 10 * 1024 * 1024:  # 10MB
                score = 80
                health["issues"].append(f"Moderate replication lag: {max_lag / 1024 / 1024:.1f}MB")

            health["checks"]["replication"] = {
                "score": score,
                "replica_count": len(result),
                "max_lag_bytes": max_lag,
                "details": result
            }
        else:
            health["checks"]["replication"] = {
                "score": 100,
                "replica_count": 0,
                "message": "No replication configured or this is a replica"
            }

    async def _check_wraparound(self, health: dict) -> None:
        """Check transaction ID wraparound risk."""
        query = """
            SELECT
                datname,
                age(datfrozenxid) as xid_age,
                2147483647 - age(datfrozenxid) as xids_remaining,
                ROUND(100.0 * age(datfrozenxid) / 2147483647, 2) as pct_towards_wraparound
            FROM pg_database
            WHERE datname NOT IN ('template0', 'template1')
            ORDER BY age(datfrozenxid) DESC
            LIMIT 5
        """
        result = await self.sql_driver.execute_query(query)

        if result:
            max_pct = max(r.get("pct_towards_wraparound", 0) or 0 for r in result)

            score = 100
            if max_pct > 75:
                score = 20
                health["issues"].append(f"Critical: Transaction wraparound at {max_pct}%")
                health["recommendations"].append("Urgently run VACUUM FREEZE on affected databases")
            elif max_pct > 50:
                score = 70
                health["issues"].append(f"Warning: Transaction wraparound at {max_pct}%")
                health["recommendations"].append("Schedule VACUUM FREEZE to prevent wraparound")

            health["checks"]["wraparound"] = {
                "score": score,
                "max_percent": max_pct,
                "details": result
            }

    async def _check_disk_usage(self, health: dict) -> None:
        """Check database disk usage."""
        query = """
            SELECT
                pg_database.datname,
                pg_size_pretty(pg_database_size(pg_database.datname)) as size,
                pg_database_size(pg_database.datname) as size_bytes
            FROM pg_database
            WHERE datname NOT IN ('template0', 'template1')
            ORDER BY pg_database_size(pg_database.datname) DESC
        """
        result = await self.sql_driver.execute_query(query)

        total_size = sum(r.get("size_bytes", 0) or 0 for r in result) if result else 0

        health["checks"]["disk_usage"] = {
            "score": 100,  # Can't determine actual disk capacity from SQL
            "total_database_size_bytes": total_size,
            "total_database_size": f"{total_size / 1024 / 1024 / 1024:.2f} GB",
            "details": result
        }

    async def _check_bgwriter(self, health: dict) -> None:
        """Check background writer statistics."""
        query = """
            SELECT
                checkpoints_timed,
                checkpoints_req,
                CASE WHEN checkpoints_timed + checkpoints_req > 0
                     THEN ROUND(100.0 * checkpoints_timed / (checkpoints_timed + checkpoints_req), 1)
                     ELSE 100 END as timed_pct,
                buffers_checkpoint,
                buffers_clean,
                buffers_backend,
                CASE WHEN buffers_checkpoint + buffers_clean + buffers_backend > 0
                     THEN ROUND(100.0 * buffers_backend / (buffers_checkpoint + buffers_clean + buffers_backend), 1)
                     ELSE 0 END as backend_pct
            FROM pg_stat_bgwriter
        """
        result = await self.sql_driver.execute_query(query)

        if result:
            row = result[0]
            timed_pct = row.get("timed_pct", 100) or 100
            backend_pct = row.get("backend_pct", 0) or 0

            score = 100
            if timed_pct < 90:
                score -= 20
                health["issues"].append(f"Too many requested checkpoints: {100 - timed_pct}% not timed")
                health["recommendations"].append("Consider increasing checkpoint_timeout or max_wal_size")

            if backend_pct > 20:
                score -= 30
                health["issues"].append(f"Backend processes doing {backend_pct}% of buffer writes")
                health["recommendations"].append("Increase shared_buffers or bgwriter settings")

            health["checks"]["bgwriter"] = {
                "score": max(0, score),
                "timed_checkpoint_pct": timed_pct,
                "backend_write_pct": backend_pct,
                "details": row
            }

    async def _check_checkpoints(self, health: dict) -> None:
        """Check checkpoint frequency and duration."""
        query = """
            SELECT
                total_checkpoints,
                seconds_since_start,
                CASE WHEN seconds_since_start > 0
                     THEN ROUND(3600.0 * total_checkpoints / seconds_since_start, 2)
                     ELSE 0 END as checkpoints_per_hour
            FROM (
                SELECT
                    checkpoints_timed + checkpoints_req as total_checkpoints,
                    EXTRACT(epoch FROM now() - stats_reset) as seconds_since_start
                FROM pg_stat_bgwriter
            ) s
        """
        result = await self.sql_driver.execute_query(query)

        if result:
            row = result[0]
            cp_per_hour = row.get("checkpoints_per_hour", 0) or 0

            score = 100
            if cp_per_hour > 6:  # More than 6 per hour is concerning
                score = 70
                health["issues"].append(f"High checkpoint frequency: {cp_per_hour}/hour")
                health["recommendations"].append("Increase checkpoint_timeout or max_wal_size to reduce checkpoint frequency")

            health["checks"]["checkpoints"] = {
                "score": score,
                "checkpoints_per_hour": cp_per_hour,
                "details": row
            }


class ActiveQueriesToolHandler(ToolHandler):
    """Tool handler for viewing active queries and connections."""

    name = "get_active_queries"
    title = "Active Queries Monitor"
    read_only_hint = True
    destructive_hint = False
    idempotent_hint = True
    open_world_hint = False
    description = """Get information about currently active queries and connections.

Note: By default, this tool excludes system/background processes and focuses
on client backend queries to help you analyze your application's query patterns.
System catalog queries are filtered out unless explicitly requested.

Shows:
- All active queries and their duration
- Idle transactions that may be holding locks
- Blocked queries waiting for locks
- Connection state breakdown

Useful for:
- Identifying long-running queries
- Finding queries that might need optimization
- Detecting stuck transactions
- Troubleshooting lock contention"""

    def __init__(self, sql_driver: SqlDriver):
        self.sql_driver = sql_driver

    def get_tool_definition(self) -> Tool:
        return Tool(
            name=self.name,
            description=self.description,
            inputSchema={
                "type": "object",
                "properties": {
                    "min_duration_seconds": {
                        "type": "integer",
                        "description": "Minimum query duration in seconds to include",
                        "default": 0
                    },
                    "include_idle": {
                        "type": "boolean",
                        "description": "Include idle connections",
                        "default": False
                    },
                    "include_system": {
                        "type": "boolean",
                        "description": "Include system/background processes",
                        "default": False
                    },
                    "database": {
                        "type": "string",
                        "description": "Filter by specific database"
                    }
                },
                "required": []
            },
            annotations=self.get_annotations()
        )

    async def run_tool(self, arguments: dict[str, Any]) -> Sequence[TextContent]:
        try:
            min_duration = arguments.get("min_duration_seconds", 0)
            include_idle = arguments.get("include_idle", False)
            include_system = arguments.get("include_system", False)
            database = arguments.get("database")

            # Get user filter for excluding specific user IDs
            user_filter = get_user_filter()
            activity_filter = user_filter.get_activity_filter()

            # Build filters
            filters = []
            params = []

            if not include_idle:
                filters.append("state != 'idle'")

            if not include_system:
                filters.append("backend_type = 'client backend'")
                # Also exclude queries against system catalogs
                filters.append("query NOT LIKE '%%pg_catalog%%'")
                filters.append("query NOT LIKE '%%information_schema%%'")

            if database:
                filters.append("datname = %s")
                params.append(database)

            if min_duration > 0:
                filters.append("EXTRACT(epoch FROM now() - query_start) >= %s")
                params.append(min_duration)

            # Build where clause with user filter
            where_parts = filters.copy()
            if activity_filter:
                # Remove leading "AND " from the filter
                where_parts.append(activity_filter.lstrip("AND ").strip())

            where_clause = "WHERE " + " AND ".join(where_parts) if where_parts else ""

            query = f"""
                SELECT
                    pid,
                    datname as database,
                    usename as username,
                    client_addr,
                    state,
                    wait_event_type,
                    wait_event,
                    EXTRACT(epoch FROM now() - query_start)::integer as duration_seconds,
                    EXTRACT(epoch FROM now() - xact_start)::integer as transaction_seconds,
                    LEFT(query, 500) as query,
                    backend_type
                FROM pg_stat_activity
                {where_clause}
                ORDER BY
                    CASE WHEN state = 'active' THEN 0 ELSE 1 END,
                    query_start ASC
            """

            result = await self.sql_driver.execute_query(query, params if params else None)

            # Get summary statistics (with user filter)
            summary_query = f"""
                SELECT
                    state,
                    COUNT(*) as count
                FROM pg_stat_activity
                WHERE backend_type = 'client backend'
                  {activity_filter}
                GROUP BY state
            """
            summary = await self.sql_driver.execute_query(summary_query)

            # Find blocked queries (with user filter)
            blocked_query = f"""
                SELECT
                    blocked.pid as blocked_pid,
                    blocked.query as blocked_query,
                    blocking.pid as blocking_pid,
                    blocking.query as blocking_query,
                    blocked.wait_event_type,
                    blocked.wait_event
                FROM pg_stat_activity blocked
                JOIN pg_stat_activity blocking ON blocking.pid = ANY(pg_blocking_pids(blocked.pid))
                WHERE blocked.wait_event_type = 'Lock'
                  {activity_filter.replace('usesysid', 'blocked.usesysid') if activity_filter else ''}
                LIMIT 10
            """
            blocked = await self.sql_driver.execute_query(blocked_query)

            output = {
                "summary": {
                    "by_state": {row["state"]: row["count"] for row in summary} if summary else {},
                    "total_connections": len(result) if result else 0
                },
                "active_queries": result,
                "blocked_queries": blocked if blocked else []
            }

            # Add warnings for long-running queries
            warnings = []
            if result:
                for row in result:
                    duration = row.get("duration_seconds", 0) or 0
                    if duration > 300:  # 5 minutes
                        warnings.append(
                            f"Long-running query (PID {row['pid']}): {duration}s - Consider investigating"
                        )
                    if row.get("state") == "idle in transaction" and row.get("transaction_seconds", 0) > 60:
                        warnings.append(
                            f"Idle transaction (PID {row['pid']}): {row['transaction_seconds']}s - May be holding locks"
                        )

            if warnings:
                output["warnings"] = warnings

            return self.format_json_result(output)

        except Exception as e:
            return self.format_error(e)


class WaitEventsToolHandler(ToolHandler):
    """Tool handler for analyzing wait events."""

    name = "analyze_wait_events"
    title = "Wait Events Analyzer"
    read_only_hint = True
    destructive_hint = False
    idempotent_hint = True
    open_world_hint = False
    description = """Analyze PostgreSQL wait events to identify bottlenecks.

Note: This tool focuses on client backend processes and excludes system
background processes to help identify bottlenecks in your application queries.

Wait events indicate what processes are waiting for:
- Lock: Waiting for locks on tables/rows
- IO: Waiting for disk I/O
- CPU: Waiting for CPU time
- Client: Waiting for client communication
- Extension: Waiting in extension code

This helps identify:
- I/O bottlenecks
- Lock contention patterns
- Resource saturation"""

    def __init__(self, sql_driver: SqlDriver):
        self.sql_driver = sql_driver

    def get_tool_definition(self) -> Tool:
        return Tool(
            name=self.name,
            description=self.description,
            inputSchema={
                "type": "object",
                "properties": {
                    "active_only": {
                        "type": "boolean",
                        "description": "Only include active (running) queries",
                        "default": True
                    }
                },
                "required": []
            },
            annotations=self.get_annotations()
        )

    async def run_tool(self, arguments: dict[str, Any]) -> Sequence[TextContent]:
        try:
            active_only = arguments.get("active_only", True)

            # Get user filter for excluding specific user IDs
            user_filter = get_user_filter()
            activity_filter = user_filter.get_activity_filter()

            state_filter = "AND state = 'active'" if active_only else ""

            # Current wait events
            query = f"""
                SELECT
                    wait_event_type,
                    wait_event,
                    COUNT(*) as count,
                    array_agg(DISTINCT pid) as pids
                FROM pg_stat_activity
                WHERE wait_event IS NOT NULL
                  AND backend_type = 'client backend'
                  {state_filter}
                  {activity_filter}
                GROUP BY wait_event_type, wait_event
                ORDER BY count DESC
            """

            result = await self.sql_driver.execute_query(query)

            # Detailed breakdown by wait type
            type_query = f"""
                SELECT
                    wait_event_type,
                    COUNT(*) as count
                FROM pg_stat_activity
                WHERE wait_event_type IS NOT NULL
                  AND backend_type = 'client backend'
                  {state_filter}
                  {activity_filter}
                GROUP BY wait_event_type
                ORDER BY count DESC
            """

            type_result = await self.sql_driver.execute_query(type_query)

            analysis = {
                "issues": [],
                "recommendations": []
            }

            # Analyze wait types
            if type_result:
                for row in type_result:
                    wait_type = row.get("wait_event_type")
                    count = row.get("count", 0)

                    if wait_type == "Lock" and count > 5:
                        analysis["issues"].append(f"{count} processes waiting on locks")
                        analysis["recommendations"].append(
                            "Investigate lock contention using pg_locks and pg_blocking_pids()"
                        )
                    elif wait_type == "IO" and count > 10:
                        analysis["issues"].append(f"{count} processes waiting on I/O")
                        analysis["recommendations"].append(
                            "Consider tuning I/O settings or increasing shared_buffers"
                        )
                    elif wait_type == "BufferPin" and count > 0:
                        analysis["issues"].append(f"{count} processes waiting on buffer pins")
                        analysis["recommendations"].append(
                            "This may indicate contention on frequently accessed pages"
                        )

            output = {
                "wait_events": result,
                "by_type": type_result,
                "analysis": analysis
            }

            return self.format_json_result(output)

        except Exception as e:
            return self.format_error(e)


class DatabaseSettingsToolHandler(ToolHandler):
    """Tool handler for reviewing and recommending database settings."""

    name = "review_settings"
    title = "Configuration Settings Reviewer"
    read_only_hint = True
    destructive_hint = False
    idempotent_hint = True
    open_world_hint = False
    description = """Review PostgreSQL configuration settings and get recommendations.

Analyzes key performance-related settings:
- Memory settings (shared_buffers, work_mem, etc.)
- Checkpoint settings
- WAL settings
- Autovacuum settings
- Connection settings

Compares against best practices and system resources."""

    def __init__(self, sql_driver: SqlDriver):
        self.sql_driver = sql_driver

    def get_tool_definition(self) -> Tool:
        return Tool(
            name=self.name,
            description=self.description,
            inputSchema={
                "type": "object",
                "properties": {
                    "category": {
                        "type": "string",
                        "description": "Category of settings to review",
                        "enum": ["all", "memory", "checkpoint", "wal", "autovacuum", "connections"],
                        "default": "all"
                    },
                    "include_all_settings": {
                        "type": "boolean",
                        "description": "Include all settings, not just performance-related ones",
                        "default": False
                    }
                },
                "required": []
            },
            annotations=self.get_annotations()
        )

    async def run_tool(self, arguments: dict[str, Any]) -> Sequence[TextContent]:
        try:
            category = arguments.get("category", "all")
            include_all = arguments.get("include_all_settings", False)

            # Define important settings by category
            setting_categories = {
                "memory": [
                    "shared_buffers", "work_mem", "maintenance_work_mem",
                    "effective_cache_size", "huge_pages", "temp_buffers"
                ],
                "checkpoint": [
                    "checkpoint_timeout", "checkpoint_completion_target",
                    "checkpoint_warning", "max_wal_size", "min_wal_size"
                ],
                "wal": [
                    "wal_level", "wal_buffers", "wal_compression",
                    "synchronous_commit", "fsync", "full_page_writes"
                ],
                "autovacuum": [
                    "autovacuum", "autovacuum_max_workers",
                    "autovacuum_naptime", "autovacuum_vacuum_threshold",
                    "autovacuum_analyze_threshold", "autovacuum_vacuum_scale_factor",
                    "autovacuum_analyze_scale_factor", "autovacuum_vacuum_cost_limit"
                ],
                "connections": [
                    "max_connections", "superuser_reserved_connections",
                    "tcp_keepalives_idle", "tcp_keepalives_interval",
                    "statement_timeout", "idle_in_transaction_session_timeout"
                ]
            }

            if category == "all":
                settings_to_check = [s for cat in setting_categories.values() for s in cat]
            else:
                settings_to_check = setting_categories.get(category, [])

            if include_all:
                query = """
                    SELECT
                        name, setting, unit, category, short_desc,
                        context, vartype, source, boot_val, reset_val
                    FROM pg_settings
                    ORDER BY category, name
                """
                result = await self.sql_driver.execute_query(query)
            else:
                placeholders = ",".join(["%s"] * len(settings_to_check))
                query = f"""
                    SELECT
                        name, setting, unit, category, short_desc,
                        context, vartype, source, boot_val, reset_val
                    FROM pg_settings
                    WHERE name IN ({placeholders})
                    ORDER BY category, name
                """
                result = await self.sql_driver.execute_query(query, settings_to_check)

            # Analyze settings and generate recommendations
            recommendations = []
            settings_dict = {r["name"]: r for r in result} if result else {}

            # Memory recommendations
            if "shared_buffers" in settings_dict:
                sb = settings_dict["shared_buffers"]
                sb_value = self._parse_size(sb["setting"], sb.get("unit"))
                if sb_value < 128 * 1024 * 1024:  # Less than 128MB
                    recommendations.append({
                        "setting": "shared_buffers",
                        "current": sb["setting"] + (sb.get("unit") or ""),
                        "recommendation": "Consider increasing to at least 25% of system RAM",
                        "severity": "high"
                    })

            if "work_mem" in settings_dict:
                wm = settings_dict["work_mem"]
                wm_value = self._parse_size(wm["setting"], wm.get("unit"))
                if wm_value < 4 * 1024 * 1024:  # Less than 4MB
                    recommendations.append({
                        "setting": "work_mem",
                        "current": wm["setting"] + (wm.get("unit") or ""),
                        "recommendation": "Consider increasing for complex queries (4MB-64MB typical)",
                        "severity": "medium"
                    })

            if "effective_cache_size" in settings_dict:
                ec = settings_dict["effective_cache_size"]
                recommendations.append({
                    "setting": "effective_cache_size",
                    "current": ec["setting"] + (ec.get("unit") or ""),
                    "recommendation": "Should be ~75% of total system RAM for dedicated DB servers",
                    "severity": "info"
                })

            # Checkpoint recommendations
            if "checkpoint_completion_target" in settings_dict:
                cct = settings_dict["checkpoint_completion_target"]
                if float(cct["setting"]) < 0.9:
                    recommendations.append({
                        "setting": "checkpoint_completion_target",
                        "current": cct["setting"],
                        "recommendation": "Consider increasing to 0.9 to spread checkpoint I/O",
                        "severity": "medium"
                    })

            # Autovacuum recommendations
            if "autovacuum" in settings_dict:
                av = settings_dict["autovacuum"]
                if av["setting"] == "off":
                    recommendations.append({
                        "setting": "autovacuum",
                        "current": "off",
                        "recommendation": "CRITICAL: Enable autovacuum to prevent transaction wraparound",
                        "severity": "critical"
                    })

            output = {
                "settings": result,
                "recommendations": recommendations,
                "category_analyzed": category
            }

            return self.format_json_result(output)

        except Exception as e:
            return self.format_error(e)

    def _parse_size(self, value: str, unit: str | None) -> int:
        """Parse a size value to bytes."""
        try:
            num = int(value)
            multipliers = {
                "B": 1, "kB": 1024, "MB": 1024**2, "GB": 1024**3,
                "8kB": 8 * 1024, "16kB": 16 * 1024, "32kB": 32 * 1024
            }
            return num * multipliers.get(unit or "B", 1)
        except (ValueError, TypeError):
            return 0
