"""Buffer cache and TOAST storage analysis tools."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

from mcp.types import TextContent, Tool

from pgtuner_mcp.services.sql_driver import (
    SqlDriver,
    check_extension_installed,
    get_postgres_version,
)
from pgtuner_mcp.tools.toolhandler import ToolHandler

_TOP_N_HARD_CAP = 100


class AnalyzeBufferCacheHandler(ToolHandler):
    """Analyze shared_buffers contents via pg_buffercache."""

    name = "analyze_buffer_cache"
    description = (
        "Inspect shared_buffers contents using pg_buffercache extension. "
        "Returns top-N relations by pages cached, % of shared_buffers, "
        "dirty page counts, and avg usagecount. Requires pg_buffercache."
    )
    title = "Buffer Cache Analysis"
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
                    "top_n": {
                        "type": "integer",
                        "description": "Top-N relations (max 100, scan is O(shared_buffers))",
                        "default": 20,
                        "minimum": 1,
                        "maximum": _TOP_N_HARD_CAP,
                    },
                    "schema_name": {"type": "string", "default": "public"},
                    "include_indexes": {"type": "boolean", "default": True},
                },
            },
            annotations=self.get_annotations(),
        )

    async def run_tool(self, arguments: dict[str, Any]) -> Sequence[TextContent]:
        top_n = min(int(arguments.get("top_n", 20)), _TOP_N_HARD_CAP)
        schema = arguments.get("schema_name", "public")
        include_indexes = bool(arguments.get("include_indexes", True))

        try:
            if not await check_extension_installed(self.driver, "pg_buffercache"):
                return self.format_json_result({
                    "error": "pg_buffercache extension not installed",
                    "remediation": "CREATE EXTENSION pg_buffercache; (requires superuser)",
                })

            rk_filter = "('r','i')" if include_indexes else "('r')"
            sql = f"""
                WITH sb AS (
                    SELECT setting::bigint * 8192 AS bytes
                    FROM pg_settings WHERE name = 'shared_buffers'
                ),
                bc AS (
                    SELECT
                        c.relname AS relation,
                        n.nspname AS schema,
                        CASE c.relkind WHEN 'r' THEN 'table' WHEN 'i' THEN 'index'
                                       ELSE c.relkind::text END AS kind,
                        COUNT(*) AS pages,
                        COUNT(*) FILTER (WHERE b.isdirty) AS dirty_pages,
                        AVG(b.usagecount)::numeric(10,2) AS avg_usagecount
                    FROM pg_buffercache b
                    JOIN pg_class c ON b.relfilenode = pg_relation_filenode(c.oid)
                    JOIN pg_namespace n ON n.oid = c.relnamespace
                    WHERE n.nspname = %s
                      AND c.relkind IN {rk_filter}
                      AND n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
                    GROUP BY c.relname, n.nspname, c.relkind
                )
                SELECT bc.*, (bc.pages * 8192.0 / 1024 / 1024)::numeric(10,2) AS mb_cached,
                       ((bc.pages * 8192.0 / sb.bytes) * 100)::numeric(5,2)
                           AS pct_of_shared_buffers
                FROM bc, sb
                ORDER BY bc.pages DESC
                LIMIT %s
            """
            rows = await self.driver.execute_query(sql, [schema, top_n]) or []
            sb_row = await self.driver.execute_query(
                "SELECT (setting::bigint * 8192 / 1024 / 1024) AS mb "
                "FROM pg_settings WHERE name='shared_buffers'"
            )
            shared_buffers_mb = sb_row[0]["mb"] if sb_row else None

            dirty_total = sum((r.get("dirty_pages") or 0) for r in rows)
            recs: list[str] = []
            if dirty_total > 1000:
                recs.append(
                    f"{dirty_total} dirty pages cached — consider tuning "
                    "checkpoint_timeout / max_wal_size if you see checkpoint spikes"
                )

            return self.format_json_result({
                "shared_buffers_mb": shared_buffers_mb,
                "top_relations": rows,
                "dirty_summary": {"total_dirty_pages": dirty_total},
                "recommendations": recs,
            })
        except Exception as e:
            return self.format_error(e)


class AnalyzeToastStorageHandler(ToolHandler):
    """Analyze TOAST storage strategies and compression."""

    name = "analyze_toast_storage"
    description = (
        "Inspect TOAST storage for tables: per-column storage strategy, "
        "compression method (PGLZ vs LZ4 on PG14+), and TOAST relation size. "
        "Recommends LZ4 for EXTENDED columns on PG14+ when current is PGLZ."
    )
    title = "TOAST Storage Analysis"
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
                    "schema_name": {"type": "string", "default": "public"},
                    "table_name": {"type": ["string", "null"], "default": None},
                    "top_n": {"type": "integer", "default": 20, "minimum": 1, "maximum": 100},
                },
            },
            annotations=self.get_annotations(),
        )

    async def run_tool(self, arguments: dict[str, Any]) -> Sequence[TextContent]:
        schema = arguments.get("schema_name", "public")
        table = arguments.get("table_name")
        top_n = min(int(arguments.get("top_n", 20)), 100)

        try:
            pg_version = await get_postgres_version(self.driver)

            table_filter = "AND c.relname = %s" if table else ""
            params: list[Any] = [schema]
            if table:
                params.append(table)
            params.append(top_n)

            tables_sql = f"""
                SELECT c.oid AS reloid, c.relname AS table_name, n.nspname AS schema,
                       c.reltoastrelid AS toast_oid,
                       CASE WHEN c.reltoastrelid <> 0
                            THEN (pg_relation_size(c.reltoastrelid) / 1024.0 / 1024.0)::numeric(12,2)
                            ELSE 0 END AS toast_size_mb
                FROM pg_class c
                JOIN pg_namespace n ON n.oid = c.relnamespace
                WHERE n.nspname = %s AND c.relkind = 'r' {table_filter}
                ORDER BY toast_size_mb DESC NULLS LAST
                LIMIT %s
            """
            tables = await self.driver.execute_query(tables_sql, params) or []

            results: list[dict[str, Any]] = []
            recs: list[str] = []
            for t in tables:
                cols_sql = """
                    SELECT a.attname AS name,
                           pg_catalog.format_type(a.atttypid, a.atttypmod) AS type,
                           a.attstorage AS storage,
                           a.attcompression AS compression
                    FROM pg_attribute a
                    JOIN pg_type ty ON ty.oid = a.atttypid
                    WHERE a.attrelid = %s
                      AND a.attnum > 0 AND NOT a.attisdropped
                      AND ty.typlen = -1
                """
                cols = await self.driver.execute_query(cols_sql, [t["reloid"]]) or []
                col_payload = []
                for c in cols:
                    storage = {"p": "PLAIN", "e": "EXTERNAL",
                               "m": "MAIN", "x": "EXTENDED"}.get(c["storage"], c["storage"])
                    compression = {"p": "pglz", "l": "lz4", " ": "default", "": "default"}.get(
                        c["compression"] or " ", str(c["compression"])
                    )
                    rec = None
                    if (pg_version >= 14 and storage == "EXTENDED"
                            and compression == "pglz" and (t.get("toast_size_mb") or 0) > 10):
                        rec = (f"Consider ALTER TABLE {t['schema']}.{t['table_name']} "
                               f"ALTER COLUMN {c['name']} SET COMPRESSION lz4;")
                        recs.append(rec)
                    col_payload.append({
                        "name": c["name"], "type": c["type"],
                        "storage": storage, "compression": compression,
                        "recommendation": rec,
                    })
                if col_payload:
                    results.append({
                        "schema": t["schema"], "table": t["table_name"],
                        "toast_size_mb": t["toast_size_mb"],
                        "columns": col_payload,
                    })

            return self.format_json_result({
                "pg_version": pg_version,
                "tables": results,
                "summary": {"tables_analyzed": len(results),
                            "recommendations_count": len(recs)},
                "recommendations": recs,
            })
        except Exception as e:
            return self.format_error(e)
