"""Replication and WAL health monitoring tools."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

from mcp.types import TextContent, Tool

from pgtuner_mcp.services.sql_driver import SqlDriver, get_postgres_version
from pgtuner_mcp.tools.toolhandler import ToolHandler


class CheckReplicationHealthHandler(ToolHandler):
    """Comprehensive replication and WAL health check."""

    name = "check_replication_health"
    description = (
        "Check PostgreSQL replication and WAL health. Inspects replication slots, "
        "standby lag, WAL retention, logical subscriptions, and prepared transactions. "
        "Detects slot bloat (a common outage cause), lagging replicas, and orphaned 2PC."
    )
    title = "Replication & WAL Health"
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
                    "action": {
                        "type": "string",
                        "enum": ["slots", "lag", "wal_retention", "logical_subs",
                                 "prepared_xacts", "all"],
                        "description": "Which subset to check (default: all)",
                        "default": "all",
                    },
                    "min_lag_mb": {
                        "type": "integer",
                        "description": "Threshold MB for flagging slot retention / replica lag",
                        "default": 100,
                        "minimum": 0,
                    },
                },
            },
            annotations=self.get_annotations(),
        )

    async def run_tool(self, arguments: dict[str, Any]) -> Sequence[TextContent]:
        action = arguments.get("action", "all")
        min_lag_mb = int(arguments.get("min_lag_mb", 100))
        min_lag_bytes = min_lag_mb * 1024 * 1024

        try:
            pg_version = await get_postgres_version(self.driver)
            result: dict[str, Any] = {
                "pg_version": pg_version,
                "slots": [],
                "replication": [],
                "wal_receiver": None,
                "subscriptions": [],
                "prepared_xacts": [],
                "issues": [],
                "recommendations": [],
            }

            if action in ("slots", "wal_retention", "all"):
                await self._check_slots(result, pg_version, min_lag_bytes)
            if action in ("lag", "all"):
                await self._check_replication_lag(result, min_lag_bytes)
                await self._check_wal_receiver(result)
            if action in ("logical_subs", "all"):
                await self._check_subscriptions(result, min_lag_bytes)
            if action in ("prepared_xacts", "all"):
                await self._check_prepared_xacts(result)

            return self.format_json_result(result)
        except Exception as e:
            return self.format_error(e)

    async def _check_slots(
        self, result: dict[str, Any], pg_version: int, min_lag_bytes: int
    ) -> None:
        if pg_version >= 14:
            sql = """
                SELECT
                    s.slot_name,
                    s.slot_type,
                    s.active,
                    s.restart_lsn::text AS restart_lsn,
                    pg_wal_lsn_diff(
                        CASE WHEN pg_is_in_recovery()
                             THEN pg_last_wal_replay_lsn()
                             ELSE pg_current_wal_lsn() END,
                        s.restart_lsn
                    ) AS wal_retained_bytes,
                    s.two_phase
                FROM pg_replication_slots s
            """
        else:
            sql = """
                SELECT
                    slot_name,
                    slot_type,
                    active,
                    restart_lsn::text AS restart_lsn,
                    pg_wal_lsn_diff(
                        CASE WHEN pg_is_in_recovery()
                             THEN pg_last_wal_replay_lsn()
                             ELSE pg_current_wal_lsn() END,
                        restart_lsn
                    ) AS wal_retained_bytes,
                    false AS two_phase
                FROM pg_replication_slots
            """
        rows = await self.driver.execute_query(sql) or []
        result["slots"] = rows

        for slot in rows:
            retained = slot.get("wal_retained_bytes") or 0
            if not slot.get("active") and retained > min_lag_bytes:
                result["issues"].append(
                    f"Inactive slot '{slot['slot_name']}' retains "
                    f"{retained // (1024*1024)}MB of WAL"
                )
                result["recommendations"].append(
                    f"Drop inactive slot if unused: "
                    f"SELECT pg_drop_replication_slot('{slot['slot_name']}');"
                )

    async def _check_replication_lag(
        self, result: dict[str, Any], min_lag_bytes: int
    ) -> None:
        sql = """
            SELECT
                application_name,
                client_addr::text AS client_addr,
                state,
                sent_lsn::text AS sent_lsn,
                write_lsn::text AS write_lsn,
                flush_lsn::text AS flush_lsn,
                replay_lsn::text AS replay_lsn,
                pg_wal_lsn_diff(sent_lsn, replay_lsn) AS replay_lag_bytes,
                EXTRACT(EPOCH FROM replay_lag) AS replay_lag_seconds
            FROM pg_stat_replication
        """
        rows = await self.driver.execute_query(sql) or []
        result["replication"] = rows
        for r in rows:
            lag = r.get("replay_lag_bytes") or 0
            if lag > min_lag_bytes:
                result["issues"].append(
                    f"Standby '{r.get('application_name')}' replay-lags by "
                    f"{lag // (1024*1024)}MB"
                )

    async def _check_wal_receiver(self, result: dict[str, Any]) -> None:
        try:
            rows = await self.driver.execute_query(
                "SELECT status, last_msg_send_time, latest_end_lsn::text AS latest_end_lsn, "
                "EXTRACT(EPOCH FROM (now() - last_msg_receipt_time)) AS receipt_lag_seconds "
                "FROM pg_stat_wal_receiver"
            ) or []
            result["wal_receiver"] = rows[0] if rows else None
        except Exception:
            result["wal_receiver"] = None

    async def _check_subscriptions(
        self, result: dict[str, Any], min_lag_bytes: int
    ) -> None:
        sql = """
            SELECT
                s.subname,
                s.subenabled,
                ss.received_lsn::text AS received_lsn,
                ss.latest_end_lsn::text AS latest_end_lsn,
                EXTRACT(EPOCH FROM (now() - ss.last_msg_receipt_time)) AS apply_lag_seconds
            FROM pg_subscription s
            LEFT JOIN pg_stat_subscription ss ON ss.subid = s.oid
        """
        try:
            rows = await self.driver.execute_query(sql) or []
        except Exception:
            rows = []
        result["subscriptions"] = rows
        for r in rows:
            lag = r.get("apply_lag_seconds") or 0
            if lag and lag > 60:
                result["issues"].append(
                    f"Logical sub '{r.get('subname')}' lags by {int(lag)}s"
                )

    async def _check_prepared_xacts(self, result: dict[str, Any]) -> None:
        rows = await self.driver.execute_query(
            "SELECT gid, prepared, owner, database, "
            "EXTRACT(EPOCH FROM (now() - prepared)) AS age_seconds "
            "FROM pg_prepared_xacts"
        ) or []
        result["prepared_xacts"] = rows
        for r in rows:
            age = r.get("age_seconds") or 0
            if age > 300:
                result["issues"].append(
                    f"Prepared xact '{r['gid']}' is {int(age)}s old — likely orphan"
                )
                result["recommendations"].append(
                    f"Connect to database '{r['database']}' and run: "
                    f"ROLLBACK PREPARED '{r['gid']}'; if confirmed stale"
                )
