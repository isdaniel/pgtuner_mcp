"""Unit tests for CheckReplicationHealthHandler."""

import json
from unittest.mock import AsyncMock

import pytest

from pgtuner_mcp.tools.tools_replication import CheckReplicationHealthHandler


class TestCheckReplicationHealthHandler:
    def test_tool_definition_has_correct_shape(self, mock_sql_driver):
        handler = CheckReplicationHealthHandler(mock_sql_driver)
        tool = handler.get_tool_definition()

        assert tool.name == "check_replication_health"
        assert "replication" in tool.description.lower()
        assert tool.inputSchema["type"] == "object"
        assert "action" in tool.inputSchema["properties"]
        assert tool.inputSchema["properties"]["action"]["enum"] == [
            "slots", "lag", "wal_retention", "logical_subs", "prepared_xacts", "all"
        ]
        assert tool.inputSchema["properties"]["min_lag_mb"]["type"] == "integer"

    @pytest.mark.asyncio
    async def test_slots_action_returns_slot_list(self, mock_sql_driver):
        mock_sql_driver.execute_query = AsyncMock(side_effect=[
            [{"server_version": "16.2"}],
            [
                {
                    "slot_name": "standby_1",
                    "slot_type": "physical",
                    "active": True,
                    "restart_lsn": "0/3000000",
                    "wal_retained_bytes": 50 * 1024 * 1024,
                    "two_phase": False,
                },
                {
                    "slot_name": "dead_slot",
                    "slot_type": "physical",
                    "active": False,
                    "restart_lsn": "0/1000000",
                    "wal_retained_bytes": 500 * 1024 * 1024,
                    "two_phase": False,
                },
            ],
        ])

        handler = CheckReplicationHealthHandler(mock_sql_driver)
        result = await handler.run_tool({"action": "slots", "min_lag_mb": 100})

        payload = json.loads(result[0].text)
        assert len(payload["slots"]) == 2
        assert any("dead_slot" in i for i in payload["issues"])

    @pytest.mark.asyncio
    async def test_slots_action_falls_back_on_pg13(self, mock_sql_driver):
        mock_sql_driver.execute_query = AsyncMock(side_effect=[
            [{"server_version": "13.10"}],
            [{
                "slot_name": "old", "slot_type": "physical", "active": True,
                "restart_lsn": "0/100", "wal_retained_bytes": 0, "two_phase": False,
            }],
        ])
        handler = CheckReplicationHealthHandler(mock_sql_driver)
        result = await handler.run_tool({"action": "slots"})
        payload = json.loads(result[0].text)
        assert payload["pg_version"] == 13
        assert len(payload["slots"]) == 1

    @pytest.mark.asyncio
    async def test_prepared_xacts_flags_orphans(self, mock_sql_driver):
        mock_sql_driver.execute_query = AsyncMock(side_effect=[
            [{"server_version": "16.2"}],
            [{"gid": "stale_gid", "prepared": "2026-01-01", "owner": "app",
              "database": "db", "age_seconds": 9999.0}],
        ])
        handler = CheckReplicationHealthHandler(mock_sql_driver)
        result = await handler.run_tool({"action": "prepared_xacts"})
        payload = json.loads(result[0].text)
        assert any("stale_gid" in i for i in payload["issues"])
        assert any("ROLLBACK PREPARED" in r for r in payload["recommendations"])

    @pytest.mark.asyncio
    async def test_returns_error_on_query_failure(self, mock_sql_driver):
        mock_sql_driver.execute_query = AsyncMock(side_effect=Exception("boom"))
        handler = CheckReplicationHealthHandler(mock_sql_driver)
        result = await handler.run_tool({"action": "slots"})
        assert "Error" in result[0].text
        assert "boom" in result[0].text

    @pytest.mark.asyncio
    async def test_lag_action_flags_lagging_standby(self, mock_sql_driver):
        mock_sql_driver.execute_query = AsyncMock(side_effect=[
            [{"server_version": "16.2"}],
            [{  # pg_stat_replication
                "application_name": "standby_a",
                "client_addr": "10.0.0.5",
                "state": "streaming",
                "sent_lsn": "0/9000000",
                "write_lsn": "0/8500000",
                "flush_lsn": "0/8500000",
                "replay_lsn": "0/1000000",
                "replay_lag_bytes": 500 * 1024 * 1024,
                "replay_lag_seconds": 12.0,
            }],
            [],  # pg_stat_wal_receiver
        ])
        handler = CheckReplicationHealthHandler(mock_sql_driver)
        result = await handler.run_tool({"action": "lag", "min_lag_mb": 100})
        payload = json.loads(result[0].text)
        assert len(payload["replication"]) == 1
        assert any("standby_a" in i and "MB" in i for i in payload["issues"])

    @pytest.mark.asyncio
    async def test_logical_subs_flags_lagging_subscription(self, mock_sql_driver):
        mock_sql_driver.execute_query = AsyncMock(side_effect=[
            [{"server_version": "16.2"}],
            [{
                "subname": "orders_sub", "subenabled": True,
                "received_lsn": "0/100", "latest_end_lsn": "0/200",
                "apply_lag_seconds": 600.0,
            }],
        ])
        handler = CheckReplicationHealthHandler(mock_sql_driver)
        result = await handler.run_tool({"action": "logical_subs"})
        payload = json.loads(result[0].text)
        assert any("orders_sub" in i and "600s" in i for i in payload["issues"])

    @pytest.mark.asyncio
    async def test_all_action_returns_full_shape(self, mock_sql_driver):
        # Order: version, slots, replication, wal_receiver, subscriptions, prepared_xacts
        mock_sql_driver.execute_query = AsyncMock(side_effect=[
            [{"server_version": "16.2"}],
            [],  # slots
            [],  # replication
            [],  # wal_receiver
            [],  # subscriptions
            [],  # prepared_xacts
        ])
        handler = CheckReplicationHealthHandler(mock_sql_driver)
        result = await handler.run_tool({"action": "all"})
        payload = json.loads(result[0].text)
        assert set(payload.keys()) >= {
            "pg_version", "slots", "replication", "wal_receiver",
            "subscriptions", "prepared_xacts", "issues", "recommendations",
        }
        assert payload["issues"] == []
        assert payload["recommendations"] == []

    @pytest.mark.asyncio
    async def test_min_lag_mb_default_is_100(self, mock_sql_driver):
        # Inactive slot retaining 150MB > default 100MB threshold -> should flag
        mock_sql_driver.execute_query = AsyncMock(side_effect=[
            [{"server_version": "16.2"}],
            [{
                "slot_name": "lonely", "slot_type": "physical", "active": False,
                "restart_lsn": "0/1", "wal_retained_bytes": 150 * 1024 * 1024,
                "two_phase": False,
            }],
        ])
        handler = CheckReplicationHealthHandler(mock_sql_driver)
        # NOTE: no min_lag_mb in arguments — should default to 100
        result = await handler.run_tool({"action": "slots"})
        payload = json.loads(result[0].text)
        assert any("lonely" in i for i in payload["issues"])

    @pytest.mark.asyncio
    async def test_active_slot_not_flagged_as_bloat(self, mock_sql_driver):
        """Active slots are serving a live consumer; only inactive slots are flagged
        as bloat to avoid false positives during normal standby resync."""
        mock_sql_driver.execute_query = AsyncMock(side_effect=[
            [{"server_version": "16.2"}],
            [{
                "slot_name": "busy", "slot_type": "physical", "active": True,
                "restart_lsn": "0/1", "wal_retained_bytes": 999 * 1024 * 1024,
                "two_phase": False,
            }],
        ])
        handler = CheckReplicationHealthHandler(mock_sql_driver)
        result = await handler.run_tool({"action": "slots", "min_lag_mb": 100})
        payload = json.loads(result[0].text)
        assert len(payload["slots"]) == 1
        assert payload["issues"] == []
