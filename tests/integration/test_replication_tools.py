import json

import pytest

pytestmark = pytest.mark.integration


@pytest.mark.asyncio
@pytest.mark.parametrize("action", ["slots", "lag", "wal_retention",
                                      "logical_subs", "prepared_xacts", "all"])
async def test_check_replication_health_all_actions(live_driver, action):
    from pgtuner_mcp.tools.tools_replication import CheckReplicationHealthHandler
    h = CheckReplicationHealthHandler(live_driver)
    result = await h.run_tool({"action": action, "min_lag_mb": 100})
    payload = json.loads(result[0].text)
    assert "pg_version" in payload


@pytest.mark.asyncio
async def test_replication_health_detects_prepared_xact(live_driver):
    # Prepare a stale 2PC, check it surfaces, then clean up
    await live_driver.execute_query(
        "BEGIN; INSERT INTO audit (event) VALUES ('stale'); PREPARE TRANSACTION 'stale_test';",
        force_readonly=False,
    )
    try:
        from pgtuner_mcp.tools.tools_replication import CheckReplicationHealthHandler
        h = CheckReplicationHealthHandler(live_driver)
        result = await h.run_tool({"action": "prepared_xacts"})
        payload = json.loads(result[0].text)
        assert any(p["gid"] == "stale_test" for p in payload["prepared_xacts"])
    finally:
        await live_driver.execute_query(
            "ROLLBACK PREPARED 'stale_test'", force_readonly=False
        )
