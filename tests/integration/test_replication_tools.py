import pytest

from .conftest import parse_tool_json

pytestmark = pytest.mark.integration


@pytest.mark.asyncio
@pytest.mark.parametrize("action", ["slots", "lag", "wal_retention",
                                      "logical_subs", "prepared_xacts", "all"])
async def test_check_replication_health_all_actions(live_driver, action):
    from pgtuner_mcp.tools.tools_replication import CheckReplicationHealthHandler
    h = CheckReplicationHealthHandler(live_driver)
    result = await h.run_tool({"action": action, "min_lag_mb": 100})
    payload = parse_tool_json(result)
    assert "pg_version" in payload


@pytest.mark.asyncio
async def test_replication_health_detects_prepared_xact(live_driver, database_uri):
    """Prepares a 2PC xact via an autocommit connection (required by PG),
    asserts the replication health tool surfaces it, then cleans up.
    """
    import psycopg

    # Prepare a stale 2PC xact via autocommit connection. PREPARE TRANSACTION
    # and ROLLBACK PREPARED cannot run inside an implicit transaction block,
    # so a dedicated autocommit connection is required.
    async with await psycopg.AsyncConnection.connect(
        database_uri, autocommit=True
    ) as conn:
        async with conn.cursor() as cur:
            await cur.execute("BEGIN")
            await cur.execute("INSERT INTO audit (event) VALUES ('stale')")
            await cur.execute("PREPARE TRANSACTION 'stale_test'")

    try:
        from pgtuner_mcp.tools.tools_replication import CheckReplicationHealthHandler
        h = CheckReplicationHealthHandler(live_driver)
        result = await h.run_tool({"action": "prepared_xacts"})
        payload = parse_tool_json(result)
        assert any(p["gid"] == "stale_test" for p in payload["prepared_xacts"])
    finally:
        async with await psycopg.AsyncConnection.connect(
            database_uri, autocommit=True
        ) as conn:
            async with conn.cursor() as cur:
                try:
                    await cur.execute("ROLLBACK PREPARED 'stale_test'")
                except Exception:
                    pass  # already rolled back / never created
