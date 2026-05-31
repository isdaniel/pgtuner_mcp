"""Integration test fixtures requiring a live PostgreSQL."""

from __future__ import annotations

import os
import pathlib

import pytest
import pytest_asyncio

from pgtuner_mcp.services.sql_driver import DbConnPool, SqlDriver

_VERSION_PORT = {"14": "5414", "15": "5415", "16": "5416", "17": "5417"}
_SEED_DIR = pathlib.Path(__file__).parent / "seed"


def _database_uri() -> str:
    version = os.environ.get("PGTUNER_TEST_PG_VERSION", "16")
    port_override = os.environ.get("PGTUNER_TEST_PG_PORT_OVERRIDE")
    port = port_override or _VERSION_PORT.get(version, "5416")
    host = os.environ.get("PGTUNER_TEST_PG_HOST", "localhost")
    return f"postgresql://postgres:test@{host}:{port}/pgtuner_test"


@pytest.fixture(scope="session")
def pg_version() -> int:
    return int(os.environ.get("PGTUNER_TEST_PG_VERSION", "16"))


@pytest.fixture(scope="session")
def database_uri() -> str:
    return _database_uri()


@pytest_asyncio.fixture(scope="session")
async def live_driver(database_uri):
    pool = DbConnPool(database_uri)
    await pool.connect()
    driver = SqlDriver(pool)
    try:
        yield driver
    finally:
        await pool.close()


@pytest_asyncio.fixture(scope="session", autouse=True)
async def seeded_db(live_driver):
    for fname in sorted(_SEED_DIR.glob("*.sql")):
        sql = fname.read_text()
        for stmt in [s.strip() for s in sql.split(";") if s.strip()]:
            try:
                await live_driver.execute_query(stmt, force_readonly=False)
            except Exception:
                pass
    # Cleanup any stray prepared xacts so re-runs are clean
    try:
        rows = await live_driver.execute_query(
            "SELECT gid FROM pg_prepared_xacts", force_readonly=False
        ) or []
        for r in rows:
            await live_driver.execute_query(
                f"ROLLBACK PREPARED '{r['gid']}'", force_readonly=False
            )
    except Exception:
        pass
    yield
