import pytest

from .conftest import parse_tool_json

pytestmark = pytest.mark.integration


@pytest.mark.asyncio
async def test_check_database_health(live_driver):
    from pgtuner_mcp.tools.tools_health import DatabaseHealthToolHandler
    h = DatabaseHealthToolHandler(live_driver)
    result = await h.run_tool({"include_recommendations": True})
    parse_tool_json(result)


@pytest.mark.asyncio
async def test_get_active_queries(live_driver):
    from pgtuner_mcp.tools.tools_health import ActiveQueriesToolHandler
    h = ActiveQueriesToolHandler(live_driver)
    result = await h.run_tool({})
    parse_tool_json(result)


@pytest.mark.asyncio
async def test_analyze_wait_events(live_driver):
    from pgtuner_mcp.tools.tools_health import WaitEventsToolHandler
    h = WaitEventsToolHandler(live_driver)
    result = await h.run_tool({})
    parse_tool_json(result)


@pytest.mark.asyncio
async def test_review_settings(live_driver):
    from pgtuner_mcp.tools.tools_health import DatabaseSettingsToolHandler
    h = DatabaseSettingsToolHandler(live_driver)
    result = await h.run_tool({"category": "memory"})
    parse_tool_json(result)
