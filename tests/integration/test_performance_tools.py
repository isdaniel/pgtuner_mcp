import json

import pytest

from .conftest import parse_tool_json

pytestmark = pytest.mark.integration


@pytest.mark.asyncio
async def test_get_slow_queries(live_driver):
    from pgtuner_mcp.tools.tools_performance import GetSlowQueriesToolHandler
    h = GetSlowQueriesToolHandler(live_driver)
    result = await h.run_tool({"limit": 5, "min_calls": 1})
    parse_tool_json(result)  # parses


@pytest.mark.asyncio
async def test_analyze_query(live_driver):
    from pgtuner_mcp.tools.tools_performance import AnalyzeQueryToolHandler
    h = AnalyzeQueryToolHandler(live_driver)
    result = await h.run_tool({"query": "SELECT id FROM users LIMIT 1", "analyze": True})
    parse_tool_json(result)


@pytest.mark.asyncio
async def test_get_table_stats(live_driver):
    from pgtuner_mcp.tools.tools_performance import TableStatsToolHandler
    h = TableStatsToolHandler(live_driver)
    result = await h.run_tool({"schema_name": "public", "limit": 10})
    text = result[0].text
    json.loads(text)
    assert "users" in text or "orders" in text


@pytest.mark.asyncio
async def test_analyze_disk_io_patterns(live_driver):
    from pgtuner_mcp.tools.tools_performance import DiskIOPatternToolHandler
    h = DiskIOPatternToolHandler(live_driver)
    result = await h.run_tool({"analysis_type": "all"})
    parse_tool_json(result)
