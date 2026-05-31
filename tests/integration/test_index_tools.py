import json

import pytest

from .conftest import parse_tool_json

pytestmark = pytest.mark.integration


@pytest.mark.asyncio
async def test_get_index_recommendations(live_driver):
    from pgtuner_mcp.services.index_advisor import IndexAdvisor
    from pgtuner_mcp.tools.tools_index import IndexAdvisorToolHandler
    advisor = IndexAdvisor(live_driver)
    h = IndexAdvisorToolHandler(advisor)
    result = await h.run_tool({"max_recommendations": 3, "min_improvement_percent": 5})
    parse_tool_json(result)


@pytest.mark.asyncio
async def test_explain_with_indexes(live_driver):
    from pgtuner_mcp.services.hypopg_service import HypoPGService
    from pgtuner_mcp.tools.tools_index import ExplainQueryToolHandler
    h = ExplainQueryToolHandler(live_driver, HypoPGService(live_driver))
    result = await h.run_tool({
        "query": "SELECT * FROM orders WHERE user_id = 1",
        "hypothetical_indexes": [{"table": "orders", "columns": ["user_id"]}],
    })
    parse_tool_json(result)


@pytest.mark.asyncio
async def test_manage_hypothetical_indexes(live_driver):
    from pgtuner_mcp.services.hypopg_service import HypoPGService
    from pgtuner_mcp.tools.tools_index import HypoPGToolHandler
    h = HypoPGToolHandler(HypoPGService(live_driver))
    result = await h.run_tool({"action": "reset"})
    parse_tool_json(result)


@pytest.mark.asyncio
async def test_find_unused_indexes(live_driver):
    from pgtuner_mcp.tools.tools_index import UnusedIndexesToolHandler
    h = UnusedIndexesToolHandler(live_driver)
    result = await h.run_tool({"schema_name": "public"})
    text = result[0].text
    json.loads(text)
