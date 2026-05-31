import pytest

from .conftest import parse_tool_json

pytestmark = pytest.mark.integration


@pytest.mark.asyncio
async def test_analyze_table_bloat(live_driver):
    from pgtuner_mcp.tools.tools_bloat import TableBloatToolHandler
    h = TableBloatToolHandler(live_driver)
    result = await h.run_tool({"schema_name": "public", "min_table_size_gb": 0})
    parse_tool_json(result)


@pytest.mark.asyncio
async def test_analyze_index_bloat(live_driver):
    from pgtuner_mcp.tools.tools_bloat import IndexBloatToolHandler
    h = IndexBloatToolHandler(live_driver)
    result = await h.run_tool({"schema_name": "public", "min_index_size_gb": 0,
                                "min_bloat_percent": 0})
    parse_tool_json(result)


@pytest.mark.asyncio
async def test_get_bloat_summary(live_driver):
    from pgtuner_mcp.tools.tools_bloat import DatabaseBloatSummaryToolHandler
    h = DatabaseBloatSummaryToolHandler(live_driver)
    result = await h.run_tool({"schema_name": "public", "min_size_gb": 0})
    parse_tool_json(result)
