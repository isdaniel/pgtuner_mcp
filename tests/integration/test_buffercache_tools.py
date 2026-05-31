import pytest

from .conftest import parse_tool_json

pytestmark = pytest.mark.integration


@pytest.mark.asyncio
async def test_analyze_buffer_cache(live_driver):
    from pgtuner_mcp.tools.tools_buffercache import AnalyzeBufferCacheHandler
    h = AnalyzeBufferCacheHandler(live_driver)
    result = await h.run_tool({"top_n": 10})
    payload = parse_tool_json(result)
    assert "top_relations" in payload or "error" in payload


@pytest.mark.asyncio
async def test_analyze_toast_storage_finds_docs(live_driver):
    from pgtuner_mcp.tools.tools_buffercache import AnalyzeToastStorageHandler
    h = AnalyzeToastStorageHandler(live_driver)
    result = await h.run_tool({"schema_name": "public"})
    payload = parse_tool_json(result)
    table_names = {t["table"] for t in payload.get("tables", [])}
    assert "docs" in table_names
