import pytest

from .conftest import parse_tool_json

pytestmark = pytest.mark.integration


@pytest.mark.asyncio
async def test_lint_query_static(live_driver):
    from pgtuner_mcp.tools.tools_linter import LintQueryHandler
    h = LintQueryHandler()
    result = await h.run_tool({"query": "SELECT * FROM users"})
    payload = parse_tool_json(result)
    assert any(f["rule"] == "select-star" for f in payload["findings"])


@pytest.mark.asyncio
async def test_lint_workload_finds_or_of_equals(live_driver):
    from pgtuner_mcp.tools.tools_linter import LintWorkloadHandler
    h = LintWorkloadHandler(live_driver)
    result = await h.run_tool({"limit": 50, "min_calls": 1,
                                "severity_threshold": "info"})
    payload = parse_tool_json(result)
    all_rule_ids = {f["rule"] for r in payload.get("ranked_findings", [])
                                for f in r["findings"]}
    assert "or-of-equals" in all_rule_ids
