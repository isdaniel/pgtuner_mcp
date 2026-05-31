import json

import pytest

pytestmark = pytest.mark.integration


@pytest.mark.asyncio
@pytest.mark.parametrize("action", ["progress", "needs_vacuum", "autovacuum_status",
                                      "recent_activity"])
async def test_monitor_vacuum_progress(live_driver, action):
    from pgtuner_mcp.tools.tools_vacuum import VacuumProgressToolHandler
    h = VacuumProgressToolHandler(live_driver)
    result = await h.run_tool({"action": action, "schema_name": "public"})
    json.loads(result[0].text)
