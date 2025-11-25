"""Tools package for MCP PostgreSQL Tuning Expert."""

from .toolhandler import ToolHandler
from .tools_health import (
    ActiveQueriesToolHandler,
    DatabaseHealthToolHandler,
    DatabaseSettingsToolHandler,
    WaitEventsToolHandler,
)
from .tools_index import (
    ExplainQueryToolHandler,
    HypoPGToolHandler,
    IndexAdvisorToolHandler,
    UnusedIndexesToolHandler,
)
from .tools_performance import (
    AnalyzeQueryToolHandler,
    GetSlowQueriesToolHandler,
    TableStatsToolHandler,
)

__all__ = [
    "ToolHandler",
    # Performance tools
    "GetSlowQueriesToolHandler",
    "AnalyzeQueryToolHandler",
    "TableStatsToolHandler",
    # Index tools
    "IndexAdvisorToolHandler",
    "ExplainQueryToolHandler",
    "HypoPGToolHandler",
    "UnusedIndexesToolHandler",
    # Health tools
    "DatabaseHealthToolHandler",
    "ActiveQueriesToolHandler",
    "WaitEventsToolHandler",
    "DatabaseSettingsToolHandler",
]
