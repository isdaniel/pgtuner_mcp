"""Tools package for MCP PostgreSQL Tuning Expert."""

from .toolhandler import ToolHandler
from .tools_bloat import (
    DatabaseBloatSummaryToolHandler,
    IndexBloatToolHandler,
    TableBloatToolHandler,
)
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
    DiskIOPatternToolHandler,
    GetSlowQueriesToolHandler,
    TableStatsToolHandler,
)
from .tools_vacuum import (
    VacuumProgressToolHandler,
)

__all__ = [
    "ToolHandler",
    # Performance tools
    "GetSlowQueriesToolHandler",
    "AnalyzeQueryToolHandler",
    "TableStatsToolHandler",
    "DiskIOPatternToolHandler",
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
    # Bloat detection tools
    "TableBloatToolHandler",
    "IndexBloatToolHandler",
    "DatabaseBloatSummaryToolHandler",
    # Vacuum monitoring tools
    "VacuumProgressToolHandler",
]
