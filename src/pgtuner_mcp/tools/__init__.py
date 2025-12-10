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
    GetSlowQueriesToolHandler,
    TableStatsToolHandler,
)
from .tools_query_history import (
    QueryPlanHistoryToolHandler,
    clear_plan_history,
    get_plan_history,
)
from .tools_query_rewrite import (
    QueryRewriteSuggestionsToolHandler,
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
    # Query plan history tools
    "QueryPlanHistoryToolHandler",
    "get_plan_history",
    "clear_plan_history",
    # Vacuum monitoring tools
    "VacuumProgressToolHandler",
    # Query rewrite tools
    "QueryRewriteSuggestionsToolHandler",
]
