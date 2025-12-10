"""
pgtuner-mcp: PostgreSQL MCP Performance Tuning Server

This server implements a modular, extensible design pattern for PostgreSQL
performance tuning with HypoPG support for hypothetical index testing.
Supports stdio, SSE, and streamable-http MCP server modes.
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import logging
import os
import sys
import traceback
import re
import json
from collections.abc import AsyncIterator, Sequence
from typing import Any

from mcp.server import Server
from mcp.types import (
    CompleteResult,
    Completion,
    EmbeddedResource,
    GetPromptResult,
    ImageContent,
    Prompt,
    PromptArgument,
    PromptMessage,
    Resource,
    ResourceTemplate,
    TextContent,
    Tool,
)

# HTTP-related imports (imported conditionally)
try:
    import uvicorn
    from mcp.server.sse import SseServerTransport
    from mcp.server.streamable_http_manager import StreamableHTTPSessionManager
    from starlette.applications import Starlette
    from starlette.middleware.cors import CORSMiddleware
    from starlette.requests import Request
    from starlette.routing import Mount, Route
    HTTP_AVAILABLE = True
except ImportError:
    HTTP_AVAILABLE = False

# Import tool handlers
from .services import DbConnPool, HypoPGService, IndexAdvisor, SqlDriver, get_user_filter
from .tools.toolhandler import ToolHandler
from .tools.tools_bloat import (
    DatabaseBloatSummaryToolHandler,
    IndexBloatToolHandler,
    TableBloatToolHandler,
)
from .tools.tools_health import (
    ActiveQueriesToolHandler,
    DatabaseHealthToolHandler,
    DatabaseSettingsToolHandler,
    WaitEventsToolHandler,
)
from .tools.tools_index import (
    ExplainQueryToolHandler,
    HypoPGToolHandler,
    IndexAdvisorToolHandler,
    UnusedIndexesToolHandler,
)
from .tools.tools_performance import (
    AnalyzeQueryToolHandler,
    DiskIOPatternToolHandler,
    GetSlowQueriesToolHandler,
    TableStatsToolHandler,
)
from .tools.tools_vacuum import (
    VacuumProgressToolHandler,
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("pgtuner_mcp")

# Create the MCP server instance
app = Server("pgtuner_mcp")

# Global tool handlers registry
tool_handlers: dict[str, ToolHandler] = {}

# Global database connection pool and SQL driver
db_pool: DbConnPool | None = None
sql_driver: SqlDriver | None = None


def add_tool_handler(tool_handler: ToolHandler) -> None:
    """
    Register a tool handler with the server.

    Args:
        tool_handler: The tool handler instance to register
    """
    global tool_handlers
    tool_handlers[tool_handler.name] = tool_handler
    logger.info(f"Registered tool handler: {tool_handler.name}")


def get_tool_handler(name: str) -> ToolHandler | None:
    """
    Retrieve a tool handler by name.

    Args:
        name: The name of the tool handler

    Returns:
        The tool handler instance or None if not found
    """
    return tool_handlers.get(name)

def get_sql_driver() -> SqlDriver:
    """
    Get the global SQL driver instance.

    Returns:
        The SQL driver instance

    Raises:
        RuntimeError: If the SQL driver is not initialized
    """
    global sql_driver
    if sql_driver is None:
        raise RuntimeError("SQL driver not initialized")
    return sql_driver


def register_all_tools() -> None:
    """
    Register all available tool handlers.

    This function serves as the central registry for all tools.
    New tool handlers should be added here for automatic registration.
    """
    driver = get_sql_driver()
    hypopg_service = HypoPGService(driver)
    index_advisor = IndexAdvisor(driver)

    # Performance analysis tools
    add_tool_handler(GetSlowQueriesToolHandler(driver))
    add_tool_handler(AnalyzeQueryToolHandler(driver))
    add_tool_handler(TableStatsToolHandler(driver))
    add_tool_handler(DiskIOPatternToolHandler(driver))

    # Index tuning tools
    add_tool_handler(IndexAdvisorToolHandler(index_advisor))
    add_tool_handler(ExplainQueryToolHandler(driver, hypopg_service))
    add_tool_handler(HypoPGToolHandler(hypopg_service))
    add_tool_handler(UnusedIndexesToolHandler(driver))

    # Database health tools
    add_tool_handler(DatabaseHealthToolHandler(driver))
    add_tool_handler(ActiveQueriesToolHandler(driver))
    add_tool_handler(WaitEventsToolHandler(driver))
    add_tool_handler(DatabaseSettingsToolHandler(driver))

    # Bloat detection tools (using pgstattuple extension)
    add_tool_handler(TableBloatToolHandler(driver))
    add_tool_handler(IndexBloatToolHandler(driver))
    add_tool_handler(DatabaseBloatSummaryToolHandler(driver))

    # Vacuum progress monitoring tools
    add_tool_handler(VacuumProgressToolHandler(driver))

    logger.info(f"Registered {len(tool_handlers)} tool handlers")


def create_starlette_app(mcp_server: Server, *, debug: bool = False) -> Starlette:
    """
    Create a Starlette application that can serve the provided mcp server with SSE.

    Args:
        mcp_server: The MCP server instance
        debug: Whether to enable debug mode

    Returns:
        Starlette application instance
    """
    if not HTTP_AVAILABLE:
        raise RuntimeError("HTTP dependencies not available. Install with: pip install starlette uvicorn")

    sse = SseServerTransport("/messages/")

    async def handle_sse(request: Request) -> None:
        async with sse.connect_sse(
                request.scope,
                request.receive,
                request._send,
        ) as (read_stream, write_stream):
            await mcp_server.run(
                read_stream,
                write_stream,
                mcp_server.create_initialization_options(),
            )

    return Starlette(
        debug=debug,
        routes=[
            Route("/sse", endpoint=handle_sse),
            Mount("/messages/", app=sse.handle_post_message),
        ],
    )


def create_streamable_http_app(mcp_server: Server, *, debug: bool = False, stateless: bool = False) -> Starlette:
    """
    Create a Starlette application with StreamableHTTPSessionManager.
    Implements the MCP Streamable HTTP protocol with a single /mcp endpoint.

    Args:
        mcp_server: The MCP server instance
        debug: Whether to enable debug mode
        stateless: If True, creates a fresh transport for each request with no session tracking

    Returns:
        Starlette application instance
    """
    if not HTTP_AVAILABLE:
        raise RuntimeError("HTTP dependencies not available. Install with: pip install starlette uvicorn")

    # Create the session manager
    session_manager = StreamableHTTPSessionManager(
        app=mcp_server,
        event_store=None,  # No event store for now (no resumability)
        json_response=False,
        stateless=stateless,
    )

    class StreamableHTTPRoute:
        """ASGI app wrapper for the streamable HTTP handler"""
        async def __call__(self, scope, receive, send):
            await session_manager.handle_request(scope, receive, send)

    @contextlib.asynccontextmanager
    async def lifespan(app: Starlette) -> AsyncIterator[None]:
        """Context manager for session manager lifecycle."""
        async with session_manager.run():
            logger.info("Streamable HTTP session manager started!")
            try:
                yield
            finally:
                logger.info("Streamable HTTP session manager shutting down...")

    # Create Starlette app with a single endpoint
    starlette_app = Starlette(
        debug=debug,
        routes=[
            Route("/mcp", endpoint=StreamableHTTPRoute()),
        ],
        lifespan=lifespan,
    )

    # Add CORS middleware
    starlette_app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["GET", "POST", "OPTIONS"],
        allow_headers=["*"],
        expose_headers=["mcp-session-id", "mcp-protocol-version"],
        max_age=86400,
    )

    return starlette_app


@app.list_tools()
async def list_tools() -> list[Tool]:
    """
    List all available tools.

    Returns:
        List of Tool objects describing all registered tools
    """
    try:
        tools = [handler.get_tool_definition() for handler in tool_handlers.values()]
        logger.info(f"Listed {len(tools)} available tools")
        return tools
    except Exception as e:
        logger.exception(f"Error listing tools: {str(e)}")
        raise


@app.call_tool()
async def call_tool(name: str, arguments: Any) -> Sequence[TextContent | ImageContent | EmbeddedResource]:
    """
    Execute a tool with the provided arguments.

    Args:
        name: The name of the tool to execute
        arguments: The arguments to pass to the tool

    Returns:
        Sequence of MCP content objects

    Raises:
        RuntimeError: If the tool execution fails
    """
    try:
        # Validate arguments
        if not isinstance(arguments, dict):
            raise RuntimeError("Arguments must be a dictionary")

        # Get the tool handler
        tool_handler = get_tool_handler(name)
        if not tool_handler:
            raise ValueError(f"Unknown tool: {name}")

        logger.info(f"Executing tool: {name} with arguments: {list(arguments.keys())}")

        # Execute the tool
        result = await tool_handler.run_tool(arguments)

        logger.info(f"Tool {name} executed successfully")
        return result

    except Exception as e:
        logger.exception(f"Error executing tool {name}: {str(e)}")
        error_traceback = traceback.format_exc()
        logger.error(f"Full traceback: {error_traceback}")

        # Return error as text content
        return [
            TextContent(
                type="text",
                text=f"Error executing tool '{name}': {str(e)}"
            )
        ]


@app.completion()
async def handle_completion(ref: Any, argument: Any) -> CompleteResult:
    """
    Handle completion requests for prompts and resources.

    This server does not provide completion suggestions for any arguments,
    but implements this handler to satisfy the MCP protocol requirements
    for servers that need to work with proxies like mcp-proxy.

    Args:
        ref: Reference to the prompt or resource being completed
        argument: The argument being completed

    Returns:
        Empty completion result
    """
    logger.debug(f"Completion requested for ref: {ref}, argument: {argument}")
    return CompleteResult(
        completion=Completion(
            values=[],
            total=0,
            hasMore=False
        )
    )


# =============================================================================
# PROMPTS - Pre-defined prompt templates for common PostgreSQL tuning workflows
# =============================================================================

# Define available prompts for PostgreSQL performance tuning
PROMPTS: dict[str, Prompt] = {
    "diagnose_slow_queries": Prompt(
        name="diagnose_slow_queries",
        title="Diagnose Slow Queries",
        description="Analyze slow queries and provide optimization recommendations. "
                    "This prompt guides the AI to systematically investigate query performance issues.",
        arguments=[
            PromptArgument(
                name="min_duration_ms",
                description="Minimum query duration in milliseconds to consider (default: 1000)",
                required=False
            ),
            PromptArgument(
                name="limit",
                description="Maximum number of slow queries to analyze (default: 10)",
                required=False
            )
        ]
    ),
    "index_optimization": Prompt(
        name="index_optimization",
        title="Index Optimization Analysis",
        description="Comprehensive index analysis including unused indexes, missing indexes, "
                    "and hypothetical index testing recommendations.",
        arguments=[
            PromptArgument(
                name="table_name",
                description="Specific table to analyze (optional, analyzes all if not provided)",
                required=False
            ),
            PromptArgument(
                name="schema_name",
                description="Schema to analyze (default: public)",
                required=False
            )
        ]
    ),
    "health_check": Prompt(
        name="health_check",
        title="Database Health Check",
        description="Perform a comprehensive PostgreSQL health assessment covering "
                    "connections, cache ratios, locks, replication, and more.",
        arguments=[
            PromptArgument(
                name="verbose",
                description="Include detailed statistics (true/false, default: false)",
                required=False
            )
        ]
    ),
    "query_tuning": Prompt(
        name="query_tuning",
        title="Query Tuning Assistant",
        description="Analyze a specific SQL query and provide detailed tuning recommendations "
                    "including execution plan analysis and index suggestions.",
        arguments=[
            PromptArgument(
                name="query",
                description="The SQL query to analyze and tune",
                required=True
            ),
            PromptArgument(
                name="test_indexes",
                description="Test hypothetical indexes (true/false, default: true)",
                required=False
            )
        ]
    ),
    "performance_baseline": Prompt(
        name="performance_baseline",
        title="Performance Baseline Report",
        description="Generate a comprehensive performance baseline report including "
                    "table statistics, query patterns, and configuration review.",
        arguments=[]
    ),
}


@app.list_prompts()
async def list_prompts() -> list[Prompt]:
    """
    List all available prompts.

    Returns:
        List of Prompt objects describing available prompt templates
    """
    logger.info(f"Listed {len(PROMPTS)} available prompts")
    return list(PROMPTS.values())


@app.get_prompt()
async def get_prompt(name: str, arguments: dict[str, str] | None = None) -> GetPromptResult:
    """
    Get a specific prompt with its messages.

    Args:
        name: The name of the prompt to retrieve
        arguments: Optional arguments to customize the prompt

    Returns:
        GetPromptResult with the prompt messages

    Raises:
        ValueError: If the prompt is not found
    """
    if name not in PROMPTS:
        raise ValueError(f"Unknown prompt: {name}")

    prompt = PROMPTS[name]
    args = arguments or {}

    # Generate prompt messages based on the prompt type
    messages = _generate_prompt_messages(name, args)

    logger.info(f"Generated prompt: {name} with {len(messages)} messages")
    return GetPromptResult(
        description=prompt.description,
        messages=messages
    )


def _generate_prompt_messages(prompt_name: str, args: dict[str, str]) -> list[PromptMessage]:
    """
    Generate prompt messages based on the prompt type and arguments.

    Args:
        prompt_name: The name of the prompt
        args: Arguments passed to the prompt

    Returns:
        List of PromptMessage objects
    """
    if prompt_name == "diagnose_slow_queries":
        min_duration = args.get("min_duration_ms", "1000")
        limit = args.get("limit", "10")
        return [
            PromptMessage(
                role="user",
                content=TextContent(
                    type="text",
                    text=f"""Please help me diagnose slow queries in my PostgreSQL database.

**Task**: Analyze slow queries and provide optimization recommendations.

**Steps to follow**:
1. First, use the `get_slow_queries` tool with min_mean_time_ms={min_duration} and limit={limit}
2. For the top slowest queries, use `analyze_query` to examine their execution plans
3. Use `get_index_recommendations` to find potential index improvements
4. Check table statistics with `get_table_stats` for tables involved in slow queries

**Expected Output**:
- List of slow queries with their execution statistics
- Execution plan analysis for critical queries
- Specific index recommendations with CREATE INDEX statements
- Any table maintenance recommendations (VACUUM, ANALYZE)

Please start by fetching the slow queries."""
                )
            )
        ]

    elif prompt_name == "index_optimization":
        schema = args.get("schema_name", "public")
        table = args.get("table_name", "")
        table_filter = f" for table '{table}'" if table else ""
        return [
            PromptMessage(
                role="user",
                content=TextContent(
                    type="text",
                    text=f"""Please perform a comprehensive index optimization analysis{table_filter} in schema '{schema}'.

**Task**: Analyze indexes and provide optimization recommendations.

**Steps to follow**:
1. Use `find_unused_indexes` with schema_name='{schema}' to identify unused and duplicate indexes
2. Use `get_index_recommendations` to find missing indexes based on query workload
3. For any recommended indexes, use `explain_with_indexes` with hypothetical_indexes to verify improvement
4. Use `get_table_stats` to check table sizes and access patterns

**Expected Output**:
- List of unused indexes that can be safely dropped (with DROP INDEX statements)
- List of duplicate/overlapping indexes
- Recommended new indexes with estimated improvement percentages
- Verification of recommended indexes using hypothetical testing
- Total potential storage savings from removing unused indexes

Please start the analysis."""
                )
            )
        ]

    elif prompt_name == "health_check":
        verbose = args.get("verbose", "false").lower() == "true"
        return [
            PromptMessage(
                role="user",
                content=TextContent(
                    type="text",
                    text=f"""Please perform a comprehensive health check on my PostgreSQL database.

**Task**: Assess overall database health and identify potential issues.

**Steps to follow**:
1. Use `check_database_health` with verbose={str(verbose).lower()} to get overall health metrics
2. Use `get_active_queries` to check for any problematic running queries
3. Use `analyze_wait_events` to identify performance bottlenecks
4. Use `review_settings` to check configuration for optimization opportunities

**Expected Output**:
- Overall health score with breakdown by category
- Current issues and warnings
- Active query analysis (long-running, blocked queries)
- Wait event analysis
- Configuration recommendations
- Priority-ordered action items

Please start the health check."""
                )
            )
        ]

    elif prompt_name == "query_tuning":
        query = args.get("query", "")
        test_indexes = args.get("test_indexes", "true").lower() == "true"
        if not query:
            return [
                PromptMessage(
                    role="user",
                    content=TextContent(
                        type="text",
                        text="Please provide a SQL query to analyze. Use the query_tuning prompt with the 'query' argument."
                    )
                )
            ]
        return [
            PromptMessage(
                role="user",
                content=TextContent(
                    type="text",
                    text=f"""Please help me tune this SQL query for better performance:

```sql
{query}
```

**Task**: Analyze and optimize this query.

**Steps to follow**:
1. Use `analyze_query` with the query above to examine its current execution plan
2. Identify any sequential scans, row estimate mismatches, or expensive operations
3. Use `get_index_recommendations` with workload_queries containing this query
4. {"Use `explain_with_indexes` to test recommended indexes with hypothetical_indexes" if test_indexes else "Review the recommendations without hypothetical testing"}
5. Check related table statistics with `get_table_stats`

**Expected Output**:
- Current execution plan with timing breakdown
- Identified performance issues
- Specific index recommendations
- {"Verified improvement from hypothetical indexes" if test_indexes else "Expected improvements"}
- Rewritten query suggestions if applicable

Please start the analysis."""
                )
            )
        ]

    elif prompt_name == "performance_baseline":
        return [
            PromptMessage(
                role="user",
                content=TextContent(
                    type="text",
                    text="""Please generate a comprehensive performance baseline report for my PostgreSQL database.

**Task**: Create a baseline performance report for future comparison.

**Steps to follow**:
1. Use `check_database_health` with verbose=true for detailed health metrics
2. Use `get_slow_queries` with limit=20 to capture query workload patterns
3. Use `get_table_stats` to document table sizes and access patterns
4. Use `review_settings` with category='all' to document current configuration
5. Use `find_unused_indexes` to document index utilization
6. Use `analyze_wait_events` to capture current bottleneck patterns

**Expected Output**:
A structured baseline report including:
- Database health score and metrics
- Top queries by execution time
- Table statistics summary (sizes, row counts, vacuum status)
- Current PostgreSQL configuration settings
- Index utilization summary
- Wait event distribution

This baseline can be used for comparison after making changes.

Please generate the baseline report."""
                )
            )
        ]

    # Default fallback
    return [
        PromptMessage(
            role="user",
            content=TextContent(
                type="text",
                text=f"Prompt '{prompt_name}' requested. Please use the available tools to assist."
            )
        )
    ]


# =============================================================================
# RESOURCES - Expose database information and documentation as resources
# =============================================================================

# Define available resources
RESOURCES: dict[str, Resource] = {
    "pgtuner://docs/tools": Resource(
        uri="pgtuner://docs/tools",
        name="Tool Documentation",
        title="pgtuner-mcp Tool Reference",
        description="Complete documentation of all available PostgreSQL tuning tools",
        mimeType="text/markdown"
    ),
    "pgtuner://docs/workflows": Resource(
        uri="pgtuner://docs/workflows",
        name="Workflow Guide",
        title="PostgreSQL Tuning Workflows",
        description="Common PostgreSQL performance tuning workflows and best practices",
        mimeType="text/markdown"
    ),
    "pgtuner://docs/prompts": Resource(
        uri="pgtuner://docs/prompts",
        name="Prompt Templates",
        title="Available Prompt Templates",
        description="Documentation of available prompt templates for guided tuning sessions",
        mimeType="text/markdown"
    ),
}

# Define available resource templates for dynamic database information
RESOURCE_TEMPLATES: list[ResourceTemplate] = [
    ResourceTemplate(
        uriTemplate="pgtuner://table/{schema}/{table_name}/stats",
        name="Table Statistics",
        title="Table Statistics Resource",
        description="Get detailed statistics for a specific user/client table including size, row counts, and access patterns. "
                    "Note: This resource only provides data for user tables, not system tables. "
                    "Parameters: schema (e.g., 'public'), table_name (e.g., 'users')",
        mimeType="application/json"
    ),
    ResourceTemplate(
        uriTemplate="pgtuner://table/{schema}/{table_name}/indexes",
        name="Table Indexes",
        title="Table Index Information",
        description="Get all indexes defined on a specific user/client table with usage statistics. "
                    "Note: This resource only provides data for user table indexes, not system table indexes. "
                    "Parameters: schema (e.g., 'public'), table_name (e.g., 'orders')",
        mimeType="application/json"
    ),
    ResourceTemplate(
        uriTemplate="pgtuner://query/{query_hash}/stats",
        name="Query Statistics",
        title="Query Performance Statistics",
        description="Get performance statistics for a specific query by its hash from pg_stat_statements. "
                    "Parameters: query_hash (the queryid from pg_stat_statements)",
        mimeType="application/json"
    ),
    ResourceTemplate(
        uriTemplate="pgtuner://settings/{category}",
        name="PostgreSQL Settings",
        title="PostgreSQL Configuration Settings",
        description="Get PostgreSQL configuration settings by category. "
                    "Parameters: category (one of: memory, checkpoint, wal, autovacuum, connections, all)",
        mimeType="application/json"
    ),
    ResourceTemplate(
        uriTemplate="pgtuner://health/{check_type}",
        name="Health Check",
        title="Database Health Check",
        description="Get specific health check information focused on user/client tables and operations. "
                    "System tables are excluded from analysis. "
                    "Parameters: check_type (one of: connections, cache, locks, replication, bloat, all)",
        mimeType="application/json"
    ),
]


@app.list_resources()
async def list_resources() -> list[Resource]:
    """
    List all available resources.

    Returns:
        List of Resource objects describing available resources
    """
    logger.info(f"Listed {len(RESOURCES)} available resources")
    return list(RESOURCES.values())


@app.list_resource_templates()
async def list_resource_templates() -> list[ResourceTemplate]:
    """
    List all available resource templates.

    Resource templates allow dynamic access to database information
    using parameterized URIs.

    Returns:
        List of ResourceTemplate objects describing available templates
    """
    logger.info(f"Listed {len(RESOURCE_TEMPLATES)} resource templates")
    return RESOURCE_TEMPLATES


@app.read_resource()
async def read_resource(uri: str) -> str:
    """
    Read a specific resource by URI.

    Supports both static resources and dynamic resource templates.

    Args:
        uri: The URI of the resource to read

    Returns:
        Resource contents as string

    Raises:
        ValueError: If the resource is not found
    """

    uri_str = str(uri)

    # Static resources
    if uri_str == "pgtuner://docs/tools":
        return _get_tools_documentation()
    elif uri_str == "pgtuner://docs/workflows":
        return _get_workflows_documentation()
    elif uri_str == "pgtuner://docs/prompts":
        return _get_prompts_documentation()

    # Dynamic resource templates
    # Table statistics: pgtuner://table/{schema}/{table_name}/stats
    match = re.match(r"pgtuner://table/([^/]+)/([^/]+)/stats", uri_str)
    if match:
        schema, table_name = match.groups()
        return await _get_table_stats_resource(schema, table_name)

    # Table indexes: pgtuner://table/{schema}/{table_name}/indexes
    match = re.match(r"pgtuner://table/([^/]+)/([^/]+)/indexes", uri_str)
    if match:
        schema, table_name = match.groups()
        return await _get_table_indexes_resource(schema, table_name)

    # Query statistics: pgtuner://query/{query_hash}/stats
    match = re.match(r"pgtuner://query/([^/]+)/stats", uri_str)
    if match:
        query_hash = match.group(1)
        return await _get_query_stats_resource(query_hash)

    # PostgreSQL settings: pgtuner://settings/{category}
    match = re.match(r"pgtuner://settings/([^/]+)", uri_str)
    if match:
        category = match.group(1)
        return await _get_settings_resource(category)

    # Health check: pgtuner://health/{check_type}
    match = re.match(r"pgtuner://health/([^/]+)", uri_str)
    if match:
        check_type = match.group(1)
        return await _get_health_resource(check_type)

    raise ValueError(f"Unknown resource: {uri}")


async def _get_table_stats_resource(schema: str, table_name: str) -> str:
    """Get table statistics as JSON."""

    driver = get_sql_driver()

    query = """
        SELECT
            schemaname,
            relname as table_name,
            n_live_tup as live_rows,
            n_dead_tup as dead_rows,
            n_mod_since_analyze as modifications_since_analyze,
            last_vacuum,
            last_autovacuum,
            last_analyze,
            last_autoanalyze,
            vacuum_count,
            autovacuum_count,
            analyze_count,
            autoanalyze_count,
            seq_scan,
            seq_tup_read,
            idx_scan,
            idx_tup_fetch,
            n_tup_ins as inserts,
            n_tup_upd as updates,
            n_tup_del as deletes,
            n_tup_hot_upd as hot_updates
        FROM pg_stat_user_tables
        WHERE schemaname = %s AND relname = %s
    """

    result = await driver.execute_query(query, (schema, table_name))

    if not result:
        return json.dumps({"error": f"Table {schema}.{table_name} not found"}, indent=2)

    # Get table size
    size_query = """
        SELECT
            pg_size_pretty(pg_total_relation_size(quote_ident(%s) || '.' || quote_ident(%s))) as total_size,
            pg_size_pretty(pg_table_size(quote_ident(%s) || '.' || quote_ident(%s))) as table_size,
            pg_size_pretty(pg_indexes_size(quote_ident(%s) || '.' || quote_ident(%s))) as indexes_size
    """
    size_result = await driver.execute_query(
        size_query, (schema, table_name, schema, table_name, schema, table_name)
    )

    row = result[0]
    stats = {
        "schema": row["schemaname"],
        "table_name": row["table_name"],
        "row_counts": {
            "live_rows": row["live_rows"],
            "dead_rows": row["dead_rows"],
            "dead_row_ratio": round(row["dead_rows"] / max(row["live_rows"], 1) * 100, 2)
        },
        "size": size_result[0] if size_result else {},
        "maintenance": {
            "last_vacuum": str(row["last_vacuum"]) if row["last_vacuum"] else None,
            "last_autovacuum": str(row["last_autovacuum"]) if row["last_autovacuum"] else None,
            "last_analyze": str(row["last_analyze"]) if row["last_analyze"] else None,
            "last_autoanalyze": str(row["last_autoanalyze"]) if row["last_autoanalyze"] else None,
            "modifications_since_analyze": row["modifications_since_analyze"]
        },
        "access_patterns": {
            "sequential_scans": row["seq_scan"],
            "sequential_rows_read": row["seq_tup_read"],
            "index_scans": row["idx_scan"],
            "index_rows_fetched": row["idx_tup_fetch"]
        },
        "modifications": {
            "inserts": row["inserts"],
            "updates": row["updates"],
            "deletes": row["deletes"],
            "hot_updates": row["hot_updates"]
        }
    }

    return json.dumps(stats, indent=2, default=str)


async def _get_table_indexes_resource(schema: str, table_name: str) -> str:
    """Get table indexes as JSON."""

    driver = get_sql_driver()

    query = """
        SELECT
            i.indexrelname as index_name,
            i.idx_scan as scans,
            i.idx_tup_read as tuples_read,
            i.idx_tup_fetch as tuples_fetched,
            pg_size_pretty(pg_relation_size(i.indexrelid)) as size,
            pg_relation_size(i.indexrelid) as size_bytes,
            idx.indisunique as is_unique,
            idx.indisprimary as is_primary,
            pg_get_indexdef(i.indexrelid) as definition
        FROM pg_stat_user_indexes i
        JOIN pg_index idx ON i.indexrelid = idx.indexrelid
        WHERE i.schemaname = %s AND i.relname = %s
        ORDER BY pg_relation_size(i.indexrelid) DESC
    """

    result = await driver.execute_query(query, (schema, table_name))

    indexes = []
    for row in result:
        indexes.append({
            "name": row["index_name"],
            "is_unique": row["is_unique"],
            "is_primary": row["is_primary"],
            "size": row["size"],
            "size_bytes": row["size_bytes"],
            "usage": {
                "scans": row["scans"],
                "tuples_read": row["tuples_read"],
                "tuples_fetched": row["tuples_fetched"]
            },
            "definition": row["definition"]
        })

    return json.dumps({
        "schema": schema,
        "table_name": table_name,
        "index_count": len(indexes),
        "indexes": indexes
    }, indent=2, default=str)


async def _get_query_stats_resource(query_hash: str) -> str:
    """Get query statistics by hash from pg_stat_statements."""

    driver = get_sql_driver()

    # Get user filter for excluding specific user IDs
    user_filter = get_user_filter()
    statements_filter = user_filter.get_statements_filter()

    query = f"""
        SELECT
            queryid,
            query,
            calls,
            total_exec_time,
            mean_exec_time,
            min_exec_time,
            max_exec_time,
            stddev_exec_time,
            rows,
            shared_blks_hit,
            shared_blks_read,
            shared_blks_dirtied,
            shared_blks_written,
            local_blks_hit,
            local_blks_read,
            temp_blks_read,
            temp_blks_written
        FROM pg_stat_statements
        WHERE queryid::text = %s
          {statements_filter}
    """

    try:
        result = await driver.execute_query(query, (query_hash,))
    except Exception as e:
        return json.dumps({
            "error": "pg_stat_statements extension may not be installed or enabled",
            "details": str(e)
        }, indent=2)

    if not result:
        return json.dumps({"error": f"Query with hash {query_hash} not found"}, indent=2)

    row = result[0]
    cache_hit_ratio = 0
    total_blocks = row["shared_blks_hit"] + row["shared_blks_read"]
    if total_blocks > 0:
        cache_hit_ratio = round(row["shared_blks_hit"] / total_blocks * 100, 2)

    stats = {
        "query_id": str(row["queryid"]),
        "query": row["query"][:500] + "..." if len(row["query"]) > 500 else row["query"],
        "execution": {
            "calls": row["calls"],
            "total_time_ms": round(row["total_exec_time"], 2),
            "mean_time_ms": round(row["mean_exec_time"], 2),
            "min_time_ms": round(row["min_exec_time"], 2),
            "max_time_ms": round(row["max_exec_time"], 2),
            "stddev_time_ms": round(row["stddev_exec_time"], 2),
            "rows_returned": row["rows"],
            "avg_rows_per_call": round(row["rows"] / max(row["calls"], 1), 2)
        },
        "buffer_usage": {
            "shared_blocks_hit": row["shared_blks_hit"],
            "shared_blocks_read": row["shared_blks_read"],
            "cache_hit_ratio": cache_hit_ratio,
            "shared_blocks_dirtied": row["shared_blks_dirtied"],
            "shared_blocks_written": row["shared_blks_written"],
            "temp_blocks_read": row["temp_blks_read"],
            "temp_blocks_written": row["temp_blks_written"]
        }
    }

    return json.dumps(stats, indent=2, default=str)


async def _get_settings_resource(category: str) -> str:
    """Get PostgreSQL settings by category."""

    driver = get_sql_driver()

    category_filters = {
        "memory": ["shared_buffers", "work_mem", "maintenance_work_mem", "effective_cache_size",
                   "wal_buffers", "temp_buffers", "huge_pages"],
        "checkpoint": ["checkpoint_timeout", "checkpoint_completion_target", "checkpoint_warning",
                       "max_wal_size", "min_wal_size"],
        "wal": ["wal_level", "wal_compression", "wal_log_hints", "synchronous_commit",
                "wal_writer_delay", "wal_writer_flush_after"],
        "autovacuum": ["autovacuum", "autovacuum_max_workers", "autovacuum_naptime",
                       "autovacuum_vacuum_threshold", "autovacuum_analyze_threshold",
                       "autovacuum_vacuum_scale_factor", "autovacuum_analyze_scale_factor"],
        "connections": ["max_connections", "superuser_reserved_connections",
                        "idle_in_transaction_session_timeout", "statement_timeout"]
    }

    if category == "all":
        settings_filter = [item for sublist in category_filters.values() for item in sublist]
    elif category in category_filters:
        settings_filter = category_filters[category]
    else:
        return json.dumps({
            "error": f"Unknown category: {category}",
            "valid_categories": list(category_filters.keys()) + ["all"]
        }, indent=2)

    placeholders = ", ".join(["%s"] * len(settings_filter))
    query = f"""
        SELECT name, setting, unit, context, short_desc, boot_val, reset_val
        FROM pg_settings
        WHERE name IN ({placeholders})
        ORDER BY name
    """

    result = await driver.execute_query(query, tuple(settings_filter))

    settings = []
    for row in result:
        settings.append({
            "name": row["name"],
            "current_value": row["setting"],
            "unit": row["unit"],
            "context": row["context"],
            "description": row["short_desc"],
            "boot_value": row["boot_val"],
            "reset_value": row["reset_val"]
        })

    return json.dumps({
        "category": category,
        "setting_count": len(settings),
        "settings": settings
    }, indent=2, default=str)


async def _get_health_resource(check_type: str) -> str:
    """Get database health information by check type."""

    driver = get_sql_driver()

    # Get user filter for excluding specific user IDs
    user_filter = get_user_filter()
    activity_filter = user_filter.get_activity_filter()

    if check_type == "connections":
        query = f"""
            SELECT
                count(*) as total_connections,
                count(*) FILTER (WHERE state = 'active') as active,
                count(*) FILTER (WHERE state = 'idle') as idle,
                count(*) FILTER (WHERE state = 'idle in transaction') as idle_in_transaction,
                count(*) FILTER (WHERE wait_event_type IS NOT NULL) as waiting,
                (SELECT setting::int FROM pg_settings WHERE name = 'max_connections') as max_connections
            FROM pg_stat_activity
            WHERE backend_type = 'client backend'
              {activity_filter}
        """
        result = await driver.execute_query(query)
        row = result[0]
        usage_pct = round(row["total_connections"] / row["max_connections"] * 100, 2)

        return json.dumps({
            "check_type": "connections",
            "max_connections": row["max_connections"],
            "current_connections": row["total_connections"],
            "usage_percentage": usage_pct,
            "breakdown": {
                "active": row["active"],
                "idle": row["idle"],
                "idle_in_transaction": row["idle_in_transaction"],
                "waiting": row["waiting"]
            },
            "status": "warning" if usage_pct > 80 else "ok"
        }, indent=2, default=str)

    elif check_type == "cache":
        query = """
            SELECT
                sum(heap_blks_read) as heap_read,
                sum(heap_blks_hit) as heap_hit,
                sum(idx_blks_read) as idx_read,
                sum(idx_blks_hit) as idx_hit
            FROM pg_statio_user_tables
        """
        result = await driver.execute_query(query)
        row = result[0]

        heap_total = (row["heap_hit"] or 0) + (row["heap_read"] or 0)
        idx_total = (row["idx_hit"] or 0) + (row["idx_read"] or 0)

        heap_ratio = round((row["heap_hit"] or 0) / max(heap_total, 1) * 100, 2)
        idx_ratio = round((row["idx_hit"] or 0) / max(idx_total, 1) * 100, 2)

        return json.dumps({
            "check_type": "cache",
            "table_cache_hit_ratio": heap_ratio,
            "index_cache_hit_ratio": idx_ratio,
            "status": "warning" if heap_ratio < 95 or idx_ratio < 95 else "ok",
            "recommendation": "Consider increasing shared_buffers" if heap_ratio < 95 else None
        }, indent=2, default=str)

    elif check_type == "locks":
        query = """
            SELECT
                count(*) as total_locks,
                count(*) FILTER (WHERE granted = false) as waiting_locks,
                count(*) FILTER (WHERE mode LIKE '%Exclusive%') as exclusive_locks
            FROM pg_locks
        """
        result = await driver.execute_query(query)
        row = result[0]

        return json.dumps({
            "check_type": "locks",
            "total_locks": row["total_locks"],
            "waiting_locks": row["waiting_locks"],
            "exclusive_locks": row["exclusive_locks"],
            "status": "warning" if row["waiting_locks"] > 5 else "ok"
        }, indent=2, default=str)

    elif check_type == "replication":
        query = """
            SELECT
                client_addr,
                state,
                sent_lsn,
                write_lsn,
                flush_lsn,
                replay_lsn,
                pg_wal_lsn_diff(sent_lsn, replay_lsn) as replication_lag_bytes
            FROM pg_stat_replication
        """
        result = await driver.execute_query(query)

        replicas = []
        for row in result:
            replicas.append({
                "client_addr": str(row["client_addr"]),
                "state": row["state"],
                "lag_bytes": row["replication_lag_bytes"]
            })

        return json.dumps({
            "check_type": "replication",
            "replica_count": len(replicas),
            "replicas": replicas,
            "status": "ok" if all(r.get("state") == "streaming" for r in replicas) else "warning"
        }, indent=2, default=str)

    elif check_type == "bloat":
        query = """
            SELECT
                schemaname,
                relname as table_name,
                n_dead_tup as dead_tuples,
                n_live_tup as live_tuples,
                CASE WHEN n_live_tup > 0
                    THEN round(100.0 * n_dead_tup / n_live_tup, 2)
                    ELSE 0
                END as dead_tuple_ratio
            FROM pg_stat_user_tables
            WHERE n_dead_tup > 1000
            ORDER BY n_dead_tup DESC
            LIMIT 10
        """
        result = await driver.execute_query(query)

        bloated_tables = []
        for row in result:
            bloated_tables.append({
                "schema": row["schemaname"],
                "table": row["table_name"],
                "dead_tuples": row["dead_tuples"],
                "live_tuples": row["live_tuples"],
                "dead_ratio_pct": float(row["dead_tuple_ratio"])
            })

        return json.dumps({
            "check_type": "bloat",
            "bloated_table_count": len(bloated_tables),
            "tables": bloated_tables,
            "status": "warning" if bloated_tables else "ok"
        }, indent=2, default=str)

    elif check_type == "all":
        # Aggregate all checks
        checks = {}
        for ct in ["connections", "cache", "locks", "bloat"]:
            result = await _get_health_resource(ct)
            checks[ct] = json.loads(result)

        overall_status = "ok"
        for check in checks.values():
            if check.get("status") == "warning":
                overall_status = "warning"
                break

        return json.dumps({
            "check_type": "all",
            "overall_status": overall_status,
            "checks": checks
        }, indent=2, default=str)

    else:
        return json.dumps({
            "error": f"Unknown check type: {check_type}",
            "valid_types": ["connections", "cache", "locks", "replication", "bloat", "all"]
        }, indent=2)


def _get_tools_documentation() -> str:
    """Generate documentation for all available tools."""
    docs = """# pgtuner-mcp Tool Reference

## Overview
pgtuner-mcp provides a comprehensive set of tools for PostgreSQL performance tuning and monitoring.

**Important Note**: All tools in this MCP server focus exclusively on user/client tables and indexes.
System catalog tables (pg_catalog, information_schema, pg_toast) are automatically excluded from
all analyses. This ensures the tools focus on optimizing your application's custom database objects.

## Performance Analysis Tools

### get_slow_queries
Retrieve slow queries from PostgreSQL using pg_stat_statements.
- **Parameters**: limit, min_calls, min_mean_time_ms, order_by
- **Use case**: Identify queries that need optimization
- **Note**: System catalog queries are excluded

### analyze_query
Analyze a SQL query's execution plan and performance characteristics.
- **Parameters**: query (required), analyze, buffers, verbose, format, settings
- **Use case**: Deep dive into query execution plans
- **Warning**: With analyze=true, the query is actually executed

### get_table_stats
Get detailed statistics for user/client database tables including size, row counts, and access patterns.
- **Parameters**: schema_name, table_name, include_indexes, order_by
- **Use case**: Identify tables needing maintenance or optimization
- **Note**: Only analyzes user tables, excludes system tables

## Index Tuning Tools

### get_index_recommendations
Get AI-powered index recommendations based on query workload analysis.
- **Parameters**: workload_queries, max_recommendations, min_improvement_percent, include_hypothetical_testing, target_tables
- **Use case**: Find missing indexes that would improve performance
- **Note**: Only analyzes user/client tables

### explain_with_indexes
Run EXPLAIN on a query with optional hypothetical indexes (requires HypoPG).
- **Parameters**: query (required), hypothetical_indexes, analyze
- **Use case**: Test index changes without creating real indexes

### manage_hypothetical_indexes
Manage HypoPG hypothetical indexes for testing.
- **Parameters**: action (required), table, columns, index_type, unique, index_id
- **Actions**: create, list, drop, reset, estimate_size, check
- **Use case**: Create and manage temporary test indexes

### find_unused_indexes
Find user/client indexes that are not being used or are duplicates.
- **Parameters**: schema_name, min_size_mb, max_scan_ratio, include_duplicates
- **Use case**: Identify indexes that can be safely dropped
- **Note**: Only analyzes user indexes, excludes system indexes

## Database Health Tools

### check_database_health
Perform a comprehensive database health check.
- **Parameters**: include_recommendations, verbose
- **Checks**: Connections, cache ratios, locks, replication, wraparound, disk usage, checkpoints
- **Use case**: Overall database health assessment
- **Note**: Focuses on user tables for bloat and cache analysis

### get_active_queries
Get information about currently active queries and connections.
- **Parameters**: min_duration_seconds, include_idle, include_system, database
- **Use case**: Monitor running queries and detect issues
- **Note**: By default excludes system processes and catalog queries

### analyze_wait_events
Analyze PostgreSQL wait events to identify bottlenecks.
- **Parameters**: active_only
- **Use case**: Identify I/O, lock, or CPU bottlenecks
- **Note**: Focuses on client backend processes

### review_settings
Review PostgreSQL configuration settings and get recommendations.
- **Parameters**: category, include_all_settings
- **Categories**: all, memory, checkpoint, wal, autovacuum, connections
- **Use case**: Configuration optimization

## Bloat Detection Tools (pgstattuple)

These tools use the pgstattuple extension to detect table and index bloat.
Requires: CREATE EXTENSION IF NOT EXISTS pgstattuple;

### analyze_table_bloat
Analyze table bloat using pgstattuple to get accurate tuple-level statistics.
- **Parameters**: table_name, schema_name, use_approx, min_table_size_mb, include_toast
- **Output**: Dead tuple counts, free space, wasted space percentage
- **Use case**: Identify tables needing VACUUM or VACUUM FULL
- **Note**: use_approx=true uses pgstattuple_approx for faster analysis on large tables

### analyze_index_bloat
Analyze B-tree index bloat using pgstatindex.
- **Parameters**: index_name, table_name, schema_name, min_index_size_mb, min_bloat_percent
- **Output**: Leaf density, fragmentation, empty/deleted pages
- **Use case**: Identify indexes needing REINDEX
- **Supports**: B-tree (pgstatindex), GIN (pgstatginindex), Hash (pgstathashindex)

### get_bloat_summary
Get a comprehensive overview of database bloat across tables and indexes.
- **Parameters**: schema_name, top_n, min_size_mb
- **Output**: Top bloated tables, top bloated indexes, total reclaimable space, priority actions
- **Use case**: Quick assessment of database maintenance needs
"""
    return docs


def _get_workflows_documentation() -> str:
    """Generate documentation for common workflows."""
    return """# PostgreSQL Tuning Workflows

## Workflow 1: Slow Query Investigation

1. **Identify slow queries**
   ```
   Use: get_slow_queries with limit=10, order_by="mean_time"
   ```

2. **Analyze execution plans**
   ```
   Use: analyze_query for each slow query
   Look for: Sequential scans, row estimate mismatches, sorts spilling to disk
   ```

3. **Get index recommendations**
   ```
   Use: get_index_recommendations with the slow queries
   ```

4. **Test with hypothetical indexes**
   ```
   Use: explain_with_indexes with hypothetical_indexes
   Verify: Estimated improvement > 20%
   ```

5. **Create beneficial indexes**
   - Review CREATE INDEX statements
   - Consider maintenance overhead
   - Test in staging first

## Workflow 2: Index Cleanup

1. **Find unused indexes**
   ```
   Use: find_unused_indexes with include_duplicates=true
   ```

2. **Verify indexes are truly unused**
   - Check time since stats reset
   - Consider seasonal query patterns
   - Review application code

3. **Identify duplicate indexes**
   - Look for indexes with same leading columns
   - Keep the most selective index

4. **Drop unnecessary indexes**
   - Start with obvious duplicates
   - Monitor query performance after dropping

## Workflow 3: Health Check Routine

1. **Daily checks**
   ```
   Use: check_database_health
   Use: get_active_queries with min_duration_seconds=60
   ```

2. **Weekly analysis**
   ```
   Use: get_table_stats to identify bloated tables
   Use: review_settings with category="autovacuum"
   ```

3. **Monthly review**
   ```
   Use: find_unused_indexes
   Use: get_index_recommendations
   Use: review_settings with category="all"
   ```

## Workflow 4: Query Optimization

1. **Baseline the query**
   ```
   Use: analyze_query with analyze=false first
   Record: Current cost and plan
   ```

2. **Identify issues**
   - Sequential scans on large tables
   - Nested loops with high iterations
   - Sort/hash operations spilling to disk

3. **Generate recommendations**
   ```
   Use: get_index_recommendations with workload_queries=[your_query]
   ```

4. **Test improvements**
   ```
   Use: explain_with_indexes with hypothetical_indexes
   Compare: New cost vs baseline
   ```

5. **Implement and verify**
   - Create recommended indexes
   - Re-run analyze_query with analyze=true
   - Monitor pg_stat_statements

## Workflow 5: Bloat Detection and Cleanup

This workflow uses the pgstattuple extension for accurate bloat detection.
Prerequisite: CREATE EXTENSION IF NOT EXISTS pgstattuple;

1. **Get bloat overview**
   ```
   Use: get_bloat_summary with schema_name="public", top_n=10
   Review: Total reclaimable space and priority actions
   ```

2. **Analyze specific tables**
   ```
   Use: analyze_table_bloat with table_name="your_table"
   Check: dead_tuple_percent and wasted_percent
   For large tables: use_approx=true for faster analysis
   ```

3. **Analyze index bloat**
   ```
   Use: analyze_index_bloat with table_name="your_table"
   Check: avg_leaf_density (< 70% indicates bloat)
   Check: leaf_fragmentation percentage
   ```

4. **Maintenance actions**
   - For tables with high dead tuples: VACUUM ANALYZE table_name;
   - For tables with >30% wasted space: VACUUM FULL table_name; (requires exclusive lock)
   - Alternative for online defrag: pg_repack -t schema.table_name
   - For bloated indexes: REINDEX INDEX CONCURRENTLY index_name;

5. **Prevent future bloat**
   ```
   Use: review_settings with category="autovacuum"
   Consider: Tuning per-table autovacuum settings
   ALTER TABLE table_name SET (autovacuum_vacuum_scale_factor = 0.1);
   ```
"""


def _get_prompts_documentation() -> str:
    """Generate documentation for available prompts."""
    docs = """# Available Prompt Templates

pgtuner-mcp provides pre-defined prompt templates for common PostgreSQL tuning workflows.
These prompts guide the AI through systematic analysis processes.

## diagnose_slow_queries
**Purpose**: Analyze slow queries and provide optimization recommendations.

**Arguments**:
- `min_duration_ms`: Minimum query duration to consider (default: 1000)
- `limit`: Maximum queries to analyze (default: 10)

**What it does**:
1. Fetches slow queries from pg_stat_statements
2. Analyzes execution plans for top queries
3. Generates index recommendations
4. Checks table statistics

## index_optimization
**Purpose**: Comprehensive index analysis and optimization.

**Arguments**:
- `table_name`: Specific table to analyze (optional)
- `schema_name`: Schema to analyze (default: public)

**What it does**:
1. Identifies unused and duplicate indexes
2. Recommends new indexes based on workload
3. Tests recommendations with hypothetical indexes
4. Calculates potential storage savings

## health_check
**Purpose**: Comprehensive database health assessment.

**Arguments**:
- `verbose`: Include detailed statistics (default: false)

**What it does**:
1. Runs comprehensive health checks
2. Analyzes active queries
3. Identifies wait event bottlenecks
4. Reviews configuration settings

## query_tuning
**Purpose**: Analyze and optimize a specific SQL query.

**Arguments**:
- `query` (required): The SQL query to analyze
- `test_indexes`: Test hypothetical indexes (default: true)

**What it does**:
1. Analyzes current execution plan
2. Identifies performance issues
3. Generates index recommendations
4. Tests improvements with hypothetical indexes

## performance_baseline
**Purpose**: Generate a performance baseline report.

**Arguments**: None

**What it does**:
1. Captures comprehensive health metrics
2. Documents query workload patterns
3. Records table statistics
4. Documents current configuration
5. Captures index utilization
6. Records wait event distribution

Use this baseline for comparison after making changes.
"""
    return docs


async def initialize_db_pool(database_uri: str) -> None:
    """
    Initialize the database connection pool and SQL driver.

    Args:
        database_uri: PostgreSQL connection URI
    """
    global db_pool, sql_driver
    db_pool = DbConnPool(database_uri)
    await db_pool.connect()
    sql_driver = SqlDriver(db_pool)
    logger.info("Database connection pool and SQL driver initialized successfully")


async def cleanup_db_pool() -> None:
    """
    Clean up the database connection pool and SQL driver.
    """
    global db_pool, sql_driver
    sql_driver = None
    if db_pool is not None:
        await db_pool.close()
        db_pool = None
        logger.info("Database connection pool closed")


async def main():
    """
    Main entry point for the pgtuner_mcp server.
    Supports both stdio and SSE modes based on command line arguments.
    """
    # Parse command line arguments
    parser = argparse.ArgumentParser(
        description='pgtuner_mcp: PostgreSQL MCP Performance Tuning Server - supports stdio, SSE, and streamable-http modes'
    )
    parser.add_argument(
        '--mode',
        choices=['stdio', 'sse', 'streamable-http'],
        default='stdio',
        help='Server mode: stdio (default), sse, or streamable-http'
    )
    parser.add_argument(
        '--host',
        default='0.0.0.0',
        help='Host to bind to (HTTP modes only, default: 0.0.0.0)'
    )
    parser.add_argument(
        '--port',
        type=int,
        default=None,
        help='Port to listen on (HTTP modes only, default: from PORT env var or 8080)'
    )
    parser.add_argument(
        '--stateless',
        action='store_true',
        help='Run in stateless mode (streamable-http only, creates fresh transport per request)'
    )
    parser.add_argument(
        '--debug',
        action='store_true',
        help='Enable debug mode'
    )
    parser.add_argument(
        '--database-url',
        default=None,
        help='PostgreSQL connection URL (or use DATABASE_URI env var)'
    )

    args = parser.parse_args()

    # Get port from environment variable or command line argument, or default to 8080
    port = args.port if args.port is not None else int(os.environ.get("PORT", 8080))

    # Set debug logging if requested
    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)
        logger.setLevel(logging.DEBUG)

    try:
        # Get database URL from environment variable or command line
        database_url = args.database_url or os.environ.get("DATABASE_URI")

        if not database_url:
            logger.error("No database URL provided. Set DATABASE_URI environment variable or use --database-url")
            print("Error: No database URL provided. Set DATABASE_URI environment variable or use --database-url",
                  file=sys.stderr)
            sys.exit(1)


        # Initialize database connection pool
        await initialize_db_pool(database_url)

        # Register all tools
        register_all_tools()

        logger.info(f"Starting pgtuner_mcp server in {args.mode} mode...")
        logger.info(f"Python version: {sys.version}")
        logger.info(f"Registered tools: {list(tool_handlers.keys())}")

        # Run the server in the specified mode
        await run_server(args.mode, args.host, port, args.debug, args.stateless)

    except Exception as e:
        logger.exception(f"Failed to start server: {str(e)}")
        raise
    finally:
        await cleanup_db_pool()


async def run_server(mode: str, host: str = "0.0.0.0", port: int = 8080, debug: bool = False, stateless: bool = False):
    """
    Unified server runner that supports stdio, SSE, and streamable-http modes.

    Args:
        mode: Server mode ("stdio", "sse", or "streamable-http")
        host: Host to bind to (HTTP modes only)
        port: Port to listen on (HTTP modes only)
        debug: Whether to enable debug mode
        stateless: Whether to use stateless mode (streamable-http only)
    """
    if mode == "stdio":
        logger.info("Starting stdio server...")

        from mcp.server.stdio import stdio_server

        async with stdio_server() as (read_stream, write_stream):
            await app.run(
                read_stream,
                write_stream,
                app.create_initialization_options()
            )

    elif mode == "sse":
        if not HTTP_AVAILABLE:
            raise RuntimeError(
                "SSE mode requires additional dependencies. "
                "Install with: pip install starlette uvicorn"
            )

        logger.info(f"Starting SSE server on {host}:{port}...")
        logger.info(f"Endpoints: http://{host}:{port}/sse, http://{host}:{port}/messages/")

        # Create Starlette app with SSE transport
        starlette_app = create_starlette_app(app, debug=debug)

        # Configure uvicorn
        config = uvicorn.Config(
            app=starlette_app,
            host=host,
            port=port,
            log_level="debug" if debug else "info"
        )

        # Run the server
        server = uvicorn.Server(config)
        await server.serve()

    elif mode == "streamable-http":
        if not HTTP_AVAILABLE:
            raise RuntimeError(
                "Streamable HTTP mode requires additional dependencies. "
                "Install with: pip install starlette uvicorn"
            )

        mode_desc = "stateless" if stateless else "stateful"
        logger.info(f"Starting Streamable HTTP server ({mode_desc}) on {host}:{port}...")
        logger.info(f"Endpoint: http://{host}:{port}/mcp")

        # Create Starlette app with Streamable HTTP transport
        starlette_app = create_streamable_http_app(app, debug=debug, stateless=stateless)

        # Configure uvicorn
        config = uvicorn.Config(
            app=starlette_app,
            host=host,
            port=port,
            log_level="debug" if debug else "info"
        )

        # Run the server (session manager lifecycle is handled by lifespan)
        server = uvicorn.Server(config)
        await server.serve()

    else:
        raise ValueError(f"Unknown mode: {mode}")


if __name__ == "__main__":
    asyncio.run(main())
