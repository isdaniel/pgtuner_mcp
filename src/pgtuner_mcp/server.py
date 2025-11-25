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
from collections.abc import AsyncIterator, Sequence
from typing import Any

from mcp.server import Server
from mcp.types import (
    EmbeddedResource,
    ImageContent,
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
from .services import DbConnPool, HypoPGService, IndexAdvisor, SqlDriver
from .tools.toolhandler import ToolHandler
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
    GetSlowQueriesToolHandler,
    TableStatsToolHandler,
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

# Global database connection pool
db_pool: DbConnPool | None = None


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


def get_db_pool() -> DbConnPool:
    """
    Get the global database connection pool.

    Returns:
        The database connection pool

    Raises:
        RuntimeError: If the database pool is not initialized
    """
    global db_pool
    if db_pool is None:
        raise RuntimeError("Database connection pool not initialized")
    return db_pool


def register_all_tools() -> None:
    """
    Register all available tool handlers.

    This function serves as the central registry for all tools.
    New tool handlers should be added here for automatic registration.
    """
    pool = get_db_pool()
    sql_driver = SqlDriver(pool)
    hypopg_service = HypoPGService(sql_driver)
    index_advisor = IndexAdvisor(sql_driver)

    # Performance analysis tools
    add_tool_handler(GetSlowQueriesToolHandler(sql_driver))
    add_tool_handler(AnalyzeQueryToolHandler(sql_driver))
    add_tool_handler(TableStatsToolHandler(sql_driver))

    # Index tuning tools
    add_tool_handler(IndexAdvisorToolHandler(index_advisor))
    add_tool_handler(ExplainQueryToolHandler(sql_driver, hypopg_service))
    add_tool_handler(HypoPGToolHandler(hypopg_service))
    add_tool_handler(UnusedIndexesToolHandler(sql_driver))

    # Database health tools
    add_tool_handler(DatabaseHealthToolHandler(sql_driver))
    add_tool_handler(ActiveQueriesToolHandler(sql_driver))
    add_tool_handler(WaitEventsToolHandler(sql_driver))
    add_tool_handler(DatabaseSettingsToolHandler(sql_driver))

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


async def initialize_db_pool(database_uri: str) -> None:
    """
    Initialize the database connection pool.

    Args:
        database_uri: PostgreSQL connection URI
    """
    global db_pool
    db_pool = DbConnPool(database_uri)
    await db_pool.connect()
    logger.info("Database connection pool initialized successfully")


async def cleanup_db_pool() -> None:
    """
    Clean up the database connection pool.
    """
    global db_pool
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
