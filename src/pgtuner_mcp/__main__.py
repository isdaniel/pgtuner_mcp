"""
Main entry point for the pgtuner_mcp server.
"""

import asyncio
import sys

from .server import main


def run():
    """Run the server with the appropriate event loop for the platform."""
    # On Windows, psycopg async requires SelectorEventLoop instead of ProactorEventLoop
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    asyncio.run(main())


if __name__ == "__main__":
    run()
