"""
pgtuner_mcp: PostgreSQL MCP Performance Tuning Server

A Model Context Protocol (MCP) server for AI-powered PostgreSQL performance tuning.
"""

from .server import main
from .__main__ import run

__version__ = "0.1.0"
__all__ = ["main", "run"]
