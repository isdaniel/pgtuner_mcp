"""
pgtuner_mcp: PostgreSQL MCP Performance Tuning Server

A Model Context Protocol (MCP) server for AI-powered PostgreSQL performance tuning.
"""

from importlib.metadata import version, PackageNotFoundError

from .server import main
from .__main__ import run


def _get_version() -> str:
    """Get version from package metadata or fallback to pyproject.toml."""
    try:
        return version("pgtuner_mcp")
    except PackageNotFoundError:
        # Fallback: read from pyproject.toml if package is not installed
        try:
            from pathlib import Path
            import tomllib

            pyproject_path = Path(__file__).parent.parent.parent / "pyproject.toml"
            if pyproject_path.exists():
                with open(pyproject_path, "rb") as f:
                    data = tomllib.load(f)
                return data.get("project", {}).get("version", "0.0.0")
        except Exception:
            pass
        return "0.0.0"


__version__ = _get_version()
__all__ = ["main", "run"]
