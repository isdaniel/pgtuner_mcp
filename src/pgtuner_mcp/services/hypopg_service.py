"""
HypoPG Service for managing hypothetical indexes in PostgreSQL.

HypoPG is a PostgreSQL extension that allows testing indexes without actually creating them.
This service provides a clean interface for working with HypoPG functionality.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any

from psycopg import sql

from .sql_driver import (
    SqlDriver,
    check_extension_available,
    check_extension_installed,
    get_postgres_version,
)

logger = logging.getLogger(__name__)


@dataclass
class HypotheticalIndex:
    """Represents a hypothetical index created by HypoPG."""
    indexrelid: int
    index_name: str
    schema_name: str | None = None
    table_name: str | None = None
    am_name: str | None = None  # Access method (btree, hash, etc.)
    definition: str | None = None
    estimated_size: int | None = None


@dataclass
class HypoPGStatus:
    """Status of HypoPG extension."""
    is_installed: bool
    is_available: bool
    version: str | None = None
    message: str = ""


class HypoPGService:
    """
    Service for managing hypothetical indexes with HypoPG.

    HypoPG allows creating virtual indexes that:
    - Don't consume disk space or CPU to create
    - Allow testing index effectiveness without actual creation
    - Work with EXPLAIN to show how PostgreSQL would use them
    - Don't affect EXPLAIN ANALYZE (actual execution)
    """

    def __init__(self, driver: SqlDriver):
        """
        Initialize the HypoPG service.

        Args:
            driver: SQL driver instance for executing queries
        """
        self.driver = driver
        self._status_cache: HypoPGStatus | None = None

    async def check_status(self, force_refresh: bool = False) -> HypoPGStatus:
        """
        Check the status of HypoPG extension.

        Args:
            force_refresh: Force refresh of cached status

        Returns:
            HypoPGStatus with installation status and message
        """
        if self._status_cache and not force_refresh:
            return self._status_cache

        status = HypoPGStatus(is_installed=False, is_available=False)

        try:
            # Check if installed
            is_installed = await check_extension_installed(self.driver, "hypopg")

            if is_installed:
                # Get version
                result = await self.driver.execute_query(
                    "SELECT extversion FROM pg_extension WHERE extname = 'hypopg'"
                )
                if result:
                    status.version = result[0].get("extversion")

                status.is_installed = True
                status.is_available = True
                status.message = f"HypoPG extension (version {status.version}) is installed and ready."
            else:
                # Check if available
                is_available = await check_extension_available(self.driver, "hypopg")
                status.is_available = is_available

                if is_available:
                    status.message = (
                        "HypoPG extension is available but not installed.\n"
                        "To install: CREATE EXTENSION hypopg;\n"
                        "This allows testing hypothetical indexes without actually creating them."
                    )
                else:
                    pg_version = await get_postgres_version(self.driver)
                    status.message = (
                        "HypoPG extension is not available on this server.\n\n"
                        f"To install HypoPG for PostgreSQL {pg_version}:\n"
                        f"- Debian/Ubuntu: sudo apt-get install postgresql-{pg_version}-hypopg\n"
                        f"- RHEL/CentOS: sudo yum install postgresql{pg_version}-hypopg\n"
                        "- MacOS: brew install hypopg\n\n"
                        "After installing the package, run: CREATE EXTENSION hypopg;"
                    )

        except Exception as e:
            logger.error(f"Error checking HypoPG status: {e}")
            status.message = f"Error checking HypoPG status: {e}"

        self._status_cache = status
        return status

    async def ensure_available(self) -> bool:
        """
        Ensure HypoPG is available for use.

        Returns:
            True if HypoPG is installed and ready

        Raises:
            RuntimeError: If HypoPG is not available
        """
        status = await self.check_status()
        if not status.is_installed:
            raise RuntimeError(status.message)
        return True

    async def create_index(
        self,
        table: str,
        columns: list[str],
        using: str = "btree",
        schema: str | None = None,
        where: str | None = None,
        include: list[str] | None = None,
    ) -> HypotheticalIndex:
        """
        Create a hypothetical index.

        Args:
            table: Table name
            columns: List of column names for the index
            using: Index access method (btree, hash, brin, bloom)
            schema: Schema name (optional)
            where: Partial index condition (optional)
            include: Columns to include (INCLUDE clause, optional)

        Returns:
            HypotheticalIndex with the created index info
        """
        await self.ensure_available()

        # Build the CREATE INDEX statement using safe SQL composition
        # Use sql.Identifier for proper escaping of table and column names
        if schema:
            table_ident = sql.Identifier(schema, table)
        else:
            table_ident = sql.Identifier(table)

        # Validate and whitelist the index access method
        valid_access_methods = {"btree", "hash", "brin", "bloom", "gist", "gin", "spgist"}
        if using.lower() not in valid_access_methods:
            raise ValueError(f"Invalid index access method: {using}")

        columns_ident = sql.SQL(", ").join(sql.Identifier(col) for col in columns)

        # Build the base CREATE INDEX statement
        create_stmt = sql.SQL("CREATE INDEX ON {} USING {} ({})").format(
            table_ident,
            sql.SQL(using),  # validated above
            columns_ident
        )

        if include:
            include_ident = sql.SQL(", ").join(sql.Identifier(col) for col in include)
            create_stmt = sql.Composed([create_stmt, sql.SQL(" INCLUDE ("), include_ident, sql.SQL(")")])

        if where:
            # WHERE clause is user-provided SQL expression - use as-is but document the risk
            # Note: The WHERE clause is intentionally passed through as the user's filter expression
            create_stmt = sql.Composed([create_stmt, sql.SQL(" WHERE "), sql.SQL(where)])

        # Convert to string for hypopg_create_index (which expects a SQL statement as text)
        create_stmt_str = create_stmt.as_string()

        # Create the hypothetical index using parameterized query
        result = await self.driver.execute_query(
            "SELECT * FROM hypopg_create_index(%s)",
            [create_stmt_str]
        )

        if not result:
            raise RuntimeError(f"Failed to create hypothetical index: {create_stmt}")

        row = result[0]
        index = HypotheticalIndex(
            indexrelid=row.get("indexrelid"),
            index_name=row.get("indexname"),
            table_name=table,
            schema_name=schema,
            am_name=using,
        )

        # Get the definition
        index.definition = await self.get_index_definition(index.indexrelid)
        index.estimated_size = await self.get_index_size(index.indexrelid)

        logger.info(f"Created hypothetical index: {index.index_name}")
        return index

    async def create_index_from_sql(self, create_index_sql: str) -> HypotheticalIndex:
        """
        Create a hypothetical index from a CREATE INDEX SQL statement.

        Args:
            create_index_sql: Complete CREATE INDEX statement

        Returns:
            HypotheticalIndex with the created index info

        Note:
            The create_index_sql parameter is expected to be a valid CREATE INDEX
            statement. This method uses parameterized queries to pass the statement
            to hypopg_create_index(), which only processes CREATE INDEX statements
            and ignores any other SQL.
        """
        await self.ensure_available()

        # Validate that it looks like a CREATE INDEX statement
        normalized = create_index_sql.strip().upper()
        if not normalized.startswith("CREATE") or "INDEX" not in normalized:
            raise ValueError("Invalid CREATE INDEX statement")

        # Use parameterized query - hypopg_create_index only processes CREATE INDEX
        result = await self.driver.execute_query(
            "SELECT * FROM hypopg_create_index(%s)",
            [create_index_sql]
        )

        if not result:
            raise RuntimeError(f"Failed to create hypothetical index: {create_index_sql}")

        row = result[0]
        index = HypotheticalIndex(
            indexrelid=row.get("indexrelid"),
            index_name=row.get("indexname"),
        )

        # Get additional info
        index.definition = await self.get_index_definition(index.indexrelid)
        index.estimated_size = await self.get_index_size(index.indexrelid)

        logger.info(f"Created hypothetical index: {index.index_name}")
        return index

    async def list_indexes(self) -> list[HypotheticalIndex]:
        """
        List all hypothetical indexes in the current session.

        Returns:
            List of HypotheticalIndex objects
        """
        await self.ensure_available()

        result = await self.driver.execute_query(
            "SELECT * FROM hypopg_list_indexes"
        )

        if not result:
            return []

        indexes = []
        for row in result:
            index = HypotheticalIndex(
                indexrelid=row.get("indexrelid"),
                index_name=row.get("index_name"),
                schema_name=row.get("schema_name"),
                table_name=row.get("table_name"),
                am_name=row.get("am_name"),
            )
            # Get definition and size
            index.definition = await self.get_index_definition(index.indexrelid)
            index.estimated_size = await self.get_index_size(index.indexrelid)
            indexes.append(index)

        return indexes

    async def get_index_definition(self, indexrelid: int) -> str | None:
        """
        Get the CREATE INDEX statement for a hypothetical index.

        Args:
            indexrelid: OID of the hypothetical index

        Returns:
            CREATE INDEX statement or None
        """
        try:
            result = await self.driver.execute_query(
                "SELECT hypopg_get_indexdef(%s) as indexdef",
                [indexrelid]
            )
            if result:
                return result[0].get("indexdef")
        except Exception as e:
            logger.warning(f"Could not get index definition: {e}")
        return None

    async def get_index_size(self, indexrelid: int) -> int | None:
        """
        Get the estimated size of a hypothetical index.

        Args:
            indexrelid: OID of the hypothetical index

        Returns:
            Estimated size in bytes or None
        """
        try:
            result = await self.driver.execute_query(
                "SELECT hypopg_relation_size(%s) as size",
                [indexrelid]
            )
            if result:
                return result[0].get("size")
        except Exception as e:
            logger.warning(f"Could not get index size: {e}")
        return None

    async def drop_index(self, indexrelid: int) -> bool:
        """
        Drop a specific hypothetical index.

        Args:
            indexrelid: OID of the hypothetical index to drop

        Returns:
            True if successful
        """
        await self.ensure_available()

        try:
            await self.driver.execute_query(
                "SELECT hypopg_drop_index(%s)",
                [indexrelid]
            )
            logger.info(f"Dropped hypothetical index: {indexrelid}")
            return True
        except Exception as e:
            logger.error(f"Failed to drop hypothetical index: {e}")
            return False

    async def reset(self) -> bool:
        """
        Remove all hypothetical indexes.

        Returns:
            True if successful
        """
        await self.ensure_available()

        try:
            await self.driver.execute_query("SELECT hypopg_reset()")
            logger.info("Reset all hypothetical indexes")
            return True
        except Exception as e:
            logger.error(f"Failed to reset hypothetical indexes: {e}")
            return False

    async def hide_index(self, indexrelid: int) -> bool:
        """
        Hide an existing index from the planner.

        Args:
            indexrelid: OID of the index to hide (can be real or hypothetical)

        Returns:
            True if successful
        """
        await self.ensure_available()

        try:
            result = await self.driver.execute_query(
                "SELECT hypopg_hide_index(%s)",
                [indexrelid]
            )
            if result:
                return result[0].get("hypopg_hide_index", False)
        except Exception as e:
            logger.error(f"Failed to hide index: {e}")
        return False

    async def unhide_index(self, indexrelid: int) -> bool:
        """
        Unhide a previously hidden index.

        Args:
            indexrelid: OID of the index to unhide

        Returns:
            True if successful
        """
        await self.ensure_available()

        try:
            result = await self.driver.execute_query(
                "SELECT hypopg_unhide_index(%s)",
                [indexrelid]
            )
            if result:
                return result[0].get("hypopg_unhide_index", False)
        except Exception as e:
            logger.error(f"Failed to unhide index: {e}")
        return False

    async def unhide_all_indexes(self) -> bool:
        """
        Unhide all hidden indexes.

        Returns:
            True if successful
        """
        await self.ensure_available()

        try:
            await self.driver.execute_query("SELECT hypopg_unhide_all_indexes()")
            return True
        except Exception as e:
            logger.error(f"Failed to unhide all indexes: {e}")
            return False

    async def list_hidden_indexes(self) -> list[dict[str, Any]]:
        """
        List all hidden indexes.

        Returns:
            List of hidden index information
        """
        await self.ensure_available()

        try:
            result = await self.driver.execute_query(
                "SELECT * FROM hypopg_hidden_indexes"
            )
            if result:
                return result
        except Exception as e:
            logger.warning(f"Could not list hidden indexes: {e}")
        return []

    async def explain_with_hypothetical_index(
        self,
        query: str,
        table: str,
        columns: list[str],
        using: str = "btree",
    ) -> dict[str, Any]:
        """
        Create a hypothetical index and explain a query with it.

        Args:
            query: SQL query to explain
            table: Table name for the index
            columns: List of column names for the index
            using: Index access method

        Returns:
            Dictionary with before/after explain plans and improvement info
        """
        await self.ensure_available()

        # Get explain plan before
        before_result = await self.driver.execute_query(
            f"EXPLAIN (FORMAT JSON, COSTS TRUE) {query}"
        )
        before_plan = before_result[0] if before_result else {}

        # Create hypothetical index
        hypo_index = await self.create_index(table, columns, using)

        try:
            # Get explain plan after
            after_result = await self.driver.execute_query(
                f"EXPLAIN (FORMAT JSON, COSTS TRUE) {query}"
            )
            after_plan = after_result[0] if after_result else {}

            # Extract costs
            before_cost = self._extract_total_cost(before_plan)
            after_cost = self._extract_total_cost(after_plan)

            improvement = None
            if before_cost and after_cost and after_cost > 0:
                improvement = ((before_cost - after_cost) / before_cost) * 100

            return {
                "hypothetical_index": {
                    "indexrelid": hypo_index.indexrelid,
                    "name": hypo_index.index_name,
                    "definition": hypo_index.definition,
                    "estimated_size": hypo_index.estimated_size,
                },
                "before": {
                    "plan": before_plan,
                    "total_cost": before_cost,
                },
                "after": {
                    "plan": after_plan,
                    "total_cost": after_cost,
                },
                "improvement_percentage": improvement,
                "would_use_index": self._plan_uses_index(after_plan, hypo_index.index_name),
            }

        finally:
            # Clean up the hypothetical index
            await self.drop_index(hypo_index.indexrelid)

    def _extract_total_cost(self, plan: dict[str, Any]) -> float | None:
        """Extract total cost from an EXPLAIN plan."""
        try:
            if isinstance(plan, dict):
                # Handle JSON format
                query_plan = plan.get("QUERY PLAN", plan)
                if isinstance(query_plan, list) and query_plan:
                    first_plan = query_plan[0]
                    if isinstance(first_plan, dict):
                        return first_plan.get("Plan", {}).get("Total Cost")
        except Exception:
            pass
        return None

    def _plan_uses_index(self, plan: dict[str, Any], index_name: str) -> bool:
        """Check if a plan uses a specific index."""
        try:
            plan_str = str(plan)
            return index_name in plan_str
        except Exception:
            return False
