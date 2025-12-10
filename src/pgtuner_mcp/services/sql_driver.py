"""
SQL Driver for PostgreSQL connections using psycopg connection pool.
"""

from __future__ import annotations

import logging
from typing import Any
from urllib.parse import urlparse, urlunparse

from psycopg import OperationalError
from psycopg.rows import dict_row
from psycopg_pool import AsyncConnectionPool, PoolTimeout

logger = logging.getLogger(__name__)


def obfuscate_password(connection_string: str | None) -> str | None:
    """
    Obfuscate the password in a connection string for logging.

    Args:
        connection_string: PostgreSQL connection string

    Returns:
        Connection string with password replaced by asterisks
    """
    if not connection_string:
        return None

    try:
        parsed = urlparse(connection_string)
        if parsed.password:
            # Replace password with asterisks
            netloc = f"{parsed.username}:****@{parsed.hostname}"
            if parsed.port:
                netloc += f":{parsed.port}"
            return urlunparse(parsed._replace(netloc=netloc))
        return connection_string
    except Exception:
        return "****"


class DbConnPool:
    """Database connection manager using psycopg's connection pool."""

    def __init__(self, connection_url: str | None = None):
        """
        Initialize the connection pool.

        Args:
            connection_url: PostgreSQL connection URL
        """
        self.connection_url = connection_url
        self.pool: AsyncConnectionPool | None = None
        self._is_valid = False
        self._last_error: str | None = None

    async def connect(self, connection_url: str | None = None) -> AsyncConnectionPool:
        """
        Initialize connection pool with retry logic.

        Args:
            connection_url: PostgreSQL connection URL (optional if set in constructor)

        Returns:
            The initialized connection pool

        Raises:
            ValueError: If connection fails
        """
        # If we already have a valid pool, return it
        if self.pool and self._is_valid:
            return self.pool

        url = connection_url or self.connection_url
        self.connection_url = url

        if not url:
            self._is_valid = False
            self._last_error = "Database connection URL not provided"
            raise ValueError(self._last_error)

        # Close any existing pool before creating a new one
        await self.close()

        try:
            # Configure connection pool with appropriate settings
            self.pool = AsyncConnectionPool(
                conninfo=url,
                min_size=1,
                max_size=5,
                open=False,  # Don't connect immediately
            )

            # Open the pool explicitly
            await self.pool.open()

            # Test the connection pool by executing a simple query
            async with self.pool.connection() as conn:
                async with conn.cursor() as cursor:
                    await cursor.execute("SELECT 1")

            self._is_valid = True
            self._last_error = None
            logger.info(f"Connected to database: {obfuscate_password(url)}")
            return self.pool

        except Exception as e:
            self._is_valid = False
            self._last_error = str(e)

            # Clean up failed pool
            await self.close()

            raise ValueError(f"Connection attempt failed: {obfuscate_password(str(e))}") from e

    async def close(self) -> None:
        """Close the connection pool."""
        if self.pool:
            try:
                await self.pool.close()
            except Exception as e:
                logger.warning(f"Error closing pool: {e}")
            finally:
                self.pool = None
                self._is_valid = False

    @property
    def is_valid(self) -> bool:
        """Check if the connection pool is valid."""
        return self._is_valid

    @property
    def last_error(self) -> str | None:
        """Get the last error message."""
        return self._last_error

    async def reconnect(self) -> bool:
        """
        Attempt to reconnect to the database if the pool is invalid.

        Returns:
            True if reconnection was successful, False otherwise
        """
        if self._is_valid and self.pool:
            return True

        if not self.connection_url:
            logger.error("Cannot reconnect: no connection URL stored")
            return False

        try:
            await self.connect(self.connection_url)
            return True
        except Exception as e:
            logger.error(f"Reconnection failed: {e}")
            return False


class SqlDriver:
    """
    Adapter class that wraps a PostgreSQL connection pool.
    Provides methods for executing queries with proper transaction handling.
    """

    def __init__(self, pool: DbConnPool):
        """
        Initialize with a database connection pool.

        Args:
            pool: Database connection pool
        """
        self.pool = pool

    async def execute_query(
        self,
        query: str,
        params: list[Any] | None = None,
        force_readonly: bool = True,
    ) -> list[dict[str, Any]] | None:
        """
        Execute a query and return results.

        Args:
            query: SQL query to execute
            params: Query parameters
            force_readonly: Whether to enforce read-only mode

        Returns:
            List of dictionaries representing rows or None for non-SELECT queries

        Raises:
            ValueError: If pool is not connected and reconnection fails
            Exception: If query execution fails
        """
        # Attempt reconnection if pool is invalid
        if not self.pool.pool or not self.pool.is_valid:
            logger.warning("Pool not connected, attempting to reconnect...")
            if await self.pool.reconnect():
                logger.info("Reconnection successful")
            else:
                raise ValueError(
                    f"Database pool not connected and reconnection failed. "
                    f"Last error: {self.pool.last_error}"
                )

        try:
            async with self.pool.pool.connection() as connection:
                return await self._execute_with_connection(
                    connection, query, params, force_readonly
                )
        except (ConnectionError, OSError, TimeoutError, OperationalError, PoolTimeout) as e:
            self.pool._is_valid = False
            self.pool._last_error = str(e)
            logger.error(f"Connection error, marking pool as invalid: {e}")
            raise
        except Exception as e:
            logger.error(f"Error executing query: {e}")
            raise

    async def _execute_with_connection(
        self,
        connection,
        query: str,
        params: list[Any] | None,
        force_readonly: bool
    ) -> list[dict[str, Any]] | None:
        """
        Execute query with the given connection.

        Args:
            connection: Database connection
            query: SQL query to execute
            params: Query parameters
            force_readonly: Whether to enforce read-only mode

        Returns:
            List of dictionaries representing rows or None
        """
        transaction_started = False

        try:
            async with connection.cursor(row_factory=dict_row) as cursor:
                # Start read-only transaction if requested
                if force_readonly:
                    await cursor.execute("BEGIN TRANSACTION READ ONLY")
                    transaction_started = True

                if params:
                    await cursor.execute(query, params)
                else:
                    await cursor.execute(query)

                # For multiple statements, move to the last statement's results
                while cursor.nextset():
                    pass

                # No results (like DDL statements)
                if cursor.description is None:
                    if not force_readonly:
                        await cursor.execute("COMMIT")
                    elif transaction_started:
                        await cursor.execute("ROLLBACK")
                        transaction_started = False
                    return None

                # Get results from the last statement only
                rows = await cursor.fetchall()

                # End the transaction appropriately
                if not force_readonly:
                    await cursor.execute("COMMIT")
                elif transaction_started:
                    await cursor.execute("ROLLBACK")
                    transaction_started = False

                return [dict(row) for row in rows]

        except Exception as e:
            # Try to roll back the transaction if it's still active
            if transaction_started:
                try:
                    await connection.rollback()
                except Exception as rollback_error:
                    logger.error(f"Error rolling back transaction: {rollback_error}")

            logger.error(f"Error executing query ({query[:100]}...): {e}")
            raise


async def check_extension_installed(driver: SqlDriver, extension_name: str) -> bool:
    """
    Check if a PostgreSQL extension is installed.

    Args:
        driver: SQL driver instance
        extension_name: Name of the extension to check

    Returns:
        True if the extension is installed
    """
    try:
        result = await driver.execute_query(
            "SELECT 1 FROM pg_extension WHERE extname = %s",
            [extension_name]
        )
        return result is not None and len(result) > 0
    except Exception:
        return False


async def check_extension_available(driver: SqlDriver, extension_name: str) -> bool:
    """
    Check if a PostgreSQL extension is available (can be installed).

    Args:
        driver: SQL driver instance
        extension_name: Name of the extension to check

    Returns:
        True if the extension is available
    """
    try:
        result = await driver.execute_query(
            "SELECT 1 FROM pg_available_extensions WHERE name = %s",
            [extension_name]
        )
        return result is not None and len(result) > 0
    except Exception:
        return False


async def get_postgres_version(driver: SqlDriver) -> int:
    """
    Get the major PostgreSQL version as an integer.

    Args:
        driver: SQL driver instance

    Returns:
        Major PostgreSQL version (e.g., 16 for PostgreSQL 16.2)
    """
    try:
        result = await driver.execute_query("SHOW server_version")
        if result:
            version_string = result[0]["server_version"]
            major_version = version_string.split(".")[0]
            return int(major_version)
    except Exception as e:
        logger.warning(f"Could not determine PostgreSQL version: {e}")
    return 0
