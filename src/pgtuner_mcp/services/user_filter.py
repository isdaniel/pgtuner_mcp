"""
User filtering configuration for excluding specific user IDs from queries.

This module provides functionality to parse and manage excluded user IDs
from environment variables. The excluded user IDs are used to filter out
system or monitoring users from pg_stat_activity and pg_stat_statements queries.
"""

from __future__ import annotations

import logging
import os
from typing import Any

logger = logging.getLogger(__name__)

# Environment variable name for excluded user IDs
EXCLUDE_USERIDS_ENV = "PGTUNER_EXCLUDE_USERIDS"


class UserFilter:
    """
    Manages user ID filtering for PostgreSQL system views.

    This class parses excluded user IDs from an environment variable and
    provides methods to generate SQL filter clauses for excluding those users
    from queries against pg_stat_activity (usesysid) and pg_stat_statements (userid).

    Environment Variable:
        PGTUNER_EXCLUDE_USERIDS: Comma-separated list of user IDs (OIDs) to exclude.
                                  Example: "16384,16385,16386"

    Usage:
        user_filter = UserFilter()

        # For pg_stat_activity queries (usesysid column)
        filter_clause = user_filter.get_activity_filter()

        # For pg_stat_statements queries (userid column)
        filter_clause = user_filter.get_statements_filter()
    """

    _instance: UserFilter | None = None
    _excluded_userids: list[int] | None = None

    def __new__(cls) -> UserFilter:
        """Singleton pattern to ensure consistent configuration."""
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._load_config()
        return cls._instance

    def _load_config(self) -> None:
        """Load and parse the excluded user IDs from environment variable."""
        self._excluded_userids = []

        env_value = os.environ.get(EXCLUDE_USERIDS_ENV, "").strip()
        if not env_value:
            logger.info(f"No {EXCLUDE_USERIDS_ENV} environment variable set, no users will be excluded")
            return

        # Parse comma-separated user IDs
        for userid_str in env_value.split(","):
            userid_str = userid_str.strip()
            if not userid_str:
                continue

            try:
                userid = int(userid_str)
                self._excluded_userids.append(userid)
                logger.debug(f"Added user ID {userid} to exclusion list")
            except ValueError:
                logger.warning(f"Invalid user ID in {EXCLUDE_USERIDS_ENV}: '{userid_str}' (must be integer)")

        if self._excluded_userids:
            logger.info(f"Configured to exclude {len(self._excluded_userids)} user ID(s): {self._excluded_userids}")

    @classmethod
    def reload(cls) -> None:
        """
        Reload the configuration from environment variables.

        Useful for testing or when environment variables change.
        """
        cls._instance = None
        cls()

    @property
    def has_exclusions(self) -> bool:
        """Check if any user IDs are configured for exclusion."""
        return bool(self._excluded_userids)

    def get_activity_filter(self) -> str:
        """
        Get SQL filter clause for pg_stat_activity queries.

        Uses the 'usesysid' column which represents the OID of the user
        who owns the backend process.

        Returns:
            SQL WHERE clause fragment, or empty string if no exclusions.
            Example: "AND usesysid NOT IN (16384, 16385)"
        """
        return self._get_filter_clause("usesysid")

    def get_statements_filter(self) -> str:
        """
        Get SQL filter clause for pg_stat_statements queries.

        Uses the 'userid' column which represents the OID of the user
        who executed the statement.

        Returns:
            SQL WHERE clause fragment, or empty string if no exclusions.
            Example: "AND userid NOT IN (16384, 16385)"
        """
        return self._get_filter_clause("userid")

    def _get_filter_clause(self, column_name: str) -> str:
        """
        Generate a SQL NOT IN clause for the specified column.

        Args:
            column_name: The column name to filter on

        Returns:
            SQL WHERE clause fragment, or empty string if no exclusions.
        """
        if not self._excluded_userids:
            return ""

        # Build the NOT IN clause with the user IDs
        userids_str = ", ".join(str(uid) for uid in self._excluded_userids)
        return f"AND {column_name} NOT IN ({userids_str})"

    def get_filter_params(self) -> list[int]:
        """
        Get excluded user IDs as a list suitable for parameterized queries.

        Returns:
            List of excluded user IDs (can be empty)
        """
        return list(self._excluded_userids) if self._excluded_userids else []


def get_user_filter() -> UserFilter:
    """
    Get the singleton UserFilter instance.

    Returns:
        The configured UserFilter instance
    """
    return UserFilter()
