"""
Index Advisor service for PostgreSQL index recommendations.

This service analyzes queries and workloads to recommend optimal indexes.
It uses HypoPG for hypothetical index testing when available.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any

try:
    from pglast import parse_sql
    HAS_PGLAST = True
except ImportError:
    HAS_PGLAST = False

from .hypopg_service import HypoPGService
from .sql_driver import SqlDriver
from .user_filter import get_user_filter

logger = logging.getLogger(__name__)

@dataclass
class IndexRecommendation:
    """Represents an index recommendation."""
    table: str
    columns: list[str]
    using: str = "btree"
    estimated_size_bytes: int = 0
    estimated_improvement: float | None = None
    reason: str = ""
    create_statement: str = ""

    @property
    def definition(self) -> str:
        """Generate the CREATE INDEX statement."""
        if self.create_statement:
            return self.create_statement
        columns_str = ", ".join(self.columns)
        return f"CREATE INDEX ON {self.table} USING {self.using} ({columns_str})"


@dataclass
class WorkloadAnalysisResult:
    """Result of workload analysis."""
    recommendations: list[IndexRecommendation] = field(default_factory=list)
    analyzed_queries: int = 0
    total_improvement: float | None = None
    error: str | None = None


class IndexAdvisor:
    """
    Service for analyzing queries and recommending indexes.

    Uses a combination of:
    - Query parsing to extract referenced columns
    - pg_stat_statements for workload analysis
    - HypoPG for hypothetical index testing
    """

    def __init__(self, driver: SqlDriver):
        """
        Initialize the index advisor.

        Args:
            driver: SQL driver instance for executing queries
        """
        self.driver = driver
        self.hypopg = HypoPGService(driver)

    async def analyze_query(
        self,
        query: str,
        max_recommendations: int = 5,
    ) -> WorkloadAnalysisResult:
        """
        Analyze a single query and recommend indexes.

        Args:
            query: SQL query to analyze
            max_recommendations: Maximum number of recommendations

        Returns:
            WorkloadAnalysisResult with recommendations
        """
        result = WorkloadAnalysisResult(analyzed_queries=1)

        try:
            # Parse the query to extract table and column info
            columns_by_table = self._extract_columns_from_query(query)

            if not columns_by_table:
                result.error = "Could not extract columns from query"
                return result

            # Check HypoPG availability
            hypopg_status = await self.hypopg.check_status()

            # Generate candidate indexes
            candidates = self._generate_candidate_indexes(columns_by_table)

            # If HypoPG is available, test the candidates
            if hypopg_status.is_installed:
                result.recommendations = await self._evaluate_with_hypopg(
                    query, candidates, max_recommendations
                )
            else:
                # Without HypoPG, just return the candidates
                result.recommendations = candidates[:max_recommendations]
                for rec in result.recommendations:
                    rec.reason = "Recommended based on query structure (HypoPG not available for testing)"

            # Calculate total improvement if we have it
            if result.recommendations:
                improvements = [r.estimated_improvement for r in result.recommendations if r.estimated_improvement]
                if improvements:
                    result.total_improvement = max(improvements)

        except Exception as e:
            logger.error(f"Error analyzing query: {e}")
            result.error = str(e)

        return result

    async def analyze_queries(
        self,
        queries: list[str],
        max_recommendations: int = 10,
    ) -> WorkloadAnalysisResult:
        """
        Analyze multiple queries and recommend indexes.

        Args:
            queries: List of SQL queries to analyze
            max_recommendations: Maximum number of recommendations

        Returns:
            WorkloadAnalysisResult with recommendations
        """
        result = WorkloadAnalysisResult(analyzed_queries=len(queries))

        try:
            # Collect columns from all queries
            all_columns_by_table: dict[str, set[str]] = {}

            for query in queries:
                columns_by_table = self._extract_columns_from_query(query)
                for table, columns in columns_by_table.items():
                    if table not in all_columns_by_table:
                        all_columns_by_table[table] = set()
                    all_columns_by_table[table].update(columns)

            if not all_columns_by_table:
                result.error = "Could not extract columns from any query"
                return result

            # Generate candidate indexes
            candidates = self._generate_candidate_indexes(
                {t: list(c) for t, c in all_columns_by_table.items()}
            )

            # Check HypoPG availability
            hypopg_status = await self.hypopg.check_status()

            if hypopg_status.is_installed:
                # Evaluate each candidate against all queries
                scored_candidates = []

                for candidate in candidates:
                    total_improvement = 0
                    queries_improved = 0

                    for query in queries:
                        try:
                            test_result = await self.hypopg.explain_with_hypothetical_index(
                                query,
                                candidate.table,
                                candidate.columns,
                                candidate.using,
                            )
                            if test_result.get("would_use_index") and test_result.get("improvement_percentage"):
                                total_improvement += test_result["improvement_percentage"]
                                queries_improved += 1
                        except Exception:
                            continue

                    if queries_improved > 0:
                        candidate.estimated_improvement = total_improvement / queries_improved
                        candidate.reason = f"Improves {queries_improved}/{len(queries)} queries"
                        scored_candidates.append(candidate)

                # Sort by improvement
                scored_candidates.sort(
                    key=lambda x: x.estimated_improvement or 0,
                    reverse=True
                )
                result.recommendations = scored_candidates[:max_recommendations]
            else:
                result.recommendations = candidates[:max_recommendations]
                for rec in result.recommendations:
                    rec.reason = "Recommended based on query structure"

        except Exception as e:
            logger.error(f"Error analyzing queries: {e}")
            result.error = str(e)

        return result

    async def analyze_workload(
        self,
        min_calls: int = 50,
        min_avg_time_ms: float = 5.0,
        limit: int = 100,
        max_recommendations: int = 10,
    ) -> WorkloadAnalysisResult:
        """
        Analyze workload from pg_stat_statements and recommend indexes.

        Args:
            min_calls: Minimum call count to consider
            min_avg_time_ms: Minimum average execution time in ms
            limit: Maximum queries to analyze
            max_recommendations: Maximum index recommendations

        Returns:
            WorkloadAnalysisResult with recommendations
        """
        result = WorkloadAnalysisResult()

        # Check if pg_stat_statements is available
        try:
            check_result = await self.driver.execute_query(
                "SELECT 1 FROM pg_extension WHERE extname = 'pg_stat_statements'"
            )
            if not check_result:
                result.error = (
                    "pg_stat_statements extension is required for workload analysis.\n"
                    "Install with: CREATE EXTENSION pg_stat_statements;"
                )
                return result
        except Exception as e:
            result.error = f"Error checking pg_stat_statements: {e}"
            return result

        # Get user filter for excluding specific user IDs
        user_filter = get_user_filter()
        statements_filter = user_filter.get_statements_filter()

        # Get top queries from pg_stat_statements (excluding system catalog queries)
        try:
            # Build query with parameterized values to prevent SQL injection
            base_query = r"""
                SELECT
                    query,
                    calls,
                    mean_exec_time,
                    total_exec_time
                FROM pg_stat_statements
                WHERE calls >= %s
                  AND mean_exec_time >= %s
                  AND query NOT LIKE '%%pg_catalog%%'
                  AND query NOT LIKE '%%information_schema%%'
                  AND query NOT LIKE '%%pg_toast%%'
                  AND query NOT LIKE '%%pg_%%'
                  AND query NOT LIKE '%%$%%'
                  AND query ~* '^\s*SELECT'
            """
            # statements_filter contains validated integer user IDs, safe to append
            query = f"{base_query} {statements_filter} ORDER BY total_exec_time DESC LIMIT %s"
            queries_result = await self.driver.execute_query(
                query, [min_calls, min_avg_time_ms, limit]
            )

            if not queries_result:
                result.error = "No queries found matching criteria"
                return result

            queries = [row.get("query") for row in queries_result if row.get("query")]
            result.analyzed_queries = len(queries)

            # Analyze the collected queries
            return await self.analyze_queries(queries, max_recommendations)

        except Exception as e:
            logger.error(f"Error analyzing workload: {e}")
            result.error = str(e)
            return result

    async def get_existing_indexes(self, table: str) -> list[dict[str, Any]]:
        """
        Get existing indexes for a table.

        Args:
            table: Table name

        Returns:
            List of index information
        """
        result = await self.driver.execute_query(
            """
            SELECT
                i.relname as index_name,
                am.amname as access_method,
                array_agg(a.attname ORDER BY x.ordinality) as columns,
                ix.indisunique as is_unique,
                ix.indisprimary as is_primary,
                pg_relation_size(i.oid) as size_bytes
            FROM pg_index ix
            JOIN pg_class t ON t.oid = ix.indrelid
            JOIN pg_class i ON i.oid = ix.indexrelid
            JOIN pg_am am ON am.oid = i.relam
            JOIN pg_attribute a ON a.attrelid = t.oid
            JOIN LATERAL unnest(ix.indkey) WITH ORDINALITY AS x(attnum, ordinality)
                ON a.attnum = x.attnum
            WHERE t.relname = %s
            GROUP BY i.relname, am.amname, ix.indisunique, ix.indisprimary, i.oid
            """,
            [table]
        )

        if not result:
            return []

        return result

    async def get_index_health(self, schema: str = "public") -> dict[str, Any]:
        """
        Analyze index health for a schema.

        Note: This analyzes only user/client indexes and excludes system
        catalog indexes (pg_catalog, information_schema, pg_toast).

        Args:
            schema: Schema name

        Returns:
            Dictionary with index health information
        """
        health = {
            "duplicate_indexes": [],
            "unused_indexes": [],
            "bloated_indexes": [],
            "invalid_indexes": [],
        }

        # Find duplicate indexes
        try:
            dup_result = await self.driver.execute_query(
                """
                SELECT
                    pg_size_pretty(sum(pg_relation_size(idx))::bigint) as size,
                    array_agg(idx) as indexes,
                    (array_agg(idx))[1] as index1,
                    (array_agg(idx))[2] as index2
                FROM (
                    SELECT
                        indexrelid::regclass as idx,
                        indrelid::regclass as tbl,
                        indkey::text as key
                    FROM pg_index
                    WHERE indrelid::regclass::text LIKE %s || '.%%'
                ) sub
                GROUP BY tbl, key
                HAVING count(*) > 1
                """,
                [schema]
            )
            if dup_result:
                health["duplicate_indexes"] = dup_result
        except Exception as e:
            logger.warning(f"Error finding duplicate indexes: {e}")

        # Find unused indexes (user tables only, exclude system schemas)
        try:
            unused_result = await self.driver.execute_query(
                """
                SELECT
                    schemaname,
                    relname as table_name,
                    indexrelname as index_name,
                    idx_scan as scans,
                    pg_size_pretty(pg_relation_size(indexrelid)) as size
                FROM pg_stat_user_indexes
                WHERE schemaname = %s
                  AND schemaname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
                  AND idx_scan = 0
                  AND indexrelname NOT LIKE '%%_pkey'
                ORDER BY pg_relation_size(indexrelid) DESC
                """,
                [schema]
            )
            if unused_result:
                health["unused_indexes"] = unused_result
        except Exception as e:
            logger.warning(f"Error finding unused indexes: {e}")

        # Find invalid indexes
        try:
            invalid_result = await self.driver.execute_query("""
                SELECT
                    c.relname as index_name,
                    n.nspname as schema_name,
                    t.relname as table_name
                FROM pg_class c
                JOIN pg_index i ON c.oid = i.indexrelid
                JOIN pg_class t ON i.indrelid = t.oid
                JOIN pg_namespace n ON c.relnamespace = n.oid
                WHERE NOT i.indisvalid
            """)
            if invalid_result:
                health["invalid_indexes"] = invalid_result
        except Exception as e:
            logger.warning(f"Error finding invalid indexes: {e}")

        return health

    def _extract_columns_from_query(self, query: str) -> dict[str, list[str]]:
        """
        Extract table and column information from a query.

        Uses pglast to parse the SQL query and extract referenced tables and columns.
        This is particularly useful for identifying columns used in WHERE, JOIN,
        ORDER BY, and GROUP BY clauses.

        Args:
            query: SQL query

        Returns:
            Dictionary mapping table names to column lists
        """
        if not HAS_PGLAST:
            logger.warning("pglast not installed, cannot parse query for column extraction")
            return {}

        try:
            parsed = parse_sql(query)
            if not parsed:
                logger.debug("No statements found in query")
                return {}

            columns_by_table: dict[str, set[str]] = {}
            table_aliases: dict[str, str] = {}  # alias -> table name

            # Extract tables and their aliases, then columns
            for stmt in parsed:
                self._extract_tables_from_node(stmt, table_aliases)
                self._extract_columns_from_node(stmt, columns_by_table, table_aliases)

            return {t: list(c) for t, c in columns_by_table.items() if c}
        except Exception as e:
            logger.warning(f"Could not parse query for column extraction: {e}")
            return {}

    def _extract_tables_from_node(self, node: Any, table_aliases: dict[str, str]) -> None:
        """
        Recursively extract table names and their aliases from AST node.

        Args:
            node: AST node to process
            table_aliases: Dictionary to populate with alias -> table name mappings
        """
        if node is None:
            return

        node_class = type(node).__name__

        # RangeVar represents a table reference
        if node_class == "RangeVar":
            table_name = getattr(node, "relname", None)
            alias_node = getattr(node, "alias", None)
            if table_name:
                if alias_node:
                    alias_name = getattr(alias_node, "aliasname", None)
                    if alias_name:
                        table_aliases[alias_name] = table_name
                # Also map table name to itself for non-aliased references
                table_aliases[table_name] = table_name

        # Recursively process child nodes
        if hasattr(node, "__dict__"):
            for key, value in node.__dict__.items():
                if key.startswith("_"):
                    continue
                if isinstance(value, (list, tuple)):
                    for item in value:
                        self._extract_tables_from_node(item, table_aliases)
                else:
                    self._extract_tables_from_node(value, table_aliases)

    def _extract_columns_from_node(
        self, node: Any, columns: dict[str, set[str]], table_aliases: dict[str, str]
    ) -> None:
        """
        Recursively extract column references from AST node.

        Args:
            node: AST node to process
            columns: Dictionary to populate with table -> column set mappings
            table_aliases: Dictionary of alias -> table name mappings
        """
        if node is None:
            return

        node_class = type(node).__name__

        # ColumnRef represents a column reference (e.g., table.column or just column)
        if node_class == "ColumnRef":
            fields = getattr(node, "fields", None)
            if fields:
                # Fields can be: (column,) or (table/alias, column)
                field_names = []
                for f in fields:
                    f_class = type(f).__name__
                    if f_class == "String":
                        field_names.append(getattr(f, "sval", ""))
                    elif f_class == "A_Star":
                        # SELECT * - skip this
                        return

                if len(field_names) == 2:
                    # table.column or alias.column
                    table_or_alias, col_name = field_names
                    table_name = table_aliases.get(table_or_alias, table_or_alias)
                    if table_name and col_name:
                        if table_name not in columns:
                            columns[table_name] = set()
                        columns[table_name].add(col_name)
                elif len(field_names) == 1 and table_aliases:
                    # Just column name - associate with first known table if only one
                    col_name = field_names[0]
                    # Get unique table names (not aliases)
                    unique_tables = set(table_aliases.values())
                    if len(unique_tables) == 1:
                        table_name = list(unique_tables)[0]
                        if table_name not in columns:
                            columns[table_name] = set()
                        columns[table_name].add(col_name)

        # Recursively process child nodes
        if hasattr(node, "__dict__"):
            for key, value in node.__dict__.items():
                if key.startswith("_"):
                    continue
                if isinstance(value, (list, tuple)):
                    for item in value:
                        self._extract_columns_from_node(item, columns, table_aliases)
                else:
                    self._extract_columns_from_node(value, columns, table_aliases)

    def _generate_candidate_indexes(
        self,
        columns_by_table: dict[str, list[str]]
    ) -> list[IndexRecommendation]:
        """
        Generate candidate indexes from column information.

        Args:
            columns_by_table: Dictionary mapping tables to columns

        Returns:
            List of index recommendations
        """
        candidates = []

        for table, columns in columns_by_table.items():
            if not columns:
                continue

            # Single column indexes
            for col in columns:
                candidates.append(IndexRecommendation(
                    table=table,
                    columns=[col],
                    reason="Single column index on frequently used column"
                ))

            # Multi-column index if we have multiple columns
            if len(columns) > 1:
                # Limit to 3 columns
                combo_columns = columns[:3]
                candidates.append(IndexRecommendation(
                    table=table,
                    columns=combo_columns,
                    reason="Composite index for multi-column filter"
                ))

        return candidates

    async def _evaluate_with_hypopg(
        self,
        query: str,
        candidates: list[IndexRecommendation],
        max_recommendations: int,
    ) -> list[IndexRecommendation]:
        """
        Evaluate candidate indexes using HypoPG.

        Args:
            query: Query to test
            candidates: List of candidate indexes
            max_recommendations: Maximum recommendations to return

        Returns:
            Evaluated and sorted recommendations
        """
        evaluated = []

        # Reset any existing hypothetical indexes
        await self.hypopg.reset()

        for candidate in candidates:
            try:
                test_result = await self.hypopg.explain_with_hypothetical_index(
                    query,
                    candidate.table,
                    candidate.columns,
                    candidate.using,
                )

                if test_result.get("would_use_index"):
                    candidate.estimated_improvement = test_result.get("improvement_percentage")
                    candidate.estimated_size_bytes = test_result.get("hypothetical_index", {}).get("estimated_size", 0)
                    candidate.create_statement = test_result.get("hypothetical_index", {}).get("definition", "")
                    candidate.reason = f"Estimated {candidate.estimated_improvement:.1f}% improvement" if candidate.estimated_improvement else "Would be used by query"
                    evaluated.append(candidate)

            except Exception as e:
                logger.warning(f"Error evaluating candidate {candidate}: {e}")

        # Sort by improvement
        evaluated.sort(key=lambda x: x.estimated_improvement or 0, reverse=True)

        return evaluated[:max_recommendations]
