"""Query rewrite suggestions tool handlers."""

from __future__ import annotations

import re
from collections.abc import Sequence
from typing import Any

from mcp.types import TextContent, Tool

from ..services import SqlDriver
from .toolhandler import ToolHandler


class QueryRewriteSuggestionsToolHandler(ToolHandler):
    """Tool handler for suggesting query rewrites for common anti-patterns."""

    name = "get_query_rewrite_suggestions"
    title = "Query Rewrite Advisor"
    read_only_hint = True
    destructive_hint = False
    idempotent_hint = True
    open_world_hint = False
    description = """Suggest query rewrites for common SQL anti-patterns.

Analyzes SQL queries and suggests optimized alternatives for:

Anti-patterns detected:
- SELECT * usage (suggests specific column selection)
- Functions on indexed columns in WHERE clause (prevents index usage)
- Implicit type conversions
- NOT IN with subqueries (suggests NOT EXISTS or LEFT JOIN)
- OR conditions that prevent index usage
- LIKE patterns starting with wildcard
- ORDER BY with LIMIT without proper index
- Correlated subqueries that could be JOINs
- DISTINCT when GROUP BY would be more efficient
- Multiple OR conditions on same column (suggests IN)
- UNION when UNION ALL would suffice
- COUNT(*) for existence checks (suggests EXISTS)

Returns:
- List of detected anti-patterns
- Suggested rewrites with explanations
- Severity level for each issue
- Potential performance impact

This tool performs static analysis and does not execute queries."""

    def __init__(self, sql_driver: SqlDriver):
        self.sql_driver = sql_driver

    def get_tool_definition(self) -> Tool:
        return Tool(
            name=self.name,
            description=self.description,
            inputSchema={
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": "SQL query to analyze for anti-patterns"
                    },
                    "include_explain": {
                        "type": "boolean",
                        "description": "Include EXPLAIN output to enhance suggestions",
                        "default": False
                    },
                    "table_info": {
                        "type": "boolean",
                        "description": "Fetch table/index information to improve suggestions",
                        "default": True
                    }
                },
                "required": ["query"]
            },
            annotations=self.get_annotations()
        )

    async def run_tool(self, arguments: dict[str, Any]) -> Sequence[TextContent]:
        try:
            self.validate_required_args(arguments, ["query"])

            query = arguments["query"]
            include_explain = arguments.get("include_explain", False)
            table_info = arguments.get("table_info", True)

            # Normalize query for analysis
            normalized_query = self._normalize_query(query)

            # Detect anti-patterns
            suggestions = []

            # Run all anti-pattern detectors
            suggestions.extend(self._check_select_star(normalized_query, query))
            suggestions.extend(self._check_function_on_column(normalized_query, query))
            suggestions.extend(self._check_not_in_subquery(normalized_query, query))
            suggestions.extend(self._check_or_conditions(normalized_query, query))
            suggestions.extend(self._check_like_wildcard_prefix(normalized_query, query))
            suggestions.extend(self._check_distinct_vs_group_by(normalized_query, query))
            suggestions.extend(self._check_multiple_or_same_column(normalized_query, query))
            suggestions.extend(self._check_union_vs_union_all(normalized_query, query))
            suggestions.extend(self._check_count_for_existence(normalized_query, query))
            suggestions.extend(self._check_implicit_conversion(normalized_query, query))
            suggestions.extend(self._check_correlated_subquery(normalized_query, query))
            suggestions.extend(self._check_order_by_limit(normalized_query, query))
            suggestions.extend(self._check_null_comparisons(normalized_query, query))
            suggestions.extend(self._check_cartesian_product(normalized_query, query))

            # Optionally get EXPLAIN output for enhanced analysis
            explain_analysis = None
            if include_explain:
                explain_analysis = await self._get_explain_analysis(query)
                if explain_analysis:
                    suggestions.extend(explain_analysis.get("suggestions", []))

            # Optionally fetch table/index information
            table_analysis = None
            if table_info:
                table_analysis = await self._analyze_tables_in_query(normalized_query)

            # Sort by severity
            severity_order = {"critical": 0, "high": 1, "medium": 2, "low": 3, "info": 4}
            suggestions.sort(key=lambda x: severity_order.get(x.get("severity", "info"), 5))

            output = {
                "query": query,
                "suggestions_count": len(suggestions),
                "suggestions": suggestions,
                "summary": self._generate_summary(suggestions)
            }

            if explain_analysis:
                output["explain_analysis"] = explain_analysis.get("plan_info")

            if table_analysis:
                output["table_info"] = table_analysis

            return self.format_json_result(output)

        except Exception as e:
            return self.format_error(e)

    def _normalize_query(self, query: str) -> str:
        """Normalize query for pattern matching."""
        # Convert to uppercase for consistent matching
        normalized = query.upper()
        # Normalize whitespace
        normalized = " ".join(normalized.split())
        return normalized

    def _check_select_star(self, normalized: str, original: str) -> list[dict[str, Any]]:
        """Check for SELECT * usage."""
        suggestions = []

        if re.search(r'\bSELECT\s+\*\s+FROM\b', normalized):
            # Try to extract table name
            table_match = re.search(r'\bSELECT\s+\*\s+FROM\s+([^\s,;]+)', normalized)
            table_name = table_match.group(1) if table_match else "table"

            suggestions.append({
                "pattern": "SELECT *",
                "severity": "medium",
                "issue": "Using SELECT * retrieves all columns, which may include unnecessary data",
                "impact": "Increased I/O, memory usage, and network transfer. May prevent covering index usage.",
                "suggestion": f"Specify only the columns you need: SELECT column1, column2, ... FROM {table_name}",
                "example_rewrite": None  # Would need schema info to provide specific columns
            })

        return suggestions

    def _check_function_on_column(self, normalized: str, original: str) -> list[dict[str, Any]]:
        """Check for functions on indexed columns in WHERE clause."""
        suggestions = []

        # Common function patterns on columns
        patterns = [
            (r'\bWHERE\s+.*\bUPPER\s*\(\s*(\w+)\s*\)', 'UPPER', 'Create a functional index or use ILIKE'),
            (r'\bWHERE\s+.*\bLOWER\s*\(\s*(\w+)\s*\)', 'LOWER', 'Create a functional index or use ILIKE'),
            (r'\bWHERE\s+.*\bDATE\s*\(\s*(\w+)\s*\)', 'DATE', 'Use range conditions: column >= date AND column < date + 1'),
            (r'\bWHERE\s+.*\bYEAR\s*\(\s*(\w+)\s*\)', 'YEAR', 'Use range conditions on the timestamp column'),
            (r'\bWHERE\s+.*\bMONTH\s*\(\s*(\w+)\s*\)', 'MONTH', 'Use range conditions on the timestamp column'),
            (r'\bWHERE\s+.*\bCOALESCE\s*\(\s*(\w+)', 'COALESCE', 'Consider handling NULL separately or use IS NOT DISTINCT FROM'),
            (r'\bWHERE\s+.*\bTRIM\s*\(\s*(\w+)\s*\)', 'TRIM', 'Store pre-trimmed values or create a functional index'),
            (r'\bWHERE\s+.*\bCAST\s*\(\s*(\w+)', 'CAST', 'Ensure data types match to avoid implicit casting'),
            (r'\bWHERE\s+.*\bTO_CHAR\s*\(\s*(\w+)', 'TO_CHAR', 'Use native type comparison instead of converting to string'),
            (r'\bWHERE\s+.*\bEXTRACT\s*\([^)]+\s+FROM\s+(\w+)\)', 'EXTRACT', 'Use range conditions on the original column'),
        ]

        for pattern, func_name, fix in patterns:
            match = re.search(pattern, normalized)
            if match:
                column = match.group(1) if match.lastindex else "column"
                suggestions.append({
                    "pattern": f"Function on column ({func_name})",
                    "severity": "high",
                    "issue": f"Using {func_name}() on column '{column}' in WHERE clause prevents index usage",
                    "impact": "Forces full table scan even if an index exists on the column",
                    "suggestion": fix,
                    "example_rewrite": self._get_function_rewrite_example(func_name, column)
                })

        return suggestions

    def _get_function_rewrite_example(self, func_name: str, column: str) -> str | None:
        """Get example rewrite for function on column."""
        examples = {
            "UPPER": f"-- Instead of: WHERE UPPER({column}) = 'VALUE'\n-- Use: WHERE {column} ILIKE 'value'",
            "LOWER": f"-- Instead of: WHERE LOWER({column}) = 'value'\n-- Use: WHERE {column} ILIKE 'value'",
            "DATE": f"-- Instead of: WHERE DATE({column}) = '2024-01-01'\n-- Use: WHERE {column} >= '2024-01-01' AND {column} < '2024-01-02'",
            "YEAR": f"-- Instead of: WHERE YEAR({column}) = 2024\n-- Use: WHERE {column} >= '2024-01-01' AND {column} < '2025-01-01'",
            "COALESCE": f"-- Instead of: WHERE COALESCE({column}, '') = 'value'\n-- Use: WHERE ({column} = 'value' OR ({column} IS NULL AND '' = 'value'))",
        }
        return examples.get(func_name)

    def _check_not_in_subquery(self, normalized: str, original: str) -> list[dict[str, Any]]:
        """Check for NOT IN with subquery."""
        suggestions = []

        if re.search(r'\bNOT\s+IN\s*\(\s*SELECT\b', normalized):
            suggestions.append({
                "pattern": "NOT IN (subquery)",
                "severity": "high",
                "issue": "NOT IN with subquery can be inefficient and has NULL handling issues",
                "impact": "Poor performance with large datasets. Returns no rows if subquery contains NULL.",
                "suggestion": "Use NOT EXISTS or LEFT JOIN with IS NULL check",
                "example_rewrite": """-- Instead of: SELECT * FROM t1 WHERE col NOT IN (SELECT col FROM t2)
-- Use NOT EXISTS:
SELECT * FROM t1 WHERE NOT EXISTS (SELECT 1 FROM t2 WHERE t2.col = t1.col)
-- Or use LEFT JOIN:
SELECT t1.* FROM t1 LEFT JOIN t2 ON t1.col = t2.col WHERE t2.col IS NULL"""
            })

        return suggestions

    def _check_or_conditions(self, normalized: str, original: str) -> list[dict[str, Any]]:
        """Check for OR conditions that might prevent index usage."""
        suggestions = []

        # Count OR conditions
        or_count = len(re.findall(r'\bOR\b', normalized))

        if or_count > 3:
            suggestions.append({
                "pattern": "Multiple OR conditions",
                "severity": "medium",
                "issue": f"Query contains {or_count} OR conditions which may prevent efficient index usage",
                "impact": "Optimizer may resort to full table scan instead of using indexes",
                "suggestion": "Consider using UNION ALL for complex OR conditions, or restructure with IN clause",
                "example_rewrite": """-- Instead of: WHERE a = 1 OR a = 2 OR a = 3
-- Use: WHERE a IN (1, 2, 3)

-- For different columns, consider UNION ALL:
-- Instead of: WHERE (a = 1 AND b = 2) OR (a = 3 AND b = 4)
-- Use:
-- SELECT * FROM t WHERE a = 1 AND b = 2
-- UNION ALL
-- SELECT * FROM t WHERE a = 3 AND b = 4"""
            })

        return suggestions

    def _check_like_wildcard_prefix(self, normalized: str, original: str) -> list[dict[str, Any]]:
        """Check for LIKE patterns starting with wildcard."""
        suggestions = []

        if re.search(r"\bLIKE\s+'%", normalized) or re.search(r"\bILIKE\s+'%", normalized):
            suggestions.append({
                "pattern": "LIKE '%...' (leading wildcard)",
                "severity": "high",
                "issue": "LIKE pattern starting with % cannot use B-tree indexes",
                "impact": "Forces full table scan or index scan",
                "suggestion": "Consider full-text search (tsvector/tsquery), trigram indexes (pg_trgm), or reverse indexes",
                "example_rewrite": """-- For suffix search, consider reverse index:
-- CREATE INDEX idx_reverse ON table (reverse(column));
-- SELECT * FROM table WHERE reverse(column) LIKE reverse('%suffix');

-- For contains search, consider pg_trgm:
-- CREATE EXTENSION pg_trgm;
-- CREATE INDEX idx_trgm ON table USING gin (column gin_trgm_ops);
-- SELECT * FROM table WHERE column LIKE '%pattern%';"""
            })

        return suggestions

    def _check_distinct_vs_group_by(self, normalized: str, original: str) -> list[dict[str, Any]]:
        """Check if DISTINCT could be replaced with GROUP BY."""
        suggestions = []

        # Check for SELECT DISTINCT with aggregation that could use GROUP BY
        if re.search(r'\bSELECT\s+DISTINCT\b', normalized):
            # Check if there's also an aggregate function
            has_aggregate = bool(re.search(r'\b(COUNT|SUM|AVG|MIN|MAX)\s*\(', normalized))

            if has_aggregate:
                suggestions.append({
                    "pattern": "DISTINCT with aggregation",
                    "severity": "medium",
                    "issue": "Using DISTINCT with aggregate functions may indicate incorrect query structure",
                    "impact": "May produce unexpected results or inefficient execution",
                    "suggestion": "Review query logic - typically GROUP BY should be used with aggregates",
                    "example_rewrite": """-- Instead of: SELECT DISTINCT department, COUNT(*) FROM employees
-- Use: SELECT department, COUNT(*) FROM employees GROUP BY department"""
                })
            else:
                suggestions.append({
                    "pattern": "SELECT DISTINCT",
                    "severity": "low",
                    "issue": "DISTINCT requires sorting or hashing all result rows",
                    "impact": "Can be expensive for large result sets",
                    "suggestion": "Ensure DISTINCT is necessary. Consider if data model changes could eliminate duplicates at source.",
                    "example_rewrite": None
                })

        return suggestions

    def _check_multiple_or_same_column(self, normalized: str, original: str) -> list[dict[str, Any]]:
        """Check for multiple OR conditions on the same column."""
        suggestions = []

        # Pattern: column = value OR column = value
        pattern = r'(\w+)\s*=\s*[^)]+\s+OR\s+\1\s*='
        if re.search(pattern, normalized):
            match = re.search(pattern, normalized)
            column = match.group(1) if match else "column"
            suggestions.append({
                "pattern": "Multiple OR on same column",
                "severity": "low",
                "issue": f"Multiple OR conditions on column '{column}' can be simplified",
                "impact": "Minor readability and potential optimization issue",
                "suggestion": "Use IN clause instead of multiple OR conditions",
                "example_rewrite": f"-- Instead of: WHERE {column} = 'a' OR {column} = 'b' OR {column} = 'c'\n-- Use: WHERE {column} IN ('a', 'b', 'c')"
            })

        return suggestions

    def _check_union_vs_union_all(self, normalized: str, original: str) -> list[dict[str, Any]]:
        """Check for UNION that could be UNION ALL."""
        suggestions = []

        # Count UNION (not UNION ALL)
        union_count = len(re.findall(r'\bUNION\b(?!\s+ALL)', normalized))

        if union_count > 0:
            suggestions.append({
                "pattern": "UNION without ALL",
                "severity": "low",
                "issue": f"Query uses UNION {union_count} time(s) which removes duplicates",
                "impact": "UNION performs an implicit DISTINCT, requiring additional sorting/hashing",
                "suggestion": "If duplicate removal is not needed, use UNION ALL for better performance",
                "example_rewrite": """-- If duplicates are acceptable or impossible:
-- Instead of: SELECT a FROM t1 UNION SELECT a FROM t2
-- Use: SELECT a FROM t1 UNION ALL SELECT a FROM t2"""
            })

        return suggestions

    def _check_count_for_existence(self, normalized: str, original: str) -> list[dict[str, Any]]:
        """Check for COUNT(*) used for existence checks."""
        suggestions = []

        # Pattern: WHERE (SELECT COUNT(*) ...) > 0 or similar
        if re.search(r'\(\s*SELECT\s+COUNT\s*\(\s*\*?\s*\)\s+FROM', normalized):
            if re.search(r'>\s*0|>=\s*1|!=\s*0|<>\s*0', normalized):
                suggestions.append({
                    "pattern": "COUNT(*) for existence check",
                    "severity": "medium",
                    "issue": "Using COUNT(*) > 0 to check existence is inefficient",
                    "impact": "COUNT(*) scans all matching rows when you only need to know if any exist",
                    "suggestion": "Use EXISTS subquery which stops at first match",
                    "example_rewrite": """-- Instead of: SELECT * FROM t1 WHERE (SELECT COUNT(*) FROM t2 WHERE t2.id = t1.id) > 0
-- Use: SELECT * FROM t1 WHERE EXISTS (SELECT 1 FROM t2 WHERE t2.id = t1.id)"""
                })

        return suggestions

    def _check_implicit_conversion(self, normalized: str, original: str) -> list[dict[str, Any]]:
        """Check for potential implicit type conversions."""
        suggestions = []

        # Check for comparing strings with numbers (common pattern)
        if re.search(r"=\s*'\d+'", normalized) and re.search(r"=\s*\d+(?!')", normalized):
            suggestions.append({
                "pattern": "Potential implicit conversion",
                "severity": "medium",
                "issue": "Query may have implicit type conversions between string and numeric values",
                "impact": "Implicit conversions can prevent index usage and cause unexpected results",
                "suggestion": "Ensure data types match between columns and compared values",
                "example_rewrite": """-- Ensure types match:
-- If column is INTEGER: WHERE id = 123 (not '123')
-- If column is VARCHAR: WHERE code = '123' (not 123)"""
            })

        return suggestions

    def _check_correlated_subquery(self, normalized: str, original: str) -> list[dict[str, Any]]:
        """Check for correlated subqueries that could be JOINs."""
        suggestions = []

        # Pattern: subquery that references outer table
        if re.search(r'WHERE\s+.*\(\s*SELECT\s+.*WHERE\s+.*\.\w+\s*=\s*\w+\.\w+', normalized):
            suggestions.append({
                "pattern": "Correlated subquery",
                "severity": "medium",
                "issue": "Correlated subquery executes once for each row in the outer query",
                "impact": "Can be very slow for large tables as it multiplies execution",
                "suggestion": "Consider rewriting as JOIN or using window functions",
                "example_rewrite": """-- Instead of:
-- SELECT *, (SELECT MAX(date) FROM orders o WHERE o.customer_id = c.id)
-- FROM customers c

-- Use JOIN:
-- SELECT c.*, o.max_date
-- FROM customers c
-- LEFT JOIN (SELECT customer_id, MAX(date) as max_date FROM orders GROUP BY customer_id) o
-- ON c.id = o.customer_id"""
            })

        return suggestions

    def _check_order_by_limit(self, normalized: str, original: str) -> list[dict[str, Any]]:
        """Check for ORDER BY with LIMIT that might need index."""
        suggestions = []

        if re.search(r'\bORDER\s+BY\b.*\bLIMIT\b', normalized):
            # Check if there's a WHERE clause (index opportunity)
            has_where = bool(re.search(r'\bWHERE\b', normalized))

            suggestions.append({
                "pattern": "ORDER BY with LIMIT",
                "severity": "info",
                "issue": "ORDER BY with LIMIT can be slow without proper index",
                "impact": "Without index, PostgreSQL may sort entire result set before limiting",
                "suggestion": "Ensure there's an index matching the ORDER BY columns" +
                             (" (consider composite index with WHERE clause columns)" if has_where else ""),
                "example_rewrite": f"""-- For: SELECT * FROM table {'WHERE status = active ' if has_where else ''}ORDER BY created_at DESC LIMIT 10
-- Create index: CREATE INDEX idx_{'status_' if has_where else ''}created_at ON table ({'status, ' if has_where else ''}created_at DESC)"""
            })

        return suggestions

    def _check_null_comparisons(self, normalized: str, original: str) -> list[dict[str, Any]]:
        """Check for incorrect NULL comparisons."""
        suggestions = []

        # Check for = NULL or != NULL
        if re.search(r'[!=]=\s*NULL\b', normalized) or re.search(r'<>\s*NULL\b', normalized):
            suggestions.append({
                "pattern": "Incorrect NULL comparison",
                "severity": "critical",
                "issue": "Using = or != with NULL always returns NULL (unknown), not true/false",
                "impact": "Query will not return expected results. This is likely a bug.",
                "suggestion": "Use IS NULL or IS NOT NULL for NULL comparisons",
                "example_rewrite": """-- Instead of: WHERE column = NULL
-- Use: WHERE column IS NULL

-- Instead of: WHERE column != NULL or WHERE column <> NULL
-- Use: WHERE column IS NOT NULL"""
            })

        return suggestions

    def _check_cartesian_product(self, normalized: str, original: str) -> list[dict[str, Any]]:
        """Check for potential Cartesian products (CROSS JOINs)."""
        suggestions = []

        # Check for multiple tables without proper JOIN or WHERE conditions
        if re.search(r'\bCROSS\s+JOIN\b', normalized):
            suggestions.append({
                "pattern": "CROSS JOIN",
                "severity": "high",
                "issue": "CROSS JOIN creates a Cartesian product of all rows",
                "impact": "Result size = rows_in_t1 × rows_in_t2, can be extremely large",
                "suggestion": "Verify this is intentional. Usually INNER/LEFT JOIN with ON clause is needed.",
                "example_rewrite": """-- If you need to match rows:
-- Instead of: SELECT * FROM t1 CROSS JOIN t2
-- Use: SELECT * FROM t1 INNER JOIN t2 ON t1.key = t2.key"""
            })

        # Check for comma-separated tables without WHERE (old-style join)
        from_match = re.search(r'\bFROM\s+(\w+)\s*,\s*(\w+)', normalized)
        if from_match and not re.search(r'\bWHERE\b.*' + from_match.group(1) + r'.*' + from_match.group(2), normalized):
            suggestions.append({
                "pattern": "Comma-separated tables (potential Cartesian product)",
                "severity": "high",
                "issue": "Multiple tables in FROM clause without proper join condition",
                "impact": "May create unintended Cartesian product",
                "suggestion": "Use explicit JOIN syntax with ON clause",
                "example_rewrite": """-- Instead of: SELECT * FROM t1, t2 WHERE t1.id = t2.t1_id
-- Use: SELECT * FROM t1 INNER JOIN t2 ON t1.id = t2.t1_id"""
            })

        return suggestions

    async def _get_explain_analysis(self, query: str) -> dict[str, Any] | None:
        """Get EXPLAIN output for enhanced analysis."""
        try:
            explain_query = f"EXPLAIN (FORMAT JSON) {query}"
            results = await self.sql_driver.execute_query(explain_query)

            if not results:
                return None

            import json
            plan_data = results[0].get("QUERY PLAN", results)
            if isinstance(plan_data, str):
                plan_data = json.loads(plan_data)

            suggestions = []
            plan_info = {}

            if isinstance(plan_data, list) and plan_data:
                root = plan_data[0]
                plan = root.get("Plan", root)

                plan_info = {
                    "total_cost": plan.get("Total Cost"),
                    "plan_rows": plan.get("Plan Rows"),
                    "node_type": plan.get("Node Type")
                }

                # Check for Seq Scan on large tables
                self._analyze_plan_node(plan, suggestions)

            return {
                "plan_info": plan_info,
                "suggestions": suggestions
            }

        except Exception:
            return None

    def _analyze_plan_node(self, node: dict, suggestions: list, depth: int = 0) -> None:
        """Recursively analyze plan nodes for issues."""
        if not isinstance(node, dict):
            return

        node_type = node.get("Node Type", "")
        plan_rows = node.get("Plan Rows", 0)

        # Check for Seq Scan on large tables
        if node_type == "Seq Scan" and plan_rows > 10000:
            table = node.get("Relation Name", "unknown")
            filter_cond = node.get("Filter")
            suggestions.append({
                "pattern": "Sequential Scan (from EXPLAIN)",
                "severity": "medium",
                "issue": f"Sequential scan on '{table}' with estimated {plan_rows} rows",
                "impact": "Full table scan may be slow for large tables",
                "suggestion": f"Consider adding an index" + (f" for filter: {filter_cond}" if filter_cond else ""),
                "example_rewrite": None
            })

        # Recurse into child nodes
        for child in node.get("Plans", []):
            self._analyze_plan_node(child, suggestions, depth + 1)

    async def _analyze_tables_in_query(self, normalized: str) -> dict[str, Any] | None:
        """Analyze tables mentioned in the query."""
        try:
            # Extract table names (simplified - may not catch all cases)
            tables = set()
            for match in re.finditer(r'\bFROM\s+(\w+)', normalized):
                tables.add(match.group(1).lower())
            for match in re.finditer(r'\bJOIN\s+(\w+)', normalized):
                tables.add(match.group(1).lower())

            if not tables:
                return None

            # Use parameterized query with ANY(array) to prevent SQL injection
            query = """
                SELECT
                    c.relname as table_name,
                    pg_size_pretty(pg_table_size(c.oid)) as table_size,
                    s.n_live_tup as row_estimate,
                    (SELECT COUNT(*) FROM pg_indexes WHERE tablename = c.relname) as index_count
                FROM pg_class c
                JOIN pg_namespace n ON n.oid = c.relnamespace
                LEFT JOIN pg_stat_user_tables s ON s.relid = c.oid
                WHERE c.relname = ANY(%s)
                  AND n.nspname NOT IN ('pg_catalog', 'information_schema')
            """

            results = await self.sql_driver.execute_query(query, [list(tables)])
            return {"tables": results} if results else None

        except Exception:
            return None

    def _generate_summary(self, suggestions: list[dict[str, Any]]) -> dict[str, Any]:
        """Generate summary of suggestions."""
        severity_counts = {"critical": 0, "high": 0, "medium": 0, "low": 0, "info": 0}

        for s in suggestions:
            severity = s.get("severity", "info")
            severity_counts[severity] = severity_counts.get(severity, 0) + 1

        if severity_counts["critical"] > 0:
            overall = "critical"
            message = "Critical issues found that may cause incorrect results or severe performance problems"
        elif severity_counts["high"] > 0:
            overall = "needs_attention"
            message = "High-impact issues found that should be addressed for better performance"
        elif severity_counts["medium"] > 0:
            overall = "review_recommended"
            message = "Some optimization opportunities identified"
        elif severity_counts["low"] > 0 or severity_counts["info"] > 0:
            overall = "minor_issues"
            message = "Minor improvements possible but query is generally acceptable"
        else:
            overall = "good"
            message = "No significant issues detected"

        return {
            "overall_status": overall,
            "message": message,
            "severity_counts": severity_counts
        }
