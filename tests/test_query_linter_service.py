"""Unit tests for the query_linter service."""

import pytest

from pgtuner_mcp.services.query_linter import (
    QueryLinter,
    Severity,
    list_rules,
)


class TestRuleRegistry:
    def test_seven_rules_registered(self):
        rules = list_rules()
        assert {r.rule_id for r in rules} == {
            "select-star", "implicit-cast", "or-of-equals",
            "non-sargable-like", "unbounded-select",
            "not-in-nullable", "function-on-indexed-col",
        }

    def test_disabled_rules_via_env(self, monkeypatch):
        monkeypatch.setenv("PGTUNER_LINT_DISABLED_RULES", "select-star,unbounded-select")
        linter = QueryLinter()
        active_ids = {r.rule_id for r in linter.active_rules}
        assert "select-star" not in active_ids
        assert "unbounded-select" not in active_ids
        assert "implicit-cast" in active_ids


class TestRules:
    def _ids(self, findings):
        return {f.rule for f in findings}

    def test_select_star_positive(self):
        f = QueryLinter().lint("SELECT * FROM users")
        assert "select-star" in self._ids(f)

    def test_select_star_negative(self):
        f = QueryLinter().lint("SELECT id FROM users")
        assert "select-star" not in self._ids(f)

    def test_or_of_equals_positive(self):
        f = QueryLinter().lint("SELECT id FROM t WHERE col = 1 OR col = 2 OR col = 3")
        assert "or-of-equals" in self._ids(f)

    def test_or_of_equals_negative_different_cols(self):
        f = QueryLinter().lint("SELECT id FROM t WHERE a = 1 OR b = 2")
        assert "or-of-equals" not in self._ids(f)

    def test_non_sargable_like_positive(self):
        f = QueryLinter().lint("SELECT id FROM t WHERE name LIKE '%foo%'")
        assert "non-sargable-like" in self._ids(f)

    def test_non_sargable_like_negative_prefix(self):
        f = QueryLinter().lint("SELECT id FROM t WHERE name LIKE 'foo%'")
        assert "non-sargable-like" not in self._ids(f)

    def test_unbounded_select_positive(self):
        f = QueryLinter().lint("SELECT id, name FROM users WHERE active")
        assert "unbounded-select" in self._ids(f)

    def test_unbounded_select_negative_with_limit(self):
        f = QueryLinter().lint("SELECT id FROM users LIMIT 10")
        assert "unbounded-select" not in self._ids(f)

    def test_unbounded_select_negative_aggregate(self):
        f = QueryLinter().lint("SELECT count(*) FROM users")
        assert "unbounded-select" not in self._ids(f)

    def test_function_on_indexed_col_positive(self):
        f = QueryLinter().lint("SELECT id FROM users WHERE lower(email) = 'x@y.z'")
        assert "function-on-indexed-col" in self._ids(f)

    def test_implicit_cast_positive(self):
        f = QueryLinter().lint("SELECT id FROM users WHERE created_at = '2026-01-01'")
        assert "implicit-cast" in self._ids(f)

    def test_severity_threshold_filters(self):
        f = QueryLinter().lint("SELECT * FROM users", threshold=Severity.ERROR)
        assert "select-star" not in self._ids(f)

    def test_parse_error_returns_structured_finding(self):
        f = QueryLinter().lint("this is not sql at all !!!")
        assert any(x.rule == "parse-error" for x in f)


class TestRulesExtended:
    """Negative cases and AST-traversal coverage to reach the spec acceptance bar."""

    def _ids(self, findings):
        return {f.rule for f in findings}

    def test_implicit_cast_negative_numeric_literal(self):
        f = QueryLinter().lint("SELECT id FROM users WHERE created_at = 12345")
        assert "implicit-cast" not in self._ids(f)

    def test_function_on_indexed_col_negative_no_function(self):
        f = QueryLinter().lint("SELECT id FROM users WHERE email = 'x@y.z'")
        assert "function-on-indexed-col" not in self._ids(f)

    def test_not_in_nullable_positive(self):
        f = QueryLinter().lint(
            "SELECT id FROM users WHERE id NOT IN (SELECT user_id FROM orders)"
        )
        assert "not-in-nullable" in self._ids(f)

    def test_not_in_nullable_negative_not_exists(self):
        f = QueryLinter().lint(
            "SELECT id FROM users u WHERE NOT EXISTS (SELECT 1 FROM orders o WHERE o.user_id = u.id)"
        )
        assert "not-in-nullable" not in self._ids(f)

    def test_select_star_in_cte(self):
        f = QueryLinter().lint(
            "WITH x AS (SELECT * FROM users) SELECT id FROM x"
        )
        # AST visitor should descend into CTE body
        assert "select-star" in self._ids(f)

    def test_select_star_in_subquery(self):
        f = QueryLinter().lint(
            "SELECT id FROM (SELECT * FROM users) sub"
        )
        assert "select-star" in self._ids(f)

    def test_select_star_in_union(self):
        f = QueryLinter().lint(
            "SELECT id FROM users UNION ALL SELECT * FROM users"
        )
        assert "select-star" in self._ids(f)

    def test_severity_threshold_keeps_warning(self):
        f = QueryLinter().lint("SELECT * FROM users", threshold=Severity.WARNING)
        # select-star is WARNING - included by WARNING threshold
        assert "select-star" in self._ids(f)

    def test_disabled_via_explicit_arg(self):
        linter = QueryLinter(disabled={"select-star"})
        f = linter.lint("SELECT * FROM users")
        assert "select-star" not in {x.rule for x in f}

    def test_empty_sql_parse_error(self):
        f = QueryLinter().lint("")
        assert any(x.rule == "parse-error" for x in f)

    def test_or_of_equals_positive_column_on_right(self):
        """`1 = col OR 2 = col` is the same anti-pattern as col-on-left."""
        f = QueryLinter().lint("SELECT id FROM t WHERE 1 = col OR 2 = col OR 3 = col")
        assert "or-of-equals" in self._ids(f)

    def test_function_on_indexed_col_nested(self):
        """Nested function calls like lower(coalesce(col, '')) still defeat
        plain indexes and should be flagged."""
        f = QueryLinter().lint(
            "SELECT id FROM users WHERE lower(coalesce(email, '')) = 'x@y.z'"
        )
        assert "function-on-indexed-col" in self._ids(f)
