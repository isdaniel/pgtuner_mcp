"""Static query anti-pattern linter using pglast AST."""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from enum import Enum
from typing import Any

try:
    from pglast import ast as _pgast
    from pglast import parse_sql
    PGLAST_AVAILABLE = True
except ImportError:  # pragma: no cover
    PGLAST_AVAILABLE = False
    _pgast = None  # type: ignore


class Severity(str, Enum):
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"


_SEVERITY_RANK = {Severity.INFO: 0, Severity.WARNING: 1, Severity.ERROR: 2}


@dataclass
class Finding:
    rule: str
    severity: Severity
    message: str
    location: dict[str, int] = field(default_factory=dict)
    suggestion: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "rule": self.rule,
            "severity": self.severity.value,
            "message": self.message,
            "location": self.location,
            "suggestion": self.suggestion,
        }


class Rule:
    rule_id: str = ""
    severity: Severity = Severity.INFO

    def check(self, root: Any, sql: str) -> list[Finding]:
        raise NotImplementedError


def _is_ast_node(obj: Any) -> bool:
    return _pgast is not None and isinstance(obj, _pgast.Node)


def _walk(node: Any):
    """Yield every AST node under `node`."""
    if node is None:
        return
    if isinstance(node, (list, tuple)):
        for item in node:
            yield from _walk(item)
        return
    if not _is_ast_node(node):
        return
    yield node
    slots = getattr(type(node), "__slots__", None) or ()
    for s in slots:
        try:
            v = getattr(node, s, None)
        except AttributeError:
            continue
        if v is None or isinstance(v, (bool, int, str, float)):
            continue
        yield from _walk(v)


def _col_name(colref: Any) -> str:
    """Extract a dotted column name from a ColumnRef node."""
    fields = getattr(colref, "fields", None) or ()
    parts = []
    for f in fields:
        if type(f).__name__ == "String":
            sval = getattr(f, "sval", None) or getattr(f, "str", None)
            if sval:
                parts.append(sval)
    return ".".join(parts)


class SelectStarRule(Rule):
    rule_id = "select-star"
    severity = Severity.WARNING

    def check(self, root: Any, sql: str) -> list[Finding]:
        for n in _walk(root):
            if type(n).__name__ != "ResTarget":
                continue
            val = getattr(n, "val", None)
            if val is not None and type(val).__name__ == "ColumnRef":
                fields = getattr(val, "fields", None) or ()
                if any(type(f).__name__ == "A_Star" for f in fields):
                    return [Finding(
                        self.rule_id, self.severity,
                        "SELECT * - list explicit columns to avoid over-fetching",
                        suggestion="Replace * with the columns actually needed",
                    )]
        return []


class ImplicitCastRule(Rule):
    rule_id = "implicit-cast"
    # INFO: without column-type info we cannot tell text-vs-text (safe) from
    # timestamp-vs-text (suspect). Users must opt in via severity_threshold=info.
    severity = Severity.INFO

    # LIKE/ILIKE are text-only operators; flagging them as implicit-cast is
    # always wrong and overlaps with NonSargableLikeRule.
    _LIKE_OPS = frozenset({
        "~~", "LIKE", "~~*", "ILIKE",
        "!~~", "NOT LIKE", "!~~*", "NOT ILIKE",
    })

    def check(self, root: Any, sql: str) -> list[Finding]:
        for n in _walk(root):
            if type(n).__name__ != "A_Expr":
                continue
            op_nodes = getattr(n, "name", None) or ()
            op_name = "".join(
                (getattr(x, "sval", "") or "") for x in op_nodes
            ).upper()
            if op_name in self._LIKE_OPS:
                continue
            lexpr, rexpr = getattr(n, "lexpr", None), getattr(n, "rexpr", None)
            for left, right in ((lexpr, rexpr), (rexpr, lexpr)):
                if (left is not None and type(left).__name__ == "ColumnRef"
                        and right is not None and type(right).__name__ == "A_Const"):
                    val = getattr(right, "val", None)
                    if val is not None and type(val).__name__ == "String":
                        col = _col_name(left)
                        if col:
                            return [Finding(
                                self.rule_id, self.severity,
                                f"Column '{col}' compared to text literal - verify "
                                "the column type to avoid implicit cast disabling index use",
                                suggestion=f"Cast the literal explicitly, e.g. "
                                           f"WHERE {col} = '...'::<col_type>",
                            )]
        return []


class OrOfEqualsRule(Rule):
    rule_id = "or-of-equals"
    severity = Severity.INFO

    def check(self, root: Any, sql: str) -> list[Finding]:
        for n in _walk(root):
            if type(n).__name__ != "BoolExpr":
                continue
            boolop = getattr(n, "boolop", None)
            boolop_name = getattr(boolop, "name", str(boolop))
            if "OR" not in str(boolop_name).upper():
                continue
            args = getattr(n, "args", None) or ()
            eq_cols: list[str] = []
            all_args_match = True
            for a in args:
                if type(a).__name__ != "A_Expr":
                    all_args_match = False
                    continue
                # Only consider equality
                name = getattr(a, "name", None) or ()
                op_names = [getattr(x, "sval", "") or "" for x in name]
                if "=" not in op_names:
                    all_args_match = False
                    continue
                lexpr = getattr(a, "lexpr", None)
                rexpr = getattr(a, "rexpr", None)
                if lexpr is not None and type(lexpr).__name__ == "ColumnRef":
                    eq_cols.append(_col_name(lexpr))
                elif rexpr is not None and type(rexpr).__name__ == "ColumnRef":
                    eq_cols.append(_col_name(rexpr))
                else:
                    all_args_match = False
            # Only suggest IN(...) when EVERY arg in the OR-chain is `col = const`
            # on the same column — otherwise the rewrite would silently drop the
            # other predicates.
            if (all_args_match and len(eq_cols) >= 2
                    and len(set(eq_cols)) == 1 and eq_cols[0]):
                return [Finding(
                    self.rule_id, self.severity,
                    f"OR-chain on '{eq_cols[0]}' could be rewritten as IN(...)",
                    suggestion=f"WHERE {eq_cols[0]} IN (v1, v2, ...)",
                )]
        return []


class NonSargableLikeRule(Rule):
    rule_id = "non-sargable-like"
    severity = Severity.WARNING

    def check(self, root: Any, sql: str) -> list[Finding]:
        for n in _walk(root):
            if type(n).__name__ != "A_Expr":
                continue
            name = getattr(n, "name", None) or ()
            op_names = [(getattr(x, "sval", "") or "").upper() for x in name]
            if not any(op in ("~~", "LIKE", "~~*", "ILIKE") for op in op_names):
                continue
            rexpr = getattr(n, "rexpr", None)
            if rexpr is None or type(rexpr).__name__ != "A_Const":
                continue
            val = getattr(rexpr, "val", None)
            if val is not None and type(val).__name__ == "String":
                pat = getattr(val, "sval", "") or ""
                if pat.startswith("%"):
                    return [Finding(
                        self.rule_id, self.severity,
                        f"LIKE pattern '{pat}' leads with wildcard - cannot use a B-tree index",
                        suggestion="Use a trigram (pg_trgm) index or full-text search",
                    )]
        return []


class UnboundedSelectRule(Rule):
    rule_id = "unbounded-select"
    severity = Severity.INFO

    def check(self, root: Any, sql: str) -> list[Finding]:
        try:
            stmt = root[0].stmt
        except (IndexError, AttributeError):
            return []
        if type(stmt).__name__ != "SelectStmt":
            return []
        if getattr(stmt, "limitCount", None) is not None:
            return []
        if getattr(stmt, "groupClause", None):
            return []
        targets = getattr(stmt, "targetList", None) or ()
        for t in targets:
            val = getattr(t, "val", None)
            if val is not None and type(val).__name__ == "FuncCall":
                # Treat any top-level FuncCall as an aggregate-style result
                return []
        return [Finding(
            self.rule_id, self.severity,
            "Top-level SELECT has no LIMIT and is not an aggregate - may return unbounded rows",
            suggestion="Add LIMIT N if appropriate for this caller",
        )]


class NotInNullableRule(Rule):
    rule_id = "not-in-nullable"
    severity = Severity.ERROR

    def check(self, root: Any, sql: str) -> list[Finding]:
        # NOT IN (subquery) parses as BoolExpr(NOT) wrapping SubLink(ANY_SUBLINK).
        # Detect that pattern.
        for n in _walk(root):
            if type(n).__name__ != "BoolExpr":
                continue
            boolop = getattr(n, "boolop", None)
            if "NOT" not in str(getattr(boolop, "name", str(boolop))).upper():
                continue
            args = getattr(n, "args", None) or ()
            for a in args:
                if type(a).__name__ != "SubLink":
                    continue
                sublink_type = getattr(a, "subLinkType", None)
                sl_name = str(getattr(sublink_type, "name", str(sublink_type))).upper()
                if "ANY_SUBLINK" in sl_name:
                    return [Finding(
                        self.rule_id, self.severity,
                        "NOT IN (subquery) - if the subquery yields NULL, the entire "
                        "result is NULL (correctness bug)",
                        suggestion="Rewrite as NOT EXISTS (subquery) or add IS NOT NULL filter",
                    )]
        return []


class FunctionOnIndexedColRule(Rule):
    rule_id = "function-on-indexed-col"
    severity = Severity.WARNING

    def check(self, root: Any, sql: str) -> list[Finding]:
        for n in _walk(root):
            if type(n).__name__ != "A_Expr":
                continue
            for side in (getattr(n, "lexpr", None), getattr(n, "rexpr", None)):
                if side is None or type(side).__name__ != "FuncCall":
                    continue
                # Walk the function-call subtree so nested forms like
                # lower(coalesce(col, '')) are also flagged.
                if any(type(sub).__name__ == "ColumnRef" for sub in _walk(side)):
                    fname_parts = [
                        getattr(f, "sval", "") or ""
                        for f in (getattr(side, "funcname", None) or ())
                    ]
                    fname = ".".join(p for p in fname_parts if p)
                    return [Finding(
                        self.rule_id, self.severity,
                        f"Function '{fname}()' applied to a column in WHERE - "
                        "defeats plain index unless an expression index exists",
                        suggestion=(
                            "Create an expression index matching the WHERE-clause "
                            "expression (e.g., CREATE INDEX ON tbl (expr);)"
                        ),
                    )]
        return []


_RULES: list[Rule] = [
    SelectStarRule(), ImplicitCastRule(), OrOfEqualsRule(),
    NonSargableLikeRule(), UnboundedSelectRule(),
    NotInNullableRule(), FunctionOnIndexedColRule(),
]


def list_rules() -> list[Rule]:
    return list(_RULES)


class QueryLinter:
    def __init__(self, disabled: set[str] | None = None):
        if disabled is None:
            env_val = os.environ.get("PGTUNER_LINT_DISABLED_RULES", "")
            disabled = {x.strip() for x in env_val.split(",") if x.strip()}
        self.active_rules = [r for r in _RULES if r.rule_id not in disabled]

    def lint(self, sql: str, threshold: Severity = Severity.INFO) -> list[Finding]:
        if not PGLAST_AVAILABLE:
            return [Finding("linter-unavailable", Severity.ERROR,
                            "pglast not installed", {})]
        if not sql or not sql.strip():
            return [Finding("parse-error", Severity.ERROR,
                            "Could not parse SQL: empty input", {})]
        try:
            tree = parse_sql(sql)
        except Exception as e:
            return [Finding("parse-error", Severity.ERROR,
                            f"Could not parse SQL: {e}", {})]
        if not tree:
            return [Finding("parse-error", Severity.ERROR,
                            "Could not parse SQL: no statements found", {})]
        findings: list[Finding] = []
        for rule in self.active_rules:
            try:
                findings.extend(rule.check(tree, sql))
            except Exception:
                continue
        min_rank = _SEVERITY_RANK[threshold]
        return [f for f in findings if _SEVERITY_RANK[f.severity] >= min_rank]
