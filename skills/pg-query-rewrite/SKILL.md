---
name: pg-query-rewrite
description: Guides SQL query optimization through execution plan analysis and common rewrite patterns. Covers subquery-to-JOIN conversion, CTE materialization control, OR-to-UNION, NOT IN-to-NOT EXISTS, pagination optimization, and SELECT * elimination. Uses analyze_query with format and settings parameters for deep plan analysis.
---

# PostgreSQL Query Rewrite Patterns

This skill guides the agent through analyzing SQL queries and applying common restructuring patterns to improve performance, using pgtuner-mcp execution plan analysis tools.

## When to Use This Skill

Use this skill when the user:

- Has a specific slow query and wants it rewritten for better performance
- Asks "how can I make this query faster?"
- Reports that adding indexes didn't help (query structure is the issue)
- Has queries with subqueries, CTEs, OR conditions, or NOT IN that could be improved
- Asks about query optimization patterns or best practices
- Has queries with `SELECT *` or inefficient pagination

## MCP Resources Available

- `pgtuner://docs/tools` -- Reference for `analyze_query` parameters and options
- `pgtuner://query/{query_hash}/stats` -- Get statistics for a specific query by its `queryid`
- `pgtuner://table/{schema}/{table_name}/indexes` -- Check available indexes on involved tables

## Related MCP Prompt

This skill is closely related to the **`query_tuning`** MCP Prompt, which provides a structured query optimization workflow.

## Prerequisites

- The pgtuner-mcp server must be connected with a valid `DATABASE_URI`
- Required: `pg_stat_statements` extension (for identifying slow queries to rewrite)
- The user should provide the specific SQL query to optimize

## Agent Decision Tree

```
Start: User provides a slow query
  |
  +--> Step 1: Analyze the execution plan
  |
  +--> Look at the plan for these patterns:
       |
       +--> SubPlan / Correlated Subquery?
       |    --> Apply Pattern 1: Subquery to JOIN
       |
       +--> CTE Scan with many rows materialized?
       |    --> Apply Pattern 2: CTE Materialization Control
       |
       +--> BitmapOr / multiple OR conditions?
       |    --> Apply Pattern 3: OR to UNION ALL
       |
       +--> NOT IN with possible NULLs?
       |    --> Apply Pattern 4: NOT IN to NOT EXISTS
       |
       +--> Seq Scan on query with OFFSET + LIMIT?
       |    --> Apply Pattern 5: Keyset Pagination
       |
       +--> Many columns fetched but few used?
       |    --> Apply Pattern 6: Eliminate SELECT *
       |
       +--> Sort on non-indexed column?
       |    --> Recommend index (redirect to pg-index-optimization)
       |
       +--> No obvious rewrite opportunity?
            --> Check statistics freshness (ANALYZE table)
            --> Check index recommendations
            --> Consider configuration tuning (work_mem, etc.)
```

## Query Rewrite Workflow

### Step 1: Analyze the Original Query

First, get the execution plan in text format for readability:

```
Tool: analyze_query
Parameters:
  query: "<the user's slow query>"
  analyze: true
  buffers: true
  settings: true
  format: "text"
```

**Why `settings: true`:** Reveals if session-level settings (e.g., `SET work_mem`) affect the plan. Important when the same query behaves differently in different contexts.

**Why `format: "text"`:** The text format shows the indented plan tree that is easiest to reason about visually. For programmatic analysis, also run with `format: "json"` if needed.

**Key plan nodes to identify:**

| Plan Node | Indicates | Potential Rewrite |
|-----------|-----------|-------------------|
| `SubPlan` | Correlated subquery (runs per row) | Convert to JOIN |
| `CTE Scan` | CTE materialized to temp storage | Add `NOT MATERIALIZED` (PG12+) |
| `BitmapOr` | Multiple OR conditions | UNION ALL |
| `Nested Loop` with high loops | Cross-join-like behavior | Review join conditions |
| `Sort` (external merge) | Disk-based sort | Index or increase work_mem |
| `Seq Scan` on large table | Missing index or SELECT * | Add index or limit columns |
| `Hash Join` (many batches) | Hash spilling to disk | Increase work_mem |

### Step 2: Check Table Context

For tables involved in the query, check their indexes and statistics:

Read resource: `pgtuner://table/{schema}/{table_name}/indexes`

```
Tool: get_table_stats
Parameters:
  table_name: "<table_name>"
  schema_name: "public"
  include_indexes: true
```

This reveals:
- Available indexes that the query optimizer could use
- Whether statistics are stale (`last_analyze` timestamp)
- Table size (affects plan choices)

### Step 3: Apply Rewrite Patterns

Based on the execution plan analysis, apply the appropriate patterns below.

---

## Rewrite Patterns

### Pattern 1: Correlated Subquery to JOIN

**Symptom in plan:** `SubPlan` node executing once per row of the outer query.

**Before (slow):**
```sql
SELECT o.id, o.total,
  (SELECT c.name FROM customers c WHERE c.id = o.customer_id)
FROM orders o
WHERE o.created_at > '2024-01-01';
```

**After (fast):**
```sql
SELECT o.id, o.total, c.name
FROM orders o
LEFT JOIN customers c ON c.id = o.customer_id
WHERE o.created_at > '2024-01-01';
```

**Why:** The subquery executes once per order row. The JOIN lets PostgreSQL choose the optimal join strategy (hash join, merge join) based on data size.

**Verify:** Re-run `analyze_query` on the rewritten query. The `SubPlan` node should be gone.

### Pattern 2: CTE Materialization Control (PG12+)

**Symptom in plan:** `CTE Scan` materializing many rows into temp storage, then filtering.

**Before (materializes everything):**
```sql
WITH recent_orders AS (
  SELECT * FROM orders WHERE created_at > '2024-01-01'
)
SELECT * FROM recent_orders WHERE customer_id = 42;
```

**After (allows predicate pushdown):**
```sql
WITH recent_orders AS NOT MATERIALIZED (
  SELECT * FROM orders WHERE created_at > '2024-01-01'
)
SELECT * FROM recent_orders WHERE customer_id = 42;
```

**Or simply inline the CTE:**
```sql
SELECT * FROM orders
WHERE created_at > '2024-01-01' AND customer_id = 42;
```

**Why:** In PG11 and earlier, CTEs are always materialized (an optimization fence). In PG12+, non-recursive CTEs referenced once are inlined by default, but explicit `MATERIALIZED` / `NOT MATERIALIZED` gives control.

**When to KEEP materialization:**
- The CTE is referenced multiple times (avoids re-execution)
- The CTE acts as an intentional optimization fence (rare but valid)

### Pattern 3: OR Conditions to UNION ALL

**Symptom in plan:** `BitmapOr` with multiple `BitmapAnd` children, or sequential scan when OR prevents index use.

**Before (can't use single index efficiently):**
```sql
SELECT * FROM events
WHERE user_id = 42 OR event_type = 'critical';
```

**After (each branch uses its own index):**
```sql
SELECT * FROM events WHERE user_id = 42
UNION ALL
SELECT * FROM events WHERE event_type = 'critical' AND user_id != 42;
```

**Why:** PostgreSQL can use a different index for each UNION branch. The `AND user_id != 42` in the second branch prevents duplicates (alternative: use `UNION` but it adds a sort/dedup step).

**When NOT to apply:**
- If `BitmapOr` is already efficient (small result sets from each condition)
- If the OR conditions share the same indexed column

### Pattern 4: NOT IN to NOT EXISTS

**Symptom in plan:** Anti-join not chosen, or unexpected behavior with NULLs.

**Before (NULL-unsafe, can return wrong results):**
```sql
SELECT * FROM customers
WHERE id NOT IN (SELECT customer_id FROM blacklist);
```

**After (NULL-safe, often faster):**
```sql
SELECT * FROM customers c
WHERE NOT EXISTS (
  SELECT 1 FROM blacklist b WHERE b.customer_id = c.id
);
```

**Why:**
- If `blacklist.customer_id` contains any NULL, `NOT IN` returns no rows at all (SQL three-valued logic)
- `NOT EXISTS` handles NULLs correctly
- PostgreSQL can use an Anti Join for `NOT EXISTS`, which is typically faster

**Alternative (also good):**
```sql
SELECT c.* FROM customers c
LEFT JOIN blacklist b ON b.customer_id = c.id
WHERE b.customer_id IS NULL;
```

### Pattern 5: OFFSET/LIMIT to Keyset Pagination

**Symptom in plan:** `Sort` + `Limit` with large OFFSET (e.g., `OFFSET 10000`). PostgreSQL must fetch and discard all rows up to the offset.

**Before (slow at high offsets):**
```sql
SELECT * FROM events
ORDER BY created_at DESC
LIMIT 20 OFFSET 10000;
```

**After (constant time regardless of page depth):**
```sql
SELECT * FROM events
WHERE created_at < '2024-06-15T10:30:00'  -- last value from previous page
ORDER BY created_at DESC
LIMIT 20;
```

**Why:** Keyset pagination uses an index range scan starting from the last seen value. Performance is O(1) per page instead of O(N) where N is the offset.

**Requirements:**
- An index on the ORDER BY column(s)
- The ordering must be deterministic (add PK as tiebreaker if needed)
- The application must track the last value from the previous page

### Pattern 6: Eliminate SELECT * and Over-Fetching

**Symptom in plan:** Large `width` in plan nodes, or `Seq Scan` when an `Index Only Scan` could be used.

**Before:**
```sql
SELECT * FROM orders WHERE customer_id = 42;
-- Fetches all 25 columns including large text/json fields
```

**After:**
```sql
SELECT id, order_date, total, status FROM orders WHERE customer_id = 42;
-- Fetches only the 4 needed columns
```

**Why:**
- Reduces I/O (especially for wide tables with TOAST columns)
- Enables `Index Only Scan` if all requested columns are in the index
- Reduces network transfer to the application

**To enable Index Only Scan, create a covering index:**
```sql
CREATE INDEX idx_orders_cust_covering
ON orders (customer_id) INCLUDE (id, order_date, total, status);
```

Then verify:
```
Tool: explain_with_indexes
Parameters:
  query: "SELECT id, order_date, total, status FROM orders WHERE customer_id = 42"
  hypothetical_indexes:
    - table: "orders"
      columns: ["customer_id"]
      index_type: "btree"
  analyze: false
```

## Step 4: Verify the Rewrite

After applying any rewrite pattern, ALWAYS verify improvement:

```
Tool: analyze_query
Parameters:
  query: "<rewritten query>"
  analyze: true
  buffers: true
  format: "text"
```

**Compare before vs after:**
- Execution time (ms)
- Buffer hits vs reads
- Plan structure changes (SubPlan gone? Index scan instead of Seq scan?)
- Rows estimates vs actuals (did the rewrite help the planner?)

IF the rewrite did NOT improve performance:
- The original plan may already be optimal for the data distribution
- Check if `ANALYZE <table>` is needed (stale statistics)
- Consider the `pg-index-optimization` skill instead
- Consider the `pg-config-tuning` skill for `work_mem` or planner settings

## When to Stop and Ask the User

- **If the query involves business logic**: "This query uses a correlated subquery that may have been intentional for correctness. Would you like me to verify that the rewritten version produces identical results?"
- **If the rewrite changes semantics (NOT IN vs NOT EXISTS with NULLs)**: "The NOT IN may behave differently if the subquery contains NULLs. Do you want me to explain the difference?"
- **If no rewrite helps**: "The query structure appears optimal. The performance issue may be due to missing indexes, stale statistics, or configuration. Should I investigate those areas?"
- **If the query is auto-generated (ORM)**: "This appears to be ORM-generated SQL. Optimizing it may require changes to the ORM query builder or using raw SQL for this specific case."

## PostgreSQL Version Notes

| Feature | Version |
|---------|---------|
| CTE inlining (automatic) | PG12+ (non-recursive, single-reference CTEs) |
| `NOT MATERIALIZED` / `MATERIALIZED` hints | PG12+ |
| `INCLUDE` columns in indexes | PG11+ |
| Parallel query for subqueries | PG12+ (improved in PG14+) |
| Memoize node for nested loops | PG14+ |
| Incremental sort | PG13+ |

## Production Safety

- `analyze_query` with `analyze: true` executes the query. For INSERT/UPDATE/DELETE, the tool wraps in `BEGIN READ ONLY` / `ROLLBACK` for safety, but always use caution.
- Query rewrites should be tested against a representative dataset before deploying to production.
- Verify that the rewritten query returns identical results, especially for NULLs, duplicates, and edge cases.
- `explain_with_indexes` with hypothetical indexes is safe (HypoPG indexes are session-only and never persisted).
