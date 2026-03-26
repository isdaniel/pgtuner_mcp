---
name: pg-index-optimization
description: Performs comprehensive PostgreSQL index optimization including unused index cleanup, duplicate detection, missing index recommendations, and hypothetical index testing with HypoPG. Provides actionable CREATE/DROP INDEX statements with estimated impact.
---

# PostgreSQL Index Optimization

This skill guides comprehensive index analysis and optimization for PostgreSQL databases using pgtuner-mcp tools, including HypoPG hypothetical index testing.

## When to Use This Skill

Use this skill when the user:

- Asks to optimize or review database indexes
- Wants to find and remove unused or duplicate indexes
- Reports slow queries that might benefit from better indexing
- Asks "what indexes should I create?" or "which indexes can I drop?"
- Mentions high storage usage from indexes or slow write performance
- Wants to test index ideas without creating them in production

## MCP Resources Available

- `pgtuner://docs/tools` -- Full tool reference documentation
- `pgtuner://table/{schema}/{table_name}/indexes` -- Quick view of existing indexes on a table (lightweight)
- `pgtuner://table/{schema}/{table_name}/stats` -- Table statistics for context
- `pgtuner://query/{query_hash}/stats` -- Per-query stats to understand which queries benefit from indexes

## Related MCP Prompt

This skill corresponds to the **`index_optimization`** MCP Prompt. If the user triggers that prompt, follow this skill's workflow.

## Prerequisites

- The pgtuner-mcp server must be connected with a valid `DATABASE_URI`
- Required: `pg_stat_statements` extension for workload analysis
- Recommended: `hypopg` extension for hypothetical index testing
- Recommended: `pgstattuple` extension for index bloat analysis

## Pre-Flight Checks

Before starting, check extension availability:
1. Use `manage_hypothetical_indexes` with `action: "check"` to verify HypoPG. If unavailable, skip Phase 3 (hypothetical testing) and rely on heuristic-based recommendations.
2. If `analyze_index_bloat` returns an error about `pgstattuple`, note that bloat measurements will be estimated rather than exact.

## Agent Decision Tree

```
User asks about indexes
  |
  +--> "Which indexes can I drop?"
  |    --> Start with Phase 1 (cleanup), focus on Step 1.1
  |
  +--> "What indexes should I create?"
  |    --> Start with Phase 2 (missing indexes)
  |
  +--> "Comprehensive index review"
  |    --> Follow all 4 phases in order
  |
  +--> "Test a specific index idea"
  |    --> Skip to Phase 3 (hypothetical testing)
  |
  +--> After finding unused indexes, also check:
       --> Read pgtuner://table/{schema}/{table}/indexes for quick context
       --> Cross-reference: are the "unused" indexes UNIQUE? (warn user)
       --> Cross-reference: how long has the DB been up?
```

## Optimization Workflow

### Phase 1: Index Inventory and Cleanup

#### Step 1.1: Find Unused Indexes

```
Tool: find_unused_indexes
Parameters:
  schema_name: "public"
  min_size_mb: 1
  include_duplicates: true
```

**What to analyze:**
- **Unused indexes** (idx_scan = 0): Safe to drop if the database has been running long enough for representative workload
- **Rarely used indexes** (idx_scan < 10 over weeks): Candidates for removal
- **Duplicate indexes**: Indexes that are a prefix of another index (e.g., `idx_a` on `(col1)` is redundant if `idx_b` on `(col1, col2)` exists)
- **Overlapping indexes**: Multiple indexes covering similar column combinations

**Before recommending DROP:**
1. Check how long the database has been up: `SELECT pg_postmaster_start_time()`
2. Check if `pg_stat_statements` was recently reset
3. Consider if the index supports a rarely-run but critical report query
4. Always warn about unique indexes (they enforce constraints, not just speed)

**Generate cleanup SQL:**
```sql
-- DROP INDEX CONCURRENTLY avoids blocking writes
DROP INDEX CONCURRENTLY IF EXISTS schema.index_name;
```

#### Step 1.2: Analyze Index Bloat

```
Tool: analyze_index_bloat
Parameters:
  schema_name: "public"
  min_index_size_gb: 0.1    # Lower threshold to catch more issues
  min_bloat_percent: 20
```

**What to analyze:**
- **avg_leaf_density < 70%**: Fragmented, consider REINDEX
- **Estimated bloat > 40%**: Significant wasted space
- B-tree indexes degrade over time with UPDATE/DELETE patterns

**Remediation:**
```sql
-- REINDEX CONCURRENTLY (PostgreSQL 12+) avoids blocking
REINDEX INDEX CONCURRENTLY schema.index_name;
```

### Phase 2: Missing Index Analysis

#### Step 2.1: Workload-Based Recommendations

```
Tool: get_index_recommendations
Parameters:
  max_recommendations: 10
  min_improvement_percent: 10
  include_hypothetical_testing: true
```

If the user has specific queries to optimize:
```
Tool: get_index_recommendations
Parameters:
  workload_queries: ["SELECT ... FROM orders WHERE ...", "SELECT ... FROM users JOIN ..."]
  max_recommendations: 10
  min_improvement_percent: 10
  include_hypothetical_testing: true
  target_tables: ["orders", "users"]   # Optional: focus on specific tables
```

**Evaluate each recommendation:**
- Improvement > 50%: Strong candidate
- Improvement 20-50%: Good candidate, verify with hypothetical testing
- Improvement 10-20%: Marginal, weigh against write overhead

#### Step 2.2: Check Table Access Patterns

For tables flagged in recommendations:

```
Tool: get_table_stats
Parameters:
  table_name: "<table_name>"
  include_indexes: true
  order_by: "seq_scans"
```

**What to analyze:**
- **High seq_scan count with low idx_scan**: Table needs better indexing
- **seq_tup_read >> idx_tup_fetch**: Most reads are full scans
- **Large table with only PK index**: Likely needs additional indexes

### Phase 3: Hypothetical Index Testing

#### Step 3.1: Test Recommended Indexes

For each promising recommendation, test with HypoPG:

```
Tool: explain_with_indexes
Parameters:
  query: "<query that will benefit>"
  hypothetical_indexes:
    - table: "orders"
      columns: ["customer_id", "created_at"]
      index_type: "btree"
      unique: false
    - table: "orders"
      columns: ["status"]
      index_type: "btree"
  analyze: false
```

**Evaluate the result:**
- Compare before/after costs
- Verify the plan actually uses the new index
- Check if Index Scan or Index Only Scan is chosen (Index Only Scan is better)

#### Step 3.2: Advanced HypoPG Testing

For more complex scenarios, start by checking HypoPG availability:

```
Tool: manage_hypothetical_indexes
Parameters:
  action: "check"
```

If available, test advanced index types:

```
Tool: manage_hypothetical_indexes
Parameters:
  action: "create"
  table: "orders"
  columns: ["customer_id", "created_at"]
  index_type: "btree"
  schema: "public"
  include: ["order_total"]   # Covering index for Index Only Scan
```

Estimate the storage cost of a hypothetical index:
```
Tool: manage_hypothetical_indexes
Parameters:
  action: "estimate_size"
  table: "orders"
  columns: ["customer_id", "created_at"]
  index_type: "btree"
```

Test partial indexes (index only matching rows):
```
Tool: manage_hypothetical_indexes
Parameters:
  action: "create"
  table: "orders"
  columns: ["created_at"]
  index_type: "btree"
  where: "status = 'pending'"   # Only index pending orders
```

Test what happens if you hide an existing index:
```
Tool: manage_hypothetical_indexes
Parameters:
  action: "hide"
  index_id: <real_index_oid>
```

Verify which indexes are currently hidden:
```
Tool: manage_hypothetical_indexes
Parameters:
  action: "list_hidden"
```

List all currently created hypothetical indexes:
```
Tool: manage_hypothetical_indexes
Parameters:
  action: "list"
```

Then explain the query to see if performance changes:
```
Tool: manage_hypothetical_indexes
Parameters:
  action: "explain_with_index"
  query: "SELECT ... FROM orders WHERE ..."
  table: "orders"
  columns: ["customer_id", "created_at"]
```

Restore hidden indexes and clean up when done:
```
Tool: manage_hypothetical_indexes
Parameters:
  action: "reset"
```

### Phase 4: Generate Action Plan

Compile all findings into a prioritized action plan.

## Output Format

### Index Cleanup (Safe to Drop)

| Index | Table | Size | Reason | DROP Statement |
|-------|-------|------|--------|----------------|
| idx_old_status | orders | 245 MB | 0 scans in 30 days | `DROP INDEX CONCURRENTLY idx_old_status;` |

### Indexes to REINDEX (Bloated)

| Index | Table | Size | Bloat % | REINDEX Statement |
|-------|-------|------|---------|-------------------|
| idx_orders_date | orders | 1.2 GB | 45% | `REINDEX INDEX CONCURRENTLY idx_orders_date;` |

### New Indexes to Create

| Table | Columns | Type | Improvement | CREATE Statement |
|-------|---------|------|-------------|------------------|
| orders | (customer_id, created_at) | btree | 72% | `CREATE INDEX CONCURRENTLY idx_orders_cust_date ON orders (customer_id, created_at);` |

### Net Impact Summary

- Estimated storage savings from drops: X MB
- Estimated storage cost of new indexes: Y MB
- Net storage change: +/- Z MB
- Estimated query improvement: summarize

## Index Design Guidelines

Present these guidelines when making recommendations:

### Column Order in Composite Indexes
1. Equality columns first (WHERE col = value)
2. Range columns next (WHERE col BETWEEN / > / <)
3. Sort columns last (ORDER BY col)

### Index Type Selection
| Use Case | Index Type |
|----------|-----------|
| Equality and range queries (default) | btree |
| Equality-only lookups | hash |
| Full-text search | gin |
| Geometric / range data types | gist |
| Very large tables, time-series | brin |

### When NOT to Add an Index
- Table has < 1,000 rows (seq scan is faster)
- Column has very low cardinality (e.g., boolean) unless combined in composite
- Table is write-heavy with rare reads
- Query already runs under 1ms

## Important Notes

- Always use `CONCURRENTLY` for CREATE/DROP/REINDEX in production to avoid blocking
- HypoPG indexes exist only in the current session and are not persisted
- After creating real indexes, run `ANALYZE table_name` to update statistics
- Monitor write performance after adding indexes: check `pg_stat_user_tables.n_tup_ins/upd/del`
- Wait for representative workload before dropping "unused" indexes (minimum 1-2 weeks of normal traffic)
- Before dropping any index, capture the CREATE INDEX statement for rollback

## Iterative Verification

After applying index changes:

1. **After CREATE INDEX**: Re-run `analyze_query` on affected queries to confirm the plan uses the new index.
2. **After DROP INDEX**: Re-run `analyze_query` to ensure no critical query regressed. Monitor application error rates.
3. **After REINDEX**: Re-run `analyze_index_bloat` to confirm bloat was resolved.
4. **After all changes**: Re-run `find_unused_indexes` to confirm the new state is clean.

## When to Stop and Ask the User

- **Before dropping a UNIQUE index**: "This index enforces a uniqueness constraint. Dropping it will remove the constraint. Are you sure?"
- **Before dropping indexes on a table with unclear workload**: "The database has only been running for X days. Some indexes may serve monthly reporting queries. Should we wait for a full workload cycle?"
- **If HypoPG is not available**: "The HypoPG extension is not installed. I can provide heuristic-based recommendations, but cannot verify improvement with hypothetical testing. Would you like to proceed or install HypoPG first?"
- **If estimated storage for new indexes is significant**: "The recommended indexes will use approximately X GB of storage. Is this acceptable?"

## Cross-References to Other Skills

- **Slow query driving the index need**: Use `pg-slow-query-diagnosis` skill
- **Query rewrite might be better than an index**: Use `pg-query-rewrite` skill
- **Index bloat requiring deeper investigation**: Use `pg-bloat-analysis` skill
- **I/O impact of indexes**: Use `pg-io-deep-dive` skill

## PostgreSQL Version Notes

| Feature | Version |
|---------|---------|
| `INCLUDE` columns in indexes (covering indexes) | PG11+ |
| `REINDEX CONCURRENTLY` | PG12+ |
| Deduplication for B-tree indexes | PG13+ |
| `pg_stat_progress_create_index` | PG12+ |

## Production Safety

- All diagnostic tools are read-only. No indexes are created or dropped by the tools.
- HypoPG indexes are session-scoped and automatically cleaned up.
- `manage_hypothetical_indexes` with `action: "hide"` only hides the index from the planner in the current session. It does not affect other sessions or actual data.
- All CREATE/DROP/REINDEX statements are recommendations for the user to execute.
