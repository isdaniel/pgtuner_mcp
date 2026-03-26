---
name: pg-slow-query-diagnosis
description: Diagnoses slow PostgreSQL queries by analyzing pg_stat_statements, execution plans, index opportunities, and table statistics. Guides the agent through a systematic investigation workflow using pgtuner-mcp tools to produce actionable optimization recommendations.
---

# PostgreSQL Slow Query Diagnosis

This skill guides systematic diagnosis and optimization of slow queries in PostgreSQL databases using the pgtuner-mcp server tools.

## When to Use This Skill

Use this skill when the user:

- Reports slow queries or poor query performance
- Asks "why is my query slow?" or "how can I speed up this query?"
- Wants to find and fix the top resource-consuming queries
- Mentions high query latency, timeouts, or slow API responses
- Asks to investigate `pg_stat_statements` data

## MCP Resources Available

Before calling tools, the agent can read lightweight resources for quick context:

- `pgtuner://docs/tools` -- Full tool reference documentation
- `pgtuner://docs/workflows` -- Recommended workflow patterns
- `pgtuner://query/{query_hash}/stats` -- Detailed stats for a specific query by `queryid` (from `get_slow_queries` output)
- `pgtuner://table/{schema}/{table_name}/stats` -- Quick table statistics without running the full tool
- `pgtuner://table/{schema}/{table_name}/indexes` -- Indexes on a specific table

## Related MCP Prompt

This skill corresponds to the **`diagnose_slow_queries`** MCP Prompt and partially to **`query_tuning`**. If the user triggers either prompt, follow this skill's workflow.

## Prerequisites

- The pgtuner-mcp server must be connected with a valid `DATABASE_URI`
- Required: `pg_stat_statements` extension (check availability early -- if missing, tool will return an error)
- Optional: `hypopg` extension for hypothetical index testing
- Optional: `pglast` library on the server for SQL parsing in index recommendations

## Pre-Flight Checks

Before starting the workflow, verify extension availability:
- If `get_slow_queries` returns an error about `pg_stat_statements`, inform the user it must be enabled and provide setup instructions.
- If the user wants hypothetical index testing, use `manage_hypothetical_indexes` with `action: "check"` to verify HypoPG is available. If not, skip Step 4 and rely on heuristic recommendations.

## Agent Decision Tree

```
User reports slow queries
  |
  +--> User has a SPECIFIC query?
  |    YES --> Skip Step 1, go directly to Step 2 (analyze_query)
  |    NO  --> Start with Step 1 (get_slow_queries)
  |
  +--> Step 1 returns 0 results?
  |    --> Check if pg_stat_statements was recently reset
  |    --> ASK user: "pg_stat_statements has limited data.
  |        Can you provide a specific query, or should we wait
  |        for more workload data to accumulate?"
  |
  +--> Step 2 shows temp_blks_written > 0?
  |    YES --> work_mem issue. Go to Step 6 (I/O) before indexes.
  |    NO  --> Continue to Step 3 (index recommendations)
  |
  +--> Step 3 shows < 10% improvement from indexes?
  |    YES --> Query structure may be the issue.
  |            Redirect to pg-query-rewrite skill.
  |    NO  --> Continue to Step 4 (verify with HypoPG)
  |
  +--> Step 2 shows row estimate mismatch > 10x?
       YES --> Run ANALYZE on the table FIRST, then re-check
       NO  --> Continue with index recommendations
```

## Diagnosis Workflow

Follow these steps in order. At each step, analyze the output before proceeding.

### Step 1: Identify the Slowest Queries

Call the `get_slow_queries` tool to retrieve the most expensive queries:

```
Tool: get_slow_queries
Parameters:
  limit: 10           # Start with top 10
  min_calls: 5        # Focus on queries called frequently enough to matter
  order_by: mean_time # Sort by average execution time
```

Also check for over-fetching queries (returning too many rows):
```
Tool: get_slow_queries
Parameters:
  limit: 10
  min_calls: 5
  order_by: rows       # Find queries returning the most rows
```

**What to look for:**
- Queries with high `mean_time` (> 100ms warrants investigation)
- Queries with high `total_time` (high total even if individual calls are fast)
- High `rows` returned vs actual need (over-fetching -- use `order_by: rows`)
- Queries with low `shared_blks_hit / (shared_blks_hit + shared_blks_read)` cache hit ratio
- Queries with `temp_blks_written > 0` (spilling to disk -- `work_mem` issue)

**Decision point:** If no slow queries are found, check if `pg_stat_statements` has been recently reset. Suggest the user wait for workload data to accumulate, or ask them to provide a specific query.

**Cross-reference with resources:** For any interesting `queryid` found, read `pgtuner://query/{queryid}/stats` for deeper per-query statistics without re-running the tool.

### Step 2: Analyze Execution Plans

For each problematic query identified in Step 1, run EXPLAIN ANALYZE:

```
Tool: analyze_query
Parameters:
  query: "<the slow query>"
  analyze: true     # Execute the query to get actual timing
  buffers: true     # Show buffer usage (I/O information)
  settings: true    # Show GUC settings that affect the plan
  verbose: false    # Keep output readable
  format: "text"    # Human-readable indented plan
```

The `settings: true` parameter reveals if session-level settings (e.g., modified `work_mem`) affect the plan. The `format: "text"` produces the classic readable plan; use `format: "json"` for structured programmatic analysis when needed.

**What to look for in the execution plan:**
- **Sequential scans on large tables** (> 10,000 rows): Indicates missing indexes
- **Row estimate mismatches**: When `actual rows` differs from `estimated rows` by 10x or more, statistics are stale (run `ANALYZE`)
- **Nested Loop with high iterations**: Consider if a Hash Join or Merge Join would be better
- **Sort operations using disk** (`external merge`): Increase `work_mem` or add an index for the sort key
- **Hash Batches > 1**: Hash table spilling to disk, consider increasing `work_mem`
- **Bitmap Heap Scan with many recheck cond rows**: The index is not selective enough

**Present findings** as a table:

| Issue | Location in Plan | Impact | Recommended Fix |
|-------|-----------------|--------|-----------------|
| Sequential scan | Seq Scan on orders | High - 2.3M rows | Create index on filter columns |
| Row estimate mismatch | ... | Medium | Run ANALYZE on table |

### Step 3: Get Index Recommendations

Use the index advisor to find optimization opportunities:

```
Tool: get_index_recommendations
Parameters:
  workload_queries: ["<query1>", "<query2>"]  # The slow queries from Step 1
  max_recommendations: 10
  min_improvement_percent: 10
  include_hypothetical_testing: true
```

**What to look for:**
- Indexes with > 50% estimated improvement should be prioritized
- Check if recommended indexes overlap with existing ones
- Consider the write overhead: each index slows INSERT/UPDATE/DELETE

### Step 4: Verify with Hypothetical Indexes

For the most promising index recommendations, verify using HypoPG:

```
Tool: explain_with_indexes
Parameters:
  query: "<the slow query>"
  hypothetical_indexes:
    - table: "orders"
      columns: ["customer_id", "created_at"]
      index_type: "btree"
  analyze: false   # Use false for hypothetical testing
```

**What to look for:**
- Cost reduction percentage (aim for > 30% to justify the index)
- Plan changes: Sequential Scan -> Index Scan or Bitmap Index Scan
- If improvement is minimal, the index may not be the right solution

### Step 5: Check Table Health

For tables involved in slow queries, check their maintenance status:

```
Tool: get_table_stats
Parameters:
  table_name: "<table_name>"
  schema_name: "public"
  include_indexes: true
```

**What to look for:**
- **High dead tuple ratio** (> 10%): Table needs `VACUUM`
- **Stale statistics** (`last_analyze` is old): Run `ANALYZE`
- **Sequential scan ratio** (seq_scan >> idx_scan): Missing indexes
- **Large table size** with no indexes: Index candidates

### Step 6: Check Disk I/O (Optional, for complex cases)

If buffer/IO issues were found in the execution plan:

```
Tool: analyze_disk_io_patterns
Parameters:
  analysis_type: "all"
  top_n: 20
```

**What to look for:**
- Low buffer cache hit ratio (< 99% is a warning for OLTP)
- Tables with high heap_blks_read (cold data)
- Excessive temp file usage (increase `work_mem`)

## Output Format

Present the final diagnosis as:

### Summary
Brief overview of findings (2-3 sentences).

### Critical Issues (Fix Immediately)
Numbered list of urgent issues with specific remediation SQL.

### Recommended Optimizations
Prioritized list with:
- **Issue**: What was found
- **Impact**: Estimated improvement
- **Fix**: Exact SQL command (CREATE INDEX, ANALYZE, VACUUM, config change)

### Configuration Suggestions
Any `postgresql.conf` changes that would help (e.g., `work_mem`, `effective_cache_size`).

## Common Patterns and Solutions

| Symptom | Likely Cause | Solution |
|---------|-------------|----------|
| Seq Scan on large table | Missing index | CREATE INDEX on WHERE/JOIN columns |
| Row estimate 1 vs actual 50000 | Stale statistics | ANALYZE table_name |
| Sort Method: external merge | work_mem too low | SET work_mem = '256MB' (session) |
| Nested Loop (actual loops=10000) | Bad join strategy | Check join column indexes, statistics |
| Hash Batches: 16 | work_mem too low for hash | Increase work_mem |
| Bitmap Heap Scan (lossy) | Index not selective | Consider composite index |

## Iterative Verification

After applying any fix, always verify:

1. **After creating an index**: Re-run `analyze_query` on the slow query to confirm the plan now uses the new index and timing improved.
2. **After running ANALYZE**: Re-run `analyze_query` to confirm row estimates are now accurate.
3. **After increasing work_mem**: Re-run the query to confirm temp files are eliminated (check `buffers: true` output).
4. **After query rewrite**: Compare before/after execution plans and timing.

## When to Stop and Ask the User

- **Before `analyze_query` with `analyze: true` on write queries**: "This will execute the query. For INSERT/UPDATE/DELETE, the tool wraps it in READ ONLY/ROLLBACK for safety, but please confirm you want to proceed."
- **If no clear performance issue is found**: "The database-side query performance appears normal. The issue may be in the application layer (N+1 queries, missing caching, network latency). Would you like me to investigate further?"
- **If recommendations require superuser**: "Creating this extension requires superuser privileges. Do you have access, or should I provide instructions for your DBA?"
- **If index recommendations have high write overhead**: "This table has X inserts/sec. Adding Y new indexes will increase write latency. Is this acceptable?"

## Cross-References to Other Skills

- **Query needs structural rewrite** (subqueries, CTEs, OR patterns): Use `pg-query-rewrite` skill
- **Index optimization beyond this query**: Use `pg-index-optimization` skill
- **work_mem / configuration tuning needed**: Use `pg-config-tuning` skill
- **I/O patterns need investigation**: Use `pg-io-deep-dive` skill
- **Table bloat causing slow scans**: Use `pg-bloat-analysis` skill

## PostgreSQL Version Notes

| Feature | Version | Impact on This Workflow |
|---------|---------|------------------------|
| `pg_stat_statements` `total_exec_time` | PG13+ (renamed from `total_time`) | Column name may differ |
| `compute_query_id` setting | PG14+ | Required for `queryid` in `pg_stat_statements` |
| `pg_stat_statements.track_planning` | PG13+ | Enables planning time tracking |
| CTE inlining | PG12+ | Non-recursive CTEs may be optimized differently |
| Memoize plan node | PG14+ | Improves nested loop performance |
| Incremental sort | PG13+ | Optimizer may choose different sort strategies |

## Important Notes

- `analyze_query` with `analyze: true` actually executes the query. Use caution with INSERT/UPDATE/DELETE statements (they are wrapped in READ ONLY transaction for safety).
- Always verify index recommendations with `explain_with_indexes` before applying in production.
- Consider write amplification: each new index adds overhead to every write operation on the table.
- If `pg_stat_statements` data is sparse, ask the user to collect more workload data first.
- The `PGTUNER_EXCLUDE_USERIDS` environment variable can filter out monitoring user queries from `get_slow_queries` results. Suggest configuring it if internal monitoring queries pollute the results.

## Production Safety

- All diagnostic tools (`get_slow_queries`, `get_table_stats`, `analyze_disk_io_patterns`) are read-only.
- `analyze_query` with `analyze: true` executes the query but wraps it in `BEGIN TRANSACTION READ ONLY` / `ROLLBACK`.
- `explain_with_indexes` creates temporary HypoPG indexes that exist only in the session and are never persisted.
- No tool in this workflow modifies data or schema. All CREATE INDEX / ANALYZE / VACUUM commands are recommendations for the user to execute.
