---
name: pg-bloat-analysis
description: Detects and remediates table and index bloat in PostgreSQL using pgstattuple analysis. Identifies fragmentation, dead tuples, and wasted space with prioritized maintenance actions including VACUUM, REINDEX, and pg_repack recommendations.
---

# PostgreSQL Bloat Analysis

This skill guides detection, analysis, and remediation of table and index bloat in PostgreSQL databases using pgtuner-mcp tools.

## When to Use This Skill

Use this skill when the user:

- Reports tables or indexes growing unexpectedly large
- Asks about database bloat, dead tuples, or fragmentation
- Mentions disk space running low
- Sees slow sequential scans on tables that should be small
- Asks "why is my table so large?" or "how do I reclaim disk space?"
- Notices `VACUUM` not keeping up or autovacuum running constantly
- Reports degraded performance after bulk DELETE or UPDATE operations

## MCP Resources Available

- `pgtuner://health/bloat` -- Quick bloat health assessment (lightweight)
- `pgtuner://table/{schema}/{table_name}/stats` -- Table statistics including dead tuple counts
- `pgtuner://settings/autovacuum` -- Autovacuum configuration (relevant for root cause analysis)

## Related MCP Prompt

This skill is referenced by the **`health_check`** MCP Prompt (Step 6: Quick Bloat Scan). For dedicated bloat investigation, follow this skill's full workflow.

## Prerequisites

- The pgtuner-mcp server must be connected with a valid `DATABASE_URI`
- Required: `pgstattuple` extension (for accurate bloat measurement)
- Without `pgstattuple`: The tools fall back to statistical estimation (less accurate)

## Pre-Flight Checks

- If `analyze_table_bloat` or `get_bloat_summary` returns an error about `pgstattuple`, inform the user: "The pgstattuple extension is not installed. Bloat measurements will be estimated from `pg_stat_user_tables` statistics, which is less accurate. For precise measurements, install pgstattuple: `CREATE EXTENSION pgstattuple;`"

## Agent Decision Tree

```
User reports bloat concerns
  |
  +--> General "how bloated is my database?"
  |    --> Start with Step 1 (get_bloat_summary)
  |
  +--> Specific table is too large
  |    --> Skip to Step 2 (analyze_table_bloat) for that table
  |
  +--> Disk space running low
  |    --> Start with Step 1, then prioritize high-bloat tables
  |
  +--> After Step 1, triage results:
       |
       +--> Dead tuple ratio > 20% on any table?
       |    --> Deep-dive Step 2, then investigate root cause (Step 5)
       |
       +--> XID age > 800M on any table?
       |    --> URGENT: redirect to pg-vacuum-tuning emergency procedures
       |
       +--> Index bloat > 40%?
       |    --> Step 3 (index bloat analysis), recommend REINDEX
       |
       +--> Autovacuum not keeping up?
            --> Step 4 (vacuum status), then pg-vacuum-tuning skill
```

## Bloat Analysis Workflow

### Step 1: Database-Wide Bloat Survey

Start with a high-level overview:

```
Tool: get_bloat_summary
Parameters:
  schema_name: "public"
  top_n: 20
  min_size_gb: 0.01    # Lower threshold to catch smaller tables too
```

**What this provides:**
- Combined table and index bloat data
- Prioritized maintenance action list (high/medium/low priority)
- Total wasted space estimate

**Triage the results:**
- **High priority**: Tables with dead tuple ratio > 20% or approaching wraparound
- **Medium priority**: Tables with dead tuple ratio 10-20% or significant index bloat
- **Low priority**: Minor bloat that autovacuum should handle

### Step 2: Deep Table Bloat Analysis

For tables flagged in the survey, check dead tuple density:

```
Tool: get_table_stats
Parameters:
  schema_name: "public"
  include_indexes: false
  order_by: "dead_tuples"
```

Then do a deep analysis on the worst offenders:

```
Tool: analyze_table_bloat
Parameters:
  table_name: "<flagged_table>"
  schema_name: "public"
  use_approx: false      # Use exact measurement for important tables
  include_toast: true     # Check TOAST tables too (large text/json columns)
```

For very large tables (> 100 GB), use approximate mode to avoid long scans:
```
Tool: analyze_table_bloat
Parameters:
  table_name: "<large_table>"
  use_approx: true
  min_table_size_gb: 0.1
```

**Interpret the results:**

| Metric | Healthy | Warning | Critical |
|--------|---------|---------|----------|
| dead_tuple_percent | < 5% | 5-20% | > 20% |
| free_space_percent | < 10% | 10-30% | > 30% |
| tuple_percent (live data density) | > 80% | 50-80% | < 50% |

**Severity scoring (from the tool):**
- Score >= 6: Critical bloat
- Score >= 4: High bloat
- Score >= 2: Moderate bloat
- Score >= 1: Low bloat

### Step 3: Index Bloat Analysis

For indexes on bloated tables:

```
Tool: analyze_index_bloat
Parameters:
  table_name: "<table_name>"
  schema_name: "public"
  min_index_size_gb: 0.01
  min_bloat_percent: 15
```

**Interpret the results:**

| Index Type | Metric | Healthy | Bloated |
|-----------|--------|---------|---------|
| B-tree | avg_leaf_density | > 80% | < 70% |
| B-tree | estimated_bloat | < 20% | > 30% |
| GIN | pending_pages | 0 | > 100 |

**Common causes of index bloat:**
- HOT updates not possible (indexed columns being updated)
- Long-running transactions preventing dead tuple cleanup
- Monotonically increasing keys with deletions (B-tree right-growth pattern)

### Step 4: Vacuum Status Check

Understand why bloat accumulated:

```
Tool: monitor_vacuum_progress
Parameters:
  action: "needs_vacuum"
  schema_name: "public"
  min_dead_tuples: 100
```

Check autovacuum configuration:
```
Tool: monitor_vacuum_progress
Parameters:
  action: "autovacuum_status"
```

Check recent vacuum history:
```
Tool: monitor_vacuum_progress
Parameters:
  action: "recent_activity"
```

**What to look for:**
- Tables that need vacuum but haven't been vacuumed
- Autovacuum workers at capacity (all slots busy)
- Tables with aggressive thresholds that never get vacuumed
- Transaction wraparound risk (XID age approaching `autovacuum_freeze_max_age`)

### Step 5: Root Cause Analysis

Based on the data collected, identify why bloat is accumulating:

| Symptom | Root Cause | Evidence |
|---------|-----------|----------|
| High dead tuples, recent vacuum | Update/delete rate exceeds vacuum throughput | `n_dead_tup` still high after vacuum |
| High dead tuples, no recent vacuum | Autovacuum not running on this table | `last_autovacuum` is NULL or very old |
| High dead tuples, vacuum running | Long-running transaction holding back cleanup | `backend_xmin` in `pg_stat_activity` |
| Index bloat but table is OK | HOT updates not possible | Updated columns are indexed |
| Free space high, dead tuples low | Space not returned to OS after mass delete | Table was VACUUMed but not reclaimed |

## Remediation Plan

### For Table Bloat

**Mild bloat (dead tuples 5-20%):**
```sql
-- Standard VACUUM (reclaims dead tuples, does NOT return space to OS)
VACUUM VERBOSE schema.table_name;
```

**Severe bloat (dead tuples > 20% or free space > 30%):**
```sql
-- VACUUM FULL rewrites the table (LOCKS TABLE - schedule during maintenance window)
VACUUM FULL VERBOSE schema.table_name;
-- Then update statistics
ANALYZE schema.table_name;
```

**Alternative for production (no downtime):**
```sql
-- pg_repack (requires pg_repack extension)
-- Rewrites table online without exclusive lock
pg_repack --table schema.table_name --no-superuser-check -d dbname
```

### For Index Bloat

**Moderate index bloat (20-40%):**
```sql
-- REINDEX CONCURRENTLY (PostgreSQL 12+, no blocking)
REINDEX INDEX CONCURRENTLY schema.index_name;
```

**Severe index bloat (> 40%):**
```sql
-- For all indexes on a table
REINDEX TABLE CONCURRENTLY schema.table_name;
```

### Autovacuum Tuning

If autovacuum is not keeping up:

```sql
-- Per-table autovacuum tuning for high-churn tables
ALTER TABLE schema.table_name SET (
  autovacuum_vacuum_scale_factor = 0.01,      -- Vacuum at 1% dead tuples (default 20%)
  autovacuum_vacuum_threshold = 1000,          -- Minimum dead tuples before vacuum
  autovacuum_analyze_scale_factor = 0.005,     -- Analyze at 0.5% changes
  autovacuum_vacuum_cost_delay = 2             -- Faster vacuum (default 20ms)
);
```

Global autovacuum tuning:
```sql
-- Make autovacuum more aggressive globally
ALTER SYSTEM SET autovacuum_max_workers = 6;           -- Default 3
ALTER SYSTEM SET autovacuum_vacuum_cost_delay = '2ms'; -- Default 20ms
ALTER SYSTEM SET autovacuum_naptime = '15s';            -- Default 1min
-- Requires reload: SELECT pg_reload_conf();
```

## Output Format

### Bloat Summary

| Object | Type | Size | Bloat % | Dead Tuples | Priority | Action |
|--------|------|------|---------|-------------|----------|--------|
| orders | table | 12 GB | 35% | 4.2 GB | High | VACUUM FULL or pg_repack |
| idx_orders_date | index | 3.2 GB | 42% | 1.3 GB | High | REINDEX CONCURRENTLY |

### Root Cause
Brief explanation of why bloat accumulated.

### Maintenance Script
Ordered SQL commands to resolve the issues, with timing and locking notes.

### Prevention Recommendations
Autovacuum tuning and application-level changes to prevent recurrence.

## Important Notes

- `analyze_table_bloat` with `use_approx: false` reads the entire table (I/O intensive on large tables)
- `VACUUM FULL` requires an exclusive lock on the table (blocks all reads and writes)
- `REINDEX CONCURRENTLY` requires PostgreSQL 12+
- Always run `ANALYZE` after `VACUUM FULL` or `REINDEX` to update statistics
- TOAST table bloat is often overlooked: use `include_toast: true` for tables with large text/json columns
- `pgstattuple` extension must be installed for accurate measurements. Without it, results are estimated.

## Iterative Verification

After applying remediation:

1. **After VACUUM**: Re-run `analyze_table_bloat` to confirm dead tuples were cleaned up. Note: `VACUUM` does not reduce table size on disk (only marks space as reusable).
2. **After VACUUM FULL**: Re-run `analyze_table_bloat` to confirm both dead tuples and free space were reclaimed.
3. **After REINDEX**: Re-run `analyze_index_bloat` to confirm avg_leaf_density improved.
4. **After autovacuum tuning**: Monitor with `monitor_vacuum_progress` `action: "progress"` over the next hours/days to confirm vacuum is keeping up.

## When to Stop and Ask the User

- **Before recommending VACUUM FULL**: "VACUUM FULL requires an exclusive lock on the table, blocking all reads and writes for the duration. For a X GB table, this could take Y minutes. When is your maintenance window? Alternatively, pg_repack can do this online."
- **If pgstattuple is not installed**: "Bloat measurements are estimated. For accurate results, install pgstattuple. Would you like to proceed with estimates?"
- **If bloat is moderate (< 20%)**: "The bloat level is within normal range and autovacuum should handle it. No immediate action needed. Would you like me to tune autovacuum for better ongoing maintenance?"
- **If root cause is long-running transactions**: "Bloat is accumulating because of idle-in-transaction sessions blocking vacuum. This requires application-level fixes. Would you like me to investigate the blocking sessions?"

## Cross-References to Other Skills

- **Autovacuum tuning for bloat prevention**: Use `pg-vacuum-tuning` skill
- **Index-specific bloat and optimization**: Use `pg-index-optimization` skill
- **I/O impact of bloated tables**: Use `pg-io-deep-dive` skill
- **Lock issues blocking vacuum**: Use `pg-lock-diagnosis` skill

## PostgreSQL Version Notes

| Feature | Version |
|---------|---------|
| `pgstattuple_approx()` | PG9.5+ |
| `REINDEX CONCURRENTLY` | PG12+ |
| `pg_stat_progress_vacuum` | PG9.6+ |
| Improved autovacuum cost delay default (2ms) | PG12+ |

## Production Safety

- `analyze_table_bloat` and `analyze_index_bloat` are read-only but I/O intensive (especially with `use_approx: false` on large tables)
- `get_bloat_summary` is read-only and safe
- All VACUUM, REINDEX, and ALTER TABLE commands are recommendations, not executed by the tools
- For large tables (> 50 GB), always use `use_approx: true` to avoid long scan times
