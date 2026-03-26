---
name: pg-vacuum-tuning
description: Monitors and tunes PostgreSQL vacuum operations including autovacuum configuration, vacuum progress tracking, wraparound prevention, and per-table vacuum strategy. Provides tuning recommendations for high-churn OLTP and large analytical tables.
---

# PostgreSQL Vacuum Tuning

This skill guides monitoring, troubleshooting, and tuning of PostgreSQL vacuum operations using pgtuner-mcp tools.

## When to Use This Skill

Use this skill when the user:

- Asks about autovacuum configuration or tuning
- Reports autovacuum running too slowly or too aggressively
- Mentions transaction ID wraparound warnings
- Sees tables accumulating dead tuples
- Asks "why is autovacuum always running?" or "why isn't autovacuum cleaning my table?"
- Reports table bloat growing over time
- Wants to understand vacuum progress on a long-running operation
- Asks about VACUUM FREEZE or preventing forced shutdowns

## MCP Resources Available

- `pgtuner://settings/autovacuum` -- Current autovacuum configuration (lightweight)
- `pgtuner://health/bloat` -- Quick bloat assessment
- `pgtuner://table/{schema}/{table_name}/stats` -- Per-table dead tuple counts and vacuum timestamps

## Related MCP Prompt

This skill is related to the **`health_check`** MCP Prompt (wraparound dimension) and the vacuum aspects of `pg-bloat-analysis` skill.

## Prerequisites

- The pgtuner-mcp server must be connected with a valid `DATABASE_URI`
- Access to `pg_stat_activity`, `pg_stat_user_tables`, `pg_stat_progress_vacuum`

## Agent Decision Tree

```
User asks about vacuum
  |
  +--> "Why is autovacuum always running?"
  |    --> Start Step 1 (progress), then Step 3 (autovacuum config)
  |    --> May be normal for high-churn tables. Check if it's completing.
  |
  +--> "Why isn't my table being vacuumed?"
  |    --> Start Step 2 (needs_vacuum), then Step 3 (autovacuum config)
  |    --> Check if scale_factor threshold is too high for large tables
  |
  +--> "Transaction wraparound warning"
  |    --> URGENT: Start Step 2 (needs_vacuum), check XID age
  |    --> IF XID age > 1.2B: Go to Emergency Procedures immediately
  |
  +--> "How long will this vacuum take?"
  |    --> Start Step 1 (progress) to see current phase and completion %
  |
  +--> "General vacuum tuning"
       --> Follow Steps 1-4 in order, then Tuning Recommendations
```

## Vacuum Monitoring Workflow

### Step 1: Check Current Vacuum Activity

```
Tool: monitor_vacuum_progress
Parameters:
  action: "progress"
```

**What this shows:**
- Currently running VACUUM operations (manual and autovacuum)
- Phase of each vacuum (scanning heap, vacuuming indexes, truncating heap, etc.)
- Progress percentage and estimated time remaining
- Which autovacuum workers are active

**Vacuum phases explained:**

| Phase | Description | Duration |
|-------|-------------|----------|
| initializing | Starting up | Brief |
| scanning heap | Finding dead tuples | Proportional to table size |
| vacuuming indexes | Cleaning index entries | Depends on number of indexes |
| vacuuming heap | Removing dead tuples from heap | Proportional to dead tuples |
| cleaning up indexes | Final index cleanup | Brief |
| truncating heap | Returning pages to OS (end of table only) | Brief |
| performing final cleanup | Updating statistics | Brief |

### Step 2: Identify Tables Needing Vacuum

```
Tool: monitor_vacuum_progress
Parameters:
  action: "needs_vacuum"
  schema_name: "public"
  min_dead_tuples: 500
  include_toast: true
```

**What to look for:**

| Condition | Risk Level | Action |
|-----------|-----------|--------|
| Dead tuples > autovacuum threshold | Normal | Autovacuum should pick it up soon |
| Dead tuples >> threshold, no recent vacuum | Warning | Autovacuum may be overwhelmed |
| XID age > 200 million | Warning | Monitor, autovacuum should handle |
| XID age > 1 billion | Critical | Manual VACUUM FREEZE immediately |
| XID age > 1.5 billion | Emergency | Database will shut down at 2 billion |

**Transaction wraparound explained:**
PostgreSQL uses 32-bit transaction IDs (XIDs). After ~2 billion transactions, old XIDs "wrap around" and data could appear to be in the future. PostgreSQL will force a shutdown at 2^31 - 1 million XIDs to prevent data corruption. Autovacuum normally prevents this by freezing old XIDs, but if vacuum can't keep up, manual intervention is needed.

### Step 3: Review Autovacuum Configuration

```
Tool: monitor_vacuum_progress
Parameters:
  action: "autovacuum_status"
```

Also check the settings:
```
Tool: review_settings
Parameters:
  category: "autovacuum"
```

**Key autovacuum parameters:**

| Parameter | Default | Recommended for OLTP | Purpose |
|-----------|---------|---------------------|---------|
| `autovacuum` | on | on (never turn off) | Master switch |
| `autovacuum_max_workers` | 3 | 4-6 | Parallel vacuum workers |
| `autovacuum_naptime` | 1min | 15-30s | Time between autovacuum runs |
| `autovacuum_vacuum_threshold` | 50 | 50 | Min dead tuples before vacuum |
| `autovacuum_vacuum_scale_factor` | 0.2 | 0.02-0.05 | Fraction of table that triggers vacuum |
| `autovacuum_analyze_threshold` | 50 | 50 | Min changes before analyze |
| `autovacuum_analyze_scale_factor` | 0.1 | 0.01-0.02 | Fraction of table that triggers analyze |
| `autovacuum_vacuum_cost_delay` | 2ms (PG12+) | 2ms | Pause between cost-limited work |
| `autovacuum_vacuum_cost_limit` | -1 (uses vacuum_cost_limit=200) | 400-1000 | Work limit per cycle |
| `autovacuum_freeze_max_age` | 200M | 200M | Forces vacuum to prevent wraparound |

**The scale factor problem:**
With default `autovacuum_vacuum_scale_factor = 0.2`, a 100-million-row table needs 20 million dead tuples before autovacuum triggers. This is almost always too high for large tables.

### Step 4: Check Recent Vacuum History

```
Tool: monitor_vacuum_progress
Parameters:
  action: "recent_activity"
```

**Categories:**
- **never**: Table has never been vacuumed (new table or vacuum disabled)
- **stale**: Last vacuum > 7 days ago
- **recent**: Last vacuum 1-7 days ago
- **fresh**: Last vacuum within 24 hours

**What to investigate:**
- Tables in "never" or "stale" category
- Tables where autovacuum ran recently but dead tuples are still high (vacuum not effective)
- Tables with very frequent vacuum (may indicate excessive churn)

### Step 5: Check for Vacuum Blockers

If vacuum is not making progress, check for long-running transactions:

```
Tool: get_active_queries
Parameters:
  include_idle: true
  min_duration_seconds: 60
```

**Common vacuum blockers:**
- `idle in transaction` sessions hold back the visibility horizon
- Long-running queries prevent dead tuple cleanup for rows they might still see
- Unused replication slots prevent WAL cleanup and can prevent vacuum from removing dead tuples

## Tuning Recommendations

### For High-Churn OLTP Tables

Tables with millions of INSERTs/UPDATEs/DELETEs per day:

```sql
-- Aggressive per-table settings
ALTER TABLE schema.hot_table SET (
  autovacuum_vacuum_scale_factor = 0.01,       -- Trigger at 1% dead tuples
  autovacuum_vacuum_threshold = 1000,           -- Or at least 1000 dead tuples
  autovacuum_analyze_scale_factor = 0.005,      -- Analyze at 0.5% changes
  autovacuum_vacuum_cost_delay = 0,             -- No throttling for this table
  autovacuum_vacuum_cost_limit = 1000           -- Higher work limit
);
```

### For Large Analytical Tables

Tables that are mostly read with occasional bulk loads:

```sql
-- Less frequent but thorough vacuum
ALTER TABLE schema.fact_table SET (
  autovacuum_vacuum_scale_factor = 0.05,       -- 5% threshold (OK for large read-mostly tables)
  autovacuum_vacuum_threshold = 10000,
  autovacuum_enabled = true,                    -- Ensure it's on
  autovacuum_freeze_min_age = 100000000,        -- Freeze less aggressively
  autovacuum_freeze_table_age = 300000000       -- Full-table freeze less often
);
```

### For Append-Only / Time-Series Tables

Tables where old data is never updated:

```sql
-- Minimal vacuum needed since there are few dead tuples
ALTER TABLE schema.log_table SET (
  autovacuum_vacuum_scale_factor = 0.1,
  autovacuum_vacuum_threshold = 10000,
  autovacuum_freeze_min_age = 50000000,         -- Freeze sooner (data won't change)
  fillfactor = 100                               -- Pack pages full (no updates expected)
);
```

### Global Autovacuum Tuning

```sql
-- For servers with SSDs and sufficient I/O capacity
ALTER SYSTEM SET autovacuum_max_workers = 6;
ALTER SYSTEM SET autovacuum_naptime = '15s';
ALTER SYSTEM SET autovacuum_vacuum_cost_delay = '0ms';    -- No throttling on SSD
ALTER SYSTEM SET autovacuum_vacuum_cost_limit = 1000;
ALTER SYSTEM SET vacuum_cost_limit = 1000;

-- For wraparound prevention
ALTER SYSTEM SET autovacuum_freeze_max_age = '300000000'; -- 300M (default 200M)

-- Apply: SELECT pg_reload_conf();
```

## Output Format

### Current Vacuum Status

| Table | Dead Tuples | XID Age | Last Vacuum | Last Autovacuum | Action |
|-------|------------|---------|-------------|-----------------|--------|
| orders | 2.5M (12%) | 450M | 3 days ago | 2 days ago | Tune scale_factor |
| sessions | 150K (45%) | 1.1B | never | never | VACUUM FREEZE now |

### Autovacuum Health
- Workers: 2/3 active
- Configuration assessment
- Tables exceeding thresholds

### Recommended Changes
Per-table and global tuning SQL, ordered by priority.

### Wraparound Risk Assessment
Tables ranked by XID age with time-to-wraparound estimate.

## Emergency Procedures

### Transaction Wraparound Emergency (XID age > 1.2 billion)

```sql
-- 1. Cancel non-essential queries
SELECT pg_cancel_backend(pid) FROM pg_stat_activity
WHERE state != 'idle' AND query NOT LIKE 'autovacuum%'
AND backend_start < now() - interval '1 hour';

-- 2. Run manual VACUUM FREEZE on the affected table
VACUUM (FREEZE, VERBOSE) schema.table_name;

-- 3. Monitor progress
SELECT * FROM pg_stat_progress_vacuum;
```

### Autovacuum Completely Stalled

```sql
-- 1. Check for blocking transactions
SELECT pid, state, xact_start, query
FROM pg_stat_activity
WHERE xact_start < now() - interval '1 hour'
ORDER BY xact_start;

-- 2. Terminate long idle-in-transaction sessions (careful!)
SELECT pg_terminate_backend(pid) FROM pg_stat_activity
WHERE state = 'idle in transaction'
AND xact_start < now() - interval '1 hour';

-- 3. Verify autovacuum is enabled
SHOW autovacuum;

-- 4. Force autovacuum to recheck
SELECT pg_reload_conf();
```

## Important Notes

- Never disable autovacuum (`autovacuum = off`). It prevents transaction wraparound.
- `VACUUM FULL` is rarely needed and locks the table. Prefer regular `VACUUM` or `pg_repack`.
- Per-table settings override global settings and are stored in `pg_class.reloptions`.
- After bulk DELETE operations, run manual `VACUUM` to promptly reclaim space.
- Monitoring tools in this skill are read-only and safe for production.
- Autovacuum parameter changes via `ALTER SYSTEM` require `pg_reload_conf()` (no restart).
- Per-table settings via `ALTER TABLE ... SET` take effect on the next autovacuum run.

## Iterative Verification

After applying vacuum tuning changes:

1. **After autovacuum tuning**: Monitor with `monitor_vacuum_progress` `action: "progress"` over the next hours to confirm autovacuum is now running on the target tables.
2. **After manual VACUUM**: Re-run `monitor_vacuum_progress` `action: "needs_vacuum"` to confirm dead tuples were cleaned up.
3. **After resolving blocking transactions**: Re-run `get_active_queries` with `include_idle: true` to confirm no more blockers, then re-check vacuum progress.
4. **After VACUUM FREEZE**: Read `pgtuner://table/{schema}/{table_name}/stats` to verify XID age was reset.

## When to Stop and Ask the User

- **If XID age > 1 billion**: "CRITICAL: Transaction wraparound risk on table X. This requires immediate VACUUM FREEZE. Shall I provide the emergency procedure? All other work should be paused."
- **If autovacuum is disabled**: "Autovacuum is turned OFF. This is extremely dangerous as it can lead to transaction wraparound and database shutdown. Shall I enable it immediately?"
- **If vacuum blockers are long-running application transactions**: "Vacuum progress is blocked by long-running transactions from the application. This requires application-level fixes. Would you like me to identify the specific sessions?"
- **If the user asks about VACUUM FULL**: "VACUUM FULL rewrites the entire table with an exclusive lock. For a production table, consider pg_repack instead. Do you have a maintenance window?"

## Cross-References to Other Skills

- **Bloat analysis and remediation**: Use `pg-bloat-analysis` skill
- **Lock contention blocking vacuum**: Use `pg-lock-diagnosis` skill
- **Connection issues (idle-in-transaction)**: Use `pg-connection-analysis` skill
- **Autovacuum settings as part of config tuning**: Use `pg-config-tuning` skill

## PostgreSQL Version Notes

| Feature | Version |
|---------|---------|
| `pg_stat_progress_vacuum` | PG9.6+ |
| `pg_stat_progress_vacuum.num_dead_item_ids` | PG14+ |
| `VACUUM (PARALLEL n)` | PG13+ (parallel index vacuum) |
| Improved autovacuum cost delay default (2ms) | PG12+ |
| `log_autovacuum_min_duration` | PG9.4+ (log slow autovacuum runs) |

## Production Safety

- All `monitor_vacuum_progress` calls are read-only
- `review_settings` is read-only
- `get_active_queries` is read-only
- Emergency `pg_cancel_backend` / `pg_terminate_backend` calls should only be used with explicit user approval
- Autovacuum tuning via `ALTER SYSTEM` / `ALTER TABLE ... SET` does not require a restart
