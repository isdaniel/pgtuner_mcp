---
name: pg-performance-baseline
description: Generates a comprehensive PostgreSQL performance baseline report capturing health metrics, query workload patterns, table statistics, configuration settings, index utilization, and I/O patterns. Designed for before/after comparison when making changes.
---

# PostgreSQL Performance Baseline

This skill guides creation of a comprehensive performance baseline report for a PostgreSQL database, intended for comparison before and after making configuration changes, schema modifications, or infrastructure upgrades.

## When to Use This Skill

Use this skill when the user:

- Is about to make database configuration changes and wants a "before" snapshot
- Wants to document current database performance for reference
- Needs to compare performance across time periods
- Asks "how is my database performing?" or "give me a performance report"
- Is planning a migration and needs to capture current metrics
- Wants to establish SLAs or performance targets based on current state

## MCP Resources Available

- `pgtuner://docs/tools` -- Tool reference for parameter details
- `pgtuner://docs/prompts` -- Available prompt templates
- `pgtuner://settings/{category}` -- Settings by category (use for structured capture: `memory`, `checkpoint`, `wal`, `autovacuum`, `connections`)
- `pgtuner://health/{check_type}` -- Targeted health metrics by type
- `pgtuner://table/{schema}/{table_name}/stats` -- Quick per-table stats

## Related MCP Prompt

This skill corresponds to the **`performance_baseline`** MCP Prompt. If the user triggers that prompt, follow this skill's workflow.

## Prerequisites

- The pgtuner-mcp server must be connected with a valid `DATABASE_URI`
- Required: `pg_stat_statements` extension (for query workload data)
- Recommended: `pgstattuple` extension (for bloat measurements)
- Recommended: Sufficient uptime since last `pg_stat_statements` reset for meaningful workload data

## Agent Decision Logic

```
Before starting:
  |
  +--> Is this a "before" baseline (pre-change)?
  |    --> Collect all 8 steps, save results systematically
  |    --> Tell user: "Baseline collected. After making changes, ask me to
  |        collect another baseline and I will generate a comparison."
  |
  +--> Is this an "after" baseline (post-change)?
  |    --> Collect all 8 steps with identical parameters
  |    --> Generate the Comparison Template with before/after diff
  |
  +--> Is this a standalone performance report?
       --> Collect all 8 steps
       --> Include recommendations inline (not just metrics)
```

## Baseline Collection Workflow

Collect all data systematically. Each step gathers a different performance dimension.

### Step 1: Health Metrics

```
Tool: check_database_health
Parameters:
  include_recommendations: true
  verbose: true
```

**Record:**
- Overall health score
- Per-dimension scores (connections, cache, locks, replication, wraparound, disk, bgwriter, checkpoints)
- PostgreSQL version
- Uptime since last restart

### Step 2: Query Workload Profile

```
Tool: get_slow_queries
Parameters:
  limit: 20
  min_calls: 1
  order_by: "mean_time"
```

Then also sort by total time to find the highest aggregate cost:
```
Tool: get_slow_queries
Parameters:
  limit: 20
  min_calls: 1
  order_by: "calls"
```

Also capture over-fetching patterns:
```
Tool: get_slow_queries
Parameters:
  limit: 10
  min_calls: 5
  order_by: "rows"
```

**Record:**
- Top 20 queries by mean execution time
- Top 20 queries by call frequency
- Top 10 queries by rows returned (over-fetching candidates)
- Total query count and aggregate execution time
- Cache hit ratios per query
- Queries with `temp_blks_written > 0` (work_mem issues)

### Step 3: Table Statistics

```
Tool: get_table_stats
Parameters:
  schema_name: "public"
  include_indexes: true
  order_by: "size"
```

Also check for vacuum maintenance gaps:
```
Tool: get_table_stats
Parameters:
  schema_name: "public"
  include_indexes: false
  order_by: "last_vacuum"
```

And tables with most sequential scans (index candidates):
```
Tool: get_table_stats
Parameters:
  schema_name: "public"
  include_indexes: false
  order_by: "seq_scans"
```

**Record:**
- Table sizes (data + TOAST + indexes)
- Row counts and dead tuple ratios
- Sequential scan vs index scan ratios
- Last vacuum and analyze timestamps
- Index count and sizes per table

### Step 4: Configuration Snapshot

Capture settings by category for structured comparison:

```
Tool: review_settings
Parameters:
  category: "memory"
  include_all_settings: false
```

```
Tool: review_settings
Parameters:
  category: "checkpoint"
  include_all_settings: false
```

```
Tool: review_settings
Parameters:
  category: "wal"
  include_all_settings: false
```

```
Tool: review_settings
Parameters:
  category: "autovacuum"
  include_all_settings: false
```

```
Tool: review_settings
Parameters:
  category: "connections"
  include_all_settings: false
```

**Record key settings in categories:**

**Memory:**
- shared_buffers
- effective_cache_size
- work_mem
- maintenance_work_mem
- huge_pages

**WAL / Checkpoints:**
- wal_level
- max_wal_size
- min_wal_size
- checkpoint_completion_target
- wal_compression

**Autovacuum:**
- autovacuum_max_workers
- autovacuum_naptime
- autovacuum_vacuum_scale_factor
- autovacuum_vacuum_cost_delay

**Connections:**
- max_connections
- superuser_reserved_connections

**Planner:**
- random_page_cost
- effective_io_concurrency
- default_statistics_target

### Step 5: Index Utilization

```
Tool: find_unused_indexes
Parameters:
  schema_name: "public"
  min_size_mb: 0.1
  include_duplicates: true
```

**Record:**
- Total number of indexes
- Unused indexes (count and total size)
- Duplicate/overlapping indexes
- Total index storage footprint

### Step 6: Wait Event Distribution

```
Tool: analyze_wait_events
Parameters:
  active_only: true
```

**Record:**
- Wait event type distribution
- Top wait events by count
- Any Lock or IO waits indicating contention

### Step 7: Disk I/O Patterns

```
Tool: analyze_disk_io_patterns
Parameters:
  analysis_type: "all"
  top_n: 20
```

**Record:**
- Buffer pool hit ratio
- Top tables by I/O volume
- Temp file usage statistics
- Checkpoint I/O statistics
- Backend vs checkpointer write ratio

### Step 8: Bloat Overview (Optional but recommended)

```
Tool: get_bloat_summary
Parameters:
  schema_name: "public"
  top_n: 10
```

**Record:**
- Top bloated tables and estimated waste
- Top bloated indexes
- Overall estimated reclaimable space

## Output Format

Structure the baseline as a timestamped report:

### Performance Baseline Report

**Database:** `<database_name>`
**Collected:** `<timestamp>`
**PostgreSQL Version:** `<version>`
**Uptime:** `<uptime>`

---

#### 1. Health Summary

| Dimension | Score | Notes |
|-----------|-------|-------|
| Overall | XX | |
| Connections | XX | X/Y used |
| Cache | XX | Hit ratio: XX.X% |
| Locks | XX | |
| Wraparound | XX | Oldest XID: XXM |
| Checkpoints | XX | Requested: X%, Timed: X% |

#### 2. Workload Profile

| Metric | Value |
|--------|-------|
| Total unique queries tracked | X |
| Top query mean time | X ms |
| Top query total time | X sec |
| Average cache hit ratio | XX.X% |
| Queries > 100ms mean time | X |
| Queries > 1s mean time | X |

**Top 5 by mean execution time:**

| Query (truncated) | Mean Time | Calls | Total Time |
|-------------------|-----------|-------|------------|
| SELECT ... | X ms | X | X s |

#### 3. Storage Profile

| Metric | Value |
|--------|-------|
| Total tables | X |
| Total data size | X GB |
| Total index size | X GB |
| Largest table | X (X GB) |
| Tables with > 10% dead tuples | X |
| Tables never vacuumed | X |

#### 4. Index Profile

| Metric | Value |
|--------|-------|
| Total indexes | X |
| Unused indexes | X (X MB wasted) |
| Duplicate indexes | X |
| Index-to-table size ratio | X:1 |

#### 5. Configuration Highlights

| Setting | Current | Recommended | Gap |
|---------|---------|-------------|-----|
| shared_buffers | X | X | OK / Needs change |
| work_mem | X | X | OK / Needs change |
| ... | ... | ... | ... |

#### 6. I/O Profile

| Metric | Value |
|--------|-------|
| Buffer cache hit ratio | XX.X% |
| Temp files created (since reset) | X |
| Checkpoint frequency (req/timed) | X% / X% |

#### 7. Current Issues

Numbered list of any active problems found during the baseline collection.

---

### Comparison Template

When collecting a second baseline after changes, present a diff:

| Metric | Before | After | Change |
|--------|--------|-------|--------|
| Health score | 82 | 91 | +9 |
| Cache hit ratio | 97.2% | 99.4% | +2.2% |
| Top query mean time | 450ms | 120ms | -73% |
| Unused index waste | 2.4 GB | 0.3 GB | -88% |

## Best Practices

- **Collect baselines at consistent times**: Same day of week, similar workload period
- **Do not reset pg_stat_statements** between before/after baselines
- **Record the exact timestamp** of collection for accurate comparison
- **Run the same version of tools** for both baselines
- **Capture system metrics too** (CPU, memory, disk I/O from OS) if possible, as they complement database-level metrics
- **Save the raw tool output** in addition to the summary for deeper post-analysis

## Important Notes

- All data collection is read-only and safe for production
- The quality of workload data depends on how long `pg_stat_statements` has been collecting since the last reset
- Very recently restarted databases will have incomplete statistics
- Table statistics require regular `ANALYZE` to be accurate: check `last_analyze` timestamps
- Bloat measurements with `pgstattuple` are I/O intensive on large tables (use `use_approx: true` for tables > 50 GB)

## Iterative Verification

Since baselines are snapshots, "verification" means collecting a second baseline for comparison:

1. **After configuration changes**: Collect a new baseline using identical parameters. Compare using the Comparison Template.
2. **After index changes**: Re-collect Step 2 (workload) and Step 5 (index utilization) to verify query improvements and index cleanup.
3. **After vacuum/bloat remediation**: Re-collect Step 8 (bloat overview) and Step 3 (table stats) to confirm space reclamation.
4. **After all changes**: Wait for representative workload (at least 24 hours of normal traffic) before collecting the "after" baseline.

## When to Stop and Ask the User

- **If pg_stat_statements was recently reset**: "Query workload data is limited because pg_stat_statements was recently reset. The baseline will be incomplete for the workload dimension. Would you like to proceed or wait for more data?"
- **If this is a post-change baseline**: "Is this baseline being collected after making changes? If so, I will generate a before/after comparison. Do you have the previous baseline results?"
- **If the database was recently restarted**: "The database was restarted recently. Cumulative statistics (buffer hits, checkpoint counts) may not be representative yet."

## Cross-References to Other Skills

After collecting a baseline, the agent should recommend specific skills based on findings:

- **Health score issues**: Use `pg-health-check` for deeper investigation
- **Slow queries identified**: Use `pg-slow-query-diagnosis` skill
- **Unused indexes found**: Use `pg-index-optimization` skill
- **Bloat detected**: Use `pg-bloat-analysis` skill
- **Configuration gaps**: Use `pg-config-tuning` skill
- **I/O concerns**: Use `pg-io-deep-dive` skill

## PostgreSQL Version Notes

| Feature | Version | Impact on Baseline |
|---------|---------|-------------------|
| `pg_stat_statements` `total_exec_time` | PG13+ | Column renamed from `total_time` |
| `pg_stat_io` | PG16+ | Additional I/O metrics available |
| `compute_query_id` | PG14+ | Required for queryid |
| `pgstattuple_approx()` | PG9.5+ | Faster bloat estimation |

## Production Safety

- All baseline collection is strictly read-only
- No data, configuration, or schema is modified
- Bloat measurements (`analyze_table_bloat` with `use_approx: false`) are I/O intensive and should use `use_approx: true` for very large tables during peak hours
