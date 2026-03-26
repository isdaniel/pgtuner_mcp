---
name: pg-io-deep-dive
description: Deep-dive investigation of PostgreSQL I/O patterns using targeted analysis types (buffer pool, tables, indexes, temp files, checkpoints). Covers pg_stat_io for PG16+, correlates I/O patterns with slow queries, and guides diagnosis of cache misses, temp file spills, and checkpoint storms.
---

# PostgreSQL I/O Deep-Dive

This skill guides a thorough investigation of PostgreSQL I/O performance using the full capabilities of the `analyze_disk_io_patterns` tool, correlating I/O data with query workload and configuration.

## When to Use This Skill

Use this skill when the user:

- Reports high disk I/O or slow disk performance
- Sees queries with high `blks_read` or low cache hit ratios
- Reports `temp_blks_written` in slow queries (work_mem too low)
- Sees frequent checkpoint warnings in PostgreSQL logs
- Has "IO" wait events from `analyze_wait_events`
- Asks "why is my database doing so much disk I/O?"
- Is sizing storage or evaluating SSD vs HDD performance

## MCP Resources Available

- `pgtuner://health/cache` -- Quick cache hit ratio check (lightweight, no full health scan)
- `pgtuner://settings/memory` -- Current memory configuration
- `pgtuner://settings/checkpoint` -- Current checkpoint configuration
- `pgtuner://table/{schema}/{table_name}/stats` -- Table-level I/O statistics

## Related MCP Prompt

This skill extends the I/O analysis portions of the **`health_check`** and **`diagnose_slow_queries`** MCP Prompts.

## Prerequisites

- The pgtuner-mcp server must be connected with a valid `DATABASE_URI`
- PostgreSQL 12+ required. PostgreSQL 16+ recommended for `pg_stat_io` metrics.

## Agent Decision Tree: Where to Start

```
Is the I/O problem...
  |
  +--> GENERAL (overall slow, high iowait)?
  |    --> Start with Step 1 (Buffer Pool) then Step 5 (Checkpoints)
  |
  +--> QUERY-SPECIFIC (one slow query with high blks_read)?
  |    --> Start with Step 2 (Table I/O) then Step 3 (Index I/O)
  |
  +--> TEMP-FILE RELATED (sort/hash spilling to disk)?
  |    --> Start with Step 4 (Temp Files)
  |
  +--> CHECKPOINT RELATED (log warnings about checkpoints)?
  |    --> Start with Step 5 (Checkpoints)
  |
  +--> UNKNOWN?
       --> Start with Step 1 (Buffer Pool) for overall picture
```

## I/O Investigation Workflow

### Step 1: Buffer Pool Analysis

```
Tool: analyze_disk_io_patterns
Parameters:
  analysis_type: "buffer_pool"
  top_n: 20
```

**What this reveals:**
- Overall buffer cache hit ratio (shared buffers effectiveness)
- Buffer allocation rate and eviction patterns
- Backend writes vs checkpointer writes

**Interpret results:**

| Metric | Healthy | Warning | Critical |
|--------|---------|---------|----------|
| Cache hit ratio | > 99% | 95-99% | < 95% |
| Backend writes > 0 | Rare | Occasional | Frequent (checkpointer not keeping up) |

**IF cache hit ratio < 99%:**
```
Tool: review_settings
Parameters:
  category: "memory"
```
Check if `shared_buffers` is undersized for the working set.

**IF backend writes > 0:**
Checkpointer/bgwriter is falling behind. Check `bgwriter_lru_maxpages` and checkpoint frequency.

### Step 2: Table I/O Hotspots

```
Tool: analyze_disk_io_patterns
Parameters:
  analysis_type: "tables"
  schema_name: "public"
  top_n: 20
  min_size_gb: 0.01
```

**What this reveals:**
- Per-table breakdown of `heap_blks_hit` vs `heap_blks_read`
- Tables causing the most physical disk reads
- Tables with poor cache hit ratios

**Agent reasoning:**
- Tables with high `heap_blks_read` relative to `heap_blks_hit`: Data is not fitting in shared_buffers
- Very large tables with mostly sequential access: Consider partitioning or BRIN indexes
- Tables with high `idx_blks_read`: Index is too large for buffer cache

**Cross-reference with table stats:**
For the top I/O-heavy tables:
```
Tool: get_table_stats
Parameters:
  table_name: "<hot_table>"
  schema_name: "public"
  include_indexes: true
  order_by: "seq_scans"
```

IF `seq_scan` count is very high AND the table is large, the table likely needs better indexes to avoid full scans.

### Step 3: Index I/O Patterns

```
Tool: analyze_disk_io_patterns
Parameters:
  analysis_type: "indexes"
  top_n: 20
  min_size_gb: 0.01
```

**What this reveals:**
- Per-index physical read rates
- Indexes that are too large for the buffer cache
- Indexes that are read-heavy but rarely hit in cache

**Agent reasoning:**
- Bloated indexes have more pages to read. Cross-check:
  ```
  Tool: analyze_index_bloat
  Parameters:
    table_name: "<table_with_hot_index>"
    schema_name: "public"
    min_index_size_gb: 0.01
    min_bloat_percent: 20
  ```
- Indexes on rarely-accessed columns waste buffer cache space. Cross-check with `find_unused_indexes`.

### Step 4: Temp File Analysis

```
Tool: analyze_disk_io_patterns
Parameters:
  analysis_type: "temp_files"
  top_n: 20
```

**What this reveals:**
- Total temp files created and their aggregate size
- Whether sorts and hash joins are spilling to disk

**Correlate with slow queries:**
```
Tool: get_slow_queries
Parameters:
  limit: 20
  min_calls: 1
  order_by: "mean_time"
```

Look for queries with `temp_blks_read > 0` or `temp_blks_written > 0` in the results. These queries are the ones spilling to disk.

**Then analyze the worst offender:**
```
Tool: analyze_query
Parameters:
  query: "<query_with_temp_blks>"
  analyze: true
  buffers: true
  settings: true
  format: "text"
```

The `settings: true` parameter reveals if a session-level `SET work_mem` is in effect. The `buffers: true` shows temp buffer usage in the plan.

**Look for in the plan:**
- `Sort Method: external merge Disk: XXkB` -> `work_mem` too low for this sort
- `Hash Batches: 16` -> Hash table spilling, increase `work_mem`
- `Buffers: temp read=XX written=XX` -> Direct confirmation of temp file usage

**Fix:**
```sql
-- Session-level (test first):
SET work_mem = '256MB';
-- Then re-run the query to verify temp files are eliminated.

-- Global (if many queries affected):
ALTER SYSTEM SET work_mem = '128MB';
SELECT pg_reload_conf();
```

### Step 5: Checkpoint I/O Analysis

```
Tool: analyze_disk_io_patterns
Parameters:
  analysis_type: "checkpoints"
  top_n: 10
```

**What this reveals:**
- Timed checkpoints vs requested checkpoints ratio
- Checkpoint write time and sync time
- Buffers written per checkpoint

**Interpret:**

| Metric | Healthy | Problem |
|--------|---------|---------|
| Requested checkpoints | < 10% of total | > 20% (max_wal_size too small) |
| Checkpoint write time | < 30 seconds | > 60 seconds (disk too slow or too much dirty data) |
| Sync time | < 5 seconds | > 10 seconds (fsync bottleneck) |

**IF requested checkpoints > 20%:**
```
Tool: review_settings
Parameters:
  category: "checkpoint"
```

Recommend increasing `max_wal_size` (e.g., from 1GB to 4GB or 8GB).

### Step 6: pg_stat_io Analysis (PostgreSQL 16+ Only)

The `analyze_disk_io_patterns` tool with `analysis_type: "all"` automatically includes `pg_stat_io` data when running on PostgreSQL 16+. This provides:

- I/O statistics broken down by backend type (client backend, autovacuum, checkpointer, etc.)
- Read/write/extend/fsync counts per object type (relation, temp relation, WAL)
- Hit rates per context (normal, vacuum, bulkread, bulkwrite)

**Agent decision:** If the PostgreSQL version is < 16, the `pg_stat_io` section will show a message saying it's not available. Skip this section and rely on the other analysis types.

**What to look for in pg_stat_io:**
- High `reads` in `bulkread` context: Sequential scans pulling large data volumes
- High `extends` on temp relations: Confirm temp file spill issues (Step 4)
- Autovacuum backend with high reads: Vacuum is scanning cold data, confirm with `monitor_vacuum_progress`

## Output Format

### I/O Analysis Report

**Buffer Pool:**
- Cache hit ratio: XX.X%
- Status: Healthy / Warning / Critical
- Recommendation: (if needed)

**Table I/O Hotspots:**

| Table | Size | Reads | Hits | Hit Ratio | Issue |
|-------|------|-------|------|-----------|-------|
| orders | 12GB | 450K | 2.1M | 82.4% | Low hit ratio -- undersized shared_buffers or missing index |

**Temp File Impact:**
- Total temp files since reset: X
- Total temp bytes: X GB
- Affected queries: list with `work_mem` recommendations

**Checkpoint Performance:**
- Requested: X% / Timed: X%
- Avg write time: X sec
- Recommendation: (if needed)

**Action Items (prioritized):**
1. ...
2. ...

## Iterative Verification

After applying any I/O-related fix:

1. **After increasing `shared_buffers`** (requires restart): Wait for cache to warm up (30+ minutes under normal workload), then re-run `analysis_type: "buffer_pool"`.
2. **After increasing `work_mem`**: Re-run the offending query with `analyze_query` (buffers: true) to confirm no more temp files.
3. **After increasing `max_wal_size`**: Monitor checkpoint frequency over 24 hours, then re-run `analysis_type: "checkpoints"`.

## When to Stop and Ask the User

- **If cache hit ratio is fine (> 99%) but queries are still slow**: The problem is not I/O. Redirect to `pg-slow-query-diagnosis` skill.
- **If temp file spill is caused by a single massive query**: Ask the user if increasing `work_mem` globally is acceptable, or suggest session-level `SET` for that specific query.
- **If the server is on HDD and I/O is saturated**: Ask "Is migrating to SSD an option? Most I/O tuning has limited effect on saturated spinning disks."

## PostgreSQL Version Notes

| Feature | Version | Notes |
|---------|---------|-------|
| `pg_stat_io` view | PG16+ | Detailed per-backend I/O statistics |
| `wal_compression = lz4` | PG15+ | Reduces WAL I/O |
| `BUFFERS` in EXPLAIN | PG9.4+ | Universally available |
| `checkpoint_flush_after` | PG9.6+ | Limit OS page cache dirty page accumulation |

## Production Safety

- All `analyze_disk_io_patterns` calls are read-only. No data is modified.
- `analyze_query` with `analyze: true` executes the query. Use caution with write queries.
- `review_settings` is read-only.
- I/O statistics are cumulative since server start or last reset. Short uptime means less meaningful data.
