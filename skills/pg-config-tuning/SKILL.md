---
name: pg-config-tuning
description: Guides PostgreSQL configuration tuning based on hardware profile, workload type, and current settings. Covers memory, WAL, checkpoints, autovacuum, planner, and connection settings with specific ALTER SYSTEM recommendations. Teaches the agent to use review_settings with individual categories and understand restart vs reload requirements.
---

# PostgreSQL Configuration Tuning

This skill guides systematic PostgreSQL configuration tuning using pgtuner-mcp tools, producing hardware-aware ALTER SYSTEM recommendations.

## When to Use This Skill

Use this skill when the user:

- Asks to tune or optimize `postgresql.conf` settings
- Reports running on default PostgreSQL configuration
- Asks "what settings should I change?" or "how should I configure shared_buffers?"
- Is deploying a new database and wants optimal initial configuration
- Asks about specific settings (work_mem, shared_buffers, effective_cache_size, etc.)
- Reports frequent checkpoints, high I/O, or memory pressure

## MCP Resources Available

Before calling tools, the agent can read lightweight resources for context:

- `pgtuner://settings/{category}` -- Retrieve settings by category: `memory`, `checkpoint`, `wal`, `autovacuum`, `connections`
- `pgtuner://docs/tools` -- Reference for all available tool parameters
- `pgtuner://docs/workflows` -- Recommended workflow patterns

## Related MCP Prompt

This skill corresponds to the **`health_check`** MCP Prompt (which includes settings review as Step 4). For a settings-focused session, this skill provides deeper guidance.

## Prerequisites

- The pgtuner-mcp server must be connected with a valid `DATABASE_URI`
- For best results, the agent should know the server's hardware profile (RAM, CPU cores, storage type)

## Agent Decision Logic: Gathering Context

Before recommending settings, the agent MUST understand the environment. Follow this decision tree:

```
1. Do I know the server's total RAM?
   YES -> proceed
   NO  -> ASK the user: "How much total RAM does the database server have?"

2. Do I know the storage type?
   YES -> proceed
   NO  -> ASK the user: "Is the database on SSD or spinning disk (HDD)?"

3. Do I know the workload type?
   YES -> proceed
   NO  -> ASK the user: "Is this primarily OLTP (many short transactions),
          OLAP (few complex analytical queries), or mixed?"

4. Do I know max_connections requirement?
   YES -> proceed
   NO  -> ASK the user: "How many concurrent connections do you expect?
          Are you using a connection pooler like PgBouncer?"
```

## Configuration Tuning Workflow

### Step 1: Audit Current Memory Settings

```
Tool: review_settings
Parameters:
  category: "memory"
  include_all_settings: false
```

**Key settings and tuning rules:**

| Setting | Formula | Notes |
|---------|---------|-------|
| `shared_buffers` | 25% of RAM (max ~16GB usually optimal) | Largest impact setting. More is not always better. |
| `effective_cache_size` | 50-75% of RAM | Planner hint only, does not allocate memory |
| `work_mem` | `(RAM * 0.25) / max_connections` | Per-operation, not per-connection. Can be 2-4x this for OLAP. |
| `maintenance_work_mem` | 256MB - 2GB | Used by VACUUM, CREATE INDEX. Higher = faster maintenance. |
| `huge_pages` | `try` on Linux with hugepages configured | Can improve TLB performance for large shared_buffers |

**Agent reasoning example:**
> Server has 64GB RAM, 200 max_connections, SSD, OLTP workload:
> - shared_buffers = 16GB (25% of 64GB)
> - effective_cache_size = 48GB (75% of 64GB)
> - work_mem = 80MB ((64GB * 0.25) / 200 = ~80MB)
> - maintenance_work_mem = 2GB

### Step 2: Audit Checkpoint and WAL Settings

```
Tool: review_settings
Parameters:
  category: "checkpoint"
  include_all_settings: false
```

Then separately:
```
Tool: review_settings
Parameters:
  category: "wal"
  include_all_settings: false
```

**Key settings:**

| Setting | Recommendation | Why |
|---------|---------------|-----|
| `checkpoint_completion_target` | 0.9 | Spread checkpoint writes over 90% of the interval |
| `max_wal_size` | 2GB - 8GB | Larger = fewer checkpoints = less I/O. Increase if requested checkpoints > 10% |
| `min_wal_size` | 1GB - 2GB | Keep WAL files pre-allocated |
| `wal_compression` | `on` (PG15+: `lz4` or `zstd`) | Reduces WAL volume, saves I/O |
| `wal_buffers` | 64MB (or `-1` for auto) | Auto-tuned from shared_buffers. 64MB is safe max. |
| `full_page_writes` | `on` (never turn off) | Required for crash safety |

**Validate with I/O data:**
```
Tool: analyze_disk_io_patterns
Parameters:
  analysis_type: "checkpoints"
  top_n: 10
```

IF requested checkpoints > 20% of total checkpoints, THEN `max_wal_size` is too low.

### Step 3: Audit Autovacuum Settings

```
Tool: review_settings
Parameters:
  category: "autovacuum"
  include_all_settings: false
```

**Recommendations based on workload:**

| Workload | autovacuum_max_workers | vacuum_cost_delay | vacuum_scale_factor |
|----------|----------------------|-------------------|---------------------|
| OLTP (high write) | 4-6 | 0ms (SSD) / 2ms (HDD) | 0.02 (2%) |
| OLAP (low write) | 3 | 2ms | 0.1 (10%) |
| Mixed | 4-5 | 0-2ms | 0.05 (5%) |

### Step 4: Audit Connection Settings

```
Tool: review_settings
Parameters:
  category: "connections"
  include_all_settings: false
```

**Key decisions:**

| Situation | Recommendation |
|-----------|---------------|
| `max_connections` > 200 without pooler | Reduce to actual need + add PgBouncer |
| Using PgBouncer in transaction mode | `max_connections` = 50-100 is often sufficient |
| No `statement_timeout` set | Set to 30s-60s for OLTP to prevent runaway queries |
| No `idle_in_transaction_session_timeout` | Set to 60s-300s to prevent connection/vacuum blocking |

### Step 5: Audit Planner Settings

```
Tool: review_settings
Parameters:
  category: "all"
  include_all_settings: false
```

Focus on planner-related settings:

| Setting | SSD | HDD | Impact |
|---------|-----|-----|--------|
| `random_page_cost` | 1.1 | 4.0 | Low value encourages index scans (correct for SSD) |
| `seq_page_cost` | 1.0 | 1.0 | Usually leave at default |
| `effective_io_concurrency` | 200 | 2 | Prefetch for bitmap heap scans |
| `default_statistics_target` | 100-500 | 100-500 | Higher = better cardinality estimates, slower ANALYZE |

### Step 6: Cross-Validate with Health Check

```
Tool: check_database_health
Parameters:
  include_recommendations: true
  verbose: true
```

Use health check results to validate configuration choices:
- Low cache hit ratio -> confirm `shared_buffers` increase
- High checkpoint frequency -> confirm `max_wal_size` increase
- Connection saturation -> confirm `max_connections` tuning

## Output Format

### Configuration Report

**Server Profile:**
- RAM: X GB
- Storage: SSD / HDD
- Workload: OLTP / OLAP / Mixed
- PostgreSQL Version: X.X

### Recommended Changes

```sql
-- === MEMORY (requires restart) ===
ALTER SYSTEM SET shared_buffers = '16GB';           -- Was: 128MB
ALTER SYSTEM SET huge_pages = 'try';                -- Was: off

-- === MEMORY (reload only) ===
ALTER SYSTEM SET effective_cache_size = '48GB';      -- Was: 4GB
ALTER SYSTEM SET work_mem = '80MB';                  -- Was: 4MB
ALTER SYSTEM SET maintenance_work_mem = '2GB';       -- Was: 64MB

-- === WAL / CHECKPOINTS (reload only) ===
ALTER SYSTEM SET max_wal_size = '4GB';               -- Was: 1GB
ALTER SYSTEM SET checkpoint_completion_target = '0.9'; -- Was: 0.5

-- === AUTOVACUUM (reload only) ===
ALTER SYSTEM SET autovacuum_max_workers = '5';        -- Was: 3
ALTER SYSTEM SET autovacuum_vacuum_cost_delay = '0';  -- Was: 20ms

-- === PLANNER (reload only) ===
ALTER SYSTEM SET random_page_cost = '1.1';           -- Was: 4.0 (SSD)
ALTER SYSTEM SET effective_io_concurrency = '200';    -- Was: 1 (SSD)

-- === CONNECTIONS (reload unless noted) ===
ALTER SYSTEM SET idle_in_transaction_session_timeout = '60s'; -- Was: 0
ALTER SYSTEM SET statement_timeout = '30s';                    -- Was: 0
```

### Apply Changes

```sql
-- For reload-only changes:
SELECT pg_reload_conf();

-- For restart-required changes (shared_buffers, huge_pages):
-- Schedule a maintenance window and restart PostgreSQL
```

### Restart vs Reload Reference

| Requires Restart | Reload Only |
|-----------------|-------------|
| shared_buffers | effective_cache_size |
| huge_pages | work_mem |
| max_connections | maintenance_work_mem |
| wal_buffers | max_wal_size |
| | checkpoint_completion_target |
| | random_page_cost |
| | autovacuum_* settings |
| | statement_timeout |

## When to Stop and Ask the User

- **Before recommending `shared_buffers` change**: Requires PostgreSQL restart. Ask: "Changing shared_buffers requires a database restart. When is your next maintenance window?"
- **Before recommending `max_connections` reduction**: May reject connections. Ask: "Lowering max_connections could reject connections if current usage exceeds the new limit. What is your peak connection count?"
- **If workload type is unclear**: Do not guess. Ask the user.
- **If the system is a managed database** (RDS, Cloud SQL, etc.): Some settings are not modifiable. Ask: "Are you running self-managed PostgreSQL or a managed service (RDS, Cloud SQL, Aurora)?"

## PostgreSQL Version Notes

| Setting / Feature | Version Notes |
|-------------------|---------------|
| `wal_compression = lz4/zstd` | PG15+. Prior versions only support `on` (pglz). |
| `huge_pages = try` | Linux only. Not applicable on Windows. |
| `compute_query_id` | PG14+. Required for `pg_stat_statements` queryid. |
| `recovery_min_apply_delay` | Replica only (PG12+). |

## Production Safety

- All `review_settings` calls are read-only. No changes are made by the tools.
- `ALTER SYSTEM` writes to `postgresql.auto.conf`, not `postgresql.conf`. Changes are not applied until `pg_reload_conf()` or restart.
- Always verify changes with `SHOW setting_name` after reload/restart.
- Keep a backup of the original settings before making changes.
