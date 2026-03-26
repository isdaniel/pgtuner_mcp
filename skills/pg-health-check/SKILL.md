---
name: pg-health-check
description: Performs comprehensive PostgreSQL database health assessments covering connections, cache performance, locks, replication, wait events, active queries, and configuration review. Produces a scored health report with prioritized recommendations.
---

# PostgreSQL Health Check

This skill guides a comprehensive health assessment of a PostgreSQL database using pgtuner-mcp tools, producing a scored report with prioritized action items.

## When to Use This Skill

Use this skill when the user:

- Asks for a database health check or status report
- Reports general performance degradation without specific symptoms
- Wants a routine database checkup or audit
- Asks "is my database healthy?" or "what should I fix first?"
- Is setting up monitoring or wants to establish a baseline
- Mentions connection issues, high CPU, or unexplained slowness

## MCP Resources Available

Before running full tool scans, the agent can use targeted resources:

- `pgtuner://health/{check_type}` -- Targeted health checks: `connections`, `cache`, `locks`, `replication`, `bloat`, `all` (lightweight, avoids running the full health tool)
- `pgtuner://settings/{category}` -- Settings by category: `memory`, `checkpoint`, `wal`, `autovacuum`, `connections`
- `pgtuner://docs/workflows` -- Recommended workflow patterns

**Smart resource usage:** After the full health check in Step 1, use targeted resources for follow-up instead of re-running the full tool. For example, if the locks dimension scores low, read `pgtuner://health/locks` for quick re-checks after remediation.

## Related MCP Prompt

This skill corresponds to the **`health_check`** MCP Prompt. If the user triggers that prompt, follow this skill's workflow.

## Prerequisites

- The pgtuner-mcp server must be connected with a valid `DATABASE_URI`
- Required: `pg_stat_statements` extension
- Recommended: monitoring user with access to `pg_stat_activity`, `pg_stat_bgwriter`, `pg_locks`

## Agent Decision Tree

```
User asks for health check
  |
  +--> Quick check or full check?
  |    QUICK --> Read pgtuner://health/all resource (fast, no tool call)
  |    FULL  --> Start with Step 1 (check_database_health)
  |
  +--> After Step 1, which dimensions scored low?
       |
       +--> Connections < 90 --> Prioritize Step 2, then pg-connection-analysis skill
       +--> Cache < 90      --> Prioritize Step 5 (I/O), then pg-io-deep-dive skill
       +--> Locks < 90      --> Prioritize Step 2 + Step 3, then pg-lock-diagnosis skill
       +--> Wraparound < 90 --> URGENT: check pg-vacuum-tuning skill
       +--> Checkpoints < 90 --> Prioritize Step 5, then pg-config-tuning skill
       +--> All >= 90       --> Summary report, suggest pg-performance-baseline
```

## Health Check Workflow

### Step 1: Overall Health Assessment

```
Tool: check_database_health
Parameters:
  include_recommendations: true
  verbose: true
```

**What this checks (8 health dimensions):**
1. **Connections**: Usage ratio vs `max_connections`
2. **Cache hit ratio**: `shared_buffers` effectiveness
3. **Lock contention**: Blocking locks and deadlocks
4. **Replication**: Lag and slot status
5. **Transaction wraparound**: XID age approaching limits
6. **Disk usage**: Tablespace utilization
7. **Background writer**: Checkpoint and bgwriter efficiency
8. **Checkpoint performance**: Timing and frequency

**Interpret the scores:**
- **90-100 (Healthy)**: No immediate action needed
- **70-89 (Warning)**: Issues that should be addressed soon
- **< 70 (Critical)**: Immediate attention required

Record the overall score and any dimensions scoring below 90.

### Step 2: Active Query Analysis

```
Tool: get_active_queries
Parameters:
  include_idle: true          # Include idle-in-transaction
  include_system: false
  min_duration_seconds: 0     # Show all active queries
```

**What to look for:**

| Condition | Severity | Action |
|-----------|----------|--------|
| Query running > 5 minutes | Warning | Investigate the query, may need optimization |
| Query running > 30 minutes | Critical | Consider canceling unless it's a known batch job |
| idle in transaction > 60s | Warning | Application not closing transactions properly |
| idle in transaction > 5 min | Critical | Leaking connections, blocks vacuum |
| Blocked queries waiting on locks | Critical | Identify the blocker and resolve |
| Connection count > 80% of max | Warning | Add connection pooling (PgBouncer) |

### Step 3: Wait Event Analysis

```
Tool: analyze_wait_events
Parameters:
  active_only: true
```

**Interpret wait event types:**

| Wait Type | Meaning | Common Solutions |
|-----------|---------|-----------------|
| **Lock** | Row-level or table-level contention | Optimize transactions, reduce lock duration |
| **IO/DataFileRead** | Reading data from disk | Increase `shared_buffers`, add RAM, or optimize queries |
| **IO/WALWrite** | WAL write bottleneck | Use faster disk for pg_wal, tune `wal_buffers` |
| **BufferPin** | Buffer contention | Reduce concurrent access to hot pages |
| **LWLock/buffer_mapping** | Shared buffer contention | Increase `shared_buffers` or reduce concurrent access |
| **Client/ClientRead** | Waiting for client response | Network latency or slow application |
| **Activity/WalSenderMain** | Replication sender idle | Normal for streaming replication |

### Step 4: Configuration Review

```
Tool: review_settings
Parameters:
  category: "all"
  include_all_settings: false
```

**Key settings to validate:**

| Setting | Rule of Thumb | Red Flag |
|---------|--------------|----------|
| `shared_buffers` | 25% of RAM | < 128 MB |
| `effective_cache_size` | 50-75% of RAM | < 1 GB |
| `work_mem` | 32-256 MB depending on `max_connections` | < 4 MB |
| `maintenance_work_mem` | 256 MB - 2 GB | < 64 MB |
| `checkpoint_completion_target` | 0.9 | < 0.7 |
| `random_page_cost` | 1.1 for SSD, 4.0 for HDD | Default 4.0 on SSD |
| `effective_io_concurrency` | 200 for SSD, 2 for HDD | Default 1 on SSD |
| `max_connections` | Based on actual need | > 200 without pooler |
| `autovacuum` | on | off |
| `wal_level` | replica or logical | minimal (can't do replication) |
| `max_wal_size` | 2-8 GB | < 1 GB (frequent checkpoints) |

### Step 5: Disk I/O Assessment (If health score < 90 or IO waits detected)

Use targeted analysis types based on which health dimension scored low:

For cache-related issues:
```
Tool: analyze_disk_io_patterns
Parameters:
  analysis_type: "buffer_pool"
  top_n: 20
```

For checkpoint-related issues:
```
Tool: analyze_disk_io_patterns
Parameters:
  analysis_type: "checkpoints"
  top_n: 10
```

For a comprehensive I/O overview:
```
Tool: analyze_disk_io_patterns
Parameters:
  analysis_type: "all"
  top_n: 20
```

**What to look for:**
- Buffer pool hit ratio < 99% for OLTP workloads
- Tables with high `heap_blks_read` relative to `heap_blks_hit`
- Excessive temp file usage (queries spilling to disk)
- Backend fsync count > 0 (not all writes going through checkpointer)
- Requested checkpoints > timed checkpoints (increase `max_wal_size`)

### Step 6: Quick Bloat Scan (If vacuum metrics are concerning)

```
Tool: get_bloat_summary
Parameters:
  schema_name: "public"
  top_n: 10
```

**What to look for:**
- Tables with dead tuple ratio > 10%
- Tables approaching transaction wraparound
- Large tables that haven't been vacuumed recently

## Output Format

### Health Report Card

| Dimension | Score | Status | Key Finding |
|-----------|-------|--------|-------------|
| Connections | 95 | Healthy | 23/100 connections used |
| Cache | 99.8 | Healthy | Cache hit ratio: 99.8% |
| Locks | 85 | Warning | 2 blocking locks detected |
| Replication | 100 | Healthy | No replication configured |
| Wraparound | 92 | Healthy | Oldest XID age: 450M |
| Disk | 78 | Warning | /data at 82% capacity |
| Background Writer | 88 | Warning | High buffer allocation rate |
| Checkpoints | 65 | Critical | 40% requested checkpoints |
| **Overall** | **88** | **Warning** | |

### Critical Issues (Fix Now)

Numbered list with specific remediation steps and SQL commands.

### Warnings (Fix Soon)

Issues that should be addressed within days/weeks.

### Observations (Monitor)

Things that are currently OK but worth watching.

### Recommended Configuration Changes

```sql
-- Present as ALTER SYSTEM commands with explanations
ALTER SYSTEM SET shared_buffers = '4GB';        -- Currently 128MB, server has 16GB RAM
ALTER SYSTEM SET max_wal_size = '4GB';           -- Reduce checkpoint frequency
-- Requires restart: shared_buffers
-- Reload only: max_wal_size (SELECT pg_reload_conf();)
```

## Health Check Cheatsheet

### Quick Triage Priority
1. **Connection saturation** > 90%: Immediate risk of outage
2. **Transaction wraparound** XID age > 1 billion: Risk of forced shutdown
3. **Blocking locks** > 30 seconds: Active user impact
4. **Long idle-in-transaction**: Blocks autovacuum, causes bloat
5. **Cache hit ratio** < 95%: Widespread performance degradation
6. **Checkpoint frequency**: Too many requested checkpoints waste I/O

### When to Escalate
- XID age > 1.5 billion: Emergency, run manual VACUUM FREEZE
- Connections at 100%: Emergency, consider `pg_terminate_backend()` for idle connections
- Replication lag growing continuously: Replica may never catch up
- OOM killer active: Reduce `shared_buffers` or `work_mem`

## Important Notes

- The health check is read-only and safe to run on production databases
- Health scores are relative: a score of 85 is acceptable for many workloads
- Run health checks regularly (weekly) to detect trends
- Compare results against previous baselines for meaningful trending
- Some settings require a PostgreSQL restart to take effect (marked with `(restart)` in `pg_settings`)

## Iterative Verification

After applying remediation for any health dimension:

1. Read `pgtuner://health/{check_type}` for the specific dimension to verify improvement
2. For settings changes that require reload: verify with `SHOW setting_name` after `pg_reload_conf()`
3. For lock contention fixes: re-run `get_active_queries` with `include_idle: true` to confirm

## When to Stop and Ask the User

- **If all dimensions score > 90**: "Your database is healthy. Would you like me to generate a baseline report for future comparison?"
- **If wraparound risk is detected (XID age > 1 billion)**: "URGENT: Transaction wraparound risk detected. This requires immediate VACUUM FREEZE. Do you want me to provide the emergency procedure?"
- **If configuration changes require restart**: "The recommended `shared_buffers` change requires a PostgreSQL restart. When is your next maintenance window?"
- **If the issue is outside database scope** (e.g., network, application): "The database health metrics look fine. The performance issue may be in the application layer or infrastructure."

## Cross-References to Other Skills

- **Deep connection analysis**: Use `pg-connection-analysis` skill
- **Lock contention investigation**: Use `pg-lock-diagnosis` skill
- **I/O deep-dive**: Use `pg-io-deep-dive` skill
- **Configuration tuning**: Use `pg-config-tuning` skill
- **Vacuum/wraparound issues**: Use `pg-vacuum-tuning` skill
- **Bloat detected**: Use `pg-bloat-analysis` skill
- **Establishing a baseline**: Use `pg-performance-baseline` skill

## PostgreSQL Version Notes

| Feature | Version |
|---------|---------|
| `pg_stat_io` (detailed I/O stats) | PG16+ |
| `pg_stat_wal` (WAL statistics) | PG14+ |
| `pg_stat_progress_vacuum` | PG9.6+ |
| `idle_in_transaction_session_timeout` | PG9.6+ |
| `client_connection_check_interval` | PG14+ |

## Production Safety

- All tools used in this workflow are read-only
- No data or configuration is modified
- Health checks can be run during peak traffic without impact
