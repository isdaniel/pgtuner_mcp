---
name: pg-lock-diagnosis
description: Diagnoses PostgreSQL lock contention including blocking lock chains, deadlock patterns, idle-in-transaction problems, and wait event analysis. Guides the agent through identifying blockers, understanding lock types, and recommending transaction and configuration changes.
---

# PostgreSQL Lock Contention Diagnosis

This skill guides investigation and resolution of lock contention issues in PostgreSQL using pgtuner-mcp tools.

## When to Use This Skill

Use this skill when the user:

- Reports queries being blocked or timing out
- Sees "Lock" wait events in `pg_stat_activity`
- Experiences deadlocks (logged in PostgreSQL error log)
- Reports `idle in transaction` sessions causing problems
- Asks "why are my queries waiting?" or "what is blocking this query?"
- Sees application-level timeout errors due to lock waits
- Reports slow response times during peak concurrent usage

## MCP Resources Available

- `pgtuner://health/locks` -- Quick lock health check (lightweight, targeted)
- `pgtuner://health/connections` -- Connection state overview
- `pgtuner://settings/connections` -- Connection and timeout configuration

## Related MCP Prompt

This skill extends the lock analysis portions of the **`health_check`** MCP Prompt (which checks locks at a surface level).

## Prerequisites

- The pgtuner-mcp server must be connected with a valid `DATABASE_URI`
- Access to `pg_stat_activity`, `pg_locks` system views

## Agent Decision Tree

```
What is the user's symptom?
  |
  +--> "Queries are being blocked / timing out"
  |    --> Start with Step 1 (Active Queries) to find blockers
  |
  +--> "Deadlock errors in logs"
  |    --> Start with Step 2 (Wait Events) then Step 1 with include_idle
  |
  +--> "idle in transaction problems"
  |    --> Start with Step 1 (include_idle: true, min_duration_seconds: 60)
  |
  +--> "General lock performance concerns"
  |    --> Start with Step 3 (Health Check for locks) then Step 2
  |
  +--> "Lock waits during maintenance (VACUUM, REINDEX, etc.)"
       --> Start with Step 1 then check Step 4 (Configuration)
```

## Lock Investigation Workflow

### Step 1: Identify Active Queries and Blockers

```
Tool: get_active_queries
Parameters:
  include_idle: true
  include_system: false
  min_duration_seconds: 0
```

**What to look for:**

| State | Duration | Severity | Action |
|-------|----------|----------|--------|
| `active` | < 5s | Normal | No issue |
| `active` | > 30s | Warning | Investigate the query |
| `active` | > 5 min | Critical | Consider canceling |
| `idle in transaction` | > 60s | Warning | Application not closing transactions |
| `idle in transaction` | > 5 min | Critical | Blocks vacuum and other transactions |
| `idle in transaction (aborted)` | any | Warning | Failed transaction not rolled back |

**Agent reasoning for blocking chains:**

The tool output includes information about blocked queries and their blockers. Build a blocking chain:

```
Blocker (PID: 1234, idle in transaction, started 10 min ago)
  |
  +--> Blocked (PID: 2345, waiting for RowExclusiveLock on orders)
  |
  +--> Blocked (PID: 3456, waiting for AccessShareLock on orders)
```

The **root blocker** is the session that is NOT waiting on any lock itself. This is usually the target for resolution.

**IF root blocker is `idle in transaction`:** The application opened a transaction but never committed/rolled back.

**IF root blocker is `active` with a long-running query:** The query is holding locks while executing. May need optimization.

### Step 2: Analyze Wait Events

```
Tool: analyze_wait_events
Parameters:
  active_only: false
```

Note: Using `active_only: false` to include idle-in-transaction sessions in the wait event analysis.

**Lock-related wait events:**

| Wait Event | Meaning | Common Cause |
|------------|---------|-------------|
| `Lock:relation` | Waiting for table-level lock | DDL operations, VACUUM FULL, LOCK TABLE |
| `Lock:tuple` | Waiting for row-level lock | Concurrent UPDATE/DELETE on same row |
| `Lock:transactionid` | Waiting for another transaction to complete | Transaction holding row lock hasn't committed |
| `Lock:virtualxid` | Waiting for virtual transaction ID | Usually brief, indicates snapshot conflict |
| `Lock:extend` | Waiting to extend a relation | Concurrent inserts on same table, consider fillfactor |
| `Lock:advisory` | Waiting for advisory lock | Application-level locking |

**IF many `Lock:tuple` waits:**
Multiple transactions are competing for the same rows. Recommend:
- Shorter transactions
- Access rows in consistent order to avoid deadlocks
- Consider `SELECT ... FOR UPDATE SKIP LOCKED` for queue-like patterns

**IF many `Lock:relation` waits:**
A DDL or maintenance operation is blocking normal queries:
- `VACUUM FULL` takes `AccessExclusiveLock`
- `ALTER TABLE` takes `AccessExclusiveLock`
- `CREATE INDEX` (without `CONCURRENTLY`) takes `ShareLock`

### Step 3: Targeted Lock Health Check

For a quick initial assessment, read the MCP resource first:

Read resource: `pgtuner://health/locks`

For a deeper check:
```
Tool: check_database_health
Parameters:
  include_recommendations: true
  verbose: true
```

Focus on the "locks" dimension score:
- Score >= 90: Lock contention is minimal
- Score 70-89: Some contention, investigate
- Score < 70: Significant contention, urgent

### Step 4: Review Lock-Related Configuration

```
Tool: review_settings
Parameters:
  category: "connections"
  include_all_settings: false
```

**Key lock-related settings:**

| Setting | Default | Recommended | Purpose |
|---------|---------|-------------|---------|
| `lock_timeout` | 0 (infinite) | 5s-30s | Maximum time to wait for a lock before failing |
| `deadlock_timeout` | 1s | 1s (usually fine) | Time before checking for deadlocks |
| `idle_in_transaction_session_timeout` | 0 (infinite) | 60s-300s | Auto-terminate idle-in-transaction |
| `statement_timeout` | 0 (infinite) | 30s-120s | Maximum query execution time |
| `max_locks_per_transaction` | 64 | 64 (increase if needed) | Lock table size (rarely needs changing) |

### Step 5: Correlate with Query Patterns

If specific queries are repeatedly involved in lock contention:

```
Tool: get_slow_queries
Parameters:
  limit: 20
  min_calls: 5
  order_by: "mean_time"
```

Look for UPDATE/DELETE queries with high mean time -- they may be holding locks longer than necessary.

Then analyze the lock-holding query:
```
Tool: analyze_query
Parameters:
  query: "<the lock-holding query>"
  analyze: true
  buffers: true
  verbose: false
```

IF the query is slow due to missing indexes, adding an index will reduce lock hold time (because the query completes faster).

## Common Lock Contention Patterns and Solutions

### Pattern 1: Long-Running Transaction Blocks Everything

**Symptoms:** Multiple sessions waiting on `Lock:transactionid`. One session in `idle in transaction` state for minutes.

**Root cause:** Application opens transaction, does work outside the DB (API call, file processing), then comes back to commit.

**Solution:**
```sql
-- Set idle_in_transaction timeout
ALTER SYSTEM SET idle_in_transaction_session_timeout = '60s';
SELECT pg_reload_conf();

-- To immediately terminate the idle session (use with caution):
SELECT pg_terminate_backend(<blocker_pid>);
```

### Pattern 2: Deadlocks Between Concurrent Updates

**Symptoms:** `deadlock detected` errors in PostgreSQL logs.

**Root cause:** Two transactions update the same rows in different order:
- Transaction A: UPDATE row 1, then UPDATE row 2
- Transaction B: UPDATE row 2, then UPDATE row 1

**Solution:**
- Ensure transactions always access rows in a consistent order (e.g., by primary key)
- Use `SELECT ... FOR UPDATE` to acquire locks upfront in a deterministic order
- Keep transactions as short as possible

### Pattern 3: DDL Blocking DML

**Symptoms:** `ALTER TABLE` or `CREATE INDEX` blocks all queries on the table.

**Solution:**
```sql
-- Use CONCURRENTLY for index operations
CREATE INDEX CONCURRENTLY idx_name ON table_name (column);
REINDEX INDEX CONCURRENTLY idx_name;

-- For ALTER TABLE, consider:
SET lock_timeout = '5s';  -- Fail fast if lock not acquired
ALTER TABLE ...;           -- Retry during low-traffic period
```

### Pattern 4: VACUUM FULL Blocking Queries

**Symptoms:** `VACUUM FULL` holds `AccessExclusiveLock`, blocking all access.

**Solution:**
- Use regular `VACUUM` instead (only takes `ShareUpdateExclusiveLock`, does not block reads/writes)
- Use `pg_repack` for online table rewriting without exclusive locks
- Schedule `VACUUM FULL` only during maintenance windows

### Pattern 5: Lock Queue Amplification

**Symptoms:** One DDL statement queued behind a long-running query, then all subsequent queries queue behind the DDL.

**Explanation:** PostgreSQL lock queue is FIFO. If an `ALTER TABLE` (needs `AccessExclusiveLock`) waits for a long SELECT, all new queries also wait behind the ALTER.

**Solution:**
```sql
-- Set a lock_timeout before DDL
SET lock_timeout = '3s';
ALTER TABLE ...;
-- If it fails, retry later. Don't let the DDL queue block everyone.
```

## Output Format

### Lock Contention Report

**Current Blocking Chains:**
```
Root Blocker: PID 1234 | State: idle in transaction | Duration: 12 min
  Query: BEGIN; UPDATE orders SET ... WHERE id = 42;
  |
  +--> Blocked: PID 2345 | Waiting: 3 min | Lock: RowExclusiveLock on orders
  |    Query: UPDATE orders SET status = 'shipped' WHERE id = 42;
  |
  +--> Blocked: PID 3456 | Waiting: 2 min | Lock: AccessShareLock on orders
       Query: SELECT * FROM orders WHERE id = 42;
```

**Wait Event Distribution:**

| Wait Type | Count | Top Events |
|-----------|-------|------------|
| Lock | 5 | tuple (3), transactionid (2) |
| IO | 2 | DataFileRead (2) |

**Recommendations:**
1. Immediate: Terminate PID 1234 (idle in transaction for 12 min)
2. Configuration: Set `idle_in_transaction_session_timeout = '60s'`
3. Application: Fix transaction handling in the orders update flow

## Iterative Verification

After resolving lock contention:

1. Re-run `get_active_queries` with `include_idle: true` to confirm no more blocking chains
2. Re-run `analyze_wait_events` to confirm Lock wait events have decreased
3. Monitor over the next hour to ensure the pattern doesn't recur

## When to Stop and Ask the User

- **Before terminating a backend**: Always ask: "PID XXXX is blocking N other sessions and has been idle in transaction for X minutes. Should I provide the command to terminate it?"
- **If the blocker is a known batch job**: Ask: "The blocking session appears to be a batch operation. Is this expected? Should we wait for it to complete?"
- **If lock contention is application-level**: "The lock contention is caused by application transaction patterns. This requires application code changes, not database tuning. Would you like me to describe the recommended changes?"

## PostgreSQL Version Notes

| Feature | Version |
|---------|---------|
| `idle_in_transaction_session_timeout` | PG9.6+ |
| `REINDEX CONCURRENTLY` | PG12+ |
| `Lock wait event type breakdown` | PG9.6+ (detailed wait events) |
| `pg_blocking_pids()` function | PG9.6+ |

## Production Safety

- All diagnostic tools used here are read-only
- `pg_cancel_backend()` attempts a graceful cancel (query stops, transaction stays open)
- `pg_terminate_backend()` forcefully terminates the session (use as last resort)
- Never terminate the `autovacuum` launcher unless absolutely necessary
- Setting `lock_timeout` before DDL is a safe practice to prevent queue amplification
