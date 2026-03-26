---
name: pg-connection-analysis
description: Analyzes PostgreSQL connection utilization patterns including active, idle, and idle-in-transaction sessions. Diagnoses connection leaks, saturation, and provides PgBouncer pooling recommendations. Guides the agent to use get_active_queries with targeted filters and health resources for connection monitoring.
---

# PostgreSQL Connection Analysis

This skill guides analysis and optimization of PostgreSQL connection usage using pgtuner-mcp tools.

## When to Use This Skill

Use this skill when the user:

- Reports "too many connections" errors or connection refused
- Asks about connection pooling or PgBouncer setup
- Sees high connection counts in monitoring
- Reports idle connections consuming resources
- Asks "how many connections is my database using?" or "do I need a connection pooler?"
- Reports memory pressure related to per-connection overhead

## MCP Resources Available

- `pgtuner://health/connections` -- Quick connection utilization check (lightweight)
- `pgtuner://settings/connections` -- Connection-related configuration

## Related MCP Prompt

This skill supplements the **`health_check`** MCP Prompt's connection dimension.

## Prerequisites

- The pgtuner-mcp server must be connected with a valid `DATABASE_URI`

## Agent Decision Tree

```
What is the user's symptom?
  |
  +--> "Too many connections / connection refused"
  |    --> Start with Step 1 (Connection State Breakdown), urgent
  |
  +--> "Should I use connection pooling?"
  |    --> Start with Step 1, then Step 3 (Pooling Analysis)
  |
  +--> "Idle connections eating memory"
  |    --> Start with Step 1 (focus on idle), then Step 4 (Configuration)
  |
  +--> "General connection health question"
       --> Start with Quick Check (MCP Resource), then Step 1 if needed
```

## Connection Analysis Workflow

### Quick Check: Use MCP Resource First

Before running full tool queries, the agent can read the lightweight resource:

Read resource: `pgtuner://health/connections`

This provides the connection utilization ratio without running the full health check tool. If this shows > 80% utilization, proceed with the full workflow.

### Step 1: Connection State Breakdown

```
Tool: get_active_queries
Parameters:
  include_idle: true
  include_system: true
  min_duration_seconds: 0
```

**Categorize all connections:**

| State | Meaning | Healthy Ratio |
|-------|---------|--------------|
| `active` | Executing a query | 10-30% of total |
| `idle` | Connected but doing nothing | OK if moderate |
| `idle in transaction` | In open transaction, not executing | Should be < 5% |
| `idle in transaction (aborted)` | Transaction failed, not rolled back | Should be 0 |
| `fastpath function call` | Executing a fast-path function | Rare |
| `disabled` | track_activities is off | Should not appear |

**Agent reasoning:**

| Finding | Diagnosis | Severity |
|---------|-----------|----------|
| > 80% idle connections | Connection leak or no pooling | Warning |
| > 10% idle in transaction | Application transaction bugs | Warning-Critical |
| Active connections > 50% | Either healthy or overloaded | Check CPU |
| Total connections > 80% of max | Saturation risk | Critical |
| Connections from many unique IPs | No pooling in use | Check if pooler needed |

**For multi-database setups, filter by database:**
```
Tool: get_active_queries
Parameters:
  include_idle: true
  database: "<specific_database>"
```

### Step 2: Connection Duration Analysis

```
Tool: get_active_queries
Parameters:
  include_idle: true
  min_duration_seconds: 300
```

Long-lived connections (> 5 minutes) that are idle may indicate:
- **Connection pooler in session mode**: Normal (connections are held open)
- **No connection pooler**: Possible leak. Application should close connections when done.
- **Long idle in transaction**: Application bug. Most harmful pattern.

**Then check for truly stuck sessions:**
```
Tool: get_active_queries
Parameters:
  include_idle: true
  min_duration_seconds: 3600
```

Sessions idle for > 1 hour are almost always leaks or misconfigured poolers.

### Step 3: Connection Pooling Assessment

Evaluate whether a connection pooler is needed based on the data:

**Signs you need a connection pooler:**

| Indicator | Threshold | Why |
|-----------|-----------|-----|
| `max_connections` > 100 | Most apps | PostgreSQL per-connection overhead is ~5-10MB |
| Most connections are idle | > 60% idle | Wasting RAM on idle connections |
| Many short-lived connections | Connection churn visible | Pooler amortizes connect/disconnect overhead |
| Multiple application servers | > 3 | Each opens its own pool, multiplies connections |

**Connection pooler recommendations:**

| Scenario | PgBouncer Mode | max_connections | Pool Size |
|----------|---------------|-----------------|-----------|
| Web app, short transactions | `transaction` | 50-100 | 20-50 per app server |
| Microservices, many apps | `transaction` | 100-200 | 10-20 per service |
| App uses prepared statements | `session` | Match app pool size | Same as max_connections |
| Analytical, long queries | `session` or no pooler | As needed | N/A |

**Memory calculation:**
```
Per-connection RAM = ~10MB (shared_buffers mappings + work_mem potential + catalog cache)
100 idle connections = ~1GB wasted RAM
With PgBouncer: 20 active connections = ~200MB
Savings: ~800MB returned to OS cache (improves effective_cache_size)
```

### Step 4: Configuration Review

```
Tool: review_settings
Parameters:
  category: "connections"
  include_all_settings: false
```

**Key settings to evaluate:**

| Setting | Current | Recommendation |
|---------|---------|---------------|
| `max_connections` | Check | Set to actual need + 10% buffer. With PgBouncer: 50-100 |
| `superuser_reserved_connections` | Check | 3-5 (ensure admin can always connect) |
| `idle_in_transaction_session_timeout` | Check | 60s (kill idle transactions after 60s) |
| `statement_timeout` | Check | 30s for OLTP, 0 for batch jobs (use per-role settings) |
| `tcp_keepalives_idle` | Check | 60-120 (detect dead connections faster) |
| `tcp_keepalives_interval` | Check | 10 (retry interval for keepalives) |
| `tcp_keepalives_count` | Check | 6 (number of keepalive retries before disconnect) |
| `client_connection_check_interval` | Check (PG14+) | 5000 (check if client disconnected every 5s) |

### Step 5: Wait Events for Connection Issues

```
Tool: analyze_wait_events
Parameters:
  active_only: false
```

**Connection-related wait events:**

| Wait Event | Meaning |
|------------|---------|
| `Client:ClientRead` | Backend waiting for client to send data (slow client or network) |
| `Client:ClientWrite` | Backend waiting for client to receive data (slow client or network) |
| `IPC:ProcArrayGroupUpdate` | Contention during snapshot acquisition (too many connections) |

Many `Client:ClientRead` waits suggest network latency between application and database. Consider:
- Co-locating application and database
- Using connection pooler close to the application

## Output Format

### Connection Analysis Report

**Summary:**
| Metric | Value |
|--------|-------|
| Total connections | X / max_connections (Y%) |
| Active | X (Y%) |
| Idle | X (Y%) |
| Idle in transaction | X (Y%) |
| Idle > 1 hour | X |

**Connection State Distribution:**
```
active:                 ||||||||| 15 (15%)
idle:                   |||||||||||||||||||||||||||||||||||||||||||| 70 (70%)
idle in transaction:    |||||||||| 12 (12%)
idle in txn (aborted):  || 3 (3%)
```

**Risk Assessment:**
- Connection saturation risk: Low / Medium / High / Critical
- Memory waste from idle connections: ~X MB
- Idle-in-transaction impact: None / Minor / Significant

**Recommendations:**
1. ...
2. ...

## When to Stop and Ask the User

- **Before recommending PgBouncer**: "Is a connection pooler already in use? If so, which one and in what mode?"
- **Before recommending `max_connections` reduction**: "What is the peak connection count? Reducing max_connections during high traffic could cause connection refused errors."
- **If application-level changes are needed**: "The idle-in-transaction problem is caused by application code. The application needs to ensure transactions are committed or rolled back promptly. Would you like specific guidance for your framework?"
- **If managed database**: "Are you on a managed service (RDS, Cloud SQL)? Connection limits may be tied to instance size and not directly configurable."

## PostgreSQL Version Notes

| Feature | Version |
|---------|---------|
| `idle_in_transaction_session_timeout` | PG9.6+ |
| `client_connection_check_interval` | PG14+ |
| `log_connections` / `log_disconnections` | All versions |

## Production Safety

- All diagnostic queries are read-only
- `pg_terminate_backend()` should only be used for clearly leaked connections
- Changing `max_connections` requires a PostgreSQL restart
- `idle_in_transaction_session_timeout` is reload-only (`pg_reload_conf()`)
- TCP keepalive settings are reload-only
