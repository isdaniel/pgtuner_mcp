# PostgreSQL Performance Tuning MCP

[![PyPI - Version](https://img.shields.io/pypi/v/pgtuner-mcp)](https://pypi.org/project/pgtuner-mcp/)
[![PyPI - Downloads](https://img.shields.io/pypi/dm/pgtuner-mcp)](https://pypi.org/project/pgtuner-mcp/)
[![Python 3.10+](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org/downloads/)
[![Pepy Total Downloads](https://img.shields.io/pepy/dt/pgtuner-mcp)](https://pypi.org/project/pgtuner-mcp/)
[![Docker Pulls](https://img.shields.io/docker/pulls/dog830228/pgtuner_mcp)](https://hub.docker.com/r/dog830228/pgtuner_mcp)

<a href="https://glama.ai/mcp/servers/@isdaniel/pgtuner-mcp">
  <img width="380" height="200" src="https://glama.ai/mcp/servers/@isdaniel/pgtuner-mcp/badge" />
</a>

A Model Context Protocol (MCP) server that provides AI-powered PostgreSQL performance tuning capabilities. This server helps identify slow queries, recommend optimal indexes, analyze execution plans, and leverage HypoPG for hypothetical index testing.

## Features

### Query Analysis
- Retrieve slow queries from `pg_stat_statements` with detailed statistics
- Analyze query execution plans with `EXPLAIN` and `EXPLAIN ANALYZE`
- Identify performance bottlenecks with automated plan analysis
- Monitor active queries and detect long-running transactions

### Index Tuning
- AI-powered index recommendations based on query workload analysis
- Hypothetical index testing with **HypoPG** extension (no disk usage)
- Find unused and duplicate indexes for cleanup
- Estimate index sizes before creation
- Test query plans with proposed indexes before implementing

### Database Health
- Comprehensive health scoring with multiple checks
- Connection utilization monitoring
- Cache hit ratio analysis (buffer and index)
- Lock contention detection
- Vacuum health and transaction ID wraparound monitoring
- Replication lag monitoring
- Background writer and checkpoint analysis

### Vacuum Monitoring
- Track long-running VACUUM and VACUUM FULL operations in real-time
- Monitor autovacuum progress and performance
- Identify tables that need vacuuming
- View recent vacuum activity history
- Analyze autovacuum configuration effectiveness

### I/O Performance Analysis
- Analyze disk read/write patterns across tables and indexes
- Identify I/O bottlenecks and hot tables
- Monitor buffer cache hit ratios
- Track temporary file usage indicating work_mem issues
- Analyze checkpoint and background writer I/O
- PostgreSQL 16+ enhanced pg_stat_io metrics support

### Configuration Analysis
- Review PostgreSQL settings by category
- Get recommendations for memory, checkpoint, WAL, autovacuum, and connection settings
- Identify suboptimal configurations

### MCP Prompts & Resources
- Pre-defined prompt templates for common tuning workflows
- Dynamic resources for table stats, index info, and health checks
- Comprehensive documentation resources

## Installation

### Standard Installation (for MCP clients like Claude Desktop)

```bash
pip install pgtuner_mcp
```

Or using `uv`:

```bash
uv pip install pgtuner_mcp
```

### Manual Installation

```bash
git clone https://github.com/isdaniel/pgtuner_mcp.git
cd pgtuner_mcp
pip install -e .
```

## Configuration

### Environment Variables

| Variable | Description | Required |
|----------|-------------|----------|
| `DATABASE_URI` | PostgreSQL connection string | Yes |
| `PGTUNER_EXCLUDE_USERIDS` | Comma-separated list of user IDs (OIDs) to exclude from monitoring | No |

**Connection String Format:** `postgresql://user:password@host:port/database`

### Minimal User Permissions

To run this MCP server, the PostgreSQL user requires specific permissions to query system catalogs and extensions. Below are the minimal permissions needed for different feature sets.

#### Basic Permissions (Required for Core Functionality)

```sql
-- Create a dedicated monitoring user
CREATE USER pgtuner_monitor WITH PASSWORD 'secure_password';

-- Grant connection to the target database
GRANT CONNECT ON DATABASE your_database TO pgtuner_monitor;

-- Grant usage on schemas
GRANT USAGE ON SCHEMA public TO pgtuner_monitor;
GRANT USAGE ON SCHEMA pg_catalog TO pgtuner_monitor;

-- Grant SELECT on user tables and indexes (for table stats and analysis)
GRANT SELECT ON ALL TABLES IN SCHEMA public TO pgtuner_monitor;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO pgtuner_monitor;

-- Grant access to system catalog views (read-only)
GRANT pg_read_all_stats TO pgtuner_monitor;  -- PostgreSQL 10+
```

#### Extension-Specific Permissions

**For pgstattuple (Bloat Detection):**

```sql
-- Create the extension (requires superuser or appropriate privileges)
CREATE EXTENSION IF NOT EXISTS pgstattuple;

-- Grant execution on pgstattuple functions
GRANT EXECUTE ON FUNCTION pgstattuple(regclass) TO pgtuner_monitor;
GRANT EXECUTE ON FUNCTION pgstattuple_approx(regclass) TO pgtuner_monitor;
GRANT EXECUTE ON FUNCTION pgstatindex(regclass) TO pgtuner_monitor;
GRANT EXECUTE ON FUNCTION pgstatginindex(regclass) TO pgtuner_monitor;
GRANT EXECUTE ON FUNCTION pgstathashindex(regclass) TO pgtuner_monitor;

-- Alternative: Use pg_stat_scan_tables role (PostgreSQL 14+)
GRANT pg_stat_scan_tables TO pgtuner_monitor;
```

**For HypoPG (Hypothetical Index Testing):**

```sql
-- Create the extension (requires superuser or appropriate privileges)
CREATE EXTENSION IF NOT EXISTS hypopg;

-- Grant SELECT on HypoPG views
GRANT SELECT ON hypopg_list_indexes TO pgtuner_monitor;
GRANT SELECT ON hypopg_hidden_indexes TO pgtuner_monitor;

-- Grant execution on HypoPG functions with proper signatures
GRANT EXECUTE ON FUNCTION hypopg_create_index(text) TO pgtuner_monitor;
GRANT EXECUTE ON FUNCTION hypopg_drop_index(oid) TO pgtuner_monitor;
GRANT EXECUTE ON FUNCTION hypopg_reset() TO pgtuner_monitor;
GRANT EXECUTE ON FUNCTION hypopg_hide_index(oid) TO pgtuner_monitor;
GRANT EXECUTE ON FUNCTION hypopg_unhide_index(oid) TO pgtuner_monitor;
GRANT EXECUTE ON FUNCTION hypopg_relation_size(oid) TO pgtuner_monitor;

-- Note: HypoPG operations are session-scoped and don't affect the actual database
```

#### Complete Setup Script

```sql
-- 1. Create the monitoring user
CREATE USER pgtuner_monitor WITH PASSWORD 'secure_password';

-- 2. Grant connection and schema access
GRANT CONNECT ON DATABASE your_database TO pgtuner_monitor;
GRANT USAGE ON SCHEMA public TO pgtuner_monitor;

-- 3. Grant read access to user tables
GRANT SELECT ON ALL TABLES IN SCHEMA public TO pgtuner_monitor;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO pgtuner_monitor;

-- 4. Grant system statistics access
GRANT pg_read_all_stats TO pgtuner_monitor;  -- PostgreSQL 10+

-- Grant access to pg_stat_statements views explicitly
GRANT SELECT ON pg_stat_statements TO pgtuner_monitor;
GRANT SELECT ON pg_stat_statements_info TO pgtuner_monitor;

-- 5. Install and grant access to extensions (as superuser)
-- pg_stat_statements (required)
CREATE EXTENSION IF NOT EXISTS pg_stat_statements;

-- pgstattuple (for bloat detection)
CREATE EXTENSION IF NOT EXISTS pgstattuple;
GRANT pg_stat_scan_tables TO pgtuner_monitor;  -- PostgreSQL 14+
-- OR grant individual functions:
-- GRANT EXECUTE ON FUNCTION pgstattuple(regclass) TO pgtuner_monitor;
-- GRANT EXECUTE ON FUNCTION pgstattuple_approx(regclass) TO pgtuner_monitor;
-- GRANT EXECUTE ON FUNCTION pgstatindex(regclass) TO pgtuner_monitor;

-- hypopg (for hypothetical index testing)
CREATE EXTENSION IF NOT EXISTS hypopg;
GRANT SELECT ON hypopg_list_indexes TO pgtuner_monitor;
GRANT SELECT ON hypopg_hidden_indexes TO pgtuner_monitor;
GRANT EXECUTE ON FUNCTION hypopg_create_index(text) TO pgtuner_monitor;
GRANT EXECUTE ON FUNCTION hypopg_drop_index(oid) TO pgtuner_monitor;
GRANT EXECUTE ON FUNCTION hypopg_reset() TO pgtuner_monitor;
GRANT EXECUTE ON FUNCTION hypopg_hide_index(oid) TO pgtuner_monitor;
GRANT EXECUTE ON FUNCTION hypopg_unhide_index(oid) TO pgtuner_monitor;
GRANT EXECUTE ON FUNCTION hypopg_relation_size(oid) TO pgtuner_monitor;

-- 6. Verify permissions
SET ROLE pgtuner_monitor;
SELECT * FROM pg_stat_statements LIMIT 1;
SELECT * FROM pg_stat_activity WHERE pid = pg_backend_pid();
SELECT * FROM pgstattuple('pg_class') LIMIT 1;
SELECT * FROM hypopg_list_indexes();
RESET ROLE;
```

### Excluding Specific Users from Monitoring

You can exclude specific PostgreSQL users from being included in query analysis and monitoring results. This is useful for filtering out:
- Monitoring or replication users
- System accounts
- Internal application service accounts

Set the `PGTUNER_EXCLUDE_USERIDS` environment variable with a comma-separated list of user OIDs:

```bash
# Exclude user IDs 16384, 16385, and 16386
export PGTUNER_EXCLUDE_USERIDS="16384,16385,16386"
```

To find the OID for a specific PostgreSQL user:

```sql
SELECT usesysid, usename FROM pg_user WHERE usename = 'monitoring_user';
```

When configured, the following queries are filtered:
- `pg_stat_activity` queries (filters on `usesysid` column)
- `pg_stat_statements` queries (filters on `userid` column)

This affects tools like `get_slow_queries`, `get_active_queries`, `analyze_wait_events`, `check_database_health`, and `get_index_recommendations`.

### MCP Client Configuration

Add to your `cline_mcp_settings.json` or Claude Desktop config:

```json
{
  "mcpServers": {
    "pgtuner_mcp": {
      "command": "python",
      "args": ["-m", "pgtuner_mcp"],
      "env": {
        "DATABASE_URI": "postgresql://user:password@localhost:5432/mydb"
      },
      "disabled": false,
      "autoApprove": []
    }
  }
}
```

Or Streamable HTTP Mode

```json
{
  "mcpServers": {
    "pgtuner_mcp": {
      "type": "http",
      "url": "http://localhost:8080/mcp"
    }
  }
}
```

## Server Modes

### 1. Standard MCP Mode (Default)

```bash
# Default mode (stdio)
python -m pgtuner_mcp

# Explicitly specify stdio mode
python -m pgtuner_mcp --mode stdio
```

### 2. HTTP SSE Mode (Legacy Web Applications)

The SSE (Server-Sent Events) mode provides a web-based transport for MCP communication. It's useful for web applications and clients that need HTTP-based communication.

```bash
# Start SSE server on default host/port (0.0.0.0:8080)
python -m pgtuner_mcp --mode sse

# Specify custom host and port
python -m pgtuner_mcp --mode sse --host localhost --port 3000

# Enable debug mode
python -m pgtuner_mcp --mode sse --debug
```

**SSE Endpoints:**

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/sse` | GET | SSE connection endpoint - clients connect here to receive server events |
| `/messages` | POST | Send messages/requests to the server |

**MCP Client Configuration for SSE:**

For MCP clients that support SSE transport (like Claude Desktop or custom clients):

```json
{
  "mcpServers": {
    "pgtuner_mcp": {
      "type": "sse",
      "url": "http://localhost:8080/sse"
    }
  }
}
```

### 3. Streamable HTTP Mode (Modern MCP Protocol - Recommended)

The streamable-http mode implements the modern MCP Streamable HTTP protocol with a single `/mcp` endpoint. It supports both stateful (session-based) and stateless modes.

```bash
# Start Streamable HTTP server in stateful mode (default)
python -m pgtuner_mcp --mode streamable-http

# Start in stateless mode (fresh transport per request)
python -m pgtuner_mcp --mode streamable-http --stateless

# Specify custom host and port
python -m pgtuner_mcp --mode streamable-http --host localhost --port 8080

# Enable debug mode
python -m pgtuner_mcp --mode streamable-http --debug
```

**Stateful vs Stateless:**
- **Stateful (default)**: Maintains session state across requests using `mcp-session-id` header. Ideal for long-running interactions.
- **Stateless**: Creates a fresh transport for each request with no session tracking. Ideal for serverless deployments or simple request/response patterns.

**Endpoint:** `http://{host}:{port}/mcp`

## Available Tools

> **Note**: All tools focus exclusively on user/application tables and indexes. System catalog tables (`pg_catalog`, `information_schema`, `pg_toast`) are automatically excluded from all analyses.

### Performance Analysis Tools

| Tool | Description |
|------|-------------|
| `get_slow_queries` | Retrieve slow queries from pg_stat_statements with detailed stats (total time, mean time, calls, cache hit ratio). Excludes system catalog queries. |
| `analyze_query` | Analyze a query's execution plan with EXPLAIN ANALYZE, including automated issue detection |
| `get_table_stats` | Get detailed table statistics including size, row counts, dead tuples, and access patterns |
| `analyze_disk_io_patterns` | Analyze disk I/O read/write patterns, identify hot tables, buffer cache efficiency, and I/O bottlenecks. Supports filtering by analysis type (all, buffer_pool, tables, indexes, temp_files, checkpoints). |

### Index Tuning Tools

| Tool | Description |
|------|-------------|
| `get_index_recommendations` | AI-powered index recommendations based on query workload analysis |
| `explain_with_indexes` | Run EXPLAIN with hypothetical indexes to test improvements without creating real indexes |
| `manage_hypothetical_indexes` | Create, list, drop, or reset HypoPG hypothetical indexes. Supports hide/unhide existing indexes. |
| `find_unused_indexes` | Find unused and duplicate indexes that can be safely dropped |

### Database Health Tools

| Tool | Description |
|------|-------------|
| `check_database_health` | Comprehensive health check with scoring (connections, cache, locks, replication, wraparound, disk, checkpoints) |
| `get_active_queries` | Monitor active queries, find long-running transactions and blocked queries. By default excludes system processes. |
| `analyze_wait_events` | Analyze wait events to identify I/O, lock, or CPU bottlenecks. Focuses on client backend processes. |
| `review_settings` | Review PostgreSQL settings by category with optimization recommendations |

### Bloat Detection Tools (pgstattuple)

| Tool | Description |
|------|-------------|
| `analyze_table_bloat` | Analyze table bloat using pgstattuple extension. Shows dead tuple counts, free space, and wasted space percentage. |
| `analyze_index_bloat` | Analyze B-tree index bloat using pgstatindex. Shows leaf density, fragmentation, and empty/deleted pages. Also supports GIN and Hash indexes. |
| `get_bloat_summary` | Get a comprehensive overview of database bloat with top bloated tables/indexes, total reclaimable space, and priority maintenance actions. |

### Vacuum Monitoring Tools

| Tool | Description |
|------|-------------|
| `monitor_vacuum_progress` | Track manual VACUUM, VACUUM FULL, and autovacuum operations. Monitor progress percentage, dead tuples collected, index vacuum rounds, and estimated time remaining. Includes autovacuum configuration review and tables needing maintenance. |

### Tool Parameters

#### get_slow_queries
- `limit`: Maximum queries to return (default: 10)
- `min_calls`: Minimum call count filter (default: 1)
- `min_mean_time_ms`: Minimum mean (average) execution time in milliseconds filter
- `order_by`: Sort by `mean_time`, `calls`, or `rows`

#### analyze_query
- `query` (required): SQL query to analyze
- `analyze`: Execute query with EXPLAIN ANALYZE (default: true)
- `buffers`: Include buffer statistics (default: true)
- `format`: Output format - `json`, `text`, `yaml`, `xml`

#### get_index_recommendations
- `workload_queries`: Optional list of specific queries to analyze
- `max_recommendations`: Maximum recommendations (default: 10)
- `min_improvement_percent`: Minimum improvement threshold (default: 10%)
- `include_hypothetical_testing`: Test with HypoPG (default: true)
- `target_tables`: Focus on specific tables

#### check_database_health
- `include_recommendations`: Include actionable recommendations (default: true)
- `verbose`: Include detailed statistics (default: false)

#### analyze_table_bloat
- `table_name`: Name of a specific table to analyze (optional)
- `schema_name`: Schema name (default: `public`)
- `use_approx`: Use `pgstattuple_approx` for faster analysis on large tables (default: false)
- `min_table_size_gb`: Minimum table size in GB to include in schema-wide scan (default: 5)
- `include_toast`: Include TOAST table analysis (default: false)

#### analyze_index_bloat
- `index_name`: Name of a specific index to analyze (optional)
- `table_name`: Analyze all indexes on this table (optional)
- `schema_name`: Schema name (default: `public`)
- `min_index_size_gb`: Minimum index size in GB to include (default: 5)
- `min_bloat_percent`: Only show indexes with bloat above this percentage (default: 20)

#### get_bloat_summary
- `schema_name`: Schema to analyze (default: `public`)
- `top_n`: Number of top bloated objects to show (default: 10)
- `min_size_gb`: Minimum object size in GB to include (default: 5)

#### monitor_vacuum_progress
- `action`: Action to perform - `progress` (monitor active vacuum operations), `needs_vacuum` (find tables needing vacuum), `autovacuum_status` (review autovacuum configuration), or `recent_activity` (view recent vacuum history)
- `schema_name`: Schema to analyze (default: `public`, used with `needs_vacuum` action)
- `top_n`: Number of results to return (default: 20)

#### analyze_disk_io_patterns
- `analysis_type`: Type of I/O analysis - `all` (comprehensive), `buffer_pool` (cache hit ratios), `tables` (table I/O patterns), `indexes` (index I/O patterns), `temp_files` (temporary file usage), or `checkpoints` (checkpoint I/O statistics)
- `schema_name`: Schema to analyze (default: `public`)
- `top_n`: Number of top I/O-intensive objects to show (default: 20)
- `min_size_gb`: Minimum object size in GB to include (default: 1)

## MCP Prompts

The server includes pre-defined prompt templates for guided tuning sessions:

| Prompt | Description |
|--------|-------------|
| `diagnose_slow_queries` | Systematic slow query investigation workflow |
| `index_optimization` | Comprehensive index analysis and cleanup |
| `health_check` | Full database health assessment |
| `query_tuning` | Optimize a specific SQL query |
| `performance_baseline` | Generate a baseline report for comparison |

## MCP Resources

### Static Resources
- `pgtuner://docs/tools` - Complete tool documentation
- `pgtuner://docs/workflows` - Common tuning workflows guide
- `pgtuner://docs/prompts` - Prompt template documentation

### Dynamic Resource Templates
- `pgtuner://table/{schema}/{table_name}/stats` - Table statistics
- `pgtuner://table/{schema}/{table_name}/indexes` - Table index information
- `pgtuner://query/{query_hash}/stats` - Query performance statistics
- `pgtuner://settings/{category}` - PostgreSQL settings (memory, checkpoint, wal, autovacuum, connections, all)
- `pgtuner://health/{check_type}` - Health checks (connections, cache, locks, replication, bloat, all)

## PostgreSQL Extension Setup

### HypoPG Extension

HypoPG enables testing indexes without actually creating them. This is extremely useful for:
- Testing if a proposed index would be used by the query planner
- Comparing execution plans with different index strategies
- Estimating storage requirements before committing

#### Enable HypoPG in Database

HypoPG enables testing hypothetical indexes without creating them on disk.

```sql
-- Create the extension
CREATE EXTENSION IF NOT EXISTS hypopg;

-- Verify installation
SELECT * FROM hypopg_list_indexes();
```

### pg_stat_statements Extension

The `pg_stat_statements` extension is **required** for query performance analysis. It tracks planning and execution statistics for all SQL statements executed by a server.

#### Step 1: Enable the Extension in postgresql.conf

Add the following to your `postgresql.conf` file:

```ini
# Required: Load pg_stat_statements module
shared_preload_libraries = 'pg_stat_statements'

# Required: Enable query identifier computation
compute_query_id = on

# Maximum number of statements tracked (default: 5000)
pg_stat_statements.max = 10000

# Track all statements including nested ones (default: top)
# Options: top, all, none
pg_stat_statements.track = top

# Track utility commands like CREATE, ALTER, DROP (default: on)
pg_stat_statements.track_utility = on
```

> **Note**: After modifying `shared_preload_libraries`, a PostgreSQL server **restart** is required.

#### Step 2: Create the Extension in Your Database

```sql
-- Connect to your database and create the extension
CREATE EXTENSION IF NOT EXISTS pg_stat_statements;

-- Verify installation
SELECT * FROM pg_stat_statements LIMIT 1;
```

### pgstattuple Extension

The `pgstattuple` extension is **required** for bloat detection tools (`analyze_table_bloat`, `analyze_index_bloat`, `get_bloat_summary`). It provides functions to get tuple-level statistics for tables and indexes.

```sql
-- Create the extension
CREATE EXTENSION IF NOT EXISTS pgstattuple;

-- Verify installation
SELECT * FROM pgstattuple('pg_class') LIMIT 1;
```

### Performance Impact Considerations

| Setting | Overhead | Recommendation |
|---------|----------|----------------|
| `pg_stat_statements` | Low (~1-2%) | **Always enable** |
| `track_io_timing` | Low-Medium (~2-5%) | Enable in production, test first |
| `track_functions = all` | Low | Enable for function-heavy workloads |
| `pg_stat_statements.track_planning` | Medium | Enable only when investigating planning issues |
| `log_min_duration_statement` | Low | Recommended for slow query identification |

> **Tip**: Use `pg_test_timing` to measure the timing overhead on your specific system before enabling `track_io_timing`.

## Example Usage

### Find and Analyze Slow Queries

```python
# Get top 10 slowest queries
slow_queries = await get_slow_queries(limit=10, order_by="total_time")

# Analyze a specific query's execution plan
analysis = await analyze_query(
    query="SELECT * FROM orders WHERE user_id = 123",
    analyze=True,
    buffers=True
)
```

### Get Index Recommendations

```python
# Analyze workload and get recommendations
recommendations = await get_index_recommendations(
    max_recommendations=5,
    min_improvement_percent=20,
    include_hypothetical_testing=True
)

# Recommendations include CREATE INDEX statements
for rec in recommendations["recommendations"]:
    print(rec["create_statement"])
```

### Database Health Check

```python
# Run comprehensive health check
health = await check_database_health(
    include_recommendations=True,
    verbose=True
)

print(f"Health Score: {health['overall_score']}/100")
print(f"Status: {health['status']}")

# Review specific areas
for issue in health["issues"]:
    print(f"{issue}")
```

### Find Unused Indexes

```python
# Find indexes that can be dropped
unused = await find_unused_indexes(
    schema_name="public",
    include_duplicates=True
)

# Get DROP statements
for stmt in unused["recommendations"]:
    print(stmt)
```

## Docker

```bash
docker pull  dog830228/pgtuner_mcp

# Streamable HTTP mode (recommended for web applications)
docker run -p 8080:8080 \
  -e DATABASE_URI=postgresql://user:pass@host:5432/db \
  dog830228/pgtuner_mcp --mode streamable-http

# Streamable HTTP stateless mode (for serverless)
docker run -p 8080:8080 \
  -e DATABASE_URI=postgresql://user:pass@host:5432/db \
  dog830228/pgtuner_mcp --mode streamable-http --stateless

# SSE mode (legacy web applications)
docker run -p 8080:8080 \
  -e DATABASE_URI=postgresql://user:pass@host:5432/db \
  dog830228/pgtuner_mcp --mode sse

# stdio mode (for MCP clients like Claude Desktop)
docker run -i \
  -e DATABASE_URI=postgresql://user:pass@host:5432/db \
  dog830228/pgtuner_mcp --mode stdio
```

## Requirements

- **Python**: 3.10+
- **PostgreSQL**: 12+ (recommended: 14+)
- **Extensions**:
  - `pg_stat_statements` (required for query analysis)
  - `hypopg` (optional, for hypothetical index testing)

## Dependencies

Core dependencies:
- `mcp[cli]>=1.12.0` - Model Context Protocol SDK
- `psycopg[binary,pool]>=3.1.0` - PostgreSQL adapter with connection pooling
- `pglast>=7.10` - PostgreSQL query parser

Optional (for HTTP modes):
- `starlette>=0.27.0` - ASGI framework
- `uvicorn>=0.23.0` - ASGI server

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

<!-- Need to add this line for MCP registry publication -->
<!-- mcp-name: io.github.isdaniel/pgtuner_mcp -->
