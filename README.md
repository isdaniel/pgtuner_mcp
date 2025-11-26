# PostgreSQL Performance Tuning MCP

[![PyPI - Version](https://img.shields.io/pypi/v/pgtuner-mcp)](https://pypi.org/project/pgtuner-mcp/)
[![PyPI - Downloads](https://img.shields.io/pypi/dm/pgtuner-mcp)](https://pypi.org/project/pgtuner-mcp/)
[![Python 3.10+](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org/downloads/)
[![Pepy Total Downloads](https://img.shields.io/pepy/dt/pgtuner-mcp)](https://pypi.org/project/pgtuner-mcp/)
[![Docker Pulls](https://img.shields.io/docker/pulls/dog830228/pgtuner_mcp)](https://hub.docker.com/r/dog830228/pgtuner)

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

**Connection String Format:** `postgresql://user:password@host:port/database`

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

```bash
# Start SSE server on default host/port (0.0.0.0:8080)
python -m pgtuner_mcp --mode sse

# Specify custom host and port
python -m pgtuner_mcp --mode sse --host localhost --port 3000

# Enable debug mode
python -m pgtuner_mcp --mode sse --debug
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

### Performance Analysis Tools

| Tool | Description |
|------|-------------|
| `get_slow_queries` | Retrieve slow queries from pg_stat_statements with detailed stats (total time, mean time, calls, cache hit ratio) |
| `analyze_query` | Analyze a query's execution plan with EXPLAIN ANALYZE, including automated issue detection |
| `get_table_stats` | Get detailed table statistics including size, row counts, dead tuples, and access patterns |

### Index Tuning Tools

| Tool | Description |
|------|-------------|
| `get_index_recommendations` | AI-powered index recommendations based on query workload analysis |
| `explain_with_indexes` | Run EXPLAIN with hypothetical indexes to test improvements without creating real indexes |
| `manage_hypothetical_indexes` | Create, list, drop, or reset HypoPG hypothetical indexes |
| `find_unused_indexes` | Find unused and duplicate indexes that can be safely dropped |

### Database Health Tools

| Tool | Description |
|------|-------------|
| `check_database_health` | Comprehensive health check with scoring (connections, cache, locks, replication, wraparound, disk, checkpoints) |
| `get_active_queries` | Monitor active queries, find long-running transactions and blocked queries |
| `analyze_wait_events` | Analyze wait events to identify I/O, lock, or CPU bottlenecks |
| `review_settings` | Review PostgreSQL settings by category with optimization recommendations |

### Tool Parameters

#### get_slow_queries
- `limit`: Maximum queries to return (default: 10)
- `min_calls`: Minimum call count filter (default: 1)
- `min_total_time_ms`: Minimum total execution time filter
- `order_by`: Sort by `total_time`, `mean_time`, `calls`, or `rows`

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
