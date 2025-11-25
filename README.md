# PostgreSQL performance tuning MCP

[![PyPI - Version](https://img.shields.io/pypi/v/pgtuner-mcp)](https://pypi.org/project/pgtuner-mcp/)
[![PyPI - Downloads](https://img.shields.io/pypi/dm/pgtuner-mcp)](https://pypi.org/project/pgtuner-mcp/)


<a href="https://glama.ai/mcp/servers/@isdaniel/pgtuner-mcp">
  <img width="380" height="200" src="https://glama.ai/mcp/servers/@isdaniel/pgtuner-mcp/badge" />
</a>

A Model Context Protocol (MCP) server that provides AI-powered PostgreSQL performance tuning capabilities. This server helps identify slow queries, recommend optimal indexes, analyze execution plans, and leverage HypoPG for hypothetical index testing.

## Features

### Query Analysis
- Get top resource-consuming queries from `pg_stat_statements`
- Analyze query execution plans with `EXPLAIN` and `EXPLAIN ANALYZE`
- Identify slow queries and bottlenecks

### Index Tuning
- Smart index recommendations based on query workload
- Hypothetical index testing with **HypoPG** extension
- Index health analysis (duplicate, unused, bloated indexes)
- Estimate index size before creation

### Database Health
- Connection utilization monitoring
- Vacuum health and transaction ID wraparound checks
- Replication lag monitoring
- Buffer cache hit rate analysis
- Sequence limit warnings

### HypoPG Integration
When the HypoPG extension is available, the server can:
- Create hypothetical indexes without actual disk usage
- Test how PostgreSQL would use potential indexes
- Compare query plans with and without proposed indexes
- Hide existing indexes to test removal impact

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
git clone https://github.com/example/pgtuner_mcp.git
cd pgtuner_mcp
pip install -e .
```

## Configuration

### Environment Variables

- `DATABASE_URI`: PostgreSQL connection string (required)
  - Format: `postgresql://user:password@host:port/database`

### MCP Client Configuration

Add to your `cline_mcp_settings.json`:

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

### Query Analysis Tools

1. **`get_top_queries`** - Get the slowest or most resource-intensive queries
   - Parameters: `sort_by` (total_time, mean_time, resources), `limit`

2. **`explain_query`** - Explain the execution plan for a SQL query
   - Parameters: `sql`, `analyze` (boolean), `hypothetical_indexes` (optional)

### Index Tuning Tools

3. **`analyze_workload_indexes`** - Analyze frequently executed queries and recommend optimal indexes
   - Parameters: `max_index_size_mb`, `method` (dta, greedy)

4. **`analyze_query_indexes`** - Analyze specific SQL queries and recommend indexes
   - Parameters: `queries` (list), `max_index_size_mb`

5. **`get_index_recommendations`** - Get index recommendations for a single query
   - Parameters: `query`, `max_recommendations`

6. **`test_hypothetical_index`** - Test how a hypothetical index would affect query performance
   - Parameters: `table`, `columns`, `query`, `using` (btree, hash, etc.)

7. **`list_hypothetical_indexes`** - List all current hypothetical indexes

8. **`reset_hypothetical_indexes`** - Remove all hypothetical indexes

### Database Health Tools

9. **`analyze_db_health`** - Comprehensive database health analysis
   - Parameters: `health_type` (index, connection, vacuum, sequence, replication, buffer, constraint, all)

10. **`get_index_health`** - Analyze index health (duplicate, unused, bloated)

### Utility Tools

11. **`execute_sql`** - Execute a SQL query (respects access mode)
    - Parameters: `sql`

12. **`list_schemas`** - List all schemas in the database

13. **`get_table_info`** - Get detailed information about a table
    - Parameters: `schema`, `table`

## HypoPG Extension

#### Enable in Database
```sql
CREATE EXTENSION hypopg;
```

## Example Usage

### Find Slow Queries

```python
# Get top 10 resource-consuming queries
result = await get_top_queries(sort_by="resources", limit=10)
```

### Analyze and Optimize a Query

```python
# Get explain plan
plan = await explain_query(
    sql="SELECT * FROM orders WHERE user_id = 123 AND status = 'pending'"
)

# Get index recommendations
recommendations = await analyze_query_indexes(
    queries=["SELECT * FROM orders WHERE user_id = 123 AND status = 'pending'"]
)

# Test hypothetical index
test_result = await test_hypothetical_index(
    table="orders",
    columns=["user_id", "status"],
    query="SELECT * FROM orders WHERE user_id = 123 AND status = 'pending'"
)
```

### Database Health Check

```python
# Run all health checks
health = await analyze_db_health(health_type="all")

# Check specific areas
index_health = await analyze_db_health(health_type="index")
vacuum_health = await analyze_db_health(health_type="vacuum")
```

## Docker

### Build

```bash
docker build -t pgtuner_mcp .
```

### Run

```bash
# Streamable HTTP mode (recommended)
docker run -p 8080:8080 \
  -e DATABASE_URI=postgresql://user:pass@host:5432/db \
  pgtuner_mcp --mode streamable-http

# Streamable HTTP stateless mode
docker run -p 8080:8080 \
  -e DATABASE_URI=postgresql://user:pass@host:5432/db \
  pgtuner_mcp --mode streamable-http --stateless

# SSE mode (legacy)
docker run -p 8080:8080 \
  -e DATABASE_URI=postgresql://user:pass@host:5432/db \
  pgtuner_mcp --mode sse

# stdio mode (for MCP clients)
docker run \
  -e DATABASE_URI=postgresql://user:pass@host:5432/db \
  pgtuner_mcp
```

## Requirements

- Python 3.10+
- PostgreSQL 12+ (recommended: 14+)
- `pg_stat_statements` extension (for query analysis)
- `hypopg` extension (optional, for hypothetical index testing)

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.
