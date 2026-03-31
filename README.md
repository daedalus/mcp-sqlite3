# mcp-sqlite3

> MCP server exposing sqlite3 library functionality

[![PyPI](https://img.shields.io/pypi/v/mcp-sqlite3.svg)](https://pypi.org/project/mcp-sqlite3/)
[![Python](https://img.shields.io/pypi/pyversions/mcp-sqlite3.svg)](https://pypi.org/project/mcp-sqlite3/)
[![Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json)](https://github.com/astral-sh/ruff)

## Install

```bash
pip install mcp-sqlite3
```

## Usage

```python
from mcp_sqlite3 import mcp_server

mcp_server.run()
```

## CLI

```bash
mcp-sqlite3 --help
```

## API

Exposes complete sqlite3 functionality as MCP tools.

### Connection Tools
- `connect_database` - Open a database connection
- `close_connection` - Close a database connection
- `commit` / `rollback` - Transaction control

### SQL Execution Tools
- `execute_query` - Execute a query and return results
- `execute_many` - Execute with multiple parameter sets
- `execute_script` - Execute a SQL script

### Schema Tools
- `list_tables` - List all tables
- `get_table_info` - Get table schema
- `create_table` / `drop_table` - DDL operations

### CRUD Tools
- `insert_row` / `update_rows` / `delete_rows` / `select_rows`

## Development

```bash
git clone https://github.com/dclavijo/mcp-sqlite3.git
cd mcp-sqlite3
pip install -e ".[test]"

# run tests
pytest

# format
ruff format src/ tests/

# lint
ruff check src/ tests/

# type check
mypy src/
```

## MCP Server

mcp-name: io.github.dclavijo/mcp-sqlite3
