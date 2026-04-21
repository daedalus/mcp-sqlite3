# MCP SQLite3

MCP server exposing sqlite3 library functionality.

## When to use this skill

Use this skill when you need to:
- Create and manage SQLite databases
- Execute SQL queries
- Handle transactions
- Schema operations

## Tools

**Connection:**
- `connect_database`, `close_connection`
- `commit`, `rollback`

**SQL Execution:**
- `execute_query`, `execute_many`, `execute_script`

**Schema:**
- `list_tables`, `get_table_info`
- `create_table`, `drop_table`

**CRUD:**
- `insert_row`, `update_rows`, `delete_rows`, `select_rows`

## Install

```bash
pip install mcp-sqlite3
```