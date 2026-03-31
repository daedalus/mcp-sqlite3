# SPEC.md — mcp-sqlite3

## Purpose
An MCP server that exposes the complete Python sqlite3 standard library functionality as tools and resources, enabling AI assistants to interact with SQLite databases through the Model Context Protocol.

## Scope

### In Scope
- All sqlite3 module functions: `connect`, `adapt`, `register_adapter`, `register_converter`, `complete_statement`, `enable_callback_tracebacks`
- Connection class methods: `close`, `commit`, `rollback`, `execute`, `executemany`, `executescript`, `cursor`, `blobopen`, `backup`, `create_function`, `create_aggregate`, `create_window_function`, `create_collation`, `iterdump`, `set_authorizer`, `set_progress_handler`, `set_trace_callback`, `enable_load_extension`, `load_extension`, `serialize`, `deserialize`, `row_factory`
- Cursor class methods: `execute`, `executemany`, `executescript`, `fetchone`, `fetchmany`, `fetchall`, `fetchall_as_lists`, `setinputsizes`, `setoutputsize`, `scroll`, `description`, `rowcount`, `lastrowid`, `arraysize`
- Row class methods: `keys`, `values`, `items`, `index`, `count`
- Blob class methods: `read`, `write`, `seek`, `tell`, `close`
- All SQLite constants (SQLITE_OK, SQLITE_BUSY, etc.)
- All sqlite3 exceptions (Error, DatabaseError, OperationalError, etc.)
- Database management tools: create/open/close databases, table operations, CRUD operations

### Not in Scope
- GUI or web interfaces
- Database migration tools
- ORM functionality
- Multiple database connections (handled by client)

## Public API / Interface

### MCP Tools (exposed via fastmcp)

#### Connection Tools
- `connect_database(database: str, timeout: float = 5.0, detect_types: int = 0, isolation_level: str | None = None, check_same_thread: bool = True, factory: str | None = None, cached_statements: int = 100, uri: bool = False) -> dict` - Open a database connection
- `close_connection(conn_id: str) -> bool` - Close a database connection
- `commit(conn_id: str) -> bool` - Commit transaction
- `rollback(conn_id: str) -> bool` - Rollback transaction
- `serialize_database(conn_id: str, name: str = 'main') -> str` - Serialize database to bytes (base64)
- `deserialize_database(conn_id: str, data: str, name: str = 'main', database: str | None = None) -> bool` - Deserialize database

#### SQL Execution Tools
- `execute_query(conn_id: str, sql: str, params: list = []) -> dict` - Execute a query and return results
- `execute_many(conn_id: str, sql: str, params_list: list) -> dict` - Execute with multiple parameter sets
- `execute_script(conn_id: str, sql_script: str) -> dict` - Execute a SQL script
- `fetch_results(conn_id: str, cursor_id: str, fetch_size: int | None = None) -> dict` - Fetch results from a cursor

#### Database Schema Tools
- `list_tables(conn_id: str) -> list[str]` - List all tables
- `get_table_info(conn_id: str, table_name: str) -> list[dict]` - Get table schema
- `get_columns(conn_id: str, table_name: str) -> list[dict]` - Get column information
- `get_indexes(conn_id: str, table_name: str | None = None) -> list[dict]` - Get index information
- `get_primary_keys(conn_id: str, table_name: str) -> list[str]` - Get primary keys
- `get_foreign_keys(conn_id: str, table_name: str) -> list[dict]` - Get foreign keys

#### Table Operations Tools
- `create_table(conn_id: str, table_name: str, columns: list[dict], if_not_exists: bool = True) -> dict` - Create a table
- `drop_table(conn_id: str, table_name: str, if_exists: bool = True) -> dict` - Drop a table
- `rename_table(conn_id: str, old_name: str, new_name: str) -> dict` - Rename a table
- `alter_table_add_column(conn_id: str, table_name: str, column_def: str) -> dict` - Add column
- `vacuum(conn_id: str) -> dict` - Vacuum database

#### CRUD Tools
- `insert_row(conn_id: str, table_name: str, data: dict) -> dict` - Insert a row
- `update_rows(conn_id: str, table_name: str, data: dict, where: str, where_params: list = []) -> dict` - Update rows
- `delete_rows(conn_id: str, table_name: str, where: str, where_params: list = []) -> dict` - Delete rows
- `select_rows(conn_id: str, table_name: str, columns: str = '*', where: str = '', where_params: list = [], order_by: str = '', limit: int | None = None, offset: int | None = None) -> dict` - Select rows

#### Function/Procedure Tools
- `create_python_function(conn_id: str, name: str, func_code: str, n_arg: int = -1, deterministic: bool = False) -> dict` - Register Python function
- `create_python_aggregate(conn_id: str, name: str, step_code: str, finalize_code: str, n_arg: int = -1) -> dict` - Register Python aggregate
- `drop_function(conn_id: str, name: str) -> dict` - Drop a function

#### Backup/Restore Tools
- `backup_database(conn_id: str, target_path: str, pages: int = -1, name: str = 'main') -> dict` - Backup database
- `restore_database(conn_id: str, source_path: str) -> dict` - Restore database

#### Utility Tools
- `get_sqlite_version() -> str` - Get SQLite version
- `get_sqlite3_version() -> str` - Get sqlite3 module version
- `complete_sql_statement(sql: str) -> bool` - Check if SQL is complete
- `register_adapter(py_type: str, sql_type: str) -> dict` - Register type adapter
- `register_converter(typename: str, converter_code: str) -> dict` - Register type converter
- `get_table_sql(conn_id: str, table_name: str) -> str` - Get CREATE TABLE SQL
- `export_sql_dump(conn_id: str) -> str` - Export database as SQL dump

### MCP Resources
- `sqlite3://connections` - List of active connections
- `sqlite3://schema/{conn_id}` - Database schema overview
- `sqlite3://version` - SQLite version info

## Data Formats

### Connection State
Connections are stored in memory with unique string IDs (UUID4). Each connection tracks:
- Connection object
- Database path
- Open cursors
- Creation timestamp

### Query Results
All query results are returned as JSON-serializable dicts:
```python
{
    "success": bool,
    "data": list[dict] | None,
    "columns": list[str] | None,
    "rowcount": int | None,
    "lastrowid": int | None,
    "error": str | None
}
```

### Database Schema Format
```python
{
    "tables": [
        {
            "name": str,
            "columns": [...],
            "indexes": [...],
            "primary_keys": [...],
            "foreign_keys": [...]
        }
    ]
}
```

## Edge Cases
1. **Connection limits**: Max 10 concurrent connections per server instance
2. **SQL injection prevention**: Parameterized queries only; raw SQL strings must use `?` placeholders
3. **Large result sets**: Cursor-based fetching with configurable batch size (default 100)
4. **Concurrent access**: SQLite's locking behavior handled gracefully
5. **Memory databases**: `:memory:` databases supported but not persisted
6. **BLOB handling**: Binary data base64-encoded for JSON serialization
7. **Transaction safety**: Explicit commit/rollback with automatic rollback on error
8. **Resource cleanup**: Auto-cleanup of connections/cursors on server restart
9. **URI database paths**: Support for `file:` URI format
10. **Read-only connections**: Support for immutable database access

## Performance & Constraints
- SQLite is single-threaded; all operations are serialized
- In-memory databases (`':memory:'`) have no persistence
- Maximum SQL statement length: 1GB (SQLite limit)
- Connection timeout defaults to 5 seconds
- Statement cache size: 100 statements per connection
