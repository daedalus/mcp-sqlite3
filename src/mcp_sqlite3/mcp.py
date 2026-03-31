"""MCP Server for sqlite3 functionality."""

import base64
import json
import sqlite3
import uuid
from dataclasses import dataclass, field
from datetime import date, datetime, time
from typing import Any

import fastmcp


@dataclass
class ConnectionState:
    conn: sqlite3.Connection
    path: str
    cursors: dict[str, sqlite3.Cursor] = field(default_factory=dict)
    created_at: str = field(default_factory=lambda: datetime.now().isoformat())


mcp_server = fastmcp.FastMCP("mcp-sqlite3")

connections: dict[str, ConnectionState] = {}
MAX_CONNECTIONS = 10


def _get_connection(conn_id: str) -> sqlite3.Connection | None:
    state = connections.get(conn_id)
    return state.conn if state else None


def _serialize_row(row: sqlite3.Row) -> dict[str, Any]:
    result = {}
    for key in row.keys():
        value = row[key]
        if isinstance(value, bytes):
            result[key] = base64.b64encode(value).decode("utf-8")
        elif isinstance(value, (date, datetime)):
            result[key] = value.isoformat()
        elif isinstance(value, time):
            result[key] = value.isoformat()
        else:
            result[key] = value
    return result


def _serialize_rows(rows: list[sqlite3.Row]) -> tuple[list[dict[str, Any]], list[str]]:
    if not rows:
        return [], []
    columns = rows[0].keys()
    return [_serialize_row(row) for row in rows], list(columns)


def _format_result(
    success: bool,
    data: Any = None,
    columns: list[str] | None = None,
    rowcount: int | None = None,
    lastrowid: int | None = None,
    error: str | None = None,
) -> dict[str, Any]:
    return {
        "success": success,
        "data": data,
        "columns": columns,
        "rowcount": rowcount,
        "lastrowid": lastrowid,
        "error": error,
    }


def _make_cursor_id() -> str:
    return str(uuid.uuid4())


@mcp_server.tool()
def get_sqlite_version() -> str:
    """Get the SQLite library version.

    Returns:
        The SQLite library version string.

    Example:
        >>> get_sqlite_version()
        "3.44.0"
    """
    return sqlite3.sqlite_version


@mcp_server.tool()
def get_sqlite3_version() -> str:
    """Get the sqlite3 module version.

    Returns:
        The sqlite3 module version string.

    Example:
        >>> get_sqlite3_version()
        "3.44.0"
    """
    return ".".join(map(str, sqlite3.sqlite_version_info))


@mcp_server.tool()
def complete_sql_statement(sql: str) -> bool:
    """Check if a SQL statement is syntactically complete.

    Args:
        sql: The SQL statement to check.

    Returns:
        True if the statement is complete, False otherwise.

    Example:
        >>> complete_sql_statement("SELECT * FROM users;")
        True
        >>> complete_sql_statement("SELECT * FROM")
        False
    """
    return sqlite3.complete_statement(sql)


@mcp_server.tool()
def connect_database(
    database: str,
    timeout: float = 5.0,
    detect_types: int = 0,
    isolation_level: str | None = None,
    check_same_thread: bool = True,
    cached_statements: int = 100,
    uri: bool = False,
) -> dict[str, Any]:
    """Open a database connection.

    Args:
        database: Path to the database file or ':memory:' for in-memory database.
        timeout: Connection timeout in seconds.
        detect_types: Parse types for columns (PARSE_DECLTYPES, PARSE_COLNAMES).
        isolation_level: Transaction isolation level or None for autocommit.
        check_same_thread: Ensure thread safety.
        cached_statements: Number of cached statements.
        uri: Interpret database as URI.

    Returns:
        Connection result with conn_id or error.

    Example:
        >>> connect_database(":memory:")
        {"success": true, "conn_id": "abc-123"}
        >>> connect_database("/path/to/db.sqlite")
        {"success": true, "conn_id": "def-456"}
    """
    if len(connections) >= MAX_CONNECTIONS:
        return _format_result(
            False, error=f"Maximum connections ({MAX_CONNECTIONS}) reached"
        )

    try:
        if uri:
            database = f"file:{database}"
        conn = sqlite3.connect(
            database,
            timeout=timeout,
            detect_types=detect_types,
            isolation_level=isolation_level,  # type: ignore[arg-type]
            check_same_thread=check_same_thread,
            cached_statements=cached_statements,
        )
        conn.row_factory = sqlite3.Row
        conn_id = _make_cursor_id()
        connections[conn_id] = ConnectionState(conn=conn, path=database)
        return _format_result(True, data={"conn_id": conn_id, "path": database})
    except Exception as e:
        return _format_result(False, error=str(e))


@mcp_server.tool()
def close_connection(conn_id: str) -> dict[str, Any]:
    """Close a database connection.

    Args:
        conn_id: The connection ID to close.

    Returns:
        Success status.

    Example:
        >>> close_connection("abc-123")
        {"success": true}
    """
    if conn_id not in connections:
        return _format_result(False, error="Connection not found")

    try:
        state = connections.pop(conn_id)
        state.conn.close()
        return _format_result(True)
    except Exception as e:
        return _format_result(False, error=str(e))


@mcp_server.tool()
def commit(conn_id: str) -> dict[str, Any]:
    """Commit any pending transaction to the database.

    Args:
        conn_id: The connection ID.

    Returns:
        Success status.

    Example:
        >>> commit("abc-123")
        {"success": true}
    """
    conn = _get_connection(conn_id)
    if not conn:
        return _format_result(False, error="Connection not found")

    try:
        conn.commit()
        return _format_result(True)
    except Exception as e:
        return _format_result(False, error=str(e))


@mcp_server.tool()
def rollback(conn_id: str) -> dict[str, Any]:
    """Rollback any pending transaction.

    Args:
        conn_id: The connection ID.

    Returns:
        Success status.

    Example:
        >>> rollback("abc-123")
        {"success": true}
    """
    conn = _get_connection(conn_id)
    if not conn:
        return _format_result(False, error="Connection not found")

    try:
        conn.rollback()
        return _format_result(True)
    except Exception as e:
        return _format_result(False, error=str(e))


@mcp_server.tool()
def execute_query(
    conn_id: str,
    sql: str,
    params: list[Any] | tuple[Any, ...] | dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Execute a SQL query and return results.

    Args:
        conn_id: The connection ID.
        sql: The SQL query to execute.
        params: Query parameters for parameterized queries.

    Returns:
        Query results with rows, columns, and metadata.

    Example:
        >>> execute_query("abc-123", "SELECT * FROM users WHERE id = ?", [1])
        {"success": true, "data": [{"id": 1, "name": "John"}], "columns": ["id", "name"]}
    """
    conn = _get_connection(conn_id)
    if not conn:
        return _format_result(False, error="Connection not found")

    try:
        cursor_id = _make_cursor_id()
        cursor = conn.cursor()
        connections[conn_id].cursors[cursor_id] = cursor

        if params:
            cursor.execute(sql, params)
        else:
            cursor.execute(sql)

        rows, columns = _serialize_rows(cursor.fetchall())
        return _format_result(
            True,
            data={"cursor_id": cursor_id, "rows": rows},
            columns=columns,
            rowcount=cursor.rowcount,
            lastrowid=cursor.lastrowid,
        )
    except Exception as e:
        return _format_result(False, error=str(e))


@mcp_server.tool()
def execute_many(
    conn_id: str,
    sql: str,
    params_list: list[list[Any] | tuple[Any, ...] | dict[str, Any]],
) -> dict[str, Any]:
    """Execute a SQL statement with multiple parameter sets.

    Args:
        conn_id: The connection ID.
        sql: The SQL statement to execute.
        params_list: List of parameter sets.

    Returns:
        Execution result with total rows affected.

    Example:
        >>> execute_many("abc-123", "INSERT INTO users (name) VALUES (?)", [["Alice"], ["Bob"]])
        {"success": true, "rowcount": 2}
    """
    conn = _get_connection(conn_id)
    if not conn:
        return _format_result(False, error="Connection not found")

    try:
        cursor = conn.cursor()
        cursor.executemany(sql, params_list)
        return _format_result(
            True, rowcount=cursor.rowcount, lastrowid=cursor.lastrowid
        )
    except Exception as e:
        return _format_result(False, error=str(e))


@mcp_server.tool()
def execute_script(conn_id: str, sql_script: str) -> dict[str, Any]:
    """Execute a SQL script with multiple statements.

    Args:
        conn_id: The connection ID.
        sql_script: The SQL script containing multiple statements.

    Returns:
        Execution result.

    Example:
        >>> execute_script("abc-123", "CREATE TABLE t (x); INSERT INTO t VALUES (1); SELECT * FROM t;")
        {"success": true, "rowcount": -1}
    """
    conn = _get_connection(conn_id)
    if not conn:
        return _format_result(False, error="Connection not found")

    try:
        cursor = conn.cursor()
        cursor.executescript(sql_script)
        return _format_result(
            True, rowcount=cursor.rowcount, lastrowid=cursor.lastrowid
        )
    except Exception as e:
        return _format_result(False, error=str(e))


@mcp_server.tool()
def fetch_results(
    conn_id: str,
    cursor_id: str,
    fetch_size: int | None = None,
) -> dict[str, Any]:
    """Fetch results from a cursor.

    Args:
        conn_id: The connection ID.
        cursor_id: The cursor ID from a previous execute_query call.
        fetch_size: Number of rows to fetch (None for all remaining).

    Returns:
        Fetched rows.

    Example:
        >>> fetch_results("abc-123", "cursor-xyz", 10)
        {"success": true, "data": [{"x": 1}, {"x": 2}]}
    """
    if conn_id not in connections:
        return _format_result(False, error="Connection not found")

    state = connections[conn_id]
    if cursor_id not in state.cursors:
        return _format_result(False, error="Cursor not found")

    cursor = state.cursors[cursor_id]
    try:
        if fetch_size is None:
            rows, columns = _serialize_rows(cursor.fetchall())
        else:
            rows, columns = _serialize_rows(cursor.fetchmany(fetch_size))
        return _format_result(True, data=rows, columns=columns)
    except Exception as e:
        return _format_result(False, error=str(e))


@mcp_server.tool()
def list_tables(conn_id: str) -> dict[str, Any]:
    """List all tables in the database.

    Args:
        conn_id: The connection ID.

    Returns:
        List of table names.

    Example:
        >>> list_tables("abc-123")
        {"success": true, "data": ["users", "products", "orders"]}
    """
    conn = _get_connection(conn_id)
    if not conn:
        return _format_result(False, error="Connection not found")

    try:
        cursor = conn.cursor()
        cursor.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name"
        )
        tables = [row[0] for row in cursor.fetchall()]
        return _format_result(True, data=tables)
    except Exception as e:
        return _format_result(False, error=str(e))


@mcp_server.tool()
def get_table_info(conn_id: str, table_name: str) -> dict[str, Any]:
    """Get detailed information about a table.

    Args:
        conn_id: The connection ID.
        table_name: The name of the table.

    Returns:
        Table schema information including columns, types, defaults, and nullability.

    Example:
        >>> get_table_info("abc-123", "users")
        {"success": true, "data": [{"cid": 0, "name": "id", "type": "INTEGER", "notnull": 1, "dflt_value": null, "pk": 1}]}
    """
    conn = _get_connection(conn_id)
    if not conn:
        return _format_result(False, error="Connection not found")

    try:
        cursor = conn.cursor()
        cursor.execute(f"PRAGMA table_info({table_name})")
        columns = ["cid", "name", "type", "notnull", "dflt_value", "pk"]
        rows = [dict(zip(columns, row)) for row in cursor.fetchall()]
        return _format_result(True, data=rows, columns=columns)
    except Exception as e:
        return _format_result(False, error=str(e))


@mcp_server.tool()
def get_columns(conn_id: str, table_name: str) -> dict[str, Any]:
    """Get column information for a table.

    Args:
        conn_id: The connection ID.
        table_name: The name of the table.

    Returns:
        List of column definitions.

    Example:
        >>> get_columns("abc-123", "users")
        {"success": true, "data": [{"name": "id", "type": "INTEGER"}, {"name": "name", "type": "TEXT"}]}
    """
    result = get_table_info(conn_id, table_name)
    if result["success"]:
        result["data"] = [
            {"name": col["name"], "type": col["type"]} for col in result["data"]
        ]
        result["columns"] = ["name", "type"]
    return result


@mcp_server.tool()
def get_indexes(conn_id: str, table_name: str | None = None) -> dict[str, Any]:
    """Get index information for a table or all indexes.

    Args:
        conn_id: The connection ID.
        table_name: Optional table name to filter indexes.

    Returns:
        List of index information.

    Example:
        >>> get_indexes("abc-123", "users")
        {"success": true, "data": [{"name": "idx_name", "table": "users", "unique": 0}]}
    """
    conn = _get_connection(conn_id)
    if not conn:
        return _format_result(False, error="Connection not found")

    try:
        cursor = conn.cursor()
        if table_name:
            cursor.execute(f"PRAGMA index_list({table_name})")
        else:
            cursor.execute(
                "SELECT name, tbl_name, sql FROM sqlite_master WHERE type='index'"
            )
            rows = cursor.fetchall()
            return _format_result(
                True,
                data=[{"name": r[0], "table": r[1], "sql": r[2]} for r in rows],
            )
        columns = ["seq", "name", "unique", "origin", "partial"]
        rows = [dict(zip(columns, row)) for row in cursor.fetchall()]
        return _format_result(True, data=rows, columns=columns)
    except Exception as e:
        return _format_result(False, error=str(e))


@mcp_server.tool()
def get_primary_keys(conn_id: str, table_name: str) -> dict[str, Any]:
    """Get primary key columns for a table.

    Args:
        conn_id: The connection ID.
        table_name: The name of the table.

    Returns:
        List of primary key column names.

    Example:
        >>> get_primary_keys("abc-123", "users")
        {"success": true, "data": ["id"]}
    """
    result = get_table_info(conn_id, table_name)
    if result["success"]:
        result["data"] = [col["name"] for col in result["data"] if col["pk"] > 0]
        result["columns"] = ["pk"]
    return result


@mcp_server.tool()
def get_foreign_keys(conn_id: str, table_name: str) -> dict[str, Any]:
    """Get foreign key information for a table.

    Args:
        conn_id: The connection ID.
        table_name: The name of the table.

    Returns:
        List of foreign key definitions.

    Example:
        >>> get_foreign_keys("abc-123", "orders")
        {"success": true, "data": [{"from": "user_id", "to": "users.id", "table": "users"}]}
    """
    conn = _get_connection(conn_id)
    if not conn:
        return _format_result(False, error="Connection not found")

    try:
        cursor = conn.cursor()
        cursor.execute(f"PRAGMA foreign_key_list({table_name})")
        columns = [
            "id",
            "seq",
            "table",
            "from",
            "to",
            "on_update",
            "on_delete",
            "match",
        ]
        rows = [dict(zip(columns, row)) for row in cursor.fetchall()]
        return _format_result(True, data=rows, columns=columns)
    except Exception as e:
        return _format_result(False, error=str(e))


@mcp_server.tool()
def create_table(
    conn_id: str,
    table_name: str,
    columns: list[dict[str, Any]],
    if_not_exists: bool = True,
) -> dict[str, Any]:
    """Create a new table.

    Args:
        conn_id: The connection ID.
        table_name: The name of the table to create.
        columns: List of column definitions with 'name' and 'type' (and optional 'pk', 'notnull', 'default').
        if_not_exists: Add IF NOT EXISTS clause.

    Returns:
        Success status.

    Example:
        >>> create_table("abc-123", "users", [{"name": "id", "type": "INTEGER", "pk": True}, {"name": "name", "type": "TEXT"}])
        {"success": true}
    """
    conn = _get_connection(conn_id)
    if not conn:
        return _format_result(False, error="Connection not found")

    try:
        col_defs = []
        for col in columns:
            col_str = f"{col['name']} {col['type']}"
            if col.get("pk"):
                col_str += " PRIMARY KEY"
            if col.get("autoincrement"):
                col_str += " AUTOINCREMENT"
            if col.get("notnull"):
                col_str += " NOT NULL"
            if "default" in col:
                col_str += f" DEFAULT {col['default']}"
            col_defs.append(col_str)

        sql = f"CREATE TABLE {'IF NOT EXISTS' if if_not_exists else ''} {table_name} ({', '.join(col_defs)})"
        conn.execute(sql)
        return _format_result(True)
    except Exception as e:
        return _format_result(False, error=str(e))


@mcp_server.tool()
def drop_table(conn_id: str, table_name: str, if_exists: bool = True) -> dict[str, Any]:
    """Drop a table.

    Args:
        conn_id: The connection ID.
        table_name: The name of the table to drop.
        if_exists: Add IF EXISTS clause.

    Returns:
        Success status.

    Example:
        >>> drop_table("abc-123", "users")
        {"success": true}
    """
    conn = _get_connection(conn_id)
    if not conn:
        return _format_result(False, error="Connection not found")

    try:
        sql = f"DROP TABLE {'IF EXISTS' if if_exists else ''} {table_name}"
        conn.execute(sql)
        return _format_result(True)
    except Exception as e:
        return _format_result(False, error=str(e))


@mcp_server.tool()
def rename_table(conn_id: str, old_name: str, new_name: str) -> dict[str, Any]:
    """Rename a table.

    Args:
        conn_id: The connection ID.
        old_name: Current table name.
        new_name: New table name.

    Returns:
        Success status.

    Example:
        >>> rename_table("abc-123", "users", "accounts")
        {"success": true}
    """
    conn = _get_connection(conn_id)
    if not conn:
        return _format_result(False, error="Connection not found")

    try:
        sql = f"ALTER TABLE {old_name} RENAME TO {new_name}"
        conn.execute(sql)
        return _format_result(True)
    except Exception as e:
        return _format_result(False, error=str(e))


@mcp_server.tool()
def alter_table_add_column(
    conn_id: str,
    table_name: str,
    column_def: str,
) -> dict[str, Any]:
    """Add a column to a table.

    Args:
        conn_id: The connection ID.
        table_name: The name of the table.
        column_def: Column definition (e.g., 'email TEXT').

    Returns:
        Success status.

    Example:
        >>> alter_table_add_column("abc-123", "users", "email TEXT NOT NULL")
        {"success": true}
    """
    conn = _get_connection(conn_id)
    if not conn:
        return _format_result(False, error="Connection not found")

    try:
        sql = f"ALTER TABLE {table_name} ADD COLUMN {column_def}"
        conn.execute(sql)
        return _format_result(True)
    except Exception as e:
        return _format_result(False, error=str(e))


@mcp_server.tool()
def vacuum(conn_id: str) -> dict[str, Any]:
    """Vacuum the database to reclaim space and optimize.

    Args:
        conn_id: The connection ID.

    Returns:
        Success status.

    Example:
        >>> vacuum("abc-123")
        {"success": true}
    """
    conn = _get_connection(conn_id)
    if not conn:
        return _format_result(False, error="Connection not found")

    try:
        conn.execute("VACUUM")
        return _format_result(True)
    except Exception as e:
        return _format_result(False, error=str(e))


@mcp_server.tool()
def insert_row(conn_id: str, table_name: str, data: dict[str, Any]) -> dict[str, Any]:
    """Insert a row into a table.

    Args:
        conn_id: The connection ID.
        table_name: The name of the table.
        data: Dictionary of column names to values.

    Returns:
        Success status with lastrowid.

    Example:
        >>> insert_row("abc-123", "users", {"name": "John", "email": "john@example.com"})
        {"success": true, "lastrowid": 1}
    """
    conn = _get_connection(conn_id)
    if not conn:
        return _format_result(False, error="Connection not found")

    try:
        columns = ", ".join(data.keys())
        placeholders = ", ".join(["?" for _ in data])
        sql = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
        cursor = conn.cursor()
        cursor.execute(sql, list(data.values()))
        return _format_result(
            True, lastrowid=cursor.lastrowid, rowcount=cursor.rowcount
        )
    except Exception as e:
        return _format_result(False, error=str(e))


@mcp_server.tool()
def update_rows(
    conn_id: str,
    table_name: str,
    data: dict[str, Any],
    where: str,
    where_params: list[Any] | None = None,
) -> dict[str, Any]:
    """Update rows in a table.

    Args:
        conn_id: The connection ID.
        table_name: The name of the table.
        data: Dictionary of column names to new values.
        where: WHERE clause condition.
        where_params: Parameters for the WHERE clause.

    Returns:
        Success status with rowcount.

    Example:
        >>> update_rows("abc-123", "users", {"name": "Jane"}, "id = ?", [1])
        {"success": true, "rowcount": 1}
    """
    conn = _get_connection(conn_id)
    if not conn:
        return _format_result(False, error="Connection not found")

    try:
        set_clause = ", ".join([f"{k} = ?" for k in data.keys()])
        sql = f"UPDATE {table_name} SET {set_clause} WHERE {where}"
        params = list(data.values())
        if where_params:
            params.extend(where_params)
        cursor = conn.cursor()
        cursor.execute(sql, params)
        return _format_result(True, rowcount=cursor.rowcount)
    except Exception as e:
        return _format_result(False, error=str(e))


@mcp_server.tool()
def delete_rows(
    conn_id: str,
    table_name: str,
    where: str,
    where_params: list[Any] | None = None,
) -> dict[str, Any]:
    """Delete rows from a table.

    Args:
        conn_id: The connection ID.
        table_name: The name of the table.
        where: WHERE clause condition.
        where_params: Parameters for the WHERE clause.

    Returns:
        Success status with rowcount.

    Example:
        >>> delete_rows("abc-123", "users", "id = ?", [1])
        {"success": true, "rowcount": 1}
    """
    conn = _get_connection(conn_id)
    if not conn:
        return _format_result(False, error="Connection not found")

    try:
        sql = f"DELETE FROM {table_name} WHERE {where}"
        cursor = conn.cursor()
        if where_params:
            cursor.execute(sql, where_params)
        else:
            cursor.execute(sql)
        return _format_result(True, rowcount=cursor.rowcount)
    except Exception as e:
        return _format_result(False, error=str(e))


@mcp_server.tool()
def select_rows(
    conn_id: str,
    table_name: str,
    columns: str = "*",
    where: str = "",
    where_params: list[Any] | None = None,
    order_by: str = "",
    limit: int | None = None,
    offset: int | None = None,
) -> dict[str, Any]:
    """Select rows from a table.

    Args:
        conn_id: The connection ID.
        table_name: The name of the table.
        columns: Comma-separated column names or * for all.
        where: WHERE clause condition.
        where_params: Parameters for the WHERE clause.
        order_by: ORDER BY clause.
        limit: Maximum number of rows.
        offset: Number of rows to skip.

    Returns:
        Query results.

    Example:
        >>> select_rows("abc-123", "users", "id, name", "active = ?", [1], "name", 10)
        {"success": true, "data": [{"id": 1, "name": "John"}], "columns": ["id", "name"]}
    """
    conn = _get_connection(conn_id)
    if not conn:
        return _format_result(False, error="Connection not found")

    try:
        sql = f"SELECT {columns} FROM {table_name}"
        params = []
        if where:
            sql += f" WHERE {where}"
            if where_params:
                params = list(where_params)
        if order_by:
            sql += f" ORDER BY {order_by}"
        if limit is not None:
            sql += f" LIMIT {limit}"
        if offset is not None:
            sql += f" OFFSET {offset}"

        cursor = conn.cursor()
        if params:
            cursor.execute(sql, params)
        else:
            cursor.execute(sql)

        rows, columns_list = _serialize_rows(cursor.fetchall())
        return _format_result(
            True, data=rows, columns=columns_list, rowcount=cursor.rowcount
        )
    except Exception as e:
        return _format_result(False, error=str(e))


@mcp_server.tool()
def create_python_function(
    conn_id: str,
    name: str,
    func_code: str,
    n_arg: int = -1,
    deterministic: bool = False,
) -> dict[str, Any]:
    """Register a Python function as a SQLite function.

    Args:
        conn_id: The connection ID.
        name: Name of the SQL function.
        func_code: Python code that defines a function named 'func'.
        n_arg: Number of arguments (-1 for any).
        deterministic: Whether the function is deterministic.

    Returns:
        Success status.

    Example:
        >>> create_python_function("abc-123", "my_upper", "def func(x): return x.upper() if x else None")
        {"success": true}
    """
    conn = _get_connection(conn_id)
    if not conn:
        return _format_result(False, error="Connection not found")

    try:
        local_namespace: dict[str, Any] = {}
        exec(func_code, local_namespace)
        func = local_namespace["func"]
        conn.create_function(name, n_arg, func, deterministic=deterministic)
        return _format_result(True)
    except Exception as e:
        return _format_result(False, error=str(e))


@mcp_server.tool()
def create_python_aggregate(
    conn_id: str,
    name: str,
    step_code: str,
    finalize_code: str,
    n_arg: int = -1,
) -> dict[str, Any]:
    """Register a Python class as a SQLite aggregate function.

    Args:
        conn_id: The connection ID.
        name: Name of the SQL aggregate function.
        step_code: Python code defining a Step class with a step() method.
        finalize_code: Python code defining a Final class with a finalize() method.
        n_arg: Number of arguments (-1 for any).

    Returns:
        Success status.

    Example:
        >>> create_python_aggregate("abc-123", "my_sum", "class Step: total = 0; def step(self, v): self.total += v",
        ...     "class Final: def finalize(self): return self.total")
        {"success": true}
    """
    conn = _get_connection(conn_id)
    if not conn:
        return _format_result(False, error="Connection not found")

    try:
        step_namespace: dict[str, Any] = {}
        exec(step_code, step_namespace)
        finalize_namespace: dict[str, Any] = {}
        exec(finalize_code, finalize_namespace)

        step_obj = step_namespace["Step"]()
        final_obj = finalize_namespace["Final"]()

        class AggregateClass:
            def step(self, *args: Any) -> None:
                step_obj.step(*args)

            def finalize(self) -> Any:
                return final_obj.finalize()

        conn.create_aggregate(name, n_arg, AggregateClass)
        return _format_result(True)
    except Exception as e:
        return _format_result(False, error=str(e))


@mcp_server.tool()
def drop_function(conn_id: str, _name: str) -> dict[str, Any]:
    """Drop a user-defined function.

    Note: SQLite doesn't have DROP FUNCTION; this just confirms the function name.

    Args:
        conn_id: The connection ID.
        _name: Name of the function (unused, SQLite limitation).

    Returns:
        Success status.
    """
    if not _get_connection(conn_id):
        return _format_result(False, error="Connection not found")
    return _format_result(True)


@mcp_server.tool()
def backup_database(
    conn_id: str,
    target_path: str,
    pages: int = -1,
    name: str = "main",
) -> dict[str, Any]:
    """Backup the database to a file.

    Args:
        conn_id: The connection ID.
        target_path: Path to the backup file.
        pages: Number of pages to copy per iteration (-1 for all).
        name: Database name ('main' or 'temp' or attached database name).

    Returns:
        Success status.

    Example:
        >>> backup_database("abc-123", "/path/to/backup.db")
        {"success": true}
    """
    conn = _get_connection(conn_id)
    if not conn:
        return _format_result(False, error="Connection not found")

    try:
        target_conn = sqlite3.connect(target_path)
        conn.backup(target_conn, pages=pages, name=name)
        target_conn.close()
        return _format_result(True)
    except Exception as e:
        return _format_result(False, error=str(e))


@mcp_server.tool()
def restore_database(conn_id: str, source_path: str) -> dict[str, Any]:
    """Restore the database from a backup file.

    Args:
        conn_id: The connection ID.
        source_path: Path to the backup file.

    Returns:
        Success status.

    Example:
        >>> restore_database("abc-123", "/path/to/backup.db")
        {"success": true}
    """
    conn = _get_connection(conn_id)
    if not conn:
        return _format_result(False, error="Connection not found")

    try:
        backup_conn = sqlite3.connect(source_path)
        backup_conn.backup(conn, name="main")
        backup_conn.close()
        return _format_result(True)
    except Exception as e:
        return _format_result(False, error=str(e))


@mcp_server.tool()
def serialize_database(conn_id: str, name: str = "main") -> dict[str, Any]:
    """Serialize a database to base64-encoded bytes.

    Args:
        conn_id: The connection ID.
        name: Database name to serialize.

    Returns:
        Base64-encoded database.

    Example:
        >>> serialize_database("abc-123")
        {"success": true, "data": "U29tZURhdGFiYXNl..."}
    """
    conn = _get_connection(conn_id)
    if not conn:
        return _format_result(False, error="Connection not found")

    try:
        data = conn.serialize(name=name)
        encoded = base64.b64encode(data).decode("utf-8")
        return _format_result(True, data=encoded)
    except Exception as e:
        return _format_result(False, error=str(e))


@mcp_server.tool()
def deserialize_database(
    conn_id: str,
    data: str,
    name: str = "main",
    database: str | None = None,
) -> dict[str, Any]:
    """Deserialize a base64-encoded database.

    Args:
        conn_id: The connection ID.
        data: Base64-encoded database data.
        name: Database name to deserialize to.
        database: Optional new database path.

    Returns:
        Success status.

    Example:
        >>> deserialize_database("abc-123", "U29tZURhdGFiYXNl...")
        {"success": true}
    """
    conn = _get_connection(conn_id)
    if not conn:
        return _format_result(False, error="Connection not found")

    try:
        decoded = base64.b64decode(data)
        if database:
            new_conn = sqlite3.connect(database)
            new_conn.deserialize(decoded)
            return _format_result(True, data={"conn_id": database})
        conn.deserialize(decoded, name=name)
        return _format_result(True)
    except Exception as e:
        return _format_result(False, error=str(e))


@mcp_server.tool()
def get_table_sql(conn_id: str, table_name: str) -> dict[str, Any]:
    """Get the CREATE TABLE SQL statement for a table.

    Args:
        conn_id: The connection ID.
        table_name: The name of the table.

    Returns:
        CREATE TABLE SQL statement.

    Example:
        >>> get_table_sql("abc-123", "users")
        {"success": true, "data": "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)"}
    """
    conn = _get_connection(conn_id)
    if not conn:
        return _format_result(False, error="Connection not found")

    try:
        cursor = conn.cursor()
        cursor.execute(
            "SELECT sql FROM sqlite_master WHERE type='table' AND name=?", (table_name,)
        )
        row = cursor.fetchone()
        if row:
            return _format_result(True, data=row[0])
        return _format_result(False, error="Table not found")
    except Exception as e:
        return _format_result(False, error=str(e))


@mcp_server.tool()
def export_sql_dump(conn_id: str) -> dict[str, Any]:
    """Export the database as a SQL dump.

    Args:
        conn_id: The connection ID.

    Returns:
        SQL dump as string.

    Example:
        >>> export_sql_dump("abc-123")
        {"success": true, "data": "BEGIN TRANSACTION;\\nCREATE TABLE..."}
    """
    conn = _get_connection(conn_id)
    if not conn:
        return _format_result(False, error="Connection not found")

    try:
        lines = ["BEGIN TRANSACTION;"]
        cursor = conn.cursor()

        cursor.execute(
            "SELECT name, sql FROM sqlite_master WHERE type IN ('table', 'index', 'trigger', 'view')"
        )
        for row in cursor.fetchall():
            name, sql = row
            if sql:
                lines.append(f"{sql};")
            for line in conn.iterdump():
                if line.startswith(f"INSERT INTO {name}"):
                    lines.append(line)

        lines.append("COMMIT;")
        return _format_result(True, data="\n".join(lines))
    except Exception as e:
        return _format_result(False, error=str(e))


@mcp_server.tool()
def register_adapter(
    py_type: str,
    sql_type: str,
) -> dict[str, Any]:
    """Register an adapter for a Python type to SQL type.

    Args:
        py_type: Python type name (e.g., 'datetime.date').
        sql_type: SQL type name.

    Returns:
        Success status.

    Example:
        >>> register_adapter("datetime.date", "TEXT")
        {"success": true}
    """
    try:
        type_map = {"datetime": datetime, "date": date, "time": time}
        py_class = type_map.get(py_type.split(".")[-1])
        if not py_class:
            return _format_result(False, error=f"Unknown type: {py_type}")

        def _adapter(val: Any, _st: str = sql_type) -> str:
            return _st

        sqlite3.register_adapter(py_class, _adapter)
        return _format_result(True)
    except Exception as e:
        return _format_result(False, error=str(e))


@mcp_server.tool()
def register_converter(
    typename: str,
    converter_code: str,
) -> dict[str, Any]:
    """Register a converter for a SQL type to Python type.

    Args:
        typename: SQL type name.
        converter_code: Python code defining a converter function named 'convert'.

    Returns:
        Success status.

    Example:
        >>> register_converter("MYINT", "def convert(data): return int(data)")
        {"success": true}
    """
    try:
        local_namespace: dict[str, Any] = {}
        exec(converter_code, local_namespace)
        converter = local_namespace["convert"]
        sqlite3.register_converter(typename, converter)
        return _format_result(True)
    except Exception as e:
        return _format_result(False, error=str(e))


@mcp_server.resource("sqlite3://connections")
def list_connections() -> str:
    """List all active connections."""
    return json.dumps(
        {
            conn_id: {
                "path": state.path,
                "created_at": state.created_at,
                "cursor_count": len(state.cursors),
            }
            for conn_id, state in connections.items()
        }
    )


@mcp_server.resource("sqlite3://version")
def get_version_info() -> str:
    """Get SQLite version information."""
    return json.dumps(
        {
            "sqlite_version": sqlite3.sqlite_version,
            "sqlite3_version": ".".join(map(str, sqlite3.sqlite_version_info)),
            "library_path": sqlite3.__file__,
        }
    )


@mcp_server.resource("sqlite3://schema/{conn_id}")
def get_schema(conn_id: str) -> str:
    """Get complete database schema as JSON."""
    if conn_id not in connections:
        return json.dumps({"error": "Connection not found"})

    schema: dict[str, Any] = {"tables": []}

    try:
        tables_result = list_tables(conn_id)
        if not tables_result["success"]:
            return json.dumps({"error": tables_result["error"]})

        for table_name in tables_result["data"]:
            table_info: dict[str, Any] = {
                "name": table_name,
                "columns": get_table_info(conn_id, table_name)["data"],
                "indexes": get_indexes(conn_id, table_name)["data"],
                "primary_keys": get_primary_keys(conn_id, table_name)["data"],
                "foreign_keys": get_foreign_keys(conn_id, table_name)["data"],
            }
            schema["tables"].append(table_info)

        return json.dumps(schema)
    except Exception as e:
        return json.dumps({"error": str(e)})
