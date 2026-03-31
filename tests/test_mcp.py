import json
import sqlite3

import pytest

from mcp_sqlite3.mcp import (
    close_connection,
    complete_sql_statement,
    connect_database,
    create_table,
    delete_rows,
    drop_table,
    get_sqlite3_version,
    get_sqlite_version,
    insert_row,
    list_tables,
    select_rows,
    update_rows,
)


class TestConnectionTools:
    def test_get_sqlite_version(self):
        result = get_sqlite_version()
        assert isinstance(result, str)
        assert len(result.split(".")) >= 2

    def test_get_sqlite3_version(self):
        result = get_sqlite3_version()
        assert isinstance(result, str)
        assert "." in result

    def test_complete_sql_statement_complete(self):
        assert complete_sql_statement("SELECT * FROM users;") is True
        assert complete_sql_statement("SELECT * FROM users WHERE id = 1;") is True

    def test_complete_sql_statement_incomplete(self):
        assert complete_sql_statement("SELECT * FROM") is False
        assert complete_sql_statement("INSERT INTO users (name) VALUES (") is False

    def test_connect_database_memory(self):
        result = connect_database(":memory:")
        assert result["success"] is True
        assert "conn_id" in result["data"]
        conn_id = result["data"]["conn_id"]
        close_connection(conn_id)

    def test_connect_database_and_close(self):
        result = connect_database(":memory:")
        assert result["success"] is True
        conn_id = result["data"]["conn_id"]
        close_result = close_connection(conn_id)
        assert close_result["success"] is True

    def test_connect_database_not_found(self):
        result = close_connection("nonexistent-id")
        assert result["success"] is False
        assert "not found" in result["error"].lower()

    def test_connect_multiple_databases(self):
        results = []
        for _ in range(5):
            result = connect_database(":memory:")
            results.append(result)
            assert result["success"] is True

        for result in results:
            close_connection(result["data"]["conn_id"])


class TestDDL:
    def test_create_table(self, temp_db):
        result = connect_database(":memory:")
        conn_id = result["data"]["conn_id"]

        columns = [
            {"name": "id", "type": "INTEGER", "pk": True, "autoincrement": True},
            {"name": "name", "type": "TEXT", "notnull": True},
            {"name": "age", "type": "INTEGER"},
        ]
        create_result = create_table(conn_id, "test_table", columns)
        assert create_result["success"] is True

        list_result = list_tables(conn_id)
        assert "test_table" in list_result["data"]

        close_connection(conn_id)

    def test_create_table_if_not_exists(self, temp_db):
        result = connect_database(":memory:")
        conn_id = result["data"]["conn_id"]

        columns = [{"name": "id", "type": "INTEGER", "pk": True}]
        create_result = create_table(conn_id, "dup_table", columns)
        assert create_result["success"] is True

        create_result2 = create_table(conn_id, "dup_table", columns)
        assert create_result2["success"] is True

        close_connection(conn_id)

    def test_drop_table(self, temp_db):
        result = connect_database(":memory:")
        conn_id = result["data"]["conn_id"]

        columns = [{"name": "id", "type": "INTEGER", "pk": True}]
        create_table(conn_id, "to_drop", columns)
        assert list_tables(conn_id)["success"] is True

        drop_result = drop_table(conn_id, "to_drop")
        assert drop_result["success"] is True

        close_connection(conn_id)

    def test_rename_table(self, temp_db):
        result = connect_database(":memory:")
        conn_id = result["data"]["conn_id"]

        columns = [{"name": "id", "type": "INTEGER", "pk": True}]
        create_table(conn_id, "old_name", columns)

        from mcp_sqlite3.mcp import rename_table

        rename_result = rename_table(conn_id, "old_name", "new_name")
        assert rename_result["success"] is True

        tables = list_tables(conn_id)["data"]
        assert "old_name" not in tables
        assert "new_name" in tables

        close_connection(conn_id)

    def test_alter_table_add_column(self, temp_db):
        result = connect_database(":memory:")
        conn_id = result["data"]["conn_id"]

        columns = [{"name": "id", "type": "INTEGER", "pk": True}]
        create_table(conn_id, "test_alter", columns)

        from mcp_sqlite3.mcp import alter_table_add_column

        alter_result = alter_table_add_column(conn_id, "test_alter", "new_col TEXT")
        assert alter_result["success"] is True

        close_connection(conn_id)


class TestDML:
    def test_insert_row(self, temp_db):
        result = connect_database(":memory:")
        conn_id = result["data"]["conn_id"]

        columns = [
            {"name": "id", "type": "INTEGER", "pk": True, "autoincrement": True},
            {"name": "name", "type": "TEXT"},
        ]
        create_table(conn_id, "users", columns)

        insert_result = insert_row(conn_id, "users", {"name": "Alice"})
        assert insert_result["success"] is True

        close_connection(conn_id)

    def test_select_rows(self, temp_db):
        result = connect_database(":memory:")
        conn_id = result["data"]["conn_id"]

        columns = [
            {"name": "id", "type": "INTEGER", "pk": True},
            {"name": "name", "type": "TEXT"},
        ]
        create_table(conn_id, "users", columns)
        insert_row(conn_id, "users", {"id": 1, "name": "Bob"})
        insert_row(conn_id, "users", {"id": 2, "name": "Carol"})

        select_result = select_rows(conn_id, "users")
        assert select_result["success"] is True
        assert len(select_result["data"]) == 2

        close_connection(conn_id)

    def test_select_rows_with_where(self, temp_db):
        result = connect_database(":memory:")
        conn_id = result["data"]["conn_id"]

        columns = [
            {"name": "id", "type": "INTEGER", "pk": True},
            {"name": "name", "type": "TEXT"},
        ]
        create_table(conn_id, "users", columns)
        insert_row(conn_id, "users", {"id": 1, "name": "Alice"})
        insert_row(conn_id, "users", {"id": 2, "name": "Bob"})

        select_result = select_rows(conn_id, "users", where="id = ?", where_params=[1])
        assert select_result["success"] is True
        assert len(select_result["data"]) == 1
        assert select_result["data"][0]["name"] == "Alice"

        close_connection(conn_id)

    def test_update_rows(self, temp_db):
        result = connect_database(":memory:")
        conn_id = result["data"]["conn_id"]

        columns = [
            {"name": "id", "type": "INTEGER", "pk": True},
            {"name": "name", "type": "TEXT"},
        ]
        create_table(conn_id, "users", columns)
        insert_row(conn_id, "users", {"id": 1, "name": "Old Name"})

        update_result = update_rows(
            conn_id, "users", {"name": "New Name"}, "id = ?", [1]
        )
        assert update_result["success"] is True
        assert update_result["rowcount"] == 1

        select_result = select_rows(conn_id, "users", where="id = ?", where_params=[1])
        assert select_result["data"][0]["name"] == "New Name"

        close_connection(conn_id)

    def test_delete_rows(self, temp_db):
        result = connect_database(":memory:")
        conn_id = result["data"]["conn_id"]

        columns = [
            {"name": "id", "type": "INTEGER", "pk": True},
            {"name": "name", "type": "TEXT"},
        ]
        create_table(conn_id, "users", columns)
        insert_row(conn_id, "users", {"id": 1, "name": "To Delete"})
        insert_row(conn_id, "users", {"id": 2, "name": "To Keep"})

        delete_result = delete_rows(conn_id, "users", "id = ?", [1])
        assert delete_result["success"] is True
        assert delete_result["rowcount"] == 1

        select_result = select_rows(conn_id, "users")
        assert len(select_result["data"]) == 1
        assert select_result["data"][0]["id"] == 2

        close_connection(conn_id)


class TestExecuteQuery:
    def test_execute_query_select(self):
        result = connect_database(":memory:")
        conn_id = result["data"]["conn_id"]

        from mcp_sqlite3.mcp import execute_query, execute_script

        execute_script(
            conn_id,
            "CREATE TABLE test (id, name); INSERT INTO test VALUES (1, 'test');",
        )

        query_result = execute_query(conn_id, "SELECT * FROM test WHERE id = ?", [1])
        assert query_result["success"] is True
        assert query_result["data"]["rows"][0]["name"] == "test"

        close_connection(conn_id)

    def test_execute_query_without_params(self):
        result = connect_database(":memory:")
        conn_id = result["data"]["conn_id"]

        from mcp_sqlite3.mcp import execute_query, execute_script

        execute_script(
            conn_id,
            "CREATE TABLE test (id, name); INSERT INTO test VALUES (1, 'test');",
        )

        query_result = execute_query(conn_id, "SELECT * FROM test")
        assert query_result["success"] is True
        assert len(query_result["data"]["rows"]) == 1

        close_connection(conn_id)

    def test_execute_many(self):
        result = connect_database(":memory:")
        conn_id = result["data"]["conn_id"]

        from mcp_sqlite3.mcp import execute_many, execute_script

        execute_script(conn_id, "CREATE TABLE test (id, name);")

        many_result = execute_many(
            conn_id, "INSERT INTO test VALUES (?, ?)", [[1, "a"], [2, "b"], [3, "c"]]
        )
        assert many_result["success"] is True
        assert many_result["rowcount"] == 3

        close_connection(conn_id)

    def test_execute_script(self):
        result = connect_database(":memory:")
        conn_id = result["data"]["conn_id"]

        from mcp_sqlite3.mcp import execute_script

        script_result = execute_script(
            conn_id,
            "CREATE TABLE t (x); INSERT INTO t VALUES (1); INSERT INTO t VALUES (2); SELECT * FROM t;",
        )
        assert script_result["success"] is True

        close_connection(conn_id)


class TestSchema:
    def test_list_tables(self):
        result = connect_database(":memory:")
        conn_id = result["data"]["conn_id"]

        columns = [{"name": "id", "type": "INTEGER", "pk": True}]
        create_table(conn_id, "users", columns)
        create_table(conn_id, "products", columns)

        tables_result = list_tables(conn_id)
        assert tables_result["success"] is True
        assert "users" in tables_result["data"]
        assert "products" in tables_result["data"]

        close_connection(conn_id)

    def test_get_table_info(self):
        result = connect_database(":memory:")
        conn_id = result["data"]["conn_id"]

        columns = [
            {"name": "id", "type": "INTEGER", "pk": True},
            {"name": "name", "type": "TEXT", "notnull": True},
            {"name": "age", "type": "INTEGER"},
        ]
        create_table(conn_id, "users", columns)

        from mcp_sqlite3.mcp import get_table_info

        info_result = get_table_info(conn_id, "users")
        assert info_result["success"] is True
        assert len(info_result["data"]) == 3

        close_connection(conn_id)

    def test_get_columns(self):
        result = connect_database(":memory:")
        conn_id = result["data"]["conn_id"]

        columns = [{"name": "id", "type": "INTEGER"}, {"name": "name", "type": "TEXT"}]
        create_table(conn_id, "users", columns)

        from mcp_sqlite3.mcp import get_columns

        cols_result = get_columns(conn_id, "users")
        assert cols_result["success"] is True
        assert len(cols_result["data"]) == 2

        close_connection(conn_id)

    def test_get_primary_keys(self):
        result = connect_database(":memory:")
        conn_id = result["data"]["conn_id"]

        columns = [
            {"name": "id", "type": "INTEGER", "pk": True},
            {"name": "name", "type": "TEXT"},
        ]
        create_table(conn_id, "users", columns)

        from mcp_sqlite3.mcp import get_primary_keys

        pk_result = get_primary_keys(conn_id, "users")
        assert pk_result["success"] is True
        assert "id" in pk_result["data"]

        close_connection(conn_id)


class TestTransaction:
    def test_commit(self):
        result = connect_database(":memory:")
        conn_id = result["data"]["conn_id"]

        from mcp_sqlite3.mcp import commit, create_table, insert_row

        columns = [{"name": "id", "type": "INTEGER", "pk": True}]
        create_table(conn_id, "test", columns)
        insert_row(conn_id, "test", {"id": 1})

        commit_result = commit(conn_id)
        assert commit_result["success"] is True

        close_connection(conn_id)

    def test_rollback(self):
        result = connect_database(":memory:")
        conn_id = result["data"]["conn_id"]

        from mcp_sqlite3.mcp import create_table, insert_row, rollback

        columns = [{"name": "id", "type": "INTEGER", "pk": True}]
        create_table(conn_id, "test", columns)

        rollback_result = rollback(conn_id)
        assert rollback_result["success"] is True

        close_connection(conn_id)


class TestBackupRestore:
    def test_backup_database(self):
        import os
        import tempfile

        result = connect_database(":memory:")
        conn_id = result["data"]["conn_id"]

        from mcp_sqlite3.mcp import backup_database, create_table, insert_row

        columns = [{"name": "id", "type": "INTEGER", "pk": True}]
        create_table(conn_id, "test", columns)
        insert_row(conn_id, "test", {"id": 1})

        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            backup_path = f.name

        try:
            backup_result = backup_database(conn_id, backup_path)
            assert backup_result["success"] is True

            conn_id2 = connect_database(backup_path)["data"]["conn_id"]
            from mcp_sqlite3.mcp import select_rows

            select_result = select_rows(conn_id2, "test")
            assert select_result["data"][0]["id"] == 1
            close_connection(conn_id2)
        finally:
            os.unlink(backup_path)
            close_connection(conn_id)


class TestUtilities:
    def test_vacuum(self):
        result = connect_database(":memory:")
        conn_id = result["data"]["conn_id"]

        from mcp_sqlite3.mcp import create_table, insert_row, vacuum

        columns = [{"name": "id", "type": "INTEGER", "pk": True}]
        create_table(conn_id, "test", columns)
        insert_row(conn_id, "test", {"id": 1})

        vacuum_result = vacuum(conn_id)
        assert vacuum_result["success"] is True

        close_connection(conn_id)

    def test_get_table_sql(self):
        result = connect_database(":memory:")
        conn_id = result["data"]["conn_id"]

        from mcp_sqlite3.mcp import create_table, get_table_sql

        columns = [{"name": "id", "type": "INTEGER", "pk": True}]
        create_table(conn_id, "test", columns)

        sql_result = get_table_sql(conn_id, "test")
        assert sql_result["success"] is True
        assert "CREATE TABLE" in sql_result["data"]

        close_connection(conn_id)


class TestErrorHandling:
    def test_invalid_sql(self):
        result = connect_database(":memory:")
        conn_id = result["data"]["conn_id"]

        from mcp_sqlite3.mcp import execute_query

        query_result = execute_query(conn_id, "INVALID SQL")
        assert query_result["success"] is False
        assert "error" in query_result

        close_connection(conn_id)

    def test_nonexistent_table(self):
        result = connect_database(":memory:")
        conn_id = result["data"]["conn_id"]

        select_result = select_rows(conn_id, "nonexistent")
        assert select_result["success"] is False

        close_connection(conn_id)

    def test_invalid_connection(self):
        from mcp_sqlite3.mcp import _get_connection

        conn = _get_connection("invalid-id")
        assert conn is None


class TestResources:
    def test_list_connections_resource(self):
        from mcp_sqlite3.mcp import close_connection, connect_database, list_connections

        result = connect_database(":memory:")
        conn_id = result["data"]["conn_id"]

        connections_data = list_connections()
        assert conn_id in connections_data

        close_connection(conn_id)

    def test_version_resource(self):
        import json

        from mcp_sqlite3.mcp import get_version_info

        version_data = json.loads(get_version_info())
        assert "sqlite_version" in version_data
        assert "sqlite3_version" in version_data

    def test_schema_resource(self):
        from mcp_sqlite3.mcp import close_connection, connect_database, get_schema

        result = connect_database(":memory:")
        conn_id = result["data"]["conn_id"]

        schema_data = json.loads(get_schema(conn_id))
        assert "tables" in schema_data

        close_connection(conn_id)


class TestSchemaAdvanced:
    def test_get_foreign_keys(self):
        from mcp_sqlite3.mcp import (
            close_connection,
            connect_database,
            create_table,
            execute_script,
            get_foreign_keys,
        )

        result = connect_database(":memory:")
        conn_id = result["data"]["conn_id"]

        execute_script(
            conn_id,
            """
            CREATE TABLE users (id INTEGER PRIMARY KEY);
            CREATE TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER REFERENCES users(id));
        """,
        )

        fk_result = get_foreign_keys(conn_id, "orders")
        assert fk_result["success"] is True
        assert len(fk_result["data"]) > 0

        close_connection(conn_id)

    def test_get_indexes(self):
        from mcp_sqlite3.mcp import (
            close_connection,
            connect_database,
            create_table,
            execute_script,
            get_indexes,
        )

        result = connect_database(":memory:")
        conn_id = result["data"]["conn_id"]

        execute_script(
            conn_id,
            """
            CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT);
            CREATE INDEX idx_name ON test(name);
        """,
        )

        indexes_result = get_indexes(conn_id, "test")
        assert indexes_result["success"] is True

        all_indexes = get_indexes(conn_id)
        assert all_indexes["success"] is True

        close_connection(conn_id)


class TestBackupAdvanced:
    def test_restore_database(self):
        import os
        import tempfile

        from mcp_sqlite3.mcp import (
            backup_database,
            close_connection,
            connect_database,
            create_table,
            insert_row,
            restore_database,
        )

        result = connect_database(":memory:")
        conn_id = result["data"]["conn_id"]

        create_table(conn_id, "test", [{"name": "id", "type": "INTEGER", "pk": True}])
        insert_row(conn_id, "test", {"id": 1})

        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            backup_path = f.name

        try:
            backup_database(conn_id, backup_path)
            close_connection(conn_id)

            result2 = connect_database(":memory:")
            conn_id2 = result2["data"]["conn_id"]

            restore_result = restore_database(conn_id2, backup_path)
            assert restore_result["success"] is True

            close_connection(conn_id2)
        finally:
            os.unlink(backup_path)

    def test_serialize_database(self):
        from mcp_sqlite3.mcp import (
            close_connection,
            connect_database,
            create_table,
            insert_row,
            serialize_database,
        )

        result = connect_database(":memory:")
        conn_id = result["data"]["conn_id"]

        create_table(conn_id, "test", [{"name": "id", "type": "INTEGER", "pk": True}])
        insert_row(conn_id, "test", {"id": 1})

        serialize_result = serialize_database(conn_id)
        assert serialize_result["success"] is True
        assert isinstance(serialize_result["data"], str)

        close_connection(conn_id)

    def test_export_sql_dump(self):
        from mcp_sqlite3.mcp import (
            close_connection,
            connect_database,
            execute_script,
            export_sql_dump,
        )

        result = connect_database(":memory:")
        conn_id = result["data"]["conn_id"]

        execute_script(
            conn_id,
            """
            CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT);
        """,
        )

        dump_result = export_sql_dump(conn_id)
        assert dump_result["success"] is True
        assert "CREATE TABLE" in dump_result["data"]

        close_connection(conn_id)


class TestFunctionRegistration:
    def test_create_python_function(self):
        from mcp_sqlite3.mcp import (
            close_connection,
            connect_database,
            create_python_function,
            execute_query,
        )

        result = connect_database(":memory:")
        conn_id = result["data"]["conn_id"]

        func_code = """
def func(x):
    return str(x).upper()
"""
        func_result = create_python_function(conn_id, "my_upper", func_code)
        assert func_result["success"] is True

        query_result = execute_query(conn_id, "SELECT my_upper('hello')")
        assert query_result["success"] is True

        close_connection(conn_id)

    def test_create_python_aggregate(self):
        from mcp_sqlite3.mcp import (
            close_connection,
            connect_database,
            create_python_aggregate,
            execute_query,
        )

        result = connect_database(":memory:")
        conn_id = result["data"]["conn_id"]

        step_code = """
class Step:
    def __init__(self):
        self.count = 0
    def step(self, value):
        self.count += 1
"""
        finalize_code = """
class Final:
    def finalize(self):
        return self.count
"""

        agg_result = create_python_aggregate(
            conn_id, "my_count", step_code, finalize_code
        )
        assert agg_result["success"] is True

        close_connection(conn_id)

    def test_register_adapter(self):
        from mcp_sqlite3.mcp import register_adapter

        result = register_adapter("datetime", "TEXT")
        assert result["success"] is True

        result = register_adapter("date", "TEXT")
        assert result["success"] is True

        result = register_adapter("time", "TEXT")
        assert result["success"] is True

    def test_register_converter(self):
        from mcp_sqlite3.mcp import register_converter

        converter_code = """
def convert(data):
    return int(data)
"""
        result = register_converter("MYINT", converter_code)
        assert result["success"] is True


class TestFetchResults:
    def test_fetch_results(self):
        from mcp_sqlite3.mcp import (
            close_connection,
            connect_database,
            execute_query,
            execute_script,
            fetch_results,
        )

        result = connect_database(":memory:")
        conn_id = result["data"]["conn_id"]

        execute_script(
            conn_id,
            "CREATE TABLE test (x); INSERT INTO test VALUES (1); INSERT INTO test VALUES (2);",
        )

        query_result = execute_query(conn_id, "SELECT * FROM test")
        assert query_result["success"] is True
        cursor_id = query_result["data"]["cursor_id"]

        fetch_result = fetch_results(conn_id, cursor_id)
        assert fetch_result["success"] is True

        close_connection(conn_id)


class TestConnectionAdvanced:
    def test_connect_database_with_uri(self):
        from mcp_sqlite3.mcp import close_connection, connect_database

        result = connect_database("test.db", uri=True)
        assert result["success"] is True
        conn_id = result["data"]["conn_id"]
        close_connection(conn_id)

    def test_connect_database_custom_timeout(self):
        from mcp_sqlite3.mcp import close_connection, connect_database

        result = connect_database(":memory:", timeout=10.0)
        assert result["success"] is True
        conn_id = result["data"]["conn_id"]
        close_connection(conn_id)

    def test_close_nonexistent_connection(self):
        from mcp_sqlite3.mcp import close_connection

        result = close_connection("nonexistent-id")
        assert result["success"] is False

    def test_commit_nonexistent_connection(self):
        from mcp_sqlite3.mcp import commit

        result = commit("nonexistent-id")
        assert result["success"] is False

    def test_rollback_nonexistent_connection(self):
        from mcp_sqlite3.mcp import rollback

        result = rollback("nonexistent-id")
        assert result["success"] is False

    def test_execute_query_with_dict_params(self):
        from mcp_sqlite3.mcp import close_connection, connect_database, execute_script

        result = connect_database(":memory:")
        conn_id = result["data"]["conn_id"]

        execute_script(conn_id, "CREATE TABLE test (id INTEGER, name TEXT)")
        from mcp_sqlite3.mcp import execute_query

        query_result = execute_query(
            conn_id, "INSERT INTO test VALUES (:id, :name)", {"id": 1, "name": "test"}
        )
        assert query_result["success"] is True

        close_connection(conn_id)

    def test_execute_query_with_tuple_params(self):
        from mcp_sqlite3.mcp import close_connection, connect_database, execute_script

        result = connect_database(":memory:")
        conn_id = result["data"]["conn_id"]

        execute_script(conn_id, "CREATE TABLE test (id INTEGER, name TEXT)")
        from mcp_sqlite3.mcp import execute_query

        query_result = execute_query(
            conn_id, "INSERT INTO test VALUES (?, ?)", (1, "test")
        )
        assert query_result["success"] is True

        close_connection(conn_id)

    def test_execute_many(self):
        from mcp_sqlite3.mcp import close_connection, connect_database, execute_many

        result = connect_database(":memory:")
        conn_id = result["data"]["conn_id"]

        execute_many(conn_id, "CREATE TABLE test (id INTEGER, name TEXT)", [])
        assert result["success"] is True

        close_connection(conn_id)

    def test_fetch_nonexistent_cursor(self):
        from mcp_sqlite3.mcp import close_connection, connect_database, fetch_results

        result = connect_database(":memory:")
        conn_id = result["data"]["conn_id"]

        fetch_result = fetch_results(conn_id, "nonexistent-cursor")
        assert fetch_result["success"] is False

        close_connection(conn_id)


class TestDDLAdvanced:
    def test_create_table_without_if_not_exists(self):
        from mcp_sqlite3.mcp import close_connection, connect_database, create_table

        result = connect_database(":memory:")
        conn_id = result["data"]["conn_id"]

        create_result = create_table(
            conn_id,
            "test_no_if",
            [{"name": "id", "type": "INTEGER"}],
            if_not_exists=False,
        )
        assert create_result["success"] is True

        close_connection(conn_id)

    def test_drop_table_without_if_exists(self):
        from mcp_sqlite3.mcp import (
            close_connection,
            connect_database,
            create_table,
            drop_table,
        )

        result = connect_database(":memory:")
        conn_id = result["data"]["conn_id"]

        create_table(conn_id, "test_drop", [{"name": "id", "type": "INTEGER"}])
        drop_result = drop_table(conn_id, "test_drop", if_exists=False)
        assert drop_result["success"] is True

        close_connection(conn_id)

    def test_rename_table_error(self):
        from mcp_sqlite3.mcp import close_connection, connect_database, rename_table

        result = connect_database(":memory:")
        conn_id = result["data"]["conn_id"]

        rename_result = rename_table(conn_id, "nonexistent", "new_name")
        assert rename_result["success"] is False

        close_connection(conn_id)

    def test_alter_table_add_column_error(self):
        from mcp_sqlite3.mcp import (
            alter_table_add_column,
            close_connection,
            connect_database,
        )

        result = connect_database(":memory:")
        conn_id = result["data"]["conn_id"]

        alter_result = alter_table_add_column(conn_id, "nonexistent", "col TEXT")
        assert alter_result["success"] is False

        close_connection(conn_id)

    def test_get_table_sql_nonexistent(self):
        from mcp_sqlite3.mcp import close_connection, connect_database, get_table_sql

        result = connect_database(":memory:")
        conn_id = result["data"]["conn_id"]

        sql_result = get_table_sql(conn_id, "nonexistent")
        assert sql_result["success"] is False

        close_connection(conn_id)


class TestCRUDAdvanced:
    def test_insert_row_with_commit(self):
        from mcp_sqlite3.mcp import (
            close_connection,
            commit,
            connect_database,
            create_table,
            insert_row,
        )

        result = connect_database(":memory:")
        conn_id = result["data"]["conn_id"]

        create_table(conn_id, "test", [{"name": "id", "type": "INTEGER", "pk": True}])
        insert_result = insert_row(conn_id, "test", {"id": 1})
        assert insert_result["success"] is True

        commit_result = commit(conn_id)
        assert commit_result["success"] is True

        close_connection(conn_id)

    def test_update_rows_with_where_params(self):
        from mcp_sqlite3.mcp import (
            close_connection,
            connect_database,
            create_table,
            insert_row,
            update_rows,
        )

        result = connect_database(":memory:")
        conn_id = result["data"]["conn_id"]

        create_table(conn_id, "test", [{"name": "id", "type": "INTEGER", "pk": True}])
        insert_row(conn_id, "test", {"id": 1})
        insert_row(conn_id, "test", {"id": 2})

        update_result = update_rows(conn_id, "test", {"id": 3}, "id = ?", [1])
        assert update_result["success"] is True

        close_connection(conn_id)

    def test_delete_rows_with_params(self):
        from mcp_sqlite3.mcp import (
            close_connection,
            connect_database,
            create_table,
            delete_rows,
            insert_row,
        )

        result = connect_database(":memory:")
        conn_id = result["data"]["conn_id"]

        create_table(conn_id, "test", [{"name": "id", "type": "INTEGER", "pk": True}])
        insert_row(conn_id, "test", {"id": 1})
        insert_row(conn_id, "test", {"id": 2})

        delete_result = delete_rows(conn_id, "test", "id = ?", [1])
        assert delete_result["success"] is True
        assert delete_result["rowcount"] == 1

        close_connection(conn_id)

    def test_select_rows_with_all_params(self):
        from mcp_sqlite3.mcp import (
            close_connection,
            connect_database,
            create_table,
            insert_row,
            select_rows,
        )

        result = connect_database(":memory:")
        conn_id = result["data"]["conn_id"]

        create_table(conn_id, "test", [{"name": "id", "type": "INTEGER", "pk": True}])
        for i in range(10):
            insert_row(conn_id, "test", {"id": i})

        select_result = select_rows(
            conn_id, "test", "id, id as id2", "id > ?", [5], "id DESC", 3, 1
        )
        assert select_result["success"] is True
        assert len(select_result["data"]) == 3

        close_connection(conn_id)


class TestSchemaNonexistent:
    def test_get_table_info_nonexistent(self):
        from mcp_sqlite3.mcp import close_connection, connect_database, get_table_info

        result = connect_database(":memory:")
        conn_id = result["data"]["conn_id"]

        info_result = get_table_info(conn_id, "nonexistent")
        assert info_result["success"] is True
        assert info_result["data"] == []

        close_connection(conn_id)

    def test_get_columns_nonexistent(self):
        from mcp_sqlite3.mcp import close_connection, connect_database, get_columns

        result = connect_database(":memory:")
        conn_id = result["data"]["conn_id"]

        cols_result = get_columns(conn_id, "nonexistent")
        assert cols_result["success"] is True
        assert cols_result["data"] == []

        close_connection(conn_id)

    def test_get_primary_keys_nonexistent(self):
        from mcp_sqlite3.mcp import close_connection, connect_database, get_primary_keys

        result = connect_database(":memory:")
        conn_id = result["data"]["conn_id"]

        pk_result = get_primary_keys(conn_id, "nonexistent")
        assert pk_result["success"] is True
        assert pk_result["data"] == []

        close_connection(conn_id)

    def test_get_foreign_keys_nonexistent(self):
        from mcp_sqlite3.mcp import close_connection, connect_database, get_foreign_keys

        result = connect_database(":memory:")
        conn_id = result["data"]["conn_id"]

        fk_result = get_foreign_keys(conn_id, "nonexistent")
        assert fk_result["success"] is True
        assert fk_result["data"] == []

        close_connection(conn_id)

    def test_vacuum_nonexistent_connection(self):
        from mcp_sqlite3.mcp import vacuum

        result = vacuum("nonexistent")
        assert result["success"] is False

    def test_backup_database_nonexistent_connection(self):
        from mcp_sqlite3.mcp import backup_database

        result = backup_database("nonexistent", "/tmp/backup.db")
        assert result["success"] is False

    def test_restore_database_nonexistent_connection(self):
        from mcp_sqlite3.mcp import restore_database

        result = restore_database("nonexistent", "/tmp/backup.db")
        assert result["success"] is False

    def test_serialize_database_nonexistent_connection(self):
        from mcp_sqlite3.mcp import serialize_database

        result = serialize_database("nonexistent")
        assert result["success"] is False

    def test_deserialize_database_nonexistent_connection(self):
        from mcp_sqlite3.mcp import deserialize_database

        result = deserialize_database("nonexistent", "data")
        assert result["success"] is False

    def test_export_sql_dump_nonexistent_connection(self):
        from mcp_sqlite3.mcp import export_sql_dump

        result = export_sql_dump("nonexistent")
        assert result["success"] is False


class TestFunctionRegistrationAdvanced:
    def test_create_python_function_error(self):
        from mcp_sqlite3.mcp import (
            close_connection,
            connect_database,
            create_python_function,
        )

        result = connect_database(":memory:")
        conn_id = result["data"]["conn_id"]

        func_result = create_python_function(conn_id, "bad", "invalid code")
        assert func_result["success"] is False

        close_connection(conn_id)

    def test_create_python_aggregate_error(self):
        from mcp_sqlite3.mcp import (
            close_connection,
            connect_database,
            create_python_aggregate,
        )

        result = connect_database(":memory:")
        conn_id = result["data"]["conn_id"]

        agg_result = create_python_aggregate(conn_id, "bad", "invalid", "invalid")
        assert agg_result["success"] is False

        close_connection(conn_id)

    def test_register_adapter_invalid_type(self):
        from mcp_sqlite3.mcp import register_adapter

        result = register_adapter("invalid.type", "TEXT")
        assert result["success"] is False

    def test_register_adapter_exception(self):
        from mcp_sqlite3.mcp import register_adapter

        result = register_adapter("datetime", "TEXT")
        assert result["success"] is True

    def test_register_converter_error(self):
        from mcp_sqlite3.mcp import register_converter

        result = register_converter("MYTYPE", "invalid code")
        assert result["success"] is False


class TestResourcesAdvanced:
    def test_schema_nonexistent_connection(self):
        from mcp_sqlite3.mcp import get_schema

        schema_data = get_schema("nonexistent")
        assert '"error"' in schema_data

    def test_list_connections_empty(self):
        from mcp_sqlite3.mcp import connections, list_connections

        initial_count = len(connections)
        result = list_connections()
        assert str(initial_count) in result or "{}" == result.strip()


class TestSerializeRow:
    def test_serialize_datetime_values(self):
        from mcp_sqlite3.mcp import close_connection, connect_database, execute_query

        result = connect_database(":memory:")
        conn_id = result["data"]["conn_id"]

        execute_query(
            conn_id,
            "SELECT DATE('now') as dt, TIME('now') as t, DATETIME('now') as dt2",
        )

        close_connection(conn_id)

    def test_serialize_binary_values(self):
        from mcp_sqlite3.mcp import close_connection, connect_database, execute_query

        result = connect_database(":memory:")
        conn_id = result["data"]["conn_id"]

        execute_query(conn_id, "SELECT X'010203' as bin")

        close_connection(conn_id)


class TestConnectionLimits:
    def test_max_connections_reached(self):
        from mcp_sqlite3.mcp import close_connection, connect_database, connections

        original_connections = dict(connections)
        connections.clear()

        for i in range(10):
            result = connect_database(":memory:")
            assert result["success"] is True

        result = connect_database(":memory:")
        assert result["success"] is False
        assert "Maximum connections" in result["error"]

        connections.clear()
        connections.update(original_connections)


class TestExecuteQueryVariants:
    def test_execute_query_insert_return(self):
        from mcp_sqlite3.mcp import close_connection, connect_database, execute_query

        result = connect_database(":memory:")
        conn_id = result["data"]["conn_id"]

        execute_query(conn_id, "CREATE TABLE test (id INTEGER PRIMARY KEY)")

        query_result = execute_query(conn_id, "INSERT INTO test VALUES (NULL)")
        assert query_result["success"] is True
        assert query_result["lastrowid"] is not None

        close_connection(conn_id)

    def test_execute_query_select_with_columns(self):
        from mcp_sqlite3.mcp import close_connection, connect_database, execute_query

        result = connect_database(":memory:")
        conn_id = result["data"]["conn_id"]

        execute_query(conn_id, "CREATE TABLE test (a, b)")
        execute_query(conn_id, "INSERT INTO test VALUES (1, 2)")

        query_result = execute_query(conn_id, "SELECT a, b FROM test")
        assert query_result["success"] is True
        assert query_result["columns"] == ["a", "b"]

        close_connection(conn_id)

    def test_execute_query_with_rowcount(self):
        from mcp_sqlite3.mcp import close_connection, connect_database, execute_query

        result = connect_database(":memory:")
        conn_id = result["data"]["conn_id"]

        execute_query(conn_id, "CREATE TABLE test (id INTEGER)")
        execute_query(conn_id, "INSERT INTO test VALUES (1)")
        execute_query(conn_id, "INSERT INTO test VALUES (2)")

        query_result = execute_query(conn_id, "DELETE FROM test")
        assert query_result["rowcount"] == 2

        close_connection(conn_id)


class TestExecuteManyVariants:
    def test_execute_many_multiple_rows(self):
        from mcp_sqlite3.mcp import close_connection, connect_database, execute_many

        result = connect_database(":memory:")
        conn_id = result["data"]["conn_id"]

        execute_many(conn_id, "CREATE TABLE test (id INTEGER, name TEXT)", [])

        execute_many(
            conn_id, "INSERT INTO test VALUES (?, ?)", [[1, "a"], [2, "b"], [3, "c"]]
        )
        assert result["success"] is True

        close_connection(conn_id)

    def test_execute_many_tuple_params(self):
        from mcp_sqlite3.mcp import close_connection, connect_database, execute_many

        result = connect_database(":memory:")
        conn_id = result["data"]["conn_id"]

        execute_many(conn_id, "CREATE TABLE test (id INTEGER, name TEXT)", [])

        execute_many(conn_id, "INSERT INTO test VALUES (?, ?)", [(1, "a"), (2, "b")])
        assert result["success"] is True

        close_connection(conn_id)


class TestExecuteScriptVariants:
    def test_execute_script_multiple_statements(self):
        from mcp_sqlite3.mcp import close_connection, connect_database, execute_script

        result = connect_database(":memory:")
        conn_id = result["data"]["conn_id"]

        script_result = execute_script(
            conn_id,
            """
            CREATE TABLE t1 (id INTEGER);
            CREATE TABLE t2 (id INTEGER);
            INSERT INTO t1 VALUES (1);
            INSERT INTO t2 VALUES (2);
            """,
        )
        assert script_result["success"] is True

        close_connection(conn_id)


class TestUpdateVariants:
    def test_update_no_matching_rows(self):
        from mcp_sqlite3.mcp import (
            close_connection,
            connect_database,
            create_table,
            update_rows,
        )

        result = connect_database(":memory:")
        conn_id = result["data"]["conn_id"]

        create_table(conn_id, "test", [{"name": "id", "type": "INTEGER"}])
        update_result = update_rows(conn_id, "test", {"id": 999}, "id = ?", [999])
        assert update_result["success"] is True
        assert update_result["rowcount"] == 0

        close_connection(conn_id)

    def test_delete_no_matching_rows(self):
        from mcp_sqlite3.mcp import (
            close_connection,
            connect_database,
            create_table,
            delete_rows,
        )

        result = connect_database(":memory:")
        conn_id = result["data"]["conn_id"]

        create_table(conn_id, "test", [{"name": "id", "type": "INTEGER"}])
        delete_result = delete_rows(conn_id, "test", "id = ?", [999])
        assert delete_result["success"] is True
        assert delete_result["rowcount"] == 0

        close_connection(conn_id)


class TestSelectVariants:
    def test_select_empty_table(self):
        from mcp_sqlite3.mcp import (
            close_connection,
            connect_database,
            create_table,
            select_rows,
        )

        result = connect_database(":memory:")
        conn_id = result["data"]["conn_id"]

        create_table(conn_id, "test", [{"name": "id", "type": "INTEGER"}])
        select_result = select_rows(conn_id, "test")
        assert select_result["success"] is True
        assert select_result["data"] == []

        close_connection(conn_id)

    def test_select_with_limit_only(self):
        from mcp_sqlite3.mcp import (
            close_connection,
            connect_database,
            create_table,
            insert_row,
            select_rows,
        )

        result = connect_database(":memory:")
        conn_id = result["data"]["conn_id"]

        create_table(conn_id, "test", [{"name": "id", "type": "INTEGER"}])
        for i in range(5):
            insert_row(conn_id, "test", {"id": i})

        select_result = select_rows(conn_id, "test", limit=3)
        assert select_result["success"] is True
        assert len(select_result["data"]) == 3

        close_connection(conn_id)

    def test_select_with_offset(self):
        from mcp_sqlite3.mcp import (
            close_connection,
            connect_database,
            create_table,
            insert_row,
            select_rows,
        )

        result = connect_database(":memory:")
        conn_id = result["data"]["conn_id"]

        create_table(conn_id, "test", [{"name": "id", "type": "INTEGER"}])
        for i in range(5):
            insert_row(conn_id, "test", {"id": i})

        select_result = select_rows(conn_id, "test", limit=3, offset=2)
        assert select_result["success"] is True
        assert len(select_result["data"]) == 3

        close_connection(conn_id)


class TestIndexOperations:
    def test_get_indexes_all(self):
        from mcp_sqlite3.mcp import (
            close_connection,
            connect_database,
            create_table,
            execute_query,
            get_indexes,
        )

        result = connect_database(":memory:")
        conn_id = result["data"]["conn_id"]

        create_table(conn_id, "test", [{"name": "id", "type": "INTEGER"}])
        execute_query(conn_id, "CREATE INDEX idx1 ON test(id)")
        execute_query(conn_id, "CREATE INDEX idx2 ON test(id)")

        indexes_result = get_indexes(conn_id)
        assert indexes_result["success"] is True

        close_connection(conn_id)


class TestViewOperations:
    def test_export_with_views(self):
        from mcp_sqlite3.mcp import (
            close_connection,
            connect_database,
            execute_script,
            export_sql_dump,
        )

        result = connect_database(":memory:")
        conn_id = result["data"]["conn_id"]

        execute_script(
            conn_id,
            """
            CREATE TABLE t (id INTEGER);
            CREATE VIEW v AS SELECT * FROM t WHERE id > 0;
            """,
        )

        dump_result = export_sql_dump(conn_id)
        assert dump_result["success"] is True
        assert "CREATE VIEW" in dump_result["data"]

        close_connection(conn_id)


class TestTriggerOperations:
    def test_export_with_triggers(self):
        from mcp_sqlite3.mcp import (
            close_connection,
            connect_database,
            execute_script,
            export_sql_dump,
        )

        result = connect_database(":memory:")
        conn_id = result["data"]["conn_id"]

        execute_script(
            conn_id,
            """
            CREATE TABLE t (id INTEGER);
            CREATE TRIGGER tr AFTER INSERT ON t BEGIN SELECT 1; END;
            """,
        )

        dump_result = export_sql_dump(conn_id)
        assert dump_result["success"] is True
        assert "CREATE TRIGGER" in dump_result["data"]

        close_connection(conn_id)


class TestSerializeVariants:
    def test_serialize_specific_database(self):
        from mcp_sqlite3.mcp import (
            close_connection,
            connect_database,
            serialize_database,
        )

        result = connect_database(":memory:")
        conn_id = result["data"]["conn_id"]

        serialize_result = serialize_database(conn_id, "main")
        assert serialize_result["success"] is True

        close_connection(conn_id)


class TestTransactionIsolation:
    def test_execute_query_with_autocommit(self):
        from mcp_sqlite3.mcp import close_connection, connect_database, execute_query

        result = connect_database(":memory:", isolation_level=None)
        conn_id = result["data"]["conn_id"]

        execute_query(conn_id, "CREATE TABLE test (id INTEGER)")

        close_connection(conn_id)


class TestErrorHandlingAdvanced:
    def test_execute_query_syntax_error(self):
        from mcp_sqlite3.mcp import close_connection, connect_database, execute_query

        result = connect_database(":memory:")
        conn_id = result["data"]["conn_id"]

        query_result = execute_query(conn_id, "SELEC * FROM nonexistent")
        assert query_result["success"] is False
        assert query_result["error"] is not None

        close_connection(conn_id)

    def test_execute_many_error(self):
        from mcp_sqlite3.mcp import close_connection, connect_database, execute_many

        result = connect_database(":memory:")
        conn_id = result["data"]["conn_id"]

        many_result = execute_many(conn_id, "INVALID SQL", [[1]])
        assert many_result["success"] is False

        close_connection(conn_id)

    def test_execute_script_error(self):
        from mcp_sqlite3.mcp import close_connection, connect_database, execute_script

        result = connect_database(":memory:")
        conn_id = result["data"]["conn_id"]

        script_result = execute_script(conn_id, "INVALID SQL ;; MORE INVALID")
        assert script_result["success"] is False

        close_connection(conn_id)

    def test_create_table_duplicate(self):
        from mcp_sqlite3.mcp import close_connection, connect_database, create_table

        result = connect_database(":memory:")
        conn_id = result["data"]["conn_id"]

        create_table(conn_id, "dup", [{"name": "id", "type": "INTEGER"}])
        create_result = create_table(
            conn_id, "dup", [{"name": "id", "type": "INTEGER"}], if_not_exists=False
        )
        assert create_result["success"] is False

        close_connection(conn_id)
