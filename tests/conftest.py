import pytest


@pytest.fixture
def temp_db():
    """Create a temporary in-memory database for testing."""
    import sqlite3

    conn = sqlite3.connect(":memory:")
    yield conn
    conn.close()


@pytest.fixture
def sample_table_sql():
    """Sample SQL for creating a test table."""
    return """
    CREATE TABLE users (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL,
        email TEXT UNIQUE,
        age INTEGER,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP
    )
    """
