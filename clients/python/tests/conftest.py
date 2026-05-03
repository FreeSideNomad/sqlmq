"""Test infrastructure: testcontainers SQL Server + per-test database.

Pattern (mirrors the Java harness):
* Session-scoped SQL Server 2022 container (one per pytest invocation).
* Function-scoped fresh database with all sqlmq migrations applied.
* Each test gets a fresh PGMQueue / AsyncPGMQueue pointed at that database.

Migrations are applied without Flyway: we read the .sql files from
``sql/migrations/`` and execute them, splitting on ``GO`` (the T-SQL batch
separator) the same way ``sqlcmd`` and Flyway do internally.
"""

from __future__ import annotations

import os
import re
import time
import uuid
from pathlib import Path

import pyodbc
import pytest
from testcontainers.mssql import SqlServerContainer

REPO_ROOT = Path(__file__).resolve().parents[3]
MIGRATIONS_DIR = REPO_ROOT / "sql" / "migrations"


# --- container ------------------------------------------------------------

@pytest.fixture(scope="session")
def mssql_container():
    """Spin up SQL Server 2022 once per test session."""
    img = "mcr.microsoft.com/mssql/server:2022-latest"
    container = SqlServerContainer(image=img, dialect="mssql+pyodbc")
    container.start()
    try:
        # SqlServerContainer exposes only sqlalchemy-shaped accessors; pull
        # the bits we need for raw pyodbc connections.
        host = container.get_container_host_ip()
        port = int(container.get_exposed_port(container.port))
        user = container.username
        password = container.password
        # Wait until SQL Server is actually accepting logins (not just up).
        _wait_for_login(host, port, user, password)
        yield {
            "host": host,
            "port": port,
            "username": user,
            "password": password,
        }
    finally:
        container.stop()


def _driver_name() -> str:
    installed = pyodbc.drivers()
    for cand in ("ODBC Driver 18 for SQL Server", "ODBC Driver 17 for SQL Server"):
        if cand in installed:
            return cand
    raise RuntimeError(
        f"No Microsoft ODBC Driver for SQL Server installed; have: {installed}"
    )


def _admin_conn_string(host: str, port: int, user: str, password: str, database: str = "master") -> str:
    return (
        f"DRIVER={{{_driver_name()}}};"
        f"SERVER={host},{port};"
        f"DATABASE={database};"
        f"UID={user};PWD={password};"
        f"Encrypt=no;TrustServerCertificate=yes;"
    )


def _wait_for_login(host: str, port: int, user: str, password: str, timeout_s: int = 90) -> None:
    """Poll until a connect succeeds. Testcontainers' wait_for_logs isn't
    deterministic for SQL Server's ready signal."""
    deadline = time.time() + timeout_s
    last_exc: Exception | None = None
    while time.time() < deadline:
        try:
            with pyodbc.connect(_admin_conn_string(host, port, user, password), autocommit=True) as conn:
                conn.cursor().execute("SELECT 1").fetchone()
            return
        except Exception as e:
            last_exc = e
            time.sleep(1.5)
    raise RuntimeError(f"SQL Server never became ready: {last_exc}")


# --- migration runner -----------------------------------------------------

# Match `GO` on its own line (case-insensitive, with optional whitespace).
_GO_SPLIT_RE = re.compile(r"^\s*GO\s*$", re.IGNORECASE | re.MULTILINE)


def _apply_migrations(host: str, port: int, user: str, password: str, database: str) -> None:
    """Apply every V*.sql migration in version order against ``database``."""
    files = sorted(MIGRATIONS_DIR.glob("V*.sql"))
    if not files:
        raise RuntimeError(f"No migrations found in {MIGRATIONS_DIR}")
    conn_str = _admin_conn_string(host, port, user, password, database=database)
    with pyodbc.connect(conn_str, autocommit=True) as conn:
        cur = conn.cursor()
        for f in files:
            sql = f.read_text(encoding="utf-8")
            for batch in _GO_SPLIT_RE.split(sql):
                stmt = batch.strip()
                if not stmt:
                    continue
                try:
                    cur.execute(stmt)
                    # Drain any result sets to avoid out-of-order issues.
                    while True:
                        if cur.description is not None:
                            cur.fetchall()
                        if not cur.nextset():
                            break
                except pyodbc.Error as exc:
                    raise RuntimeError(
                        f"Migration {f.name} failed on batch:\n{stmt[:400]}...\n{exc}"
                    ) from exc


# --- per-test database ----------------------------------------------------

@pytest.fixture
def fresh_db(mssql_container):
    """Create a fresh database, apply migrations, drop it after the test."""
    info = mssql_container
    db_name = "sqlmq_pytest_" + uuid.uuid4().hex
    admin_str = _admin_conn_string(info["host"], info["port"], info["username"], info["password"])
    with pyodbc.connect(admin_str, autocommit=True) as conn:
        conn.cursor().execute(f"CREATE DATABASE [{db_name}]")
    try:
        _apply_migrations(info["host"], info["port"], info["username"], info["password"], db_name)
        yield {**info, "database": db_name}
    finally:
        with pyodbc.connect(admin_str, autocommit=True) as conn:
            cur = conn.cursor()
            try:
                cur.execute(f"ALTER DATABASE [{db_name}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE")
                cur.execute(f"DROP DATABASE [{db_name}]")
            except pyodbc.Error:
                pass


@pytest.fixture
def queue_factory(fresh_db):
    """Return a callable producing a fresh sync PGMQueue against the test db."""
    from sqlmq import PGMQueue

    created: list[PGMQueue] = []

    def _make(**overrides) -> PGMQueue:
        q = PGMQueue(
            host=fresh_db["host"],
            port=str(fresh_db["port"]),
            database=fresh_db["database"],
            username=fresh_db["username"],
            password=fresh_db["password"],
            **overrides,
        )
        created.append(q)
        return q

    yield _make


@pytest.fixture
def async_queue_factory(fresh_db):
    """Return a callable producing a fresh AsyncPGMQueue against the test db."""
    from sqlmq import AsyncPGMQueue

    created: list[AsyncPGMQueue] = []

    def _make(**overrides) -> AsyncPGMQueue:
        q = AsyncPGMQueue(
            host=fresh_db["host"],
            port=str(fresh_db["port"]),
            database=fresh_db["database"],
            username=fresh_db["username"],
            password=fresh_db["password"],
            **overrides,
        )
        created.append(q)
        return q

    yield _make


def unique_queue_name(prefix: str = "q") -> str:
    """Generate a unique 60-char-safe queue name for a test case."""
    return f"{prefix}_{uuid.uuid4().hex[:16]}"
