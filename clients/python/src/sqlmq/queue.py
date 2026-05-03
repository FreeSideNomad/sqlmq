"""Synchronous PGMQueue: strict pgmq-API-compatible client for sqlmq.

The public surface of this class mirrors ``tembo_pgmq_python.queue.PGMQueue``
exactly. Methods that have no sqlmq equivalent raise ``NotImplementedError``
with a message pointing at the underlying design choice.

See ``clients/python/README.md`` for the compatibility matrix.
"""

from __future__ import annotations

import logging
import os
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, List, Optional

import pyodbc

from .messages import Message, QueueMetrics
from . import _sql
from ._validation import validate_queue_name as _validate_name


def _build_conn_string(
    host: str,
    port: int | str,
    database: str,
    username: str,
    password: str,
    driver: Optional[str] = None,
    encrypt: bool = False,
    trust_server_certificate: bool = True,
    extra: Optional[str] = None,
) -> str:
    """Assemble an ODBC connection string for SQL Server."""
    if driver is None:
        # Pick the highest-version Microsoft ODBC driver that's installed.
        installed = pyodbc.drivers()
        for cand in ("ODBC Driver 18 for SQL Server", "ODBC Driver 17 for SQL Server"):
            if cand in installed:
                driver = cand
                break
        if driver is None:
            raise RuntimeError(
                "No Microsoft ODBC Driver for SQL Server is installed. "
                "Install msodbcsql18 (preferred) or msodbcsql17. "
                "See: https://learn.microsoft.com/sql/connect/odbc/download-odbc-driver-for-sql-server"
            )

    parts = [
        f"DRIVER={{{driver}}}",
        f"SERVER={host},{port}",
        f"DATABASE={database}",
        f"UID={username}",
        f"PWD={password}",
        f"Encrypt={'yes' if encrypt else 'no'}",
        f"TrustServerCertificate={'yes' if trust_server_certificate else 'no'}",
    ]
    if extra:
        parts.append(extra)
    return ";".join(parts) + ";"


@dataclass
class PGMQueue:
    """Strict pgmq-API-compatible client for sqlmq on SQL Server.

    The public method signatures match ``tembo_pgmq_python.PGMQueue`` from
    tembo-pgmq-python 0.10.0 byte-for-byte (kwargs, defaults, return types).
    Methods whose semantics cannot be honored (unlogged, partitioned, set_vt,
    detach_archive) raise ``NotImplementedError``.

    Connection: pass host/port/database/username/password (defaults sourced
    from ``MSSQL_*`` env vars), or pass ``conn_string=`` for full control.

    Each method also accepts an optional ``conn=`` kwarg for transaction
    composition, mirroring the pgmq client's escape hatch.
    """

    # Connection params (env-default, like tembo's PG_* shape).
    host: str = field(default_factory=lambda: os.getenv("MSSQL_HOST", "localhost"))
    port: str = field(default_factory=lambda: os.getenv("MSSQL_PORT", "1433"))
    database: str = field(default_factory=lambda: os.getenv("MSSQL_DATABASE", "sqlmq"))
    username: str = field(default_factory=lambda: os.getenv("MSSQL_USERNAME", "sa"))
    password: str = field(default_factory=lambda: os.getenv("MSSQL_PASSWORD", ""))

    # Defaults that match pgmq.
    delay: int = 0
    vt: int = 30

    # Pool sizing — pyodbc uses connection pooling at the driver-manager
    # level by default; pool_size is accepted for API compatibility but is
    # not authoritative.
    pool_size: int = 10

    # Escape hatch: caller-provided connection string.
    conn_string: Optional[str] = None

    # Misc compatibility-shape fields.
    kwargs: dict = field(default_factory=dict)
    verbose: bool = False
    log_filename: Optional[str] = None

    # Internal state (init=False).
    logger: logging.Logger = field(init=False)
    _conn_string: str = field(init=False)
    _lock: threading.Lock = field(init=False)
    _connections: list = field(init=False)

    def __post_init__(self) -> None:
        self._initialize_logging()
        if self.conn_string:
            self._conn_string = self.conn_string
        else:
            self._conn_string = _build_conn_string(
                self.host, self.port, self.database, self.username, self.password,
                **self.kwargs,
            )
        self._lock = threading.Lock()
        self._connections = []

    # ------------------------------------------------------------------ infra

    def _initialize_logging(self) -> None:
        self.logger = logging.getLogger(__name__)
        if self.verbose:
            log_filename = self.log_filename or datetime.utcnow().strftime(
                "sqlmq_debug_%Y%m%d_%H%M%S.log"
            )
            handler = logging.FileHandler(filename=os.path.join(os.getcwd(), log_filename))
            handler.setFormatter(
                logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
            )
            self.logger.addHandler(handler)
            self.logger.setLevel(logging.DEBUG)
        else:
            self.logger.setLevel(logging.WARNING)

    def _new_connection(self) -> pyodbc.Connection:
        """Create a fresh autocommit pyodbc connection.

        We rely on the ODBC driver-manager's process-wide connection pool
        for reuse; opening many connections is cheap once warmed up.
        """
        conn = pyodbc.connect(self._conn_string, autocommit=True)
        return conn

    class _CursorScope:
        """Context manager yielding (conn, cursor). Closes cursor; closes conn
        only if it owns it (i.e., caller did not pass one in)."""

        def __init__(self, owner: "PGMQueue", external_conn: Optional[Any]) -> None:
            self.owner = owner
            self.external_conn = external_conn
            self.conn: Optional[pyodbc.Connection] = None
            self.cursor: Optional[pyodbc.Cursor] = None

        def __enter__(self):
            if self.external_conn is not None:
                self.conn = self.external_conn
                self.cursor = self.conn.cursor()
            else:
                self.conn = self.owner._new_connection()
                self.cursor = self.conn.cursor()
            return self.conn, self.cursor

        def __exit__(self, exc_type, exc, tb):
            if self.cursor is not None:
                try:
                    self.cursor.close()
                except Exception:
                    pass
            if self.external_conn is None and self.conn is not None:
                try:
                    self.conn.close()
                except Exception:
                    pass
            return False

    def _scope(self, conn: Optional[Any]) -> "_CursorScope":
        return PGMQueue._CursorScope(self, conn)

    @staticmethod
    def _drain_to_first_resultset(cursor: pyodbc.Cursor) -> Optional[list]:
        """Fetch the first non-empty resultset's rows.

        sqlmq stored procs use ``EXEC sp_executesql`` internally, which can
        emit a leading empty resultset before the meaningful one. Walk through
        until we find one with column metadata, fetchall, and stop.
        """
        while True:
            if cursor.description is not None:
                return cursor.fetchall()
            if not cursor.nextset():
                return None

    @staticmethod
    def _drain_with_description(cursor: pyodbc.Cursor) -> tuple[list, Optional[Any]]:
        """Like ``_drain_to_first_resultset`` but also returns the cursor's
        description tuple, so callers can decode columns by name."""
        while True:
            if cursor.description is not None:
                desc = cursor.description
                return cursor.fetchall(), desc
            if not cursor.nextset():
                return [], None

    # ------------------------------------------------------------- pgmq surface

    def validate_queue_name(self, queue_name: str, conn=None) -> None:
        """Validate a queue name. Raises ``ValueError`` on mismatch.

        pgmq's server-side proc raises a SQL error; we mirror by raising
        ``ValueError`` client-side, which is the same behavior callers see
        through the pgmq client (errors propagate from the cursor).
        """
        _validate_name(queue_name)

    def create_queue(self, queue: str, unlogged: bool = False, conn=None) -> None:
        """Create a queue.

        ``unlogged=True`` raises ``NotImplementedError``: sqlmq's in-memory
        variant was retired in V014 (see docs/superpowers/archive/ for the
        rationale — its perf advantage did not survive durability hardening).
        """
        if unlogged:
            raise NotImplementedError(
                "sqlmq does not support unlogged queues; the in-memory variant was "
                "retired in V014. See docs/superpowers/archive/ for rationale."
            )
        _validate_name(queue)
        with self._scope(conn) as (_c, cur):
            # Positional args map to: @name, @storage, @grouped, @payload_type,
            # @max_delivery_count.  We hard-code @storage='ondisk' since it's
            # the only option after V015 retired the inmemory backend.
            cur.execute(_sql.CREATE_QUEUE, queue, "ondisk", 0, "json", None)

    def create_partitioned_queue(
        self,
        queue: str,
        partition_interval: int = 10000,
        retention_interval: int = 100000,
        conn=None,
    ) -> None:
        """Always raises ``NotImplementedError``."""
        raise NotImplementedError(
            "sqlmq does not support partitioned queues. pgmq's partitioning "
            "depends on pg_partman, which has no SQL Server equivalent. "
            "Tracked for a future release."
        )

    def drop_queue(self, queue: str, partitioned: bool = False, conn=None) -> bool:
        """Drop a queue. Returns ``True`` if dropped, ``False`` if it didn't exist.

        ``partitioned`` is accepted for signature compatibility but ignored —
        sqlmq has no partitioned queues.
        """
        _validate_name(queue)
        with self._scope(conn) as (_c, cur):
            try:
                cur.execute(_sql.DROP_QUEUE, queue)
                return True
            except pyodbc.Error as exc:
                # sqlmq throws 50010 'Queue does not exist.' for missing queues.
                # pgmq's drop_queue returns False in that case; mirror.
                if "Queue does not exist" in str(exc):
                    return False
                raise

    def list_queues(self, conn=None) -> List[str]:
        """Return all queue names."""
        with self._scope(conn) as (_c, cur):
            cur.execute(_sql.LIST_QUEUES)
            return [row[0] for row in cur.fetchall()]

    def send(
        self,
        queue: str,
        message: dict,
        delay: int = 0,
        tz: Optional[datetime] = None,
        conn=None,
    ) -> int:
        """Send a single message. Returns the new ``msg_id``.

        ``tz`` (delay-until-timestamp) is not supported — sqlmq operates in
        UTC and only accepts ``delay_seconds``. Passing ``tz`` raises
        ``NotImplementedError`` rather than silently being ignored.
        """
        if tz is not None:
            raise NotImplementedError(
                "sqlmq.send does not support a tz/timestamp deadline; sqlmq "
                "operates exclusively in UTC and only accepts integer "
                "delay_seconds. Pre-compute the offset and pass `delay=`."
            )
        _validate_name(queue)
        encoded = _sql.encode_message(message)
        with self._scope(conn) as (_c, cur):
            # @queue, @message, @message_bin, @headers, @delay_seconds
            cur.execute(_sql.SEND, queue, encoded, None, None, int(delay))
            rows = self._drain_to_first_resultset(cur)
            if not rows:
                raise RuntimeError("sqlmq.send returned no rows; expected msg_id")
            return int(rows[0][0])

    def send_batch(
        self,
        queue: str,
        messages: List[dict],
        delay: int = 0,
        tz: Optional[datetime] = None,
        conn=None,
    ) -> List[int]:
        """Send a batch of messages. Returns the new msg_ids in caller order.

        sqlmq's ``send_batch`` proc takes a TVP. pyodbc has no native TVP
        passing for ``EXEC``, so we emit a single batch that declares a TVP
        variable, populates it via ``INSERT...VALUES``, and ``EXEC``s the
        proc. One round trip per ``send_batch`` call.
        """
        if tz is not None:
            raise NotImplementedError(
                "sqlmq.send_batch does not support tz; pass `delay=` instead."
            )
        _validate_name(queue)
        if not messages:
            return []

        rows = _sql.send_tvp_rows(messages, delay)
        # rows: [(message, message_bin, headers, delay_seconds), ...]
        # Build a parameterized VALUES clause. Each row contributes 4 params.
        values_clause = ",".join(["(?, ?, ?, ?)"] * len(rows))
        sql = (
            "DECLARE @t dbo.sqlmq_send_tvp; "
            f"INSERT INTO @t (message, message_bin, headers, delay_seconds) VALUES {values_clause}; "
            "EXEC sqlmq.send_batch @queue = ?, @messages = @t;"
        )
        # pyodbc execute params are positional and flat; we flatten rows then
        # append the @queue param at the end.
        params: list[Any] = []
        for r in rows:
            params.extend(r)
        params.append(queue)

        with self._scope(conn) as (_c, cur):
            cur.execute(sql, *params)
            result_rows = self._drain_to_first_resultset(cur)
            if result_rows is None:
                return []
            return [int(r[0]) for r in result_rows]

    @staticmethod
    def _row_to_message(row: Any, description: Any) -> Message:
        """Map a sqlmq result row to a Message dataclass.

        Different procs return slightly different column shapes:
        ``sqlmq.read`` returns
            (msg_id, read_ct, enqueued_at, vt, group_key, message, headers)
        but ``sqlmq.pop`` returns
            (msg_id, read_ct, enqueued_at, vt, message, headers).
        We drive off the cursor's ``description`` to find the ``message``
        column by name rather than by positional offset.
        """
        names = [d[0] for d in description]
        idx = names.index("message")
        return Message(
            msg_id=int(row[0]),
            read_ct=int(row[1]),
            enqueued_at=row[2],
            vt=row[3],
            message=_sql.decode_message(row[idx]),
        )

    def read(self, queue: str, vt: Optional[int] = None, conn=None) -> Optional[Message]:
        """Read a single message. Returns ``None`` if the queue is empty."""
        _validate_name(queue)
        vt_secs = vt if vt is not None else self.vt
        with self._scope(conn) as (_c, cur):
            cur.execute(_sql.READ, queue, int(vt_secs), 1)
            rows, desc = self._drain_with_description(cur)
            if not rows:
                return None
            return self._row_to_message(rows[0], desc)

    def read_batch(
        self, queue: str, vt: Optional[int] = None, batch_size: int = 1, conn=None
    ) -> Optional[List[Message]]:
        """Read up to ``batch_size`` messages. Returns ``[]`` (not ``None``)
        when the queue is empty, mirroring pgmq's actual behavior."""
        _validate_name(queue)
        vt_secs = vt if vt is not None else self.vt
        with self._scope(conn) as (_c, cur):
            cur.execute(_sql.READ, queue, int(vt_secs), int(batch_size))
            rows, desc = self._drain_with_description(cur)
            return [self._row_to_message(r, desc) for r in rows]

    def read_with_poll(
        self,
        queue: str,
        vt: Optional[int] = None,
        qty: int = 1,
        max_poll_seconds: int = 5,
        poll_interval_ms: int = 100,
        conn=None,
    ) -> Optional[List[Message]]:
        """Poll-read messages, blocking up to ``max_poll_seconds``.

        sqlmq has no server-side long-poll equivalent (deliberate per design;
        see ``research/sqlserver-longpoll.md``). We implement a Hangfire-style
        client-side backoff: call ``read_batch`` repeatedly, sleeping
        ``poll_interval_ms`` between empty reads until either we get a
        non-empty result or the deadline elapses.
        """
        _validate_name(queue)
        deadline = time.monotonic() + max_poll_seconds
        sleep_s = max(poll_interval_ms, 1) / 1000.0
        while True:
            msgs = self.read_batch(queue, vt=vt, batch_size=qty, conn=conn)
            if msgs:
                return msgs
            if time.monotonic() >= deadline:
                return msgs  # [] — pgmq returns the empty list, we mirror.
            time.sleep(sleep_s)

    def pop(self, queue: str, conn=None) -> Optional[Message]:
        """Atomically read+delete one message. Returns ``None`` if empty.

        pgmq's type hint says ``Message`` but in practice their client returns
        ``None`` for an empty queue (their ``messages[0]`` would IndexError —
        the type hint is wrong). We mirror the actual behavior, returning
        ``None`` rather than raising.
        """
        _validate_name(queue)
        with self._scope(conn) as (_c, cur):
            cur.execute(_sql.POP, queue)
            rows, desc = self._drain_with_description(cur)
            if not rows:
                return None
            return self._row_to_message(rows[0], desc)

    def _exec_msg_id_tvp(
        self,
        proc_call: str,
        queue: str,
        msg_ids: List[int],
        conn: Optional[Any],
        extra_param: Optional[Any] = None,
    ) -> int:
        """Call a proc that takes (queue, msg_id_tvp[, extra]) and returns a
        ROWCOUNT-style result. Returns the integer result."""
        if not msg_ids:
            return 0
        values_clause = ",".join(["(?)"] * len(msg_ids))
        if "archive" in proc_call.lower():
            sql = (
                "DECLARE @ids dbo.sqlmq_msg_id_tvp; "
                f"INSERT INTO @ids (msg_id) VALUES {values_clause}; "
                "EXEC sqlmq.archive @queue = ?, @msg_ids = @ids, @reason = ?;"
            )
        else:
            sql = (
                "DECLARE @ids dbo.sqlmq_msg_id_tvp; "
                f"INSERT INTO @ids (msg_id) VALUES {values_clause}; "
                "EXEC sqlmq.[delete] @queue = ?, @msg_ids = @ids;"
            )
        params: list[Any] = [int(i) for i in msg_ids]
        params.append(queue)
        if "archive" in proc_call.lower():
            params.append(extra_param)
        with self._scope(conn) as (_c, cur):
            cur.execute(sql, *params)
            rows = self._drain_to_first_resultset(cur) or []
            if not rows:
                return 0
            return int(rows[0][0])

    def delete(self, queue: str, msg_id: int, conn=None) -> bool:
        """Delete one message. Returns ``True`` iff a row was actually deleted."""
        _validate_name(queue)
        n = self._exec_msg_id_tvp("delete", queue, [msg_id], conn)
        return n > 0

    def delete_batch(self, queue: str, msg_ids: List[int], conn=None) -> List[int]:
        """Delete a batch of messages.

        Returns the list of msg_ids that were actually deleted, matching
        pgmq's contract.

        Note: sqlmq's ``delete`` proc returns a count, not the surviving id
        list. To honor the pgmq shape we run the deletes inside an explicit
        transaction and read the affected rows back via OUTPUT — but since
        the proc encapsulates the DELETE, we instead re-derive the deleted
        set by SELECTing the live ids before the call and intersecting.
        """
        _validate_name(queue)
        if not msg_ids:
            return []
        # Cheapest correct strategy: ask sqlmq to delete, then check which of
        # the requested ids no longer exist. Since sqlmq does not expose the
        # OUTPUT row set through the proc, this two-step approach is the only
        # honest way to satisfy pgmq's contract without modifying the proc.
        # (See the README compat notes for why we accept the extra round trip.)
        with self._scope(conn) as (_c, cur):
            qtable = "q_" + queue
            placeholders = ",".join(["?"] * len(msg_ids))
            cur.execute(
                f"SELECT msg_id FROM sqlmq.[{qtable}] WHERE msg_id IN ({placeholders})",
                *[int(i) for i in msg_ids],
            )
            present_before = {int(r[0]) for r in cur.fetchall()}

            n = self._exec_msg_id_tvp("delete", queue, list(present_before), conn)
            if n == 0:
                return []

            cur.execute(
                f"SELECT msg_id FROM sqlmq.[{qtable}] WHERE msg_id IN ({placeholders})",
                *[int(i) for i in msg_ids],
            )
            present_after = {int(r[0]) for r in cur.fetchall()}
            deleted = present_before - present_after
            # Preserve caller-supplied order, like pgmq.
            return [i for i in msg_ids if i in deleted]

    def archive(self, queue: str, msg_id: int, conn=None) -> bool:
        """Archive one message. Returns ``True`` iff a row was actually moved."""
        _validate_name(queue)
        n = self._exec_msg_id_tvp("archive", queue, [msg_id], conn, extra_param=None)
        return n > 0

    def archive_batch(self, queue: str, msg_ids: List[int], conn=None) -> List[int]:
        """Archive a batch of messages. Returns the ids actually archived.

        Same shape note as ``delete_batch``: sqlmq.archive returns a count,
        so we round-trip a SELECT to derive the surviving set.
        """
        _validate_name(queue)
        if not msg_ids:
            return []
        with self._scope(conn) as (_c, cur):
            qtable = "q_" + queue
            placeholders = ",".join(["?"] * len(msg_ids))
            cur.execute(
                f"SELECT msg_id FROM sqlmq.[{qtable}] WHERE msg_id IN ({placeholders})",
                *[int(i) for i in msg_ids],
            )
            present_before = {int(r[0]) for r in cur.fetchall()}

            n = self._exec_msg_id_tvp("archive", queue, list(present_before), conn, extra_param=None)
            if n == 0:
                return []

            cur.execute(
                f"SELECT msg_id FROM sqlmq.[{qtable}] WHERE msg_id IN ({placeholders})",
                *[int(i) for i in msg_ids],
            )
            present_after = {int(r[0]) for r in cur.fetchall()}
            archived = present_before - present_after
            return [i for i in msg_ids if i in archived]

    def purge(self, queue: str, conn=None) -> int:
        """Delete every message in a queue. Returns the count purged."""
        _validate_name(queue)
        with self._scope(conn) as (_c, cur):
            cur.execute(_sql.PURGE, queue)
            rows = self._drain_to_first_resultset(cur) or []
            return int(rows[0][0]) if rows else 0

    def metrics(self, queue: str, conn=None) -> QueueMetrics:
        """Return metrics for a single queue."""
        _validate_name(queue)
        with self._scope(conn) as (_c, cur):
            cur.execute(_sql.METRICS, queue)
            rows = self._drain_to_first_resultset(cur) or []
            if not rows:
                raise RuntimeError(f"sqlmq.metrics returned no rows for queue {queue!r}")
            row = rows[0]
            # sqlmq columns: queue_name, queue_length, total_messages,
            # oldest_msg_age_seconds, dlq_count.
            return QueueMetrics(
                queue_name=str(row[0]),
                queue_length=int(row[1] or 0),
                newest_msg_age_sec=None,
                oldest_msg_age_sec=int(row[3]) if row[3] is not None else None,
                total_messages=int(row[2] or 0),
                scrape_time=_sql.utcnow(),
            )

    def metrics_all(self, conn=None) -> List[QueueMetrics]:
        """Return metrics for every queue."""
        with self._scope(conn) as (_c, cur):
            cur.execute(_sql.METRICS_ALL)
            rows = self._drain_to_first_resultset(cur) or []
            now = _sql.utcnow()
            out = []
            for row in rows:
                out.append(
                    QueueMetrics(
                        queue_name=str(row[0]),
                        queue_length=int(row[1] or 0),
                        newest_msg_age_sec=None,
                        oldest_msg_age_sec=int(row[3]) if row[3] is not None else None,
                        total_messages=int(row[2] or 0),
                        scrape_time=now,
                    )
                )
            return out

    def set_vt(self, queue: str, msg_id: int, vt: int, conn=None) -> Message:
        """Always raises ``NotImplementedError``."""
        raise NotImplementedError(
            "sqlmq does not yet support set_vt. Tracked for a future release; "
            "would require adding a sqlmq.set_vt stored proc."
        )

    def detach_archive(self, queue: str, conn=None) -> None:
        """Always raises ``NotImplementedError``."""
        raise NotImplementedError(
            "sqlmq does not support detach_archive (a pgmq-specific feature for "
            "partitioned archive tables; sqlmq archives are single-table per queue)."
        )
