"""Internal SQL helpers and stored-proc call shapes.

Centralizes the EXEC strings and the conversion helpers between Python types
and what pyodbc expects when invoking sqlmq stored procs.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any, Iterable, Sequence


# --- EXEC templates ---------------------------------------------------------
# All sqlmq procs take named @-parameters. pyodbc supports passing positional
# parameters into EXEC by name with `{call proc(?, ?)}` style, but the
# clearest spelling is plain `EXEC schema.proc ?, ?`. We use the latter.

CREATE_QUEUE = "{CALL sqlmq.create_queue (?, ?, ?, ?, ?)}"
DROP_QUEUE = "{CALL sqlmq.drop_queue (?)}"
LIST_QUEUES = "SELECT queue_name FROM sqlmq.queues()"
SEND = "{CALL sqlmq.send (?, ?, ?, ?, ?)}"
SEND_BATCH = "{CALL sqlmq.send_batch (?, ?)}"
READ = "{CALL sqlmq.[read] (?, ?, ?)}"
POP = "{CALL sqlmq.pop (?)}"
DELETE = "{CALL sqlmq.[delete] (?, ?)}"
ARCHIVE = "{CALL sqlmq.archive (?, ?, ?)}"
PURGE = "{CALL sqlmq.purge_queue (?)}"
METRICS = "{CALL sqlmq.metrics (?)}"
METRICS_ALL = "{CALL sqlmq.metrics_all}"


def encode_message(message: dict) -> str:
    """Serialize a message dict to the JSON string sqlmq stores."""
    return json.dumps(message, default=str)


def decode_message(raw: Any) -> dict:
    """Decode a message column back into a dict.

    sqlmq stores ``message`` as ``NVARCHAR(MAX)`` with ``CHECK (ISJSON=1)``,
    so we always parse it as JSON. Defensive against non-string values
    (some odbc drivers may return bytes for nvarchar columns under exotic
    configurations).
    """
    if raw is None:
        return {}
    if isinstance(raw, bytes):
        raw = raw.decode("utf-8")
    return json.loads(raw)


def utcnow() -> datetime:
    """Return current UTC time as a naive datetime, matching pgmq's shape."""
    return datetime.now(timezone.utc).replace(tzinfo=None)


def msg_id_tvp_rows(msg_ids: Iterable[int]) -> list[tuple[int]]:
    """Build the row sequence for a ``dbo.sqlmq_msg_id_tvp`` parameter."""
    return [(int(i),) for i in msg_ids]


def send_tvp_rows(messages: Sequence[dict], delay: int) -> list[tuple]:
    """Build the row sequence for a ``dbo.sqlmq_send_tvp`` parameter.

    Columns: (message NVARCHAR(MAX), message_bin VARBINARY(MAX),
              headers NVARCHAR(MAX), delay_seconds INT).
    For JSON-payload queues we always set message_bin/headers to None.
    """
    return [(encode_message(m), None, None, int(delay)) for m in messages]
