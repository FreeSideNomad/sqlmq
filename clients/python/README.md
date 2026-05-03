# sqlmq (Python client)

Strict pgmq-API-compatible Python client for [sqlmq](https://github.com/freesidenomad/sqlmq) on SQL Server.

The goal: drop-in replacement for [`tembo-pgmq-python`](https://pypi.org/project/tembo-pgmq-python/) `0.10.0`.
Code that previously imported `from tembo_pgmq_python import PGMQueue` should be portable by changing
*only* the import line and the connection params:

```diff
-from tembo_pgmq_python import PGMQueue
+from sqlmq import PGMQueue

-q = PGMQueue(host="localhost", port="5432", database="postgres",
-             username="postgres", password="postgres")
+q = PGMQueue(host="localhost", port="1433", database="myapp",
+             username="sa",       password="P@ssw0rd!")

 q.create_queue("jobs")
 mid = q.send("jobs", {"job_id": 42, "kind": "resize"})
 msg = q.read("jobs")
 q.delete("jobs", msg.msg_id)
```

Every other call site stays identical — same method names, same kwargs, same defaults,
same return types.

## Install

```bash
pip install sqlmq               # not yet on PyPI; install from source for now
pip install -e clients/python   # development install from this monorepo
```

You also need a Microsoft ODBC Driver for SQL Server installed on the host:

* macOS: `brew install msodbcsql18` (or `msodbcsql17`)
* Debian/Ubuntu: follow [Microsoft's instructions](https://learn.microsoft.com/sql/connect/odbc/linux-mac/installing-the-microsoft-odbc-driver-for-sql-server)
* Windows: ships with SQL Server tooling

The library auto-detects the highest installed version (18 preferred, 17 fallback).

## Quickstart

```python
from sqlmq import PGMQueue

q = PGMQueue(
    host="localhost", port="1433",
    database="sqlmq",
    username="sa", password="P@ssw0rd!",
)

q.create_queue("orders")
mid = q.send("orders", {"order_id": 42, "amount": 19.99})
msg = q.read("orders")
print(msg.message)           # {'order_id': 42, 'amount': 19.99}
q.delete("orders", msg.msg_id)
```

Async:

```python
import asyncio
from sqlmq import AsyncPGMQueue

async def main():
    q = AsyncPGMQueue(host=..., port=..., database=..., username=..., password=...)
    await q.init()
    await q.create_queue("orders")
    mid = await q.send("orders", {"order_id": 1})
    msg = await q.read("orders")
    await q.delete("orders", msg.msg_id)

asyncio.run(main())
```

## pgmq compatibility matrix

| pgmq method | sqlmq behavior | Notes |
|---|---|---|
| `create_queue(queue)` | OK | Maps to `EXEC sqlmq.create_queue` with `@storage='ondisk'`, `@grouped=0`, `@payload_type='json'`. |
| `create_queue(queue, unlogged=True)` | **`NotImplementedError`** | sqlmq retired its in-memory variant in V014 (it lost its perf advantage after durability hardening). |
| `create_partitioned_queue(...)` | **`NotImplementedError`** | Depends on `pg_partman`; no SQL Server equivalent. Tracked for a future release. |
| `drop_queue(queue)` | OK | Returns `True` if dropped, `False` if missing. `partitioned=True` raises `NotImplementedError` (sqlmq has no partitioned queues; mirrors the rejection in `create_partitioned_queue`). |
| `list_queues()` | OK | |
| `validate_queue_name(name)` | OK (client-side) | Mirrors V003 hardening: `^[A-Za-z_][A-Za-z0-9_]{0,59}$`. Raises `ValueError` on mismatch. |
| `send(queue, message, delay=0, headers=None)` | OK | `tz=` is rejected with `NotImplementedError` (sqlmq is UTC-only and accepts integer `delay_seconds`). `headers=` is a sqlmq extension to the strict pgmq 0.10 surface — JSON-encoded and stored on the row; visible to consumers via `Message.headers`. |
| `send_batch(queue, messages, delay=0, headers=None)` | OK | Single round trip: declares a `dbo.sqlmq_send_tvp` variable, populates via `INSERT VALUES`, then `EXEC sqlmq.send_batch`. Same `tz=` caveat. `headers=` (sqlmq extension) is applied uniformly to every message in the batch. |
| `read(queue, vt=None)` | OK | `vt` defaults to 30 (matches pgmq default). Returns `Message` or `None`. |
| `read_batch(queue, vt=None, batch_size=1)` | OK | Returns `[]` (not `None`) when empty — matches the pgmq client's actual runtime behavior. |
| `read_with_poll(queue, ...)` | OK (client-side polling) | sqlmq has no server-side long-poll (deliberate; see `research/sqlserver-longpoll.md`). We implement Hangfire-style backoff: loop calling `read_batch` with `time.sleep(poll_interval_ms/1000)` between empty results until first hit or `max_poll_seconds` elapsed. |
| `pop(queue)` | OK | Returns `Message` or `None`. (pgmq's type hint says `Message` but their impl actually returns `None` on empty queues; we mirror the actual behavior.) |
| `delete(queue, msg_id)` | OK | |
| `delete_batch(queue, msg_ids)` | OK (with extra round trip) | sqlmq's `delete` proc returns a row count, not the surviving id list. To honor pgmq's contract (return the deleted ids in caller order) we `SELECT` the live ids before and after the proc call and intersect. Cost: 2 extra round trips. |
| `archive(queue, msg_id)` | OK | |
| `archive_batch(queue, msg_ids)` | OK (with extra round trip) | Same shape note as `delete_batch`. |
| `purge(queue)` | OK | Returns the count purged. |
| `metrics(queue)` | OK | `scrape_time` is filled client-side from `datetime.utcnow()`. Both `oldest_msg_age_sec` and `newest_msg_age_sec` are surfaced from the proc (since V016). |
| `metrics_all()` | OK | Same shape as `metrics`. |
| `set_vt(queue, msg_id, vt)` | **`NotImplementedError`** | Tracked for a future release; would require a `sqlmq.set_vt` stored proc. |
| `detach_archive(queue)` | **`NotImplementedError`** | pgmq-specific feature for partitioned archive tables; sqlmq archives are single-table per queue. |

## sqlmq-extension features (small departures from strict pgmq 0.10)

This client is **mostly strict pgmq-compat**, with one small superset:

* `send(..., headers=...)` and `send_batch(..., headers=...)` accept an
  optional dict (sqlmq stores it on the row's `headers` column). The
  read-back `Message.headers` field surfaces it. pgmq 0.10's `send` does
  not carry headers, so consumers writing strict-pgmq-portable code can
  simply omit the kwarg.

The following sqlmq features are deliberately **not** exposed:

* grouped queues (`@grouped=1`) and `read_grouped`
* binary payloads (`@payload_type='binary'`)
* per-queue DLQ caps (`@max_delivery_count`) and `dlq_sweep`

If you need them, call the underlying T-SQL procedures directly via your own
pyodbc/SQLAlchemy connection — the schema is documented in `sql/migrations/`.

## Friendly aliases

For users who don't care about pgmq compat, two aliases are exported:

```python
from sqlmq import SqlmqQueue, AsyncSqlmqQueue   # same classes, friendlier names
```

## Development

```bash
cd clients/python
python3 -m venv .venv && source .venv/bin/activate
pip install -e .[dev]
pytest                          # runs the full suite (sync + async) against testcontainers
pytest -k "not async"           # sync only
pytest -k async                 # async only
```

Tests spin up `mcr.microsoft.com/mssql/server:2022-latest` once per pytest
invocation (via `testcontainers`), then create a fresh database and apply all
sqlmq migrations per test function.

## Property-based tests

`tests/test_properties.py` exercises the strict pgmq contract with
[hypothesis](https://hypothesis.readthedocs.io/) — 30 randomised examples per
property × 10 properties = 300 round-trip examples per run, each against a
freshly-created queue that is dropped on exit. Identical property names and
semantics live in the Java client's `PropertyIT.java` so the two libraries
share an invariant set.

Properties exercised:

| Id  | Property |
|-----|----------|
| P1  | send/read round-trip preserves the message |
| P2  | `send` returns monotonic, unique `msg_id`s per queue (V010 applock) |
| P3  | VT honored within a session (read-then-read returns `None`) |
| P4  | delete-then-read returns `None` |
| P5  | archive moves rather than copies (`queue_length` drops to 0) |
| P6  | purge clears the active queue |
| P7  | `send_batch` returns N ids in caller order |
| P8  | `delete_batch` with unknown ids returns `[]` |
| P9  | headers round-trip when sent (sqlmq-extension `headers=` kwarg) |
| P10 | sanity: `read` on an empty queue returns `None` / `[]` |

Run them in isolation:

```bash
cd clients/python
uv venv && uv pip install -e '.[dev]'
uv run pytest tests/test_properties.py -v
```

Wall-time is ~60 s on a warm Docker daemon.

## License

Apache 2.0 (matches the parent repository).
