# pgmq Technical Surface Report

**Source**: https://github.com/pgmq/pgmq (cloned `main`, 2026-05-02)
**Version pinned in `pgmq.control`**: `1.11.1` (`pgmq-extension/pgmq.control:2`)
**Postgres support**: 14–18 (`README.md:24`)
**All citations are to files in the upstream repo unless otherwise noted.**

The schema name everything lives under is `pgmq`. Per-queue tables are named:
- `pgmq.q_<queue_name>` — active queue (`pgmq-extension/sql/pgmq.sql:294`, `format_table_name`)
- `pgmq.a_<queue_name>` — archive
Queue names are limited to 47 chars (`pgmq.sql:1108`) so the full identifier ≤ 63.

---

## 1. Public API surface

All functions live in schema `pgmq`. Unless noted, every function is implemented as `LANGUAGE plpgsql` or `LANGUAGE sql` and runs **inside the caller's transaction** — there is no autonomous transaction. This is the central transactional property: send/read/delete/archive participate in the caller's BEGIN/COMMIT/ROLLBACK. The isolation test at `pgmq-extension/test/specs/transaction_tests.spec` and its expected output `pgmq-extension/test/expected/transaction_tests.out` verify this:

- An uncommitted `send` is invisible to other sessions' `read` (`transaction_tests.out:17-21`).
- A `read` that is rolled back releases the message back to the queue (`transaction_tests.out:51-61`).

### 1.1 Queue lifecycle

| Function | Signature | Returns | Notes |
|---|---|---|---|
| `pgmq.create(queue_name TEXT)` | `pgmq.sql:1471` | void | Wraps `create_non_partitioned`. |
| `pgmq.create_non_partitioned(queue_name TEXT)` | `pgmq.sql:1138` | void | Creates `q_*` and `a_*` tables, `_vt_idx`, `archived_at_idx_*`, inserts into `pgmq.meta`. Holds an xact-level advisory lock per queue (`acquire_queue_lock`). |
| `pgmq.create_unlogged(queue_name TEXT)` | `pgmq.sql:1206` | void | Same as above but the **active queue table is `UNLOGGED`** (line 1218). Archive table is **logged**. |
| `pgmq.create_partitioned(queue_name TEXT, partition_interval TEXT DEFAULT '10000', retention_interval TEXT DEFAULT '100000')` | `pgmq.sql:1317` | void | Requires `pg_partman`. See §6. |
| `pgmq.convert_archive_partitioned(table_name TEXT, partition_interval TEXT DEFAULT '10000', retention_interval TEXT DEFAULT '100000', leading_partition INT DEFAULT 10)` | `pgmq.sql:1518` | void | Renames existing `a_*` to `*_old`, recreates as partitioned. Migration helper. |
| `pgmq.drop_queue(queue_name TEXT)` | `pgmq.sql:1037` | boolean | Drops `q_*` and `a_*`, deletes from `meta`, removes pg_partman config rows if partitioned. Acquires advisory lock. |
| `pgmq.drop_queue(queue_name TEXT, partitioned BOOLEAN)` | `pgmq.sql:1021` | boolean | **DEPRECATED**, raises a WARNING and forwards to single-arg version. To be removed in v2.0. |
| `pgmq.detach_archive(queue_name TEXT)` | `pgmq.sql:912` | void | **DEPRECATED no-op**. Logs a WARNING. To be removed in v2.0. Archive tables are no longer extension members. |
| `pgmq.list_queues()` | `pgmq.sql:886` | `SETOF pgmq.queue_record` | `SELECT * FROM pgmq.meta`. |
| `pgmq.validate_queue_name(queue_name TEXT)` | `pgmq.sql:1105` | void | Throws if name > 47 chars. |
| `pgmq.format_table_name(queue_name TEXT, prefix TEXT)` | `pgmq.sql:287` | text | Internal helper; rejects `$`, `;`, `--`, `'`. |
| `pgmq.acquire_queue_lock(queue_name TEXT)` | `pgmq.sql:113` | void | `pg_advisory_xact_lock(hashtext('pgmq.queue_'||queue_name))`. Used inside create/drop to serialize. |

### 1.2 Send

All `send*` overloads return `SETOF BIGINT` (the assigned `msg_id`s).

| Function | Signature | Loc |
|---|---|---|
| `pgmq.send(queue_name, msg)` | 2-arg, no headers/delay | `pgmq.sql:673` |
| `pgmq.send(queue_name, msg, headers JSONB)` | 3-arg | `pgmq.sql:681` |
| `pgmq.send(queue_name, msg, delay INTEGER)` | 3-arg, integer delay seconds | `pgmq.sql:690` |
| `pgmq.send(queue_name, msg, delay TIMESTAMPTZ)` | 3-arg, absolute delay | `pgmq.sql:699` |
| `pgmq.send(queue_name, msg, headers JSONB, delay INTEGER)` | 4-arg | `pgmq.sql:708` |
| `pgmq.send(queue_name, msg, headers JSONB, delay TIMESTAMPTZ)` | 4-arg, base implementation | `pgmq.sql:650` |

The base implementation is just:

```sql
INSERT INTO pgmq.%I (vt, message, headers) VALUES ($2, $1, $3) RETURNING msg_id;
```

(`pgmq.sql:660-666`). `delay` is the **initial value of `vt`**, not an offset — `clock_timestamp()` is used in the no-delay case (`pgmq.sql:677`). Integer-delay overloads compute `clock_timestamp() + make_interval(secs => delay)`.

`send_batch` overloads return `SETOF BIGINT` (one row per inserted msg). Same 6-overload shape (`pgmq.sql:761,774,782,791,800,809`). Implementation uses `unnest()`:

```sql
INSERT INTO pgmq.%I (vt, message, headers)
SELECT $2, unnest($1), unnest(coalesce($3, ARRAY[]::jsonb[]))
RETURNING msg_id;
```
(`pgmq.sql:750-752`). Validation in `_validate_batch_params` (`pgmq.sql:718`) requires `headers` length match `msgs` length when non-NULL; empty `msgs` raises.

**Topic send** (publish-subscribe via routing key):

| Function | Loc | Returns |
|---|---|---|
| `pgmq.send_topic(routing_key, msg)` | `pgmq.sql:1963` | integer (count of queues delivered to) |
| `pgmq.send_topic(routing_key, msg, delay INTEGER)` | `pgmq.sql:1974` | integer |
| `pgmq.send_topic(routing_key, msg, headers JSONB, delay INTEGER)` | `pgmq.sql:1927` | integer |
| `pgmq.send_batch_topic(routing_key, msgs)` | `pgmq.sql:2059` | `TABLE(queue_name TEXT, msg_id BIGINT)` |
| `pgmq.send_batch_topic(routing_key, msgs, headers)` | `pgmq.sql:2072` | same |
| `pgmq.send_batch_topic(routing_key, msgs, delay INTEGER)` | `pgmq.sql:2086` | same |
| `pgmq.send_batch_topic(routing_key, msgs, delay TIMESTAMPTZ)` | `pgmq.sql:2100` | same |
| `pgmq.send_batch_topic(routing_key, msgs, headers, delay INTEGER)` | `pgmq.sql:2114` | same |
| `pgmq.send_batch_topic(routing_key, msgs, headers, delay TIMESTAMPTZ)` | `pgmq.sql:2021` | same (base impl) |

`send_topic` looks up matching `topic_bindings` by `routing_key ~ compiled_regex` and calls `pgmq.send` per matched queue (`pgmq.sql:1949-1957`). All sends happen inside one transaction — partial failure rolls back everything.

### 1.3 Read / Poll

All read functions return `SETOF pgmq.message_record` (defined `pgmq.sql:89`):
```
msg_id BIGINT, read_ct INTEGER, enqueued_at TIMESTAMPTZ, last_read_at TIMESTAMPTZ,
vt TIMESTAMPTZ, message JSONB, headers JSONB
```

| Function | Signature | Loc |
|---|---|---|
| `pgmq.read(queue_name, vt INTEGER, qty INTEGER, conditional JSONB DEFAULT '{}')` | `pgmq.sql:300` | basic read, conditional filter via JSONB containment `@>` |
| `pgmq.read_with_poll(queue_name, vt, qty, max_poll_seconds INTEGER DEFAULT 5, poll_interval_ms INTEGER DEFAULT 100, conditional JSONB DEFAULT '{}')` | `pgmq.sql:476` | long-poll; sleeps via `pg_sleep` |
| `pgmq.read_grouped(queue_name, vt, qty)` | `pgmq.sql:343` | SQS-style FIFO, fills batch from earliest group |
| `pgmq.read_grouped_with_poll(...)` | `pgmq.sql:442` | poll variant |
| `pgmq.read_grouped_rr(queue_name, vt, qty)` | `pgmq.sql:122` | round-robin layered across groups |
| `pgmq.read_grouped_rr_with_poll(...)` | `pgmq.sql:207` | poll variant |
| `pgmq.read_grouped_head(queue_name, vt, qty)` | `pgmq.sql:241` | one message per group, up to `qty` groups |

**Conditional filter** (`pgmq.sql:317-320`) is marked experimental in docs (`docs/api/sql/functions.md:419`):

```sql
WHERE vt <= clock_timestamp() AND CASE
    WHEN %L != '{}'::jsonb THEN (message @> %2$L)::integer
    ELSE 1
END = 1
```

i.e. JSONB containment (`@>`).

The grouped functions all key off the JSONB header `x-pgmq-group` (`pgmq.sql:137`, `:256`, `:358`, etc.):
```sql
COALESCE(headers->>'x-pgmq-group', '_default_fifo_group')
```
Messages without that header all fall into one logical FIFO group. See §3 and §8 for concurrency mechanics.

### 1.4 Pop

| Function | Loc | Returns |
|---|---|---|
| `pgmq.pop(queue_name TEXT, qty INTEGER DEFAULT 1)` | `pgmq.sql:922` | `SETOF pgmq.message_record` |

Pop is read-and-delete in one statement; **at-most-once** semantics if the consumer crashes (`docs/api/sql/functions.md:697`). Implementation:

```sql
WITH cte AS (
    SELECT msg_id FROM pgmq.%I
    WHERE vt <= clock_timestamp()
    ORDER BY msg_id ASC
    LIMIT $1
    FOR UPDATE SKIP LOCKED
)
DELETE FROM pgmq.%I
WHERE msg_id IN (select msg_id from cte)
RETURNING msg_id, read_ct, enqueued_at, last_read_at, vt, message, headers;
```
(`pgmq.sql:931-942`).

### 1.5 Delete

Permanent removal — no archive copy.

| Function | Loc | Returns |
|---|---|---|
| `pgmq.delete(queue_name TEXT, msg_id BIGINT)` | `pgmq.sql:603` | boolean (true if a row was deleted) |
| `pgmq.delete(queue_name TEXT, msg_ids BIGINT[])` | `pgmq.sql:628` | `SETOF BIGINT` (ids actually deleted) |

The batch overload silently skips ids that don't exist (`pgmq.sql:639-642`). Both are simple `DELETE ... RETURNING msg_id`.

### 1.6 Archive

Move from `q_*` to `a_*` in one CTE.

| Function | Loc | Returns |
|---|---|---|
| `pgmq.archive(queue_name TEXT, msg_id BIGINT)` | `pgmq.sql:540` | boolean |
| `pgmq.archive(queue_name TEXT, msg_ids BIGINT[])` | `pgmq.sql:573` | `SETOF BIGINT` |

Implementation pattern (`pgmq.sql:553-561`):
```sql
WITH archived AS (
    DELETE FROM pgmq.%I WHERE msg_id = $1
    RETURNING msg_id, vt, read_ct, enqueued_at, last_read_at, message, headers
)
INSERT INTO pgmq.%I (msg_id, vt, read_ct, enqueued_at, last_read_at, message, headers)
SELECT msg_id, vt, read_ct, enqueued_at, last_read_at, message, headers
FROM archived
RETURNING msg_id;
```

`archived_at` defaults to `now()` so it's set automatically by the archive table's column default (`pgmq.sql:1170`).

### 1.7 Purge

| Function | Loc | Returns |
|---|---|---|
| `pgmq.purge_queue(queue_name TEXT)` | `pgmq.sql:894` | bigint (number of rows purged) |

Counts rows then `TRUNCATE TABLE` (`pgmq.sql:901-904`). **TRUNCATE in Postgres is transactional** but holds an `ACCESS EXCLUSIVE` lock — concurrent reads/sends will block.

### 1.8 Visibility-timeout adjustment (`set_vt`)

| Function | Loc | Returns |
|---|---|---|
| `pgmq.set_vt(queue_name, msg_id BIGINT, vt TIMESTAMPTZ)` | `pgmq.sql:951` | `SETOF pgmq.message_record` |
| `pgmq.set_vt(queue_name, msg_id BIGINT, vt INTEGER)` | `pgmq.sql:972` | same; integer means seconds-from-now |
| `pgmq.set_vt(queue_name, msg_ids BIGINT[], vt TIMESTAMPTZ)` | `pgmq.sql:978` | `SETOF pgmq.message_record` (batch) |
| `pgmq.set_vt(queue_name, msg_ids BIGINT[], vt INTEGER)` | `pgmq.sql:1002` | batch, seconds-from-now |

Plain `UPDATE ... SET vt = $1 WHERE msg_id = $2 RETURNING ...` (`pgmq.sql:960-963`). No locking guard — caller can shrink VT below `clock_timestamp()` to make a message immediately re-readable (commonly used for retry).

### 1.9 Metrics

Return type `pgmq.metrics_result` (`pgmq.sql:819`):
```
queue_name TEXT, queue_length BIGINT, newest_msg_age_sec INT, oldest_msg_age_sec INT,
total_messages BIGINT, scrape_time TIMESTAMPTZ, queue_visible_length BIGINT
```

| Function | Loc | Returns |
|---|---|---|
| `pgmq.metrics(queue_name TEXT)` | `pgmq.sql:830` | `pgmq.metrics_result` (single row) |
| `pgmq.metrics_all()` | `pgmq.sql:872` | `SETOF pgmq.metrics_result` |

See §7 for cost analysis.

### 1.10 NOTIFY trigger management

| Function | Loc | Returns |
|---|---|---|
| `pgmq.enable_notify_insert(queue_name TEXT, throttle_interval_ms INTEGER DEFAULT 250)` | `pgmq.sql:1644` | void |
| `pgmq.disable_notify_insert(queue_name TEXT)` | `pgmq.sql:1681` | void |
| `pgmq.update_notify_insert(queue_name TEXT, throttle_interval_ms INTEGER)` | `pgmq.sql:1714` | void |
| `pgmq.list_notify_insert_throttles()` | `pgmq.sql:1698` | `TABLE(queue_name TEXT, throttle_interval_ms INTEGER, last_notified_at TIMESTAMPTZ)` |
| `pgmq.notify_queue_listeners()` (TRIGGER) | `pgmq.sql:1616` | trigger function |

Channel name is `pgmq.q_<queue_name>.INSERT` (`pgmq.sql:1637`). See §9.

### 1.11 Topic / routing

| Function | Loc | Returns |
|---|---|---|
| `pgmq.bind_topic(pattern TEXT, queue_name TEXT)` | `pgmq.sql:1852` | void |
| `pgmq.unbind_topic(pattern TEXT, queue_name TEXT)` | `pgmq.sql:1873` | boolean |
| `pgmq.list_topic_bindings()` | `pgmq.sql:1985` | `TABLE(pattern, queue_name, bound_at, compiled_regex)` |
| `pgmq.list_topic_bindings(queue_name TEXT)` | `pgmq.sql:2002` | same, filtered |
| `pgmq.test_routing(routing_key TEXT)` | `pgmq.sql:1904` | `TABLE(pattern, queue_name, compiled_regex)` |
| `pgmq.validate_routing_key(routing_key TEXT)` | `pgmq.sql:1739` | boolean (raises on invalid) |
| `pgmq.validate_topic_pattern(pattern TEXT)` | `pgmq.sql:1788` | boolean (raises on invalid) |

### 1.12 FIFO indexing helpers

| Function | Loc | Returns |
|---|---|---|
| `pgmq.create_fifo_index(queue_name TEXT)` | `pgmq.sql:1498` | void; creates GIN index on `headers` |
| `pgmq.create_fifo_indexes_all()` | `pgmq.sql:1507` | void |
| `pgmq._create_fifo_index_if_not_exists(queue_name TEXT)` | `pgmq.sql:1480` | internal |

### 1.13 Internal / private helpers

- `pgmq._validate_batch_params(msgs, headers)` (`pgmq.sql:718`) — guard
- `pgmq._send_batch(queue_name, msgs, headers, delay)` (`pgmq.sql:738`) — actual batch insert without validation; called by `send_batch_topic` to skip redundant validation
- `pgmq._get_pg_partman_schema()` (`pgmq.sql:1011`)
- `pgmq._get_pg_partman_major_version()` (`pgmq.sql:1308`)
- `pgmq._extension_exists(extension_name)` (`pgmq.sql:1288`)
- `pgmq._ensure_pg_partman_installed()` (`pgmq.sql:1299`)
- `pgmq._get_partition_col(partition_interval)` (`pgmq.sql:1273`) — returns `'msg_id'` when interval parses as int, else `'enqueued_at'`
- `pgmq._belongs_to_pgmq(table_name)` (`pgmq.sql:1118`)

### 1.14 Transactional vs. not — summary

| Op | Transactional | Notes |
|---|---|---|
| `send`, `send_batch`, `send_topic`, `send_batch_topic` | yes | Standard INSERT; rollback discards. |
| `read`, `read_with_poll`, `read_grouped*` | yes | UPDATE inside CTE; rollback releases lock and reverts `vt`/`read_ct`. |
| `pop` | yes | DELETE with RETURNING. |
| `delete`, `archive`, `purge_queue`, `set_vt` | yes | Plain DML / TRUNCATE. |
| `create*`, `drop_queue` | yes (plus advisory lock) | DDL is transactional in Postgres. |
| NOTIFY trigger fires | yes — `PG_NOTIFY` is delivered **on commit** | Listeners get nothing if the inserting txn rolls back (`pgmq.sql:1637`). |
| `read_with_poll` `pg_sleep` | yes, but note the txn is held open while sleeping | See §4 / §9. |

---

## 2. Table layout per queue

### 2.1 Active queue table `pgmq.q_<name>` (non-partitioned)

From `pgmq.create_non_partitioned` (`pgmq.sql:1148-1161`):

```sql
CREATE TABLE IF NOT EXISTS pgmq.%I (
    msg_id       BIGINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    read_ct      INT DEFAULT 0 NOT NULL,
    enqueued_at  TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    last_read_at TIMESTAMP WITH TIME ZONE,
    vt           TIMESTAMP WITH TIME ZONE NOT NULL,
    message      JSONB,
    headers      JSONB
);
```

Column-by-column:

| Column | Type | Notes |
|---|---|---|
| `msg_id` | `BIGINT` | PK, `GENERATED ALWAYS AS IDENTITY` (uses an implicit sequence `q_<name>_msg_id_seq`). Used as ordering key throughout. |
| `read_ct` | `INT NOT NULL DEFAULT 0` | Incremented on every `read*`. **No retry-cap is enforced** by pgmq — clients must check it themselves. |
| `enqueued_at` | `TIMESTAMPTZ NOT NULL DEFAULT now()` | Set on insert. |
| `last_read_at` | `TIMESTAMPTZ NULL` | Set by `read*`. NULL means never read. |
| `vt` | `TIMESTAMPTZ NOT NULL` | The **visibility-timeout deadline**. Message is visible iff `vt <= clock_timestamp()`. On insert this is set from the `delay` arg of `send`. |
| `message` | `JSONB NULL` | Payload. |
| `headers` | `JSONB NULL` | Optional metadata; used for FIFO grouping (`x-pgmq-group`). |

Indexes:

```sql
CREATE INDEX %I ON pgmq.%I (vt ASC);  -- name: q_<name>_vt_idx
```
(`pgmq.sql:1181`). The PK gives the `msg_id` index.

Optional GIN index on `headers` via `pgmq.create_fifo_index()` (`pgmq.sql:1487-1492`):
```sql
CREATE INDEX IF NOT EXISTS %I ON pgmq.%I USING GIN (headers);
```

### 2.2 Archive table `pgmq.a_<name>`

From `pgmq.create_non_partitioned` (`pgmq.sql:1163-1177`):

```sql
CREATE TABLE IF NOT EXISTS pgmq.%I (
    msg_id       BIGINT PRIMARY KEY,                          -- NOT identity; original msg_id preserved
    read_ct      INT DEFAULT 0 NOT NULL,
    enqueued_at  TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    last_read_at TIMESTAMP WITH TIME ZONE,
    archived_at  TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,   -- new column
    vt           TIMESTAMP WITH TIME ZONE NOT NULL,
    message      JSONB,
    headers      JSONB
);
```

Differences from `q_*`:
- `msg_id` is **plain BIGINT PK** (no IDENTITY) — value carried over from active table.
- Adds `archived_at TIMESTAMPTZ NOT NULL DEFAULT now()`.

Index:
```sql
CREATE INDEX %I ON pgmq.%I (archived_at);  -- name: archived_at_idx_<queue_name>
```
(`pgmq.sql:1188`).

### 2.3 Unlogged variant

`pgmq.create_unlogged` (`pgmq.sql:1206`) is identical to `create_non_partitioned` except the active queue table is `CREATE UNLOGGED TABLE` (`pgmq.sql:1218`). The archive table is **still logged** (`pgmq.sql:1233`) — durability is preserved for archived messages but not for in-flight ones. UNLOGGED tables are wiped on crash recovery.

### 2.4 Metadata tables (singletons)

`pgmq.meta` (`pgmq.sql:17-22`):
```sql
CREATE TABLE IF NOT EXISTS pgmq.meta (
    queue_name      VARCHAR UNIQUE NOT NULL,
    is_partitioned  BOOLEAN NOT NULL,
    is_unlogged     BOOLEAN NOT NULL,
    created_at      TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL
);
```
No PK declared, but `queue_name` is UNIQUE. One row per queue.

`pgmq.notify_insert_throttle` (`pgmq.sql:35-42`):
```sql
CREATE UNLOGGED TABLE IF NOT EXISTS pgmq.notify_insert_throttle (
    queue_name           VARCHAR UNIQUE NOT NULL
        REFERENCES pgmq.meta(queue_name) ON DELETE CASCADE,
    throttle_interval_ms INTEGER NOT NULL DEFAULT 0,
    last_notified_at     TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT to_timestamp(0)
);
```
Note **UNLOGGED** — `last_notified_at` resets on crash, which is harmless. Partial index:
```sql
CREATE INDEX idx_notify_throttle_active
    ON pgmq.notify_insert_throttle (queue_name, last_notified_at)
    WHERE throttle_interval_ms > 0;
```
(`pgmq.sql:44-46`).

`pgmq.topic_bindings` (`pgmq.sql:48-69`):
```sql
CREATE TABLE IF NOT EXISTS pgmq.topic_bindings (
    pattern        text NOT NULL,
    queue_name     text NOT NULL REFERENCES pgmq.meta(queue_name) ON DELETE CASCADE,
    bound_at       TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    compiled_regex text GENERATED ALWAYS AS (
        '^' || replace(replace(
            regexp_replace(pattern, '([.+?{}()|\[\]\\^$])', '\\\1', 'g'),
            '*', '[^.]+'),
            '#', '.*') || '$'
        ) STORED,
    CONSTRAINT topic_bindings_unique_pattern_queue UNIQUE (pattern, queue_name)
);
CREATE INDEX idx_topic_bindings_covering
    ON pgmq.topic_bindings (pattern) INCLUDE (queue_name, compiled_regex);
```

`compiled_regex` is a **STORED generated column** that pre-compiles the AMQP-style pattern at write time. Covering index is INCLUDE-style.

### 2.5 Types

`pgmq.message_record` (`pgmq.sql:89-97`) — return shape for read/pop/set_vt.
`pgmq.queue_record` (`pgmq.sql:99-104`) — mirrors `pgmq.meta`.
`pgmq.metrics_result` (`pgmq.sql:819-827`).

### 2.6 Extension config dumps

So that `pg_dump` of an installed extension actually carries data:
```sql
PERFORM pg_extension_config_dump('pgmq.meta', '');
PERFORM pg_extension_config_dump('pgmq.notify_insert_throttle', '');
PERFORM pg_extension_config_dump('pgmq.topic_bindings', '');
```
(`pgmq.sql:80-82`). Per-queue `q_*`/`a_*` tables are NOT extension members (since recent versions), so they're dumped by ordinary `pg_dump`.

---

## 3. Visibility-timeout (VT) mechanics

### 3.1 The model

A message is **visible** iff `vt <= clock_timestamp()`. There is no separate "in-flight" column. `read` works by:
1. Locking visible rows (oldest by `msg_id` first) with `FOR UPDATE SKIP LOCKED`.
2. Updating those rows to push `vt` into the future (`vt = clock_timestamp() + <vt_seconds>`).
3. Returning the updated rows.

Once the row is updated, *for the duration of the holding transaction* the row is locked AND has a future `vt`; once the txn commits, the row stays "claimed" because of the future `vt`. Concurrent `read`s skip it on both grounds.

If the consumer crashes (or rolls back), the UPDATE is reverted — the row's `vt` snaps back to the original value, the lock is released, and another reader can pick it up immediately. This is the at-least-once recovery path. See `transaction_tests.spec` and `transaction_tests.out:51-61`.

### 3.2 The exact `read` SQL

`pgmq.sql:311-336`:

```sql
WITH cte AS (
    SELECT msg_id
    FROM pgmq.%I
    WHERE vt <= clock_timestamp() AND CASE
        WHEN %L != '{}'::jsonb THEN (message @> %2$L)::integer
        ELSE 1
    END = 1
    ORDER BY msg_id ASC
    LIMIT $1
    FOR UPDATE SKIP LOCKED
)
UPDATE pgmq.%I m
SET
    last_read_at = clock_timestamp(),
    vt = clock_timestamp() + %L,
    read_ct = read_ct + 1
FROM cte
WHERE m.msg_id = cte.msg_id
RETURNING m.msg_id, m.read_ct, m.enqueued_at, m.last_read_at, m.vt, m.message, m.headers;
```

Key choices:
- **`ORDER BY msg_id ASC`** — FIFO within the visible set. Combined with the IDENTITY column on `msg_id`, this gives queue-order delivery.
- **`FOR UPDATE SKIP LOCKED`** — avoids waiting on rows another reader has under lock; that's the entire concurrency story for the basic `read`.
- **The CTE `LIMIT` is applied before locking**; SKIP LOCKED inside the CTE means the optimizer needs to scan rows it can lock — if many rows are locked, Postgres scans further to fill the LIMIT.
- **The outer UPDATE `WHERE m.msg_id = cte.msg_id`** has no additional `vt <= now()` guard. Two CTE readers cannot both lock the same row (SKIP LOCKED), so this is safe in the basic case. The `read_grouped_rr` variant adds a guard `AND m.vt <= clock_timestamp()` (`pgmq.sql:192`) "to avoid duplicate reads under races" — this hints at edge cases the maintainers worry about (probably interaction with concurrent `set_vt`).
- **`clock_timestamp()`** (not `now()`/`statement_timestamp()`) — uses wall-clock, not transaction timestamp. So multiple reads in the same transaction stamp different VT deadlines.

### 3.3 The vt index

`q_<name>_vt_idx` is a **plain B-tree on `(vt ASC)`** (`pgmq.sql:1181`). When most rows are visible, this index is mostly useless because the planner will scan; when many rows are invisible (under VT), it's effective. There is **no partial index** like `WHERE vt <= now()` because that predicate is non-immutable.

### 3.4 Grouped read locking

`read_grouped` (`pgmq.sql:343-438`) is a multi-CTE that:

1. `fifo_groups` — find min `msg_id` per group among visible messages.
2. `locked_groups` — `FOR UPDATE SKIP LOCKED` those head messages (`pgmq.sql:374-375`).
3. `group_priorities` — rank groups by their head `msg_id`.
4. `filtered_groups` — `NOT EXISTS` check that no earlier message in this group is currently invisible (i.e. no in-flight predecessor with `vt > clock_timestamp()`). This is the FIFO-within-group invariant (`pgmq.sql:386-394`).
5. `available_messages` — pull more messages from the same group via `CROSS JOIN LATERAL`.
6. `selected_messages` — `FOR UPDATE SKIP LOCKED` again on the chosen msg_ids (`pgmq.sql:418-423`).
7. UPDATE.

`read_grouped_rr` (`pgmq.sql:122-203`) takes a different approach:
- Computes `head_msg_id` per group **regardless of visibility**.
- Acquires per-group `pg_try_advisory_xact_lock(hashtextextended(fifo_key, 0))` to serialize one consumer per group at a time (`pgmq.sql:152`). This is in addition to row-level `FOR UPDATE SKIP LOCKED`.
- Round-robins by `ROW_NUMBER() OVER (PARTITION BY fifo_key ORDER BY msg_id)`.
- The outer UPDATE adds the safety guard `AND m.vt <= clock_timestamp()` (`pgmq.sql:192`).

`read_grouped_head` (`pgmq.sql:241-284`) is simpler — one head per group, `FOR UPDATE SKIP LOCKED`.

### 3.5 What the VT does **not** protect against

- The model assumes `clock_timestamp()` is monotonic and consistent across sessions on the same DB host. Postgres uses the OS clock; clock skew on a replica/HA scenario may misbehave.
- A consumer that takes longer than `vt` to process will see its message re-delivered to another consumer. This is intentional. There is no "extend VT" beyond `set_vt`.

---

## 4. `read_with_poll` semantics

`pgmq.sql:476-535`:

```sql
CREATE FUNCTION pgmq.read_with_poll(
    queue_name TEXT,
    vt INTEGER,
    qty INTEGER,
    max_poll_seconds INTEGER DEFAULT 5,
    poll_interval_ms INTEGER DEFAULT 100,
    conditional JSONB DEFAULT '{}'
) RETURNS SETOF pgmq.message_record AS $$
DECLARE
    r pgmq.message_record;
    stop_at TIMESTAMP;
    sql TEXT;
    qtable TEXT := pgmq.format_table_name(queue_name, 'q');
BEGIN
    stop_at := clock_timestamp() + make_interval(secs => max_poll_seconds);
    LOOP
      IF (SELECT clock_timestamp() >= stop_at) THEN
        RETURN;
      END IF;

      sql := FORMAT( ... read CTE same as pgmq.read ... );

      FOR r IN EXECUTE sql USING qty LOOP
        RETURN NEXT r;
      END LOOP;
      IF FOUND THEN
        RETURN;
      ELSE
        PERFORM pg_sleep(poll_interval_ms::numeric / 1000);
      END IF;
    END LOOP;
END;
$$ LANGUAGE plpgsql;
```

Behavior:
- **Synchronous busy-poll with `pg_sleep`** between attempts. There is no integration with `LISTEN/NOTIFY` in `read_with_poll` itself — that's handled separately via `enable_notify_insert` + the client doing its own LISTEN.
- Defaults: `max_poll_seconds = 5`, `poll_interval_ms = 100` → up to ~50 attempts per call.
- Returns **immediately** the first time messages are found (`IF FOUND THEN RETURN`).
- The function **holds an open transaction** the entire time it's polling. This means it pins one Postgres backend / connection per polling consumer, and the snapshot can grow stale.
- `max_poll_seconds` has no documented hard cap — it's just an integer interval. Users could pass arbitrarily large values; the call ties up a connection that long.
- The grouped poll variants (`read_grouped_with_poll` `:442`, `read_grouped_rr_with_poll` `:207`) use the same pattern.

There is also a "real" event-driven path: `enable_notify_insert` + a client `LISTEN pgmq.q_<name>.INSERT`. The trigger throttles via `notify_insert_throttle` (`pgmq.sql:1622-1638`) — it only sends a NOTIFY if at least `throttle_interval_ms` have elapsed since the last one. Default throttle is 250ms (`pgmq.sql:1644`). NOTIFY payloads are NULL.

---

## 5. Archive vs delete

| Operation | Side effect | Schema target | Retention |
|---|---|---|---|
| `pgmq.delete(...)` | Row removed; gone forever | `q_*` only | None — no copy kept. |
| `pgmq.archive(...)` | Row moved to archive | `q_*` → `a_*` | Indefinite by default; `archived_at` column written; `archived_at_idx_<queue>` index for time-range scans. |
| `pgmq.purge_queue(...)` | All rows removed via `TRUNCATE` | `q_*` only | Nothing archived. |

Archive table schema differences (vs active queue):
- `msg_id` is plain `BIGINT PRIMARY KEY` (no IDENTITY) — the original id is preserved.
- New column `archived_at TIMESTAMPTZ NOT NULL DEFAULT now()`.
- Same `read_ct`, `enqueued_at`, `last_read_at`, `vt`, `message`, `headers` columns — so you can see the message's full lifecycle history when you go look at it.

Retention story:
- For non-partitioned archive: **none built-in**. The table grows forever. Operator's responsibility to `DELETE FROM pgmq.a_<queue> WHERE archived_at < now() - interval 'N days'`.
- For partitioned archive: `pg_partman` drops old partitions per `retention_interval` (`pgmq.sql:1446-1459`). `retention_keep_table = false`, `retention_keep_index = true`, `automatic_maintenance = 'on'`.
- `convert_archive_partitioned` exists (`pgmq.sql:1518`) to retrofit time-based partitioning onto an existing archive table.

---

## 6. Partitioned queues

Defined in `pgmq.create_partitioned(queue_name, partition_interval TEXT DEFAULT '10000', retention_interval TEXT DEFAULT '100000')` (`pgmq.sql:1317`).

### 6.1 Partition key resolution

`pgmq._get_partition_col(partition_interval)` (`pgmq.sql:1273-1286`):
- If `partition_interval` parses as INTEGER → partition by `msg_id` (numeric range).
- Otherwise (e.g. `'1 day'`, `'1 month'`) → partition by `enqueued_at` (time range).

For the **archive** table (`pgmq.sql:1404-1408`):
- If queue partitions by `enqueued_at` → archive partitions by `archived_at`.
- If queue partitions by `msg_id` → archive partitions by `msg_id`.

### 6.2 How partitions are created

```sql
CREATE TABLE IF NOT EXISTS pgmq.%I (
    msg_id       BIGINT GENERATED ALWAYS AS IDENTITY,   -- NB: no PRIMARY KEY (incompatible with RANGE partitioning by enqueued_at)
    read_ct      INT DEFAULT 0 NOT NULL,
    enqueued_at  TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    last_read_at TIMESTAMP WITH TIME ZONE,
    vt           TIMESTAMP WITH TIME ZONE NOT NULL,
    message      JSONB,
    headers      JSONB
) PARTITION BY RANGE (%I)
```
(`pgmq.sql:1339-1349`). Note **no PRIMARY KEY** on partitioned queue tables — Postgres requires the partition column to be in the PK, which would force `msg_id`-based partitioning. By dropping the PK they keep the time-based option open.

Then `pg_partman.create_parent(...)` (`pgmq.sql:1356-1364`) sets up automatic child-partition management. PG_PARTMAN_VER = 5.1.0 (`Makefile:7`). For partman v5 they use `p_type := 'range'`; older versions get `'native'`.

A `q_<name>_part_idx` is created on the partition column (`pgmq.sql:1374`).

### 6.3 Retention policy interaction

Update to `pg_partman.part_config` (`pgmq.sql:1379-1392`):
```sql
UPDATE %I.part_config
SET retention = %L,
    retention_keep_table = false,
    retention_keep_index = true,
    automatic_maintenance = 'on'
WHERE parent_table = %L;
```

`retention_interval` semantics (`docs/partitioned-queues.md:20`):
- Time-based: drop partitions whose data is older than the duration.
- Numeric: drop partitions where all `msg_id < max(msg_id) - retention_interval`.
- **Important caveat**: retention drops *whole partitions* — including messages that were never read. This is data loss if you don't archive first. The doc explicitly warns: "`retention_interval` does not apply to messages that have been deleted via `pgmq.delete()` or archived with `pgmq.archive()`. `pgmq.delete()` removes messages forever and `pgmq.archive()` moves messages to the corresponding archive table forever."

### 6.4 Maintenance prerequisite

For automatic partition maintenance (creating new ones ahead of time, dropping old), the operator must add to `postgresql.conf` (`docs/partitioned-queues.md:32-36`):
```text
shared_preload_libraries = 'pg_partman_bgw'
pg_partman_bgw.interval = 60
pg_partman_bgw.role = 'postgres'
pg_partman_bgw.dbname = 'postgres'
```

`pg_partman` keeps 4 partitions ahead of the current one by default.

### 6.5 Drop interaction

`pgmq.drop_queue` checks `pgmq.meta.is_partitioned` and if true, deletes the rows from `pg_partman.part_config` for both the queue and the archive parent tables (`pgmq.sql:1092-1099`).

---

## 7. Metrics

`pgmq.metrics_result` (`pgmq.sql:819-827`):
```
queue_name TEXT, queue_length BIGINT, newest_msg_age_sec INT,
oldest_msg_age_sec INT, total_messages BIGINT, scrape_time TIMESTAMPTZ,
queue_visible_length BIGINT
```

`pgmq.metrics(queue_name)` (`pgmq.sql:830-869`) executes:
```sql
WITH q_summary AS (
    SELECT
        count(*) as queue_length,
        count(CASE WHEN vt <= NOW() THEN 1 END) as queue_visible_length,
        EXTRACT(epoch FROM (NOW() - max(enqueued_at)))::int as newest_msg_age_sec,
        EXTRACT(epoch FROM (NOW() - min(enqueued_at)))::int as oldest_msg_age_sec,
        NOW() as scrape_time
    FROM pgmq.%I                      -- the q_* table
),
all_metrics AS (
    SELECT CASE WHEN is_called THEN last_value ELSE 0 END as total_messages
    FROM pgmq.%I                      -- the msg_id sequence
)
SELECT %L as queue_name, ...
```

Cost analysis:
- `count(*)` on the queue table — **full table scan / index scan**, O(N) where N is current queue depth. On large queues this is expensive.
- `count(CASE WHEN vt <= NOW() THEN 1 END)` — same scan, no extra cost.
- `min/max(enqueued_at)` — combined with the count, the planner can do this in one scan.
- `total_messages` is read from the **sequence's `last_value`** (`q_<name>_msg_id_seq`). This is O(1). It counts *all msg_ids ever assigned*, including deleted/archived ones — the "lifetime through-put" of the queue.
- `scrape_time` uses `NOW()` (transaction timestamp), unlike read which uses `clock_timestamp()`. So if you call `metrics` inside a long-running txn, `scrape_time` may be older than wall-clock.

`pgmq.metrics_all()` (`pgmq.sql:872-883`):
```sql
FOR row_name IN SELECT queue_name FROM pgmq.meta LOOP
    result_row := pgmq.metrics(row_name.queue_name);
    RETURN NEXT result_row;
END LOOP;
```

i.e. it loops and calls `metrics()` per queue. **Cost is O(sum of queue lengths)**. There's no batching or shortcut — calling `metrics_all` on a system with many large queues will be a noticeable scrape.

There is no separate cheap "queue length" function. If you want O(1) telemetry you have to track it yourself (or use `total_messages` which is the cumulative sequence value).

---

## 8. Concurrency guarantees and known gotchas

### 8.1 Stated guarantees (README + docs)

- **README** (`README.md:17`): "Guaranteed 'exactly once' delivery of messages to a consumer within a visibility timeout."
  - This is a slightly misleading framing. Within the VT window, no other consumer can read the same message — that's the guarantee. Outside the window (e.g. consumer takes too long), the message is re-delivered. So the actual semantics are **at-least-once**, with single-delivery guaranteed within the VT.
- **docs/api/sql/functions.md:697** (on `pop`): "utilization of pop() results in at-most-once delivery semantics if the consuming application does not guarantee processing of the message."
- **docs/fifo-queues.md:322-325** (FIFO comparison table) explicitly admits:
  - Exactly-once delivery: ❌
  - Message deduplication: ❌

There is **no built-in deduplication** (no idempotency token, no producer message-id like SQS). Producers must supply their own deduplication if they need it (e.g. by wrapping `send` with an `INSERT ... ON CONFLICT` against an application-side idempotency table).

### 8.2 Ordering

- Basic `read` returns by `ORDER BY msg_id ASC` from visible messages, but **does NOT guarantee strict FIFO across concurrent consumers**: under concurrency, consumer A may grab msg 2 while consumer B grabs msg 1 (both visible, both lockable). Messages are dispatched roughly in order but not strictly so.
- For per-key ordering, use the `read_grouped*` family with `x-pgmq-group` header. Within a group, `read_grouped` enforces "no msg_id N+1 may be returned while msg_id N is still in-flight (vt > now)" via the `NOT EXISTS` check at `pgmq.sql:386-394`.

### 8.3 Locking primitives in use

| Primitive | Where | Purpose |
|---|---|---|
| `FOR UPDATE SKIP LOCKED` | every `read*`, `pop` | non-blocking claim of visible rows |
| `pg_advisory_xact_lock(hashtext('pgmq.queue_'||name))` | `create*`, `drop_queue` | serialize queue lifecycle ops on the same name |
| `pg_try_advisory_xact_lock(hashtextextended(fifo_key,0))` | `read_grouped_rr` (`pgmq.sql:152`) | one consumer at a time per FIFO group within a transaction |
| `TRUNCATE TABLE` ACCESS EXCLUSIVE | `purge_queue` | blocks all other queue ops |
| `CREATE CONSTRAINT TRIGGER ... DEFERRABLE` | `enable_notify_insert` | fire NOTIFY at end of statement (deferred) |

### 8.4 Known issues & design notes from the issue tracker

- **Issue #394 — "race condition on pgmq.create()"** (closed). Two concurrent `pgmq.create('same_queue')` calls could race on `CREATE TABLE IF NOT EXISTS`. Fixed by adding the per-queue advisory lock at the top of every create/drop function (`pgmq.sql:1146`, `:1214`, `:1333`, `:1047`).
- **Issue #496 — "Performance issue with `read_grouped`"** (closed). Original `read_grouped` did a `NOT EXISTS` per row over the whole table; with 6000 messages in 30 groups, single calls took 3+ seconds and concurrent calls returned nothing because of `SKIP LOCKED`. Fixed in PR #497 by restructuring the CTE chain to apply `NOT EXISTS` per group then `CROSS JOIN LATERAL` to fetch up to `qty` per group (visible in current code at `pgmq.sql:385-407`). **Implication for SQL Server port**: this query is genuinely complex; benchmark on real workloads before considering it production-ready.
- **Issue #354 — "How to avoid Queue Message collision with Visibility Timeout vs Locking vs Dependencies"** (open). User asks how to prevent two cron-spawned workers from picking the same task. Answer per the maintainers' guidance: VT + SKIP LOCKED is the mechanism; that's the entire model.
- **Issue #431 — "Use LISTEN and NOTIFY to improve polling"** (closed). Implemented as `enable_notify_insert` (`pgmq.sql:1644`).
- **Issue #451 — "Batch visibility timeout updates"** (closed). Implemented as `set_vt(queue_name, msg_ids[], vt)` (`pgmq.sql:978`, `:1002`).
- **Issue #466 — "feature request add column read_at"** (closed). Implemented as `last_read_at` column (visible in `q_*` schema `pgmq.sql:1154`). Pre-1.8 versions did not have this column — see migration `pgmq--1.7.1--1.8.0.sql`.
- **Issue #467 — "feature request add column worker_id/worker_name"** (closed/declined). Maintainers declined to add a worker identity column; recommendation is to use `headers` (but headers can only be set on send, not on read — see open issue #468).
- **Issue #498 — "SQL Migration files does not update create_xxx functions to use the new last_read_at"** (closed). Migration bug — pre-1.8 queues kept getting created without `last_read_at` until fixed.

### 8.5 Things the model does NOT provide

- No FIFO across the whole queue under concurrency (only within `x-pgmq-group`).
- No producer-side dedup.
- No DLQ as a first-class concept — you build one yourself by checking `read_ct` and calling `archive` (or moving to a designated DLQ queue) when over a threshold. See README/docs/fifo-queues.md:228-235.
- No max-receive-count enforcement. You DIY in the consumer.
- No message TTL / max age — messages stay visible forever unless you delete/archive/purge them.
- No priority within a queue beyond `msg_id` order.
- No native message size limit beyond Postgres TOAST limits (1 GB JSONB max, but any JSONB > ~2KB will TOAST).

### 8.6 Implications of running inside the caller's transaction

Because send/read/etc. are not autonomous:
- A transactional consumer that does `BEGIN; read; do_work; delete; COMMIT;` gets exactly-once-within-VT for free. If `do_work` crashes, ROLLBACK reverts the read (vt + read_ct snap back) and the message is immediately re-readable.
- Producers that do `BEGIN; ...business writes...; send; COMMIT;` get transactional outbox semantics. If the business write rolls back, the message never appears in the queue.
- BUT: a long-running consumer transaction holds its row-locks (and an open snapshot) for the entire duration, blocking VACUUM and bloating tables. The pattern of "read in one txn, process, delete in another" means you don't get the rollback safety net but you free DB resources.
- `read_with_poll` with a long `max_poll_seconds` keeps a transaction open the whole sleep. This pins a connection and pessimizes vacuum.

---

## 9. Postgres-specific features that won't port cleanly to SQL Server

| Feature used | Loc | SQL Server equivalent / strategy |
|---|---|---|
| **`FOR UPDATE SKIP LOCKED`** | `pgmq.sql:323`, `:269`, `:374`, `:418`, `:509`, `:938` | SQL Server has `WITH (READPAST, UPDLOCK, ROWLOCK)` table hints. `READPAST` is the equivalent of `SKIP LOCKED`. The CTE-then-UPDATE pattern works the same: `;WITH cte AS (SELECT TOP(@qty) msg_id FROM dbo.q_<name> WITH (READPAST, UPDLOCK, ROWLOCK) WHERE vt <= SYSUTCDATETIME() ORDER BY msg_id) UPDATE q SET vt = ..., read_ct += 1, last_read_at = SYSUTCDATETIME() OUTPUT inserted.* FROM dbo.q_<name> q INNER JOIN cte ON q.msg_id = cte.msg_id;`. Use the `OUTPUT` clause instead of `RETURNING`. |
| **JSONB type** for `message` and `headers` | `pgmq.sql:1156-1157`, etc. | SQL Server has `NVARCHAR(MAX)` storing JSON text, with `JSON_VALUE`, `JSON_QUERY`, `OPENJSON`, `JSON_MODIFY`. There is no binary-storage equivalent of JSONB. **JSONB containment `message @> '{...}'`** in `read`'s conditional has no direct equivalent — you'd emulate with `JSON_VALUE(message, '$.key') = ...` or `OPENJSON(message)` with WHERE conditions, or store specific filter columns as computed projections. SQL Server 2025 (in preview) introduces a native `json` type with better path operators; otherwise NVARCHAR(MAX) + JSON_* functions. |
| **JSONB GIN index** for `headers` (`create_fifo_index`) | `pgmq.sql:1489` | No native equivalent. Two options: (a) extract the FIFO key into a persisted computed column `group_key AS JSON_VALUE(headers, '$."x-pgmq-group"') PERSISTED` and index that; (b) full-text indexing on the JSON text (clumsy). Option (a) is the right port. |
| **`GENERATED ALWAYS AS IDENTITY`** on `msg_id` | `pgmq.sql:1151` | Use SQL Server `IDENTITY(1,1)` or — better for queue throughput — a `SEQUENCE` shared per queue. Note SQL Server IDENTITY has slightly different concurrency: gaps are routine and `SCOPE_IDENTITY()` returns the last-inserted in scope. For `metrics.total_messages`, use `IDENT_CURRENT('q_<name>')` or `sys.dm_db_partition_stats` for an O(1) lifetime count. |
| **Per-queue implicit sequence** read in `metrics` (`is_called`/`last_value`) | `pgmq.sql:850` | `IDENT_CURRENT('dbo.q_<queue>')`, or maintain an explicit SEQUENCE and call `sys.sequences.current_value`. Note `IDENT_CURRENT` is not transactionally consistent across sessions — same caveat as Postgres' sequence. |
| **`UNLOGGED TABLE`** | `pgmq.sql:1218` | SQL Server has no exact equivalent. Closest: a `MEMORY_OPTIMIZED` table with `DURABILITY = SCHEMA_ONLY` (Hekaton). This is a much larger commitment than Postgres' UNLOGGED — different storage engine entirely. Alternative: just accept logging cost; SQL Server's transaction log overhead for queue inserts is usually acceptable. **Recommendation**: punt on this feature in v1 of the port; document as "use a normal table". |
| **Partitioned tables via `pg_partman`** | `pgmq.sql:1317-1469` | SQL Server has native `PARTITION FUNCTION` / `PARTITION SCHEME`. There's no auto-create-future-partitions feature equivalent to `pg_partman_bgw`. You'd write a SQL Agent job that periodically calls `ALTER PARTITION FUNCTION ... SPLIT RANGE`. Retention via `ALTER PARTITION FUNCTION ... MERGE RANGE` after switching out the oldest partition. Significantly more code than the pgmq port; consider whether to ship partitioned queues in v1 at all. |
| **Partial index** on `notify_insert_throttle` `WHERE throttle_interval_ms > 0` | `pgmq.sql:44-46` | SQL Server has **filtered indexes**: `CREATE INDEX ... ON ... WHERE throttle_interval_ms > 0`. Direct port. |
| **`LISTEN` / `NOTIFY` / `pg_notify`** in `notify_queue_listeners` trigger | `pgmq.sql:1637` | No native equivalent. Options: (a) Service Broker (heavyweight, but built-in); (b) SignalR / external pub-sub layer at the application tier; (c) a polling-only design (drop the feature). Most ports of message-queue libraries to SQL Server skip LISTEN/NOTIFY. **Recommendation**: skip in v1. |
| **`pg_advisory_xact_lock` / `pg_try_advisory_xact_lock`** in `acquire_queue_lock` and `read_grouped_rr` | `pgmq.sql:116`, `:152` | SQL Server has **`sp_getapplock`** with `@LockOwner = 'Transaction'` and `@LockMode = 'Exclusive'` (or `'Update'`). For try-lock semantics, set `@LockTimeout = 0` and check the return code. Direct semantic match. |
| **`CREATE CONSTRAINT TRIGGER ... DEFERRABLE`** for the NOTIFY trigger | `pgmq.sql:1671-1675` | SQL Server triggers are not deferrable. AFTER INSERT triggers fire after the statement, not at txn commit. If you skip LISTEN/NOTIFY (recommended), this is moot. |
| **`STORED GENERATED COLUMN`** for `compiled_regex` | `pgmq.sql:56-67` | SQL Server has **persisted computed columns**: `compiled_regex AS (... expression ...) PERSISTED`. Direct match if the expression is deterministic and uses only T-SQL functions. The pgmq pattern uses `regexp_replace` and `replace` to compile AMQP-style globs to a regex — port using `REPLACE` and possibly a CLR function for regex character escaping. |
| **`INCLUDE` covering index** | `pgmq.sql:73` | SQL Server fully supports `CREATE INDEX ... ON t(a) INCLUDE(b, c)`. Direct port. |
| **`make_interval(secs => N)`** | throughout | T-SQL: `DATEADD(SECOND, N, SYSUTCDATETIME())`. |
| **`clock_timestamp()` vs `now()`** | every read uses `clock_timestamp()` | T-SQL: `SYSUTCDATETIME()` is wall-clock per call (analogous to `clock_timestamp()`). `GETUTCDATE()` is also per-call but lower precision. SQL Server has no analog of `now()` (txn-start timestamp); use `SYSUTCDATETIME()` everywhere and document the difference if relevant. |
| **`pg_sleep(seconds_numeric)`** in `read_with_poll` | `pgmq.sql:233`, `:468`, `:531` | T-SQL: `WAITFOR DELAY '00:00:00.100'`. Note WAITFOR DELAY accepts a literal string; for a parameterized delay use `DECLARE @d CHAR(12) = ...; WAITFOR DELAY @d`. Be aware: WAITFOR holds the connection just like `pg_sleep`. |
| **`@>` JSONB containment** | `pgmq.sql:318` | No direct equivalent. Caller must filter via `JSON_VALUE` extraction or by a typed projected column. The conditional read feature is marked experimental upstream; consider deferring. |
| **`ON DELETE CASCADE` foreign keys** | `pgmq.sql:38`, `:53` | Standard SQL — direct port. |
| **`REGEXP_REPLACE` / `~` operator for regex** | `pgmq.sql:62`, `pgmq.sql:1922` | SQL Server's native `LIKE` does not support full regex. Options: SQL CLR (write a small C# function), Azure SQL has `REGEXP_LIKE` etc. For the routing-key matching, you may want to redesign: store the segments of a routing key explicitly and match via `LIKE` patterns or a recursive CTE. |
| **`information_schema.tables`** lookups in drop/create | `pgmq.sql:1058-1059`, `:1656-1657` | T-SQL: `sys.tables`/`sys.schemas` joins. Standard `INFORMATION_SCHEMA.TABLES` also exists in SQL Server. |
| **`pg_extension`, `pg_class`, `pg_depend`, `pg_namespace`** lookups | `pgmq.sql:1126-1133` | These are for the extension-membership check (which would not exist in a SQL-only port — drop). |
| **Set-returning functions `RETURNS SETOF type`** | most read/send/delete functions | T-SQL: inline TVFs (`RETURNS TABLE`) or multi-statement TVFs. Note multi-statement TVFs are slower and the optimizer treats them as opaque; prefer inline TVFs where possible. For the read-and-update pattern, you generally want a stored proc with an `OUTPUT` clause rather than a TVF (TVFs cannot perform DML). This is a meaningful structural difference: you'll likely want stored procs for `read`/`pop`/`send`/`delete`/etc., not functions. |
| **Function overloading on parameter types** | `pgmq.send(...)` etc. — 6 overloads each | T-SQL stored procedures cannot be overloaded by parameter types. You'll need either: (a) a single proc per name with all-optional parameters and convention-driven defaults; (b) distinct proc names per overload (`send_with_delay`, `send_batch_with_headers`); or (c) accept a JSON/structured parameter and dispatch. Option (a) is most ergonomic. |
| **`IDENTITY` columns and arrays of `BIGINT`** | `delete(msg_ids BIGINT[])`, `set_vt(msg_ids BIGINT[])` | T-SQL has no array type. Use **table-valued parameters (TVPs)** — declare a user-defined table type `pgmq.MsgIdList AS TABLE (msg_id BIGINT NOT NULL PRIMARY KEY)` and pass that. Or accept a comma-separated string and `STRING_SPLIT`. TVPs are the right answer; document them in client libraries. |
| **`ARRAY[]` literals in SQL** | `delete(queue_name, ARRAY[2,3])` | Replaced by TVP at the protocol level. |
| **`TRUNCATE` with FK / partition cascade** | `pgmq.sql:904` | T-SQL: `TRUNCATE TABLE` works; respect any FK constraints (the active queue table has none). |
| **`WITH ... RETURNING` (DML in CTE returning rows)** | `archive` (`pgmq.sql:553-561`), `pop` (`pgmq.sql:931-942`), every `read` | T-SQL allows `OUTPUT` from a single DML statement, but you cannot do `WITH a AS (DELETE ... RETURNING ...) INSERT INTO b ... SELECT FROM a`. Workaround for `archive`: capture deleted rows into a `@TableVariable` via `OUTPUT DELETED.* INTO @t`, then INSERT into archive from `@t`. Similar for `pop` (just OUTPUT inserted/deleted). Workaround for `read`: the CTE+UPDATE+RETURNING pattern works because the CTE selects the IDs (no DML in CTE) and the UPDATE returns via OUTPUT. Direct port. |
| **`SETOF type` return** | every read function | Inline TVF or stored proc with `SELECT` at the end. |
| **`DEFERRABLE` constraints / triggers** | NOTIFY trigger | N/A in SQL Server. |
| **`pg_partman`** and `pg_partman_bgw` background worker | partitioned queues | No equivalent ecosystem; build with PARTITION FUNCTION + Agent jobs. |

### 9.1 Particularly tricky bits to flag

1. **The whole `read_grouped` family is a 7-CTE query with multiple `FOR UPDATE SKIP LOCKED`** stages. Porting it verbatim is doable but you should benchmark; the upstream perf bug (#496) was within a year ago.
2. **`pg_try_advisory_xact_lock(hashtextextended(fifo_key, 0))`** in `read_grouped_rr` does double-duty as a per-group serializer. SQL Server's `sp_getapplock` works the same conceptually but has different retry/timeout behavior; pay attention to `@LockTimeout` (use 0 for try-semantics).
3. **`CREATE CONSTRAINT TRIGGER ... DEFERRABLE FOR EACH ROW`** for NOTIFY: the trigger is row-level but deferred to commit. SQL Server triggers are statement-level by default and not deferrable. If you port the LISTEN/NOTIFY feature via Service Broker, the model is fundamentally different (queue-based, not pub-sub).
4. **`UNLOGGED` tables**: the obvious analog is `MEMORY_OPTIMIZED ... DURABILITY = SCHEMA_ONLY` but that's a different beast (in-memory OLTP, optimistic concurrency, no `READPAST` hint). Probably skip.
5. **Identity-column-as-throughput-counter** in `metrics` is fine — SQL Server's `IDENT_CURRENT` is the equivalent — but be aware of `IDENT_CURRENT` reset behavior on TRUNCATE (it resets!). pgmq's `purge_queue` uses TRUNCATE, which would reset the counter; if you want lifetime msg_id continuity across purges, switch to a SEQUENCE.
6. **JSONB containment `@>` for the `read` conditional filter**: the upstream feature is marked experimental. Your port can defer it.
7. **`now()` vs `clock_timestamp()` matters** in pgmq. `metrics` uses `NOW()` (txn-start) for `scrape_time`; `read*` uses `clock_timestamp()` (wall-clock). T-SQL has no `now()` analog — every call to `SYSUTCDATETIME()` is wall-clock. So `scrape_time` will diverge from upstream by a few milliseconds in long transactions. Document or accept.
8. **Per-queue table proliferation**: the design creates two tables, several indexes, and (optionally) a sequence per queue. SQL Server is fine with this but watch out for `tempdb` pressure if many queues + `READPAST` lock waits accumulate. Also, `sp_rename` semantics differ from Postgres `ALTER TABLE RENAME` in subtle ways for FK constraints — the `convert_archive_partitioned` migration helper would need rewriting.
9. **Topic regex matching** uses Postgres `~` regex operator. SQL Server has no native regex. Either use SQL CLR or restructure topic matching to use `LIKE` with `_` and `%` after rewriting `*`/`#` patterns to LIKE patterns. Note that `*` (one segment) and `#` (zero or more segments) DON'T map cleanly to `_` and `%` because the segment boundary is `.` — you'd need to also assert "no `.` in this section", which `LIKE` cannot easily express. CLR regex is the cleanest path.
10. **Schema permissions**: pgmq grants SELECT to `pg_monitor` (`pgmq.sql:28-32`). SQL Server analog is granting `SELECT` and `VIEW DEFINITION` to a role used for monitoring (e.g. a `pgmq_monitor` role).

---

## Appendix A: Migration history snapshot

The repo carries every `pgmq--<from>--<to>.sql` migration from `0.7.3` → `1.11.1` (`pgmq-extension/sql/`). When porting you only need the consolidated `pgmq.sql` as the target; the migration files are useful only for understanding *what the extension supports for in-place upgrades*, which is not your concern. A few notable schema bumps:

- `pgmq--1.4.5--1.5.0.sql` — added `headers JSONB` column.
- `pgmq--1.7.1--1.8.0.sql` — added `last_read_at TIMESTAMPTZ` column. Issue #498 noted that the migration didn't update the `create_*` functions, so freshly-created queues post-1.8 still lacked `last_read_at` until later patched.
- `pgmq--1.8.1--1.9.0.sql` — FIFO grouped reads (`read_grouped`).
- `pgmq--1.9.0--1.10.0.sql` — additional FIFO variants (`read_grouped_rr`).
- `pgmq--1.10.1--1.11.0.sql` — topic bindings (`bind_topic`, `send_topic`).
- `pgmq--1.11.0--1.11.1.sql` — bug fix to `pg_extension_config_dump` for `topic_bindings`.

## Appendix B: Recommended porting scope for SQL Server v1

Based on the surface above, here is a suggested scope tier:

**Tier 1 (must-have)**: queue lifecycle (`create`, `drop_queue`, `list_queues`), `send`/`send_batch`, `read` (no conditional), `read_with_poll`, `pop`, `delete`, `archive`, `purge_queue`, `set_vt`, `metrics`/`metrics_all`. This is the core SQS-like surface and ports cleanly.

**Tier 2 (worth doing)**: FIFO grouped reads (`read_grouped*`). Complex but well-defined. Use `sp_getapplock` for per-group serialization, `READPAST/UPDLOCK/ROWLOCK` for row claim.

**Tier 3 (defer)**: partitioned queues (requires building partition-management infrastructure), unlogged queues (no clean equivalent), conditional-filter `read` (experimental upstream), topic routing (requires regex via CLR), LISTEN/NOTIFY (no equivalent — replace with polling or Service Broker).
