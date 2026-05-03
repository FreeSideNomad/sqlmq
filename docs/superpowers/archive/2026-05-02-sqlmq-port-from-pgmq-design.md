# sqlmq — Design Spec

**Date:** 2026-05-02
**Status:** Draft (pending user review)
**Repository (planned):** github.com/FreeSideNomad/sqlmq
**License (planned):** Apache 2.0

## Retired in v1

The **in-memory (Hekaton) storage variant** described as a co-equal alternative
to the on-disk variant in this spec was implemented (V011 filegroup setup,
V012 applock-serialized read, V013 per-queue natively compiled read) and then
**retired in V014**. A head-to-head bake-off across two environments (Mac
Rosetta + native x86_64 Hyper-V VM) and both implementations showed in-memory
never beat on-disk and degraded sharply at higher consumer counts: memory-
optimized tables under SNAPSHOT have no `READPAST` equivalent, so concurrent
consumers thunderclap on `SELECT TOP(1) ORDER BY msg_id`, collide on
`UPDATE`, and trigger 41302 (write-write conflict) retry storms. The on-disk
path's `READPAST + UPDLOCK + ROWLOCK + READCOMMITTEDLOCK` pattern is strictly
better for this workload. See `bench-results/scan-vm-native/` for the data
and the V014 migration header for the full rationale. References to the
in-memory variant elsewhere in this spec remain for historical context.

## Problem

[pgmq](https://github.com/tembo-io/pgmq) is a small, well-loved PostgreSQL message queue implemented as stored functions. There is no equivalent for SQL Server. Teams running on SQL Server who want pgmq's "drop-in queue, no extra services, no extra processes" model have to either roll their own table-as-queue (and re-discover every concurrency footgun) or adopt Service Broker (heavy, ignored by Microsoft since SQL 2008-R2, not supported on Azure SQL DB).

**sqlmq** is a port of pgmq's surface and ergonomics to SQL Server 2022+, delivered as pure T-SQL versioned migration scripts. Install = apply the migrations to a database. No services, no agents, no host-language runtime required by the product.

## Goals

1. **Pure T-SQL deliverable.** Install with Flyway/Liquibase/sqlcmd; no other tooling required.
2. **Portable across SQL Server 2022+ on Windows and Linux.** No CLR, no `xp_*`, no Service Broker, no FILESTREAM, no MSDTC.
3. **FIFO semantics by default**, including pgmq's grouped-FIFO ("one in-flight per group") read variant.
4. **Two storage variants behind a single logical API:** on-disk tables and memory-optimized (Hekaton) tables. Both ship in v1 specifically to compare via a head-to-head bake-off.
5. **Concurrency correctness over feature count.** Every operation has multi-session tests asserting no loss, no double-delivery, VT honored, FIFO ordering, and the grouped-FIFO invariant.
6. **Public OSS** under github.com/FreeSideNomad/sqlmq, Apache 2.0.

## Non-goals (v1)

- **No server-side `read_with_poll` proc.** Long-polling is a client-side concern. Justified by the natively-compiled-procs constraint (`WAITFOR` not allowed) plus industry practice (Hangfire / NServiceBus / Wolverine / Quartz.NET all client-poll). See `research/sqlserver-longpoll.md` for the full evidence.
- **No partitioned queues.** pgmq's partitioning depends on `pg_partman`; SQL Server's native partition function/scheme has no equivalent auto-partition worker. Defer to a later release; data model leaves room.
- **No topics / AMQP-style routing** (pgmq has these in `topic_bindings`; deferred).
- **No `LISTEN/NOTIFY`-style notifications.** SQL Server has no equivalent without Service Broker.
- **No down migrations.** Forward-only. pgmq doesn't have them either.
- **No content-type as a top-level column.** Goes in the existing `headers` JSON.

## Repository layout

```
sqlmq/
├── LICENSE                                 # Apache 2.0
├── README.md
├── CONTRIBUTING.md                          # minimal
├── CLAUDE.md                                # project guidance for Claude Code
├── .gitignore
├── .github/workflows/
│   ├── ci.yml                               # Maven verify on every push
│   └── benchmark.yml                        # opt-in nightly bake-off
├── docs/
│   └── superpowers/specs/
│       └── 2026-05-02-sqlmq-port-from-pgmq-design.md   # this file
├── research/
│   ├── pgmq-surface.md
│   ├── sqlserver-patterns.md
│   └── sqlserver-longpoll.md
├── sql/
│   └── migrations/                          # Flyway-style, forward-only
│       ├── V001__core_schema.sql
│       ├── V002__queue_create_drop_procs.sql
│       ├── V003__send_procs.sql
│       ├── V004__read_procs.sql
│       ├── V005__delete_archive_pop_procs.sql
│       ├── V006__metrics_procs.sql
│       └── V007__purge_drop_procs.sql
└── harness/
    ├── pom.xml                              # Maven
    └── src/
        ├── main/java/io/freesidenomad/sqlmq/client/
        │   ├── SqlmqClient.java             # JDBC convenience wrapper for tests
        │   └── LongPollingConsumer.java     # Hangfire-style 250ms backoff, [100,1000]ms clamp
        └── test/java/io/freesidenomad/sqlmq/
            ├── correctness/
            ├── concurrency/
            └── benchmark/
```

**Key boundary:** `harness/` is a development and CI artifact. It is not shipped to consumers. The product is `sql/migrations/`. Consumers in any language call the procs directly via their own JDBC/ODBC/etc. driver.

## Data model

### Schema

A single `sqlmq` schema, created in V001. Avoids `dbo` clutter and lets consumers grant per-schema permissions.

### Installation-wide tables (V001)

**`sqlmq.meta`** — queue registry:

| Column | Type | Notes |
|---|---|---|
| `queue_name` | `SYSNAME` PK | |
| `storage_type` | `VARCHAR(16)` NOT NULL | `'ondisk'` \| `'inmemory'` |
| `is_grouped` | `BIT` NOT NULL DEFAULT 0 | does this queue support grouped-FIFO reads |
| `payload_type` | `VARCHAR(8)` NOT NULL | `'json'` \| `'binary'` |
| `max_delivery_count` | `INT` NULL | NULL = no DLQ enforcement |
| `created_at` | `DATETIME2(7)` NOT NULL | DEFAULT `SYSUTCDATETIME()` |

**`flyway_schema_history`** — owned by Flyway, applied with first migration (or by whatever migration runner the consumer uses).

### Per-queue tables (created by `sqlmq.create_queue`)

**`sqlmq.q_<name>`** — active queue:

| Column | Type | Notes |
|---|---|---|
| `msg_id` | `BIGINT IDENTITY(1,1)` PK | Monotonic, FIFO ordering key |
| `enqueued_at` | `DATETIME2(7)` NOT NULL DEFAULT `SYSUTCDATETIME()` | |
| `vt` | `DATETIME2(7)` NOT NULL | Visibility timeout deadline |
| `read_ct` | `INT` NOT NULL DEFAULT 0 | |
| `group_key` | `NVARCHAR(255)` NULL | for grouped-FIFO; NULL = ungrouped |
| `message` | `NVARCHAR(MAX)` NOT NULL | when `payload_type='json'`; else the column is omitted |
| `message_bin` | `VARBINARY(MAX)` NOT NULL | when `payload_type='binary'`; else omitted |
| `headers` | `NVARCHAR(MAX)` NULL | JSON; `CHECK (headers IS NULL OR ISJSON(headers) = 1)` |

For `payload_type='json'`: `CHECK (ISJSON(message) = 1)` on `message`.

**`sqlmq.a_<name>`** — archive:

Same shape as `sqlmq.q_<name>` **without `IDENTITY`** (preserves original `msg_id`), plus:

| Column | Type | Notes |
|---|---|---|
| `archived_at` | `DATETIME2(7)` NOT NULL DEFAULT `SYSUTCDATETIME()` | |
| `dlq_reason` | `NVARCHAR(64)` NULL | NULL = clean archive; e.g. `'max_deliveries'` |

**Archive is always on-disk, even for in-memory queues.** Deliberate asymmetry: hot queue on memory-optimized for throughput; cold archive on disk for retention. Documented.

### Indexes

**On-disk:**
- Clustered PK on `msg_id` (FIFO scans; updates to `vt`/`read_ct` don't move rows since `msg_id` doesn't change)
- `NCI (vt, msg_id)` — dequeue seek + ordered output
- `NCI (group_key, vt, msg_id) WHERE group_key IS NOT NULL` — filtered, only grouped queues pay the cost

**In-memory:**
- All NONCLUSTERED (no clustered concept in memory-optimized tables)
- HASH on `msg_id` PK for point lookup (delete/archive single msg)
- NONCLUSTERED on `(msg_id)` for ordered scans
- NONCLUSTERED on `(vt, msg_id)`
- NONCLUSTERED on `(group_key, vt, msg_id)` — unconditional; filtered indexes not supported on memory-optimized tables

### Two TVPs (V001)

```sql
CREATE TYPE dbo.sqlmq_send_tvp AS TABLE (
    message       NVARCHAR(MAX) NULL,    -- json queues
    message_bin   VARBINARY(MAX) NULL,   -- binary queues
    headers       NVARCHAR(MAX) NULL,
    delay_seconds INT NOT NULL DEFAULT 0
);
CREATE TYPE dbo.sqlmq_msg_id_tvp AS TABLE (
    msg_id BIGINT NOT NULL PRIMARY KEY
);
```

Caller fills only the relevant message column based on the queue's `payload_type`. Procs validate alignment.

## Stored procedure API

### Naming convention

- `sqlmq.<verb>` — public dispatcher procs (and one inline TVF)
- `sqlmq._<verb>_<storage>` — internal, generic, interpreted (on-disk variant)
- `sqlmq._<verb>_inmem_<queue_name>` — internal, generated per queue, natively compiled (in-memory variant)

Underscore prefix marks "do not call directly."

### Public surface

| Object | Form | Params | Returns |
|---|---|---|---|
| `sqlmq.create_queue` | proc | `@name SYSNAME, @storage VARCHAR(16) = 'ondisk', @grouped BIT = 0, @payload_type VARCHAR(8) = 'json', @max_delivery_count INT = NULL` | — |
| `sqlmq.drop_queue` | proc | `@name SYSNAME` | — |
| `sqlmq.queues` | **inline TVF** | — | rows from `sqlmq.meta` |
| `sqlmq.send` | proc | `@queue SYSNAME, @message NVARCHAR(MAX) = NULL, @message_bin VARBINARY(MAX) = NULL, @headers NVARCHAR(MAX) = NULL, @delay_seconds INT = 0` | `msg_id BIGINT` |
| `sqlmq.send_batch` | proc | `@queue SYSNAME, @messages dbo.sqlmq_send_tvp READONLY` | result set: one `msg_id` per row |
| `sqlmq.read` | proc | `@queue SYSNAME, @vt_seconds INT, @max_count INT = 1` | `msg_id, read_ct, enqueued_at, vt, message`/`message_bin`, `headers` |
| `sqlmq.read_grouped` | proc | `@queue SYSNAME, @vt_seconds INT, @max_count INT = 1` | same shape; enforces "one in-flight per group_key" |
| `sqlmq.pop` | proc | `@queue SYSNAME` | one row, deleted in same tx |
| `sqlmq.delete` | proc | `@queue SYSNAME, @msg_ids dbo.sqlmq_msg_id_tvp READONLY` | rows deleted (count) |
| `sqlmq.archive` | proc | `@queue SYSNAME, @msg_ids dbo.sqlmq_msg_id_tvp READONLY, @reason NVARCHAR(64) = NULL` | rows archived (count) |
| `sqlmq.purge_queue` | proc | `@name SYSNAME` | rows deleted (count) |
| `sqlmq.dlq_sweep` | proc | `@queue SYSNAME` | rows swept to archive (count) |
| `sqlmq.metrics` | proc | `@queue SYSNAME` | `queue_name, queue_length, total_messages, oldest_msg_age_seconds, dlq_count` |
| `sqlmq.metrics_all` | proc | — | one row per queue (same shape) |

`sqlmq.queues` is an inline TVF because it reads only the fixed `sqlmq.meta` table — composable in user queries (`SELECT * FROM sqlmq.queues() WHERE storage_type = 'inmemory'`). All other operations either mutate state (forbidden in functions) or require dynamic SQL on `sqlmq.q_<name>` (also forbidden in inline TVFs), so they are procs.

### Storage dispatch — three categories

A natively compiled proc can only reference memory-optimized tables. Cross-engine writes (active in-memory queue → on-disk archive) cannot run inside a natively compiled module. This forces three operation categories:

| Category | Procs | Implementation |
|---|---|---|
| **Hot path, storage-dispatched** | `send`, `send_batch`, `read`, `read_grouped`, `pop`, `delete`, `metrics` | Dispatcher looks up `storage_type` in `sqlmq.meta`, then `EXEC`s either `sqlmq._<verb>_ondisk` (interpreted, generic, dynamic SQL) or `sqlmq._<verb>_inmem_<queue_name>` (per-queue natively compiled, no dynamic SQL) |
| **Cross-engine, always interpreted** | `archive`, `purge_queue`, `dlq_sweep` | One generic interpreted proc handles both storage types. Reads from active queue table (in-memory or on-disk), writes to disk-based archive. Acceptable cost — these are not the hot path. |
| **Admin, no dispatch** | `create_queue`, `drop_queue`, `metrics_all` | Single interpreted proc; for in-memory queues, `create_queue` additionally generates the per-queue natively compiled inner procs and `drop_queue` removes them. |

Result sets flow back through the dispatcher unchanged. The dispatcher overhead is one `meta` lookup per call.

### Per-queue specialization (in-memory only)

Natively compiled procs cannot use dynamic SQL, so the in-memory variant generates per-queue specialized procs at `create_queue` time — one per operation per queue. `drop_queue` cleans them up. The inner proc names embed the queue name (e.g. `sqlmq._read_inmem_orders`); the dispatcher constructs the name and `EXEC`s it.

This adds ~6 generated procs per in-memory queue. Accepted because the bake-off requires the in-memory variant to run at its true performance ceiling — interpreted-only would undercount and bias the long-term storage decision.

### Transactional contract

- All procs run in the **caller's transaction**. None do `BEGIN TRAN`/`COMMIT`/`ROLLBACK`.
- A read inside an uncommitted transaction is invisible to other consumers (UPDLOCK held). Caller commits → message in-flight. Caller rolls back → message immediately re-eligible.
- A send inside an uncommitted transaction is invisible to consumers until commit.

Matches pgmq's behavior, validated against upstream isolation tests in `research/pgmq-surface.md`.

### Delivery guarantees

- **At-least-once.** A message may be delivered more than once if a consumer reads it but fails to delete/archive before its `vt` expires (consumer crash, network drop, transaction rollback). Consumers must be idempotent.
- **No de-duplication on send.** Producers responsible for not double-sending; same as pgmq, SQS, RabbitMQ.
- **FIFO ordering by `msg_id`** for ungrouped reads (within the limits a concurrent multi-consumer queue can offer — strict in-order delivery to a *single* consumer; competing consumers may interleave).
- **Grouped-FIFO invariant** (see Concurrency model section): for any `group_key`, at most one in-flight message at a time.

## Concurrency model

### On-disk dequeue — table-as-queue with all four hints

```sql
WITH claimed AS (
    SELECT TOP (@max_count) msg_id, read_ct
    FROM sqlmq.q_<name> WITH (READPAST, UPDLOCK, ROWLOCK, READCOMMITTEDLOCK)
    WHERE vt <= SYSUTCDATETIME()
      AND (@max_dlq IS NULL OR read_ct < @max_dlq)
    ORDER BY msg_id
)
UPDATE q
SET vt = DATEADD(SECOND, @vt_seconds, SYSUTCDATETIME()),
    read_ct = read_ct + 1
OUTPUT inserted.msg_id, inserted.read_ct, inserted.enqueued_at,
       inserted.vt, inserted.message, inserted.headers
FROM sqlmq.q_<name> q
INNER JOIN claimed c ON q.msg_id = c.msg_id;
```

Why each hint matters:
- `READPAST` — skip rows held by other readers' UPDLOCK; what gives "competing consumers" without blocking
- `UPDLOCK` — acquired during the SELECT half so two readers can't both claim the same row
- `ROWLOCK` — prevent escalation to page locks (which silently defeat `READPAST`)
- `READCOMMITTEDLOCK` — force lock-based read-committed even under RCSI (Azure SQL DB default; commonly enabled in prod)

### In-memory dequeue — natively compiled, optimistic, retry on conflict

```sql
CREATE PROCEDURE sqlmq._read_inmem_<queue_name>
    @vt_seconds INT, @max_count INT, @max_dlq INT  -- @max_dlq passed in by dispatcher
WITH NATIVE_COMPILATION, SCHEMABINDING AS
BEGIN ATOMIC WITH (TRANSACTION ISOLATION LEVEL = SNAPSHOT, LANGUAGE = N'us_english')
    DECLARE @attempts INT = 0;
    WHILE @attempts < 3
    BEGIN
        BEGIN TRY
            UPDATE TOP (@max_count) sqlmq.q_<name>
            SET vt = DATEADD(SECOND, @vt_seconds, SYSUTCDATETIME()),
                read_ct = read_ct + 1
            OUTPUT inserted.msg_id, inserted.read_ct, /* ... */
            WHERE msg_id IN (
                SELECT TOP (@max_count) msg_id
                FROM sqlmq.q_<name>
                WHERE vt <= SYSUTCDATETIME()
                  AND (@max_dlq IS NULL OR read_ct < @max_dlq)  -- exclude over-cap; dlq_sweep handles them
                ORDER BY msg_id
            );
            RETURN 0;
        END TRY
        BEGIN CATCH
            IF ERROR_NUMBER() = 41302  -- write conflict under snapshot isolation
                SET @attempts += 1;
            ELSE
                THROW;
        END CATCH
    END
END
```

The dispatcher reads both `storage_type` and `max_delivery_count` from `sqlmq.meta` in one SELECT, then passes `@max_dlq` to the inner proc. No second meta lookup.

No `READPAST`/`UPDLOCK`. Concurrency is snapshot isolation + retry on write conflict. The 3-attempt cap is empirical; on persistent contention the proc returns empty and the caller backs off as if the queue were empty.

### Grouped-FIFO invariant

*For any `group_key` with an in-flight (vt > now) message, no other message with the same `group_key` may be returned until the in-flight one is deleted, archived, or its vt expires.*

```sql
WITH eligible AS (
    SELECT msg_id, group_key,
           ROW_NUMBER() OVER (PARTITION BY group_key ORDER BY msg_id) AS rn
    FROM sqlmq.q_<name> AS q WITH (READPAST, UPDLOCK, ROWLOCK, READCOMMITTEDLOCK)
    WHERE NOT EXISTS (
        SELECT 1 FROM sqlmq.q_<name> AS inflight
        WHERE inflight.group_key = q.group_key
          AND inflight.vt > SYSUTCDATETIME()
    )
),
claimed AS (
    SELECT TOP (@max_count) msg_id
    FROM eligible
    WHERE rn = 1
    ORDER BY msg_id
)
UPDATE q SET vt = DATEADD(SECOND, @vt_seconds, SYSUTCDATETIME()), read_ct = read_ct + 1
OUTPUT inserted.msg_id, /* ... */
FROM sqlmq.q_<name> q INNER JOIN claimed c ON q.msg_id = c.msg_id;
```

The `WHERE NOT EXISTS` correlated subquery is what the `(group_key, vt, msg_id)` index is for. This is the index-stress case; concurrency tests must specifically hammer it.

### DLQ flow — exclude-and-sweep

A natively compiled proc cannot write to a disk-based table, so the read proc cannot also archive over-cap messages atomically. Both variants therefore use the same pattern:

1. **Exclusion at read.** The dequeue WHERE clause includes `AND (@max_dlq IS NULL OR read_ct < @max_dlq)`. Messages that reach the cap stop being delivered. They stay in the queue, occupying space, invisible to readers.
2. **Periodic sweep.** `sqlmq.dlq_sweep @queue` (interpreted, generic, runs against either storage variant) atomically moves over-cap messages to the archive with `dlq_reason = 'max_deliveries'`. Caller's responsibility to invoke periodically — explicitly, on a schedule, or after every N reads.

Trade-off: messages over the cap occupy queue space until the sweep runs. Operators control sweep cadence based on their DLQ-volume tolerance. Symmetric across both storage variants, no engine-crossing in the hot path.

### Send concurrency

`IDENTITY(1,1)` on `msg_id` gives monotonic ordering for FIFO. Gaps on rollback are fine — order is preserved. For installations where gap-on-restart matters: document `ALTER DATABASE SCOPED CONFIGURATION SET IDENTITY_CACHE = OFF`. Not enforced by sqlmq; a DBA call.

### RCSI footgun (on-disk only)

`READPAST` is meaningless under `READ_COMMITTED_SNAPSHOT` (RCSI) — readers don't take shared locks, so there's nothing to skip, and concurrent dequeuers double-deliver. Mitigation: every on-disk dequeue statement carries `WITH (READPAST, UPDLOCK, ROWLOCK, READCOMMITTEDLOCK)`. The fourth hint forces lock-based read-committed semantics on this one statement regardless of database default. Costs nothing on a non-RCSI DB. The in-memory variant doesn't have this problem (different concurrency machinery entirely).

## Testing strategy and bake-off

### Test infrastructure

- **Java 25** — virtual threads (`Thread.ofVirtual()`, stable `StructuredTaskScope`) for the N×M concurrency tests
- **Maven** — build tool
- **JUnit 5** — `@Test`, `@ParameterizedTest`
- **Microsoft `mssql-jdbc`** — driver
- **HikariCP** — connection pool; same config across variants for benchmark fairness
- **Flyway-Core** — applies `sql/migrations/V*__*.sql` to test databases
- **Testcontainers + `mssqlserver` module** — `mcr.microsoft.com/mssql/server:2022-latest` per test class
- **AssertJ** — readable assertions

### CI

GitHub Actions on Linux runners against the Linux SQL Server 2022 image. Two workflows:
- `ci.yml` — `mvn verify`, runs Tier 1 + Tier 2 on every push
- `benchmark.yml` — runs Tier 3 (the bake-off), opt-in (manual dispatch + nightly)

Windows portability is checked manually pre-release; not gated on CI because GH-hosted Windows runners are slower and the engine on Linux is the same engine.

### Test taxonomy

#### Tier 1 — correctness (single-session)

One test per proc per storage variant. Parameterized via `@ParameterizedTest` with `@MethodSource` over `{ondisk, inmemory} × {grouped, ungrouped} × {json, binary}`. Asserts each proc's contract: `send` returns `msg_id`, `read` returns the message, `vt` is honored within one session, `archive` moves the row, etc. Fast.

#### Tier 2 — concurrency (multi-session)

The integrity bar. Each test:
1. Creates a queue
2. Uses `StructuredTaskScope.ShutdownOnFailure` to launch N virtual-thread producers + M virtual-thread consumers
3. Producers send K messages each
4. Consumers loop: `read` → simulate work → `delete`
5. Test waits for all virtual threads, then asserts global invariants

**Invariants enforced (every concurrency test):**
- **No loss:** `produced_count == delivered_count + deleted_count + still_in_queue`
- **No double-delivery:** for each `msg_id`, exactly one consumer received it
- **VT honored:** no two consumers held the same `msg_id` simultaneously
- **FIFO ordering (ungrouped):** for any single consumer, the `msg_id`s it received are monotonically increasing
- **Grouped-FIFO invariant:** for any `group_key`, no two consumers were ever holding messages from that group simultaneously
- **DLQ enforcement:** messages reaching `max_delivery_count` end up in archive with `dlq_reason='max_deliveries'`, never re-delivered

**Standard concurrency profiles** (run all invariants against each):
- **Light:** 4 producers × 4 consumers × 100 msgs each
- **Medium:** 32 × 32 × 1,000
- **Heavy:** 256 × 256 × 10,000
- **Asymmetric:** 1 × 64 × 100,000 (slow producer, fast consumers)
- **Burst:** all messages preloaded, 1024 consumers race to drain

Tier 2 must pass on both storage variants before claiming the bake-off is fair.

#### Tier 3 — benchmark (the bake-off)

Same workloads as Tier 2 but measured, not asserted. Headline metric: **end-to-end throughput (msgs/sec) at p50, p95, p99 latency.**

Bake-off rig — locked-down for reproducibility:

| Variable | Setting |
|---|---|
| SQL Server | `mcr.microsoft.com/mssql/server:2022-latest` Linux image |
| Container resource limits | 4 vCPU / 8 GB RAM (documented; can be overridden) |
| Connection pool | HikariCP, `maximumPoolSize = 64`, `connectionTimeout = 5000` |
| Workloads | The five concurrency profiles above |
| Run discipline | 30 s warmup, 60 s measurement, 5 runs per profile, report median + IQR |
| Output | `target/bench-results/<timestamp>/<storage>-<profile>.json` + `bench-summary.md` |

Both variants run identical workloads back-to-back in the same JVM and same container session. Result is two columns side-by-side per workload. **If in-memory wins decisively, on-disk gets retired** — the bake-off output is what makes that call.

### Long-polling

The Java harness ships `LongPollingConsumer` in `harness/src/main/java/io/freesidenomad/sqlmq/client/`. Pattern (from Hangfire):
- Default backoff: 250 ms
- Clamp: [100 ms, 1000 ms]
- After a non-empty result, shorten the next interval to the minimum
- After consecutive empty results, lengthen toward the maximum

Same-process `AutoResetEvent`-style notification (producer signals same-JVM consumer) is a v2 optimization — must not change the proc surface.

## Operational decisions

| Concern | Decision |
|---|---|
| License | Apache 2.0 |
| Public hosting | github.com/FreeSideNomad/sqlmq |
| Java package | `io.freesidenomad.sqlmq` |
| Maven coords | `groupId=io.freesidenomad`, `artifactId=sqlmq-harness` |
| Migrations | Flyway-style `V*__*.sql`, forward-only, idempotent NOT required (Flyway tracks applied) |
| Migration runner | Not bundled — consumers run with whatever they have |
| CI | GitHub Actions, Linux SQL Server 2022 image |
| Windows portability | Validated manually pre-release |

## Risks and open questions

1. **Per-queue natively compiled procs at scale.** A deployment with hundreds of in-memory queues will have ~6 × N generated procs. SQL Server's catalog can handle this but adds maintenance surface. Tracked as a v1 known-cost; revisit if operators report issues.

2. **Bake-off result might be ambiguous.** If on-disk and in-memory perform within ~20% of each other across all five profiles, "abandon the loser" may not be the right call. Spec the post-bake-off decision as: clear winner (>2× across all profiles) → retire loser; mixed (winner depends on workload) → keep both, document workload guidance.

3. **Sweep cadence trade-off.** The exclude-and-sweep DLQ pattern means over-cap messages remain in the active queue until `sqlmq.dlq_sweep` runs. Operators with tight DLQ-latency or queue-size budgets must call sweep frequently. Document recommended cadence (e.g., once per N reads, or every M seconds) once benchmarks measure sweep cost.

4. **JSON validation cost on send.** `CHECK (ISJSON(message) = 1)` runs on every insert. For very high-throughput JSON queues, this could be a measurable percentage of send cost. If the bake-off shows it's significant, document an opt-out (e.g., `@payload_type = 'json_unchecked'`) — but only if real evidence demands it.

5. **`max_delivery_count` change after queue creation.** v1 treats this as immutable per queue (set at `create_queue`, no `alter_queue` proc). If a real consumer needs to change it, add `sqlmq.alter_queue` in v1.x — non-breaking.

6. **Manual Windows validation discipline.** Saying "Windows pre-release" is honest but easy to skip. The release checklist must include a documented Windows run; otherwise the portability claim is aspirational.

## Out of scope (explicitly)

- Authentication / authorization (consumer's database security model handles this)
- Encryption at rest (SQL Server TDE handles this)
- Multi-database / multi-tenant per-database isolation (each install lives in one database)
- Cross-database queue references (queues are local to the database they're installed in)
- Sliding-window archive partitioning (deferred to a later release)
- pgmq's `unlogged` mode (in-memory `SCHEMA_AND_DATA` is the closest equivalent)
- pgmq's `pg_partman`-driven partitioned queues
- pgmq's topic / regex routing
- pgmq's NOTIFY/LISTEN throttling

## References

- pgmq upstream: https://github.com/tembo-io/pgmq
- pgmq surface research: `research/pgmq-surface.md` (801 lines)
- SQL Server queue patterns research: `research/sqlserver-patterns.md` (893 lines)
- SQL Server long-poll research: `research/sqlserver-longpoll.md` (749 lines)
