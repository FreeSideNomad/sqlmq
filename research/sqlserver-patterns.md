# SQL Server queue patterns — technical reference for porting pgmq to SQL Server 2022+

**Scope.** Reference notes for implementing a pgmq-equivalent message queue on SQL Server 2022 (16.x) and later, deployable on both Windows and Linux. Primary source: `learn.microsoft.com`. Secondary: Remus Rusanu, Erik Darling. All Microsoft pages were fetched live (most carry `ms.date` 2025–2026).

> Note on canonical URLs. Every Microsoft Learn link below uses `?view=sql-server-ver16` (SQL Server 2022). Pages auto-redirect to `ver17` (SQL Server 2025) but the content applies to 2022 unless we call out a 2025-only feature. Where SQL Server 2022 differs from 2025, it is flagged.

Contents:

1. Table-as-queue pattern (`READPAST` + `UPDLOCK` + `ROWLOCK`)
2. Service Broker — verdict
3. Memory-optimized tables for queues
4. `OUTPUT` clause patterns for atomic claim-and-return
5. Identity vs. sequence for monotonic IDs
6. Partitioned tables for archive / retention
7. Linux portability gotchas
8. Deadlock and contention diagnosis
9. Long-polling equivalent (`read_with_poll`)
10. Idempotent migration scripts

---

## 1. Table-as-queue with `READPAST` + `UPDLOCK` + `ROWLOCK`

### 1.1 What each hint actually does

From [Table Hints (Transact-SQL) — `learn.microsoft.com`](https://learn.microsoft.com/en-us/sql/t-sql/queries/hints-transact-sql-table?view=sql-server-ver16):

- **`READPAST`** — "Specifies that the Database Engine not read rows that are locked by other transactions. When `READPAST` is specified, **row-level locks are skipped, but page-level locks aren't skipped**. … `READPAST` is primarily used to reduce locking contention when implementing a work queue that uses a SQL Server table. A queue reader that uses `READPAST` skips past queue entries locked by other transactions to the next available queue entry, without having to wait until the other transactions release their locks." (MS Learn)
- **`UPDLOCK`** — "Specifies that update locks are to be taken and held until the transaction completes. `UPDLOCK` takes update locks for read operations only at the row-level or page-level. If `UPDLOCK` is combined with `TABLOCK`, or a table-level lock is taken for some other reason, an exclusive (X) lock is taken instead." (MS Learn)
- **`ROWLOCK`** — "Specifies that row locks are taken when page or table locks are ordinarily taken. When specified in transactions operating at the `SNAPSHOT` isolation level, row locks aren't taken unless `ROWLOCK` is combined with other table hints that require locks, such as `UPDLOCK` and `HOLDLOCK`." (MS Learn)

### 1.2 Why all three together

Each hint addresses an independent failure mode. Drop any one and the pattern silently degrades:

| Hint | Solves |
|---|---|
| `READPAST` | Concurrent consumers skip rows already claimed by another transaction instead of blocking. |
| `UPDLOCK` | The row we *intend to claim* gets a `U` lock that other readers (also using `UPDLOCK`) cannot acquire — they `READPAST` it. Plain `S` locks would let two consumers both think they have the row. |
| `ROWLOCK` | Forces the locks above to be taken at row granularity. Without `ROWLOCK`, the optimizer can choose page locks; **`READPAST` does not skip page locks**, so consumers block. |

Erik Darling in [*When `READPAST` Doesn't Read Past*](https://erikdarling.com/when-readpast-doesnt-read-past/) demonstrates the failure case directly: an `UPDATE` without `ROWLOCK` taking "a single exclusive page lock, which means our `readpast` query can't skip it." His fix: either add `ROWLOCK` to the reading query or create a covering index so the optimizer naturally chooses row locks. Quote: *"Our read query is shooting itself in the foot by trying to lock pages rather than rows."*

In his queue procedure ([*Building Reusable Queues, Part 2*](https://erikdarling.com/building-reusable-queues-part-2/)) he restates the rationale per hint:

- `READPAST`: "skip over locked rows, but not ignore locks like `NOLOCK` would do"
- `ROWLOCK`: "nicely ask the optimizer to only lock a single row"
- `UPDLOCK`: "Even though this is a `SELECT`, take an `UPDATE` lock (`LCK_M_U`)"

Remus Rusanu in [*Using tables as queues*](https://rusanu.com/2010/03/26/using-tables-as-queues/) makes the structural recommendation: *"The table must be organized as a clustered index ordered by a key that preserves the desired dequeue order."* For FIFO, that key is an `IDENTITY` or `SEQUENCE` column. Rusanu also notes the trade-off: *"If strict FIFO order is required then you have to remove the `readpast` hint"* — which serializes consumers. With `READPAST` you get **lax FIFO**: typically in-order, occasionally not under contention. For a pgmq port this is the right trade.

### 1.3 Required isolation level

From [Table Hints docs](https://learn.microsoft.com/en-us/sql/t-sql/queries/hints-transact-sql-table?view=sql-server-ver16):

> "`READPAST` can only be specified in transactions operating at the `READ COMMITTED` or `REPEATABLE READ` isolation levels."

> "The `READPAST` table hint can't be specified when the `READ_COMMITTED_SNAPSHOT` database option is set to `ON` and either of the following conditions is true: the transaction isolation level of the session is `READ COMMITTED`; the `READCOMMITTED` table hint is also specified in the query. To specify the `READPAST` hint in these cases, remove the `READCOMMITTED` table hint if present, and include the `READCOMMITTEDLOCK` table hint in the query."

This last point is operationally critical. Many production SQL Server databases run with `READ_COMMITTED_SNAPSHOT = ON` (RCSI). If the consumer code does `SET TRANSACTION ISOLATION LEVEL READ COMMITTED` and then uses `READPAST`, **the queue silently breaks under RCSI** — the engine uses row versions instead of locks, so there are no `U` locks for `READPAST` to skip. Always pair `READPAST` + `UPDLOCK` + `ROWLOCK` with `READCOMMITTEDLOCK`, or set the session to `REPEATABLE READ`. The `pgmq`-port queue procs should not assume RCSI is off.

### 1.4 Canonical T-SQL skeleton — destructive read (no visibility timeout)

From [`OUTPUT` clause docs](https://learn.microsoft.com/en-us/sql/t-sql/queries/output-clause-transact-sql?view=sql-server-ver16), the simplest dequeue, returning the deleted row to the caller:

```sql
SET XACT_ABORT ON;
SET TRANSACTION ISOLATION LEVEL READ COMMITTED;

;WITH cte AS (
    SELECT TOP (@batch_size) *
    FROM   dbo.q WITH (READPAST, UPDLOCK, ROWLOCK, READCOMMITTEDLOCK)
    ORDER  BY id   -- clustered key = dequeue order
)
DELETE FROM cte
OUTPUT deleted.*;
```

Rusanu's variant uses the same structure:

```sql
with cte as (
  select top(1) Payload
    from FifoQueue with (rowlock, readpast)
  order by Id)
delete from cte
  output deleted.Payload;
```

He notes the key invariant: *"the query execution will occur as a `DELETE`, not as a `SELECT` followed by a `DELETE`."* That is what makes the claim atomic — no race window between locating and removing the row.

### 1.5 Visibility-timeout dequeue (pgmq-equivalent `read`)

`pgmq.read` does not delete; it marks the row invisible until `vt` (visibility timeout) expires. SQL Server equivalent: a separate column `vt datetime2` and the `WHERE` clause filters on it. The claim is an `UPDATE`, not a `DELETE`:

```sql
CREATE OR ALTER PROCEDURE dbo.q_read
    @qty       int,
    @vt_secs   int
AS
BEGIN
    SET NOCOUNT, XACT_ABORT ON;

    DECLARE @now datetime2(3) = SYSUTCDATETIME();

    ;WITH claim AS (
        SELECT TOP (@qty) *
        FROM   dbo.q WITH (READPAST, UPDLOCK, ROWLOCK, READCOMMITTEDLOCK)
        WHERE  vt <= @now
        ORDER  BY id   -- FIFO
    )
    UPDATE claim
    SET    vt          = DATEADD(second, @vt_secs, @now),
           read_ct     = read_ct + 1
    OUTPUT inserted.id,
           inserted.read_ct,
           inserted.enqueued_at,
           inserted.vt,
           inserted.message;
END;
```

Notes:

- The `TOP (@qty) … ORDER BY id` inside the CTE is the *driving* selection that gets `UPDLOCK` row locks. The outer `UPDATE` reuses those locks.
- `OUTPUT inserted.*` returns the post-update state (including the new `vt`) to the caller. From the [`OUTPUT` docs](https://learn.microsoft.com/en-us/sql/t-sql/queries/output-clause-transact-sql?view=sql-server-ver16): *"Columns prefixed with `INSERTED` reflect the value after the `UPDATE`, `INSERT`, or `MERGE` statement is completed but before triggers are executed."*
- `vt` is initialized to `enqueued_at` (or `SYSUTCDATETIME()`) at insert. New messages are immediately visible.
- "Archive after delete" / `pgmq.archive` is a second `DELETE … OUTPUT deleted.* INTO dbo.q_archive` — see §6 for partitioned archive.

The MS Learn page on `OUTPUT` explicitly endorses this pattern in its **Queues** section:

> "Use the `READPAST` table hint in `UPDATE` and `DELETE` statements if your scenario allows for multiple applications to perform a destructive read from one table. This prevents locking issues that can come up if another application is already reading the first qualifying record in the table." ([MS Learn](https://learn.microsoft.com/en-us/sql/t-sql/queries/output-clause-transact-sql?view=sql-server-ver16))

### 1.6 Pitfalls

**Lock escalation.** Per [Lock Escalation guidance](https://learn.microsoft.com/en-us/sql/relational-databases/sql-server-transaction-locking-and-row-versioning-guide?view=sql-server-ver16):

> "Lock escalation is triggered when a Transact-SQL statement acquires at least 5,000 locks on a single reference of a table."

If a single dequeue tries to lock more than 5000 rows (e.g. someone calls `q_read` with `@qty = 10000`), the engine will escalate to a single `X` table lock and every other consumer blocks. Mitigations:

- Cap `@qty` in the procedure (pgmq itself caps batches well below 5000).
- `ALTER TABLE dbo.q SET (LOCK_ESCALATION = AUTO);` and partition the queue, so escalation hits a single partition rather than the whole table.
- For very hot queues, consider `ALTER TABLE … SET (LOCK_ESCALATION = DISABLE);` — accepted at your own memory-pressure risk.

**Page locks defeating `READPAST`.** Already covered in §1.2. Always provide an index whose leading key matches the `WHERE` / `ORDER BY` of the dequeue. For pgmq's primary access pattern (`WHERE vt <= now ORDER BY id`), the right index is either:

```sql
-- Option A: clustered on id, filtered nonclustered on (vt, id) for hot reads
CREATE NONCLUSTERED INDEX ix_q_vt ON dbo.q (vt, id) INCLUDE (read_ct);

-- Option B: clustered on (vt, id), if archive-after-delete keeps the table small
```

Without one, `READPAST` will routinely degrade to page-level blocking.

**Deadlock under contention.** Two consumers reading + one producer inserting can deadlock if the index leaf ordering and access pattern are not aligned. Standard mitigations:

- Always read in the same order (`ORDER BY id`) across all queue consumer code paths.
- Wrap the dequeue in a `BEGIN TRY … BEGIN CATCH WHERE ERROR_NUMBER() = 1205 retry` loop. (MS Learn's [memory-optimized retry sample](https://learn.microsoft.com/en-us/sql/relational-databases/in-memory-oltp/transactions-with-memory-optimized-tables?view=sql-server-ver16) is a good template; the same retry shape works for on-disk 1205 deadlocks.)
- Keep the dequeue transaction tiny — claim, return, commit. No application work between claim and commit.

**Optimistic ("optimized") locking on SQL Server 2022+.** SQL Server 2022 introduced [Optimized Locking](https://learn.microsoft.com/en-us/sql/relational-databases/performance/optimized-locking?view=sql-server-ver16) (TID locks via `XACT` resource type — see §8). If enabled it changes lock-acquisition mechanics; the queue pattern still works but lock-diagnosis queries return new resource types. Disable per-table if it interferes (`ALTER DATABASE … SET OPTIMIZED_LOCKING = OFF` if needed).

---

## 2. Service Broker — verdict

### 2.1 What it offers

From [SQL Server Service Broker — `learn.microsoft.com`](https://learn.microsoft.com/en-us/sql/database-engine/configure-windows/sql-server-service-broker?view=sql-server-ver16):

> "SQL Server Service Broker provide native support for messaging and queuing in the SQL Server Database Engine and Azure SQL Managed Instance."

The feature surface includes:

- **Queues** as first-class objects (`CREATE QUEUE`).
- **Services** and **contracts** binding message types to queues.
- **Conversations / dialogs** — ordered, exactly-once, transactional message exchange between two services. `BEGIN DIALOG`, `SEND ON CONVERSATION`, `RECEIVE`.
- **Conversation groups** — group related conversations so only one consumer touches them at a time (preserves ordering across multiple messages).
- **Activation** — internal or external proc fires when messages arrive (`ALTER QUEUE … WITH ACTIVATION (PROCEDURE_NAME = …, MAX_QUEUE_READERS = N)`).
- **`WAITFOR (RECEIVE …)`** — true server-side blocking dequeue with no busy-poll cost.
- **Poison message handling** — queue is auto-disabled after 5 consecutive transaction rollbacks (toggleable via `POISON_MESSAGE_HANDLING`).

### 2.2 Costs and pain points

- **Operational complexity.** Queue, service, contract, message type, route, remote service binding, conversation endpoints — every Service Broker app starts with ~5 DDL objects per logical queue, plus certificates/keys for cross-instance. The teaching curve is steep. Microsoft's own docs link out to *"the [previously published documentation](https://learn.microsoft.com/en-us/previous-versions/sql/sql-server-2008-r2/bb522893(v=sql.105)) for Service Broker concepts"* — i.e., MS hasn't done a refresh of the conceptual material since 2008-R2.
- **Conversation lifecycle bugs.** Forgetting to `END CONVERSATION` leaks rows in `sys.conversation_endpoints`. The MS Learn sample explicitly drains and ends conversations in a loop — that boilerplate is the reality.
- **Poison message disabling the entire queue.** A single bad message can quiesce the queue. You must monitor `is_receive_enabled = 0`.
- **Investment trajectory.** From the SQL Server 2022 Service Broker page: *"No significant changes were introduced in SQL Server 2019 (15.x). The following changes were introduced in SQL Server 2012 (11.x)."* The feature is in maintenance mode.
- **Cloud reach is partial.** Supported on SQL Server (Windows + Linux Standard/Enterprise) and on Azure SQL Managed Instance (with restrictions: cross-instance is "public preview", `CREATE REMOTE SERVICE BINDING` not supported, port must be 4022). **Not supported on Azure SQL Database.** This kills it for any team that wants the same code to run on SQL Server boxes and Azure SQL DB.
- **Linux: yes.** Per [SQL Server 2022 on Linux editions](https://learn.microsoft.com/en-us/sql/linux/sql-server-linux-editions-and-components-2022?view=sql-server-ver16), Service Broker is supported on Enterprise and Standard, not on Web/Express ("Client only").

### 2.3 One-paragraph verdict

**Ignore for new work.** Service Broker solves a richer problem than `pgmq` does (ordered conversations between distributed services, with activation), at the cost of a 2008-vintage object model, no Azure SQL Database support, and a conceptual surface area larger than the queue we are building. For a pgmq port the only thing we genuinely want from Service Broker is `WAITFOR (RECEIVE …)` for long-polling, and we cannot get that without dragging in queues/services/contracts. Roll our own table-as-queue per §1, and make peace with client-side polling for the `read_with_poll` semantics (§9). Revisit Service Broker only if requirements grow toward "ordered conversations with activation" — at which point it does become the right tool.

### 2.4 2026-05-03 reaffirmation: explicit edition matrix + corrected framing

**Edition availability** (from [SQL Server 2022 editions](https://learn.microsoft.com/en-us/sql/sql-server/editions-and-components-of-sql-server-2022?view=sql-server-ver16), verified 2026-05-03):

| Edition / SKU | Service Broker |
|---|---|
| SQL Server 2022 Enterprise | ✅ Yes |
| SQL Server 2022 Standard | ✅ Yes |
| SQL Server 2022 Web | ❌ No |
| SQL Server 2022 Express w/ Advanced Services | ❌ No |
| SQL Server 2022 Express | ❌ No |
| Azure SQL Database (single DB) | ❌ No |
| Azure SQL Managed Instance | ✅ Yes (with restrictions noted in §2.2) |

A hard Service Broker dependency excludes: all Express tier (the free SKU, common in dev / edge), Web edition, and Azure SQL DB single-DB. That conflicts directly with the project's first principle (*"Portable and lightweight is the whole point"*) and is sufficient justification on its own.

**Corrected framing.** §2.1–§2.2 say things like *"MS hasn't done a refresh of the conceptual material since 2008-R2"* and *"the feature is in maintenance mode."* Both are directionally accurate but easy to mis-read as "deprecated." Service Broker IS still maintained, ships in SQL Server 2022, and works as documented — it's **stable**, not abandoned. The exclusion was based on portability + workload fit, not deprecation.

**Post-bake-off retrospective.** The V012/V013 in-memory misadventure (see `docs/superpowers/archive/`) indirectly validated this call: reaching for SQL Server's "advanced" features (memory-opt + native compilation) without matching pgmq's read-with-VT workload pattern made things measurably worse, not better. Service Broker would have taught the same lesson at much higher integration cost — its dialog/conversation model is a poorer fit for pgmq's send/read/delete shape than memory-optimized tables were for parallel competing-consumer reads, and we can't even measure the trade-off without committing to ~5 DDL objects per queue plus certificates for cross-instance.

**Industry precedent confirms the call.** None of the major SQL-Server-backed queue libraries (Hangfire, NServiceBus, Wolverine, Quartz.NET — see `research/sqlserver-longpoll.md` for source citations) use Service Broker for the queue-storage pattern. They all use client-side polling against table-as-queue, the same approach sqlmq landed on independently.

**When this verdict should be revisited:** if a future requirement adds *ordered conversations between distributed services with activation* (which pgmq doesn't have either), Service Broker becomes the right tool and this exclusion no longer holds. For the current pgmq-port scope it stays excluded.

---

## 3. Memory-optimized tables (In-Memory OLTP / "Hekaton") for queues

### 3.1 Durability options on SQL Server 2022

From [Introduction to Memory-Optimized Tables](https://learn.microsoft.com/en-us/sql/relational-databases/in-memory-oltp/introduction-to-memory-optimized-tables?view=sql-server-ver16):

> "Memory-optimized tables are fully durable by default … The primary storage for memory-optimized tables is the main memory. Rows in the table are read from and written to memory. A second copy of the table data is maintained on disk, but only for durability purposes."

Two durability modes:

- **`DURABILITY = SCHEMA_AND_DATA`** (default) — fully durable; survives restart. Requires a `PRIMARY KEY`. Writes go to the transaction log.
- **`DURABILITY = SCHEMA_ONLY`** — non-durable; *"data is lost if there is a server crash or failover."* Zero log I/O. Only the schema is restored on restart.

Plus a third mode of operation: **delayed durability transactions** — *"committed transactions that aren't persisted to disk are lost in a server crash or fail over."* Configured per-database (`ALTER DATABASE … SET DELAYED_DURABILITY = ALLOWED|FORCED`).

Verdict for a queue:
- A pgmq message queue is **durable by contract** — `SCHEMA_ONLY` is wrong unless the queue is explicitly an ephemeral/cache queue. Use `SCHEMA_AND_DATA`.
- `DELAYED_DURABILITY` is a reasonable performance toggle if the application is OK with losing ~milliseconds of committed enqueues on crash.

### 3.2 Indexes — hash vs nonclustered range

From [Indexes for Memory-Optimized Tables](https://learn.microsoft.com/en-us/sql/relational-databases/in-memory-oltp/indexes-for-memory-optimized-tables?view=sql-server-ver16):

> "All memory-optimized tables must have at least one index, because it is the indexes that connect the rows together. On a memory-optimized table, every index is also memory-optimized."

Two index types:

| | Hash | Memory-optimized nonclustered (B-tree) |
|---|---|---|
| Equality lookup | Yes — fastest | Yes |
| Range scan / `>=`, `<` | **No** (becomes scan) | Yes |
| Ordered retrieval | No | Yes (forward order; reverse `ORDER BY DESC` causes scan) |
| Sized via | `BUCKET_COUNT` (must size to ~2× distinct values) | Auto |

For a queue keyed on a monotonic `id` and dequeued in order, **nonclustered range** is the right choice. Hash is for `WHERE message_id = @x`-style point lookups on the message table. Multiple indexes per table are fine: SQL Server 2017+ removed the 8-index cap. Quote: *"Starting with SQL Server 2017 (14.x) and in Azure SQL Database, there is no longer a limit on the number of indexes specific to memory-optimized tables and table types."*

There is no clustered index. There is no LOB indexing. There are no included columns. There is no traditional fragmentation — *"They do not accrue the traditional type of fragmentation within a page, so they have no fillfactor."*

### 3.3 No `READPAST` — optimistic MVCC

From [Transactions with Memory-Optimized Tables](https://learn.microsoft.com/en-us/sql/relational-databases/in-memory-oltp/transactions-with-memory-optimized-tables?view=sql-server-ver16):

> "Memory-optimized tables use the optimistic approach … Pessimistic approach uses locks to block potential conflicts before they occur. Locks are taken when the statement is executed, and released when the transaction is committed. Optimistic approach detects conflicts as they occur, and performs validation checks at commit time. Error 1205, a deadlock, can't occur for a memory-optimized table."

There are no shared / update / exclusive locks. There are also no latches. **`READPAST` is meaningless** because there's nothing to "read past." The competing-consumers race is resolved by *write-write conflict detection at the row-version level*:

> Error **41302**: "Attempted to update a row that was updated in a different transaction since the start of the present transaction. … This error condition occurs if two concurrent transactions attempt to update or delete the same row at the same time. One of the two transactions receives this error message and needs to be retried."

**Required isolation level table** (from the same page):

| Level | Memory-optimized tables |
|---|---|
| `READ UNCOMMITTED` | Not available |
| `READ COMMITTED` | Only in autocommit |
| `SNAPSHOT` | Supported — *"Internally `SNAPSHOT` is the least demanding transaction isolation level for memory-optimized tables."* |
| `REPEATABLE READ` | Supported (validated at commit) |
| `SERIALIZABLE` | Supported (validated at commit) |

For explicit transactions you must either use the `WITH (SNAPSHOT)` table hint or set `ALTER DATABASE CURRENT SET MEMORY_OPTIMIZED_ELEVATE_TO_SNAPSHOT = ON`. Otherwise error 41368 fires.

### 3.4 How a memory-optimized queue handles visibility-timeout dequeue

The pattern shifts from "lock-skip" to "optimistic claim with retry":

```sql
-- Memory-optimized queue table
CREATE TABLE dbo.q_mem
(
    id            bigint IDENTITY(1,1) NOT NULL,
    enqueued_at   datetime2(3) NOT NULL DEFAULT SYSUTCDATETIME(),
    vt            datetime2(3) NOT NULL,
    read_ct       int          NOT NULL DEFAULT 0,
    message       nvarchar(max) NOT NULL,
    CONSTRAINT pk_q_mem PRIMARY KEY NONCLUSTERED (id),
    INDEX ix_vt    NONCLUSTERED (vt, id)   -- range index for vt <= now scan
)
WITH (MEMORY_OPTIMIZED = ON, DURABILITY = SCHEMA_AND_DATA);
GO

-- Interpreted T-SQL claim. No READPAST, no UPDLOCK, no ROWLOCK.
-- Two competing claims of the same row produce error 41302; retry on caller.
SET TRANSACTION ISOLATION LEVEL SNAPSHOT;
BEGIN TRANSACTION;

DECLARE @now datetime2(3) = SYSUTCDATETIME();

;WITH claim AS (
    SELECT TOP (@qty) id, vt, read_ct
    FROM   dbo.q_mem
    WHERE  vt <= @now
    ORDER  BY id
)
UPDATE claim
SET    vt      = DATEADD(second, @vt_secs, @now),
       read_ct = read_ct + 1
OUTPUT inserted.id, inserted.read_ct, inserted.vt;

COMMIT;
```

**Trade-off.** Under low contention, optimistic concurrency is faster than the lock-based pattern (no shared/update locks at all). Under high contention with many consumers fighting for the head of the queue, every collision rolls back one transaction and retries — the throughput curve becomes worse than the pessimistic version. For a queue where consumers want a *batch* (`@qty > 1`), spread the batch reads across the head: each consumer takes `TOP (@qty) … ORDER BY id` but with an `OFFSET` distinct per consumer ID, or just live with retries. The MS Learn retry-loop pattern (error codes 41302, 41305, 41325, 41301, 41823, 41840, 41839, 1205) is mandatory boilerplate — see [the retry T-SQL example](https://learn.microsoft.com/en-us/sql/relational-databases/in-memory-oltp/transactions-with-memory-optimized-tables?view=sql-server-ver16#retry-t-sql-code-example).

### 3.5 Natively compiled stored procedures

From [Native compilation](https://learn.microsoft.com/en-us/sql/relational-databases/in-memory-oltp/native-compilation-of-tables-and-stored-procedures?view=sql-server-ver16): the proc body is converted to a DLL at create time and accesses the in-memory table data structures directly, bypassing the query interpreter. Wrapping shape:

```sql
CREATE PROCEDURE dbo.q_mem_read
    @qty       int,
    @vt_secs   int
WITH NATIVE_COMPILATION, SCHEMABINDING, EXECUTE AS OWNER
AS
BEGIN ATOMIC WITH
    (TRANSACTION ISOLATION LEVEL = SNAPSHOT, LANGUAGE = N'us_english')
    -- body
END;
```

**What you lose inside a natively compiled proc** ([supported features list](https://learn.microsoft.com/en-us/sql/relational-databases/in-memory-oltp/supported-features-for-natively-compiled-t-sql-modules?view=sql-server-ver16)):

- No access to disk-based tables. (You can join two memory-optimized tables; that's it.)
- No cursors.
- No `MERGE` (until SQL Server 2025; SQL Server 2022 supports `MERGE` against memory-optimized tables only from interpreted T-SQL, not from native procs).
- No dynamic SQL (`sp_executesql`).
- No table hints — there are no locks to hint about.
- No explicit `BEGIN/COMMIT TRANSACTION`. The `ATOMIC` block is the transaction.
- `OUTPUT` — supported, but only `OUTPUT … INTO` is restricted to memory-optimized table targets; streaming `OUTPUT` to the client works.
- No `TRY_CAST`/`TRY_CONVERT` in older versions; supported in SQL 2017+.

### 3.6 What you lose vs. on-disk

- No clustered indexes, so the physical row layout is fixed by the engine.
- No partitioning. There is no equivalent of `ALTER TABLE … SWITCH PARTITION` for memory-optimized tables. Archive-by-partition-switch (§6) is unavailable; archives must be on a separate disk-based table.
- Memory budget is bounded by edition. From [SQL Server 2022 on Linux editions](https://learn.microsoft.com/en-us/sql/linux/sql-server-linux-editions-and-components-2022?view=sql-server-ver16): *"Maximum memory-optimized data size per database in SQL Server Database Engine: Enterprise — Unlimited memory; Standard — 32 GB; Web — 16 GB; Express — 352 MB."* For a queue this is a real ceiling — large unprocessed backlogs on Standard will hit the cap.
- Cross-database transactions are forbidden. *"Cross-database transactions aren't supported for memory-optimized tables."* The queue lives in a single database.
- No distributed transactions — `BEGIN DISTRIBUTED TRANSACTION` blocks memory-optimized table access.
- Schema changes require taking the table offline (`ALTER TABLE` on memory-optimized tables works but with restrictions; some changes still require recreate-and-copy).

**Recommendation.** Build the on-disk table-as-queue pattern (§1) as the default. Keep memory-optimized as an opt-in fast path: same logical schema, switchable via configuration, useful for ephemeral / "fast-path" queues where the message volume fits in RAM. The portability matrix is friendly — memory-optimized tables are fully supported on Linux SQL Server 2022 (see §7).

---

## 4. `OUTPUT` clause patterns for atomic claim-and-return

### 4.1 The pattern

From [`OUTPUT` clause docs](https://learn.microsoft.com/en-us/sql/t-sql/queries/output-clause-transact-sql?view=sql-server-ver16):

> "Returns information from, or expressions based on, each row affected by an `INSERT`, `UPDATE`, `DELETE`, or `MERGE` statement."

The four shapes that matter for queue work:

```sql
-- 1. Destructive read with TOP and locking hints
DELETE TOP (1) dbo.q WITH (READPAST)
OUTPUT DELETED.*
WHERE id = (SELECT TOP (1) id FROM dbo.q WITH (READPAST,UPDLOCK,ROWLOCK)
            ORDER BY id);

-- 2. CTE form (Rusanu / Darling) — preferred
;WITH cte AS (
    SELECT TOP (@n) *
    FROM   dbo.q WITH (READPAST,UPDLOCK,ROWLOCK,READCOMMITTEDLOCK)
    ORDER  BY id
)
DELETE FROM cte
OUTPUT deleted.id, deleted.message;

-- 3. Visibility-timeout claim (UPDATE)
;WITH claim AS (
    SELECT TOP (@n) id, vt, read_ct
    FROM   dbo.q WITH (READPAST,UPDLOCK,ROWLOCK,READCOMMITTEDLOCK)
    WHERE  vt <= SYSUTCDATETIME()
    ORDER  BY id
)
UPDATE claim
SET    vt      = DATEADD(second, @vt_secs, SYSUTCDATETIME()),
       read_ct = read_ct + 1
OUTPUT inserted.id, inserted.message, inserted.vt;

-- 4. INSERT...OUTPUT (returning generated id)
INSERT dbo.q (vt, message)
OUTPUT inserted.id, inserted.enqueued_at
VALUES (DEFAULT, @msg);
```

Per the docs, `INSERTED` reflects post-state, `DELETED` reflects pre-state. `$action` is only available with `MERGE`.

### 4.2 `OUTPUT` vs. `OUTPUT INTO`

The two forms differ subtly:

- `OUTPUT <list>` — streams the result set straight to the client. *"Returns rows to the client even if the statement encounters errors and is rolled back."* (MS Learn) — important: don't trust the client-side rows on error.
- `OUTPUT <list> INTO @tablevar` — captures into a table variable / temp table / output table. Lets you do further work in the same proc before returning.

**Restrictions on `OUTPUT INTO` target:**
- No enabled triggers on it.
- No `FOREIGN KEY` constraints (either side).
- No `CHECK` constraints or rules.
- Cannot be a view or rowset function.
- A user-defined function cannot contain `OUTPUT INTO` against a table.

**Trigger restriction with streaming `OUTPUT`:** *"If the `OUTPUT` clause is specified without also specifying the `INTO` keyword, the target of the DML operation can't have any enabled trigger defined on it for the given DML action."* Implication: don't put triggers on the queue table, or pay the price of always using `OUTPUT INTO`.

**Parallelism:** *"An `OUTPUT` clause that returns results to the client, or table variable, always uses a serial plan."* Acceptable for a queue dequeue (the workload is small).

### 4.3 `MERGE` composition

Yes, `OUTPUT` composes with `MERGE`, and `MERGE` is the only place `$action` (`'INSERT' | 'UPDATE' | 'DELETE'`) is meaningful. Example J in MS Learn shows `OUTPUT INTO` *and* streaming `OUTPUT` in one statement. You can also **nest** the whole `MERGE` inside an outer `INSERT … SELECT … FROM (MERGE … OUTPUT …) AS Changes` — example K. For pgmq we do not need `MERGE`; the dequeue is structurally simpler as the CTE+`UPDATE` or CTE+`DELETE` form.

Caution from MS Learn: *"Be aware of `MERGE` bugs"* — see Aaron Bertrand's running list. For a queue, prefer plain `UPDATE`/`DELETE` over `MERGE`.

---

## 5. Identity vs. sequence for monotonic message IDs

### 5.1 IDENTITY behavior

From [IDENTITY (Property)](https://learn.microsoft.com/en-us/sql/t-sql/statements/create-table-transact-sql-identity-property?view=sql-server-ver16):

> "The identity property on a column doesn't guarantee … **Consecutive values after server restart or other failures** — SQL Server might cache identity values for performance reasons and some of the assigned values can be lost during a database failure or server restart. This can result in gaps in the identity value upon insert. If gaps aren't acceptable, then the application should use its own mechanism to generate key values."

> "**Reuse of values** — For a given identity property … the identity values aren't reused by the engine. If a particular insert statement fails, or if the insert statement is rolled back then the consumed identity values are lost and aren't generated again."

Concrete behaviors:

- **Rollback** → gap. Always.
- **Server restart / failover** → gap. By default the identity allocator preallocates 1000 values for `bigint` (10 for `int`) and on dirty shutdown loses what was pre-allocated.
- **Trace flag 272** (`-T272`) — disables identity caching globally; eliminates the post-restart gap at cost of a log write per insert. Not recommended for high-throughput inserts.
- **`ALTER DATABASE … SCOPED CONFIGURATION SET IDENTITY_CACHE = OFF`** — the same effect, scoped to a single database. Preferred over the trace flag.

### 5.2 SEQUENCE behavior

From [CREATE SEQUENCE](https://learn.microsoft.com/en-us/sql/t-sql/statements/create-sequence-transact-sql?view=sql-server-ver16):

> "Sequence numbers are generated outside the scope of the current transaction. They're consumed whether the transaction using the sequence number is committed or rolled back."

> "When created with the `CACHE` option, an unexpected shutdown (such as a power failure) might result in the loss of sequence numbers remaining in the cache."

> "Setting the cache argument to `NO CACHE` writes the current sequence value to the system tables every time that a sequence is used. This might slow performance by increasing disk access, but reduces the chance of unintended gaps."

Mechanically the same gap behavior as identity: rollback → gap, restart with `CACHE` → gap, `NOCACHE` → no gap but slow.

**Differences that matter for a queue:**

| | `IDENTITY` | `SEQUENCE` |
|---|---|---|
| Defined on | Column | Database object |
| Get next value without insert | No | Yes (`NEXT VALUE FOR`) |
| Shared across multiple tables | No | Yes |
| Reseed | `DBCC CHECKIDENT` | `ALTER SEQUENCE … RESTART WITH` |
| Hot-spot under heavy concurrent insert | Same — both end up appending to last clustered index page | Same |

### 5.3 Recommendation for a queue

Use **`bigint IDENTITY(1,1)`** as the clustered key.

Rationale:
- Gaps are fine. pgmq does not require dense IDs; the queue's contract is "monotonic" not "consecutive."
- One column, one table — no orchestration.
- The performance question Brent Ozar [has covered repeatedly](https://www.brentozar.com/archive/2014/08/generating-identities/) is essentially a wash; `IDENTITY` and `SEQUENCE WITH CACHE` are equivalent on inserts. The differentiator is the last-page latch contention from monotonic appends, which both share.
- If last-page contention becomes a real problem on a very hot queue, mitigations are: partitioning by hash of producer (different topic), enabling [Optimized Locking](https://learn.microsoft.com/en-us/sql/relational-databases/performance/optimized-locking?view=sql-server-ver16) which reduces lock memory, or as a last resort moving to a memory-optimized queue (no latches at all).

If you specifically need monotonic IDs without restart-gap, use `IDENTITY` plus `ALTER DATABASE SCOPED CONFIGURATION SET IDENTITY_CACHE = OFF;` per database. Performance hit is real but small for moderate enqueue rates (<10k/s).

---

## 6. Partitioned tables for archive / retention

### 6.1 Use case

`pgmq.archive` moves processed messages to `<queue>_archive`. For retention/backup-friendliness, partition the archive table by enqueue date (daily or monthly) and use **partition switching** to drop or roll old partitions out cheaply.

From [Partitioned Tables and Indexes](https://learn.microsoft.com/en-us/sql/relational-databases/partitions/partitioned-tables-and-indexes?view=sql-server-ver16):

> "You can perform maintenance or data retention operations on one or more partitions more quickly. The operations are more efficient because they target only these data subsets, instead of the whole table. … You might also switch individual partitions out of one table and into an archive table."

### 6.2 Sliding-window setup

```sql
-- 1. Function: monthly partitions
CREATE PARTITION FUNCTION pf_q_archive_month (datetime2(3))
    AS RANGE RIGHT
    FOR VALUES (
        '2026-01-01', '2026-02-01', '2026-03-01',
        '2026-04-01', '2026-05-01', '2026-06-01'
    );

-- 2. Scheme: all partitions on PRIMARY (or distribute across filegroups)
CREATE PARTITION SCHEME ps_q_archive_month
    AS PARTITION pf_q_archive_month
    ALL TO ([PRIMARY]);

-- 3. Partitioned archive table
CREATE TABLE dbo.q_archive
(
    id           bigint        NOT NULL,
    enqueued_at  datetime2(3)  NOT NULL,
    archived_at  datetime2(3)  NOT NULL CONSTRAINT df_q_archive_at DEFAULT (SYSUTCDATETIME()),
    message      nvarchar(max) NOT NULL,
    CONSTRAINT pk_q_archive PRIMARY KEY CLUSTERED (enqueued_at, id)
        ON ps_q_archive_month (enqueued_at)
);
```

The clustered key starts with the partitioning column. Per MS Learn: *"When partitioning a clustered index, the clustering key must contain the partitioning column."*

### 6.3 Switching partitions out

From [`ALTER PARTITION FUNCTION`](https://learn.microsoft.com/en-us/sql/t-sql/statements/alter-partition-function-transact-sql?view=sql-server-ver16):

> "Always keep empty partitions at both ends of the partition range. Keep the partitions at both ends to guarantee that the partition split and the partition merge don't incur any data movement."

The monthly-cycle pattern:

```sql
-- Stage table with same schema and same constraints (and same filegroup)
CREATE TABLE dbo.q_archive_stage_2026_01 (...);  -- non-partitioned, identical schema
ALTER TABLE dbo.q_archive_stage_2026_01 ADD CONSTRAINT chk_range
    CHECK (enqueued_at < '2026-02-01');  -- enforces partition boundary

-- Switch oldest partition out (instant metadata operation)
ALTER TABLE dbo.q_archive
    SWITCH PARTITION 1 TO dbo.q_archive_stage_2026_01;

-- Now drop / back up / move the stage table at leisure
DROP TABLE dbo.q_archive_stage_2026_01;

-- Merge the boundary that's now empty
ALTER PARTITION FUNCTION pf_q_archive_month () MERGE RANGE ('2026-01-01');

-- Add the new month at the high end
ALTER PARTITION SCHEME ps_q_archive_month NEXT USED [PRIMARY];
ALTER PARTITION FUNCTION pf_q_archive_month () SPLIT RANGE ('2026-07-01');
```

### 6.4 Requirements / restrictions

From [Partitioned Tables](https://learn.microsoft.com/en-us/sql/relational-databases/partitions/partitioned-tables-and-indexes?view=sql-server-ver16) and [`ALTER PARTITION FUNCTION`](https://learn.microsoft.com/en-us/sql/t-sql/statements/alter-partition-function-transact-sql?view=sql-server-ver16):

- **Aligned indexes** — every nonclustered index on the partitioned table must use the same partition function (same column type, boundaries, partition count) as the table, or `SWITCH` fails. *"When a table and its nonclustered indexes are in alignment, the database engine can switch partitions in or out of the table quickly and efficiently while maintaining the partition structure of both the table and its indexes."*
- Source and target must be on the same filegroup.
- Source and target schema must be identical (column types, nullability, computed columns, identity, etc.).
- Target must have a `CHECK` constraint that proves its data fits in exactly one partition's range.
- Foreign keys, triggers, indexed views, and `WITH (CHECK_OPTION)` rules add further constraints.
- Up to **15,000 partitions** per table or index (down from no-limit, up from 1000 pre-2012). Practical guidance: stay well under 1000 to avoid memory and DDL slowdowns. *"Avoid designs with the number of partitions in many hundreds or thousands unless strictly necessary."*
- **Edition.** Per the SQL Server 2022 on Linux editions page, table partitioning is supported on **all editions** (Enterprise, Standard, Web, Express). Pre-2016 SP1 was Enterprise-only — that history no longer applies.

For the queue port: partition the *archive* table (cheap purge), but **don't partition the live queue table** — the access pattern is "head of queue" and partitioning by enqueue time gives no win on dequeue. If contention on a single queue table becomes a problem, partition by *consumer group* / topic, not by time.

---

## 7. Linux portability gotchas

Authoritative source: [Editions and Supported Features of SQL Server 2022 — Linux](https://learn.microsoft.com/en-us/sql/linux/sql-server-linux-editions-and-components-2022?view=sql-server-ver16).

### 7.1 Things we use that DO work on Linux

- **In-Memory OLTP / memory-optimized tables.** Listed under "Scalability and performance" with "Yes" across all editions.
- **Service Broker.** Yes for Enterprise/Standard ("Service Broker (messaging and queuing)"). Web/Express are "Client only" — won't help us.
- **Table and index partitioning.** Yes across all editions.
- **Always Encrypted, TDE, RLS** — all Yes.
- **Query Store, Temporal tables, JSON, MERGE/upsert** — all Yes.

### 7.2 Things that DO NOT work on Linux that we should care about

From the "Unsupported features and services" table on the same page:

| Area | Unsupported on Linux | Impact on queue port |
|---|---|---|
| FILESTREAM, FileTable | Not supported | None — we don't store filesystem blobs |
| MSDTC / distributed transactions | Not supported | If the queue spans databases or instances using `BEGIN DISTRIBUTED TRANSACTION`, breaks on Linux. Memory-optimized tables already forbid distributed txns anyway. |
| Merge replication | Not supported | None |
| Stretch DB | Deprecated, not supported | None |
| `xp_cmdshell` and other extended sprocs | Deprecated | None |
| CLR with `EXTERNAL_ACCESS` or `UNSAFE` | Not supported | None — if we ship CLR, must be `SAFE` |
| Database mirroring | Deprecated | None — use AG instead |
| Linked servers (third-party) | Not supported | None |
| SQL Server Agent: `CmdExec`, `PowerShell`, `Queue Reader`, SSIS, SSAS, SSRS subsystems | Not supported | If queue maintenance jobs use SQL Agent, must use only `T-SQL` subsystem on Linux |
| SQL Server Agent: Alerts, Managed Backup | Not supported | Use third-party monitoring |
| TLS 1.3 | Not supported on Linux | Connection strings should target TLS 1.2 |
| Always Encrypted with secure enclaves | Not supported | None for queue scope |
| Windows integrated auth for AG endpoints | Not supported on Linux | Use certificate-based AG endpoint auth |
| SQL Server Browser | Not on Linux (single default instance only) | Connection strings: never assume named instances |

### 7.3 Implications for the port

- **Single-database queue.** Cross-DB transactions are forbidden in memory-optimized scenarios anyway and unreliable in distributed-transaction scenarios on Linux. Keep the queue and its archive in one database.
- **Use cert-based endpoint auth** if running AG on Linux. Boilerplate — one-time DBA task.
- **Don't rely on SQL Agent advanced subsystems.** Maintenance scripts (purge, partition rotation) should be T-SQL only or scheduled by an external scheduler (cron, systemd timer, K8s CronJob).
- **No FILESTREAM means message bodies must fit in normal data types.** `nvarchar(max)` / `varbinary(max)` are fine.
- **Charset / collation: no longer an issue.** Per [Introduction to Memory-Optimized Tables](https://learn.microsoft.com/en-us/sql/relational-databases/in-memory-oltp/introduction-to-memory-optimized-tables?view=sql-server-ver16): *"Starting with SQL Server 2016, and in Azure SQL Database, there are no limitations for collations or code pages that are specific to In-Memory OLTP."* (Pre-2016 had nasty BIN2-only restrictions on memory-optimized index keys.)

### 7.4 Latest Linux releases (sanity check)

Per [SQL Server 2022 on Linux Release Notes](https://learn.microsoft.com/en-us/sql/linux/sql-server-linux-release-notes-2022?view=sql-server-ver16), as of the fetch the latest is **CU 24 GDR (April 2026)**, build 16.0.4250.1, on RHEL 9, SLES 15, and Ubuntu 22.04. The container image story is healthy. SQL Server 2025 is also Linux-supported.

---

## 8. Deadlock and contention diagnosis

### 8.1 Live lock state — `sys.dm_tran_locks`

From [`sys.dm_tran_locks`](https://learn.microsoft.com/en-us/sql/relational-databases/system-dynamic-management-views/sys-dm-tran-locks-transact-sql?view=sql-server-ver16):

> "Returns information about currently active lock manager resources in SQL Server. Each row represents a currently active request to the lock manager for a lock that has been granted or is waiting to be granted."

Useful columns for queue work:
- `resource_type` — `KEY` (row in index) and `RID` (row in heap) are what `READPAST` skips. `PAGE` is what defeats `READPAST`. Watch for unexpected `PAGE` rows.
- `request_mode` — `S`, `U`, `X`, `IX`, etc. The queue dequeue should show `U` locks granted on the rows it's claiming.
- `request_status` — `GRANTED` vs `WAIT` vs `CONVERT`.
- `resource_associated_entity_id` — joins to `sys.partitions.hobt_id` to identify the index.

The MS Learn example (Example A on that page) is the standard "what's blocking what" query:

```sql
-- Lock detail
SELECT resource_type, resource_associated_entity_id,
       request_status, request_mode, request_session_id,
       resource_description
FROM   sys.dm_tran_locks
WHERE  resource_database_id = DB_ID();

-- Blocking
SELECT t1.resource_type,
       t1.resource_database_id,
       t1.resource_associated_entity_id,
       t1.request_mode,
       t1.request_session_id,
       t2.blocking_session_id
FROM   sys.dm_tran_locks AS t1
INNER JOIN sys.dm_os_waiting_tasks AS t2
    ON t1.lock_owner_address = t2.resource_address;
```

If the queue has `READPAST` and you see `WAIT` with `resource_type = PAGE`, that's the page-lock-defeats-`READPAST` failure mode (§1.6, Erik Darling).

If [Optimized Locking](https://learn.microsoft.com/en-us/sql/relational-databases/performance/optimized-locking?view=sql-server-ver16) is enabled (SQL 2022+), expect to see `XACT` and `XACT KEY` resource types representing TID locks — same view, new resource shape.

### 8.2 Deadlock graphs — the `system_health` Extended Events session

From [Use the system_health session](https://learn.microsoft.com/en-us/sql/relational-databases/extended-events/use-the-system-health-session?view=sql-server-ver16):

> "This session collects … any deadlocks that are detected, including the deadlock graph."

The session is on by default (don't touch it). Pull recent deadlock XMLs:

```sql
WITH src AS (
    SELECT CAST(xet.target_data AS xml) AS x
    FROM   sys.dm_xe_session_targets xet
    JOIN   sys.dm_xe_sessions xe
      ON   xe.address = xet.event_session_address
    WHERE  xe.name = 'system_health'
      AND  xet.target_name = 'ring_buffer'
)
SELECT  d.value('(@timestamp)[1]', 'datetime2')  AS event_time,
        d.query('.')                              AS deadlock_xml
FROM    src
CROSS APPLY x.nodes('/RingBufferTarget/event[@name="xml_deadlock_report"]') AS t(d)
ORDER BY event_time DESC;
```

For long-term retention, set up your own Extended Events session writing to the file target — the default `system_health` ring buffer is small (5 MB / 4 files standard, 100 MB / 10 files Enterprise) and will roll over.

### 8.3 What to look at first when the queue starts deadlocking

1. Confirm `READ_COMMITTED_SNAPSHOT` isn't unintentionally on (`SELECT is_read_committed_snapshot_on FROM sys.databases WHERE name = DB_NAME()`).
2. Confirm the queue table's clustered/range index leading column matches `ORDER BY` of the dequeue.
3. Confirm `READPAST + UPDLOCK + ROWLOCK` are all present.
4. Look at the deadlock graph — both victims should be queue-consumer sessions claiming the same range. If the producer is involved, the issue is index ordering, not lock hints.
5. Check `sys.dm_db_index_operational_stats` for `page_lock_count` vs `row_lock_count` on the queue index — high page lock ratio means you have the page-lock failure mode.
6. Check for [lock escalation events](https://learn.microsoft.com/en-us/sql/relational-databases/extended-events/extended-events?view=sql-server-ver16) (`lock_escalation` XEvent) — single statement >5000 locks → table-level X.

---

## 9. Long-polling equivalent (`pgmq.read_with_poll`)

### 9.1 The four candidates

| Approach | Description | Verdict |
|---|---|---|
| Client-side polling | Caller loops with a sleep | Simplest, portable, scales to any number of consumers |
| `WAITFOR DELAY` server-side | Proc loops `WAITFOR DELAY '00:00:00.250'` between checks | Cheap individually, expensive in aggregate (worker thread per session) |
| `WAITFOR (RECEIVE …)` Service Broker | True server-side blocking dequeue | Requires Service Broker (§2 — ignore) |
| Query Notifications | `SqlDependency` / `SSPROP_QP_NOTIFICATION_*` | Built on Service Broker; tied to deprecated SQL Server Native Client; do not use |

### 9.2 `WAITFOR` cost

From [`WAITFOR`](https://learn.microsoft.com/en-us/sql/t-sql/language-elements/waitfor-transact-sql?view=sql-server-ver16):

> "While the `WAITFOR` statement executes, the transaction is running and no other requests can run under the same transaction."

> "Each `WAITFOR` statement has a thread associated with it. If many `WAITFOR` statements are specified on the same server, many threads can be tied up waiting for these statements to run. **SQL Server monitors the number of `WAITFOR` statement threads, and randomly selects some of these threads to exit if the server starts to experience thread starvation.**"

That last line is the deal-breaker for `WAITFOR DELAY` server-side polling at scale: **the engine will kill long-`WAITFOR` sessions when worker threads run low**. With (say) 200 consumers each in `WAITFOR DELAY` for 1s, you've consumed 200 worker threads doing nothing. SQL Server's max worker threads on a typical machine is ~512–1024.

### 9.3 Query Notifications — deprecated path

From [Working with Query Notifications](https://learn.microsoft.com/en-us/sql/relational-databases/native-client/features/working-with-query-notifications?view=sql-server-ver15):

> "[SQL Server Native Client](https://learn.microsoft.com/en-us/sql/relational-databases/native-client/sql-server-native-client) (SNAC) isn't shipped with: SQL Server 2022 (16.x) and later versions; SQL Server Management Studio 19 and later versions. The SQL Server Native Client (SQLNCLI or SQLNCLI11) and the legacy Microsoft OLE DB Provider for SQL Server (SQLOLEDB) aren't recommended for new application development."

Query Notifications still technically works through `SqlDependency` in .NET; it's built on Service Broker. The mechanism:
- A queue and service must exist (Service Broker plumbing).
- Each subscription fires once and then must be re-subscribed.
- Notifications come through a Service Broker queue you `RECEIVE` from.

Verdict: do not use. The plumbing cost is similar to using Service Broker directly, and the developer ergonomics are worse.

### 9.4 Recommendation

**Client-side polling.** Caller calls `q_read`; if the result set is empty and `vt_secs > 0`, it sleeps for `min(poll_interval, max_poll)` and retries. Implementation lives in the client SDK, not the database. This matches what pgmq does in its Postgres implementation and what every modern queue client (SQS, RabbitMQ pull mode) does.

If absolute lowest latency matters and the consumer count is small (<30), a server-side `WAITFOR DELAY` loop with a short check interval (250 ms) is acceptable:

```sql
CREATE OR ALTER PROCEDURE dbo.q_read_with_poll
    @qty            int,
    @vt_secs        int,
    @poll_max_secs  int,
    @poll_interval_ms int = 250
AS
BEGIN
    SET NOCOUNT, XACT_ABORT ON;
    DECLARE @deadline datetime2(3) = DATEADD(second, @poll_max_secs, SYSUTCDATETIME());
    DECLARE @delay char(12) =
        '00:00:00.' + RIGHT('000' + CAST(@poll_interval_ms AS varchar(3)), 3);

    WHILE 1 = 1
    BEGIN
        ;WITH claim AS (
            SELECT TOP (@qty) *
            FROM   dbo.q WITH (READPAST, UPDLOCK, ROWLOCK, READCOMMITTEDLOCK)
            WHERE  vt <= SYSUTCDATETIME()
            ORDER  BY id
        )
        UPDATE claim
        SET    vt      = DATEADD(second, @vt_secs, SYSUTCDATETIME()),
               read_ct = read_ct + 1
        OUTPUT inserted.id, inserted.message, inserted.vt;

        IF @@ROWCOUNT > 0 RETURN;
        IF SYSUTCDATETIME() >= @deadline RETURN;

        WAITFOR DELAY @delay;
    END
END;
```

But ship it as opt-in; default to client-side polling. Document the worker-thread risk.

---

## 10. Idempotent migration scripts

### 10.1 What the SQL Server world actually does

There's no Microsoft-blessed migration tool — SQL Server projects (SSDT `.sqlproj`) are model-based and produce a generated diff at deploy time, which doesn't fit a "ship versioned `.sql` files in the package" workflow. The community converges on three patterns:

1. **Hand-rolled idempotent scripts** with `IF NOT EXISTS` guards. Most common in OSS.
2. **Flyway** (open-source schema migration) with SQL Server support. Versioned (`V1__create_q.sql`) and repeatable (`R__procs.sql`) migrations; tracks state in a `flyway_schema_history` table.
3. **DbUp** (.NET) — same model as Flyway, simpler.
4. **Roundhouse** (older, .NET) — versioned migrations.
5. **EF Core Migrations** — only relevant if you're already using EF.

For shipping a database extension like pgmq-on-SQL-Server, the right answer is **option 1**: hand-rolled idempotent scripts that any tool (Flyway, DbUp, raw `sqlcmd`) can run. The scripts have to be re-runnable; the runner is the consumer's choice.

### 10.2 Idempotent T-SQL idioms

The community-standard guards:

```sql
-- Database
IF DB_ID(N'sqlmq') IS NULL
    CREATE DATABASE sqlmq;
GO

USE sqlmq;
GO

-- Schema
IF SCHEMA_ID(N'sqlmq') IS NULL
    EXEC ('CREATE SCHEMA sqlmq AUTHORIZATION dbo;');  -- CREATE SCHEMA must be first in batch
GO

-- Table
IF OBJECT_ID(N'sqlmq.q_meta', N'U') IS NULL
BEGIN
    CREATE TABLE sqlmq.q_meta (
        queue_name sysname        NOT NULL PRIMARY KEY,
        created_at datetime2(3)   NOT NULL CONSTRAINT df_q_meta_at DEFAULT (SYSUTCDATETIME()),
        is_partitioned bit         NOT NULL DEFAULT 0
    );
END
GO

-- Add column if missing
IF NOT EXISTS (
    SELECT 1 FROM sys.columns
    WHERE object_id = OBJECT_ID(N'sqlmq.q_meta')
      AND name = N'archive_table'
)
    ALTER TABLE sqlmq.q_meta ADD archive_table sysname NULL;
GO

-- Index
IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE object_id = OBJECT_ID(N'sqlmq.q_template')
      AND name = N'ix_q_vt'
)
    CREATE NONCLUSTERED INDEX ix_q_vt
        ON sqlmq.q_template (vt, id) INCLUDE (read_ct);
GO

-- Procedure (idiomatic: CREATE OR ALTER, available since SQL Server 2016 SP1)
CREATE OR ALTER PROCEDURE sqlmq.q_read
    @queue_name sysname,
    @qty        int,
    @vt_secs    int
AS
BEGIN
    -- ...
END;
GO

-- Type (cannot be ALTERed; drop+recreate when changed)
IF TYPE_ID(N'sqlmq.queue_message') IS NOT NULL
    DROP TYPE sqlmq.queue_message;
CREATE TYPE sqlmq.queue_message AS TABLE (
    message nvarchar(max) NOT NULL
);
GO
```

### 10.3 Versioning convention

If we ship a `migrations/` directory:

```
migrations/
  V0001__initial_schema.sql        -- creates tables, indexes, base procs
  V0002__add_archive_table.sql     -- ALTER TABLE
  V0003__add_partition_function.sql
  R__procs.sql                     -- all stored procedures, CREATE OR ALTER
  R__views.sql                     -- views
```

The Flyway-style split:
- `V*` (versioned) — DDL that changes data shape; runs once, tracked.
- `R*` (repeatable) — proc/view bodies; reapplied whenever their hash changes.

This works with Flyway out of the box and with DbUp / Roundhouse / a hand-rolled bash loop equally well. The runner doesn't really matter; the structure does.

### 10.4 What to avoid

- `IF EXISTS (…) DROP TABLE; CREATE TABLE …` — destroys data on re-run.
- `GO` inside a string passed to `EXEC` — `GO` is a `sqlcmd`/SSMS batch separator, not T-SQL.
- Cross-batch variables — every `GO` resets. Pass state through temp tables or scope-bind into procs.
- DDL inside a `BEGIN TRY … BEGIN CATCH` without `XACT_ABORT ON` — partial DDL can leave the catalog in a half-state.

---

## Sources

Microsoft Learn (primary):

- [Table Hints (Transact-SQL)](https://learn.microsoft.com/en-us/sql/t-sql/queries/hints-transact-sql-table?view=sql-server-ver16)
- [OUTPUT clause (Transact-SQL)](https://learn.microsoft.com/en-us/sql/t-sql/queries/output-clause-transact-sql?view=sql-server-ver16)
- [Introduction to Memory-Optimized Tables](https://learn.microsoft.com/en-us/sql/relational-databases/in-memory-oltp/introduction-to-memory-optimized-tables?view=sql-server-ver16)
- [Transactions with Memory-Optimized Tables](https://learn.microsoft.com/en-us/sql/relational-databases/in-memory-oltp/transactions-with-memory-optimized-tables?view=sql-server-ver16)
- [Indexes for Memory-Optimized Tables](https://learn.microsoft.com/en-us/sql/relational-databases/in-memory-oltp/indexes-for-memory-optimized-tables?view=sql-server-ver16)
- [Native compilation of tables & stored procedures](https://learn.microsoft.com/en-us/sql/relational-databases/in-memory-oltp/native-compilation-of-tables-and-stored-procedures?view=sql-server-ver16)
- [CREATE SEQUENCE (Transact-SQL)](https://learn.microsoft.com/en-us/sql/t-sql/statements/create-sequence-transact-sql?view=sql-server-ver16)
- [IDENTITY (Property) (Transact-SQL)](https://learn.microsoft.com/en-us/sql/t-sql/statements/create-table-transact-sql-identity-property?view=sql-server-ver16)
- [Partitioned Tables and Indexes](https://learn.microsoft.com/en-us/sql/relational-databases/partitions/partitioned-tables-and-indexes?view=sql-server-ver16)
- [ALTER PARTITION FUNCTION (Transact-SQL)](https://learn.microsoft.com/en-us/sql/t-sql/statements/alter-partition-function-transact-sql?view=sql-server-ver16)
- [Editions and Supported Features of SQL Server 2022 — Linux](https://learn.microsoft.com/en-us/sql/linux/sql-server-linux-editions-and-components-2022?view=sql-server-ver16)
- [Release Notes for SQL Server 2022 on Linux](https://learn.microsoft.com/en-us/sql/linux/sql-server-linux-release-notes-2022?view=sql-server-ver16)
- [SQL Server Service Broker](https://learn.microsoft.com/en-us/sql/database-engine/configure-windows/sql-server-service-broker?view=sql-server-ver16)
- [WAITFOR (Transact-SQL)](https://learn.microsoft.com/en-us/sql/t-sql/language-elements/waitfor-transact-sql?view=sql-server-ver16)
- [sys.dm_tran_locks (Transact-SQL)](https://learn.microsoft.com/en-us/sql/relational-databases/system-dynamic-management-views/sys-dm-tran-locks-transact-sql?view=sql-server-ver16)
- [Use the system_health session](https://learn.microsoft.com/en-us/sql/relational-databases/extended-events/use-the-system-health-session?view=sql-server-ver16)
- [Working with Query Notifications](https://learn.microsoft.com/en-us/sql/relational-databases/native-client/features/working-with-query-notifications?view=sql-server-ver15)
- [Transaction locking and row versioning guide — Lock escalation](https://learn.microsoft.com/en-us/sql/relational-databases/sql-server-transaction-locking-and-row-versioning-guide?view=sql-server-ver16)
- [Optimized locking](https://learn.microsoft.com/en-us/sql/relational-databases/performance/optimized-locking?view=sql-server-ver16)

Community / second sources:

- Remus Rusanu, [*Using tables as queues*](https://rusanu.com/2010/03/26/using-tables-as-queues/) — canonical destructive-read pattern, FIFO via clustered index on identity, lax-vs-strict ordering trade-off.
- Erik Darling, [*When `READPAST` Doesn't Read Past*](https://erikdarling.com/when-readpast-doesnt-read-past/) — page-lock failure mode.
- Erik Darling, [*Building Reusable Queues, Part 2*](https://erikdarling.com/building-reusable-queues-part-2/) — full T-SQL queue procedure with all three hints.
- Brent Ozar, [*Generating Identities*](https://www.brentozar.com/archive/2014/08/generating-identities/) — IDENTITY vs SEQUENCE trade-offs.
