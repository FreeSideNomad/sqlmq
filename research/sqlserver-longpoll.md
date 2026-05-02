# Long-poll / blocking-dequeue patterns on SQL Server 2022 — survey for `read_with_poll`

**Question.** pgmq's `read_with_poll(qname, vt, qty, max_poll_seconds, poll_interval_ms)`
blocks for up to `max_poll_seconds` waiting for a row to become visible on an empty queue
(it spins `pg_sleep(poll_interval_ms)` inside a transaction). What's the SQL Server-2022
equivalent that satisfies the project constraints: pure T-SQL, Linux + Windows, no CLR,
no Service Broker dependency, no agents, **and** must be implementable on both on-disk
tables and memory-optimized tables with natively compiled procs?

This doc surveys the candidate patterns, costs each one, and ends with a recommendation
matrix.

---

## 0. The decisive constraint, first

Natively compiled stored procedures **cannot** call `WAITFOR DELAY`. The
[Features for natively compiled T-SQL modules](https://learn.microsoft.com/en-us/sql/relational-databases/in-memory-oltp/supported-features-for-natively-compiled-t-sql-modules?view=sql-server-ver17)
page lists the full supported control-of-flow surface — `IF…ELSE`, `WHILE`, `RETURN`,
`DECLARE`, `SET`, `TRY…CATCH`, `THROW`, `BEGIN ATOMIC`. `WAITFOR` is **not** on that
list. The companion page
[T-SQL not supported by in-memory OLTP](https://learn.microsoft.com/en-us/sql/relational-databases/in-memory-oltp/transact-sql-constructs-not-supported-by-in-memory-oltp?view=sql-server-ver17)
explicitly states that unsupported statements raise error 10794.

> "*Statement.* This statement is not supported. For more information about supported
> functions in natively compiled T-SQL modules, see Supported Features for Natively
> Compiled T-SQL Modules."

That means **any** server-side wait scheme based on `WAITFOR DELAY` or
`WAITFOR (RECEIVE …)` is impossible to express inside a natively compiled procedure.
The on-disk variant could use it; the in-memory variant cannot. That asymmetry
violates the project rule "the on-disk and in-memory variants should expose the same
logical API" — so any pattern that requires a server-side `WAITFOR` for the in-memory
queue is disqualified up front, no matter how clever.

This pre-empts roughly half the question. We still survey each pattern below because
the verdict for the on-disk variant matters and because some patterns (e.g. `sp_getapplock`
in WAIT mode) are also blocked from natively compiled procs for unrelated reasons.

---

## 1. `WAITFOR DELAY` — server-side polling loop in a stored proc

### Mechanics

Each session running

```sql
WHILE …
BEGIN
    -- try to dequeue
    IF @@ROWCOUNT > 0 RETURN;
    WAITFOR DELAY '00:00:00.200';
END
```

holds an active SQL Server **request** for the entire wait. Per
[WAITFOR (Transact-SQL) — Microsoft Learn](https://learn.microsoft.com/en-us/sql/t-sql/language-elements/waitfor-transact-sql?view=sql-server-ver17):

> "While the `WAITFOR` statement executes, the transaction is running and no other
> requests can run under the same transaction."

> "Each `WAITFOR` statement has a thread associated with it. If many `WAITFOR`
> statements are specified on the same server, many threads can be tied up waiting
> for these statements to run. **SQL Server monitors the number of `WAITFOR`
> statement threads, and randomly selects some of these threads to exit if the
> server starts to experience thread starvation.**"

That is the verbatim Microsoft statement the question asked for. The behaviour is
not advisory — when the engine decides it is in thread starvation, your `WAITFOR`
returns early and the transaction continues. From the consumer's perspective the
poll silently shortens. Worse, *which* sessions get clipped is unspecified
("randomly selects").

### Worker-thread cost

Each blocked `WAITFOR DELAY` session occupies one worker thread. The default
`max worker threads` from
[Server Configuration: max worker threads](https://learn.microsoft.com/en-us/sql/database-engine/configure-windows/configure-the-max-worker-threads-server-configuration-option?view=sql-server-ver17)
is computed as `Default Max Workers + ((logical CPUs − 4) × Workers per CPU)`:

| Logical CPUs | Default `max worker threads` (64-bit, 2017+) |
| --- | --- |
| ≤ 4 | 512 |
| 8 | 576 |
| 16 | 704 |
| 32 | 960 |
| 64 | 1472 |
| 128 | 4480 |

These pools are **shared** by every active query on the instance, including
checkpoint, lazy writer, AG threads, and ordinary OLTP traffic. A practical rule:
budget no more than ~20–30 % of the pool for blocked long-pollers.

A typical 8-vCPU SQL Server 2022 box (Standard edition, common cloud SKU): ~576
worker threads total. ~150 concurrent `WAITFOR`-blocked sessions is the upper bound
before the engine starts clipping waits at random. A 4-vCPU box (the smallest
supported SQL 2022 install): 512 total, hard cap around 100 blocked pollers before
random eviction kicks in. The
[Azure SQL support team's *Lesson Learned #339*](https://techcommunity.microsoft.com/blog/azuredbsupport/lesson-learned-339-waitfor-wait-type-delay/3779500)
demonstrates the issue with an oStress run of 198 concurrent `WAITFOR DELAY '00:00:20'`
sessions and observes "the thread is being held by the transaction that cannot be used
for other processes" and "if you have many associated with WAITFOR you could have a
thread exhaustion."

### Composability with the dequeue transaction

`WAITFOR DELAY` inside `BEGIN TRAN … COMMIT` is fine syntactically but holds locks
across the wait — which for a queue is exactly what we don't want. The pgmq pattern
works around this by `pg_sleep`-ing **without** open locks (Postgres MVCC); on SQL
Server we'd need to release locks before the wait, which means losing the row claim
we were going to verify, which means we have nothing to wait *for*. The natural
shape is `WAITFOR DELAY` outside any `BEGIN TRAN`, which is fine — but then we're
just a server-side poll loop with all the costs above and none of the benefit.

### Verdict — `WAITFOR DELAY` server-side poll loop: **don't use**

- Server-side wait can't be implemented inside the natively compiled in-memory
  variant at all (in-memory variant disqualifies the whole pattern).
- For the on-disk variant, the worker-thread cost caps concurrent consumers at the
  low hundreds on typical hardware, with random truncation under pressure — exactly
  the failure mode you don't want for a queue advertised as "concurrency is the
  product."

---

## 2. `WAITFOR (RECEIVE …)` from Service Broker — as a notification primitive only

### The pattern

Even if we keep messages in a regular table, in principle we could:

1. Create one tiny Service Broker queue per pgmq queue, used purely as a doorbell.
2. Producer's `send` proc both inserts into the data table **and** sends a 0-byte
   message to the doorbell queue.
3. Consumer's `read_with_poll` does
   `WAITFOR (RECEIVE TOP(1) message_body FROM dbo.q_doorbell), TIMEOUT @ms;`
   then re-runs the dequeue against the data table.

The `WAITFOR (RECEIVE)` form is documented on the same
[WAITFOR](https://learn.microsoft.com/en-us/sql/t-sql/language-elements/waitfor-transact-sql?view=sql-server-ver17)
page; the `TIMEOUT` keyword exists *only* for the `RECEIVE` and
`GET CONVERSATION GROUP` forms. Mechanically this is the cleanest server-side
event-wait SQL Server has — Remus Rusanu (the original Service Broker dev) calls
out the exact pattern in
[*The Mysterious Notification*](https://rusanu.com/2006/06/17/the-mysterious-notification/)
in the context of `SqlDependency`'s background listener thread. He also warns
about its scaling cost in his discussion of `SqlDependency`:

> "If each `SqlDependency` request would start its own listener, the back end
> server would quickly get swamped by all those requests issuing `WAITFOR(RECEIVE…)`
> statements, each blocking a server thread."

So `WAITFOR (RECEIVE)` has the **same** worker-thread cost as `WAITFOR DELAY` —
one blocked session per consumer. The thread-starvation note from §1 applies
identically.

### Hard blockers for our use

1. **Hard exclusion in this project's CLAUDE.md:** "Service Broker as a dependency"
   is forbidden. Even bringing in Service Broker for a doorbell queue means every
   install needs `ALTER DATABASE … SET ENABLE_BROKER`, which is a non-trivial
   operation (it requires the database to be in single-user mode if conversations
   exist). That violates "Install = run a T-SQL script against a database."
2. **Linux portability:** Service Broker is *technically* listed in the SQL Server
   2022 on Linux feature matrix
   ([Editions and supported features of SQL Server 2022 on Linux](https://learn.microsoft.com/en-us/sql/linux/sql-server-linux-editions-and-components-2022?view=sql-server-ver17)),
   but cross-instance dialogs are known to fail on OpenSSL 3 distributions
   (Ubuntu 20.04+, RHEL 8+) with cryptographic errors. For a single-database
   doorbell queue this likely doesn't matter — there's no remote endpoint —
   but it's a maturity smell on a feature Microsoft has put in maintenance mode
   for nearly a decade.
3. **Natively compiled in-memory variant:** `WAITFOR (RECEIVE)` is a `WAITFOR`
   statement; it is not in the supported control-of-flow list for natively compiled
   procs. Same asymmetry problem as §1.
4. **Per-queue admin overhead:** Each pgmq queue would need its own
   `CREATE QUEUE`, `CREATE SERVICE`, and `CREATE CONTRACT`. That's three extra
   objects per queue beyond the data table and archive table. The CLAUDE.md
   directive "minimum table count per queue" applies in spirit.

### Verdict — `WAITFOR (RECEIVE)`: **don't use**

Hard-blocked by project constraints. Even ignoring those, it inherits §1's worker
thread cost and adds Service Broker's installation footprint and admin surface
for what is essentially `WAITFOR DELAY` with a wakeup signal.

---

## 3. Query Notifications / `SqlDependency`

### Status

Functional but obsolete. `SqlDependency` is shipped in `System.Data.SqlClient`,
which Microsoft has marked as the legacy stack — the
[official guidance](https://learn.microsoft.com/en-us/dotnet/api/system.data.sqlclient.sqldependency?view=net-9.0-pp)
is to move to `Microsoft.Data.SqlClient`. (The new package does still ship
`SqlDependency`.) Underneath, it builds entirely on Service Broker — Rusanu walks
through the wiring in
[*The Mysterious Notification*](https://rusanu.com/2006/06/17/the-mysterious-notification/):
the client opens a Service Broker conversation, registers a notification subscription
on the query, and runs a background listener thread that does
`WAITFOR (RECEIVE …)` on a per-process notification queue.

### Why it doesn't apply here

- It's a **client-side library**, not a T-SQL primitive. There is no T-SQL way
  for a stored procedure to "subscribe and wake up". To use it, the consumer has
  to be a .NET (or Java with a port) process holding an open `SqlDependency`
  registration. That re-introduces a host-language dependency that the project's
  T-SQL-only rule forbids.
- It requires Service Broker enabled — same blocker as §2.
- Microsoft's own guidance (
  [Planning for Notifications](https://learn.microsoft.com/en-us/dotnet/framework/data/adonet/sql/planning-for-notifications))
  explicitly limits it to "ASP.NET or middle-tier services where there is a
  relatively small number of servers having dependencies active against the
  database. It was not designed for use in client applications." Reports of
  `conversation_endpoint` accumulation in long-running services
  ([dotnet/SqlClient issue #148](https://github.com/dotnet/corefx/issues/40136))
  show what happens when you push it past that.

### Verdict — `SqlDependency`: **don't use**

Wrong layer (client-side .NET API, not T-SQL), wrong dependency (Service Broker),
wrong scaling envelope (~10s of subscribers, not a queue with arbitrary consumer
count).

---

## 4. Change Tracking / Change Data Capture as a wakeup channel

### What you'd build

Enable Change Tracking on the queue table. Consumer maintains the last-seen
`SYS_CHANGE_VERSION` and polls
`SELECT CHANGE_TRACKING_CURRENT_VERSION()` until it advances, then re-reads.

### Costs and gotchas

- **Latency floor is still polling.** Change Tracking gives you a cheap "did
  anything change?" check, but you still have to poll it. There is no push wakeup;
  Microsoft's docs are explicit that
  [Change Tracking is a synchronous tracking mechanism](https://learn.microsoft.com/en-us/sql/relational-databases/track-changes/about-change-tracking-sql-server)
  exposed via DMVs that consumers query.
- **CDC adds a SQL Server Agent dependency.** CDC's capture job runs as an Agent
  job by default, and SQL Server Agent's
  [Linux release notes](https://learn.microsoft.com/en-us/sql/linux/sql-server-linux-release-notes-2022?view=sql-server-ver17)
  document that the `CmdExec`, `PowerShell`, `Queue Reader`, `SSIS`, `SSAS`,
  `SSRS` subsystems are unsupported on Linux. The capture job itself works on
  Linux but you've still added "Agent must be running" to the install
  prerequisites — direct violation of the "no agents" project constraint.
- **In-memory tables.** Change Tracking and CDC both target on-disk tables.
  Memory-optimized tables aren't supported by either.
- **Queue churn pathology.** A busy queue is the worst-case workload for
  Change Tracking — every send and every dequeue is a row change, so the
  side-effect tables (`change_tracking_<object_id>`, `syscommittab`) grow at
  the queue's full traffic rate. You've now built two queues for the price
  of one.

### Verdict — Change Tracking / CDC: **don't use**

Wrong tool. Adds Agent dependency (CDC), doesn't work for in-memory tables,
doubles write amplification on the queue itself, and the "wakeup" you get is
still a poll.

---

## 5. `sp_getapplock` in WAIT mode as a notification channel

### The intended pattern

- Producer: takes `sp_getapplock @Resource = 'q.<name>.doorbell', @LockMode = 'Exclusive'`
  (in transaction), enqueues, commits — the lock release is the wakeup.
- Consumer (when initial dequeue returns 0 rows): calls
  `sp_getapplock @Resource = 'q.<name>.doorbell', @LockMode = 'Shared',
  @LockTimeout = @poll_ms`. Returns when a producer's transaction commits or
  the timeout fires. Then re-runs the dequeue.

### Cost per blocked session

This is the same cost model as §1: a session blocked on `sp_getapplock` is a
session waiting on a lock, which is one worker thread held. The lock-wait
manifests as `LCK_M_S` / `LCK_M_X` in `sys.dm_os_wait_stats`. There is **no**
"randomly evict to prevent thread starvation" behaviour here — `WAITFOR` has
that special-case, generic lock waits don't — so once the worker pool is
exhausted, new sessions get `THREADPOOL` waits and the instance becomes
unresponsive to *all* logins.

In other words: `sp_getapplock` waits don't have `WAITFOR`'s self-clipping
safety valve, so the failure mode is harder, not easier. Brent Ozar's
[*Troubleshooting Mysterious Blocking Caused By sp_getapplock*](https://www.brentozar.com/archive/2024/04/troubleshooting-mysterious-blocking-caused-by-sp_getapplock/)
makes the diagnostic side of this clear:

> "It's not like `sp_getapplock` is inherently bad, any more than `BEGIN TRAN`
> is bad. It's just that detecting its long transactions is _way_ harder."

Erik Darling's
[*sp_getapplock Is Pretty Cool*](https://erikdarling.com/sp_getapplock-is-pretty-cool/)
endorses the primitive for *serialising critical sections*, not for blocking
notification — his framing is "I don't want anything else to be able to use
this section of code while I'm using it", which is a different problem.

### Hard blockers

- **Natively compiled procs cannot call `sp_getapplock`.** It's a system stored
  procedure, and the only `EXECUTE` allowed inside a natively compiled module
  is "Supported only to execute natively compiled stored procedures and
  user-defined functions."
  ([Unsupported T-SQL in In-Memory OLTP](https://learn.microsoft.com/en-us/sql/relational-databases/in-memory-oltp/transact-sql-constructs-not-supported-by-in-memory-oltp?view=sql-server-ver17)).
- **Composability with the dequeue transaction.** With `@LockOwner = 'Transaction'`,
  the consumer must already be in a transaction when it waits. That means the
  consumer holds an open transaction (and therefore version chain pressure)
  for the entire poll duration. With `@LockOwner = 'Session'` you avoid that
  but now lock release is decoupled from the producer's commit, which
  reintroduces the lost-wakeup problem this whole pattern was supposed to
  solve.
- **No fan-out.** `sp_getapplock` is a binary lock. With multiple consumers
  blocked in `Shared` mode, a single producer commit releases all of them at
  once — they all wake up, all race to dequeue, and most return empty.
  That's a thundering-herd, and it's exactly the workload that converts a
  borderline thread budget into `THREADPOOL` waits.

### Verdict — `sp_getapplock` WAIT mode: **don't use**

Doesn't work in the in-memory variant, has no `WAITFOR`-style safety valve in
the on-disk variant, and creates a thundering herd on producer commit.

---

## 6. Pure client-side polling with adaptive backoff — what real libraries do

This is the pattern with the most shipped production evidence.

### 6.1 Hangfire.SqlServer

[`SqlServerJobQueue.cs`](https://github.com/HangfireIO/Hangfire/blob/main/src/Hangfire.SqlServer/SqlServerJobQueue.cs)
implements both modes pgmq would care about. The relevant constants at the
top of the file:

```csharp
private static readonly TimeSpan LongPollingThreshold = TimeSpan.FromSeconds(1);
private static readonly int PollingQuantumMs = 1000;
private static readonly int DefaultPollingDelayMs = 200;
private static readonly int MinPollingDelayMs = 100;
internal static readonly ConcurrentDictionary<Tuple<SqlServerStorage, string>, AutoResetEvent>
    NewItemInQueueEvents = new();
```

The dequeue loop in `DequeueUsingSlidingInvisibilityTimeout`:

1. Try `FetchJob` once immediately (no wait).
2. If empty, take a **per-queue semaphore** so only one worker per queue polls
   the database at a time (everyone else waits on the semaphore — in-process,
   no SQL Server thread).
3. Inside the semaphore, loop: `FetchJob`, on miss
   `WaitHandle.WaitAny(waitArray, pollingDelayMs)` where `waitArray` includes
   the `NewItemInQueueEvents` `AutoResetEvent` for the queue plus the
   cancellation event.
4. Backoff window is clamped: `[MinPollingDelayMs=100, PollingQuantumMs=1000]`
   for "long polling" mode (when `QueuePollInterval < 1s`), else the
   user-configured interval.

The `AutoResetEvent` is a same-process optimisation — when an `Enqueue`
happens in the same .NET process, the producer signals the event so the
consumer wakes immediately; cross-process producers fall back to the
polling timer. The dequeue itself is a single non-blocking statement:

```sql
update top (1) JQ
set FetchedAt = GETUTCDATE()
output INSERTED.Id, INSERTED.JobId, INSERTED.Queue, INSERTED.FetchedAt
from [{schema}].JobQueue JQ with (forceseek, readpast, updlock, rowlock)
where Queue in @queues
  and (FetchedAt is null or FetchedAt < DATEADD(second, @timeoutSs, GETUTCDATE()));
```

There is **no** `WAITFOR DELAY` anywhere in the SQL. All waiting is on the
client. Default `QueuePollInterval = TimeSpan.Zero` → long-polling mode →
100–1000 ms client-side backoff with same-process signal optimisation.

### 6.2 NServiceBus.SqlServer transport

[`QueuePeeker.cs`](https://github.com/Particular/NServiceBus.SqlServer/blob/master/src/NServiceBus.Transport.Sql.Shared/Receiving/QueuePeeker.cs):

```csharp
class QueuePeeker(DbConnectionFactory connectionFactory,
                  IExceptionClassifier exceptionClassifier,
                  TimeSpan peekDelay) : IPeekMessagesInQueue
{
    public async Task<int> Peek(TableBasedQueue inputQueue,
                                RepeatedFailuresOverTimeCircuitBreaker circuitBreaker,
                                CancellationToken cancellationToken = default)
    {
        var messageCount = 0;
        try
        {
            using (var scope = new TransactionScope(/*…ReadCommitted, RequiresNew…*/))
            using (var connection = await connectionFactory.OpenNewConnection(cancellationToken))
            {
                messageCount = await inputQueue.TryPeek(connection, null, cancellationToken);
                scope.Complete();
            }
            circuitBreaker.Success();
        }
        catch (Exception ex) when (!exceptionClassifier.IsOperationCancelled(ex, cancellationToken)) {
            Logger.Warn("Sql peek operation failed", ex);
            await circuitBreaker.Failure(ex, cancellationToken);
        }

        if (messageCount == 0)
        {
            await Task.Delay(peekDelay, cancellationToken);
        }
        return messageCount;
    }
}
```

Same shape: fixed `peekDelay` between empty peeks, wait happens on the
client (`Task.Delay`), no `WAITFOR DELAY` in SQL.
[Particular's docs](https://docs.particular.net/transports/sql/design) confirm
the recommended range is 100 ms – 10 s with a default of 1 s.

### 6.3 Wolverine SQL Server transport

[`SqlServerQueueListener.cs`](https://github.com/JasperFx/wolverine/blob/main/src/Persistence/Wolverine.SqlServer/Transport/SqlServerQueueListener.cs):

```csharp
private async Task listenForMessagesAsync()
{
    var failedCount = 0;
    while (!_cancellation.Token.IsCancellationRequested)
    {
        try {
            var messages = _queue.Mode == EndpointMode.Durable
                ? await TryPopDurablyAsync(_queue.MaximumMessagesToReceive, _settings, _logger, _cancellation.Token)
                : await TryPopAsync(_queue.MaximumMessagesToReceive, _logger, _cancellation.Token);

            failedCount = 0;
            if (messages.Any()) {
                await _receiver.ReceivedAsync(this, messages.ToArray());
            }
            else {
                // Slow down if this is a periodically used queue
                await Task.Delay(_pollingInterval);
            }
        }
        catch (Exception e) {
            // …
            failedCount++;
            var pauseTime = failedCount > 5 ? 1.Seconds() : (failedCount * 100).Milliseconds();
            await Task.Delay(pauseTime);
        }
    }
}
```

Identical structure. Dequeue uses `WITH (UPDLOCK, READPAST, ROWLOCK)`.
Backoff on empty is a fixed `_pollingInterval` (`Task.Delay`); on error a
graduated 100 ms × failure-count up to 1 s. No server-side wait.

### 6.4 Quartz.NET ADO job store

[`JobStoreSupport.cs`](https://github.com/quartznet/quartznet/blob/main/src/Quartz/Impl/AdoJobStore/JobStoreSupport.cs):
the scheduler thread calls `SelectTriggerToAcquire` (one SQL statement) in a
loop, with `await Task.Delay(idleWaitTime)` (default 30 s, shorter near
imminent triggers) between attempts. No `WAITFOR` in the SQL.

### Pattern summary across libraries

| Library | Default empty-poll interval | Floor | Ceiling | Wakeup signal? | Server-side wait? |
| --- | --- | --- | --- | --- | --- |
| Hangfire.SqlServer | 200 ms | 100 ms | 1 s | In-process `AutoResetEvent` | No |
| NServiceBus.SqlServer | 1 s | 100 ms | 10 s | None | No |
| Wolverine | configured (default ~1 s) | 100 ms (error) | configured | None | No |
| Quartz.NET | configured | — | — | None | No |

Every one of them does the same thing: short, fixed or lightly-adaptive
client-side backoff, single non-blocking SQL statement per attempt, dequeue
uses `READPAST + UPDLOCK + ROWLOCK`. **None** uses `WAITFOR DELAY` server-side.
None uses Service Broker as a notification channel. Hangfire is the only one
that bothers with a wakeup signal at all, and it's process-local only.

### Verdict — pure client-side polling: **viable**

This is what production looks like. It's symmetric across on-disk and in-memory
(the in-memory variant just calls a natively compiled "try once" proc and the
client `Task.Delay`s between calls). It scales linearly with consumer count
(no shared limit other than connection pool size). Latency floor is the
client poll interval — typically 100–200 ms — which matches the granularity
pgmq's `read_with_poll` ships with (`poll_interval_ms` parameter, often
250 ms in practice).

---

## 7. Hybrid: client polls, server has a `WAITFOR DELAY` cap per call

### The shape

`read_with_poll` proc:

```sql
CREATE PROCEDURE dbo.read_with_poll
    @qname        sysname,
    @vt           int,
    @qty          int,
    @max_poll_ms  int,            -- e.g. 5000
    @interval_ms  int = 250
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @deadline datetime2 = DATEADD(millisecond, @max_poll_ms, SYSUTCDATETIME());
    DECLARE @delay char(12) = CONVERT(char(12), DATEADD(millisecond, @interval_ms, '00:00:00'), 114);

    WHILE SYSUTCDATETIME() < @deadline
    BEGIN
        -- non-blocking dequeue
        EXEC dbo.read @qname, @vt, @qty;
        IF @@ROWCOUNT > 0 RETURN;
        WAITFOR DELAY @delay;
    END;
END
```

This is essentially what pgmq does in Postgres, ported one-to-one. It pushes
the poll loop server-side, so the client makes one call instead of many.

### Cost analysis

The thread cost is **identical to §1**. From the engine's perspective, every
session inside the `WHILE` loop is either running a query (briefly) or
sitting in `WAITFOR DELAY` (the rest of the time). Per the `WAITFOR` doc
quote in §1, that's one worker thread held per blocked session, with random
eviction under starvation. Hosting the loop server-side doesn't change any
of that — it just hides it from the client.

What it *does* change:

- Network round trips drop from N (one per poll attempt) to 1 (one per
  `read_with_poll` call). On WAN-distance consumers this is meaningful; on
  in-datacentre consumers it is noise next to the dequeue latency itself.
- The client looks blocked, so `CancellationToken`-style mid-call abort
  becomes a SQL `KILL` instead of a TCP socket cancel — slightly worse
  ergonomics but not broken (SqlClient supports `Command.Cancel()`).

### The asymmetry kills it

Even if the on-disk variant were happy with §1's worker-thread cost, the
**in-memory** natively compiled `read_with_poll` cannot contain `WAITFOR DELAY`
or call any other proc that does (`EXECUTE` of an interpreted proc is not
allowed inside a natively compiled module). The two variants would have
fundamentally different shapes:

- On-disk: server-side loop with `WAITFOR DELAY`.
- In-memory: client-side loop with `Task.Delay`, calling a natively compiled
  `read` repeatedly.

That violates the project's "same logical API" rule. We could of course do
the client-side shape for both — but then we are back to §6, and the
server-side hybrid contributes nothing.

### Verdict — server-side `WAITFOR DELAY` cap: **risky, and pointless**

Same thread cost as §1. Doesn't work in the in-memory variant. Saves only
network round trips, which are not the bottleneck. Adopting it would mean
two different `read_with_poll` shapes — exactly what the project rules out.

---

## 8. In-memory OLTP constraint — confirmation

The decisive citation is in §0 above. To restate for completeness:

- [Features for natively compiled T-SQL modules](https://learn.microsoft.com/en-us/sql/relational-databases/in-memory-oltp/supported-features-for-natively-compiled-t-sql-modules?view=sql-server-ver17)
  lists the entire supported control-of-flow surface: `IF…ELSE`, `WHILE`,
  `RETURN`, `DECLARE @local_variable`, `SET @local_variable`, `TRY…CATCH`,
  `THROW`, `BEGIN ATOMIC`. **`WAITFOR` is not listed.**
- [T-SQL not supported by in-memory OLTP](https://learn.microsoft.com/en-us/sql/relational-databases/in-memory-oltp/transact-sql-constructs-not-supported-by-in-memory-oltp?view=sql-server-ver17)
  catches anything not listed: any unsupported statement raises error 10794.
- The same page also confirms `EXECUTE` is "Supported only to execute
  natively compiled stored procedures and user-defined functions" — so
  the trick of "have the natively compiled proc call an interpreted proc
  that does the `WAITFOR`" doesn't compile either.

This is decisive. Any `read_with_poll` that needs to be implementable as a
natively compiled stored procedure on a memory-optimized queue must do its
waiting on the client. That eliminates §1, §2, §5, §7 outright for the
in-memory variant.

---

## 9. What other SQL-Server-backed queue projects do — open the source

Three repositories, all non-trivial, all in production use:

### 9.1 Hangfire.SqlServer (HangfireIO/Hangfire)

File: [`src/Hangfire.SqlServer/SqlServerJobQueue.cs`](https://github.com/HangfireIO/Hangfire/blob/main/src/Hangfire.SqlServer/SqlServerJobQueue.cs)

When no messages are available:

- `DequeueUsingSlidingInvisibilityTimeout` (lines around the
  `while (!cancellationToken.IsCancellationRequested)` loop): calls
  `FetchJob`, on null result calls
  `WaitHandle.WaitAny(waitArray, pollingDelayMs)`.
  `pollingDelayMs` is clamped to `[100ms, 1000ms]` in long-polling mode,
  else equals `QueuePollInterval`. `waitArray` is
  `[cancellation_event, NewItemInQueueEvents[queue]]` — wakes immediately
  on same-process enqueue, otherwise polls.
- The fetch SQL is a single statement, no server-side wait.
- A per-queue `SemaphoreSlim` ensures only one worker per queue per process
  is actively polling — others wait on the semaphore in-process.

### 9.2 NServiceBus.SqlServer transport (Particular/NServiceBus.SqlServer)

File: [`src/NServiceBus.Transport.Sql.Shared/Receiving/QueuePeeker.cs`](https://github.com/Particular/NServiceBus.SqlServer/blob/master/src/NServiceBus.Transport.Sql.Shared/Receiving/QueuePeeker.cs)

When peek returns 0:

```csharp
if (messageCount == 0)
{
    if (Logger.IsDebugEnabled)
        Logger.Debug($"Input queue empty. Next peek operation will be delayed for {peekDelay}.");
    await Task.Delay(peekDelay, cancellationToken).ConfigureAwait(false);
}
```

Default `peekDelay = 1s` per
[the design docs](https://docs.particular.net/transports/sql/design),
recommended range 100 ms – 10 s. No wakeup signal, no server-side wait.

### 9.3 Wolverine SQL Server transport (JasperFx/wolverine)

File: [`src/Persistence/Wolverine.SqlServer/Transport/SqlServerQueueListener.cs`](https://github.com/JasperFx/wolverine/blob/main/src/Persistence/Wolverine.SqlServer/Transport/SqlServerQueueListener.cs)

When `TryPopAsync` returns no messages:

```csharp
if (messages.Any()) {
    await _receiver.ReceivedAsync(this, messages.ToArray());
}
else {
    // Slow down if this is a periodically used queue
    await Task.Delay(_pollingInterval);
}
```

`_pollingInterval = queue.PollingInterval ?? _settings.ScheduledJobPollingTime`.
On error: graduated backoff `failedCount * 100ms` capped at 1s. The dequeue
SQL is `DELETE … OUTPUT … FROM <queue> WITH (UPDLOCK, READPAST, ROWLOCK) ORDER BY timestamp`.
No server-side wait.

### Pattern across all three

Same answer: client-side `Task.Delay` (or equivalent), single non-blocking
SQL statement per attempt, lock hints `READPAST + UPDLOCK + ROWLOCK` on
the dequeue. The mature solutions do not use any server-side blocking
primitive. The only enrichment any of them adds is Hangfire's same-process
`AutoResetEvent` wakeup, which is purely a runtime optimisation that doesn't
change the SQL contract.

---

## 10. Recommendation matrix

Summary verdicts using the criteria the project actually cares about.

| Pattern | Latency floor | Server cost per blocked consumer | Symmetric on-disk + in-memory? | Verdict |
| --- | --- | --- | --- | --- |
| 1. `WAITFOR DELAY` server-side loop | poll interval (e.g. 200 ms) | 1 worker thread + open request | **No** — forbidden in natively compiled procs | **Don't use** |
| 2. `WAITFOR (RECEIVE)` Service Broker doorbell | ~immediate on signal | 1 worker thread + open request | **No** — forbidden in natively compiled procs; SB is a hard project exclusion | **Don't use** |
| 3. `SqlDependency` / Query Notifications | ~immediate on signal | 1 worker thread per registered listener (Service Broker under the hood) | **No** — client-side .NET API; SB dependency; ~10s of listeners only | **Don't use** |
| 4. Change Tracking / CDC as wakeup channel | poll interval | low (DMV read) but write amplification on the queue table itself | **No** — neither feature applies to memory-optimized tables; CDC needs Agent | **Don't use** |
| 5. `sp_getapplock` WAIT mode | ~immediate on producer commit | 1 worker thread, no `WAITFOR`-style safety valve, thundering herd | **No** — `EXECUTE sp_getapplock` not allowed in natively compiled procs | **Don't use** |
| 6. **Client-side polling with bounded backoff (Hangfire/NSB/Wolverine pattern)** | configured poll interval (typically 100–500 ms) | **0** — one short non-blocking statement per attempt, no held threads | **Yes** — same SQL contract for both variants; client decides cadence | **Use this** |
| 7. Hybrid: client call wraps server-side `WAITFOR DELAY` cap | configured poll interval | same as §1 | **No** — same compile failure as §1 in natively compiled procs | **Don't use** |

### Concrete recommendation for `read_with_poll`

Implement `read_with_poll` as a thin **client-side** loop on top of the
non-blocking `read` proc:

1. The on-disk and in-memory `read` procs share an identical signature:
   `read(@qname, @vt, @qty) → table of (msg_id, …)`. Both return immediately
   (zero rows or up to `@qty` rows).
2. `read_with_poll` is **not** a stored procedure at all. It is a contract
   that the host language (or a connection-level wrapper) implements as:
   "call `read` immediately; if empty, `Task.Delay(poll_interval_ms)`,
   call again; repeat until first hit or `max_poll_seconds` elapsed."
3. Default `poll_interval_ms = 250`, clamped `[100, 1000]`. Default
   `max_poll_seconds = 5`. These match Hangfire's effective range and
   pgmq's ergonomics.
4. Optional optimisation, port-2 work: maintain an in-process
   `Dictionary<queue_name, AutoResetEvent>` so same-process `send` calls
   short-circuit the poll. Hangfire shows this is worth ~one round trip
   of latency in same-process workloads and zero in cross-process.

This pattern:

- Honours every project constraint (T-SQL only, no Agent, no Service Broker,
  no host runtime *required*, Linux + Windows identical).
- Has identical behaviour across the on-disk and in-memory queue variants
  because `read` is the only stored proc involved and it is naturally
  symmetric.
- Matches what every shipping SQL Server-backed queue library does.
- Has no thread-starvation cliff: 1000 concurrent consumers polling at 250 ms
  generate ~4000 fast queries/sec, which is well within typical SQL Server
  throughput; none of those consumers are holding worker threads between
  attempts.

The thing that pgmq's `pg_sleep` loop does — keep the wait on the database
side — is the **wrong** trade on SQL Server. Postgres MVCC lets a sleeping
backend cost ~nothing; SQL Server's worker pool is a strictly limited
shared resource and `WAITFOR` has documented "we will randomly evict you"
behaviour under pressure. The right port is to flip the wait location:
keep the lightweight, fast `read` proc symmetric across variants and let
the **client** decide cadence.

---

## Sources

Microsoft Learn:

- [WAITFOR (Transact-SQL)](https://learn.microsoft.com/en-us/sql/t-sql/language-elements/waitfor-transact-sql?view=sql-server-ver17)
- [Server Configuration: max worker threads](https://learn.microsoft.com/en-us/sql/database-engine/configure-windows/configure-the-max-worker-threads-server-configuration-option?view=sql-server-ver17)
- [Features for natively compiled T-SQL modules](https://learn.microsoft.com/en-us/sql/relational-databases/in-memory-oltp/supported-features-for-natively-compiled-t-sql-modules?view=sql-server-ver17)
- [T-SQL not supported by in-memory OLTP](https://learn.microsoft.com/en-us/sql/relational-databases/in-memory-oltp/transact-sql-constructs-not-supported-by-in-memory-oltp?view=sql-server-ver17)
- [Editions and supported features of SQL Server 2022 on Linux](https://learn.microsoft.com/en-us/sql/linux/sql-server-linux-editions-and-components-2022?view=sql-server-ver17)
- [Release notes for SQL Server 2022 on Linux](https://learn.microsoft.com/en-us/sql/linux/sql-server-linux-release-notes-2022?view=sql-server-ver17)
- [About change tracking (SQL Server)](https://learn.microsoft.com/en-us/sql/relational-databases/track-changes/about-change-tracking-sql-server)
- [SqlDependency (System.Data.SqlClient)](https://learn.microsoft.com/en-us/dotnet/api/system.data.sqlclient.sqldependency?view=net-9.0-pp)
- [Planning for Notifications](https://learn.microsoft.com/en-us/dotnet/framework/data/adonet/sql/planning-for-notifications)
- [Lesson Learned #339: WAITFOR wait type DELAY](https://techcommunity.microsoft.com/blog/azuredbsupport/lesson-learned-339-waitfor-wait-type-delay/3779500)

MVPs / experts:

- Remus Rusanu — [The Mysterious Notification](https://rusanu.com/2006/06/17/the-mysterious-notification/)
- Erik Darling — [sp_getapplock Is Pretty Cool](https://erikdarling.com/sp_getapplock-is-pretty-cool/)
- Brent Ozar — [Troubleshooting Mysterious Blocking Caused By sp_getapplock](https://www.brentozar.com/archive/2024/04/troubleshooting-mysterious-blocking-caused-by-sp_getapplock/)
- SQLskills — [SQL Server THREADPOOL Wait](https://www.sqlskills.com/help/waits/threadpool/)

GitHub source code:

- Hangfire — [`SqlServerJobQueue.cs`](https://github.com/HangfireIO/Hangfire/blob/main/src/Hangfire.SqlServer/SqlServerJobQueue.cs)
- NServiceBus — [`QueuePeeker.cs`](https://github.com/Particular/NServiceBus.SqlServer/blob/master/src/NServiceBus.Transport.Sql.Shared/Receiving/QueuePeeker.cs)
- Wolverine — [`SqlServerQueueListener.cs`](https://github.com/JasperFx/wolverine/blob/main/src/Persistence/Wolverine.SqlServer/Transport/SqlServerQueueListener.cs)
- Quartz.NET — [`JobStoreSupport.cs`](https://github.com/quartznet/quartznet/blob/main/src/Quartz/Impl/AdoJobStore/JobStoreSupport.cs)
- NServiceBus design docs — [SQL Transport Design](https://docs.particular.net/transports/sql/design)
