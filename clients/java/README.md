# sqlmq (Java client)

Strict pgmq-API-compatible Java client for [sqlmq](https://github.com/freesidenomad/sqlmq) on SQL Server.

The goal: drop-in for code that previously called the
[`tembo-pgmq-python`](https://pypi.org/project/tembo-pgmq-python/) shape (or the Java port of it).
Method names match the [sqlmq-python](../python/) client camel-cased; method semantics match
1:1 — including which methods throw `UnsupportedOperationException`, which kwargs are silently
ignored, and the message + queue-metrics shapes returned.

```java
import io.freesidenomad.sqlmq.client.PgmqClient;

try (var q = new PgmqClient("localhost", 1433, "myapp", "sa", "P@ssw0rd!")) {
    q.createQueue("orders");
    long mid = q.send("orders", Map.of("order_id", 42, "amount", 19.99));
    var msg  = q.read("orders");
    msg.ifPresent(m -> q.delete("orders", m.msgId()));
}
```

## Coordinates

```xml
<dependency>
    <groupId>io.freesidenomad</groupId>
    <artifactId>sqlmq-client</artifactId>
    <version>0.1.0-SNAPSHOT</version>
</dependency>
```

Not yet published to Maven Central — vendor from this monorepo for now.

## Connections and pooling

This library does **not** bundle a connection pool. Three constructors are offered:

```java
// (1) Quick scripts: opens a fresh DriverManager connection per call.
new PgmqClient("localhost", 1433, "myapp", "sa", "P@ssw0rd!");

// (2) Same, with a caller-supplied JDBC URL.
new PgmqClient("jdbc:sqlserver://localhost:1433;databaseName=myapp;encrypt=false",
               "sa", "P@ssw0rd!");

// (3) Production: bring your own pool (HikariCP recommended).
HikariDataSource ds = ...;
new PgmqClient(ds);
```

Use form (3) in production: opening a fresh JDBC connection per call is fine
for one-shot scripts but wasteful under load. The library uses
`CallableStatement.unwrap(SQLServerCallableStatement.class)` for TVP calls,
which is HikariCP-proxy safe.

## Async

`AsyncPgmqClient` mirrors the sync surface; each method returns a
`CompletableFuture<T>`:

```java
var q = new AsyncPgmqClient(dataSource);
q.createQueue("orders")
 .thenCompose(v  -> q.send("orders", Map.of("order_id", 1)))
 .thenCompose(id -> q.read("orders"))
 .thenAccept(msg -> System.out.println(msg));
```

> **This is thread-pool dispatch, NOT true non-blocking I/O.** mssql-jdbc is a
> blocking driver; each future occupies a thread for the duration of its DB
> round trip. R2DBC-mssql is the non-blocking alternative — rejected for v0.1
> to keep the dep tree small and to match the [sqlmq-python](../python/) lib's
> stance (which uses `asyncio.to_thread`).

The default executor is `ForkJoinPool.commonPool()`. For production, supply
a dedicated executor sized to your connection pool:

```java
Executor jdbcExec = Executors.newFixedThreadPool(16);
var q = new AsyncPgmqClient(dataSource, jdbcExec);
```

## pgmq compatibility matrix

| pgmq method | sqlmq behavior | Notes |
|---|---|---|
| `createQueue(queue)` | OK | Maps to `EXEC sqlmq.create_queue` with `@storage='ondisk'`, `@grouped=0`, `@payload_type='json'`. |
| `createQueue(queue, true /* unlogged */)` | **`UnsupportedOperationException`** | sqlmq retired its in-memory variant in V014. |
| `createPartitionedQueue(...)` | **`UnsupportedOperationException`** | Depends on `pg_partman`; no SQL Server equivalent. Tracked for a future release. |
| `dropQueue(queue)` | OK | Returns `true` if dropped, `false` if missing. The `partitioned` arg is accepted but ignored. |
| `listQueues()` | OK | |
| `validateQueueName(name)` | OK (client-side) | Mirrors V003 hardening: `^[A-Za-z_][A-Za-z0-9_]{0,59}$`. Throws `IllegalArgumentException` on mismatch. |
| `send(queue, message)` / `send(queue, message, delaySeconds)` | OK | `Map<String, Object>` payloads serialized via Jackson. |
| `sendBatch(queue, messages)` / `sendBatch(queue, messages, delaySeconds)` | OK | Single round trip via mssql-jdbc's native `setStructured` + `dbo.sqlmq_send_tvp`. |
| `read(queue)` / `read(queue, vtSeconds)` | OK | `vtSeconds` defaults to 30 (matches pgmq). Returns `Optional<Message>`. |
| `readBatch(queue, vtSeconds, batchSize)` | OK | Returns `List<Message>` (empty list when queue is empty). |
| `readWithPoll(queue, vtSeconds, qty, maxPollSeconds, pollIntervalMs)` | OK (client-side polling) | sqlmq has no server-side long-poll (deliberate; see `research/sqlserver-longpoll.md`). Hangfire-style backoff: loop calling `readBatch` until first hit or deadline. |
| `pop(queue)` | OK | Returns `Optional<Message>`. |
| `delete(queue, msgId)` | OK | |
| `deleteBatch(queue, msgIds)` | OK (with extra round trip) | sqlmq's `delete` proc returns a row count, not the surviving id list. To honor pgmq's contract (return the deleted ids in caller order) we `SELECT` the live ids before and after the call and intersect. Same as the Python client. |
| `archive(queue, msgId)` | OK | |
| `archiveBatch(queue, msgIds)` | OK (with extra round trip) | Same shape note as `deleteBatch`. |
| `purge(queue)` | OK | Returns the count purged. |
| `metrics(queue)` | OK (with caveats) | `newestMsgAgeSec` is always `null` (sqlmq does not surface it). `scrapeTime` is filled client-side from `Instant.now()`. |
| `metricsAll()` | OK (with caveats) | Same caveats as `metrics`. |
| `setVt(queue, msgId, vtSeconds)` | **`UnsupportedOperationException`** | Tracked for a future release; would require a `sqlmq.set_vt` stored proc. |
| `detachArchive(queue)` | **`UnsupportedOperationException`** | pgmq-specific feature for partitioned archive tables; sqlmq archives are single-table per queue. |

## sqlmq-extension features (not exposed via this client)

This client is **strict pgmq-compat**: it deliberately does NOT expose
sqlmq's superset features:

* grouped queues (`@grouped=1`) and `read_grouped`
* binary payloads (`@payload_type='binary'`)
* per-queue DLQ caps (`@max_delivery_count`) and `dlq_sweep`
* per-message headers

If you need any of those, drop down to the harness's
`io.freesidenomad.sqlmq.client.SqlmqClient` (test infrastructure, not
published) or call the `sqlmq.*` stored procs directly via your own JDBC.

## Development

```bash
cd clients/java
mvn -B verify   # spins up SQL Server 2022 via Testcontainers, runs all *IT tests
```

Java 25 (LTS-track) is required for the matched bytecode level. The build
plugin sets `<release>25</release>`.

Tests live under `src/test/java/.../client/*IT.java` and use a JUnit 5
extension that:

1. Starts (lazily, JVM-singleton) a `mcr.microsoft.com/mssql/server:2022-latest`
   Testcontainers container.
2. Creates a fresh database per test class.
3. Applies all sqlmq Flyway migrations from `../../sql/migrations/`.
4. Drops the database in `afterAll`.

## License

Apache 2.0 (matches the parent repository).
