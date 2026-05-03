# Changelog

## 0.1.0-SNAPSHOT (2026-05-02)

Initial release.

* Strict pgmq-API-compatible Java client (`io.freesidenomad.sqlmq.client.PgmqClient`
  / `AsyncPgmqClient`) mirroring the [sqlmq-python](../python/) client's surface
  (which mirrors `tembo-pgmq-python` 0.10.0).
* Sync driver: `mssql-jdbc` 12.8. Async: `CompletableFuture` wrapping sync calls
  on a configurable `Executor` (default: `ForkJoinPool.commonPool()`).
  Documented as thread-pool dispatch, NOT true non-blocking I/O.
* TVPs (`dbo.sqlmq_send_tvp`, `dbo.sqlmq_msg_id_tvp`) passed natively via
  mssql-jdbc's `setStructured` + `SQLServerDataTable` — single round trip
  per `sendBatch`/`deleteBatch`/`archiveBatch` call. (Cleaner than the Python
  client, which has to declare-and-EXEC because pyodbc has no native TVP path.)
* Methods that have no sqlmq equivalent
  (`unlogged=true`, `createPartitionedQueue`, `setVt`, `detachArchive`)
  throw `UnsupportedOperationException` with the same wording as the Python
  client's `NotImplementedError`.
* Tests: JUnit 5 + Testcontainers SQL Server 2022 + Flyway. 46 integration
  tests, mirror of the Python suite + an async smoke set.
* Not yet published to Maven Central — vendor from this monorepo for now.
