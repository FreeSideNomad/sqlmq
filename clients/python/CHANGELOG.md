# Changelog

## 0.1.0 (2026-05-02)

Initial release.

* Strict pgmq-API-compatible client (`sqlmq.PGMQueue` / `sqlmq.AsyncPGMQueue`)
  mirroring `tembo-pgmq-python` 0.10.0's public surface.
* Sync driver: `pyodbc`. Async: `asyncio.to_thread` wrapper over the sync
  client (no separate `aioodbc` dependency).
* Methods that have no sqlmq equivalent
  (`unlogged=True`, `create_partitioned_queue`, `set_vt`, `detach_archive`,
  `send(..., tz=)`) raise `NotImplementedError` with a message pointing at
  the design rationale.
* Tests: `pytest` + `testcontainers[mssql]` spin up a real SQL Server 2022
  per session; each test gets a fresh database with all sqlmq migrations
  applied.
* Not yet published to PyPI — install from source.
