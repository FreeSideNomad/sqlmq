# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Status

Greenfield. The goal is to port [pgmq](https://github.com/tembo-io/pgmq) (PostgreSQL Message Queue) to Microsoft SQL Server. Will be published as public OSS at **github.com/FreeSideNomad/sqlmq**. Before writing implementation code, the work plan is:

1. Research prior art — pgmq itself, plus existing SQL Server queue patterns (Service Broker, table-as-queue with `READPAST` + `UPDLOCK`, etc.).
2. Produce a detailed porting plan that maps each pgmq feature to a SQL Server equivalent.
3. Build incrementally with heavy concurrency testing at every step.

This file should be updated as the architecture solidifies.

## Goals and Constraints

These are load-bearing requirements that must shape every design decision:

- **Portable and lightweight is the whole point.** This is the project's reason to exist. Install = run a T-SQL script against a database. No sidecar services, no agents, no host-language runtime required by the product itself. Every design call is judged against install footprint and operational overhead first; features second.
- **Target platform:** SQL Server 2022 and later. Must be portable between Windows and Linux installations. **Hard exclusions:** CLR, extended stored procs (`xp_*`), FILESTREAM, MSDTC, Service Broker as a dependency, anything else Windows-only.
- **Implementation surface:** T-SQL stored procedures are the entire API. Use "every trick in the SQL Server book" within the constraints above — `READPAST`, `UPDLOCK`, `OUTPUT` clauses, partitioned tables, columnstore where useful, `OPTION (RECOMPILE)` where appropriate.
- **Minimum table count per queue.** Resist adding metadata/audit/config tables. Each added object is install and upgrade burden. When tempted, ask whether it can live in an existing table or be derived.
- **In-memory option:** Provide an opt-in path that uses memory-optimized tables and natively compiled stored procedures for queues that need maximum throughput. The on-disk and in-memory variants should expose the **same logical API** — if symmetry conflicts with a feature, drop the feature.
- **Concurrency is the product.** Correctness under high concurrent producer/consumer load is the primary acceptance criterion, not feature count. Every change needs concurrency tests before it is considered done.

## pgmq feature surface to port

Use this as the checklist when planning. Validate the exact current surface against upstream pgmq before committing to a contract:

- Queue lifecycle: `create`, `create_unlogged` (SQL Server analogue: in-memory or `SIMPLE` recovery DB), `create_partitioned`, `drop_queue`, `list_queues`.
- Message ops: `send`, `send_batch`, `read` (with visibility timeout / VT), `read_with_poll`, `pop`, `delete`, `archive`, `purge_queue`.
- Metrics: `metrics`, `metrics_all`.
- Archive table per queue with the same row shape as the active queue.

## Testing posture

Concurrency bugs hide from single-threaded tests. The bar:

- Every stored proc gets a single-session correctness test **and** a multi-session contention test (N producers × M consumers, asserting no message is delivered twice, none lost, VT honored).
- Tests must run against both the on-disk and in-memory variants.
- Tests must run on SQL Server for Linux in CI (Docker image `mcr.microsoft.com/mssql/server:2022-latest`) so the Windows/Linux portability claim is enforced, not aspirational.
- When investigating a suspected race, reproduce it with a deterministic concurrent test before proposing a fix. Don't guess.

## Notes for future Claude sessions

- There is no build, lint, or test command yet — this section will be filled in once the project structure is chosen (likely a `sql/` directory of idempotent migration scripts plus a test harness in a host language; the host language has not been decided).
- Don't invent a directory layout or tooling stack in this file before the user agrees to one. Update this section once decisions are made.
- When upstream pgmq behavior is unclear, fetch the current source from the pgmq repo rather than relying on memory — the surface has evolved.
