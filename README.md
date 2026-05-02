# sqlmq

A pure T-SQL message queue for SQL Server 2022 and later. Port of [pgmq](https://github.com/tembo-io/pgmq) (PostgreSQL Message Queue) to SQL Server.

> **Status:** Design phase. No code yet. See the [design spec](docs/superpowers/specs/2026-05-02-sqlmq-port-from-pgmq-design.md).

## What it is

- **A queue, installed by applying SQL migrations.** No services, no agents, no host-language runtime required.
- **Portable.** SQL Server 2022+, Windows or Linux. No CLR, no Service Broker, no FILESTREAM, no MSDTC.
- **FIFO by default.** Plus pgmq-style grouped-FIFO ("one in-flight per group") for partition-key ordering.
- **Two storage variants** behind a single API: classic on-disk tables, and memory-optimized (Hekaton) tables with natively compiled stored procedures. v1 ships both for a head-to-head bake-off.

## What it isn't (in v1)

- Not a server-side long-poll — long-polling is a client-side concern. The [research](research/sqlserver-longpoll.md) explains why; every shipping SQL Server queue library does the same.
- Not Service Broker. Not Query Notifications.
- No partitioned queues, no topics, no LISTEN/NOTIFY, no down migrations.

## Calling shape

```sql
EXEC sqlmq.create_queue
    @name = 'orders',
    @storage = 'inmemory',          -- 'ondisk' | 'inmemory'
    @grouped = 1,                   -- enable grouped-FIFO reads
    @payload_type = 'json',         -- 'json' | 'binary'
    @max_delivery_count = 5;        -- DLQ after 5 failed deliveries

EXEC sqlmq.send
    @queue = 'orders',
    @message = N'{"order_id": 42}';

EXEC sqlmq.read_grouped
    @queue = 'orders',
    @vt_seconds = 30,
    @max_count = 10;
```

Full API in the design spec.

## Repository layout

```
sql/migrations/   # Flyway-style V*__*.sql — the product
harness/          # Java 25 + Maven test harness — not shipped to consumers
research/         # Background reading on pgmq + SQL Server patterns
docs/             # Specs and design docs
```

## Running migrations

The migrations follow Flyway naming, but Flyway is not a dependency — run them with whatever migration tool you already use:

- Flyway: drop them into your `sql/` location
- Liquibase: include each as a SQL changeset
- DbUp: load from `sql/migrations/`
- Plain `sqlcmd`: apply in filename order

## License

Apache 2.0. See [LICENSE](LICENSE).

## Acknowledgements

sqlmq is a port — not an original design. All credit for the queue model goes to the [Tembo pgmq](https://github.com/tembo-io/pgmq) team.
