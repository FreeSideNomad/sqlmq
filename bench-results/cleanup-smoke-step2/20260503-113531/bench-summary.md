# sqlmq bench results

Container: mcr.microsoft.com/mssql/server:2022-latest
Started:   2026-05-03T15:35:31.421359Z
Completed: 2026-05-03T15:35:35.961955Z

## Consumer scaling (TPS vs consumer count)

Workload: --preload 2000 msg, --message-size 64 bytes

| consumers | ondisk TPS |
|----------:|-----------:|
| 1 | 1154 |
| 4 | 1475 |
| 16 | 1585  ← peak |

Peak: ondisk @ 16 consumers (1585 TPS).

### Latency at peak (informational)

| storage | consumers | p50 (ms) | p95 (ms) | p99 (ms) |
|---------|----------:|---------:|---------:|---------:|
| ondisk | 16 | 163.28 | 255.30 | 265.32 |

