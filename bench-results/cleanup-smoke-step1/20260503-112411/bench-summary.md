# sqlmq bench results

Container: mcr.microsoft.com/mssql/server:2022-latest
Started:   2026-05-03T15:24:11.006231Z
Completed: 2026-05-03T15:24:15.657730Z

## Consumer scaling (TPS vs consumer count)

Workload: --preload 2000 msg, --message-size 64 bytes

| consumers | ondisk TPS |
|----------:|-----------:|
| 1 | 1144 |
| 4 | 1487 |
| 16 | 1488  ← peak |

Peak: ondisk @ 16 consumers (1488 TPS).

### Latency at peak (informational)

| storage | consumers | p50 (ms) | p95 (ms) | p99 (ms) |
|---------|----------:|---------:|---------:|---------:|
| ondisk | 16 | 201.77 | 327.03 | 330.81 |

