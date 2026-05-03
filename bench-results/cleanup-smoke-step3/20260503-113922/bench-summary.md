# sqlmq bench results

Container: mcr.microsoft.com/mssql/server:2022-latest
Started:   2026-05-03T15:39:22.420029Z
Completed: 2026-05-03T15:39:27.055409Z

## Consumer scaling (TPS vs consumer count)

Workload: --preload 2000 msg, --message-size 64 bytes

| consumers | ondisk TPS |
|----------:|-----------:|
| 1 | 1169 |
| 4 | 1380 |
| 16 | 1530  ← peak |

Peak: ondisk @ 16 consumers (1530 TPS).

### Latency at peak (informational)

| storage | consumers | p50 (ms) | p95 (ms) | p99 (ms) |
|---------|----------:|---------:|---------:|---------:|
| ondisk | 16 | 167.65 | 285.10 | 288.37 |

