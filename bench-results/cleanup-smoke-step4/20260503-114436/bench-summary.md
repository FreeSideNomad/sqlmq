# sqlmq bench results

Container: mcr.microsoft.com/mssql/server:2022-latest
Started:   2026-05-03T15:44:36.267241Z
Completed: 2026-05-03T15:44:40.794624Z

## Consumer scaling (TPS vs consumer count)

Workload: --preload 2000 msg, --message-size 64 bytes

| consumers | ondisk TPS |
|----------:|-----------:|
| 1 | 1184 |
| 4 | 1443 |
| 16 | 1567  ← peak |

Peak: ondisk @ 16 consumers (1567 TPS).

### Latency at peak (informational)

| storage | consumers | p50 (ms) | p95 (ms) | p99 (ms) |
|---------|----------:|---------:|---------:|---------:|
| ondisk | 16 | 151.76 | 256.18 | 263.93 |

