# sqlmq bench results

Container: mcr.microsoft.com/mssql/server:2022-latest
Started:   2026-05-02T21:41:06.430759Z
Completed: 2026-05-02T21:43:19.827005Z

## Consumer scaling (TPS vs consumer count)

Workload: --preload 10000 msg, --message-size 64 bytes

| consumers | ondisk TPS | inmemory TPS | ratio (im/od) |
|----------:|-----------:|-----------:|--------------:|
| 1 | 2106 | 1995 | 0.95x |
| 2 | 3297 | 3063 | 0.93x |
| 4 | 3747 | 3211  ← peak | 0.86x |
| 8 | 4652  ← peak | 3121 | 0.67x |
| 16 | 4246 | 3018 | 0.71x |

Peak: ondisk @ 8 consumers (4652 TPS), inmemory @ 4 consumers (3211 TPS).

### Latency at peak (informational)

| storage | consumers | p50 (ms) | p95 (ms) | p99 (ms) |
|---------|----------:|---------:|---------:|---------:|
| ondisk | 8 | 574.88 | 1070.68 | 1118.64 |
| inmemory | 4 | 1271.08 | 1825.19 | 1884.75 |

