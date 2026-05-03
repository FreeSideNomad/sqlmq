# sqlmq bench results

Container: (external --jdbc-url)
Started:   2026-05-02T23:04:57.137819Z
Completed: 2026-05-02T23:14:12.189022Z

## Consumer scaling (TPS vs consumer count)

Workload: --preload 10000 msg, --message-size 64 bytes

| consumers | ondisk TPS | inmemory TPS | ratio (im/od) |
|----------:|-----------:|-----------:|--------------:|
| 1 | 321 | 291 | 0.91x |
| 2 | 513 | 503 | 0.98x |
| 4 | 864 | 802  ← peak | 0.93x |
| 8 | 1372 | 769 | 0.56x |
| 16 | 1624  ← peak | 793 | 0.49x |

Peak: ondisk @ 16 consumers (1624 TPS), inmemory @ 4 consumers (802 TPS).

### Latency at peak (informational)

| storage | consumers | p50 (ms) | p95 (ms) | p99 (ms) |
|---------|----------:|---------:|---------:|---------:|
| ondisk | 16 | 2700.33 | 4879.79 | 5111.42 |
| inmemory | 4 | 7061.45 | 10905.27 | 11256.53 |

