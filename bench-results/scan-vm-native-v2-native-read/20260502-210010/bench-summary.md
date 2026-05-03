# sqlmq bench results

Container: (external --jdbc-url)
Started:   2026-05-03T01:00:10.459586Z
Completed: 2026-05-03T01:10:19.957297Z

## Consumer scaling (TPS vs consumer count)

Workload: --preload 10000 msg, --message-size 64 bytes

| consumers | ondisk TPS | inmemory TPS | ratio (im/od) |
|----------:|-----------:|-----------:|--------------:|
| 1 | 319 | 263 | 0.83x |
| 2 | 549 | 540 | 0.98x |
| 4 | 888 | 756  ← peak | 0.85x |
| 8 | 1307 | 515 | 0.39x |
| 16 | 1628  ← peak | 463 | 0.28x |

Peak: ondisk @ 16 consumers (1628 TPS), inmemory @ 4 consumers (756 TPS).

### Latency at peak (informational)

| storage | consumers | p50 (ms) | p95 (ms) | p99 (ms) |
|---------|----------:|---------:|---------:|---------:|
| ondisk | 16 | 2845.49 | 4837.88 | 5028.11 |
| inmemory | 4 | 7302.52 | 11430.03 | 11897.31 |

