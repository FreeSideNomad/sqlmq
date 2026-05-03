# sqlmq bench results

Container: (external --jdbc-url)
Started:   2026-05-03T00:49:59.107085Z
Completed: 2026-05-03T01:00:36.189638Z

## Consumer scaling (TPS vs consumer count)

Workload: --preload 10000 msg, --message-size 64 bytes

| consumers | ondisk TPS | inmemory TPS | ratio (im/od) |
|----------:|-----------:|-----------:|--------------:|
| 1 | 325 | 278 | 0.86x |
| 2 | 544 | 435 | 0.80x |
| 4 | 873 | 674  ← peak | 0.77x |
| 8 | 1323 | 502 | 0.38x |
| 16 | 1597  ← peak | 464 | 0.29x |

Peak: ondisk @ 16 consumers (1597 TPS), inmemory @ 4 consumers (674 TPS).

### Latency at peak (informational)

| storage | consumers | p50 (ms) | p95 (ms) | p99 (ms) |
|---------|----------:|---------:|---------:|---------:|
| ondisk | 16 | 2839.37 | 4991.68 | 5210.35 |
| inmemory | 4 | 8059.71 | 13014.67 | 13531.29 |

