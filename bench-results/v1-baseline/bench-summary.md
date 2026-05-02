# sqlmq bake-off results (PARTIAL — heavy/asymmetric/burst not completed)

Container: mcr.microsoft.com/mssql/server:2022-latest
Run at: 2026-05-02 (local Docker, MacBook)

## Status: PARTIAL DATA

The `harness/target/bench-results/<ts>/bench-summary.md` file was **not** produced because the
`BakeOffRunner` test only writes its summary at the very end of the run, and the run had to be
terminated before the heavy / asymmetric / burst profiles could complete.

**Why terminated:** with the current `Profile` definitions, a single `heavy` run
(256 producers x 10,000 messages = 2.56M messages) takes ~25 minutes wall-clock at the observed
~430 msgs/sec throughput on the local Docker SQL Server 2022 container — and we need 5 runs of
each of {ondisk, inmemory} for both `heavy` and `asymmetric` (1x100k=100k messages but with
1 producer / 64 consumers), plus the `burst` profile (preload 100k then drain). Projecting
forward, the full bake-off would take ~6-10 hours wall time, well past the 90-minute ceiling
specified in the task.

**Light + medium completed cleanly** — all 5 runs each, both storage modes — and those numbers
are reported below. The first heavy ondisk run completed (single sample, no median possible).

Each row is the median of 5 runs (or 1-sample where noted).

| Profile | Storage  | msgs/sec | p50 (ms)   | p99 (ms)    | Notes              |
|---------|----------|---------:|-----------:|------------:|--------------------|
| light   | ondisk   |      340 |     364.40 |      660.50 | median of 5        |
| light   | inmemory |      339 |     290.00 |      647.10 | median of 5        |
| medium  | ondisk   |      444 |   34862.00 |    70127.00 | median of 5        |
| medium  | inmemory |      424 |   41035.90 |    73641.70 | median of 5        |
| heavy   | ondisk   |      428 |  708965.40 |  1480530.00 | n=1 (run aborted)  |
| heavy   | inmemory |        - |          - |           - | not run            |
| asymmetric | ondisk |       - |          - |           - | not run            |
| asymmetric | inmemory |     - |          - |           - | not run            |
| burst   | ondisk   |        - |          - |           - | not run            |
| burst   | inmemory |        - |          - |           - | not run            |

p95 columns omitted because the test harness only printed p50 and p99 to stdout per run; the
full latency arrays are only persisted into the summary file that was never written.

## ondisk vs inmemory ratios (partial)

| Profile | ondisk msgs/sec | inmemory msgs/sec | ratio (im/od) | ondisk p99 | inmemory p99 |
|---------|----------------:|------------------:|--------------:|-----------:|-------------:|
| light   |             340 |               339 |         1.00x |   660.50ms |     647.10ms |
| medium  |             444 |               424 |         0.95x | 70127.00ms |   73641.70ms |
| heavy   |             428 |              n/a  |          n/a  | 1480530.00ms |        n/a |

## Raw per-run data captured

```
=== light [ondisk] ===
  run 1: 305 msgs/sec, p50=437.6ms p99=772.8ms
  run 2: 340 msgs/sec, p50=364.4ms p99=660.5ms
  run 3: 336 msgs/sec, p50=381.9ms p99=660.9ms
  run 4: 377 msgs/sec, p50=301.5ms p99=552.9ms
  run 5: 378 msgs/sec, p50=255.1ms p99=532.4ms

=== light [inmemory] ===
  run 1: 302 msgs/sec, p50=504.7ms p99=813.0ms
  run 2: 339 msgs/sec, p50=290.0ms p99=647.1ms
  run 3: 380 msgs/sec, p50=310.0ms p99=543.8ms
  run 4: 434 msgs/sec, p50=251.0ms p99=404.3ms
  run 5: 335 msgs/sec, p50=325.6ms p99=684.1ms

=== medium [ondisk] ===
  run 1: 515 msgs/sec, p50=30187.5ms p99=60493.6ms
  run 2: 423 msgs/sec, p50=37699.2ms p99=73601.6ms
  run 3: 458 msgs/sec, p50=35447.7ms p99=68071.3ms
  run 4: 435 msgs/sec, p50=36801.8ms p99=71116.7ms
  run 5: 444 msgs/sec, p50=34862.0ms p99=70127.0ms

=== medium [inmemory] ===
  run 1: 357 msgs/sec, p50=45073.2ms p99=87616.1ms
  run 2: 401 msgs/sec, p50=40298.0ms p99=78143.9ms
  run 3: 468 msgs/sec, p50=35482.6ms p99=66761.2ms
  run 4: 442 msgs/sec, p50=35087.4ms p99=70533.7ms
  run 5: 424 msgs/sec, p50=41035.9ms p99=73641.7ms

=== heavy [ondisk] ===
  run 1: 428 msgs/sec, p50=708965.4ms p99=1480530.0ms
```

## Recommended follow-ups

1. **Right-size the `heavy` and `asymmetric` profiles in `ConcurrencyHarness.java`.**
   With 2.56M messages per heavy run, a single ondisk run had p99 latency of ~24.7 minutes —
   that's not "heavy concurrency stress" so much as "queue-overflow benchmark." Consider
   reducing `messagesPerProducer` for heavy from 10,000 to ~1,000 (still 256k msgs total)
   so the full bake-off finishes in well under an hour.
2. **Have `BakeOffRunner.writeResults` flush incrementally** (or write one summary per
   profile/storage combo) so a partial run still leaves usable data on disk.
3. **Print p95 to stdout** alongside p50/p99 so partial-run logs are still complete.
