package io.freesidenomad.sqlmq.benchmark;

import io.freesidenomad.sqlmq.bench.ConcurrencyHarness;
import io.freesidenomad.sqlmq.bench.ConcurrencyHarness.Profile;
import io.freesidenomad.sqlmq.client.SqlmqClient;
import io.freesidenomad.sqlmq.support.DatabasePerTest;
import io.freesidenomad.sqlmq.support.SqlServerContainer;
import io.freesidenomad.sqlmq.support.TestQueues;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Legacy: kept for short bake-offs that are convenient to drive from JUnit. For
 * production / full-matrix performance runs prefer the standalone CLI
 * ({@code io.freesidenomad.sqlmq.bench.SqlmqBenchCli}) — it runs in its own JVM
 * with caller-controlled heap sizing, avoiding the OOM pressure that 50
 * accumulated workload runs put on a single surefire fork.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@ExtendWith(DatabasePerTest.class)
class BakeOffRunner {

    private record RunResult(String profile, String storage, double tps,
                             double p50Ms, double p95Ms, double p99Ms, int delivered) {}

    @Test
    void bakeOff(ExtensionContext ctx) throws Exception {
        var client = new SqlmqClient(DatabasePerTest.dataSource(ctx));
        var allRuns = new ArrayList<RunResult>();
        // Stable timestamped output dir, set up before any runs so partial results
        // can be flushed after each (profile, storage) pair completes. This means
        // a crash mid-bakeoff (OOM, surefire fork timeout, etc.) still leaves
        // recoverable median data on disk.
        var ts = Instant.now().toString().replace(':', '-');
        var dir = Path.of("target/bench-results", ts);
        Files.createDirectories(dir);
        System.out.println("Bake-off output dir: " + dir.toAbsolutePath());

        for (var named : BenchmarkProfiles.all()) {
            // V014: in-memory storage retired — only on-disk runs now.
            for (var storage : List.of("ondisk")) {
                System.out.printf("%n=== %s [%s] ===%n", named.name(), storage);
                var samples = new ArrayList<RunResult>();
                for (int i = 0; i < 5; i++) {
                    var sample = runOnce(client, named.name(), named.profile(),
                                         named.preloadCount(), storage);
                    samples.add(sample);
                    System.out.printf("  run %d: %.0f TPS, p50=%.1fms p95=%.1fms p99=%.1fms delivered=%d%n",
                        i + 1, sample.tps(), sample.p50Ms(), sample.p95Ms(),
                        sample.p99Ms(), sample.delivered());
                }
                samples.sort(java.util.Comparator.comparingDouble(RunResult::tps));
                allRuns.add(samples.get(2));  // median
                writeResults(allRuns, dir);  // incremental flush after each pair
            }
        }

        writeResults(allRuns, dir);
    }

    private RunResult runOnce(SqlmqClient client, String profileName, Profile profile,
                              int preloadCount, String storage) throws Exception {
        var q = TestQueues.uniqueName("bench");
        client.createQueue(q, storage, false, "json", null);

        // Producer-side latency tracking: msg_id -> nanoTime at send completion.
        var sendNanos = new ConcurrentHashMap<Long, Long>();

        // Preload phase (burst profile).
        if (preloadCount > 0) {
            int batchSize = 1000;
            for (int i = 0; i < preloadCount; i += batchSize) {
                int n = Math.min(batchSize, preloadCount - i);
                var msgs = new ArrayList<String>(n);
                for (int j = 0; j < n; j++) msgs.add("{\"i\":" + (i + j) + "}");
                long sentNanos = System.nanoTime();
                var ids = client.sendBatch(q, msgs);
                for (long id : ids) sendNanos.put(id, sentNanos);
            }
        }

        long t0 = System.nanoTime();
        var result = ConcurrencyHarness.run(client, q, profile);
        long elapsedNs = System.nanoTime() - t0;

        // Producer-side timing for non-preload runs: ConcurrencyHarness's producers don't
        // expose per-msg send times. For non-preload profiles we approximate latency from
        // the harness's own (receivedAt - t0) rather than queue-residency time.
        var latenciesNs = new ArrayList<Long>(result.deliveries().size());
        for (var d : result.deliveries()) {
            Long sent = sendNanos.get(d.msgId());
            if (sent != null) {
                latenciesNs.add(d.receivedAtNanos() - sent);
            } else {
                latenciesNs.add(d.receivedAtNanos() - t0);
            }
        }
        Collections.sort(latenciesNs);

        int delivered = result.deliveries().size();
        double seconds = elapsedNs / 1e9;
        double tps = delivered / Math.max(seconds, 0.001);

        client.dropQueue(q);

        return new RunResult(
            profileName, storage, tps,
            pct(latenciesNs, 50) / 1e6,
            pct(latenciesNs, 95) / 1e6,
            pct(latenciesNs, 99) / 1e6,
            delivered);
    }

    private long pct(List<Long> sorted, int p) {
        if (sorted.isEmpty()) return 0;
        int idx = Math.min(sorted.size() - 1, (int) Math.ceil(sorted.size() * p / 100.0) - 1);
        return sorted.get(Math.max(0, idx));
    }

    private void writeResults(List<RunResult> results, Path dir) throws IOException {
        var summary = new StringBuilder();
        summary.append("# sqlmq bake-off results\n\n");
        summary.append("Container: ").append(SqlServerContainer.get().getDockerImageName()).append("\n");
        summary.append("Run at: ").append(Instant.now()).append("\n\n");
        summary.append("Each row is the median of 5 runs.\n\n");
        summary.append("| Profile | Storage | TPS | delivered | p50 (ms) | p95 (ms) | p99 (ms) |\n");
        summary.append("|---------|---------|----:|----------:|---------:|---------:|---------:|\n");
        for (var r : results) {
            summary.append("| ").append(r.profile()).append(" | ").append(r.storage()).append(" | ")
                   .append(String.format("%.0f", r.tps())).append(" | ")
                   .append(r.delivered()).append(" | ")
                   .append(String.format("%.2f", r.p50Ms())).append(" | ")
                   .append(String.format("%.2f", r.p95Ms())).append(" | ")
                   .append(String.format("%.2f", r.p99Ms())).append(" |\n");
        }

        // V014: in-memory storage retired — no side-by-side ratios needed.
        var summaryPath = dir.resolve("bench-summary.md");
        Files.writeString(summaryPath, summary.toString());
        System.out.println("\n" + summary);
        System.out.println("Results written to: " + summaryPath.toAbsolutePath());
    }
}
