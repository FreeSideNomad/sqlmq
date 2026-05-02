package io.freesidenomad.sqlmq.bench;

import io.freesidenomad.sqlmq.bench.ConcurrencyHarness.Profile;
import io.freesidenomad.sqlmq.client.SqlmqClient;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * One-shot bench runner: creates a queue, runs the workload, computes
 * per-run throughput + percentiles, drops the queue.
 *
 * Extracted from the JUnit {@code BakeOffRunner.runOnce(...)} so the same
 * measurement logic powers both the test-driven bake-off and the standalone
 * CLI ({@link SqlmqBenchCli}).
 */
public final class BenchHarness {

    public record RunResult(
        String profile,
        String storage,
        int runIndex,        // 1-based
        double msgsPerSec,
        double p50Ms,
        double p95Ms,
        double p99Ms,
        int delivered,
        double elapsedSeconds,
        long producerErrors,
        long consumerErrors
    ) {}

    private static final AtomicLong QUEUE_SEQ = new AtomicLong();

    private BenchHarness() {}

    /** Generate a unique queue name within this JVM (does not collide across processes). */
    public static String uniqueQueueName(String prefix) {
        return prefix + "_" + QUEUE_SEQ.incrementAndGet();
    }

    /**
     * Run a single sample. Creates a fresh queue, runs the workload, drops the queue.
     *
     * @param runIndex     1-based run index used for {@link RunResult#runIndex()} only.
     * @param preloadCount 0 = none. >0 = pre-load this many JSON messages before consumers start.
     */
    public static RunResult runOnce(SqlmqClient client,
                                    String profileName,
                                    Profile profile,
                                    int preloadCount,
                                    String storage,
                                    int runIndex) throws Exception {
        var q = uniqueQueueName("bench");
        client.createQueue(q, storage, false, "json", null);

        // Producer-side latency tracking: msg_id -> nanoTime at send completion.
        // Only populated for the preload phase; in-flight producers don't expose
        // per-msg send times via ConcurrencyHarness.
        var sendNanos = new ConcurrentHashMap<Long, Long>();

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
        ConcurrencyHarness.Result result;
        try {
            result = ConcurrencyHarness.run(client, q, profile);
        } finally {
            // Always try to drop the queue, even if the workload threw — otherwise
            // a long-running CLI session leaks queues into the database.
            try { client.dropQueue(q); } catch (SQLException ignored) {}
        }
        long elapsedNs = System.nanoTime() - t0;

        var latenciesNs = new ArrayList<Long>(result.deliveries().size());
        for (var d : result.deliveries()) {
            Long sent = sendNanos.get(d.msgId());
            // Non-preload approximation: receive-time minus run start. This is queue-residency
            // for preloaded messages, otherwise harness-elapsed-since-t0 — same scheme as the
            // legacy BakeOffRunner so v1 numbers stay comparable.
            latenciesNs.add(sent != null ? (d.receivedAtNanos() - sent) : (d.receivedAtNanos() - t0));
        }
        Collections.sort(latenciesNs);

        int delivered = result.deliveries().size();
        double seconds = elapsedNs / 1e9;
        double mps = delivered / Math.max(seconds, 0.001);

        return new RunResult(
            profileName, storage, runIndex, mps,
            pct(latenciesNs, 50) / 1e6,
            pct(latenciesNs, 95) / 1e6,
            pct(latenciesNs, 99) / 1e6,
            delivered,
            seconds,
            result.producerErrors().get(),
            result.consumerErrors().get());
    }

    private static long pct(List<Long> sorted, int p) {
        if (sorted.isEmpty()) return 0;
        int idx = Math.min(sorted.size() - 1, (int) Math.ceil(sorted.size() * p / 100.0) - 1);
        return sorted.get(Math.max(0, idx));
    }
}
