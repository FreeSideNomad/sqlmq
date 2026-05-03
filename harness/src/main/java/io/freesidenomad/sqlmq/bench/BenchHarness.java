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
 *
 * Two run modes:
 * <ul>
 *   <li>{@link #runOnce} — single (profile, storage, run-index) sample. Used by
 *       the original profile-based matrix.</li>
 *   <li>{@link #runScan} — sweep over a list of consumer counts at fixed preload depth,
 *       producers=0. The headline number is drain TPS = preload / drain-seconds. Used by
 *       the CLI's {@code --scan-consumers} mode to find the consumer-count throughput peak.</li>
 * </ul>
 */
public final class BenchHarness {

    /**
     * Per-run sample. {@code tps} is the headline metric (was {@code msgsPerSec} pre-v3).
     * {@code preload} and {@code msgSizeBytes} record the workload shape for the CSV/JSON exports.
     *
     * <p>The {@code storage} field is retained as a stable column for the CSV/JSON exports and
     * always carries the literal {@code "ondisk"} since V014 retired the in-memory variant.
     * Downstream tooling can keep grouping by it without code changes.</p>
     */
    public record RunResult(
        String profile,
        String storage,
        int runIndex,        // 1-based
        int producers,
        int consumers,
        int preload,
        int msgSizeBytes,
        double tps,
        double p50Ms,
        double p95Ms,
        double p99Ms,
        int delivered,
        double elapsedSeconds,
        long producerErrors,
        long consumerErrors
    ) {}

    /** All runs are on-disk now (V014 retired in-memory). Used as the fixed storage label in {@link RunResult}. */
    public static final String STORAGE_ONDISK = "ondisk";

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
                                    int runIndex) throws Exception {
        var q = uniqueQueueName("bench");
        client.createQueue(q, false, "json", null);

        // Producer-side latency tracking: msg_id -> nanoTime at send completion.
        // Only populated for the preload phase; in-flight producers don't expose
        // per-msg send times via ConcurrencyHarness.
        var sendNanos = new ConcurrentHashMap<Long, Long>();

        if (preloadCount > 0) {
            preload(client, q, preloadCount, profile.messageSizeBytes(), sendNanos);
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
        double tps = delivered / Math.max(seconds, 0.001);

        return new RunResult(
            profileName, STORAGE_ONDISK, runIndex,
            profile.producers(), profile.consumers(), preloadCount, profile.messageSizeBytes(),
            tps,
            pct(latenciesNs, 50) / 1e6,
            pct(latenciesNs, 95) / 1e6,
            pct(latenciesNs, 99) / 1e6,
            delivered,
            seconds,
            result.producerErrors().get(),
            result.consumerErrors().get());
    }

    /**
     * Consumer-scan: preload {@code preloadCount} messages, then spin up {@code consumers}
     * consumers and measure drain TPS = preload / drain-seconds. Producers count is forced to 0.
     *
     * The {@code profile} parameter supplies batchSize, vtSeconds, quietPeriodMs and messageSizeBytes;
     * its producers/consumers/messagesPerProducer fields are ignored (overridden to 0/consumers/0).
     *
     * @param scanLabel  the {@code "scan"} label baked into {@link RunResult#profile()} so reports
     *                   can group these samples together.
     * @param consumers  number of consumer threads to spin up
     * @param preloadCount messages to preload before consumers start; must be > 0
     * @param runIndex   1-based run index
     */
    public static RunResult runScan(SqlmqClient client,
                                    String scanLabel,
                                    Profile profile,
                                    int consumers,
                                    int preloadCount,
                                    int runIndex) throws Exception {
        if (preloadCount <= 0) {
            throw new IllegalArgumentException("runScan requires preloadCount > 0, got " + preloadCount);
        }
        var q = uniqueQueueName("scan");
        client.createQueue(q, false, "json", null);

        var sendNanos = new ConcurrentHashMap<Long, Long>();
        try {
            preload(client, q, preloadCount, profile.messageSizeBytes(), sendNanos);

            // Build a consumer-only profile: producers=0, messagesPerProducer=0, consumers=C.
            var consumerOnly = new Profile(
                0, consumers, 0,
                profile.batchSize(), profile.vtSeconds(), profile.quietPeriodMs(),
                profile.messageSizeBytes());

            long t0 = System.nanoTime();
            ConcurrencyHarness.Result result;
            try {
                result = ConcurrencyHarness.run(client, q, consumerOnly);
            } finally {
                try { client.dropQueue(q); } catch (SQLException ignored) {}
            }
            long elapsedNs = System.nanoTime() - t0;

            // Drain TPS — denominator is preload count (the work to be done), not delivered
            // (which should be == preload but defensively use preload to avoid zero-div if
            // the consumers exit early on quiet detection before the queue actually drained).
            double seconds = elapsedNs / 1e9;
            double tps = preloadCount / Math.max(seconds, 0.001);

            var latenciesNs = new ArrayList<Long>(result.deliveries().size());
            for (var d : result.deliveries()) {
                Long sent = sendNanos.get(d.msgId());
                latenciesNs.add(sent != null ? (d.receivedAtNanos() - sent) : (d.receivedAtNanos() - t0));
            }
            Collections.sort(latenciesNs);

            return new RunResult(
                scanLabel, STORAGE_ONDISK, runIndex,
                0, consumers, preloadCount, profile.messageSizeBytes(),
                tps,
                pct(latenciesNs, 50) / 1e6,
                pct(latenciesNs, 95) / 1e6,
                pct(latenciesNs, 99) / 1e6,
                result.deliveries().size(),
                seconds,
                result.producerErrors().get(),
                result.consumerErrors().get());
        } catch (Exception e) {
            // If preload/setup blew up before runOnce's finally would drop, drop here.
            try { client.dropQueue(q); } catch (SQLException ignored) {}
            throw e;
        }
    }

    /**
     * Send {@code count} messages of approximately {@code msgSizeBytes} each into {@code queue}
     * via {@code sendBatch}. Records send-completion nanoTime per msg-id into {@code sendNanos}.
     */
    public static void preload(SqlmqClient client, String queue, int count, int msgSizeBytes,
                               ConcurrentHashMap<Long, Long> sendNanos) throws SQLException {
        int batchSize = 1000;
        for (int i = 0; i < count; i += batchSize) {
            int n = Math.min(batchSize, count - i);
            var msgs = new ArrayList<String>(n);
            for (int j = 0; j < n; j++) {
                msgs.add(ConcurrencyHarness.paddedPayload("\"i\":" + (i + j), msgSizeBytes));
            }
            long sentNanos = System.nanoTime();
            var ids = client.sendBatch(queue, msgs);
            for (long id : ids) sendNanos.put(id, sentNanos);
        }
    }

    private static long pct(List<Long> sorted, int p) {
        if (sorted.isEmpty()) return 0;
        int idx = Math.min(sorted.size() - 1, (int) Math.ceil(sorted.size() * p / 100.0) - 1);
        return sorted.get(Math.max(0, idx));
    }
}
