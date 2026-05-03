package io.freesidenomad.sqlmq.client;

import java.time.Instant;

/**
 * Metrics snapshot for a single queue.
 *
 * <p>Mirrors the Python client's {@code sqlmq.QueueMetrics}.</p>
 *
 * <h3>sqlmq-specific notes</h3>
 * <ul>
 *   <li>{@link #newestMsgAgeSec} is always {@code null} because sqlmq's
 *       {@code metrics} proc does not currently surface it (pgmq derives it
 *       cheaply from the same query; sqlmq's would require a second scan
 *       and was deferred).</li>
 *   <li>{@link #scrapeTime} is filled in client-side at the moment
 *       {@code metrics()} returns (UTC), since sqlmq does not emit it from
 *       the proc.</li>
 * </ul>
 */
public record QueueMetrics(
    String queueName,
    long queueLength,
    Integer newestMsgAgeSec,
    Integer oldestMsgAgeSec,
    long totalMessages,
    Instant scrapeTime
) {}
