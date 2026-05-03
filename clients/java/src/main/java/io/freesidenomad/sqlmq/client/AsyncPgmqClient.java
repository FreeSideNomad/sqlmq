package io.freesidenomad.sqlmq.client;

import javax.sql.DataSource;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;

/**
 * Async wrapper over {@link PgmqClient}.
 *
 * <p>Each method returns a {@link CompletableFuture} produced by
 * {@link CompletableFuture#supplyAsync(java.util.function.Supplier, Executor)}
 * that calls the corresponding sync method on the configured
 * {@link Executor}.</p>
 *
 * <p><strong>This is thread-pool dispatch, NOT true non-blocking I/O.</strong>
 * mssql-jdbc is a blocking JDBC driver; each task occupies a thread for the
 * duration of its DB round trip. R2DBC-mssql is the non-blocking alternative
 * — rejected for v0.1 because it adds a heavy dependency, requires a
 * different driver, and the sqlmq-python client makes the same trade-off
 * (its async path wraps the sync client via {@code asyncio.to_thread}).</p>
 *
 * <p>The default executor is {@link ForkJoinPool#commonPool()}. For
 * production workloads, supply a dedicated executor sized to the
 * connection pool — otherwise blocking JDBC tasks can starve the common
 * pool which is shared with parallel-stream and other framework code.</p>
 */
public final class AsyncPgmqClient implements AutoCloseable {

    private final PgmqClient sync;
    private final Executor executor;

    // ---------------------------------------------------------------- ctors

    public AsyncPgmqClient(String host, int port, String database,
                           String username, String password) {
        this(new PgmqClient(host, port, database, username, password), null);
    }

    public AsyncPgmqClient(String host, int port, String database,
                           String username, String password, Executor executor) {
        this(new PgmqClient(host, port, database, username, password), executor);
    }

    public AsyncPgmqClient(String jdbcUrl, String username, String password) {
        this(new PgmqClient(jdbcUrl, username, password), null);
    }

    public AsyncPgmqClient(String jdbcUrl, String username, String password, Executor executor) {
        this(new PgmqClient(jdbcUrl, username, password), executor);
    }

    public AsyncPgmqClient(DataSource dataSource) {
        this(new PgmqClient(dataSource), null);
    }

    public AsyncPgmqClient(DataSource dataSource, Executor executor) {
        this(new PgmqClient(dataSource), executor);
    }

    private AsyncPgmqClient(PgmqClient sync, Executor executor) {
        this.sync = sync;
        this.executor = executor != null ? executor : ForkJoinPool.commonPool();
    }

    /** Underlying sync client. Useful for blocking transactional code paths. */
    public PgmqClient sync() {
        return sync;
    }

    // ----------------------------------------------------------- delegation

    public CompletableFuture<Void> validateQueueName(String name) {
        return run(() -> sync.validateQueueName(name));
    }

    public CompletableFuture<Void> createQueue(String queue) {
        return run(() -> sync.createQueue(queue));
    }

    public CompletableFuture<Void> createQueue(String queue, boolean unlogged) {
        return run(() -> sync.createQueue(queue, unlogged));
    }

    public CompletableFuture<Void> createPartitionedQueue(String queue) {
        return run(() -> sync.createPartitionedQueue(queue));
    }

    public CompletableFuture<Void> createPartitionedQueue(String queue, int partitionInterval, int retentionInterval) {
        return run(() -> sync.createPartitionedQueue(queue, partitionInterval, retentionInterval));
    }

    public CompletableFuture<Boolean> dropQueue(String queue) {
        return supply(() -> sync.dropQueue(queue));
    }

    public CompletableFuture<Boolean> dropQueue(String queue, boolean partitioned) {
        return supply(() -> sync.dropQueue(queue, partitioned));
    }

    public CompletableFuture<List<String>> listQueues() {
        return supply(sync::listQueues);
    }

    public CompletableFuture<Long> send(String queue, Map<String, Object> message) {
        return supply(() -> sync.send(queue, message));
    }

    public CompletableFuture<Long> send(String queue, Map<String, Object> message, int delaySeconds) {
        return supply(() -> sync.send(queue, message, delaySeconds));
    }

    public CompletableFuture<List<Long>> sendBatch(String queue, List<Map<String, Object>> messages) {
        return supply(() -> sync.sendBatch(queue, messages));
    }

    public CompletableFuture<List<Long>> sendBatch(String queue, List<Map<String, Object>> messages, int delaySeconds) {
        return supply(() -> sync.sendBatch(queue, messages, delaySeconds));
    }

    public CompletableFuture<Optional<Message>> read(String queue) {
        return supply(() -> sync.read(queue));
    }

    public CompletableFuture<Optional<Message>> read(String queue, int vtSeconds) {
        return supply(() -> sync.read(queue, vtSeconds));
    }

    public CompletableFuture<List<Message>> readBatch(String queue, int vtSeconds, int batchSize) {
        return supply(() -> sync.readBatch(queue, vtSeconds, batchSize));
    }

    /**
     * Async {@code read_with_poll}.
     *
     * <p>The polling loop chains {@code thenCompose}-style instead of
     * holding a thread blocked on {@link Thread#sleep} so the underlying
     * executor doesn't pile up sleeping tasks during long polls. Each
     * intermediate hop schedules a fresh
     * {@link CompletableFuture#supplyAsync} after the poll-interval
     * delay via the JDK's {@link CompletableFuture#delayedExecutor}.</p>
     */
    public CompletableFuture<List<Message>> readWithPoll(
            String queue, int vtSeconds, int qty, int maxPollSeconds, int pollIntervalMs) {
        long deadlineNanos = System.nanoTime() + maxPollSeconds * 1_000_000_000L;
        return pollLoop(queue, vtSeconds, qty, pollIntervalMs, deadlineNanos);
    }

    private CompletableFuture<List<Message>> pollLoop(
            String queue, int vtSeconds, int qty, int pollIntervalMs, long deadlineNanos) {
        return readBatch(queue, vtSeconds, qty).thenCompose(msgs -> {
            if (!msgs.isEmpty()) return CompletableFuture.completedFuture(msgs);
            if (System.nanoTime() >= deadlineNanos) return CompletableFuture.completedFuture(msgs);
            // Schedule the next iteration after pollIntervalMs without
            // holding an executor thread idle.
            Executor delayed = CompletableFuture.delayedExecutor(
                Math.max(pollIntervalMs, 1), java.util.concurrent.TimeUnit.MILLISECONDS, executor);
            return CompletableFuture
                .supplyAsync(() -> null, delayed)
                .thenCompose(ignored -> pollLoop(queue, vtSeconds, qty, pollIntervalMs, deadlineNanos));
        });
    }

    public CompletableFuture<Optional<Message>> pop(String queue) {
        return supply(() -> sync.pop(queue));
    }

    public CompletableFuture<Boolean> delete(String queue, long msgId) {
        return supply(() -> sync.delete(queue, msgId));
    }

    public CompletableFuture<List<Long>> deleteBatch(String queue, List<Long> msgIds) {
        return supply(() -> sync.deleteBatch(queue, msgIds));
    }

    public CompletableFuture<Boolean> archive(String queue, long msgId) {
        return supply(() -> sync.archive(queue, msgId));
    }

    public CompletableFuture<List<Long>> archiveBatch(String queue, List<Long> msgIds) {
        return supply(() -> sync.archiveBatch(queue, msgIds));
    }

    public CompletableFuture<Integer> purge(String queue) {
        return supply(() -> sync.purge(queue));
    }

    public CompletableFuture<QueueMetrics> metrics(String queue) {
        return supply(() -> sync.metrics(queue));
    }

    public CompletableFuture<List<QueueMetrics>> metricsAll() {
        return supply(sync::metricsAll);
    }

    public CompletableFuture<Message> setVt(String queue, long msgId, int vtSeconds) {
        return supply(() -> sync.setVt(queue, msgId, vtSeconds));
    }

    public CompletableFuture<Void> detachArchive(String queue) {
        return run(() -> sync.detachArchive(queue));
    }

    @Override
    public void close() {
        sync.close();
    }

    // -------------------------------------------------------- helpers

    private <T> CompletableFuture<T> supply(java.util.function.Supplier<T> sup) {
        return CompletableFuture.supplyAsync(sup, executor);
    }

    private CompletableFuture<Void> run(Runnable r) {
        return CompletableFuture.runAsync(r, executor);
    }
}
