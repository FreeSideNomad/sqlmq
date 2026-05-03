package io.freesidenomad.sqlmq.client;

import java.sql.SQLException;
import java.util.List;
import java.util.function.Function;

/**
 * Hangfire-style adaptive backoff for client-side long-polling.
 * Default: 250ms initial, clamped to [100, 1000]ms.
 * After a hit, drops to the minimum; after consecutive misses, grows toward the maximum.
 */
public final class LongPollingConsumer {

    private final SqlmqClient client;
    private final String queue;
    private final int vtSeconds;
    private final int maxCount;
    private final long minMs;
    private final long maxMs;
    private long currentMs;

    public LongPollingConsumer(SqlmqClient client, String queue, int vtSeconds, int maxCount) {
        this(client, queue, vtSeconds, maxCount, 100, 1000, 250);
    }

    public LongPollingConsumer(SqlmqClient client, String queue, int vtSeconds, int maxCount,
                               long minMs, long maxMs, long initialMs) {
        this.client = client;
        this.queue = queue;
        this.vtSeconds = vtSeconds;
        this.maxCount = maxCount;
        this.minMs = minMs;
        this.maxMs = maxMs;
        this.currentMs = Math.min(Math.max(initialMs, minMs), maxMs);
    }

    /**
     * Poll until handler returns false or thread is interrupted.
     * Handler returns true to continue, false to stop.
     */
    public void run(Function<List<SqlmqClient.Message>, Boolean> handler)
            throws SQLException, InterruptedException {
        while (!Thread.currentThread().isInterrupted()) {
            var msgs = client.read(queue, vtSeconds, maxCount);
            if (msgs.isEmpty()) {
                Thread.sleep(currentMs);
                currentMs = Math.min(currentMs * 2, maxMs);
            } else {
                currentMs = minMs;
                if (!handler.apply(msgs)) return;
            }
        }
    }
}
