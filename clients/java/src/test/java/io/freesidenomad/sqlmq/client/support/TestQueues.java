package io.freesidenomad.sqlmq.client.support;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Per-JVM unique queue-name generator.
 *
 * <p>Two flavors:</p>
 * <ul>
 *   <li>{@link #uniqueName(String)} — short prefix + monotonic counter.
 *       Stable within a single JVM; collision-safe for the duration of
 *       one test run.</li>
 *   <li>{@link #randomName(String)} — prefix + random hex. Collision-safe
 *       across JVMs; useful when tests share a long-lived database.</li>
 * </ul>
 */
public final class TestQueues {

    private static final AtomicLong SEQ = new AtomicLong();

    private TestQueues() {}

    public static String uniqueName(String prefix) {
        return prefix + "_" + SEQ.incrementAndGet();
    }

    public static String randomName(String prefix) {
        return prefix + "_" + UUID.randomUUID().toString().replace("-", "").substring(0, 16);
    }
}
