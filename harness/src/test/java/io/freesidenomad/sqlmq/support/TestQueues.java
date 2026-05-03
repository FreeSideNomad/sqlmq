package io.freesidenomad.sqlmq.support;

import java.util.concurrent.atomic.AtomicLong;

public final class TestQueues {
    private static final AtomicLong SEQ = new AtomicLong();

    private TestQueues() {}

    /** Unique queue name per call within a JVM. */
    public static String uniqueName(String prefix) {
        return prefix + "_" + SEQ.incrementAndGet();
    }
}
