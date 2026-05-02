package io.freesidenomad.sqlmq.bench;

import io.freesidenomad.sqlmq.client.SqlmqClient;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.StructuredTaskScope;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Spawns N producers and M consumers as virtual threads.
 *
 * Producers each send `messagesPerProducer` messages.
 * Consumers loop: read(maxCount=batchSize, vt=vtSeconds) -> delete.
 * Each delivered (msg_id, consumer_id, received_at_nanos) tuple is recorded.
 *
 * Run completes when all producers are done AND queue stays empty for `quietPeriodMs`.
 *
 * Uses the JDK 25 preview {@link StructuredTaskScope} API (JEP 505, Fifth Preview).
 * Maven is configured with --enable-preview to compile and run this code.
 *
 * Lives in src/main so both the JUnit concurrency tests and the standalone CLI
 * ({@link SqlmqBenchCli}) can use it.
 */
public final class ConcurrencyHarness {

    public record Profile(int producers, int consumers, int messagesPerProducer,
                          int batchSize, int vtSeconds, long quietPeriodMs) {
        public static Profile light() { return new Profile(4, 4, 100, 10, 30, 500); }
        public static Profile medium() { return new Profile(32, 32, 1000, 10, 30, 1000); }
        public static Profile heavy() { return new Profile(256, 256, 10000, 50, 30, 2000); }
        public static Profile asymmetric() { return new Profile(1, 64, 100000, 100, 30, 2000); }
    }

    public record Delivery(long msgId, int consumerId, long receivedAtNanos, long vtExpiresAtNanos) {}

    public record Result(
        List<Delivery> deliveries,
        Set<Long> producedMsgIds,
        AtomicLong producerErrors,
        AtomicLong consumerErrors
    ) {}

    public static Result run(SqlmqClient client, String queue, Profile p) throws Exception {
        var deliveries = new java.util.concurrent.CopyOnWriteArrayList<Delivery>();
        var producedIds = ConcurrentHashMap.<Long>newKeySet();
        var producerErrs = new AtomicLong();
        var consumerErrs = new AtomicLong();
        var producersDone = new AtomicInteger(0);

        // JDK 25 preview API: open(Joiner.awaitAllSuccessfulOrThrow()) replaces the
        // older preview `new ShutdownOnFailure()` from JDK 21-23. join() throws
        // FailedException if any fork failed (no separate throwIfFailed call).
        try (var scope = StructuredTaskScope.open(
                StructuredTaskScope.Joiner.<Object>awaitAllSuccessfulOrThrow())) {
            // Producers
            for (int pi = 0; pi < p.producers(); pi++) {
                final int producerId = pi;
                scope.fork(() -> {
                    try {
                        for (int k = 0; k < p.messagesPerProducer(); k++) {
                            long id = client.send(queue,
                                "{\"p\":" + producerId + ",\"k\":" + k + "}", null);
                            producedIds.add(id);
                        }
                    } catch (SQLException e) {
                        producerErrs.incrementAndGet();
                    } finally {
                        producersDone.incrementAndGet();
                    }
                    return null;
                });
            }

            // Consumers
            for (int ci = 0; ci < p.consumers(); ci++) {
                final int consumerId = ci;
                scope.fork(() -> {
                    long lastNonEmptyNanos = System.nanoTime();
                    while (true) {
                        try {
                            var msgs = client.read(queue, p.vtSeconds(), p.batchSize());
                            if (!msgs.isEmpty()) {
                                long now = System.nanoTime();
                                lastNonEmptyNanos = now;
                                long vtExpiresNanos = now + java.util.concurrent.TimeUnit.SECONDS.toNanos(p.vtSeconds());
                                for (var m : msgs) {
                                    deliveries.add(new Delivery(m.msgId(), consumerId, now, vtExpiresNanos));
                                }
                                client.delete(queue, msgs.stream().map(SqlmqClient.Message::msgId).toList());
                            } else {
                                if (producersDone.get() == p.producers() &&
                                    java.util.concurrent.TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - lastNonEmptyNanos) > p.quietPeriodMs()) {
                                    return null;
                                }
                                Thread.sleep(20);
                            }
                        } catch (SQLException e) {
                            consumerErrs.incrementAndGet();
                            try { Thread.sleep(50); } catch (InterruptedException ie) {
                                Thread.currentThread().interrupt();
                                return null;
                            }
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            return null;
                        }
                    }
                });
            }

            scope.join();
        }

        return new Result(List.copyOf(deliveries), Set.copyOf(producedIds), producerErrs, consumerErrs);
    }

    /** Map: msg_id -> list of (consumer_id, received_at_nanos). */
    public static Map<Long, List<Delivery>> indexByMsgId(List<Delivery> deliveries) {
        var out = new java.util.HashMap<Long, List<Delivery>>();
        for (var d : deliveries) out.computeIfAbsent(d.msgId(), k -> new java.util.ArrayList<>()).add(d);
        return out;
    }
}
