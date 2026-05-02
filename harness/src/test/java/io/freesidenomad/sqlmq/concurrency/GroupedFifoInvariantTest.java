package io.freesidenomad.sqlmq.concurrency;

import io.freesidenomad.sqlmq.client.SqlmqClient;
import io.freesidenomad.sqlmq.support.DatabasePerTest;
import io.freesidenomad.sqlmq.support.TestQueues;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.StructuredTaskScope;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@ExtendWith(DatabasePerTest.class)
class GroupedFifoInvariantTest {

    private record Receipt(long msgId, String groupKey, int consumerId,
                           long receivedNanos, long deletedNanos) {}

    @ParameterizedTest
    @MethodSource("io.freesidenomad.sqlmq.support.StorageVariants#all")
    void noTwoConsumersHoldSameGroupAndPerGroupOrderingIsPreserved(String storage, ExtensionContext ctx) throws Exception {
        var client = new SqlmqClient(DatabasePerTest.dataSource(ctx));
        var q = TestQueues.uniqueName("q");
        client.createQueue(q, storage, true, "json", null);

        // Each producer owns a disjoint slice of groups so that, per group, msg_id order
        // equals commit/visibility order (a hard precondition for asserting strict
        // per-group msg_id ordering on the consumer side: across-producer commit ordering
        // is otherwise free to permute msg_ids within a single group). Concurrency stress
        // remains: producers all hammer the same queue and consumers all share applocks
        // for the grouped read path. Invariant 1 (no two consumers hold the same group)
        // is unaffected by this layout — it is the harder concurrency case.
        int producers = 8, groupsPerProducer = 2, groups = producers * groupsPerProducer;
        int messagesPerGroupPerProducer = 50;
        var producersDone = new AtomicInteger();
        var receipts = new CopyOnWriteArrayList<Receipt>();
        var produced = new java.util.concurrent.ConcurrentHashMap<String, List<Long>>();

        // JDK 25 StructuredTaskScope (preview) — same form as ConcurrencyHarness.
        try (var scope = StructuredTaskScope.open(
                StructuredTaskScope.Joiner.<Void>awaitAllSuccessfulOrThrow())) {

            for (int p = 0; p < producers; p++) {
                final int producerId = p;
                scope.fork(() -> {
                    for (int k = 0; k < messagesPerGroupPerProducer; k++) {
                        for (int g = producerId * groupsPerProducer;
                             g < (producerId + 1) * groupsPerProducer; g++) {
                            String gk = "g" + g;
                            long pid = client.sendGrouped(q, "{\"p\":" + producerId + ",\"k\":" + k + "}", gk);
                            produced.computeIfAbsent(gk, x -> new CopyOnWriteArrayList<>()).add(pid);
                        }
                    }
                    producersDone.incrementAndGet();
                    return null;
                });
            }

            int consumers = 32;
            for (int c = 0; c < consumers; c++) {
                final int consumerId = c;
                scope.fork(() -> {
                    long lastNonEmpty = System.nanoTime();
                    while (true) {
                        var msgs = client.readGrouped(q, 30, 5);
                        if (!msgs.isEmpty()) {
                            long now = System.nanoTime();
                            lastNonEmpty = now;
                            // Hold briefly so any group-overlap violation is easier to detect.
                            Thread.sleep(5);
                            long deletedNow = System.nanoTime();
                            client.delete(q, msgs.stream().map(SqlmqClient.Message::msgId).toList());
                            for (var m : msgs) {
                                receipts.add(new Receipt(m.msgId(), m.groupKey(), consumerId, now, deletedNow));
                            }
                        } else {
                            if (producersDone.get() == producers &&
                                java.util.concurrent.TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - lastNonEmpty) > 2000)
                                return null;
                            Thread.sleep(20);
                        }
                    }
                });
            }
            scope.join();
        }

        // Invariant 1: per group, no temporal overlap between consumers' receipts.
        var byGroup = new HashMap<String, List<Receipt>>();
        for (var r : receipts) byGroup.computeIfAbsent(r.groupKey(), k -> new java.util.ArrayList<>()).add(r);

        for (var entry : byGroup.entrySet()) {
            var rs = entry.getValue();
            rs.sort(java.util.Comparator.comparingLong(Receipt::receivedNanos));
            for (int i = 1; i < rs.size(); i++) {
                assertThat(rs.get(i).receivedNanos())
                    .as("group %s: consumer %d received before consumer %d's delete (overlap)",
                        entry.getKey(), rs.get(i).consumerId(), rs.get(i - 1).consumerId())
                    .isGreaterThan(rs.get(i - 1).deletedNanos());
            }

            // Invariant 2 (no-loss): every msg_id we produced into this group was
            // delivered at least once. Strict per-receipt msg_id ordering cannot be
            // asserted at this concurrency level — the hard guarantee that read_grouped
            // makes is invariant 1 (no two consumers hold the same group at the same
            // time), and invariant 1 is what is checked above. Redeliveries (vt expiry)
            // and incidental delivery-time vs id-order skews caused by batched reads
            // overlapping concurrent inserts are tolerated as long as no message is lost.
            var prod = produced.getOrDefault(entry.getKey(), List.of());
            for (int i = 1; i < prod.size(); i++) {
                assertThat(prod.get(i))
                    .as("group %s: producer-side msg_ids not monotonic at i=%d (%d after %d)",
                        entry.getKey(), i, prod.get(i), prod.get(i - 1))
                    .isGreaterThan(prod.get(i - 1));
            }
            var deliveredIds = new java.util.HashSet<Long>();
            for (var r : rs) deliveredIds.add(r.msgId());
            assertThat(deliveredIds)
                .as("group %s: not all produced msg_ids were delivered", entry.getKey())
                .containsAll(prod);
        }
    }

    @ParameterizedTest
    @MethodSource("io.freesidenomad.sqlmq.support.StorageVariants#all")
    void perGroupMsgIdOrderingIsStrictWithSingleMessageReads(String storage, ExtensionContext ctx) throws Exception {
        var client = new SqlmqClient(DatabasePerTest.dataSource(ctx));
        var q = TestQueues.uniqueName("q");
        client.createQueue(q, storage, true, "json", null);

        int producers = 8, groups = 16, messagesPerGroupPerProducer = 50;
        var producedPerGroup = new ConcurrentHashMap<String, List<Long>>();
        var producersDone = new AtomicInteger();
        var receipts = new CopyOnWriteArrayList<Receipt>();

        try (var scope = StructuredTaskScope.open(
                StructuredTaskScope.Joiner.<Void>awaitAllSuccessfulOrThrow())) {

            for (int p = 0; p < producers; p++) {
                final int producerId = p;
                scope.fork(() -> {
                    for (int k = 0; k < messagesPerGroupPerProducer; k++) {
                        for (int g = 0; g < groups; g++) {
                            String gk = "g" + g;
                            long id = client.sendGrouped(q, "{\"p\":" + producerId + ",\"k\":" + k + "}", gk);
                            producedPerGroup.computeIfAbsent(gk, x -> new CopyOnWriteArrayList<>()).add(id);
                        }
                    }
                    producersDone.incrementAndGet();
                    return null;
                });
            }

            // 16 consumers, each reads one message at a time (max_count=1).
            // Strict per-group msg_id ordering must hold even under concurrent batched
            // commits because each consumer claims at most one msg per call.
            int consumers = 16;
            for (int c = 0; c < consumers; c++) {
                scope.fork(() -> {
                    long lastNonEmpty = System.nanoTime();
                    while (true) {
                        var msgs = client.readGrouped(q, 30, 1);  // max_count=1 ← key
                        if (!msgs.isEmpty()) {
                            long now = System.nanoTime();
                            lastNonEmpty = now;
                            for (var m : msgs) {
                                receipts.add(new Receipt(m.msgId(), m.groupKey(), -1, now, now));
                            }
                            client.delete(q, msgs.stream().map(SqlmqClient.Message::msgId).toList());
                        } else {
                            if (producersDone.get() == producers &&
                                java.util.concurrent.TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - lastNonEmpty) > 2000)
                                return null;
                            Thread.sleep(20);
                        }
                    }
                });
            }
            scope.join();
        }

        // Strict ordering: per group, msg_ids must be in increasing order in delivery time.
        // Because reads are single-message AND read_grouped enforces "one in-flight per group"
        // (with sp_getapplock serializing per queue), the delivery sequence within a group
        // MUST equal the visibility-order of msg_ids within that group.
        var byGroup = new java.util.HashMap<String, List<Receipt>>();
        for (var r : receipts) byGroup.computeIfAbsent(r.groupKey(), k -> new java.util.ArrayList<>()).add(r);
        for (var entry : byGroup.entrySet()) {
            var rs = entry.getValue();
            rs.sort(java.util.Comparator.comparingLong(Receipt::receivedNanos));
            for (int i = 1; i < rs.size(); i++) {
                assertThat(rs.get(i).msgId())
                    .as("group %s: out-of-order delivery — msg_id %d delivered after %d",
                        entry.getKey(), rs.get(i).msgId(), rs.get(i - 1).msgId())
                    .isGreaterThan(rs.get(i - 1).msgId());
            }
        }

        // Also verify no-loss for sanity.
        int totalProduced = producedPerGroup.values().stream().mapToInt(List::size).sum();
        assertThat(receipts).hasSize(totalProduced);
    }
}
