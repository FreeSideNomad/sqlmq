package io.freesidenomad.sqlmq.concurrency;

import io.freesidenomad.sqlmq.client.SqlmqClient;
import io.freesidenomad.sqlmq.bench.ConcurrencyHarness;
import io.freesidenomad.sqlmq.bench.ConcurrencyHarness.Profile;
import io.freesidenomad.sqlmq.support.DatabasePerTest;
import io.freesidenomad.sqlmq.support.TestQueues;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@ExtendWith(DatabasePerTest.class)
class NoLossNoDoubleDeliveryTest {

    static Stream<Arguments> profiles() {
        // heavy/asymmetric stay out of CI by default — opt in locally.
        return Stream.of(
            Arguments.of(Profile.light()),
            Arguments.of(Profile.medium())
        );
    }

    @ParameterizedTest
    @MethodSource("profiles")
    void everyProducedMessageIsDeliveredExactlyOnce(Profile p, ExtensionContext ctx) throws Exception {
        var client = new SqlmqClient(DatabasePerTest.dataSource(ctx));
        var q = TestQueues.uniqueName("q");
        client.createQueue(q, false, "json", null);

        var result = ConcurrencyHarness.run(client, q, p);

        assertThat(result.producerErrors().get()).isZero();
        assertThat(result.consumerErrors().get()).isZero();

        // No loss: every produced id appears in deliveries.
        var deliveredIds = new java.util.HashSet<Long>();
        for (var d : result.deliveries()) deliveredIds.add(d.msgId());
        assertThat(deliveredIds).containsExactlyInAnyOrderElementsOf(result.producedMsgIds());

        // No double-delivery: each msg_id appears exactly once across all consumers.
        var index = ConcurrencyHarness.indexByMsgId(result.deliveries());
        for (var entry : index.entrySet()) {
            assertThat(entry.getValue())
                .as("msg_id %d delivered multiple times: %s", entry.getKey(), entry.getValue())
                .hasSize(1);
        }
    }
}
