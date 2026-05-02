package io.freesidenomad.sqlmq.concurrency;

import io.freesidenomad.sqlmq.client.SqlmqClient;
import io.freesidenomad.sqlmq.support.ConcurrencyHarness;
import io.freesidenomad.sqlmq.support.DatabasePerTest;
import io.freesidenomad.sqlmq.support.TestQueues;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.ExtensionContext;

import static org.assertj.core.api.Assertions.assertThat;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@ExtendWith(DatabasePerTest.class)
class VtHonoredTest {

    @Test
    void noTwoConsumersHoldSameMsgSimultaneously(ExtensionContext ctx) throws Exception {
        var client = new SqlmqClient(DatabasePerTest.dataSource(ctx));
        var q = TestQueues.uniqueName("q");
        client.createQueue(q, "ondisk", false, "json", null);

        var profile = new ConcurrencyHarness.Profile(8, 16, 500, 5, 30, 1000);
        var result = ConcurrencyHarness.run(client, q, profile);

        var index = ConcurrencyHarness.indexByMsgId(result.deliveries());
        for (var entry : index.entrySet()) {
            var ds = entry.getValue();
            ds.sort(java.util.Comparator.comparingLong(ConcurrencyHarness.Delivery::receivedAtNanos));
            for (int i = 1; i < ds.size(); i++) {
                assertThat(ds.get(i).receivedAtNanos())
                    .as("msg_id %d redelivered to consumer %d before previous VT expiry",
                        entry.getKey(), ds.get(i).consumerId())
                    .isGreaterThan(ds.get(i - 1).vtExpiresAtNanos());
            }
        }
    }
}
