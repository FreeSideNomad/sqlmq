package io.freesidenomad.sqlmq.concurrency;

import io.freesidenomad.sqlmq.client.SqlmqClient;
import io.freesidenomad.sqlmq.bench.ConcurrencyHarness;
import io.freesidenomad.sqlmq.support.DatabasePerTest;
import io.freesidenomad.sqlmq.support.TestQueues;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.HashMap;

import static org.assertj.core.api.Assertions.assertThat;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@ExtendWith(DatabasePerTest.class)
class FifoOrderingTest {

    @ParameterizedTest
    @MethodSource("io.freesidenomad.sqlmq.support.StorageVariants#all")
    void anySingleConsumersDeliveriesAreInIncreasingMsgIdOrder(String storage, ExtensionContext ctx) throws Exception {
        var client = new SqlmqClient(DatabasePerTest.dataSource(ctx));
        var q = TestQueues.uniqueName("q");
        client.createQueue(q, storage, false, "json", null);

        var p = new ConcurrencyHarness.Profile(4, 8, 1000, 1, 30, 1000); // batch=1 so per-consumer ordering is meaningful
        var result = ConcurrencyHarness.run(client, q, p);

        var perConsumer = new HashMap<Integer, java.util.List<Long>>();
        for (var d : result.deliveries())
            perConsumer.computeIfAbsent(d.consumerId(), k -> new java.util.ArrayList<>()).add(d.msgId());

        for (var entry : perConsumer.entrySet()) {
            var ids = entry.getValue();
            for (int i = 1; i < ids.size(); i++) {
                assertThat(ids.get(i))
                    .as("consumer %d received msg_id %d after %d (out of FIFO order)",
                        entry.getKey(), ids.get(i), ids.get(i - 1))
                    .isGreaterThan(ids.get(i - 1));
            }
        }
    }
}
