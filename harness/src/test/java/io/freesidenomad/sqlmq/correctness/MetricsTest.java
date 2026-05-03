package io.freesidenomad.sqlmq.correctness;

import io.freesidenomad.sqlmq.client.SqlmqClient;
import io.freesidenomad.sqlmq.support.DatabasePerTest;
import io.freesidenomad.sqlmq.support.TestQueues;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import static org.assertj.core.api.Assertions.assertThat;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@ExtendWith(DatabasePerTest.class)
class MetricsTest {

    @ParameterizedTest
    @MethodSource("io.freesidenomad.sqlmq.support.StorageVariants#all")
    void emptyQueueMetrics(String storage, ExtensionContext ctx) throws Exception {
        var client = new SqlmqClient(DatabasePerTest.dataSource(ctx));
        var q = TestQueues.uniqueName("q");
        client.createQueue(q, storage, false, "json", null);

        var m = client.metrics(q);
        assertThat(m.queueName()).isEqualTo(q);
        assertThat(m.queueLength()).isZero();
        assertThat(m.totalMessages()).isZero();
        assertThat(m.oldestMsgAgeSeconds()).isNull();
        assertThat(m.dlqCount()).isZero();
    }

    @ParameterizedTest
    @MethodSource("io.freesidenomad.sqlmq.support.StorageVariants#all")
    void metricsAfterSends(String storage, ExtensionContext ctx) throws Exception {
        var client = new SqlmqClient(DatabasePerTest.dataSource(ctx));
        var q = TestQueues.uniqueName("q");
        client.createQueue(q, storage, false, "json", null);

        client.send(q, "{}", null);
        client.send(q, "{}", null);
        client.send(q, "{}", null);

        var m = client.metrics(q);
        assertThat(m.queueLength()).isEqualTo(3);
        assertThat(m.totalMessages()).isGreaterThanOrEqualTo(3);
    }

    @ParameterizedTest
    @MethodSource("io.freesidenomad.sqlmq.support.StorageVariants#all")
    void metricsAllListsAllQueues(String storage, ExtensionContext ctx) throws Exception {
        var client = new SqlmqClient(DatabasePerTest.dataSource(ctx));
        var q1 = TestQueues.uniqueName("q");
        var q2 = TestQueues.uniqueName("q");
        client.createQueue(q1, storage, false, "json", null);
        client.createQueue(q2, storage, false, "json", null);

        var all = client.metricsAll();
        assertThat(all).extracting(SqlmqClient.Metrics::queueName).contains(q1, q2);
    }
}
