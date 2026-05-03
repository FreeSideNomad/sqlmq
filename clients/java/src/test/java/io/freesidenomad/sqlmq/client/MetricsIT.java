package io.freesidenomad.sqlmq.client;

import io.freesidenomad.sqlmq.client.support.TestContainerFixture;
import io.freesidenomad.sqlmq.client.support.TestQueues;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import javax.sql.DataSource;
import java.util.ArrayList;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** {@code metrics}, {@code metrics_all}. */
@ExtendWith(TestContainerFixture.class)
class MetricsIT {

    private PgmqClient q;

    @BeforeAll
    void init(DataSource ds) {
        q = new PgmqClient(ds);
    }

    @Test
    void metricsForEmptyQueue() {
        var name = TestQueues.uniqueName("q");
        q.createQueue(name);
        var m = q.metrics(name);
        assertThat(m.queueName()).isEqualTo(name);
        assertThat(m.queueLength()).isEqualTo(0);
        assertThat(m.totalMessages()).isEqualTo(0);
        // Empty queue: MIN/MAX(enqueued_at) are NULL → both ages null.
        assertThat(m.newestMsgAgeSec()).isNull();
        assertThat(m.oldestMsgAgeSec()).isNull();
    }

    @Test
    void metricsAfterSends() {
        var name = TestQueues.uniqueName("q");
        q.createQueue(name);
        var msgs = new ArrayList<Map<String, Object>>();
        for (int i = 0; i < 3; i++) msgs.add(Map.of("i", i));
        q.sendBatch(name, msgs);
        var m = q.metrics(name);
        assertThat(m.queueLength()).isEqualTo(3);
        assertThat(m.totalMessages()).isEqualTo(3);
        // V016: newest_msg_age_sec is now populated. Tests run faster than a
        // minute, so it should be a small non-negative int.
        assertThat(m.newestMsgAgeSec()).isNotNull();
        assertThat(m.newestMsgAgeSec()).isGreaterThanOrEqualTo(0);
        assertThat(m.newestMsgAgeSec()).isLessThan(60);
    }

    @Test
    void metricsAllReturnsEveryQueue() {
        var a = TestQueues.uniqueName("a");
        var b = TestQueues.uniqueName("b");
        q.createQueue(a);
        q.createQueue(b);
        var names = q.metricsAll().stream().map(QueueMetrics::queueName).toList();
        assertThat(names).contains(a, b);
    }
}
