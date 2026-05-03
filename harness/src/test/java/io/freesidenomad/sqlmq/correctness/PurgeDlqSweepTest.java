package io.freesidenomad.sqlmq.correctness;

import io.freesidenomad.sqlmq.client.SqlmqClient;
import io.freesidenomad.sqlmq.support.DatabasePerTest;
import io.freesidenomad.sqlmq.support.TestQueues;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.sql.PreparedStatement;
import java.sql.ResultSet;

import static org.assertj.core.api.Assertions.assertThat;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@ExtendWith(DatabasePerTest.class)
class PurgeDlqSweepTest {

    @Test
    void purgeRemovesAllMessages(ExtensionContext ctx) throws Exception {
        var client = new SqlmqClient(DatabasePerTest.dataSource(ctx));
        var q = TestQueues.uniqueName("q");
        client.createQueue(q, false, "json", null);
        for (int i = 0; i < 10; i++) client.send(q, "{}", null);
        var purged = client.purgeQueue(q);
        assertThat(purged).isEqualTo(10);
        assertThat(client.metrics(q).queueLength()).isZero();
    }

    @Test
    void dlqSweepNoOpWhenMaxDeliveryCountNull(ExtensionContext ctx) throws Exception {
        var client = new SqlmqClient(DatabasePerTest.dataSource(ctx));
        var q = TestQueues.uniqueName("q");
        client.createQueue(q, false, "json", null);
        client.send(q, "{}", null);
        for (int i = 0; i < 5; i++) {
            var msgs = client.read(q, 1, 10);
            if (msgs.isEmpty()) Thread.sleep(1100);
        }
        var swept = client.dlqSweep(q);
        assertThat(swept).isZero();
    }

    @Test
    void dlqSweepMovesOverCapMessagesWithReason(ExtensionContext ctx) throws Exception {
        var client = new SqlmqClient(DatabasePerTest.dataSource(ctx));
        var q = TestQueues.uniqueName("q");
        client.createQueue(q, false, "json", 2);
        var id = client.send(q, "{}", null);

        for (int i = 0; i < 3; i++) {
            client.read(q, 1, 10);
            Thread.sleep(1100);
        }

        var swept = client.dlqSweep(q);
        assertThat(swept).isEqualTo(1);

        try (var c = DatabasePerTest.dataSource(ctx).getConnection();
             PreparedStatement ps = c.prepareStatement(
                 "SELECT dlq_reason FROM sqlmq.[a_" + q + "] WHERE msg_id = ?")) {
            ps.setLong(1, id);
            try (ResultSet rs = ps.executeQuery()) {
                assertThat(rs.next()).isTrue();
                assertThat(rs.getString("dlq_reason")).isEqualTo("max_deliveries");
            }
        }
    }

    @Test
    void readSkipsOverCapMessagesEvenBeforeSweep(ExtensionContext ctx) throws Exception {
        var client = new SqlmqClient(DatabasePerTest.dataSource(ctx));
        var q = TestQueues.uniqueName("q");
        client.createQueue(q, false, "json", 2);
        client.send(q, "{}", null);

        for (int i = 0; i < 2; i++) {
            client.read(q, 1, 10);
            Thread.sleep(1100);
        }
        Thread.sleep(1100);
        assertThat(client.read(q, 30, 10)).isEmpty();
    }
}
