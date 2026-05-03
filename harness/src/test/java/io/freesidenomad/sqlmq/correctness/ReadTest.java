package io.freesidenomad.sqlmq.correctness;

import io.freesidenomad.sqlmq.client.SqlmqClient;
import io.freesidenomad.sqlmq.support.DatabasePerTest;
import io.freesidenomad.sqlmq.support.TestQueues;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.sql.SQLException;

import static org.assertj.core.api.Assertions.assertThat;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@ExtendWith(DatabasePerTest.class)
class ReadTest {

    @Test
    void emptyQueueReturnsEmptyList(ExtensionContext ctx) throws SQLException {
        var client = new SqlmqClient(DatabasePerTest.dataSource(ctx));
        var q = TestQueues.uniqueName("q");
        client.createQueue(q, false, "json", null);
        assertThat(client.read(q, 30, 10)).isEmpty();
    }

    @Test
    void readReturnsFifoOrderedMessagesAndIncrementsReadCt(ExtensionContext ctx) throws SQLException {
        var client = new SqlmqClient(DatabasePerTest.dataSource(ctx));
        var q = TestQueues.uniqueName("q");
        client.createQueue(q, false, "json", null);

        var id1 = client.send(q, "{\"v\":1}", null);
        var id2 = client.send(q, "{\"v\":2}", null);
        var id3 = client.send(q, "{\"v\":3}", null);

        var msgs = client.read(q, 60, 10);
        assertThat(msgs).extracting(SqlmqClient.Message::msgId).containsExactly(id1, id2, id3);
        assertThat(msgs).allSatisfy(m -> assertThat(m.readCt()).isEqualTo(1));
    }

    @Test
    void secondReadDoesNotReturnInflightMessages(ExtensionContext ctx) throws SQLException {
        var client = new SqlmqClient(DatabasePerTest.dataSource(ctx));
        var q = TestQueues.uniqueName("q");
        client.createQueue(q, false, "json", null);

        client.send(q, "{\"v\":1}", null);
        var first = client.read(q, 60, 10);
        var second = client.read(q, 60, 10);

        assertThat(first).hasSize(1);
        assertThat(second).isEmpty();
    }

    @Test
    void messagesBecomeReEligibleAfterVtExpires(ExtensionContext ctx) throws Exception {
        var client = new SqlmqClient(DatabasePerTest.dataSource(ctx));
        var q = TestQueues.uniqueName("q");
        client.createQueue(q, false, "json", null);

        client.send(q, "{\"v\":1}", null);
        var first = client.read(q, 1, 10);
        assertThat(first).hasSize(1);
        Thread.sleep(1500);
        var second = client.read(q, 60, 10);
        assertThat(second).hasSize(1);
        assertThat(second.get(0).readCt()).isEqualTo(2);
    }

    @Test
    void delayedSendNotReadableUntilDelayElapses(ExtensionContext ctx) throws Exception {
        var client = new SqlmqClient(DatabasePerTest.dataSource(ctx));
        var q = TestQueues.uniqueName("q");
        client.createQueue(q, false, "json", null);

        client.sendDelayed(q, "{\"v\":1}", null, 2);
        assertThat(client.read(q, 30, 10)).isEmpty();
        Thread.sleep(2200);
        assertThat(client.read(q, 30, 10)).hasSize(1);
    }
}
