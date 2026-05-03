package io.freesidenomad.sqlmq.correctness;

import io.freesidenomad.sqlmq.client.SqlmqClient;
import io.freesidenomad.sqlmq.support.DatabasePerTest;
import io.freesidenomad.sqlmq.support.TestQueues;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.sql.SQLException;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@ExtendWith(DatabasePerTest.class)
class SendTest {

    @Test
    void sendReturnsMonotonicMsgIds(ExtensionContext ctx) throws SQLException {
        var client = new SqlmqClient(DatabasePerTest.dataSource(ctx));
        var q = TestQueues.uniqueName("q");
        client.createQueue(q, false, "json", null);

        var id1 = client.send(q, "{\"v\":1}", null);
        var id2 = client.send(q, "{\"v\":2}", null);
        var id3 = client.send(q, "{\"v\":3}", null);

        assertThat(id1).isPositive();
        assertThat(id2).isGreaterThan(id1);
        assertThat(id3).isGreaterThan(id2);
    }

    @Test
    void invalidJsonRejected(ExtensionContext ctx) throws SQLException {
        var client = new SqlmqClient(DatabasePerTest.dataSource(ctx));
        var q = TestQueues.uniqueName("q");
        client.createQueue(q, false, "json", null);

        assertThatThrownBy(() -> client.send(q, "not json at all", null))
            .hasMessageContaining("CK_q_" + q + "_message_json");
    }

    @Test
    void invalidJsonHeadersRejected(ExtensionContext ctx) throws SQLException {
        var client = new SqlmqClient(DatabasePerTest.dataSource(ctx));
        var q = TestQueues.uniqueName("q");
        client.createQueue(q, false, "json", null);

        assertThatThrownBy(() -> client.send(q, "{\"ok\":true}", "not json headers"))
            .hasMessageContaining("CK_q_" + q + "_headers_json");
    }

    @Test
    void binaryQueueAcceptsBytes(ExtensionContext ctx) throws SQLException {
        var client = new SqlmqClient(DatabasePerTest.dataSource(ctx));
        var q = TestQueues.uniqueName("q");
        client.createQueue(q, false, "binary", null);

        var id = client.sendBinary(q, new byte[]{1, 2, 3, 4, 5}, null);
        assertThat(id).isPositive();
    }

    @Test
    void batchSendReturnsAllMsgIdsInOrder(ExtensionContext ctx) throws SQLException {
        var client = new SqlmqClient(DatabasePerTest.dataSource(ctx));
        var q = TestQueues.uniqueName("q");
        client.createQueue(q, false, "json", null);

        var ids = client.sendBatch(q, List.of("{\"a\":1}", "{\"a\":2}", "{\"a\":3}"));
        assertThat(ids).hasSize(3);
        assertThat(ids).isSorted();
    }

    @Test
    void sendToNonexistentQueueFails(ExtensionContext ctx) {
        var client = new SqlmqClient(DatabasePerTest.dataSource(ctx));
        assertThatThrownBy(() -> client.send("nope", "{}", null))
            .hasMessageContaining("does not exist");
    }
}
