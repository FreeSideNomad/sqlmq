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
import java.sql.SQLException;

import static org.assertj.core.api.Assertions.assertThat;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@ExtendWith(DatabasePerTest.class)
class DeleteArchivePopTest {

    @Test
    void deleteRemovesMessages(ExtensionContext ctx) throws SQLException {
        var client = new SqlmqClient(DatabasePerTest.dataSource(ctx));
        var q = TestQueues.uniqueName("q");
        client.createQueue(q, false, "json", null);

        client.send(q, "{\"v\":1}", null);
        client.send(q, "{\"v\":2}", null);
        var msgs = client.read(q, 60, 10);
        var deleted = client.delete(q, msgs.stream().map(SqlmqClient.Message::msgId).toList());
        assertThat(deleted).isEqualTo(2);
        assertThat(rowCount(ctx, "q_" + q)).isZero();
    }

    @Test
    void archiveMovesToArchiveTableWithReason(ExtensionContext ctx) throws SQLException {
        var client = new SqlmqClient(DatabasePerTest.dataSource(ctx));
        var q = TestQueues.uniqueName("q");
        client.createQueue(q, false, "json", null);

        client.send(q, "{\"v\":1}", null);
        var msgs = client.read(q, 60, 10);
        var ids = msgs.stream().map(SqlmqClient.Message::msgId).toList();
        var archived = client.archive(q, ids, "test_reason");
        assertThat(archived).isEqualTo(1);

        try (var c = DatabasePerTest.dataSource(ctx).getConnection();
             PreparedStatement ps = c.prepareStatement(
                 "SELECT dlq_reason FROM sqlmq.[a_" + q + "] WHERE msg_id = ?")) {
            ps.setLong(1, ids.get(0));
            try (ResultSet rs = ps.executeQuery()) {
                assertThat(rs.next()).isTrue();
                assertThat(rs.getString("dlq_reason")).isEqualTo("test_reason");
            }
        }
        assertThat(rowCount(ctx, "q_" + q)).isZero();
    }

    @Test
    void popReturnsAndDeletesAtomically(ExtensionContext ctx) throws SQLException {
        var client = new SqlmqClient(DatabasePerTest.dataSource(ctx));
        var q = TestQueues.uniqueName("q");
        client.createQueue(q, false, "json", null);

        var id1 = client.send(q, "{\"v\":1}", null);
        client.send(q, "{\"v\":2}", null);

        var popped = client.pop(q);
        assertThat(popped).isPresent();
        assertThat(popped.get().msgId()).isEqualTo(id1);
        assertThat(rowCount(ctx, "q_" + q)).isEqualTo(1);
    }

    @Test
    void popOnEmptyReturnsEmpty(ExtensionContext ctx) throws SQLException {
        var client = new SqlmqClient(DatabasePerTest.dataSource(ctx));
        var q = TestQueues.uniqueName("q");
        client.createQueue(q, false, "json", null);
        assertThat(client.pop(q)).isEmpty();
    }

    private int rowCount(ExtensionContext ctx, String table) throws SQLException {
        try (var c = DatabasePerTest.dataSource(ctx).getConnection();
             var st = c.createStatement();
             ResultSet rs = st.executeQuery("SELECT COUNT(*) AS n FROM sqlmq.[" + table + "]")) {
            rs.next();
            return rs.getInt("n");
        }
    }
}
