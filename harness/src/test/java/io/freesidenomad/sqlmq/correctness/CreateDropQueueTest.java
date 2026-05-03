package io.freesidenomad.sqlmq.correctness;

import io.freesidenomad.sqlmq.client.SqlmqClient;
import io.freesidenomad.sqlmq.support.DatabasePerTest;
import io.freesidenomad.sqlmq.support.TestQueues;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.ExtensionContext;

import javax.sql.DataSource;
import java.sql.SQLException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@ExtendWith(DatabasePerTest.class)
class CreateDropQueueTest {

    @Test
    void createsAndListsOnDiskJsonQueue(ExtensionContext ctx) throws SQLException {
        DataSource ds = DatabasePerTest.dataSource(ctx);
        var client = new SqlmqClient(ds);
        var name = TestQueues.uniqueName("q");

        client.createQueue(name, false, "json", null);

        var listed = client.listQueues();
        assertThat(listed).extracting(SqlmqClient.QueueInfo::name).contains(name);
        var info = listed.stream().filter(q -> q.name().equals(name)).findFirst().orElseThrow();
        assertThat(info.storageType()).isEqualTo("ondisk");
        assertThat(info.payloadType()).isEqualTo("json");
        assertThat(info.grouped()).isFalse();
        assertThat(info.maxDeliveryCount()).isNull();
    }

    @Test
    void createWithGroupingAndDlqCap(ExtensionContext ctx) throws SQLException {
        var client = new SqlmqClient(DatabasePerTest.dataSource(ctx));
        var name = TestQueues.uniqueName("q");
        client.createQueue(name, true, "binary", 5);
        var info = client.listQueues().stream()
            .filter(q -> q.name().equals(name)).findFirst().orElseThrow();
        assertThat(info.grouped()).isTrue();
        assertThat(info.payloadType()).isEqualTo("binary");
        assertThat(info.maxDeliveryCount()).isEqualTo(5);
    }

    @Test
    void inmemoryStorageRejectedAtSqlLayer(ExtensionContext ctx) throws SQLException {
        // V014 retired in-memory; V015 narrowed CK_sqlmq_meta_storage_type to
        // ondisk-only and re-CREATEd create_queue with an explicit THROW. The
        // Java client no longer exposes a @storage parameter, so we drive the
        // proc directly to exercise the SQL-layer guard.
        var ds = DatabasePerTest.dataSource(ctx);
        var name = TestQueues.uniqueName("q");
        try (var c = ds.getConnection();
             var cs = c.prepareCall("{call sqlmq.create_queue(?, ?, ?, ?, ?)}")) {
            cs.setString(1, name);
            cs.setString(2, "inmemory");
            cs.setBoolean(3, false);
            cs.setString(4, "json");
            cs.setNull(5, java.sql.Types.INTEGER);
            assertThatThrownBy(cs::execute)
                .hasMessageContaining("In-memory storage is not supported");
        }
    }

    @Test
    void duplicateCreateFails(ExtensionContext ctx) throws SQLException {
        var client = new SqlmqClient(DatabasePerTest.dataSource(ctx));
        var name = TestQueues.uniqueName("q");
        client.createQueue(name, false, "json", null);
        assertThatThrownBy(() -> client.createQueue(name, false, "json", null))
            .hasMessageContaining("already exists");
    }

    @Test
    void dropRemovesQueueAndTables(ExtensionContext ctx) throws SQLException {
        var ds = DatabasePerTest.dataSource(ctx);
        var client = new SqlmqClient(ds);
        var name = TestQueues.uniqueName("q");
        client.createQueue(name, false, "json", null);
        client.dropQueue(name);

        assertThat(client.listQueues()).extracting(SqlmqClient.QueueInfo::name).doesNotContain(name);

        try (var c = ds.getConnection(); var st = c.createStatement();
             var rs = st.executeQuery(
                 "SELECT COUNT(*) AS n FROM sys.tables WHERE name IN ('q_" + name + "', 'a_" + name + "')")) {
            rs.next();
            assertThat(rs.getInt("n")).isEqualTo(0);
        }
    }

    @Test
    void dropOfNonexistentFails(ExtensionContext ctx) {
        var client = new SqlmqClient(DatabasePerTest.dataSource(ctx));
        assertThatThrownBy(() -> client.dropQueue(TestQueues.uniqueName("absent")))
            .hasMessageContaining("does not exist");
    }

    @Test
    void rejectsEmptyName(ExtensionContext ctx) {
        var client = new SqlmqClient(DatabasePerTest.dataSource(ctx));
        assertThatThrownBy(() -> client.createQueue("", false, "json", null))
            .hasMessageContaining("@name must be non-empty");
    }

    @Test
    void rejectsNameWithSqlInjectionAttempt(ExtensionContext ctx) {
        var client = new SqlmqClient(DatabasePerTest.dataSource(ctx));
        assertThatThrownBy(() -> client.createQueue("foo]; DROP TABLE sqlmq.meta; --",
            false, "json", null))
            .hasMessageContaining("may only contain letters, digits, and underscores");
    }

    @Test
    void rejectsNameTooLong(ExtensionContext ctx) {
        var client = new SqlmqClient(DatabasePerTest.dataSource(ctx));
        var longName = "q".repeat(61);
        assertThatThrownBy(() -> client.createQueue(longName, false, "json", null))
            .hasMessageContaining("60 characters or fewer");
    }

    @Test
    void rejectsNameStartingWithDigit(ExtensionContext ctx) {
        var client = new SqlmqClient(DatabasePerTest.dataSource(ctx));
        assertThatThrownBy(() -> client.createQueue("1foo", false, "json", null))
            .hasMessageContaining("must start with a letter or underscore");
    }

    @Test
    void dropQueueRecoversOrphanedTables(ExtensionContext ctx) throws SQLException {
        var ds = DatabasePerTest.dataSource(ctx);
        var client = new SqlmqClient(ds);
        var name = TestQueues.uniqueName("q");
        // Simulate orphan: create the queue, then manually delete the meta row to mimic
        // a half-failed create.
        client.createQueue(name, false, "json", null);
        try (var c = ds.getConnection(); var st = c.createStatement()) {
            st.executeUpdate("DELETE FROM sqlmq.meta WHERE queue_name = '" + name + "'");
        }
        // drop_queue should now succeed and clean up the orphaned tables.
        client.dropQueue(name);
        try (var c = ds.getConnection(); var st = c.createStatement();
             var rs = st.executeQuery(
                 "SELECT COUNT(*) AS n FROM sys.tables WHERE name IN ('q_" + name + "', 'a_" + name + "')")) {
            rs.next();
            assertThat(rs.getInt("n")).isZero();
        }
    }
}
