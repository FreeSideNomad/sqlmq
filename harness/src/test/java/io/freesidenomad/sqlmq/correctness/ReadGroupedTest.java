package io.freesidenomad.sqlmq.correctness;

import io.freesidenomad.sqlmq.client.SqlmqClient;
import io.freesidenomad.sqlmq.support.DatabasePerTest;
import io.freesidenomad.sqlmq.support.TestQueues;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.ExtensionContext;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@ExtendWith(DatabasePerTest.class)
class ReadGroupedTest {

    @Test
    void readGroupedRejectsUngroupedQueue(ExtensionContext ctx) throws Exception {
        var client = new SqlmqClient(DatabasePerTest.dataSource(ctx));
        var q = TestQueues.uniqueName("q");
        client.createQueue(q, "ondisk", false, "json", null);
        assertThatThrownBy(() -> client.readGrouped(q, 30, 10))
            .hasMessageContaining("not created with grouping enabled");
    }

    @Test
    void groupedReadReturnsAtMostOnePerGroup(ExtensionContext ctx) throws Exception {
        var client = new SqlmqClient(DatabasePerTest.dataSource(ctx));
        var q = TestQueues.uniqueName("q");
        client.createQueue(q, "ondisk", true, "json", null);

        for (int g = 0; g < 3; g++)
            for (int i = 0; i < 5; i++)
                client.sendGrouped(q, "{\"g\":" + g + ",\"i\":" + i + "}", "g" + g);

        var first = client.readGrouped(q, 60, 10);
        assertThat(first).hasSize(3);
        assertThat(first.stream().map(SqlmqClient.Message::msgId).distinct().count()).isEqualTo(3);
    }

    @Test
    void groupedReadBlocksFurtherFromSameGroupUntilDeleteOrVtExpiry(ExtensionContext ctx) throws Exception {
        var client = new SqlmqClient(DatabasePerTest.dataSource(ctx));
        var q = TestQueues.uniqueName("q");
        client.createQueue(q, "ondisk", true, "json", null);
        client.sendGrouped(q, "{\"g\":0,\"i\":0}", "g0");
        client.sendGrouped(q, "{\"g\":0,\"i\":1}", "g0");

        var first = client.readGrouped(q, 60, 10);
        assertThat(first).hasSize(1);

        var second = client.readGrouped(q, 60, 10);
        assertThat(second).isEmpty();

        client.delete(q, java.util.List.of(first.get(0).msgId()));
        var third = client.readGrouped(q, 60, 10);
        assertThat(third).hasSize(1);
    }
}
