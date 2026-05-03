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

/** {@code purge}. */
@ExtendWith(TestContainerFixture.class)
class PurgeIT {

    private PgmqClient q;

    @BeforeAll
    void init(DataSource ds) {
        q = new PgmqClient(ds);
    }

    @Test
    void purgeRemovesAllMessages() {
        var name = TestQueues.uniqueName("q");
        q.createQueue(name);
        var msgs = new ArrayList<Map<String, Object>>();
        for (int i = 0; i < 5; i++) msgs.add(Map.of("i", i));
        q.sendBatch(name, msgs);
        assertThat(q.purge(name)).isEqualTo(5);
        assertThat(q.read(name)).isEmpty();
    }

    @Test
    void purgeEmptyQueueReturnsZero() {
        var name = TestQueues.uniqueName("q");
        q.createQueue(name);
        assertThat(q.purge(name)).isEqualTo(0);
    }
}
