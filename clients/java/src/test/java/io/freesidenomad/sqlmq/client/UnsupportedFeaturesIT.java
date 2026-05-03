package io.freesidenomad.sqlmq.client;

import io.freesidenomad.sqlmq.client.support.TestContainerFixture;
import io.freesidenomad.sqlmq.client.support.TestQueues;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import javax.sql.DataSource;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** pgmq features sqlmq does not support throw {@link UnsupportedOperationException}. */
@ExtendWith(TestContainerFixture.class)
class UnsupportedFeaturesIT {

    private PgmqClient q;

    @BeforeAll
    void init(DataSource ds) {
        q = new PgmqClient(ds);
    }

    @Test
    void unloggedCreateQueueThrows() {
        assertThatThrownBy(() -> q.createQueue(TestQueues.uniqueName("q"), true))
            .isInstanceOf(UnsupportedOperationException.class)
            .satisfies(t -> {
                String msg = t.getMessage().toLowerCase();
                assert msg.contains("unlogged") : msg;
                // Should reference V014 or rationale, mirroring Python's wording.
                assert msg.contains("v014") || msg.contains("rationale") : msg;
            });
    }

    @Test
    void createPartitionedQueueThrows() {
        assertThatThrownBy(() -> q.createPartitionedQueue(TestQueues.uniqueName("q")))
            .isInstanceOf(UnsupportedOperationException.class)
            .hasMessageContaining("partition");
    }

    @Test
    void setVtThrows() {
        var name = TestQueues.uniqueName("q");
        q.createQueue(name);
        long id = q.send(name, Map.of("a", 1));
        assertThatThrownBy(() -> q.setVt(name, id, 60))
            .isInstanceOf(UnsupportedOperationException.class)
            .hasMessageContaining("set_vt");
    }

    @Test
    void detachArchiveThrows() {
        var name = TestQueues.uniqueName("q");
        q.createQueue(name);
        assertThatThrownBy(() -> q.detachArchive(name))
            .isInstanceOf(UnsupportedOperationException.class)
            .hasMessageContaining("detach_archive");
    }
}
