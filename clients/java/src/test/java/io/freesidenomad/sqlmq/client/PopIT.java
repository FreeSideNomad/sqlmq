package io.freesidenomad.sqlmq.client;

import io.freesidenomad.sqlmq.client.support.TestContainerFixture;
import io.freesidenomad.sqlmq.client.support.TestQueues;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import javax.sql.DataSource;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** {@code pop}: read+delete in one atomic step. */
@ExtendWith(TestContainerFixture.class)
class PopIT {

    private PgmqClient q;

    @BeforeAll
    void init(DataSource ds) {
        q = new PgmqClient(ds);
    }

    @Test
    void popReturnsMessageAndRemoves() {
        var name = TestQueues.uniqueName("q");
        q.createQueue(name);
        q.send(name, Map.of("a", 1));
        var popped = q.pop(name);
        assertThat(popped).isPresent();
        assertThat(popped.get().message()).containsEntry("a", 1);
        assertThat(q.read(name)).isEmpty();
    }

    @Test
    void popReturnsEmptyWhenEmpty() {
        var name = TestQueues.uniqueName("q");
        q.createQueue(name);
        assertThat(q.pop(name)).isEmpty();
    }
}
