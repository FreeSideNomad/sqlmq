package io.freesidenomad.sqlmq.client;

import io.freesidenomad.sqlmq.client.support.TestContainerFixture;
import io.freesidenomad.sqlmq.client.support.TestQueues;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import javax.sql.DataSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** {@code create_queue}, {@code drop_queue}, {@code list_queues}, {@code validate_queue_name}. */
@ExtendWith(TestContainerFixture.class)
class CreateDropQueueIT {

    private PgmqClient q;

    @BeforeAll
    void init(DataSource ds) {
        q = new PgmqClient(ds);
    }

    @Test
    void createThenList() {
        var name = TestQueues.uniqueName("q");
        q.createQueue(name);
        assertThat(q.listQueues()).contains(name);
    }

    @Test
    void dropReturnsTrueWhenPresent() {
        var name = TestQueues.uniqueName("q");
        q.createQueue(name);
        assertThat(q.dropQueue(name)).isTrue();
        assertThat(q.listQueues()).doesNotContain(name);
    }

    @Test
    void dropReturnsFalseWhenAbsent() {
        var name = TestQueues.uniqueName("q");
        assertThat(q.dropQueue(name)).isFalse();
    }

    // Note: the formerly-passing dropPartitionedKwargIsIgnored test has moved to
    // UnsupportedFeaturesIT.dropQueuePartitionedTrueRejected — sqlmq now rejects
    // dropQueue(name, true) with UnsupportedOperationException to avoid masking
    // caller intent.

    @Test
    void validateQueueNameAcceptsLegal() {
        for (String name : new String[]{"a", "abc", "_x", "Q1", "abc_123", "x".repeat(60)}) {
            q.validateQueueName(name);  // no throw
        }
    }

    @Test
    void validateQueueNameRejectsIllegal() {
        for (String bad : new String[]{"", "1abc", "ab-cd", "ab.cd", "x".repeat(61), "drop table"}) {
            assertThatThrownBy(() -> q.validateQueueName(bad))
                .isInstanceOf(IllegalArgumentException.class);
        }
    }

    @Test
    void createQueueUnloggedThrows() {
        assertThatThrownBy(() -> q.createQueue(TestQueues.uniqueName("q"), /*unlogged=*/ true))
            .isInstanceOf(UnsupportedOperationException.class)
            .hasMessageContaining("unlogged");
    }

    @Test
    void createPartitionedQueueThrows() {
        assertThatThrownBy(() -> q.createPartitionedQueue(TestQueues.uniqueName("q")))
            .isInstanceOf(UnsupportedOperationException.class)
            .hasMessageContaining("partition");
    }
}
