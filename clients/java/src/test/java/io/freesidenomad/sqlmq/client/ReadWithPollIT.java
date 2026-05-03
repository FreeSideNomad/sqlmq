package io.freesidenomad.sqlmq.client;

import io.freesidenomad.sqlmq.client.support.TestContainerFixture;
import io.freesidenomad.sqlmq.client.support.TestQueues;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import javax.sql.DataSource;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** {@code read_with_poll} client-side polling. */
@ExtendWith(TestContainerFixture.class)
class ReadWithPollIT {

    private PgmqClient q;

    @BeforeAll
    void init(DataSource ds) {
        q = new PgmqClient(ds);
    }

    @Test
    void readWithPollReturnsImmediatelyWhenMessagePresent() {
        var name = TestQueues.uniqueName("q");
        q.createQueue(name);
        q.send(name, Map.of("a", 1));
        long t0 = System.nanoTime();
        var msgs = q.readWithPoll(name, 30, 1, 5, 100);
        long elapsedMs = (System.nanoTime() - t0) / 1_000_000L;
        assertThat(msgs).hasSize(1);
        assertThat(msgs.get(0).message()).containsEntry("a", 1);
        assertThat(elapsedMs).isLessThan(1_000);
    }

    @Test
    void readWithPollReturnsEmptyAfterDeadline() {
        var name = TestQueues.uniqueName("q");
        q.createQueue(name);
        long t0 = System.nanoTime();
        var msgs = q.readWithPoll(name, 30, 1, 1, 100);
        long elapsedMs = (System.nanoTime() - t0) / 1_000_000L;
        assertThat(msgs).isEmpty();
        assertThat(elapsedMs).isBetween(900L, 2_500L);
    }

    @Test
    void readWithPollPicksUpLateArrival() throws Exception {
        var name = TestQueues.uniqueName("q");
        q.createQueue(name);

        // Producer fires after 500ms on a separate thread (simulating an
        // independent producer process arriving mid-poll).
        var producer = q;
        new Thread(() -> {
            try {
                Thread.sleep(500);
                producer.send(name, Map.of("late", true));
            } catch (InterruptedException ignored) {
            }
        }, "late-producer").start();

        var msgs = q.readWithPoll(name, 30, 1, 5, 100);
        assertThat(msgs).hasSize(1);
        assertThat(msgs.get(0).message()).containsEntry("late", true);
    }
}
