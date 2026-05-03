package io.freesidenomad.sqlmq.client;

import io.freesidenomad.sqlmq.client.support.TestContainerFixture;
import io.freesidenomad.sqlmq.client.support.TestQueues;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import javax.sql.DataSource;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** {@code send}, {@code send_batch}, {@code read}, {@code read_batch}. */
@ExtendWith(TestContainerFixture.class)
class SendReadIT {

    private PgmqClient q;

    @BeforeAll
    void init(DataSource ds) {
        q = new PgmqClient(ds);
    }

    @Test
    void sendReturnsMsgId() {
        var name = TestQueues.uniqueName("q");
        q.createQueue(name);
        long id = q.send(name, Map.of("hello", "world"));
        assertThat(id).isGreaterThanOrEqualTo(1);
    }

    @Test
    void readReturnsMessage() {
        var name = TestQueues.uniqueName("q");
        q.createQueue(name);
        long id = q.send(name, Map.of("x", 1, "nested", Map.of("y", 2)));
        var got = q.read(name);
        assertThat(got).isPresent();
        var m = got.get();
        assertThat(m.msgId()).isEqualTo(id);
        assertThat(m.readCt()).isEqualTo(1);
        assertThat(m.message()).containsEntry("x", 1);
        @SuppressWarnings("unchecked")
        var nested = (Map<String, Object>) m.message().get("nested");
        assertThat(nested).containsEntry("y", 2);
    }

    @Test
    void readReturnsEmptyForEmptyQueue() {
        var name = TestQueues.uniqueName("q");
        q.createQueue(name);
        assertThat(q.read(name)).isEmpty();
    }

    @Test
    void sendBatchReturnsIds() {
        var name = TestQueues.uniqueName("q");
        q.createQueue(name);
        var msgs = new ArrayList<Map<String, Object>>();
        for (int i = 0; i < 5; i++) msgs.add(Map.of("i", i));
        var ids = q.sendBatch(name, msgs);
        assertThat(ids).hasSize(5);
        // Monotonic — IDs are an IDENTITY column.
        var sorted = new ArrayList<>(ids);
        java.util.Collections.sort(sorted);
        assertThat(ids).isEqualTo(sorted);
    }

    @Test
    void sendBatchEmptyReturnsEmpty() {
        var name = TestQueues.uniqueName("q");
        q.createQueue(name);
        assertThat(q.sendBatch(name, List.of())).isEmpty();
    }

    @Test
    void readBatchReturnsMultiple() {
        var name = TestQueues.uniqueName("q");
        q.createQueue(name);
        var msgs = new ArrayList<Map<String, Object>>();
        for (int i = 0; i < 3; i++) msgs.add(Map.of("i", i));
        q.sendBatch(name, msgs);
        var got = q.readBatch(name, 30, 10);
        assertThat(got).hasSize(3);
        var values = got.stream()
            .map(m -> ((Number) m.message().get("i")).intValue())
            .sorted().toList();
        assertThat(values).containsExactly(0, 1, 2);
    }

    @Test
    void readBatchEmptyReturnsEmptyList() {
        var name = TestQueues.uniqueName("q");
        q.createQueue(name);
        assertThat(q.readBatch(name, 30, 5)).isEmpty();
    }

    @Test
    void sendWithDelayMakesMessageInvisibleBriefly() throws InterruptedException {
        var name = TestQueues.uniqueName("q");
        q.createQueue(name);
        q.send(name, Map.of("a", 1), /*delaySeconds=*/ 2);
        assertThat(q.read(name)).isEmpty();  // still invisible

        Thread.sleep(2_500);
        var msg = q.read(name);
        assertThat(msg).isPresent();
        assertThat(msg.get().message()).containsEntry("a", 1);
    }
}
