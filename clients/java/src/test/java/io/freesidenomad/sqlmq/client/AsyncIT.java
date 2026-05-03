package io.freesidenomad.sqlmq.client;

import io.freesidenomad.sqlmq.client.support.TestContainerFixture;
import io.freesidenomad.sqlmq.client.support.TestQueues;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import javax.sql.DataSource;
import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Async smoke tests mirroring the sync coverage. */
@ExtendWith(TestContainerFixture.class)
class AsyncIT {

    private AsyncPgmqClient q;

    @BeforeAll
    void init(DataSource ds) {
        q = new AsyncPgmqClient(ds);
    }

    private static <T> T await(java.util.concurrent.CompletableFuture<T> f)
            throws InterruptedException, ExecutionException, TimeoutException {
        return f.get(15, TimeUnit.SECONDS);
    }

    @Test
    void asyncCreateDrop() throws Exception {
        var name = TestQueues.uniqueName("q");
        await(q.createQueue(name));
        assertThat(await(q.listQueues())).contains(name);
        assertThat(await(q.dropQueue(name))).isTrue();
        assertThat(await(q.dropQueue(name))).isFalse();
    }

    @Test
    void asyncSendRead() throws Exception {
        var name = TestQueues.uniqueName("q");
        await(q.createQueue(name));
        long id = await(q.send(name, Map.of("a", 1)));
        assertThat(id).isGreaterThanOrEqualTo(1);
        var got = await(q.read(name));
        assertThat(got).isPresent();
        assertThat(got.get().message()).containsEntry("a", 1);
    }

    @Test
    void asyncSendBatchReadBatch() throws Exception {
        var name = TestQueues.uniqueName("q");
        await(q.createQueue(name));
        var msgs = new ArrayList<Map<String, Object>>();
        for (int i = 0; i < 3; i++) msgs.add(Map.of("i", i));
        var ids = await(q.sendBatch(name, msgs));
        assertThat(ids).hasSize(3);
        var got = await(q.readBatch(name, 30, 10));
        assertThat(got).hasSize(3);
    }

    @Test
    void asyncPop() throws Exception {
        var name = TestQueues.uniqueName("q");
        await(q.createQueue(name));
        await(q.send(name, Map.of("x", 1)));
        var popped = await(q.pop(name));
        assertThat(popped).isPresent();
        assertThat(popped.get().message()).containsEntry("x", 1);
        assertThat(await(q.pop(name))).isEmpty();
    }

    @Test
    void asyncDeleteArchive() throws Exception {
        var name = TestQueues.uniqueName("q");
        await(q.createQueue(name));
        long a = await(q.send(name, Map.of("i", 1)));
        long b = await(q.send(name, Map.of("i", 2)));
        assertThat(await(q.delete(name, a))).isTrue();
        assertThat(await(q.archive(name, b))).isTrue();
    }

    @Test
    void asyncPurgeMetrics() throws Exception {
        var name = TestQueues.uniqueName("q");
        await(q.createQueue(name));
        var msgs = new ArrayList<Map<String, Object>>();
        for (int i = 0; i < 3; i++) msgs.add(Map.of("i", i));
        await(q.sendBatch(name, msgs));
        var m = await(q.metrics(name));
        assertThat(m.queueLength()).isEqualTo(3);
        assertThat(await(q.purge(name))).isEqualTo(3);
    }

    @Test
    void asyncReadWithPollReturnsEmptyAfterDeadline() throws Exception {
        var name = TestQueues.uniqueName("q");
        await(q.createQueue(name));
        long t0 = System.nanoTime();
        var got = await(q.readWithPoll(name, 30, 1, 1, 100));
        long elapsedMs = (System.nanoTime() - t0) / 1_000_000L;
        assertThat(got).isEmpty();
        assertThat(elapsedMs).isLessThan(2_500);
    }

    @Test
    void asyncReadWithPollPicksUpLateArrival() throws Exception {
        var name = TestQueues.uniqueName("q");
        await(q.createQueue(name));
        new Thread(() -> {
            try {
                Thread.sleep(500);
                q.send(name, Map.of("late", true)).get(5, TimeUnit.SECONDS);
            } catch (Exception ignored) {
            }
        }, "late-async-producer").start();
        var got = await(q.readWithPoll(name, 30, 1, 5, 100));
        assertThat(got).hasSize(1);
        assertThat(got.get(0).message()).containsEntry("late", true);
    }

    @Test
    void asyncUnsupportedMethodsThrow() throws Exception {
        var name = TestQueues.uniqueName("q");
        await(q.createQueue(name));

        // CompletableFuture wraps the cause in ExecutionException.
        assertThatThrownBy(() -> await(q.createQueue(TestQueues.uniqueName("q"), true)))
            .isInstanceOf(ExecutionException.class)
            .hasCauseInstanceOf(UnsupportedOperationException.class);

        assertThatThrownBy(() -> await(q.createPartitionedQueue(TestQueues.uniqueName("q"))))
            .isInstanceOf(ExecutionException.class)
            .hasCauseInstanceOf(UnsupportedOperationException.class);

        assertThatThrownBy(() -> await(q.setVt(name, 1L, 60)))
            .isInstanceOf(ExecutionException.class)
            .hasCauseInstanceOf(UnsupportedOperationException.class);

        assertThatThrownBy(() -> await(q.detachArchive(name)))
            .isInstanceOf(ExecutionException.class)
            .hasCauseInstanceOf(UnsupportedOperationException.class);
    }
}
