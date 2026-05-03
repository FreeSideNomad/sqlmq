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

/** {@code delete}, {@code delete_batch}, {@code archive}, {@code archive_batch}. */
@ExtendWith(TestContainerFixture.class)
class DeleteArchiveIT {

    private PgmqClient q;

    @BeforeAll
    void init(DataSource ds) {
        q = new PgmqClient(ds);
    }

    @Test
    void deleteReturnsTrueWhenPresent() {
        var name = TestQueues.uniqueName("q");
        q.createQueue(name);
        long id = q.send(name, Map.of("a", 1));
        assertThat(q.delete(name, id)).isTrue();
    }

    @Test
    void deleteReturnsFalseWhenAbsent() {
        var name = TestQueues.uniqueName("q");
        q.createQueue(name);
        assertThat(q.delete(name, 99999L)).isFalse();
    }

    @Test
    void deleteBatchReturnsActuallyDeletedIds() {
        var name = TestQueues.uniqueName("q");
        q.createQueue(name);
        var msgs = new ArrayList<Map<String, Object>>();
        for (int i = 0; i < 3; i++) msgs.add(Map.of("i", i));
        var ids = q.sendBatch(name, msgs);
        var bogus = new ArrayList<>(ids);
        bogus.add(99999L);
        var deleted = q.deleteBatch(name, bogus);
        var sortedDeleted = new ArrayList<>(deleted); java.util.Collections.sort(sortedDeleted);
        var sortedIds = new ArrayList<>(ids); java.util.Collections.sort(sortedIds);
        assertThat(sortedDeleted).isEqualTo(sortedIds);
    }

    @Test
    void deleteBatchEmptyReturnsEmpty() {
        var name = TestQueues.uniqueName("q");
        q.createQueue(name);
        assertThat(q.deleteBatch(name, List.of())).isEmpty();
    }

    @Test
    void archiveMovesMessage() {
        var name = TestQueues.uniqueName("q");
        q.createQueue(name);
        long id = q.send(name, Map.of("a", 1));
        assertThat(q.archive(name, id)).isTrue();
        assertThat(q.read(name)).isEmpty();
    }

    @Test
    void archiveReturnsFalseWhenAbsent() {
        var name = TestQueues.uniqueName("q");
        q.createQueue(name);
        assertThat(q.archive(name, 99999L)).isFalse();
    }

    @Test
    void archiveBatchReturnsActuallyArchivedIds() {
        var name = TestQueues.uniqueName("q");
        q.createQueue(name);
        var msgs = new ArrayList<Map<String, Object>>();
        for (int i = 0; i < 3; i++) msgs.add(Map.of("i", i));
        var ids = q.sendBatch(name, msgs);
        var bogus = new ArrayList<>(ids);
        bogus.add(99999L);
        var archived = q.archiveBatch(name, bogus);
        var sortedArchived = new ArrayList<>(archived); java.util.Collections.sort(sortedArchived);
        var sortedIds = new ArrayList<>(ids); java.util.Collections.sort(sortedIds);
        assertThat(sortedArchived).isEqualTo(sortedIds);
        assertThat(q.read(name)).isEmpty();
    }
}
