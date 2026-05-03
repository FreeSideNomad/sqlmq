package io.freesidenomad.sqlmq.concurrency;

import io.freesidenomad.sqlmq.client.SqlmqClient;
import io.freesidenomad.sqlmq.support.DatabasePerTest;
import io.freesidenomad.sqlmq.support.TestQueues;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.concurrent.StructuredTaskScope;

import static org.assertj.core.api.Assertions.assertThat;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@ExtendWith(DatabasePerTest.class)
class DlqEnforcementTest {

    @Test
    void messagesReachingMaxDeliveriesAreEventuallyArchived(ExtensionContext ctx) throws Exception {
        var client = new SqlmqClient(DatabasePerTest.dataSource(ctx));
        var q = TestQueues.uniqueName("q");
        client.createQueue(q, false, "json", 3);

        for (int i = 0; i < 100; i++) client.send(q, "{\"i\":" + i + "}", null);

        // 16 broken consumers that read but never delete; vt_seconds=1 lets messages return quickly
        var until = System.nanoTime() + java.util.concurrent.TimeUnit.SECONDS.toNanos(20);
        // Use the JDK 25 StructuredTaskScope API (preview): StructuredTaskScope.open(Joiner)
        try (var scope = StructuredTaskScope.open(
                StructuredTaskScope.Joiner.<Void>awaitAllSuccessfulOrThrow())) {
            for (int c = 0; c < 16; c++) {
                scope.fork(() -> {
                    while (System.nanoTime() < until) {
                        var msgs = client.read(q, 1, 10);
                        if (msgs.isEmpty()) Thread.sleep(50);
                    }
                    return null;
                });
            }
            scope.join();
        }

        client.dlqSweep(q);

        try (var c = DatabasePerTest.dataSource(ctx).getConnection();
             PreparedStatement ps = c.prepareStatement(
                 "SELECT COUNT(*) AS n FROM sqlmq.[a_" + q + "] WHERE dlq_reason = 'max_deliveries'")) {
            try (ResultSet rs = ps.executeQuery()) {
                rs.next();
                assertThat(rs.getInt("n")).isEqualTo(100);
            }
        }
        assertThat(client.metrics(q).queueLength()).isZero();
    }
}
