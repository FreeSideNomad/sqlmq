/*
 * Drop-in example demonstrating sqlmq-client end-to-end.
 *
 * Compile + run (assumes the jar is on the classpath):
 *
 *   mvn -B package -DskipTests
 *   javac -cp clients/java/target/sqlmq-client-0.1.0-SNAPSHOT.jar:$(ls clients/java/target/dependency/*.jar 2>/dev/null | tr '\n' :) clients/java/examples/DropInExample.java
 *   java  -cp clients/java/target/sqlmq-client-0.1.0-SNAPSHOT.jar:$(ls clients/java/target/dependency/*.jar 2>/dev/null | tr '\n' :):clients/java/examples DropInExample
 *
 * Or just paste the body of main() into your own project.
 */

import io.freesidenomad.sqlmq.client.PgmqClient;
import io.freesidenomad.sqlmq.client.Message;

import java.util.Map;
import java.util.Optional;

public class DropInExample {

    public static void main(String[] args) {
        // Adjust to point at your SQL Server. The harness's Testcontainers
        // container exposes a random port, so for local exploration you
        // probably want either to wire HikariCP against your own SQL Server
        // instance or to drive the harness directly via JDBC URL.
        try (var q = new PgmqClient("localhost", 1433, "sqlmq", "sa", "P@ssw0rd!")) {

            var queue = "demo_orders";

            // 1. Create a queue (idempotent? — no; throws if already exists).
            q.createQueue(queue);

            // 2. Send a Map payload. Returns the new msg_id.
            long mid = q.send(queue, Map.of(
                "order_id", 42,
                "amount",   19.99,
                "items",    java.util.List.of("apple", "pear")
            ));
            System.out.println("sent msg_id=" + mid);

            // 3. Read back. read() returns Optional<Message> — empty for
            //    an empty queue. Default visibility timeout is 30s.
            Optional<Message> got = q.read(queue);
            got.ifPresent(m -> {
                System.out.println("got msg_id=" + m.msgId() + " payload=" + m.message());

                // 4. Delete after processing. Returns true if a row actually
                //    moved (false would mean someone else deleted it first).
                boolean deleted = q.delete(queue, m.msgId());
                System.out.println("deleted=" + deleted);
            });

            // 5. Cleanup.
            q.dropQueue(queue);
        }
    }
}
