package io.freesidenomad.sqlmq.client;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.freesidenomad.sqlmq.client.support.MigrationApplier;
import io.freesidenomad.sqlmq.client.support.TestContainerFixture;
import io.freesidenomad.sqlmq.client.support.TestQueues;
import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.Example;
import net.jqwik.api.ForAll;
import net.jqwik.api.From;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
import net.jqwik.api.Tag;
import net.jqwik.api.constraints.LongRange;
import net.jqwik.api.constraints.Size;
import net.jqwik.api.constraints.UniqueElements;
import net.jqwik.api.lifecycle.AfterContainer;
import net.jqwik.api.lifecycle.BeforeContainer;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Property-based tests for the strict pgmq surface ({@link PgmqClient}).
 *
 * <p>Mirrors {@code clients/python/tests/test_properties.py} property-for-property:
 * same names, same semantics, same {@code tries=30} budget. Each property creates
 * its own queue (via {@link TestQueues#randomName}) and drops it on exit so
 * jqwik's shrinking machinery never sees state from a previous example.</p>
 *
 * <p>This class uses jqwik's own lifecycle ({@link BeforeContainer} /
 * {@link AfterContainer}) rather than the JUnit Jupiter
 * {@link TestContainerFixture} extension. jqwik runs {@code @Property} methods
 * via its own JUnit Platform engine and does not invoke Jupiter extensions; we
 * therefore manage the per-class DB lifecycle directly. The shared
 * {@link TestContainerFixture#container()} singleton is reused so the
 * mssql:2022-latest container starts only once across all {@code *IT}
 * classes.</p>
 *
 * <h3>Strategy notes</h3>
 * <ul>
 *   <li>Payloads are JSON-safe {@code Map<String, Object>} dictionaries built
 *       level-by-level (depth cap 3). Leaf scalars: integer, double (no
 *       NaN/inf), string (max 64 chars), boolean, null.</li>
 *   <li>Each example talks to SQL Server via the per-class Hikari pool, so
 *       30 examples per property is a deliberate ceiling for CI tractability.</li>
 *   <li>Headers (P9): scalar-valued maps. The Java client's
 *       {@code Map<String, Object>} round-trip via Jackson preserves scalars
 *       exactly; nested maps would also round-trip but add nothing to the
 *       property.</li>
 * </ul>
 */
@Tag("property")
class PropertyIT {

    /** Per-class fresh database name. Created in {@link #setupDatabase()}. */
    private static String dbName;

    /** Hikari pool against {@link #dbName}. Closed in {@link #teardownDatabase()}. */
    private static HikariDataSource dataSource;

    /** Production-shape client used by every property. Stateless wrapper around {@link #dataSource}. */
    private static PgmqClient client;

    @BeforeContainer
    static void setupDatabase() throws Exception {
        var c = TestContainerFixture.container();
        dbName = "sqlmq_property_" + UUID.randomUUID().toString().replace("-", "");

        try (Connection adminConn = DriverManager.getConnection(
                c.getJdbcUrl(), c.getUsername(), c.getPassword());
             Statement st = adminConn.createStatement()) {
            st.execute("CREATE DATABASE [" + dbName + "]");
        }

        var jdbcUrl = c.getJdbcUrl() + ";databaseName=" + dbName + ";encrypt=false";
        var cfg = new HikariConfig();
        cfg.setJdbcUrl(jdbcUrl);
        cfg.setUsername(c.getUsername());
        cfg.setPassword(c.getPassword());
        cfg.setMaximumPoolSize(8);
        cfg.setConnectionTimeout(5_000);
        dataSource = new HikariDataSource(cfg);

        MigrationApplier.applyAll(dataSource);
        client = new PgmqClient(dataSource);
    }

    @AfterContainer
    static void teardownDatabase() {
        try {
            if (dataSource != null) dataSource.close();
        } catch (Throwable ignored) {}
        try {
            var c = TestContainerFixture.container();
            try (Connection adminConn = DriverManager.getConnection(
                    c.getJdbcUrl(), c.getUsername(), c.getPassword());
                 Statement st = adminConn.createStatement()) {
                st.execute("ALTER DATABASE [" + dbName + "] SET SINGLE_USER WITH ROLLBACK IMMEDIATE");
                st.execute("DROP DATABASE [" + dbName + "]");
            }
        } catch (Throwable t) {
            System.err.println("[PropertyIT] failed to drop " + dbName + ": " + t);
        }
    }

    // ------------------------------------------------------------ properties

    /** P1: send-then-read round trip preserves the message payload. */
    @Property(tries = 30)
    void p1_sendReadRoundTripPreservesMessage(@ForAll("jsonDicts") Map<String, Object> message) {
        var name = TestQueues.randomName("p1");
        client.createQueue(name);
        try {
            client.send(name, message);
            var msg = client.read(name, 30);
            assertThat(msg).isPresent();
            assertThat(msg.get().message()).isEqualTo(message);
        } finally {
            client.dropQueue(name);
        }
    }

    /** P2: msg_ids issued by send() are strictly monotonic per queue (V010). */
    @Property(tries = 30)
    void p2_monotonicMsgIdsPerQueue(
        @ForAll @Size(min = 2, max = 20) List<@From("jsonDicts") Map<String, Object>> messages
    ) {
        var name = TestQueues.randomName("p2");
        client.createQueue(name);
        try {
            var ids = new ArrayList<Long>(messages.size());
            for (var m : messages) ids.add(client.send(name, m));
            var sorted = new ArrayList<>(ids);
            java.util.Collections.sort(sorted);
            assertThat(ids).as("ids monotonic").isEqualTo(sorted);
            assertThat(new HashSet<>(ids)).as("ids unique").hasSize(ids.size());
        } finally {
            client.dropQueue(name);
        }
    }

    /** P3: VT honored within session — a second read on a freshly-claimed
     *  message returns empty (vt has not expired). */
    @Property(tries = 30)
    void p3_vtHonoredWithinSession(@ForAll("jsonDicts") Map<String, Object> message) {
        var name = TestQueues.randomName("p3");
        client.createQueue(name);
        try {
            client.send(name, message);
            var first = client.read(name, 60);
            assertThat(first).isPresent();
            var second = client.read(name, 60);
            assertThat(second).isEmpty();
        } finally {
            client.dropQueue(name);
        }
    }

    /** P4: delete-then-read returns empty. */
    @Property(tries = 30)
    void p4_deleteThenReadReturnsEmpty(
        @ForAll @Size(min = 1, max = 20) List<@From("jsonDicts") Map<String, Object>> messages
    ) {
        var name = TestQueues.randomName("p4");
        client.createQueue(name);
        try {
            var ids = new ArrayList<Long>(messages.size());
            for (var m : messages) ids.add(client.send(name, m));
            var deleted = client.deleteBatch(name, ids);
            assertThat(new HashSet<>(deleted)).isEqualTo(new HashSet<>(ids));
            assertThat(client.read(name, 30)).isEmpty();
        } finally {
            client.dropQueue(name);
        }
    }

    /** P5: archive moves rather than copies — queue_length drops. */
    @Property(tries = 30)
    void p5_archiveMovesRatherThanCopies(@ForAll("jsonDicts") Map<String, Object> message) {
        var name = TestQueues.randomName("p5");
        client.createQueue(name);
        try {
            long id = client.send(name, message);
            // Claim it first so we know the id is present.
            var first = client.read(name, 30);
            assertThat(first).isPresent();
            assertThat(first.get().msgId()).isEqualTo(id);
            assertThat(client.archive(name, id)).isTrue();
            assertThat(client.read(name, 30)).isEmpty();
            assertThat(client.metrics(name).queueLength()).isZero();
        } finally {
            client.dropQueue(name);
        }
    }

    /** P6: purge clears the active queue and reports the count purged. */
    @Property(tries = 30)
    void p6_purgeClearsActiveQueue(
        @ForAll @Size(min = 1, max = 50) List<@From("jsonDicts") Map<String, Object>> messages
    ) {
        var name = TestQueues.randomName("p6");
        client.createQueue(name);
        try {
            for (var m : messages) client.send(name, m);
            int count = client.purge(name);
            assertThat(count).isEqualTo(messages.size());
            assertThat(client.read(name, 30)).isEmpty();
            assertThat(client.metrics(name).queueLength()).isZero();
        } finally {
            client.dropQueue(name);
        }
    }

    /** P7: send_batch returns N ids in caller order (monotonic). */
    @Property(tries = 30)
    void p7_sendBatchReturnsNIdsInCallerOrder(
        @ForAll @Size(min = 1, max = 20) List<@From("jsonDicts") Map<String, Object>> messages
    ) {
        var name = TestQueues.randomName("p7");
        client.createQueue(name);
        try {
            var ids = client.sendBatch(name, messages);
            assertThat(ids).hasSize(messages.size());
            var sorted = new ArrayList<>(ids);
            java.util.Collections.sort(sorted);
            assertThat(ids).isEqualTo(sorted);
        } finally {
            client.dropQueue(name);
        }
    }

    /** P8: delete_batch with unknown ids returns the empty list. */
    @Property(tries = 30)
    void p8_deleteBatchWithUnknownIdsReturnsEmpty(
        @ForAll @Size(min = 1, max = 20) @UniqueElements
        List<@LongRange(min = 1_000_000_000L, max = 2_000_000_000L) Long> unknownIds
    ) {
        var name = TestQueues.randomName("p8");
        client.createQueue(name);
        try {
            var result = client.deleteBatch(name, unknownIds);
            assertThat(result).isEmpty();
        } finally {
            client.dropQueue(name);
        }
    }

    /** P9 (sqlmq extension): headers passed to send round-trip via
     *  {@link Message#headers()} on read. */
    @Property(tries = 30)
    void p9_headersRoundTripWhenSent(@ForAll("flatHeaderMaps") Map<String, Object> headers) {
        var name = TestQueues.randomName("p9");
        client.createQueue(name);
        try {
            client.send(name, Map.of("v", 1), 0, headers);
            var msg = client.read(name, 30);
            assertThat(msg).isPresent();
            assertThat(msg.get().headers()).isEqualTo(headers);
        } finally {
            client.dropQueue(name);
        }
    }

    /** P10 (sanity): read on a fresh empty queue returns empty + readBatch
     *  returns []. Not strictly property-based — included alongside the
     *  property suite for completeness. Uses jqwik's {@link Example} so it
     *  runs through the same engine as the {@code @Property} methods. */
    @Example
    void p10_readOnEmptyQueueReturnsEmpty() {
        var name = TestQueues.randomName("p10");
        client.createQueue(name);
        try {
            assertThat(client.read(name)).isEmpty();
            assertThat(client.readBatch(name, 30, 10)).isEmpty();
        } finally {
            client.dropQueue(name);
        }
    }

    // ------------------------------------------------------------ providers

    /**
     * JSON-safe scalar leaves: integer, double (no NaN/inf), short string,
     * boolean, null. Mirrors the Python suite's {@code json_scalars}.
     */
    @Provide
    Arbitrary<Object> jsonScalars() {
        return Arbitraries.oneOf(
            Arbitraries.integers().between(-1_000_000, 1_000_000).map(i -> (Object) i),
            Arbitraries.doubles().between(-1e6, 1e6)
                .filter(d -> !Double.isNaN(d) && !Double.isInfinite(d))
                .map(d -> (Object) d),
            Arbitraries.strings().ofMaxLength(64).map(s -> (Object) s),
            Arbitraries.of(true, false).map(b -> (Object) b),
            Arbitraries.just((Object) null)
        );
    }

    /**
     * JSON-safe map: keys are non-empty short strings (length 1..16);
     * values are scalars or recursively-bounded sub-maps. Depth cap of 3
     * keeps shrink times reasonable while still covering nested cases.
     *
     * <p>Why a custom level-by-level recursion rather than
     * {@code Arbitraries.recursive}: we want the recursion at the value
     * position only, not on the whole map. Building level-by-level keeps the
     * type signatures concrete and the depth bound explicit.</p>
     */
    @Provide
    Arbitrary<Map<String, Object>> jsonDicts() {
        var keys = Arbitraries.strings().withCharRange('a', 'z').ofMinLength(1).ofMaxLength(16);

        // Level-0: scalar values only.
        Arbitrary<Map<String, Object>> level0 =
            Arbitraries.maps(keys, jsonScalars()).ofMinSize(0).ofMaxSize(8);

        // Level-1: values are scalars OR a level-0 map.
        Arbitrary<Object> values1 = Arbitraries.oneOf(
            jsonScalars(),
            level0.map(m -> (Object) m)
        );
        Arbitrary<Map<String, Object>> level1 =
            Arbitraries.maps(keys, values1).ofMinSize(0).ofMaxSize(8);

        // Level-2: values are scalars OR a level-1 map.
        Arbitrary<Object> values2 = Arbitraries.oneOf(
            jsonScalars(),
            level1.map(m -> (Object) m)
        );
        return Arbitraries.maps(keys, values2).ofMinSize(0).ofMaxSize(8);
    }

    /**
     * Flat (non-nested) header maps: at least one entry, scalar values only.
     * pgmq's headers have no nesting requirement and the Java client's
     * {@code Map<String, Object>} round-trip via Jackson preserves scalars
     * exactly; nested maps would also round-trip but add nothing to P9.
     */
    @Provide
    Arbitrary<Map<String, Object>> flatHeaderMaps() {
        var keys = Arbitraries.strings().withCharRange('a', 'z').ofMinLength(1).ofMaxLength(16);
        return Arbitraries.maps(keys, jsonScalars()).ofMinSize(1).ofMaxSize(8);
    }
}
