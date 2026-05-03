package io.freesidenomad.sqlmq.client;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.microsoft.sqlserver.jdbc.SQLServerCallableStatement;
import io.freesidenomad.sqlmq.client.internal.QueueNameValidator;
import io.freesidenomad.sqlmq.client.internal.SqlStatements;
import io.freesidenomad.sqlmq.client.internal.TvpHelpers;

import javax.sql.DataSource;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TimeZone;

/**
 * Strict pgmq-API-compatible synchronous client for sqlmq on SQL Server.
 *
 * <p>This class's public method names and semantics mirror the Python
 * {@code sqlmq.PGMQueue} (which mirrors {@code tembo_pgmq_python.PGMQueue}
 * 0.10.0). Methods whose semantics cannot be honored
 * ({@code unlogged}, partitioned, {@code setVt}, {@code detachArchive})
 * throw {@link UnsupportedOperationException} with the same wording as
 * the Python client's {@code NotImplementedError}.</p>
 *
 * <h2>Connections and pooling</h2>
 *
 * <p>Three constructors:</p>
 * <ol>
 *   <li>{@link #PgmqClient(String, int, String, String, String)} — assembles
 *       a JDBC URL from host/port/database and opens a fresh
 *       {@link DriverManager} connection per call. Fine for low-throughput
 *       scripts; not recommended for production.</li>
 *   <li>{@link #PgmqClient(String, String, String)} — same as above but
 *       with a caller-supplied JDBC URL.</li>
 *   <li>{@link #PgmqClient(DataSource)} — bring-your-own pool. This is
 *       the production-recommended path: wire HikariCP (or any other
 *       {@link DataSource}) and pass it in.</li>
 * </ol>
 *
 * <p>This client does NOT bundle a connection pool. The TVP path uses
 * {@link CallableStatement#unwrap(Class)} on
 * {@link SQLServerCallableStatement}, which is HikariCP-proxy safe.</p>
 *
 * <h2>JSON serialization</h2>
 *
 * <p>{@code Map<String, Object>} payloads are serialized to JSON via
 * Jackson and stored in {@code NVARCHAR(MAX)} columns with
 * {@code CHECK (ISJSON=1)}. Reading parses back to {@code Map<String, Object>}
 * (objects), {@code List<Object>} (arrays), {@code String}/{@code Number}/etc.
 * — Jackson's default mapping for {@link Object}.</p>
 *
 * <h2>Thread safety</h2>
 *
 * <p>This class is thread-safe: each method call acquires its own
 * connection from the underlying source, so concurrent calls do not
 * share JDBC state.</p>
 */
public final class PgmqClient implements AutoCloseable {

    /**
     * pgmq-default visibility timeout (seconds). Used by {@link #read(String)}.
     */
    public static final int DEFAULT_VT_SECONDS = 30;

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final TypeReference<Map<String, Object>> MAP_TYPE =
        new TypeReference<>() {};

    /**
     * UTC calendar passed to {@link ResultSet#getTimestamp(String, Calendar)}.
     * sqlmq stores timestamps in {@code DATETIME2(7)} populated by
     * {@code SYSUTCDATETIME()}; without this Calendar mssql-jdbc would
     * interpret them in the JVM's default zone.
     */
    private static final Calendar UTC_CAL = Calendar.getInstance(TimeZone.getTimeZone("UTC"));

    private final ConnectionSource connectionSource;
    private final boolean ownsCloseable;

    // ---------------------------------------------------------------- ctors

    /**
     * Build from raw connection params. A fresh {@link DriverManager}
     * connection is opened per call. For production, use the
     * {@link #PgmqClient(DataSource)} constructor with a pooled DataSource.
     */
    public PgmqClient(String host, int port, String database,
                      String username, String password) {
        this(buildJdbcUrl(host, port, database), username, password);
    }

    /**
     * Build from a caller-supplied JDBC URL plus credentials.
     */
    public PgmqClient(String jdbcUrl, String username, String password) {
        this.connectionSource = () -> DriverManager.getConnection(jdbcUrl, username, password);
        this.ownsCloseable = false;
    }

    /**
     * Build from a caller-supplied {@link DataSource}. The DataSource
     * is NOT closed by {@link #close()} — the caller owns its lifecycle.
     */
    public PgmqClient(DataSource dataSource) {
        this.connectionSource = dataSource::getConnection;
        this.ownsCloseable = false;
    }

    private static String buildJdbcUrl(String host, int port, String database) {
        // encrypt=false matches the harness defaults; consumers wanting TLS
        // should use the (jdbcUrl, user, pass) constructor and set their own
        // ;encrypt=true;trustServerCertificate=... etc.
        return "jdbc:sqlserver://" + host + ":" + port
            + ";databaseName=" + database
            + ";encrypt=false";
    }

    @FunctionalInterface
    private interface ConnectionSource {
        Connection get() throws SQLException;
    }

    // ----------------------------------------------------------- pgmq surface

    /**
     * Validate a queue name. Mirrors sqlmq.create_queue.
     *
     * @throws IllegalArgumentException if the name is null/empty or fails
     *         the regex {@code ^[A-Za-z_][A-Za-z0-9_]{0,59}$}.
     */
    public void validateQueueName(String name) {
        QueueNameValidator.validate(name);
    }

    /**
     * Create a queue with default settings.
     *
     * <p>Maps to {@code EXEC sqlmq.create_queue} with
     * {@code @storage='ondisk'}, {@code @grouped=0},
     * {@code @payload_type='json'}, {@code @max_delivery_count=NULL}.</p>
     */
    public void createQueue(String queue) {
        createQueue(queue, false);
    }

    /**
     * Create a queue. {@code unlogged=true} is rejected.
     *
     * @throws UnsupportedOperationException if {@code unlogged} is {@code true};
     *         sqlmq retired its in-memory variant in V014.
     */
    public void createQueue(String queue, boolean unlogged) {
        if (unlogged) {
            throw new UnsupportedOperationException(
                "sqlmq does not support unlogged queues; the in-memory variant was "
                    + "retired in V014. See docs/superpowers/archive/ for rationale."
            );
        }
        QueueNameValidator.validate(queue);
        try (Connection c = connectionSource.get();
             CallableStatement cs = c.prepareCall(SqlStatements.CREATE_QUEUE)) {
            cs.setString(1, queue);
            cs.setString(2, "ondisk");
            cs.setBoolean(3, false);
            cs.setString(4, "json");
            cs.setNull(5, Types.INTEGER);
            cs.execute();
        } catch (SQLException e) {
            throw new RuntimeException("createQueue(" + queue + ") failed", e);
        }
    }

    /**
     * Always throws — sqlmq has no partitioned queues (no SQL Server
     * equivalent of pg_partman).
     */
    public void createPartitionedQueue(String queue) {
        createPartitionedQueue(queue, 10000, 100000);
    }

    /**
     * Always throws — sqlmq has no partitioned queues.
     */
    public void createPartitionedQueue(String queue, int partitionInterval, int retentionInterval) {
        throw new UnsupportedOperationException(
            "sqlmq does not support partitioned queues. pgmq's partitioning "
                + "depends on pg_partman, which has no SQL Server equivalent. "
                + "Tracked for a future release."
        );
    }

    /**
     * Drop a queue. Returns {@code true} if it existed and was dropped,
     * {@code false} if the queue did not exist (matches pgmq).
     */
    public boolean dropQueue(String queue) {
        return dropQueue(queue, false);
    }

    /**
     * Drop a queue. The {@code partitioned} flag must be {@code false};
     * passing {@code true} throws {@link UnsupportedOperationException} to
     * avoid masking caller intent — sqlmq has no partitioned queues.
     *
     * @throws UnsupportedOperationException if {@code partitioned} is {@code true}.
     */
    public boolean dropQueue(String queue, boolean partitioned) {
        if (partitioned) {
            throw new UnsupportedOperationException(
                "sqlmq does not support partitioned queues; partitioned=true is "
                    + "rejected to avoid masking caller intent. See createPartitionedQueue "
                    + "for the same rationale."
            );
        }
        QueueNameValidator.validate(queue);
        try (Connection c = connectionSource.get();
             CallableStatement cs = c.prepareCall(SqlStatements.DROP_QUEUE)) {
            cs.setString(1, queue);
            cs.execute();
            return true;
        } catch (SQLException e) {
            // sqlmq.drop_queue raises 50010 'Queue does not exist.' for missing
            // queues. pgmq's drop_queue returns False; mirror.
            if (e.getMessage() != null && e.getMessage().contains("Queue does not exist")) {
                return false;
            }
            throw new RuntimeException("dropQueue(" + queue + ") failed", e);
        }
    }

    /**
     * Return all queue names registered in {@code sqlmq.meta}.
     */
    public List<String> listQueues() {
        var out = new ArrayList<String>();
        try (Connection c = connectionSource.get();
             Statement st = c.createStatement();
             ResultSet rs = st.executeQuery(SqlStatements.LIST_QUEUES)) {
            while (rs.next()) {
                out.add(rs.getString(1));
            }
        } catch (SQLException e) {
            throw new RuntimeException("listQueues failed", e);
        }
        return out;
    }

    /**
     * Send a single message with no delay. Returns the new {@code msg_id}.
     */
    public long send(String queue, Map<String, Object> message) {
        return send(queue, message, 0, null);
    }

    /**
     * Send a single message with an integer delay (seconds). Returns the
     * new {@code msg_id}.
     */
    public long send(String queue, Map<String, Object> message, int delaySeconds) {
        return send(queue, message, delaySeconds, null);
    }

    /**
     * Send a single message with an integer delay (seconds) and an optional
     * {@code headers} map. Returns the new {@code msg_id}.
     *
     * <p><strong>sqlmq extension:</strong> the {@code headers} parameter is
     * not part of the strict pgmq 0.10 surface (pgmq's {@code send} carries
     * payload only). sqlmq's storage already has a {@code headers NVARCHAR(MAX)}
     * column; this overload makes that column reachable through the strict
     * compat client so {@link Message#headers()} is not vestigial for in-app
     * workflows. Pass {@code null} for no headers.</p>
     */
    public long send(String queue, Map<String, Object> message, int delaySeconds,
                     Map<String, Object> headers) {
        QueueNameValidator.validate(queue);
        String encoded = encode(message);
        String encodedHeaders = headers == null ? null : encode(headers);
        try (Connection c = connectionSource.get();
             CallableStatement cs = c.prepareCall(SqlStatements.SEND)) {
            cs.setString(1, queue);
            cs.setString(2, encoded);
            cs.setNull(3, Types.VARBINARY);
            if (encodedHeaders == null) cs.setNull(4, Types.NVARCHAR);
            else cs.setString(4, encodedHeaders);
            cs.setInt(5, delaySeconds);
            try (ResultSet rs = cs.executeQuery()) {
                if (!rs.next()) {
                    throw new RuntimeException("sqlmq.send returned no rows; expected msg_id");
                }
                return rs.getLong("msg_id");
            }
        } catch (SQLException e) {
            throw new RuntimeException("send(" + queue + ") failed", e);
        }
    }

    /**
     * Send a batch of messages with no delay. Returns the new ids in
     * caller order.
     */
    public List<Long> sendBatch(String queue, List<Map<String, Object>> messages) {
        return sendBatch(queue, messages, 0, null);
    }

    /**
     * Send a batch of messages. Single round trip via the
     * {@code dbo.sqlmq_send_tvp} TVP — far cleaner than the Python lib's
     * declare-and-EXEC workaround because mssql-jdbc supports TVPs natively.
     */
    public List<Long> sendBatch(String queue, List<Map<String, Object>> messages, int delaySeconds) {
        return sendBatch(queue, messages, delaySeconds, null);
    }

    /**
     * Send a batch of messages with an optional {@code headers} map applied
     * uniformly to every row.
     *
     * <p>Same sqlmq-extension caveat as
     * {@link #send(String, Map, int, Map)}: pgmq's {@code send_batch} does
     * not carry headers. Use this overload when you need them; pass
     * {@code null} to keep the strict pgmq behavior.</p>
     */
    public List<Long> sendBatch(String queue, List<Map<String, Object>> messages, int delaySeconds,
                                Map<String, Object> headers) {
        QueueNameValidator.validate(queue);
        if (messages.isEmpty()) {
            return List.of();
        }
        var encoded = new ArrayList<String>(messages.size());
        for (var m : messages) {
            encoded.add(encode(m));
        }
        String encodedHeaders = headers == null ? null : encode(headers);
        var ids = new ArrayList<Long>(messages.size());
        try (Connection c = connectionSource.get();
             CallableStatement cs = c.prepareCall(SqlStatements.SEND_BATCH)) {
            cs.setString(1, queue);
            cs.unwrap(SQLServerCallableStatement.class)
                .setStructured(2, TvpHelpers.SEND_TVP_NAME,
                    TvpHelpers.buildSendTvp(encoded, delaySeconds, encodedHeaders));
            try (ResultSet rs = cs.executeQuery()) {
                while (rs.next()) {
                    ids.add(rs.getLong("msg_id"));
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException("sendBatch(" + queue + ") failed", e);
        }
        return ids;
    }

    /**
     * Read a single message with the default visibility timeout
     * ({@value #DEFAULT_VT_SECONDS}s). Returns {@link Optional#empty()}
     * for an empty queue.
     */
    public Optional<Message> read(String queue) {
        return read(queue, DEFAULT_VT_SECONDS);
    }

    /**
     * Read a single message with a caller-specified visibility timeout.
     */
    public Optional<Message> read(String queue, int vtSeconds) {
        var msgs = readBatch(queue, vtSeconds, 1);
        return msgs.isEmpty() ? Optional.empty() : Optional.of(msgs.get(0));
    }

    /**
     * Read up to {@code batchSize} messages, hiding each for {@code vtSeconds}
     * seconds. Returns an empty list if the queue is empty (mirrors pgmq's
     * runtime behavior; pgmq's type hint says {@code Optional<List<Message>>}
     * but in practice it returns the empty list).
     */
    public List<Message> readBatch(String queue, int vtSeconds, int batchSize) {
        QueueNameValidator.validate(queue);
        var out = new ArrayList<Message>();
        try (Connection c = connectionSource.get();
             CallableStatement cs = c.prepareCall(SqlStatements.READ)) {
            cs.setString(1, queue);
            cs.setInt(2, vtSeconds);
            cs.setInt(3, batchSize);
            try (ResultSet rs = cs.executeQuery()) {
                while (rs.next()) {
                    out.add(rowToMessage(rs));
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException("readBatch(" + queue + ") failed", e);
        }
        return out;
    }

    /**
     * Poll-read messages, blocking up to {@code maxPollSeconds}.
     *
     * <p>sqlmq has no server-side long-poll equivalent (deliberate; see
     * {@code research/sqlserver-longpoll.md}). We implement client-side
     * Hangfire-style backoff: call {@link #readBatch} repeatedly, sleeping
     * {@code pollIntervalMs} between empty results until either we get a
     * non-empty batch or the deadline elapses.</p>
     */
    public List<Message> readWithPoll(String queue, int vtSeconds, int qty,
                                      int maxPollSeconds, int pollIntervalMs) {
        QueueNameValidator.validate(queue);
        long deadlineNanos = System.nanoTime() + maxPollSeconds * 1_000_000_000L;
        long sleepMs = Math.max(pollIntervalMs, 1);
        while (true) {
            var msgs = readBatch(queue, vtSeconds, qty);
            if (!msgs.isEmpty()) return msgs;
            if (System.nanoTime() >= deadlineNanos) return msgs;  // empty list
            try {
                Thread.sleep(sleepMs);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("readWithPoll interrupted", e);
            }
        }
    }

    /**
     * Atomically read+delete one message. Returns {@link Optional#empty()}
     * if the queue is empty.
     *
     * <p>(pgmq's type hint says {@code Message} but their impl actually
     * returns {@code None} on empty queues — their {@code messages[0]}
     * would IndexError. We mirror the actual behavior.)</p>
     */
    public Optional<Message> pop(String queue) {
        QueueNameValidator.validate(queue);
        try (Connection c = connectionSource.get();
             CallableStatement cs = c.prepareCall(SqlStatements.POP)) {
            cs.setString(1, queue);
            try (ResultSet rs = cs.executeQuery()) {
                if (!rs.next()) return Optional.empty();
                return Optional.of(rowToMessage(rs));
            }
        } catch (SQLException e) {
            throw new RuntimeException("pop(" + queue + ") failed", e);
        }
    }

    /**
     * Delete one message. Returns {@code true} iff a row was actually
     * deleted (i.e. the id existed in the queue).
     */
    public boolean delete(String queue, long msgId) {
        QueueNameValidator.validate(queue);
        return execMsgIdProc(SqlStatements.DELETE, queue, List.of(msgId), null) > 0;
    }

    /**
     * Delete a batch of messages. Returns the ids actually deleted, in
     * caller-supplied order — matching pgmq's contract.
     *
     * <p>sqlmq's {@code delete} proc returns a count, not the surviving id
     * list. To honor pgmq's contract we {@code SELECT} the live ids before
     * the call and intersect with the live set after. Cost: 2 extra round
     * trips. Same caveat as the Python client.</p>
     */
    public List<Long> deleteBatch(String queue, List<Long> msgIds) {
        QueueNameValidator.validate(queue);
        if (msgIds.isEmpty()) return List.of();
        return surgicalDifference(queue, msgIds, /*archive=*/ false);
    }

    /**
     * Archive one message. Returns {@code true} iff a row was actually
     * moved into the archive table.
     */
    public boolean archive(String queue, long msgId) {
        QueueNameValidator.validate(queue);
        return execMsgIdProc(SqlStatements.ARCHIVE, queue, List.of(msgId), null) > 0;
    }

    /**
     * Archive a batch of messages. Same contract+caveat as
     * {@link #deleteBatch}.
     */
    public List<Long> archiveBatch(String queue, List<Long> msgIds) {
        QueueNameValidator.validate(queue);
        if (msgIds.isEmpty()) return List.of();
        return surgicalDifference(queue, msgIds, /*archive=*/ true);
    }

    /**
     * Delete every message in the queue. Returns the count purged.
     */
    public int purge(String queue) {
        QueueNameValidator.validate(queue);
        try (Connection c = connectionSource.get();
             CallableStatement cs = c.prepareCall(SqlStatements.PURGE)) {
            cs.setString(1, queue);
            try (ResultSet rs = cs.executeQuery()) {
                if (!rs.next()) return 0;
                return rs.getInt("rows_deleted");
            }
        } catch (SQLException e) {
            throw new RuntimeException("purge(" + queue + ") failed", e);
        }
    }

    /**
     * Return metrics for a single queue.
     *
     * <p>{@link QueueMetrics#scrapeTime} is filled client-side at the moment
     * {@code metrics()} returns (UTC), since sqlmq does not emit it from the
     * proc.</p>
     */
    public QueueMetrics metrics(String queue) {
        QueueNameValidator.validate(queue);
        try (Connection c = connectionSource.get();
             CallableStatement cs = c.prepareCall(SqlStatements.METRICS)) {
            cs.setString(1, queue);
            try (ResultSet rs = cs.executeQuery()) {
                if (!rs.next()) {
                    throw new RuntimeException(
                        "sqlmq.metrics returned no rows for queue '" + queue + "'");
                }
                return rowToMetrics(rs);
            }
        } catch (SQLException e) {
            throw new RuntimeException("metrics(" + queue + ") failed", e);
        }
    }

    /**
     * Return metrics for every queue.
     */
    public List<QueueMetrics> metricsAll() {
        var out = new ArrayList<QueueMetrics>();
        try (Connection c = connectionSource.get();
             CallableStatement cs = c.prepareCall(SqlStatements.METRICS_ALL)) {
            try (ResultSet rs = cs.executeQuery()) {
                while (rs.next()) {
                    out.add(rowToMetrics(rs));
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException("metricsAll failed", e);
        }
        return out;
    }

    /**
     * Always throws — sqlmq does not yet support {@code set_vt}.
     */
    public Message setVt(String queue, long msgId, int vtSeconds) {
        throw new UnsupportedOperationException(
            "sqlmq does not yet support set_vt. Tracked for a future release; "
                + "would require adding a sqlmq.set_vt stored proc."
        );
    }

    /**
     * Always throws — pgmq-specific; sqlmq archives are single-table per queue.
     */
    public void detachArchive(String queue) {
        throw new UnsupportedOperationException(
            "sqlmq does not support detach_archive (a pgmq-specific feature for "
                + "partitioned archive tables; sqlmq archives are single-table per queue)."
        );
    }

    @Override
    public void close() {
        // Nothing to close: the DataSource (if any) is caller-owned, and
        // the (host, port, ...) and (jdbcUrl, ...) variants open a fresh
        // DriverManager connection per call. Method exists so the client
        // is usable in try-with-resources, parallel to the Python class
        // exposing close-like behavior via context managers.
    }

    // --------------------------------------------------------- internals

    /**
     * Run an EXEC against {@code sqlmq.[delete]} or {@code sqlmq.archive}
     * (both share the {@code (queue, msg_id_tvp[, reason])} shape) and
     * return the proc's row count.
     */
    private int execMsgIdProc(String proc, String queue, Collection<Long> msgIds, String archiveReason) {
        if (msgIds.isEmpty()) return 0;
        try (Connection c = connectionSource.get();
             CallableStatement cs = c.prepareCall(proc)) {
            cs.setString(1, queue);
            cs.unwrap(SQLServerCallableStatement.class)
                .setStructured(2, TvpHelpers.MSG_ID_TVP_NAME,
                    TvpHelpers.buildMsgIdTvp(msgIds));
            String countCol;
            if (proc.equals(SqlStatements.ARCHIVE)) {
                if (archiveReason == null) cs.setNull(3, Types.NVARCHAR);
                else cs.setString(3, archiveReason);
                countCol = "rows_archived";
            } else {
                countCol = "rows_deleted";
            }
            try (ResultSet rs = cs.executeQuery()) {
                if (!rs.next()) return 0;
                return rs.getInt(countCol);
            }
        } catch (SQLException e) {
            throw new RuntimeException(proc + " on " + queue + " failed", e);
        }
    }

    /**
     * Implement the pgmq "return the ids actually removed in caller order"
     * contract for delete/archive batches. Same strategy as the Python lib:
     * SELECT live ids before, run the proc, SELECT live ids after, return
     * the set difference projected back into caller order.
     */
    private List<Long> surgicalDifference(String queue, List<Long> msgIds, boolean archive) {
        var qtable = "q_" + queue;
        // Snapshot which of the requested ids exist before the call.
        Set<Long> presentBefore;
        try (Connection c = connectionSource.get();
             var ps = c.prepareStatement(buildSelectIn(qtable, msgIds.size()))) {
            for (int i = 0; i < msgIds.size(); i++) ps.setLong(i + 1, msgIds.get(i));
            presentBefore = collectIds(ps);
        } catch (SQLException e) {
            throw new RuntimeException("pre-check for " + queue + " failed", e);
        }

        if (presentBefore.isEmpty()) return List.of();

        execMsgIdProc(archive ? SqlStatements.ARCHIVE : SqlStatements.DELETE,
            queue, presentBefore, null);

        // Recheck the same ids; any that were present before and are now
        // gone were genuinely removed by the proc.
        Set<Long> presentAfter;
        try (Connection c = connectionSource.get();
             var ps = c.prepareStatement(buildSelectIn(qtable, msgIds.size()))) {
            for (int i = 0; i < msgIds.size(); i++) ps.setLong(i + 1, msgIds.get(i));
            presentAfter = collectIds(ps);
        } catch (SQLException e) {
            throw new RuntimeException("post-check for " + queue + " failed", e);
        }

        var removed = new HashSet<Long>(presentBefore);
        removed.removeAll(presentAfter);
        var out = new ArrayList<Long>(removed.size());
        for (Long id : msgIds) {
            if (removed.contains(id)) out.add(id);
        }
        return out;
    }

    private static String buildSelectIn(String qtable, int n) {
        var placeholders = new StringBuilder();
        for (int i = 0; i < n; i++) {
            if (i > 0) placeholders.append(',');
            placeholders.append('?');
        }
        return "SELECT msg_id FROM sqlmq.[" + qtable + "] WHERE msg_id IN (" + placeholders + ")";
    }

    private static Set<Long> collectIds(java.sql.PreparedStatement ps) throws SQLException {
        var ids = new HashSet<Long>();
        try (ResultSet rs = ps.executeQuery()) {
            while (rs.next()) ids.add(rs.getLong(1));
        }
        return ids;
    }

    private static Message rowToMessage(ResultSet rs) throws SQLException {
        ResultSetMetaData md = rs.getMetaData();
        // Some procs return a leading group_key column (read), others don't (pop).
        // Drive off column names so we work with both.
        Integer headersIdx = null;
        for (int i = 1; i <= md.getColumnCount(); i++) {
            if ("headers".equalsIgnoreCase(md.getColumnLabel(i))) {
                headersIdx = i;
                break;
            }
        }
        Timestamp enq = rs.getTimestamp("enqueued_at", UTC_CAL);
        Timestamp vt = rs.getTimestamp("vt", UTC_CAL);
        String json = rs.getString("message");
        Map<String, Object> body = decode(json);
        Map<String, Object> headers = null;
        if (headersIdx != null) {
            String h = rs.getString(headersIdx);
            if (h != null) headers = decode(h);
        }
        return new Message(
            rs.getLong("msg_id"),
            rs.getInt("read_ct"),
            enq != null ? enq.toInstant() : null,
            vt != null ? vt.toInstant() : null,
            body,
            headers
        );
    }

    private static QueueMetrics rowToMetrics(ResultSet rs) throws SQLException {
        // sqlmq columns (V016+): queue_name, queue_length, total_messages,
        // oldest_msg_age_seconds, newest_msg_age_seconds, dlq_count.
        String name = rs.getString("queue_name");
        long len = rs.getLong("queue_length");
        long total = rs.getLong("total_messages");
        Object oldestObj = rs.getObject("oldest_msg_age_seconds");
        Integer oldest = oldestObj == null ? null : ((Number) oldestObj).intValue();
        Object newestObj = rs.getObject("newest_msg_age_seconds");
        Integer newest = newestObj == null ? null : ((Number) newestObj).intValue();
        return new QueueMetrics(name, len, newest, oldest, total, Instant.now());
    }

    private static String encode(Map<String, Object> message) {
        try {
            return MAPPER.writeValueAsString(message);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("failed to serialize message", e);
        }
    }

    private static Map<String, Object> decode(String raw) {
        if (raw == null) return Map.of();
        try {
            return MAPPER.readValue(raw, MAP_TYPE);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("failed to deserialize message JSON: " + raw, e);
        }
    }
}
