package io.freesidenomad.sqlmq.client;

import javax.sql.DataSource;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.TimeZone;

public final class SqlmqClient {

    private final DataSource ds;

    public SqlmqClient(DataSource ds) {
        this.ds = ds;
    }

    /**
     * Create a queue. As of V014/V015 the only supported storage variant is
     * on-disk; the {@code @storage} proc parameter is no longer surfaced
     * through the client API. The Java client always passes
     * {@code @storage='ondisk'} under the hood — sqlmq.create_queue's CHECK
     * constraint and explicit THROW reject anything else, so callers cannot
     * accidentally select a retired variant.
     */
    public void createQueue(String name, boolean grouped,
                            String payloadType, Integer maxDeliveryCount) throws SQLException {
        try (Connection c = ds.getConnection();
             CallableStatement cs = c.prepareCall(
                 "{call sqlmq.create_queue(?, ?, ?, ?, ?)}")) {
            cs.setString(1, name);
            cs.setString(2, "ondisk");
            cs.setBoolean(3, grouped);
            cs.setString(4, payloadType);
            if (maxDeliveryCount == null) cs.setNull(5, java.sql.Types.INTEGER);
            else cs.setInt(5, maxDeliveryCount);
            cs.execute();
        }
    }

    public void dropQueue(String name) throws SQLException {
        try (Connection c = ds.getConnection();
             CallableStatement cs = c.prepareCall("{call sqlmq.drop_queue(?)}")) {
            cs.setString(1, name);
            cs.execute();
        }
    }

    public List<QueueInfo> listQueues() throws SQLException {
        // sqlmq.meta.created_at is DATETIME2(7) populated by SYSUTCDATETIME(), so we must
        // tell the driver to interpret the value as UTC rather than the JVM default zone.
        // Passing a UTC Calendar to getTimestamp does exactly that. (mssql-jdbc only
        // supports getObject(OffsetDateTime.class) on DATETIMEOFFSET columns; on
        // DATETIME2 it raises 'conversion from datetime2 to DATETIMEOFFSET is unsupported'.)
        var utcCal = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
        var out = new ArrayList<QueueInfo>();
        try (Connection c = ds.getConnection();
             var st = c.createStatement();
             ResultSet rs = st.executeQuery(
                 "SELECT queue_name, storage_type, is_grouped, payload_type, " +
                 "       max_delivery_count, created_at FROM sqlmq.queues()")) {
            while (rs.next()) {
                out.add(new QueueInfo(
                    rs.getString("queue_name"),
                    rs.getString("storage_type"),
                    rs.getBoolean("is_grouped"),
                    rs.getString("payload_type"),
                    rs.getObject("max_delivery_count") == null ? null : rs.getInt("max_delivery_count"),
                    rs.getTimestamp("created_at", utcCal).toInstant()
                ));
            }
        }
        return out;
    }

    public long send(String queue, String message, String headers) throws SQLException {
        return sendInternal(queue, message, null, headers, 0);
    }

    public long sendDelayed(String queue, String message, String headers, int delaySeconds) throws SQLException {
        return sendInternal(queue, message, null, headers, delaySeconds);
    }

    public long sendBinary(String queue, byte[] message, String headers) throws SQLException {
        return sendInternal(queue, null, message, headers, 0);
    }

    private long sendInternal(String queue, String msg, byte[] msgBin, String headers, int delaySeconds) throws SQLException {
        try (Connection c = ds.getConnection();
             CallableStatement cs = c.prepareCall("{call sqlmq.send(?, ?, ?, ?, ?, ?)}")) {
            cs.setString(1, queue);
            if (msg == null) cs.setNull(2, java.sql.Types.NVARCHAR); else cs.setString(2, msg);
            if (msgBin == null) cs.setNull(3, java.sql.Types.VARBINARY); else cs.setBytes(3, msgBin);
            if (headers == null) cs.setNull(4, java.sql.Types.NVARCHAR); else cs.setString(4, headers);
            cs.setInt(5, delaySeconds);
            cs.setNull(6, java.sql.Types.NVARCHAR);
            try (ResultSet rs = cs.executeQuery()) {
                rs.next();
                return rs.getLong("msg_id");
            }
        }
    }

    public long sendGrouped(String queue, String message, String groupKey) throws SQLException {
        try (Connection c = ds.getConnection();
             CallableStatement cs = c.prepareCall("{call sqlmq.send(?, ?, ?, ?, ?, ?)}")) {
            cs.setString(1, queue);
            cs.setString(2, message);
            cs.setNull(3, java.sql.Types.VARBINARY);
            cs.setNull(4, java.sql.Types.NVARCHAR);
            cs.setInt(5, 0);
            cs.setString(6, groupKey);
            try (ResultSet rs = cs.executeQuery()) {
                rs.next();
                return rs.getLong("msg_id");
            }
        }
    }

    public List<Message> readGrouped(String queue, int vtSeconds, int maxCount) throws SQLException {
        var out = new ArrayList<Message>();
        try (Connection c = ds.getConnection();
             CallableStatement cs = c.prepareCall("{call sqlmq.read_grouped(?, ?, ?)}")) {
            cs.setString(1, queue);
            cs.setInt(2, vtSeconds);
            cs.setInt(3, maxCount);
            try (ResultSet rs = cs.executeQuery()) {
                var meta = rs.getMetaData();
                boolean binary = false;
                for (int i = 1; i <= meta.getColumnCount(); i++) {
                    if ("message_bin".equalsIgnoreCase(meta.getColumnLabel(i))) { binary = true; break; }
                }
                var utc = java.util.Calendar.getInstance(java.util.TimeZone.getTimeZone("UTC"));
                while (rs.next()) {
                    out.add(new Message(
                        rs.getLong("msg_id"),
                        rs.getInt("read_ct"),
                        rs.getTimestamp("enqueued_at", utc).toInstant(),
                        rs.getTimestamp("vt", utc).toInstant(),
                        rs.getString("group_key"),
                        binary ? null : rs.getString("message"),
                        binary ? rs.getBytes("message_bin") : null,
                        rs.getString("headers")
                    ));
                }
            }
        }
        return out;
    }

    public List<Long> sendBatch(String queue, List<String> jsonMessages) throws SQLException {
        var tvp = new com.microsoft.sqlserver.jdbc.SQLServerDataTable();
        tvp.addColumnMetadata("message",       java.sql.Types.NVARCHAR);
        tvp.addColumnMetadata("message_bin",   java.sql.Types.VARBINARY);
        tvp.addColumnMetadata("headers",       java.sql.Types.NVARCHAR);
        tvp.addColumnMetadata("delay_seconds", java.sql.Types.INTEGER);
        for (var m : jsonMessages) tvp.addRow(m, null, null, 0);

        var ids = new ArrayList<Long>();
        try (Connection c = ds.getConnection();
             CallableStatement cs = c.prepareCall("{call sqlmq.send_batch(?, ?)}")) {
            cs.setString(1, queue);
            cs.unwrap(com.microsoft.sqlserver.jdbc.SQLServerCallableStatement.class)
                .setStructured(2, "dbo.sqlmq_send_tvp", tvp);
            try (ResultSet rs = cs.executeQuery()) {
                while (rs.next()) ids.add(rs.getLong("msg_id"));
            }
        }
        return ids;
    }

    public List<Message> read(String queue, int vtSeconds, int maxCount) throws SQLException {
        var out = new ArrayList<Message>();
        try (Connection c = ds.getConnection();
             CallableStatement cs = c.prepareCall("{call sqlmq.[read](?, ?, ?)}")) {
            cs.setString(1, queue);
            cs.setInt(2, vtSeconds);
            cs.setInt(3, maxCount);
            try (ResultSet rs = cs.executeQuery()) {
                var meta = rs.getMetaData();
                boolean binary = false;
                for (int i = 1; i <= meta.getColumnCount(); i++) {
                    if ("message_bin".equalsIgnoreCase(meta.getColumnLabel(i))) { binary = true; break; }
                }
                // For DATETIME2 columns, use UTC Calendar (same fix as listQueues).
                var utc = java.util.Calendar.getInstance(java.util.TimeZone.getTimeZone("UTC"));
                while (rs.next()) {
                    out.add(new Message(
                        rs.getLong("msg_id"),
                        rs.getInt("read_ct"),
                        rs.getTimestamp("enqueued_at", utc).toInstant(),
                        rs.getTimestamp("vt", utc).toInstant(),
                        rs.getString("group_key"),
                        binary ? null : rs.getString("message"),
                        binary ? rs.getBytes("message_bin") : null,
                        rs.getString("headers")
                    ));
                }
            }
        }
        return out;
    }

    private com.microsoft.sqlserver.jdbc.SQLServerDataTable msgIdTvp(java.util.Collection<Long> ids) throws SQLException {
        var t = new com.microsoft.sqlserver.jdbc.SQLServerDataTable();
        t.addColumnMetadata("msg_id", java.sql.Types.BIGINT);
        for (long id : ids) t.addRow(id);
        return t;
    }

    public int delete(String queue, java.util.Collection<Long> msgIds) throws SQLException {
        if (msgIds.isEmpty()) return 0;
        try (Connection c = ds.getConnection();
             CallableStatement cs = c.prepareCall("{call sqlmq.[delete](?, ?)}")) {
            cs.setString(1, queue);
            cs.unwrap(com.microsoft.sqlserver.jdbc.SQLServerCallableStatement.class)
                .setStructured(2, "dbo.sqlmq_msg_id_tvp", msgIdTvp(msgIds));
            try (ResultSet rs = cs.executeQuery()) {
                rs.next();
                return rs.getInt("rows_deleted");
            }
        }
    }

    public int archive(String queue, java.util.Collection<Long> msgIds, String reason) throws SQLException {
        if (msgIds.isEmpty()) return 0;
        try (Connection c = ds.getConnection();
             CallableStatement cs = c.prepareCall("{call sqlmq.archive(?, ?, ?)}")) {
            cs.setString(1, queue);
            cs.unwrap(com.microsoft.sqlserver.jdbc.SQLServerCallableStatement.class)
                .setStructured(2, "dbo.sqlmq_msg_id_tvp", msgIdTvp(msgIds));
            if (reason == null) cs.setNull(3, java.sql.Types.NVARCHAR); else cs.setString(3, reason);
            try (ResultSet rs = cs.executeQuery()) {
                rs.next();
                return rs.getInt("rows_archived");
            }
        }
    }

    public java.util.Optional<Message> pop(String queue) throws SQLException {
        try (Connection c = ds.getConnection();
             CallableStatement cs = c.prepareCall("{call sqlmq.pop(?)}")) {
            cs.setString(1, queue);
            try (ResultSet rs = cs.executeQuery()) {
                if (!rs.next()) return java.util.Optional.empty();
                var meta = rs.getMetaData();
                boolean binary = false;
                for (int i = 1; i <= meta.getColumnCount(); i++) {
                    if ("message_bin".equalsIgnoreCase(meta.getColumnLabel(i))) { binary = true; break; }
                }
                var utc = java.util.Calendar.getInstance(java.util.TimeZone.getTimeZone("UTC"));
                return java.util.Optional.of(new Message(
                    rs.getLong("msg_id"),
                    rs.getInt("read_ct"),
                    rs.getTimestamp("enqueued_at", utc).toInstant(),
                    rs.getTimestamp("vt", utc).toInstant(),
                    rs.getString("group_key"),
                    binary ? null : rs.getString("message"),
                    binary ? rs.getBytes("message_bin") : null,
                    rs.getString("headers")
                ));
            }
        }
    }

    public Metrics metrics(String queue) throws SQLException {
        try (Connection c = ds.getConnection();
             CallableStatement cs = c.prepareCall("{call sqlmq.metrics(?)}")) {
            cs.setString(1, queue);
            try (ResultSet rs = cs.executeQuery()) {
                rs.next();
                return new Metrics(
                    rs.getString("queue_name"),
                    rs.getLong("queue_length"),
                    rs.getLong("total_messages"),
                    (Integer) rs.getObject("oldest_msg_age_seconds"),
                    (Integer) rs.getObject("newest_msg_age_seconds"),
                    rs.getLong("dlq_count"));
            }
        }
    }

    public List<Metrics> metricsAll() throws SQLException {
        var out = new ArrayList<Metrics>();
        try (Connection c = ds.getConnection();
             CallableStatement cs = c.prepareCall("{call sqlmq.metrics_all()}")) {
            try (ResultSet rs = cs.executeQuery()) {
                while (rs.next())
                    out.add(new Metrics(
                        rs.getString("queue_name"),
                        rs.getLong("queue_length"),
                        rs.getLong("total_messages"),
                        (Integer) rs.getObject("oldest_msg_age_seconds"),
                        (Integer) rs.getObject("newest_msg_age_seconds"),
                        rs.getLong("dlq_count")));
            }
        }
        return out;
    }

    public int purgeQueue(String name) throws SQLException {
        try (Connection c = ds.getConnection();
             CallableStatement cs = c.prepareCall("{call sqlmq.purge_queue(?)}")) {
            cs.setString(1, name);
            try (ResultSet rs = cs.executeQuery()) {
                rs.next();
                return rs.getInt("rows_deleted");
            }
        }
    }

    public int dlqSweep(String queue) throws SQLException {
        try (Connection c = ds.getConnection();
             CallableStatement cs = c.prepareCall("{call sqlmq.dlq_sweep(?)}")) {
            cs.setString(1, queue);
            try (ResultSet rs = cs.executeQuery()) {
                rs.next();
                return rs.getInt("rows_swept");
            }
        }
    }

    public record QueueInfo(
        String name, String storageType, boolean grouped, String payloadType,
        Integer maxDeliveryCount, java.time.Instant createdAt) {}

    public record Message(
        long msgId, int readCt, java.time.Instant enqueuedAt, java.time.Instant vt,
        String groupKey, String message, byte[] messageBin, String headers) {}

    public record Metrics(String queueName, long queueLength, long totalMessages,
                          Integer oldestMsgAgeSeconds, Integer newestMsgAgeSeconds,
                          long dlqCount) {}
}
