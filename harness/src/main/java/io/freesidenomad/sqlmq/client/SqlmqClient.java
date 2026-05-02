package io.freesidenomad.sqlmq.client;

import javax.sql.DataSource;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public final class SqlmqClient {

    private final DataSource ds;

    public SqlmqClient(DataSource ds) {
        this.ds = ds;
    }

    public void createQueue(String name, String storage, boolean grouped,
                            String payloadType, Integer maxDeliveryCount) throws SQLException {
        try (Connection c = ds.getConnection();
             CallableStatement cs = c.prepareCall(
                 "{call sqlmq.create_queue(?, ?, ?, ?, ?)}")) {
            cs.setString(1, name);
            cs.setString(2, storage);
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
                    rs.getTimestamp("created_at").toInstant()
                ));
            }
        }
        return out;
    }

    public record QueueInfo(
        String name, String storageType, boolean grouped, String payloadType,
        Integer maxDeliveryCount, java.time.Instant createdAt) {}
}
