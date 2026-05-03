package io.freesidenomad.sqlmq.client.internal;

/**
 * Centralized JDBC {@code CALL} strings for sqlmq stored procedures.
 *
 * <p>Kept in one place so the surface map between this client and the
 * Python client (which has its own {@code _sql.py}) is auditable side
 * by side.</p>
 */
public final class SqlStatements {

    private SqlStatements() {}

    public static final String CREATE_QUEUE = "{call sqlmq.create_queue(?, ?, ?, ?, ?)}";
    public static final String DROP_QUEUE   = "{call sqlmq.drop_queue(?)}";
    public static final String LIST_QUEUES  = "SELECT queue_name FROM sqlmq.queues()";
    public static final String SEND         = "{call sqlmq.send(?, ?, ?, ?, ?)}";
    public static final String SEND_BATCH   = "{call sqlmq.send_batch(?, ?)}";
    public static final String READ         = "{call sqlmq.[read](?, ?, ?)}";
    public static final String POP          = "{call sqlmq.pop(?)}";
    public static final String DELETE       = "{call sqlmq.[delete](?, ?)}";
    public static final String ARCHIVE      = "{call sqlmq.archive(?, ?, ?)}";
    public static final String PURGE        = "{call sqlmq.purge_queue(?)}";
    public static final String METRICS      = "{call sqlmq.metrics(?)}";
    public static final String METRICS_ALL  = "{call sqlmq.metrics_all()}";
}
