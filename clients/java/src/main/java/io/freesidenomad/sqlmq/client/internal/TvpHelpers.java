package io.freesidenomad.sqlmq.client.internal;

import com.microsoft.sqlserver.jdbc.SQLServerDataTable;

import java.sql.SQLException;
import java.sql.Types;
import java.util.Collection;

/**
 * Factories for the two SQL Server table-valued parameter (TVP) types
 * sqlmq's procs accept:
 *
 * <ul>
 *   <li>{@code dbo.sqlmq_send_tvp} — used by {@code sqlmq.send_batch}</li>
 *   <li>{@code dbo.sqlmq_msg_id_tvp} — used by {@code sqlmq.[delete]} and
 *       {@code sqlmq.archive}</li>
 * </ul>
 *
 * <p>mssql-jdbc supports TVPs natively via
 * {@link com.microsoft.sqlserver.jdbc.SQLServerCallableStatement#setStructured}.
 * Unlike pyodbc (no native TVP support, must declare-and-EXEC), Java callers
 * pass a {@code SQLServerDataTable} directly — one round trip, no T-SQL
 * scaffolding.</p>
 */
public final class TvpHelpers {

    public static final String SEND_TVP_NAME    = "dbo.sqlmq_send_tvp";
    public static final String MSG_ID_TVP_NAME  = "dbo.sqlmq_msg_id_tvp";

    private TvpHelpers() {}

    /**
     * Build a {@code dbo.sqlmq_send_tvp} table.
     *
     * <p>Columns: {@code (message NVARCHAR(MAX), message_bin VARBINARY(MAX),
     * headers NVARCHAR(MAX), delay_seconds INT)}.</p>
     *
     * <p>For JSON-payload queues we always set {@code message_bin} and
     * {@code headers} to {@code NULL}. The strict pgmq-compat client does
     * not surface binary payloads or headers.</p>
     */
    public static SQLServerDataTable buildSendTvp(Iterable<String> jsonMessages, int delaySeconds)
            throws SQLException {
        var t = new SQLServerDataTable();
        t.addColumnMetadata("message",       Types.NVARCHAR);
        t.addColumnMetadata("message_bin",   Types.VARBINARY);
        t.addColumnMetadata("headers",       Types.NVARCHAR);
        t.addColumnMetadata("delay_seconds", Types.INTEGER);
        for (var msg : jsonMessages) {
            t.addRow(msg, null, null, delaySeconds);
        }
        return t;
    }

    /**
     * Build a {@code dbo.sqlmq_msg_id_tvp} table containing one BIGINT column.
     */
    public static SQLServerDataTable buildMsgIdTvp(Collection<Long> msgIds) throws SQLException {
        var t = new SQLServerDataTable();
        t.addColumnMetadata("msg_id", Types.BIGINT);
        for (long id : msgIds) {
            t.addRow(id);
        }
        return t;
    }
}
