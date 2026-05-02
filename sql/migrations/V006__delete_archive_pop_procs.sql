-- V006: sqlmq.delete (storage-dispatched), sqlmq.archive (always interpreted; cross-engine for in-memory),
-- sqlmq.pop (read + delete in one statement, in caller's tx).

CREATE PROCEDURE sqlmq._delete_ondisk
    @queue   SYSNAME,
    @msg_ids dbo.sqlmq_msg_id_tvp READONLY
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @qtable SYSNAME = N'q_' + @queue;
    DECLARE @sql NVARCHAR(MAX) = N'
        DELETE q
          FROM sqlmq.' + QUOTENAME(@qtable) + N' AS q
         INNER JOIN @ids AS i ON q.msg_id = i.msg_id;
        SELECT @@ROWCOUNT AS rows_deleted;';
    EXEC sp_executesql @sql, N'@ids dbo.sqlmq_msg_id_tvp READONLY', @ids = @msg_ids;
END
GO

CREATE PROCEDURE sqlmq.[delete]
    @queue   SYSNAME,
    @msg_ids dbo.sqlmq_msg_id_tvp READONLY
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @storage VARCHAR(16);
    SELECT @storage = storage_type FROM sqlmq.meta WHERE queue_name = @queue;
    IF @storage IS NULL THROW 50050, 'Queue does not exist.', 1;
    IF @storage = 'ondisk' EXEC sqlmq._delete_ondisk @queue, @msg_ids;
    ELSE THROW 50051, 'In-memory delete not implemented yet (V010).', 1;
END
GO

-- Archive is always interpreted (cross-engine for in-memory queues).
CREATE PROCEDURE sqlmq.archive
    @queue   SYSNAME,
    @msg_ids dbo.sqlmq_msg_id_tvp READONLY,
    @reason  NVARCHAR(64) = NULL
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @storage VARCHAR(16), @payload VARCHAR(8);
    SELECT @storage = storage_type, @payload = payload_type
      FROM sqlmq.meta WHERE queue_name = @queue;
    IF @storage IS NULL THROW 50052, 'Queue does not exist.', 1;

    DECLARE @qtable SYSNAME = N'q_' + @queue;
    DECLARE @atable SYSNAME = N'a_' + @queue;
    DECLARE @msg_col NVARCHAR(64) =
        CASE @payload WHEN 'json' THEN N'message' ELSE N'message_bin' END;

    DECLARE @sql NVARCHAR(MAX) = N'
        DELETE q
          OUTPUT deleted.msg_id, deleted.enqueued_at, deleted.vt, deleted.read_ct,
                 deleted.group_key, deleted.' + @msg_col + N', deleted.headers,
                 SYSUTCDATETIME(), @reason
            INTO sqlmq.' + QUOTENAME(@atable) + N' (
                 msg_id, enqueued_at, vt, read_ct, group_key, ' + @msg_col + N', headers,
                 archived_at, dlq_reason)
          FROM sqlmq.' + QUOTENAME(@qtable) + N' AS q
         INNER JOIN @ids AS i ON q.msg_id = i.msg_id;
        SELECT @@ROWCOUNT AS rows_archived;';

    EXEC sp_executesql @sql,
        N'@ids dbo.sqlmq_msg_id_tvp READONLY, @reason NVARCHAR(64)',
        @ids = @msg_ids, @reason = @reason;
END
GO

-- pop: read + delete in one statement, in caller's tx.
CREATE PROCEDURE sqlmq._pop_ondisk
    @queue        SYSNAME,
    @payload_type VARCHAR(8)
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @qtable SYSNAME = N'q_' + @queue;
    DECLARE @msg_col NVARCHAR(64) =
        CASE @payload_type WHEN 'json' THEN N'message' ELSE N'message_bin' END;
    DECLARE @sql NVARCHAR(MAX) = N'
        DELETE TOP (1) q
          OUTPUT deleted.msg_id, deleted.read_ct, deleted.enqueued_at, deleted.vt,
                 deleted.' + @msg_col + N', deleted.headers
          FROM sqlmq.' + QUOTENAME(@qtable) + N' AS q WITH (READPAST, UPDLOCK, ROWLOCK, READCOMMITTEDLOCK)
         WHERE q.vt <= SYSUTCDATETIME()
           AND q.msg_id = (
               SELECT TOP (1) msg_id FROM sqlmq.' + QUOTENAME(@qtable) +
                   N' WITH (READPAST, UPDLOCK, ROWLOCK, READCOMMITTEDLOCK)
                WHERE vt <= SYSUTCDATETIME() ORDER BY msg_id);';
    EXEC sp_executesql @sql;
END
GO

CREATE PROCEDURE sqlmq.pop
    @queue SYSNAME
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @storage VARCHAR(16), @payload VARCHAR(8);
    SELECT @storage = storage_type, @payload = payload_type
      FROM sqlmq.meta WHERE queue_name = @queue;
    IF @storage IS NULL THROW 50053, 'Queue does not exist.', 1;
    IF @storage = 'ondisk' EXEC sqlmq._pop_ondisk @queue, @payload;
    ELSE THROW 50054, 'In-memory pop not implemented yet (V010).', 1;
END
GO
