CREATE PROCEDURE sqlmq.purge_queue
    @name SYSNAME
AS
BEGIN
    SET NOCOUNT ON;
    IF NOT EXISTS (SELECT 1 FROM sqlmq.meta WHERE queue_name = @name)
        THROW 50070, 'Queue does not exist.', 1;
    DECLARE @qtable SYSNAME = N'q_' + @name;
    DECLARE @sql NVARCHAR(MAX) = N'
        DELETE FROM sqlmq.' + QUOTENAME(@qtable) + N';
        SELECT @@ROWCOUNT AS rows_deleted;';
    EXEC sp_executesql @sql;
END
GO

-- dlq_sweep: move all over-cap messages from active queue to archive with dlq_reason='max_deliveries'.
-- Always interpreted (cross-engine for in-memory queues).
CREATE PROCEDURE sqlmq.dlq_sweep
    @queue SYSNAME
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @storage VARCHAR(16), @payload VARCHAR(8), @max_dlq INT;
    SELECT @storage = storage_type, @payload = payload_type, @max_dlq = max_delivery_count
      FROM sqlmq.meta WHERE queue_name = @queue;
    IF @storage IS NULL THROW 50071, 'Queue does not exist.', 1;
    IF @max_dlq IS NULL
    BEGIN
        SELECT 0 AS rows_swept;
        RETURN;
    END

    DECLARE @qtable SYSNAME = N'q_' + @queue;
    DECLARE @atable SYSNAME = N'a_' + @queue;
    DECLARE @msg_col NVARCHAR(64) = CASE @payload WHEN 'json' THEN N'message' ELSE N'message_bin' END;

    DECLARE @sql NVARCHAR(MAX) = N'
        DELETE q
          OUTPUT deleted.msg_id, deleted.enqueued_at, deleted.vt, deleted.read_ct,
                 deleted.group_key, deleted.' + @msg_col + N', deleted.headers,
                 SYSUTCDATETIME(), N''max_deliveries''
            INTO sqlmq.' + QUOTENAME(@atable) + N' (
                 msg_id, enqueued_at, vt, read_ct, group_key, ' + @msg_col + N', headers,
                 archived_at, dlq_reason)
          FROM sqlmq.' + QUOTENAME(@qtable) + N' AS q WITH (READPAST, UPDLOCK, ROWLOCK, READCOMMITTEDLOCK)
         WHERE q.read_ct >= @dlq;
        SELECT @@ROWCOUNT AS rows_swept;';
    EXEC sp_executesql @sql, N'@dlq INT', @dlq = @max_dlq;
END
GO
