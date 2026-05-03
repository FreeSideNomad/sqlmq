-- V005: sqlmq.read (dispatcher + on-disk inner). Uses READPAST/UPDLOCK/ROWLOCK/READCOMMITTEDLOCK
-- to atomically claim candidates and bump vt + read_ct in one statement. Excludes messages
-- that have hit the DLQ cap (the sweep proc handles them later).

CREATE PROCEDURE sqlmq._read_ondisk
    @queue       SYSNAME,
    @payload_type VARCHAR(8),
    @vt_seconds  INT,
    @max_count   INT,
    @max_dlq     INT
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @qtable SYSNAME = N'q_' + @queue;
    DECLARE @sql NVARCHAR(MAX);
    DECLARE @msg_col NVARCHAR(64) =
        CASE @payload_type WHEN 'json' THEN N'message' ELSE N'message_bin' END;

    SET @sql = N'
        WITH claimed AS (
            SELECT TOP (@n) msg_id
            FROM sqlmq.' + QUOTENAME(@qtable) + N'
                WITH (READPAST, UPDLOCK, ROWLOCK, READCOMMITTEDLOCK)
            WHERE vt <= SYSUTCDATETIME()
              AND (@dlq IS NULL OR read_ct < @dlq)
            ORDER BY msg_id
        )
        UPDATE q
           SET vt = DATEADD(SECOND, @vt, SYSUTCDATETIME()),
               read_ct = read_ct + 1
        OUTPUT inserted.msg_id, inserted.read_ct, inserted.enqueued_at,
               inserted.vt, inserted.' + @msg_col + N', inserted.headers
          FROM sqlmq.' + QUOTENAME(@qtable) + N' AS q
         INNER JOIN claimed AS c ON q.msg_id = c.msg_id;';

    EXEC sp_executesql @sql,
        N'@n INT, @vt INT, @dlq INT',
        @n = @max_count, @vt = @vt_seconds, @dlq = @max_dlq;
END
GO

CREATE PROCEDURE sqlmq.[read]
    @queue      SYSNAME,
    @vt_seconds INT,
    @max_count  INT = 1
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @storage VARCHAR(16), @payload VARCHAR(8), @max_dlq INT;
    SELECT @storage = storage_type, @payload = payload_type, @max_dlq = max_delivery_count
      FROM sqlmq.meta WHERE queue_name = @queue;
    IF @storage IS NULL THROW 50040, 'Queue does not exist.', 1;

    IF @storage = 'ondisk'
        EXEC sqlmq._read_ondisk @queue, @payload, @vt_seconds, @max_count, @max_dlq;
    ELSE
        THROW 50041, 'In-memory read not implemented yet (V010).', 1;
END
GO
