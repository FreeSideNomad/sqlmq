-- V009: sqlmq.read_grouped (grouped-FIFO read variant) for on-disk queues.
-- Enforces: for any group_key with an in-flight (vt > now) message, no other message
-- with the same group_key may be returned until the in-flight one is deleted, archived,
-- or its vt expires.
--
-- Also extends sqlmq.send / sqlmq._send_ondisk with @group_key parameter (CREATE OR ALTER
-- so this replaces V004's definitions). Existing positional callers continue to work
-- because @group_key has a default of NULL.
--
-- Also extends sqlmq._read_ondisk and sqlmq._pop_ondisk to OUTPUT group_key so the Java
-- Message record can carry it through.

CREATE PROCEDURE sqlmq._read_grouped_ondisk
    @queue        SYSNAME,
    @payload_type VARCHAR(8),
    @vt_seconds   INT,
    @max_count    INT,
    @max_dlq      INT
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @qtable SYSNAME = N'q_' + @queue;
    DECLARE @msg_col NVARCHAR(64) = CASE @payload_type WHEN 'json' THEN N'message' ELSE N'message_bin' END;

    -- Serialize read_grouped per queue via sp_getapplock. The grouped-FIFO invariant
    -- ("at most one in-flight message per group_key") cannot be enforced with row locks
    -- alone under READPAST + a NOT EXISTS subquery: the readers' UPDLOCKs sit on the
    -- clustered index (PK msg_id), but the inflight subquery's view of vt > now reads the
    -- nonclustered (group_key, vt, msg_id) filtered index, which is not locked by the
    -- eligible CTE's UPDLOCK on the clustered index. So a concurrent reader can see the
    -- pre-bump vt and conclude there is no inflight row in the group. A queue-scoped
    -- applock makes the eligible/claim/update statement atomic across all readers of the
    -- same queue. Cross-queue concurrency is unaffected.
    --
    -- An explicit BEGIN/COMMIT TRANSACTION is required because @LockOwner='Transaction'
    -- needs a real transaction to attach to. With JDBC autocommit there is one, but
    -- BEGIN+COMMIT inside the proc is also safe (nested COMMIT just decrements TRANCOUNT;
    -- the autocommit COMMIT at end of statement actually releases the applock + UPDLOCKs).
    BEGIN TRANSACTION;

    DECLARE @lockres INT;
    EXEC @lockres = sp_getapplock
        @Resource    = @queue,
        @LockMode    = N'Exclusive',
        @LockOwner   = N'Transaction',
        @LockTimeout = 30000;
    IF @lockres < 0
    BEGIN
        ROLLBACK TRANSACTION;
        THROW 50083, 'sp_getapplock failed for read_grouped', 1;
    END

    DECLARE @sql NVARCHAR(MAX) = N'
        WITH eligible AS (
            SELECT q.msg_id, q.group_key,
                   ROW_NUMBER() OVER (PARTITION BY q.group_key ORDER BY q.msg_id) AS rn
              FROM sqlmq.' + QUOTENAME(@qtable) + N' AS q
                   WITH (READPAST, UPDLOCK, ROWLOCK, READCOMMITTEDLOCK)
             WHERE q.vt <= SYSUTCDATETIME()
               AND (@dlq IS NULL OR q.read_ct < @dlq)
               AND (q.group_key IS NULL OR NOT EXISTS (
                    SELECT 1
                      FROM sqlmq.' + QUOTENAME(@qtable) + N' AS inflight
                     WHERE inflight.group_key = q.group_key
                       AND inflight.vt > SYSUTCDATETIME()))
        ),
        claimed AS (
            SELECT TOP (@n) msg_id
              FROM eligible
             WHERE rn = 1
             ORDER BY msg_id
        )
        UPDATE q
           SET vt = DATEADD(SECOND, @vt, SYSUTCDATETIME()),
               read_ct = read_ct + 1
        OUTPUT inserted.msg_id, inserted.read_ct, inserted.enqueued_at,
               inserted.vt, inserted.group_key,
               inserted.' + @msg_col + N', inserted.headers
          FROM sqlmq.' + QUOTENAME(@qtable) + N' AS q
         INNER JOIN claimed AS c ON q.msg_id = c.msg_id;';

    EXEC sp_executesql @sql,
        N'@n INT, @vt INT, @dlq INT',
        @n = @max_count, @vt = @vt_seconds, @dlq = @max_dlq;

    COMMIT TRANSACTION;
END
GO

CREATE PROCEDURE sqlmq.read_grouped
    @queue      SYSNAME,
    @vt_seconds INT,
    @max_count  INT = 1
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @storage VARCHAR(16), @payload VARCHAR(8), @max_dlq INT, @grouped BIT;
    SELECT @storage = storage_type, @payload = payload_type,
           @max_dlq = max_delivery_count, @grouped = is_grouped
      FROM sqlmq.meta WHERE queue_name = @queue;
    IF @storage IS NULL THROW 50080, 'Queue does not exist.', 1;
    IF @grouped = 0 THROW 50081, 'Queue was not created with grouping enabled.', 1;
    IF @storage = 'ondisk'
        EXEC sqlmq._read_grouped_ondisk @queue, @payload, @vt_seconds, @max_count, @max_dlq;
    ELSE
        THROW 50082, 'In-memory grouped read not implemented yet (V010).', 1;
END
GO

-- Extend send to accept @group_key. Use CREATE OR ALTER so this replaces V004's definitions.
CREATE OR ALTER PROCEDURE sqlmq._send_ondisk
    @queue          SYSNAME,
    @payload_type   VARCHAR(8),
    @message        NVARCHAR(MAX) = NULL,
    @message_bin    VARBINARY(MAX) = NULL,
    @headers        NVARCHAR(MAX) = NULL,
    @delay_seconds  INT = 0,
    @group_key      NVARCHAR(255) = NULL,
    @msg_id         BIGINT OUTPUT
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @qtable SYSNAME = N'q_' + @queue;
    DECLARE @vt DATETIME2(7) = DATEADD(SECOND, @delay_seconds, SYSUTCDATETIME());
    DECLARE @sql NVARCHAR(MAX);
    DECLARE @ids TABLE (id BIGINT);
    IF @payload_type = 'json'
    BEGIN
        IF @message IS NULL THROW 50020, '@message required for json queue', 1;
        SET @sql = N'INSERT INTO sqlmq.' + QUOTENAME(@qtable) +
            N' (vt, group_key, message, headers) OUTPUT inserted.msg_id VALUES (@vt, @gk, @msg, @hdr);';
        INSERT INTO @ids EXEC sp_executesql @sql,
            N'@vt DATETIME2(7), @gk NVARCHAR(255), @msg NVARCHAR(MAX), @hdr NVARCHAR(MAX)',
            @vt = @vt, @gk = @group_key, @msg = @message, @hdr = @headers;
    END
    ELSE
    BEGIN
        IF @message_bin IS NULL THROW 50021, '@message_bin required for binary queue', 1;
        SET @sql = N'INSERT INTO sqlmq.' + QUOTENAME(@qtable) +
            N' (vt, group_key, message_bin, headers) OUTPUT inserted.msg_id VALUES (@vt, @gk, @msg, @hdr);';
        INSERT INTO @ids EXEC sp_executesql @sql,
            N'@vt DATETIME2(7), @gk NVARCHAR(255), @msg VARBINARY(MAX), @hdr NVARCHAR(MAX)',
            @vt = @vt, @gk = @group_key, @msg = @message_bin, @hdr = @headers;
    END
    SELECT @msg_id = id FROM @ids;
END
GO

CREATE OR ALTER PROCEDURE sqlmq.send
    @queue         SYSNAME,
    @message       NVARCHAR(MAX) = NULL,
    @message_bin   VARBINARY(MAX) = NULL,
    @headers       NVARCHAR(MAX) = NULL,
    @delay_seconds INT = 0,
    @group_key     NVARCHAR(255) = NULL
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @storage VARCHAR(16), @payload VARCHAR(8);
    SELECT @storage = storage_type, @payload = payload_type
      FROM sqlmq.meta WHERE queue_name = @queue;
    IF @storage IS NULL THROW 50030, 'Queue does not exist.', 1;
    DECLARE @msg_id BIGINT;
    IF @storage = 'ondisk'
        EXEC sqlmq._send_ondisk @queue, @payload, @message, @message_bin, @headers,
                                 @delay_seconds, @group_key, @msg_id OUTPUT;
    ELSE
        THROW 50031, 'In-memory send not implemented yet (V010).', 1;
    SELECT @msg_id AS msg_id;
END
GO

-- Extend ungrouped read to OUTPUT group_key as well (so the Java Message record carries it).
CREATE OR ALTER PROCEDURE sqlmq._read_ondisk
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
               inserted.vt, inserted.group_key, inserted.' + @msg_col + N', inserted.headers
          FROM sqlmq.' + QUOTENAME(@qtable) + N' AS q
         INNER JOIN claimed AS c ON q.msg_id = c.msg_id;';

    EXEC sp_executesql @sql,
        N'@n INT, @vt INT, @dlq INT',
        @n = @max_count, @vt = @vt_seconds, @dlq = @max_dlq;
END
GO

CREATE OR ALTER PROCEDURE sqlmq._pop_ondisk
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
                 deleted.group_key, deleted.' + @msg_col + N', deleted.headers
          FROM sqlmq.' + QUOTENAME(@qtable) + N' AS q WITH (READPAST, UPDLOCK, ROWLOCK, READCOMMITTEDLOCK)
         WHERE q.vt <= SYSUTCDATETIME()
           AND q.msg_id = (
               SELECT TOP (1) msg_id FROM sqlmq.' + QUOTENAME(@qtable) +
                   N' WITH (READPAST, UPDLOCK, ROWLOCK, READCOMMITTEDLOCK)
                WHERE vt <= SYSUTCDATETIME() ORDER BY msg_id);';
    EXEC sp_executesql @sql;
END
GO
