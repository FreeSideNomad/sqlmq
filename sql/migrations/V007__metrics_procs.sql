CREATE PROCEDURE sqlmq.metrics
    @queue SYSNAME
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @storage VARCHAR(16);
    SELECT @storage = storage_type FROM sqlmq.meta WHERE queue_name = @queue;
    IF @storage IS NULL THROW 50060, 'Queue does not exist.', 1;

    DECLARE @qtable SYSNAME = N'q_' + @queue;
    DECLARE @atable SYSNAME = N'a_' + @queue;

    -- total_messages = largest msg_id ever issued for this queue (including deleted/archived).
    -- Read sys.identity_columns.last_value: it is NULL until the first insert, then holds the
    -- last identity value. IDENT_CURRENT returns the seed (1) before any insert, which would
    -- make an empty queue report total_messages=1 incorrectly.
    DECLARE @sql NVARCHAR(MAX) = N'
        SELECT
            @qn AS queue_name,
            (SELECT COUNT_BIG(*) FROM sqlmq.' + QUOTENAME(@qtable) + N') AS queue_length,
            ISNULL(CAST((SELECT TOP (1) last_value
                           FROM sys.identity_columns
                          WHERE object_id = OBJECT_ID(''sqlmq.' + REPLACE(@qtable, N'''', N'''''') + N''')) AS BIGINT), 0)
                AS total_messages,
            (SELECT DATEDIFF(SECOND, MIN(enqueued_at), SYSUTCDATETIME())
               FROM sqlmq.' + QUOTENAME(@qtable) + N') AS oldest_msg_age_seconds,
            (SELECT COUNT_BIG(*) FROM sqlmq.' + QUOTENAME(@atable) +
                N' WHERE dlq_reason IS NOT NULL) AS dlq_count;';
    EXEC sp_executesql @sql, N'@qn SYSNAME', @qn = @queue;
END
GO

CREATE PROCEDURE sqlmq.metrics_all
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @results TABLE (
        queue_name SYSNAME, queue_length BIGINT, total_messages BIGINT,
        oldest_msg_age_seconds INT, dlq_count BIGINT);

    DECLARE @qn SYSNAME;
    DECLARE c CURSOR LOCAL FAST_FORWARD FOR SELECT queue_name FROM sqlmq.meta;
    OPEN c;
    FETCH NEXT FROM c INTO @qn;
    WHILE @@FETCH_STATUS = 0
    BEGIN
        INSERT INTO @results EXEC sqlmq.metrics @qn;
        FETCH NEXT FROM c INTO @qn;
    END
    CLOSE c; DEALLOCATE c;
    SELECT * FROM @results ORDER BY queue_name;
END
GO
