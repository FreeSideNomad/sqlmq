-- V016: surface newest_msg_age_seconds from sqlmq.metrics (and metrics_all).
--
-- Audit finding: the strict pgmq client surface exposes a `newest_msg_age_sec`
-- field on QueueMetrics, but sqlmq's metrics proc never computed it, so the
-- field was always NULL. pgmq derives both ages cheaply from the same scan;
-- the only reason sqlmq deferred it was to keep V007 minimal. Add it now.
--
-- metrics_all simply accumulates rows from sqlmq.metrics, so its @results
-- TABLE must grow a `newest_msg_age_seconds INT` column for the INSERT to
-- succeed. We CREATE OR ALTER both procs.

CREATE OR ALTER PROCEDURE sqlmq.metrics
    @queue SYSNAME
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @storage VARCHAR(16);
    SELECT @storage = storage_type FROM sqlmq.meta WHERE queue_name = @queue;
    IF @storage IS NULL THROW 50060, 'Queue does not exist.', 1;

    DECLARE @qtable SYSNAME = N'q_' + @queue;
    DECLARE @atable SYSNAME = N'a_' + @queue;

    DECLARE @sql NVARCHAR(MAX) = N'
        SELECT
            @qn AS queue_name,
            (SELECT COUNT_BIG(*) FROM sqlmq.' + QUOTENAME(@qtable) + N') AS queue_length,
            ISNULL(CAST((SELECT last_value FROM sys.identity_columns
                    WHERE object_id = OBJECT_ID(''sqlmq.' + REPLACE(@qtable, N'''', N'''''') + N''')) AS BIGINT), 0) AS total_messages,
            (SELECT DATEDIFF(SECOND, MIN(enqueued_at), SYSUTCDATETIME())
               FROM sqlmq.' + QUOTENAME(@qtable) + N') AS oldest_msg_age_seconds,
            (SELECT DATEDIFF(SECOND, MAX(enqueued_at), SYSUTCDATETIME())
               FROM sqlmq.' + QUOTENAME(@qtable) + N') AS newest_msg_age_seconds,
            (SELECT COUNT_BIG(*) FROM sqlmq.' + QUOTENAME(@atable) +
                N' WHERE dlq_reason IS NOT NULL) AS dlq_count;';
    EXEC sp_executesql @sql, N'@qn SYSNAME', @qn = @queue;
END
GO

CREATE OR ALTER PROCEDURE sqlmq.metrics_all
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @results TABLE (
        queue_name SYSNAME, queue_length BIGINT, total_messages BIGINT,
        oldest_msg_age_seconds INT, newest_msg_age_seconds INT, dlq_count BIGINT);

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
