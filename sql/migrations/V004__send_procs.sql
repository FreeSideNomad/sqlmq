-- V004: sqlmq.send and sqlmq.send_batch (dispatchers + on-disk inner procs).
-- In-memory variants THROW until V010 (Phase 6) lands.

CREATE PROCEDURE sqlmq._send_ondisk
    @queue          SYSNAME,
    @payload_type   VARCHAR(8),
    @message        NVARCHAR(MAX) = NULL,
    @message_bin    VARBINARY(MAX) = NULL,
    @headers        NVARCHAR(MAX) = NULL,
    @delay_seconds  INT = 0,
    @msg_id         BIGINT OUTPUT
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @qtable SYSNAME = N'q_' + @queue;
    DECLARE @vt DATETIME2(7) = DATEADD(SECOND, @delay_seconds, SYSUTCDATETIME());
    DECLARE @sql NVARCHAR(MAX);

    IF @payload_type = 'json'
    BEGIN
        IF @message IS NULL THROW 50020, '@message required for json queue', 1;
        SET @sql = N'
            INSERT INTO sqlmq.' + QUOTENAME(@qtable) + N' (vt, message, headers)
            OUTPUT inserted.msg_id
            VALUES (@vt, @msg, @hdr);';
        DECLARE @ids TABLE (id BIGINT);
        INSERT INTO @ids EXEC sp_executesql @sql,
            N'@vt DATETIME2(7), @msg NVARCHAR(MAX), @hdr NVARCHAR(MAX)',
            @vt = @vt, @msg = @message, @hdr = @headers;
        SELECT @msg_id = id FROM @ids;
    END
    ELSE
    BEGIN
        IF @message_bin IS NULL THROW 50021, '@message_bin required for binary queue', 1;
        SET @sql = N'
            INSERT INTO sqlmq.' + QUOTENAME(@qtable) + N' (vt, message_bin, headers)
            OUTPUT inserted.msg_id
            VALUES (@vt, @msg, @hdr);';
        DECLARE @ids2 TABLE (id BIGINT);
        INSERT INTO @ids2 EXEC sp_executesql @sql,
            N'@vt DATETIME2(7), @msg VARBINARY(MAX), @hdr NVARCHAR(MAX)',
            @vt = @vt, @msg = @message_bin, @hdr = @headers;
        SELECT @msg_id = id FROM @ids2;
    END
END
GO

CREATE PROCEDURE sqlmq.send
    @queue         SYSNAME,
    @message       NVARCHAR(MAX) = NULL,
    @message_bin   VARBINARY(MAX) = NULL,
    @headers       NVARCHAR(MAX) = NULL,
    @delay_seconds INT = 0
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @storage VARCHAR(16), @payload VARCHAR(8);
    SELECT @storage = storage_type, @payload = payload_type
      FROM sqlmq.meta WHERE queue_name = @queue;
    IF @storage IS NULL THROW 50030, 'Queue does not exist.', 1;

    DECLARE @msg_id BIGINT;
    IF @storage = 'ondisk'
        EXEC sqlmq._send_ondisk
            @queue, @payload, @message, @message_bin, @headers, @delay_seconds, @msg_id OUTPUT;
    ELSE
        THROW 50031, 'In-memory send not implemented yet (V010).', 1;

    SELECT @msg_id AS msg_id;
END
GO

CREATE PROCEDURE sqlmq._send_batch_ondisk
    @queue        SYSNAME,
    @payload_type VARCHAR(8),
    @messages     dbo.sqlmq_send_tvp READONLY
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @qtable SYSNAME = N'q_' + @queue;
    DECLARE @sql NVARCHAR(MAX);

    IF @payload_type = 'json'
        SET @sql = N'
            INSERT INTO sqlmq.' + QUOTENAME(@qtable) + N' (vt, message, headers)
            OUTPUT inserted.msg_id
            SELECT DATEADD(SECOND, m.delay_seconds, SYSUTCDATETIME()), m.message, m.headers
              FROM @msgs AS m
             ORDER BY (SELECT NULL);';
    ELSE
        SET @sql = N'
            INSERT INTO sqlmq.' + QUOTENAME(@qtable) + N' (vt, message_bin, headers)
            OUTPUT inserted.msg_id
            SELECT DATEADD(SECOND, m.delay_seconds, SYSUTCDATETIME()), m.message_bin, m.headers
              FROM @msgs AS m
             ORDER BY (SELECT NULL);';

    EXEC sp_executesql @sql, N'@msgs dbo.sqlmq_send_tvp READONLY', @msgs = @messages;
END
GO

CREATE PROCEDURE sqlmq.send_batch
    @queue    SYSNAME,
    @messages dbo.sqlmq_send_tvp READONLY
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @storage VARCHAR(16), @payload VARCHAR(8);
    SELECT @storage = storage_type, @payload = payload_type
      FROM sqlmq.meta WHERE queue_name = @queue;
    IF @storage IS NULL THROW 50032, 'Queue does not exist.', 1;

    IF @storage = 'ondisk'
        EXEC sqlmq._send_batch_ondisk @queue, @payload, @messages;
    ELSE
        THROW 50033, 'In-memory send_batch not implemented yet (V010).', 1;
END
GO
