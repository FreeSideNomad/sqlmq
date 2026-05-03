-- V002: sqlmq.create_queue, sqlmq.drop_queue, sqlmq.queues TVF
-- In Phase 1, in-memory storage is unsupported (V009 will extend create/drop_queue).

CREATE PROCEDURE sqlmq.create_queue
    @name SYSNAME,
    @storage VARCHAR(16) = 'ondisk',
    @grouped BIT = 0,
    @payload_type VARCHAR(8) = 'json',
    @max_delivery_count INT = NULL
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    IF @storage NOT IN ('ondisk', 'inmemory')
        THROW 50001, 'Invalid @storage; must be ''ondisk'' or ''inmemory''.', 1;
    IF @payload_type NOT IN ('json', 'binary')
        THROW 50002, 'Invalid @payload_type; must be ''json'' or ''binary''.', 1;
    IF @storage = 'inmemory'
        THROW 50003, 'In-memory storage is not implemented yet (planned for V009).', 1;
    IF EXISTS (SELECT 1 FROM sqlmq.meta WHERE queue_name = @name)
        THROW 50004, 'Queue already exists.', 1;

    DECLARE @qtable SYSNAME = N'q_' + @name;
    DECLARE @atable SYSNAME = N'a_' + @name;

    DECLARE @msg_col_def NVARCHAR(MAX);
    DECLARE @msg_check  NVARCHAR(MAX);
    IF @payload_type = 'json'
    BEGIN
        SET @msg_col_def = N'message NVARCHAR(MAX) NOT NULL';
        SET @msg_check   = N', CONSTRAINT CK_' + @qtable + N'_message_json CHECK (ISJSON(message) = 1)';
    END
    ELSE
    BEGIN
        SET @msg_col_def = N'message_bin VARBINARY(MAX) NOT NULL';
        SET @msg_check   = N'';
    END

    DECLARE @ddl_q NVARCHAR(MAX) = N'
        CREATE TABLE sqlmq.' + QUOTENAME(@qtable) + N' (
            msg_id      BIGINT IDENTITY(1,1) NOT NULL PRIMARY KEY CLUSTERED,
            enqueued_at DATETIME2(7) NOT NULL CONSTRAINT DF_' + @qtable + N'_eq DEFAULT (SYSUTCDATETIME()),
            vt          DATETIME2(7) NOT NULL,
            read_ct     INT          NOT NULL CONSTRAINT DF_' + @qtable + N'_rc DEFAULT (0),
            group_key   NVARCHAR(255) NULL,
            ' + @msg_col_def + N',
            headers     NVARCHAR(MAX) NULL,
            CONSTRAINT CK_' + @qtable + N'_headers_json
                CHECK (headers IS NULL OR ISJSON(headers) = 1)
            ' + @msg_check + N'
        );
        CREATE NONCLUSTERED INDEX IX_' + @qtable + N'_vt
            ON sqlmq.' + QUOTENAME(@qtable) + N' (vt, msg_id);';

    EXEC sp_executesql @ddl_q;

    IF @grouped = 1
    BEGIN
        DECLARE @ddl_grp NVARCHAR(MAX) = N'
            CREATE NONCLUSTERED INDEX IX_' + @qtable + N'_group
                ON sqlmq.' + QUOTENAME(@qtable) + N' (group_key, vt, msg_id)
                WHERE group_key IS NOT NULL;';
        EXEC sp_executesql @ddl_grp;
    END

    DECLARE @msg_col_a NVARCHAR(MAX) =
        CASE @payload_type WHEN 'json' THEN N'message NVARCHAR(MAX) NOT NULL'
                           ELSE N'message_bin VARBINARY(MAX) NOT NULL' END;

    DECLARE @ddl_a NVARCHAR(MAX) = N'
        CREATE TABLE sqlmq.' + QUOTENAME(@atable) + N' (
            msg_id      BIGINT NOT NULL PRIMARY KEY CLUSTERED,
            enqueued_at DATETIME2(7) NOT NULL,
            vt          DATETIME2(7) NOT NULL,
            read_ct     INT          NOT NULL,
            group_key   NVARCHAR(255) NULL,
            ' + @msg_col_a + N',
            headers     NVARCHAR(MAX) NULL,
            archived_at DATETIME2(7) NOT NULL CONSTRAINT DF_' + @atable + N'_at DEFAULT (SYSUTCDATETIME()),
            dlq_reason  NVARCHAR(64) NULL
        );';
    EXEC sp_executesql @ddl_a;

    INSERT INTO sqlmq.meta (queue_name, storage_type, is_grouped, payload_type, max_delivery_count)
    VALUES (@name, @storage, @grouped, @payload_type, @max_delivery_count);
END
GO

CREATE PROCEDURE sqlmq.drop_queue
    @name SYSNAME
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    IF NOT EXISTS (SELECT 1 FROM sqlmq.meta WHERE queue_name = @name)
        THROW 50010, 'Queue does not exist.', 1;

    DECLARE @qtable SYSNAME = N'q_' + @name;
    DECLARE @atable SYSNAME = N'a_' + @name;

    DECLARE @drop NVARCHAR(MAX) =
        N'DROP TABLE IF EXISTS sqlmq.' + QUOTENAME(@qtable) + N';' +
        N'DROP TABLE IF EXISTS sqlmq.' + QUOTENAME(@atable) + N';';
    EXEC sp_executesql @drop;

    DELETE FROM sqlmq.meta WHERE queue_name = @name;
END
GO

CREATE FUNCTION sqlmq.queues()
RETURNS TABLE
AS
RETURN (
    SELECT queue_name, storage_type, is_grouped, payload_type, max_delivery_count, created_at
    FROM sqlmq.meta
);
GO
