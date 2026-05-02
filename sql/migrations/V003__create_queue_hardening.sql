-- V003: Hardening for sqlmq.create_queue and sqlmq.drop_queue.
-- Addresses code-review findings on V002:
--   * @name validation closes SQL-injection vector via constraint/index name slots
--     and bounds derived identifier lengths (so IX_q_<name>_group stays under 128).
--   * create_queue is now atomic: on any error during DDL it drops half-created
--     per-queue tables and removes any meta row before rethrowing.
--   * drop_queue is now forgiving: it can recover orphaned per-queue tables left
--     behind by an interrupted create (e.g., a crash before V003 landed).
--   * @storage and @payload_type are normalized to lowercase so 'OnDisk' works.
--
-- V002 has already been applied in CI; we cannot edit it. Instead we add a
-- new migration that uses CREATE OR ALTER PROCEDURE to replace both procs.
-- (Flyway's default naming validator only accepts purely numeric versions, so
-- V002a was rejected; V003 sorts after V002 cleanly.)

CREATE OR ALTER PROCEDURE sqlmq.create_queue
    @name SYSNAME,
    @storage VARCHAR(16) = 'ondisk',
    @grouped BIT = 0,
    @payload_type VARCHAR(8) = 'json',
    @max_delivery_count INT = NULL
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    -- @name validation: blocks SQL injection via concatenated identifiers
    -- (constraint names, default names, index names) and bounds derived
    -- name lengths. The longest derived name today is IX_q_<name>_group
    -- (~ 12 + LEN(@name) chars); 60 keeps everything well under SYSNAME's 128.
    IF @name IS NULL OR @name = N''
        THROW 50005, '@name must be non-empty.', 1;
    IF LEN(@name) > 60
        THROW 50007, '@name must be 60 characters or fewer (to leave room for derived constraint and index names).', 1;
    IF @name NOT LIKE '[A-Za-z_]%'
        THROW 50008, '@name must start with a letter or underscore.', 1;
    IF PATINDEX('%[^A-Za-z0-9_]%' COLLATE Latin1_General_BIN2, @name) <> 0
        THROW 50006, '@name may only contain letters, digits, and underscores.', 1;

    -- Normalize before validating storage/payload_type so callers can pass
    -- 'OnDisk' / 'JSON' without surprise rejections.
    SET @storage = LOWER(@storage);
    SET @payload_type = LOWER(@payload_type);

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

    BEGIN TRY
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
    END TRY
    BEGIN CATCH
        -- Best-effort cleanup of any partial state. Swallow cleanup errors so the
        -- caller sees the original failure, not a secondary one.
        DECLARE @cleanup NVARCHAR(MAX) =
            N'DROP TABLE IF EXISTS sqlmq.' + QUOTENAME(@qtable) + N';' +
            N'DROP TABLE IF EXISTS sqlmq.' + QUOTENAME(@atable) + N';';
        BEGIN TRY EXEC sp_executesql @cleanup; END TRY BEGIN CATCH END CATCH;
        DELETE FROM sqlmq.meta WHERE queue_name = @name;
        THROW;
    END CATCH
END
GO

CREATE OR ALTER PROCEDURE sqlmq.drop_queue
    @name SYSNAME
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE @qtable SYSNAME = N'q_' + @name;
    DECLARE @atable SYSNAME = N'a_' + @name;
    DECLARE @meta_existed BIT =
        CASE WHEN EXISTS (SELECT 1 FROM sqlmq.meta WHERE queue_name = @name) THEN 1 ELSE 0 END;
    DECLARE @qexists BIT =
        CASE WHEN OBJECT_ID(N'sqlmq.' + QUOTENAME(@qtable), 'U') IS NOT NULL THEN 1 ELSE 0 END;
    DECLARE @aexists BIT =
        CASE WHEN OBJECT_ID(N'sqlmq.' + QUOTENAME(@atable), 'U') IS NOT NULL THEN 1 ELSE 0 END;

    IF @meta_existed = 0 AND @qexists = 0 AND @aexists = 0
        THROW 50010, 'Queue does not exist.', 1;

    DECLARE @drop NVARCHAR(MAX) =
        N'DROP TABLE IF EXISTS sqlmq.' + QUOTENAME(@qtable) + N';' +
        N'DROP TABLE IF EXISTS sqlmq.' + QUOTENAME(@atable) + N';';
    EXEC sp_executesql @drop;

    DELETE FROM sqlmq.meta WHERE queue_name = @name;
END
GO
