-- V015: Drop the unreachable in-memory helpers + tighten meta CHECK constraint.
--
-- V014 retired the in-memory storage variant: it deleted any 'inmemory' meta
-- rows and rewrote every public dispatcher so that 'inmemory' is rejected at
-- create_queue time and the in-memory branches are gone from send/read/pop/etc.
--
-- V014 deliberately left two pieces of dead weight in place:
--   1. The interpreted helpers `sqlmq._read_inmem`, `_read_grouped_inmem`,
--      `_pop_inmem`, `_delete_inmem` and the natively compiled per-queue proc
--      generators `sqlmq._gen_inmem_send_proc`, `_gen_inmem_read_proc`,
--      `_gen_inmem_pop_proc`, `_gen_inmem_read_grouped_proc`. These are
--      unreachable post-V014 (no proc calls them, create_queue rejects
--      inmemory) but were left so V014 wouldn't be a giant migration.
--   2. The meta CHECK constraint `CK_sqlmq_meta_storage_type` still allowed
--      both 'ondisk' and 'inmemory' for migration-history coherence.
--
-- V015 closes both gaps now that the dust has settled:
--   * Drops every dead helper so the catalog is clean and stops being a
--     recurring grep hit.
--   * Narrows the CHECK constraint to `IN ('ondisk')` so the schema is
--     self-documenting. V014's tear-down step deleted any pre-existing
--     'inmemory' rows, so no rows violate the new constraint.

SET NOCOUNT ON;
SET XACT_ABORT ON;

-- ---------------------------------------------------------------------------
-- 1. Drop dead interpreted helpers (V012).
-- ---------------------------------------------------------------------------
DROP PROCEDURE IF EXISTS sqlmq._read_inmem;
DROP PROCEDURE IF EXISTS sqlmq._pop_inmem;
DROP PROCEDURE IF EXISTS sqlmq._delete_inmem;
DROP PROCEDURE IF EXISTS sqlmq._read_grouped_inmem;

-- ---------------------------------------------------------------------------
-- 2. Drop dead natively compiled per-queue proc generators (V013).
-- ---------------------------------------------------------------------------
DROP PROCEDURE IF EXISTS sqlmq._gen_inmem_send_proc;
DROP PROCEDURE IF EXISTS sqlmq._gen_inmem_read_proc;
DROP PROCEDURE IF EXISTS sqlmq._gen_inmem_pop_proc;
DROP PROCEDURE IF EXISTS sqlmq._gen_inmem_read_grouped_proc;

-- ---------------------------------------------------------------------------
-- 3. Narrow the meta CHECK constraint to ondisk-only.
-- ---------------------------------------------------------------------------
ALTER TABLE sqlmq.meta DROP CONSTRAINT CK_sqlmq_meta_storage_type;
ALTER TABLE sqlmq.meta ADD CONSTRAINT CK_sqlmq_meta_storage_type
    CHECK (storage_type IN ('ondisk'));
GO

-- ---------------------------------------------------------------------------
-- 4. Re-CREATE OR ALTER public dispatchers so the dead `IF @storage = 'inmemory'`
--    branches are removed entirely. After V014 these procs already only call
--    the on-disk path, but the @storage variable itself was still being read
--    and the explicit rejection branch was still in create_queue. Now that the
--    table-level CHECK guarantees storage_type IN ('ondisk'), the dispatchers
--    don't need to inspect storage_type at all — but we keep the lookup so
--    "queue does not exist" remains the error for missing rows.
--
--    create_queue: keep the @storage parameter for forward-compat (with default
--    'ondisk'), but THROW if anything other than 'ondisk' is passed after
--    LOWER() normalization. The CHECK constraint would also reject it, but the
--    explicit THROW gives a clearer error message.
-- ---------------------------------------------------------------------------

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

    -- @name validation (identical to V003 hardening).
    IF @name IS NULL OR @name = N''
        THROW 50005, '@name must be non-empty.', 1;
    IF LEN(@name) > 60
        THROW 50007, '@name must be 60 characters or fewer (to leave room for derived constraint and index names).', 1;
    IF @name NOT LIKE '[A-Za-z_]%'
        THROW 50008, '@name must start with a letter or underscore.', 1;
    IF PATINDEX('%[^A-Za-z0-9_]%' COLLATE Latin1_General_BIN2, @name) <> 0
        THROW 50006, '@name may only contain letters, digits, and underscores.', 1;

    SET @storage = LOWER(@storage);
    SET @payload_type = LOWER(@payload_type);

    -- Only 'ondisk' is supported as of V014/V015. The CHECK constraint on
    -- sqlmq.meta enforces this at the table layer; the explicit THROW here
    -- gives operators a clearer message than a constraint violation.
    IF @storage = 'inmemory'
        THROW 50001, 'In-memory storage is not supported. Use @storage = ''ondisk'' (the default).', 1;
    IF @storage <> 'ondisk'
        THROW 50001, 'Invalid @storage; only ''ondisk'' is supported.', 1;
    IF @payload_type NOT IN ('json', 'binary')
        THROW 50002, 'Invalid @payload_type; must be ''json'' or ''binary''.', 1;
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

        -- Archive table: always on-disk.
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
        DECLARE @cleanup NVARCHAR(MAX) =
            N'DROP TABLE IF EXISTS sqlmq.' + QUOTENAME(@qtable) + N';' +
            N'DROP TABLE IF EXISTS sqlmq.' + QUOTENAME(@atable) + N';';
        BEGIN TRY EXEC sp_executesql @cleanup; END TRY BEGIN CATCH END CATCH;
        DELETE FROM sqlmq.meta WHERE queue_name = @name;
        THROW;
    END CATCH
END
GO

-- send: drop the @storage lookup. The CHECK constraint guarantees on-disk;
-- we still need the meta row to recover @payload_type and to detect missing
-- queues.
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
    SET XACT_ABORT ON;

    DECLARE @payload VARCHAR(8);
    SELECT @payload = payload_type
      FROM sqlmq.meta WHERE queue_name = @queue;
    IF @payload IS NULL THROW 50030, 'Queue does not exist.', 1;

    -- Validate per payload_type before acquiring the applock so misuse fails fast.
    IF @payload = 'json'   AND @message     IS NULL THROW 50020, '@message required for json queue', 1;
    IF @payload = 'binary' AND @message_bin IS NULL THROW 50021, '@message_bin required for binary queue', 1;

    BEGIN TRANSACTION;

    DECLARE @rc INT;
    EXEC @rc = sp_getapplock
        @Resource    = @queue,
        @LockMode    = N'Exclusive',
        @LockOwner   = N'Transaction',
        @LockTimeout = 300000;
    IF @rc < 0
    BEGIN
        ROLLBACK TRANSACTION;
        DECLARE @msg NVARCHAR(200) = N'sqlmq.send: sp_getapplock failed (rc=' + CAST(@rc AS NVARCHAR(10)) + N')';
        THROW 50034, @msg, 1;
    END

    DECLARE @msg_id BIGINT;
    EXEC sqlmq._send_ondisk @queue, @payload, @message, @message_bin, @headers,
                             @delay_seconds, @group_key, @msg_id OUTPUT;

    COMMIT TRANSACTION;

    SELECT @msg_id AS msg_id;
END
GO

CREATE OR ALTER PROCEDURE sqlmq.send_batch
    @queue    SYSNAME,
    @messages dbo.sqlmq_send_tvp READONLY
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE @payload VARCHAR(8);
    SELECT @payload = payload_type
      FROM sqlmq.meta WHERE queue_name = @queue;
    IF @payload IS NULL THROW 50032, 'Queue does not exist.', 1;

    BEGIN TRANSACTION;

    DECLARE @rc INT;
    EXEC @rc = sp_getapplock
        @Resource    = @queue,
        @LockMode    = N'Exclusive',
        @LockOwner   = N'Transaction',
        @LockTimeout = 300000;
    IF @rc < 0
    BEGIN
        ROLLBACK TRANSACTION;
        DECLARE @msg2 NVARCHAR(200) = N'sqlmq.send_batch: sp_getapplock failed (rc=' + CAST(@rc AS NVARCHAR(10)) + N')';
        THROW 50035, @msg2, 1;
    END

    EXEC sqlmq._send_batch_ondisk @queue, @payload, @messages;

    COMMIT TRANSACTION;
END
GO

CREATE OR ALTER PROCEDURE sqlmq.[read]
    @queue      SYSNAME,
    @vt_seconds INT,
    @max_count  INT = 1
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE @payload VARCHAR(8), @max_dlq INT;
    SELECT @payload = payload_type, @max_dlq = max_delivery_count
      FROM sqlmq.meta WHERE queue_name = @queue;
    IF @payload IS NULL THROW 50040, 'Queue does not exist.', 1;

    EXEC sqlmq._read_ondisk @queue, @payload, @vt_seconds, @max_count, @max_dlq;
END
GO

CREATE OR ALTER PROCEDURE sqlmq.read_grouped
    @queue      SYSNAME,
    @vt_seconds INT,
    @max_count  INT = 1
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE @payload VARCHAR(8), @max_dlq INT, @grouped BIT;
    SELECT @payload = payload_type,
           @max_dlq = max_delivery_count, @grouped = is_grouped
      FROM sqlmq.meta WHERE queue_name = @queue;
    IF @payload IS NULL THROW 50080, 'Queue does not exist.', 1;
    IF @grouped = 0 THROW 50081, 'Queue was not created with grouping enabled.', 1;

    -- Per-queue applock + transaction match V009's _read_grouped_ondisk
    -- contract: that proc opens its own BEGIN/COMMIT TRANSACTION and acquires
    -- sp_getapplock @queue Exclusive Transaction internally.
    EXEC sqlmq._read_grouped_ondisk @queue, @payload, @vt_seconds, @max_count, @max_dlq;
END
GO

CREATE OR ALTER PROCEDURE sqlmq.[delete]
    @queue   SYSNAME,
    @msg_ids dbo.sqlmq_msg_id_tvp READONLY
AS
BEGIN
    SET NOCOUNT ON;
    IF NOT EXISTS (SELECT 1 FROM sqlmq.meta WHERE queue_name = @queue)
        THROW 50050, 'Queue does not exist.', 1;
    EXEC sqlmq._delete_ondisk @queue, @msg_ids;
END
GO

CREATE OR ALTER PROCEDURE sqlmq.pop
    @queue SYSNAME
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;
    DECLARE @payload VARCHAR(8);
    SELECT @payload = payload_type
      FROM sqlmq.meta WHERE queue_name = @queue;
    IF @payload IS NULL THROW 50053, 'Queue does not exist.', 1;
    EXEC sqlmq._pop_ondisk @queue, @payload;
END
GO

CREATE OR ALTER PROCEDURE sqlmq.dlq_sweep
    @queue SYSNAME
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @payload VARCHAR(8), @max_dlq INT;
    SELECT @payload = payload_type, @max_dlq = max_delivery_count
      FROM sqlmq.meta WHERE queue_name = @queue;
    IF @payload IS NULL THROW 50071, 'Queue does not exist.', 1;
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
          FROM sqlmq.' + QUOTENAME(@qtable) + N' AS q
               WITH (READPAST, UPDLOCK, ROWLOCK, READCOMMITTEDLOCK)
         WHERE q.read_ct >= @dlq;
        SELECT @@ROWCOUNT AS rows_swept;';
    EXEC sp_executesql @sql, N'@dlq INT', @dlq = @max_dlq;
END
GO
