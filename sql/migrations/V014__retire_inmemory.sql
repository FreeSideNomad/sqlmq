-- V014: Retire the in-memory storage variant.
--
-- Background. Phases 6/8 introduced and iterated on a memory-optimized (Hekaton)
-- variant of every queue table. Phase 7 + 8 ran a head-to-head bake-off across
-- two environments (Mac Rosetta + native x86_64 Hyper-V VM) and two
-- implementations (V012 applock-serialized read + V013 per-queue natively
-- compiled read). Conclusion: in-memory NEVER beats on-disk and degrades
-- sharply at higher consumer counts.
--
-- Why. Memory-optimized tables under SNAPSHOT have no READPAST equivalent.
-- Concurrent consumers thunderclap on SELECT TOP(1) ORDER BY msg_id, collide on
-- the UPDATE, and trigger 41302 (write-write conflict) retry storms. The
-- on-disk path's `READPAST + UPDLOCK + ROWLOCK + READCOMMITTEDLOCK` pattern
-- produces correct atomic claim semantics with parallel competing-consumer
-- throughput — strictly the right tool for this workload.
--
-- This migration:
--   1. For every existing in-memory queue: drop per-queue inner procs
--      (_send_inmem_<n>, _read_inmem_<n>, _pop_inmem_<n>, _read_grouped_inmem_<n>),
--      drop the memory-optimized queue table sqlmq.[q_<n>], drop the on-disk
--      archive sqlmq.[a_<n>], and delete the meta row.
--   2. Rewrite sqlmq.create_queue, sqlmq.drop_queue, sqlmq.send,
--      sqlmq.send_batch, sqlmq.[read], sqlmq.read_grouped, sqlmq.[delete],
--      sqlmq.pop and sqlmq.dlq_sweep so that @storage='inmemory' is rejected
--      with a clear message at create_queue time and the in-memory branches in
--      the dispatchers are gone.
--
-- The CHECK constraint on sqlmq.meta.storage_type still allows 'inmemory' for
-- migration-history coherence — dropping/recreating it would be brittle. The
-- procs reject 'inmemory' at create time, which is sufficient defense.
--
-- The interpreted helpers from V012 (sqlmq._read_inmem, _read_grouped_inmem,
-- _pop_inmem, _delete_inmem) and the natively compiled generators from V013
-- (sqlmq._gen_inmem_send_proc, _gen_inmem_read_proc, etc.) are left in place.
-- They are unreachable now (no proc calls them, create_queue rejects inmemory)
-- and dropping them would add migration noise without operational benefit.

SET NOCOUNT ON;
SET XACT_ABORT ON;

-- ---------------------------------------------------------------------------
-- 1. Tear down every existing in-memory queue.
-- ---------------------------------------------------------------------------
DECLARE retire_cursor CURSOR LOCAL FAST_FORWARD FOR
    SELECT queue_name
      FROM sqlmq.meta
     WHERE storage_type = 'inmemory';
OPEN retire_cursor;

DECLARE @qname SYSNAME;
FETCH NEXT FROM retire_cursor INTO @qname;
WHILE @@FETCH_STATUS = 0
BEGIN
    DECLARE @qtable SYSNAME = N'q_' + @qname;
    DECLARE @atable SYSNAME = N'a_' + @qname;

    -- Drop natively compiled per-queue procs first; SCHEMABINDING pins them
    -- to the queue table and would block DROP TABLE.
    DECLARE @drop_procs NVARCHAR(MAX) =
        N'DROP PROCEDURE IF EXISTS sqlmq.' + QUOTENAME(N'_send_inmem_'         + @qname) + N';' +
        N'DROP PROCEDURE IF EXISTS sqlmq.' + QUOTENAME(N'_read_inmem_'         + @qname) + N';' +
        N'DROP PROCEDURE IF EXISTS sqlmq.' + QUOTENAME(N'_pop_inmem_'          + @qname) + N';' +
        N'DROP PROCEDURE IF EXISTS sqlmq.' + QUOTENAME(N'_read_grouped_inmem_' + @qname) + N';';
    EXEC sp_executesql @drop_procs;

    DECLARE @drop_tables NVARCHAR(MAX) =
        N'DROP TABLE IF EXISTS sqlmq.' + QUOTENAME(@qtable) + N';' +
        N'DROP TABLE IF EXISTS sqlmq.' + QUOTENAME(@atable) + N';';
    EXEC sp_executesql @drop_tables;

    DELETE FROM sqlmq.meta WHERE queue_name = @qname;

    FETCH NEXT FROM retire_cursor INTO @qname;
END
CLOSE retire_cursor;
DEALLOCATE retire_cursor;
GO

-- ---------------------------------------------------------------------------
-- 2. create_queue: reject @storage='inmemory'; drop the in-memory branch.
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

    -- V014: in-memory storage retired. See header comment for rationale.
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

-- ---------------------------------------------------------------------------
-- 3. drop_queue: only on-disk handling (no per-queue native procs to drop).
-- ---------------------------------------------------------------------------
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

-- ---------------------------------------------------------------------------
-- 4. Public dispatchers: drop the in-memory branch from all of them.
-- ---------------------------------------------------------------------------

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

    DECLARE @storage VARCHAR(16), @payload VARCHAR(8);
    SELECT @storage = storage_type, @payload = payload_type
      FROM sqlmq.meta WHERE queue_name = @queue;
    IF @storage IS NULL THROW 50030, 'Queue does not exist.', 1;

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

    DECLARE @storage VARCHAR(16), @payload VARCHAR(8);
    SELECT @storage = storage_type, @payload = payload_type
      FROM sqlmq.meta WHERE queue_name = @queue;
    IF @storage IS NULL THROW 50032, 'Queue does not exist.', 1;

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

    DECLARE @storage VARCHAR(16), @payload VARCHAR(8), @max_dlq INT;
    SELECT @storage = storage_type, @payload = payload_type, @max_dlq = max_delivery_count
      FROM sqlmq.meta WHERE queue_name = @queue;
    IF @storage IS NULL THROW 50040, 'Queue does not exist.', 1;

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

    DECLARE @storage VARCHAR(16), @payload VARCHAR(8), @max_dlq INT, @grouped BIT;
    SELECT @storage = storage_type, @payload = payload_type,
           @max_dlq = max_delivery_count, @grouped = is_grouped
      FROM sqlmq.meta WHERE queue_name = @queue;
    IF @storage IS NULL THROW 50080, 'Queue does not exist.', 1;
    IF @grouped = 0 THROW 50081, 'Queue was not created with grouping enabled.', 1;

    -- Per-queue applock + transaction match V009's _read_grouped_ondisk
    -- contract: that proc opens its own BEGIN/COMMIT TRANSACTION and acquires
    -- sp_getapplock @queue Exclusive Transaction internally. Calling it
    -- directly here is correct.
    EXEC sqlmq._read_grouped_ondisk @queue, @payload, @vt_seconds, @max_count, @max_dlq;
END
GO

CREATE OR ALTER PROCEDURE sqlmq.[delete]
    @queue   SYSNAME,
    @msg_ids dbo.sqlmq_msg_id_tvp READONLY
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @storage VARCHAR(16);
    SELECT @storage = storage_type FROM sqlmq.meta WHERE queue_name = @queue;
    IF @storage IS NULL THROW 50050, 'Queue does not exist.', 1;
    EXEC sqlmq._delete_ondisk @queue, @msg_ids;
END
GO

CREATE OR ALTER PROCEDURE sqlmq.pop
    @queue SYSNAME
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;
    DECLARE @storage VARCHAR(16), @payload VARCHAR(8);
    SELECT @storage = storage_type, @payload = payload_type
      FROM sqlmq.meta WHERE queue_name = @queue;
    IF @storage IS NULL THROW 50053, 'Queue does not exist.', 1;
    EXEC sqlmq._pop_ondisk @queue, @payload;
END
GO

-- ---------------------------------------------------------------------------
-- 5. dlq_sweep: drop the storage-dispatch (only on-disk now).
-- ---------------------------------------------------------------------------
CREATE OR ALTER PROCEDURE sqlmq.dlq_sweep
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
          FROM sqlmq.' + QUOTENAME(@qtable) + N' AS q
               WITH (READPAST, UPDLOCK, ROWLOCK, READCOMMITTEDLOCK)
         WHERE q.read_ct >= @dlq;
        SELECT @@ROWCOUNT AS rows_swept;';
    EXEC sp_executesql @sql, N'@dlq INT', @dlq = @max_dlq;
END
GO
