-- V012: In-memory storage variant — extends sqlmq.create_queue / drop_queue
-- and the public dispatchers to handle @storage='inmemory'.
--
-- Companion to V011 (filegroup setup). The split is purely mechanical:
-- ALTER DATABASE statements can't run inside a Flyway transaction. The
-- companion file V012__inmemory_storage.sql.conf also sets
-- executeInTransaction=false because this migration creates / alters
-- objects that depend on memory-optimized DDL ("DDL statements ALTER, DROP
-- and CREATE inside user transactions are not supported with memory
-- optimized tables").
--
-- Per in-memory queue, sqlmq.create_queue generates:
--   * a queue table sqlmq.[q_<name>] with MEMORY_OPTIMIZED = ON, DURABILITY
--     = SCHEMA_AND_DATA
--   * an on-disk archive table sqlmq.[a_<name>] (cross-engine; same schema
--     as the on-disk variant)
--   * a per-queue natively compiled inner proc sqlmq.[_send_inmem_<name>]
--     that performs the INSERT
--
-- Public dispatchers (sqlmq.send / send_batch / read / read_grouped / delete /
-- pop) route @storage='inmemory' to the appropriate inner proc:
--   * send / send_batch  -> EXEC sqlmq.[_send_inmem_<queue>] (per-queue, native)
--   * read / read_grouped / pop / delete  -> EXEC sqlmq._{verb}_inmem (shared,
--     interpreted; dispatches via dynamic SQL similar to the on-disk variants)
--
-- Strategy note: which procs are natively compiled vs interpreted
--
-- Natively compiled stored procedures have severe restrictions:
--   * No subqueries in UPDATE/DELETE (only in SELECT).
--   * No FROM clause in UPDATE; no table source in DELETE.
--   * No cursors, no dynamic SQL, no sp_getapplock.
--   * SCHEMABINDING required, locking hints forbidden.
--
-- Together these rule out the standard "claim TOP(n) candidates AND atomically
-- bump vt" pattern that the on-disk read uses. Working around them requires
-- either a row-by-row loop emitting N small result sets, or an additional
-- memory-optimized table variable that buffers OUTPUT rows for a single
-- final SELECT. Both are complex and would add real maintenance cost.
--
-- Decision: only SEND is natively compiled (per-queue proc, single INSERT,
-- maps trivially). READ / READ_GROUPED / POP / DELETE are interpreted helpers
-- that touch the memory-optimized queue table via dynamic SQL. Interpreted
-- access transparently elevates to SNAPSHOT isolation thanks to V011's
-- MEMORY_OPTIMIZED_ELEVATE_TO_SNAPSHOT=ON DB setting.
--
-- Concurrency model for in-memory reads:
--   sqlmq.[read] / sqlmq.pop / sqlmq.read_grouped acquire the same per-queue
--   sp_getapplock that send uses, around the call to the inner read proc.
--   This serializes per-queue read claims, eliminating the 41302 (write-
--   write conflict under SNAPSHOT) churn that otherwise causes consumers
--   to occasionally observe an empty queue when messages are actually ready
--   (sustained 41302 retries can exhaust the in-proc retry budget, after
--   which the proc returns 0 rows). Cross-queue concurrency is unaffected.
--
-- Phase 7 bake-off will quantify the throughput cost of single-threaded
-- per-queue reads vs the on-disk variant's parallel READPAST claims; if
-- significant, a future revision could buffer OUTPUT rows in a memory-opt
-- table variable inside a natively compiled inner proc and emit a single
-- final SELECT (more code, more allocations per call).
--
-- Other design notes:
--
-- * For sqlmq.read_grouped we move the BEGIN TRAN + sp_getapplock UP from
--   _read_grouped_ondisk to the public dispatcher so the locking pattern is
--   identical for both storage types. _read_grouped_ondisk is rewritten to
--   the simpler "lock-already-held" form. The applock + transaction wrap the
--   call to either the on-disk inner proc or the inmemory inner proc.
--
-- * Memory-optimized tables don't support filtered indexes — IX_q_<name>_group
--   is therefore unconditional (no WHERE group_key IS NOT NULL).
--
-- * Memory-optimized tables don't support CHECK (ISJSON(...)). The JSON
--   validation constraint is omitted for in-memory queues. Callers passing
--   invalid JSON to an in-memory queue won't be rejected at insert time.
--
-- * Both message NVARCHAR(MAX) and message_bin VARBINARY(MAX) columns are
--   declared NULLABLE on the in-memory queue table; only the relevant one is
--   populated per the queue's payload_type.
--
-- * Cross-engine writes (in-memory queue table -> on-disk archive table) are
--   used by sqlmq.archive, sqlmq.dlq_sweep, and the metrics path. SQL Server
--   2014+ supports these in interpreted T-SQL when MEMORY_OPTIMIZED_ELEVATE_TO_SNAPSHOT
--   is ON, which V011 enables.
--
-- * BUCKET_COUNT for the hash index on msg_id: 1048576 (2^20). Microsoft's
--   guidance is 1-2x the expected number of unique values; for a queue with
--   ~10K-100K concurrent in-flight messages this is a generous overestimate
--   that avoids hash collisions while wasting only ~16MB of memory per queue.

-- ---------------------------------------------------------------------------
-- 1. create_queue: handle @storage='inmemory'
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

    IF @storage NOT IN ('ondisk', 'inmemory')
        THROW 50001, 'Invalid @storage; must be ''ondisk'' or ''inmemory''.', 1;
    IF @payload_type NOT IN ('json', 'binary')
        THROW 50002, 'Invalid @payload_type; must be ''json'' or ''binary''.', 1;
    IF EXISTS (SELECT 1 FROM sqlmq.meta WHERE queue_name = @name)
        THROW 50004, 'Queue already exists.', 1;

    DECLARE @qtable SYSNAME = N'q_' + @name;
    DECLARE @atable SYSNAME = N'a_' + @name;

    BEGIN TRY
        IF @storage = 'ondisk'
        BEGIN
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
        END
        ELSE  -- inmemory
        BEGIN
            -- Memory-optimized queue table. Both message columns NULLABLE; only the
            -- relevant one is populated per send. Filtered indexes / ISJSON CHECK are
            -- not supported on memory-optimized tables and are therefore omitted.
            DECLARE @ddl_qm NVARCHAR(MAX) = N'
                CREATE TABLE sqlmq.' + QUOTENAME(@qtable) + N' (
                    msg_id      BIGINT IDENTITY(1,1) NOT NULL
                                PRIMARY KEY NONCLUSTERED HASH WITH (BUCKET_COUNT=1048576),
                    enqueued_at DATETIME2(7) NOT NULL CONSTRAINT DF_' + @qtable + N'_eq DEFAULT (SYSUTCDATETIME()),
                    vt          DATETIME2(7) NOT NULL,
                    read_ct     INT          NOT NULL CONSTRAINT DF_' + @qtable + N'_rc DEFAULT (0),
                    group_key   NVARCHAR(255) COLLATE Latin1_General_100_BIN2 NULL,
                    message     NVARCHAR(MAX) NULL,
                    message_bin VARBINARY(MAX) NULL,
                    headers     NVARCHAR(MAX) NULL,
                    INDEX IX_' + @qtable + N'_vt    NONCLUSTERED (vt, msg_id),
                    INDEX IX_' + @qtable + N'_group NONCLUSTERED (group_key, vt, msg_id)
                ) WITH (MEMORY_OPTIMIZED = ON, DURABILITY = SCHEMA_AND_DATA);';
            EXEC sp_executesql @ddl_qm;
        END

        -- Archive table: always on-disk, regardless of @storage.
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

        -- For in-memory queues, generate the per-queue natively compiled send proc.
        IF @storage = 'inmemory'
        BEGIN
            EXEC sqlmq._gen_inmem_send_proc @name;
        END

        INSERT INTO sqlmq.meta (queue_name, storage_type, is_grouped, payload_type, max_delivery_count)
        VALUES (@name, @storage, @grouped, @payload_type, @max_delivery_count);
    END TRY
    BEGIN CATCH
        -- Best-effort cleanup on failure.
        DECLARE @cleanup NVARCHAR(MAX) =
            N'DROP TABLE IF EXISTS sqlmq.' + QUOTENAME(@qtable) + N';' +
            N'DROP TABLE IF EXISTS sqlmq.' + QUOTENAME(@atable) + N';';
        BEGIN TRY EXEC sp_executesql @cleanup; END TRY BEGIN CATCH END CATCH;

        -- Drop the per-queue send proc if it was created.
        DECLARE @drop_proc NVARCHAR(MAX) =
            N'DROP PROCEDURE IF EXISTS sqlmq.' + QUOTENAME(N'_send_inmem_' + @name) + N';';
        BEGIN TRY EXEC sp_executesql @drop_proc; END TRY BEGIN CATCH END CATCH;

        DELETE FROM sqlmq.meta WHERE queue_name = @name;
        THROW;
    END CATCH
END
GO

-- ---------------------------------------------------------------------------
-- 2. _gen_inmem_send_proc: generates the per-queue natively compiled send proc.
--
-- The proc's body references the queue table directly (SCHEMABINDING-bound),
-- so we can't share it across queues — each queue gets its own compiled
-- copy. Only send is natively compiled; reads/deletes/pops use shared
-- interpreted helpers that take @queue as a parameter.
-- ---------------------------------------------------------------------------
CREATE OR ALTER PROCEDURE sqlmq._gen_inmem_send_proc
    @name SYSNAME
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @qfqn NVARCHAR(300) = N'sqlmq.' + QUOTENAME(N'q_' + @name);
    DECLARE @send_name SYSNAME = N'_send_inmem_' + @name;

    -- Natively compiled INSERT. Single statement, scoped IDENTITY, no
    -- subqueries — fits comfortably within the natively-compiled subset.
    -- The dispatcher (sqlmq.send) holds the per-queue applock around this
    -- call, so commit order matches msg_id allocation order per queue.
    DECLARE @send_sql NVARCHAR(MAX) = N'
CREATE PROCEDURE sqlmq.' + QUOTENAME(@send_name) + N'
    @message       NVARCHAR(MAX) = NULL,
    @message_bin   VARBINARY(MAX) = NULL,
    @headers       NVARCHAR(MAX) = NULL,
    @delay_seconds INT = 0,
    @group_key     NVARCHAR(255) = NULL,
    @msg_id        BIGINT OUTPUT
WITH NATIVE_COMPILATION, SCHEMABINDING
AS
BEGIN ATOMIC WITH (TRANSACTION ISOLATION LEVEL = SNAPSHOT, LANGUAGE = N''us_english'')
    DECLARE @vt DATETIME2(7) = DATEADD(SECOND, @delay_seconds, SYSUTCDATETIME());
    INSERT INTO ' + @qfqn + N' (vt, group_key, message, message_bin, headers)
    VALUES (@vt, @group_key, @message, @message_bin, @headers);
    SET @msg_id = SCOPE_IDENTITY();
END';
    EXEC sp_executesql @send_sql;
END
GO

-- ---------------------------------------------------------------------------
-- 3. Shared interpreted inner procs for in-memory queues.
--
-- These access the memory-optimized queue table via dynamic SQL. They take
-- the @queue name as a parameter (no per-queue duplication needed because
-- they're not natively compiled).
--
-- Memory-opt tables don't support READPAST/UPDLOCK/ROWLOCK hints. Concurrency
-- is handled by SNAPSHOT isolation (auto-elevated by the V011 DB setting)
-- plus a retry-on-41302 loop for write-write conflicts.
-- ---------------------------------------------------------------------------

CREATE OR ALTER PROCEDURE sqlmq._read_inmem
    @queue        SYSNAME,
    @payload_type VARCHAR(8),
    @vt_seconds   INT,
    @max_count    INT,
    @max_dlq      INT
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @qtable SYSNAME = N'q_' + @queue;
    DECLARE @msg_col NVARCHAR(64) =
        CASE @payload_type WHEN 'json' THEN N'message' ELSE N'message_bin' END;

    -- Same eligible/claim/UPDATE-OUTPUT pattern as on-disk read, minus the
    -- READPAST/UPDLOCK/ROWLOCK/READCOMMITTEDLOCK hints (forbidden on memory-opt
    -- tables). The retry loop wraps the statement to handle 41302 (write-write
    -- conflict under SNAPSHOT).
    -- Defense-in-depth retry on 41302 (write-write conflict under SNAPSHOT).
    -- The dispatcher serializes per-queue reads via sp_getapplock so 41302
    -- shouldn't actually occur here in normal operation, but a small budget
    -- of retries protects against any edge case.
    DECLARE @sql NVARCHAR(MAX) = N'
        DECLARE @attempts INT = 0;
        WHILE @attempts < 3
        BEGIN
            BEGIN TRY
                WITH claimed AS (
                    SELECT TOP (@n) msg_id
                      FROM sqlmq.' + QUOTENAME(@qtable) + N'
                     WHERE vt <= SYSUTCDATETIME()
                       AND (@dlq IS NULL OR read_ct < @dlq)
                     ORDER BY msg_id
                )
                UPDATE q
                   SET vt = DATEADD(SECOND, @vt, SYSUTCDATETIME()),
                       read_ct = read_ct + 1
                OUTPUT inserted.msg_id, inserted.read_ct, inserted.enqueued_at,
                       inserted.vt, inserted.group_key,
                       inserted.' + @msg_col + N', inserted.headers
                  FROM sqlmq.' + QUOTENAME(@qtable) + N' AS q
                 INNER JOIN claimed AS c ON q.msg_id = c.msg_id;
                RETURN;
            END TRY
            BEGIN CATCH
                IF ERROR_NUMBER() = 41302 SET @attempts += 1;
                ELSE THROW;
            END CATCH
        END';
    EXEC sp_executesql @sql,
        N'@n INT, @vt INT, @dlq INT',
        @n = @max_count, @vt = @vt_seconds, @dlq = @max_dlq;
END
GO

CREATE OR ALTER PROCEDURE sqlmq._read_grouped_inmem
    @queue        SYSNAME,
    @payload_type VARCHAR(8),
    @vt_seconds   INT,
    @max_count    INT,
    @max_dlq      INT
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @qtable SYSNAME = N'q_' + @queue;
    DECLARE @msg_col NVARCHAR(64) =
        CASE @payload_type WHEN 'json' THEN N'message' ELSE N'message_bin' END;

    -- The dispatcher (sqlmq.read_grouped) holds sp_getapplock @queue Exclusive
    -- Transaction across this call. Same eligible/claim pattern as on-disk,
    -- minus the locking hints (forbidden on memory-opt) plus 41302 retry.
    DECLARE @sql NVARCHAR(MAX) = N'
        DECLARE @attempts INT = 0;
        WHILE @attempts < 3
        BEGIN
            BEGIN TRY
                WITH eligible AS (
                    SELECT q.msg_id, q.group_key,
                           ROW_NUMBER() OVER (PARTITION BY q.group_key ORDER BY q.msg_id) AS rn
                      FROM sqlmq.' + QUOTENAME(@qtable) + N' AS q
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
                 INNER JOIN claimed AS c ON q.msg_id = c.msg_id;
                RETURN;
            END TRY
            BEGIN CATCH
                IF ERROR_NUMBER() = 41302 SET @attempts += 1;
                ELSE THROW;
            END CATCH
        END';
    EXEC sp_executesql @sql,
        N'@n INT, @vt INT, @dlq INT',
        @n = @max_count, @vt = @vt_seconds, @dlq = @max_dlq;
END
GO

CREATE OR ALTER PROCEDURE sqlmq._pop_inmem
    @queue        SYSNAME,
    @payload_type VARCHAR(8)
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @qtable SYSNAME = N'q_' + @queue;
    DECLARE @msg_col NVARCHAR(64) =
        CASE @payload_type WHEN 'json' THEN N'message' ELSE N'message_bin' END;
    DECLARE @sql NVARCHAR(MAX) = N'
        DECLARE @attempts INT = 0;
        WHILE @attempts < 3
        BEGIN
            BEGIN TRY
                WITH claim AS (
                    SELECT TOP (1) msg_id
                      FROM sqlmq.' + QUOTENAME(@qtable) + N'
                     WHERE vt <= SYSUTCDATETIME()
                     ORDER BY msg_id
                )
                DELETE q
                OUTPUT deleted.msg_id, deleted.read_ct, deleted.enqueued_at, deleted.vt,
                       deleted.group_key, deleted.' + @msg_col + N', deleted.headers
                  FROM sqlmq.' + QUOTENAME(@qtable) + N' AS q
                 INNER JOIN claim AS c ON q.msg_id = c.msg_id;
                RETURN;
            END TRY
            BEGIN CATCH
                IF ERROR_NUMBER() = 41302 SET @attempts += 1;
                ELSE THROW;
            END CATCH
        END';
    EXEC sp_executesql @sql;
END
GO

CREATE OR ALTER PROCEDURE sqlmq._delete_inmem
    @queue   SYSNAME,
    @msg_ids dbo.sqlmq_msg_id_tvp READONLY
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @qtable SYSNAME = N'q_' + @queue;
    DECLARE @sql NVARCHAR(MAX) = N'
        DELETE q FROM sqlmq.' + QUOTENAME(@qtable) + N' AS q
         INNER JOIN @ids AS i ON q.msg_id = i.msg_id;
        SELECT @@ROWCOUNT AS rows_deleted;';
    EXEC sp_executesql @sql, N'@ids dbo.sqlmq_msg_id_tvp READONLY', @ids = @msg_ids;
END
GO

-- ---------------------------------------------------------------------------
-- 4. drop_queue: remove per-queue inner procs and the memory-optimized table.
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

    -- Drop the per-queue send proc first; it SCHEMABINDs the queue table
    -- and would block DROP TABLE.
    DECLARE @drop_proc NVARCHAR(MAX) =
        N'DROP PROCEDURE IF EXISTS sqlmq.' + QUOTENAME(N'_send_inmem_' + @name) + N';';
    EXEC sp_executesql @drop_proc;

    DECLARE @drop NVARCHAR(MAX) =
        N'DROP TABLE IF EXISTS sqlmq.' + QUOTENAME(@qtable) + N';' +
        N'DROP TABLE IF EXISTS sqlmq.' + QUOTENAME(@atable) + N';';
    EXEC sp_executesql @drop;

    DELETE FROM sqlmq.meta WHERE queue_name = @name;
END
GO

-- ---------------------------------------------------------------------------
-- 5. Public dispatchers — route 'inmemory' to the per-queue / shared inner procs.
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
    -- XACT_ABORT ON ensures any error inside the EXEC of an inner proc
    -- (interpreted or natively compiled) rolls back the outer transaction.
    -- Without it, a non-fatal error inside EXEC could leave the outer txn
    -- open, the COMMIT TRANSACTION would still run, and the producer would
    -- believe its msg_id was committed when in fact only the savepoint was
    -- released without persisting the row. Required for correctness of the
    -- inmemory variant in particular (savepoint semantics around BEGIN
    -- ATOMIC blocks).
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
    IF @storage = 'ondisk'
    BEGIN
        EXEC sqlmq._send_ondisk @queue, @payload, @message, @message_bin, @headers,
                                 @delay_seconds, @group_key, @msg_id OUTPUT;
    END
    ELSE
    BEGIN
        DECLARE @inner NVARCHAR(300) = N'sqlmq.' + QUOTENAME(N'_send_inmem_' + @queue);
        DECLARE @sql NVARCHAR(MAX) = N'EXEC ' + @inner +
            N' @message=@p_msg, @message_bin=@p_bin, @headers=@p_hdr, ' +
            N'@delay_seconds=@p_delay, @group_key=@p_gk, @msg_id=@p_id OUTPUT;';
        EXEC sp_executesql @sql,
            N'@p_msg NVARCHAR(MAX), @p_bin VARBINARY(MAX), @p_hdr NVARCHAR(MAX),
              @p_delay INT, @p_gk NVARCHAR(255), @p_id BIGINT OUTPUT',
            @p_msg = @message, @p_bin = @message_bin, @p_hdr = @headers,
            @p_delay = @delay_seconds, @p_gk = @group_key, @p_id = @msg_id OUTPUT;
    END

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
    SET XACT_ABORT ON;  -- see sqlmq.send for rationale

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

    IF @storage = 'ondisk'
    BEGIN
        EXEC sqlmq._send_batch_ondisk @queue, @payload, @messages;
    END
    ELSE
    BEGIN
        -- For in-memory we loop the TVP and call the per-queue _send_inmem_<queue>
        -- proc once per row. Natively compiled procs cannot accept disk-based TVPs;
        -- the per-queue applock is already held, so the loop preserves strict
        -- FIFO commit order. Returns the msg_ids in a single result set.
        DECLARE @inner NVARCHAR(300) = N'sqlmq.' + QUOTENAME(N'_send_inmem_' + @queue);
        DECLARE @sql NVARCHAR(MAX) = N'EXEC ' + @inner +
            N' @message=@p_msg, @message_bin=@p_bin, @headers=@p_hdr, ' +
            N'@delay_seconds=@p_delay, @msg_id=@p_id OUTPUT;';

        DECLARE @ids TABLE (msg_id BIGINT NOT NULL);
        DECLARE @p_msg NVARCHAR(MAX), @p_bin VARBINARY(MAX), @p_hdr NVARCHAR(MAX), @p_delay INT;
        DECLARE @p_id BIGINT;

        DECLARE c CURSOR LOCAL FAST_FORWARD FOR
            SELECT message, message_bin, headers, delay_seconds FROM @messages;
        OPEN c;
        FETCH NEXT FROM c INTO @p_msg, @p_bin, @p_hdr, @p_delay;
        WHILE @@FETCH_STATUS = 0
        BEGIN
            EXEC sp_executesql @sql,
                N'@p_msg NVARCHAR(MAX), @p_bin VARBINARY(MAX), @p_hdr NVARCHAR(MAX),
                  @p_delay INT, @p_id BIGINT OUTPUT',
                @p_msg = @p_msg, @p_bin = @p_bin, @p_hdr = @p_hdr,
                @p_delay = @p_delay, @p_id = @p_id OUTPUT;
            INSERT INTO @ids (msg_id) VALUES (@p_id);
            FETCH NEXT FROM c INTO @p_msg, @p_bin, @p_hdr, @p_delay;
        END
        CLOSE c; DEALLOCATE c;

        SELECT msg_id FROM @ids ORDER BY msg_id;
    END

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

    IF @storage = 'ondisk'
    BEGIN
        EXEC sqlmq._read_ondisk @queue, @payload, @vt_seconds, @max_count, @max_dlq;
    END
    ELSE
    BEGIN
        -- For in-memory queues, serialize read claims via the per-queue applock.
        -- Memory-opt tables don't support READPAST/UPDLOCK and rely on SNAPSHOT,
        -- which produces 41302 write-write conflicts under concurrent readers.
        -- We tried retry-on-41302 in _read_inmem (still kept as a defense in
        -- depth), but under sustained contention the retries can exhaust,
        -- causing the read proc to return 0 rows even when messages are
        -- ready — consumers then incorrectly conclude the queue is empty.
        --
        -- The applock serializes reads per queue, giving correct atomic
        -- claim semantics at the cost of single-threaded read throughput
        -- per queue. Send is already serialized by the same applock for
        -- strict FIFO, so this is consistent: per-queue work is single-
        -- threaded; cross-queue concurrency is unaffected. Phase 7 bake-off
        -- will quantify whether this is the right trade-off.
        BEGIN TRANSACTION;
        DECLARE @rc INT;
        EXEC @rc = sp_getapplock
            @Resource    = @queue,
            @LockMode    = N'Exclusive',
            @LockOwner   = N'Transaction',
            @LockTimeout = 30000;
        IF @rc < 0
        BEGIN
            ROLLBACK TRANSACTION;
            THROW 50042, 'sqlmq.[read]: sp_getapplock failed for inmemory read', 1;
        END

        EXEC sqlmq._read_inmem @queue, @payload, @vt_seconds, @max_count, @max_dlq;

        COMMIT TRANSACTION;
    END
END
GO

-- read_grouped: dispatcher acquires applock + wraps both storage variants in
-- the same transaction. _read_grouped_ondisk is rewritten to assume the lock
-- is held (no longer opens its own transaction).
CREATE OR ALTER PROCEDURE sqlmq._read_grouped_ondisk
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

    -- Caller (sqlmq.read_grouped) holds sp_getapplock @queue Exclusive Transaction.
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
END
GO

CREATE OR ALTER PROCEDURE sqlmq.read_grouped
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

    -- Acquire the per-queue applock once, here, regardless of storage type.
    -- The applock is required to make the eligible/claim/update sequence
    -- atomic across concurrent readers (see V009 commentary). For in-memory
    -- queues, the inner proc cannot itself call sp_getapplock so the lock
    -- has to live up here.
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

    IF @storage = 'ondisk'
        EXEC sqlmq._read_grouped_ondisk @queue, @payload, @vt_seconds, @max_count, @max_dlq;
    ELSE
        EXEC sqlmq._read_grouped_inmem  @queue, @payload, @vt_seconds, @max_count, @max_dlq;

    COMMIT TRANSACTION;
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
    IF @storage = 'ondisk'
        EXEC sqlmq._delete_ondisk @queue, @msg_ids;
    ELSE
        EXEC sqlmq._delete_inmem  @queue, @msg_ids;
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
    IF @storage = 'ondisk'
    BEGIN
        EXEC sqlmq._pop_ondisk @queue, @payload;
    END
    ELSE
    BEGIN
        -- See sqlmq.[read] — same per-queue applock pattern for in-memory pop.
        BEGIN TRANSACTION;
        DECLARE @rc INT;
        EXEC @rc = sp_getapplock
            @Resource    = @queue,
            @LockMode    = N'Exclusive',
            @LockOwner   = N'Transaction',
            @LockTimeout = 30000;
        IF @rc < 0
        BEGIN
            ROLLBACK TRANSACTION;
            THROW 50055, 'sqlmq.pop: sp_getapplock failed for inmemory pop', 1;
        END
        EXEC sqlmq._pop_inmem @queue, @payload;
        COMMIT TRANSACTION;
    END
END
GO

-- ---------------------------------------------------------------------------
-- 6. dlq_sweep: drop locking hints when running against a memory-optimized
-- queue table. The on-disk variant uses (READPAST, UPDLOCK, ROWLOCK,
-- READCOMMITTEDLOCK) but those are forbidden on memory-opt tables.
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

    -- Locking hints are only valid against the on-disk queue table.
    DECLARE @hints NVARCHAR(200) =
        CASE @storage WHEN 'ondisk' THEN N' WITH (READPAST, UPDLOCK, ROWLOCK, READCOMMITTEDLOCK)' ELSE N'' END;

    DECLARE @sql NVARCHAR(MAX) = N'
        DELETE q
          OUTPUT deleted.msg_id, deleted.enqueued_at, deleted.vt, deleted.read_ct,
                 deleted.group_key, deleted.' + @msg_col + N', deleted.headers,
                 SYSUTCDATETIME(), N''max_deliveries''
            INTO sqlmq.' + QUOTENAME(@atable) + N' (
                 msg_id, enqueued_at, vt, read_ct, group_key, ' + @msg_col + N', headers,
                 archived_at, dlq_reason)
          FROM sqlmq.' + QUOTENAME(@qtable) + N' AS q' + @hints + N'
         WHERE q.read_ct >= @dlq;
        SELECT @@ROWCOUNT AS rows_swept;';
    EXEC sp_executesql @sql, N'@dlq INT', @dlq = @max_dlq;
END
GO
