-- V013: Per-queue natively compiled in-memory READ / POP / READ_GROUPED procs.
--
-- Background. V012 introduced the in-memory storage variant but kept all
-- read-side code interpreted: sqlmq.[read], sqlmq.pop, and sqlmq.read_grouped
-- each acquired sp_getapplock @queue Exclusive Transaction before calling the
-- shared sqlmq._read_inmem helper. That serialized every concurrent consumer
-- on the same queue and prevented the in-memory variant from beating the
-- on-disk variant under any consumer-fan-out workload (Phase 7 bake-off
-- showed in-memory peaked at 4 consumers while on-disk continued to scale to
-- 16+).
--
-- V013 fixes that architectural flaw. It generates a per-queue natively
-- compiled inner proc at create_queue time and removes the per-call
-- sp_getapplock from the read-side dispatchers for in-memory queues. Memory-
-- optimized tables are designed for SNAPSHOT-MVCC concurrency; the
-- 41302 (write-write conflict) retry handles contention as designed. Multiple
-- consumers can now drain the queue truly in parallel, matching the on-disk
-- "competing consumers" pattern.
--
-- What stays:
--   * Send-side sp_getapplock (V010) STAYS for both ondisk and inmemory.
--     Strict per-queue FIFO requires send to serialize per queue: IDENTITY
--     allocates msg_id at INSERT execution but visibility depends on COMMIT
--     order, so concurrent producers can otherwise commit out of order.
--   * Read-side sp_getapplock for sqlmq.read_grouped STAYS for BOTH ondisk
--     and inmemory. Strict per-group FIFO ("at most one in-flight per
--     group") requires the eligible / claim sequence to run under exclusive
--     serialization across consumers. Without it, two consumers under
--     different memory-opt snapshots can each see "no inflight in group X"
--     and pick distinct rows of group X (no row-level write conflict
--     because they pick different rows). SERIALIZABLE inside the atomic
--     block was tried as an alternative but produced a 41325-storm under
--     16 concurrent consumers that the dispatcher's retry budget couldn't
--     absorb. The applock is the pragmatic choice for grouped; it doesn't
--     affect the bench-scan workload (which uses the non-grouped read).
--
-- What comes off (for in-memory queues only):
--   * The BEGIN TRANSACTION + sp_getapplock wrapper around _read_inmem in
--     sqlmq.[read].
--   * The same wrapper around _pop_inmem in sqlmq.pop.
--
-- Empirical findings while developing this migration:
--
-- * Natively compiled procs cannot use subqueries inside UPDATE/DELETE — even
--   the standard "UPDATE TOP (N) ... WHERE col IN (SELECT TOP (N) ...)" idiom
--   from the on-disk read fails with "Subqueries (queries nested inside
--   another query) is only supported in SELECT statements with natively
--   compiled modules."
--
-- * Inline DECLARE TABLE inside a natively compiled body is not supported —
--   "Inline table variables are not supported with natively compiled
--   modules." Memory-opt table types must be defined out-of-line and passed
--   as TVPs.
--
-- * Joining a TVP to the queue table for an UPDATE also fails: "Using the
--   FROM clause in an UPDATE statement and specifying a table source in a
--   DELETE statement is not supported with natively compiled modules."
--
-- * Therefore the reading pattern that DOES compile is a WHILE loop of
--   SELECT TOP (1) @id = msg_id ORDER BY msg_id (no subquery, just a
--   scalar assignment), then UPDATE WHERE msg_id = @id OUTPUT inserted.*.
--   The pure UPDATE TOP (1) WHERE vt <= NOW form (without the SELECT-into-
--   scalar step) compiles BUT does not guarantee msg_id order — the
--   optimizer may pick rows in hash-bucket order, breaking per-consumer
--   FIFO. The SELECT/UPDATE pair adds one more memory-opt lookup per
--   claimed row but is the only way to get FIFO ordering inside a
--   natively compiled module.
--
-- * For grouped reads, NOT EXISTS subqueries inside SELECT are allowed in
--   natively compiled modules. The grouped variant uses the same SELECT/
--   UPDATE pattern with the inflight-check NOT EXISTS predicate added to
--   the SELECT WHERE clause.
--
-- * Concurrency / isolation:
--     - _read_inmem and _pop_inmem use TRANSACTION ISOLATION LEVEL = SNAPSHOT.
--       Multiple consumers picking the same row collide on UPDATE -> 41302;
--       the *outer* dispatcher catches and retries (a fresh EXEC starts a
--       fresh atomic-block transaction with a fresh snapshot, eventually
--       seeing the just-committed change).
--     - _read_grouped_inmem also uses SNAPSHOT — the dispatcher's
--       sp_getapplock provides the cross-consumer serialization needed for
--       per-group FIFO; the snapshot inside ATOMIC merely needs to read
--       consistently within one call.
--
-- * 41302 / 41325 retry budget: 50 attempts in the dispatcher. Each attempt
--   is a fresh ATOMIC block on the memory-opt engine and resolves in a few
--   microseconds, so 50 attempts adds at most ~ms of latency under heavy
--   contention. Below 50 (we tried 5 first) the harness's NoLossNoDouble-
--   DeliveryTest with 32 consumers exhausted the budget often enough that
--   the test's quiet-period timer expired before the queue drained, with
--   ~30 messages "missing" (not lost — undelivered when the test gave up).
--
-- * The dispatcher merges per-iteration result sets via INSERT @captured EXEC
--   <native_proc>. Verified empirically that this collapses N OUTPUT result
--   sets into one MARS-free table at the dispatcher level.
--
-- ---------------------------------------------------------------------------
-- 1. _gen_inmem_read_proc / _gen_inmem_pop_proc / _gen_inmem_read_grouped_proc
--
-- Each generates one per-queue natively compiled proc, baking in the queue
-- table name (SCHEMABINDING requirement) and the payload column choice
-- (message NVARCHAR vs message_bin VARBINARY).
-- ---------------------------------------------------------------------------

CREATE OR ALTER PROCEDURE sqlmq._gen_inmem_read_proc
    @name SYSNAME,
    @payload_type VARCHAR(8)
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @qfqn NVARCHAR(300) = N'sqlmq.' + QUOTENAME(N'q_' + @name);
    DECLARE @proc_name SYSNAME = N'_read_inmem_' + @name;
    DECLARE @msg_col NVARCHAR(64) =
        CASE @payload_type WHEN 'json' THEN N'message' ELSE N'message_bin' END;

    -- Drop first so CREATE OR ALTER semantics work even when SCHEMABINDING
    -- pinned the prior version to an outdated table shape (e.g. table was
    -- recreated as part of a queue-recreate flow).
    DECLARE @drop NVARCHAR(MAX) =
        N'DROP PROCEDURE IF EXISTS sqlmq.' + QUOTENAME(@proc_name) + N';';
    EXEC sp_executesql @drop;

    -- WHILE-loop using SELECT TOP (1) ... ORDER BY msg_id to pick the lowest
    -- eligible msg_id, then UPDATE that exact row. This preserves per-consumer
    -- FIFO (msg_id ascending) within and across batched read calls.
    --
    -- Pattern chosen because subqueries inside UPDATE and inline table vars
    -- are both forbidden in natively compiled modules. The pure UPDATE TOP(1)
    -- form (without an ORDER BY scalar) doesn't guarantee msg_id order — the
    -- optimizer may pick rows in hash-bucket order, which breaks FIFO.
    --
    -- Each iteration's OUTPUT is a separate result set; the interpreted
    -- dispatcher merges them via INSERT @captured EXEC.
    --
    -- @id IS NULL means "no eligible rows" -> return early. The proc emits
    -- between 0 and @max_count result sets per call.
    DECLARE @sql NVARCHAR(MAX) = N'
CREATE PROCEDURE sqlmq.' + QUOTENAME(@proc_name) + N'
    @vt_seconds INT,
    @max_count  INT,
    @max_dlq    INT
WITH NATIVE_COMPILATION, SCHEMABINDING
AS
BEGIN ATOMIC WITH (TRANSACTION ISOLATION LEVEL = SNAPSHOT, LANGUAGE = N''us_english'')
    DECLARE @i INT = 0;
    DECLARE @id BIGINT;
    DECLARE @new_vt DATETIME2(7);
    WHILE @i < @max_count
    BEGIN
        SET @id = NULL;
        SELECT TOP (1) @id = msg_id
          FROM ' + @qfqn + N'
         WHERE vt <= SYSUTCDATETIME()
           AND (@max_dlq IS NULL OR read_ct < @max_dlq)
         ORDER BY msg_id;
        IF @id IS NULL RETURN;
        SET @new_vt = DATEADD(SECOND, @vt_seconds, SYSUTCDATETIME());
        UPDATE ' + @qfqn + N'
           SET vt = @new_vt,
               read_ct = read_ct + 1
        OUTPUT inserted.msg_id, inserted.read_ct, inserted.enqueued_at,
               inserted.vt, inserted.group_key,
               inserted.' + @msg_col + N', inserted.headers
         WHERE msg_id = @id;
        SET @i = @i + 1;
    END
END';
    EXEC sp_executesql @sql;
END
GO

CREATE OR ALTER PROCEDURE sqlmq._gen_inmem_pop_proc
    @name SYSNAME,
    @payload_type VARCHAR(8)
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @qfqn NVARCHAR(300) = N'sqlmq.' + QUOTENAME(N'q_' + @name);
    DECLARE @proc_name SYSNAME = N'_pop_inmem_' + @name;
    DECLARE @msg_col NVARCHAR(64) =
        CASE @payload_type WHEN 'json' THEN N'message' ELSE N'message_bin' END;

    DECLARE @drop NVARCHAR(MAX) =
        N'DROP PROCEDURE IF EXISTS sqlmq.' + QUOTENAME(@proc_name) + N';';
    EXEC sp_executesql @drop;

    -- pop is single-row by design (interface returns the head message or
    -- nothing). SELECT TOP (1) ORDER BY msg_id to pick the lowest, then
    -- DELETE WHERE msg_id = @id. ORDER BY ensures FIFO; pure DELETE TOP(1)
    -- without it could pick rows in hash-bucket order.
    DECLARE @sql NVARCHAR(MAX) = N'
CREATE PROCEDURE sqlmq.' + QUOTENAME(@proc_name) + N'
WITH NATIVE_COMPILATION, SCHEMABINDING
AS
BEGIN ATOMIC WITH (TRANSACTION ISOLATION LEVEL = SNAPSHOT, LANGUAGE = N''us_english'')
    DECLARE @id BIGINT;
    SELECT TOP (1) @id = msg_id
      FROM ' + @qfqn + N'
     WHERE vt <= SYSUTCDATETIME()
     ORDER BY msg_id;
    IF @id IS NULL RETURN;
    DELETE FROM ' + @qfqn + N'
    OUTPUT deleted.msg_id, deleted.read_ct, deleted.enqueued_at, deleted.vt,
           deleted.group_key, deleted.' + @msg_col + N', deleted.headers
     WHERE msg_id = @id;
END';
    EXEC sp_executesql @sql;
END
GO

CREATE OR ALTER PROCEDURE sqlmq._gen_inmem_read_grouped_proc
    @name SYSNAME,
    @payload_type VARCHAR(8)
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @qfqn NVARCHAR(300) = N'sqlmq.' + QUOTENAME(N'q_' + @name);
    DECLARE @proc_name SYSNAME = N'_read_grouped_inmem_' + @name;
    DECLARE @msg_col NVARCHAR(64) =
        CASE @payload_type WHEN 'json' THEN N'message' ELSE N'message_bin' END;

    DECLARE @drop NVARCHAR(MAX) =
        N'DROP PROCEDURE IF EXISTS sqlmq.' + QUOTENAME(@proc_name) + N';';
    EXEC sp_executesql @drop;

    -- Grouped read runs under the dispatcher's per-queue sp_getapplock
    -- (sqlmq.read_grouped acquires it for both ondisk and inmemory). With
    -- the applock providing cross-consumer serialization, SNAPSHOT inside
    -- the atomic block is sufficient — no SERIALIZABLE phantom validation
    -- needed and no per-iteration 41325 churn under high consumer fan-out.
    --
    -- Pattern: SELECT TOP (1) @id = msg_id WHERE eligible AND group-inflight
    -- check; if @id IS NULL break; UPDATE single-row by msg_id OUTPUT
    -- inserted.*. Within the same atomic txn, after the UPDATE bumps vt,
    -- next iteration's SELECT correctly excludes other rows of the same
    -- group via the inflight subquery.
    DECLARE @sql NVARCHAR(MAX) = N'
CREATE PROCEDURE sqlmq.' + QUOTENAME(@proc_name) + N'
    @vt_seconds INT,
    @max_count  INT,
    @max_dlq    INT
WITH NATIVE_COMPILATION, SCHEMABINDING
AS
BEGIN ATOMIC WITH (TRANSACTION ISOLATION LEVEL = SNAPSHOT, LANGUAGE = N''us_english'')
    DECLARE @i INT = 0;
    DECLARE @id BIGINT;
    DECLARE @new_vt DATETIME2(7);
    WHILE @i < @max_count
    BEGIN
        SET @id = NULL;
        SELECT TOP (1) @id = q.msg_id
          FROM ' + @qfqn + N' AS q
         WHERE q.vt <= SYSUTCDATETIME()
           AND (@max_dlq IS NULL OR q.read_ct < @max_dlq)
           AND (q.group_key IS NULL OR NOT EXISTS (
                SELECT 1 FROM ' + @qfqn + N' AS inflight
                 WHERE inflight.group_key = q.group_key
                   AND inflight.vt > SYSUTCDATETIME()))
         ORDER BY q.msg_id;
        IF @id IS NULL RETURN;
        SET @new_vt = DATEADD(SECOND, @vt_seconds, SYSUTCDATETIME());
        UPDATE ' + @qfqn + N'
           SET vt = @new_vt,
               read_ct = read_ct + 1
        OUTPUT inserted.msg_id, inserted.read_ct, inserted.enqueued_at,
               inserted.vt, inserted.group_key,
               inserted.' + @msg_col + N', inserted.headers
         WHERE msg_id = @id;
        SET @i = @i + 1;
    END
END';
    EXEC sp_executesql @sql;
END
GO

-- ---------------------------------------------------------------------------
-- 2. create_queue: also generate the per-queue native read/pop/grouped procs.
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

        -- For in-memory queues, generate the per-queue natively compiled
        -- send/read/pop procs (and the grouped read proc when grouped=1).
        IF @storage = 'inmemory'
        BEGIN
            EXEC sqlmq._gen_inmem_send_proc @name;
            EXEC sqlmq._gen_inmem_read_proc @name, @payload_type;
            EXEC sqlmq._gen_inmem_pop_proc  @name, @payload_type;
            IF @grouped = 1
                EXEC sqlmq._gen_inmem_read_grouped_proc @name, @payload_type;
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

        -- Drop any per-queue native procs that may have been created.
        DECLARE @drop_procs NVARCHAR(MAX) =
            N'DROP PROCEDURE IF EXISTS sqlmq.' + QUOTENAME(N'_send_inmem_' + @name) + N';' +
            N'DROP PROCEDURE IF EXISTS sqlmq.' + QUOTENAME(N'_read_inmem_' + @name) + N';' +
            N'DROP PROCEDURE IF EXISTS sqlmq.' + QUOTENAME(N'_pop_inmem_'  + @name) + N';' +
            N'DROP PROCEDURE IF EXISTS sqlmq.' + QUOTENAME(N'_read_grouped_inmem_' + @name) + N';';
        BEGIN TRY EXEC sp_executesql @drop_procs; END TRY BEGIN CATCH END CATCH;

        DELETE FROM sqlmq.meta WHERE queue_name = @name;
        THROW;
    END CATCH
END
GO

-- ---------------------------------------------------------------------------
-- 3. drop_queue: drop the per-queue native procs before the table
-- (SCHEMABINDING constraint).
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

    -- Drop all per-queue natively compiled procs first; they SCHEMABIND the
    -- queue table and would block DROP TABLE.
    DECLARE @drop_procs NVARCHAR(MAX) =
        N'DROP PROCEDURE IF EXISTS sqlmq.' + QUOTENAME(N'_send_inmem_' + @name) + N';' +
        N'DROP PROCEDURE IF EXISTS sqlmq.' + QUOTENAME(N'_read_inmem_' + @name) + N';' +
        N'DROP PROCEDURE IF EXISTS sqlmq.' + QUOTENAME(N'_pop_inmem_'  + @name) + N';' +
        N'DROP PROCEDURE IF EXISTS sqlmq.' + QUOTENAME(N'_read_grouped_inmem_' + @name) + N';';
    EXEC sp_executesql @drop_procs;

    DECLARE @drop NVARCHAR(MAX) =
        N'DROP TABLE IF EXISTS sqlmq.' + QUOTENAME(@qtable) + N';' +
        N'DROP TABLE IF EXISTS sqlmq.' + QUOTENAME(@atable) + N';';
    EXEC sp_executesql @drop;

    DELETE FROM sqlmq.meta WHERE queue_name = @name;
END
GO

-- ---------------------------------------------------------------------------
-- 4. Public dispatchers: drop the per-call sp_getapplock for inmemory reads
-- and route to the per-queue native proc, with INSERT-EXEC merging the
-- per-iteration result sets and a 41302/41325 retry loop.
-- ---------------------------------------------------------------------------

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
        RETURN;
    END

    -- inmemory: route to per-queue native proc, merge per-iteration result
    -- sets via INSERT-EXEC, retry on 41302 (write-write conflict under
    -- SNAPSHOT). No sp_getapplock — natively compiled SNAPSHOT MVCC is the
    -- intended concurrency model for memory-optimized tables.
    DECLARE @inner_sql NVARCHAR(MAX) =
        N'EXEC sqlmq.' + QUOTENAME(N'_read_inmem_' + @queue) +
        N' @vt_seconds=@vt, @max_count=@n, @max_dlq=@dlq;';

    IF @payload = 'json'
    BEGIN
        DECLARE @captured_j TABLE (
            msg_id BIGINT, read_ct INT, enqueued_at DATETIME2(7), vt DATETIME2(7),
            group_key NVARCHAR(255), message NVARCHAR(MAX), headers NVARCHAR(MAX)
        );
        DECLARE @attempts_j INT = 0;
        WHILE @attempts_j < 50
        BEGIN
            BEGIN TRY
                DELETE FROM @captured_j;
                INSERT INTO @captured_j (msg_id, read_ct, enqueued_at, vt, group_key, message, headers)
                EXEC sp_executesql @inner_sql,
                    N'@vt INT, @n INT, @dlq INT',
                    @vt = @vt_seconds, @n = @max_count, @dlq = @max_dlq;
                BREAK;
            END TRY
            BEGIN CATCH
                IF ERROR_NUMBER() IN (41302, 41305, 41325, 41301)
                    SET @attempts_j += 1;
                ELSE
                    THROW;
            END CATCH
        END
        SELECT msg_id, read_ct, enqueued_at, vt, group_key, message, headers
          FROM @captured_j ORDER BY msg_id;
        RETURN;
    END

    -- payload = 'binary'
    DECLARE @captured_b TABLE (
        msg_id BIGINT, read_ct INT, enqueued_at DATETIME2(7), vt DATETIME2(7),
        group_key NVARCHAR(255), message_bin VARBINARY(MAX), headers NVARCHAR(MAX)
    );
    DECLARE @attempts_b INT = 0;
    WHILE @attempts_b < 50
    BEGIN
        BEGIN TRY
            DELETE FROM @captured_b;
            INSERT INTO @captured_b (msg_id, read_ct, enqueued_at, vt, group_key, message_bin, headers)
            EXEC sp_executesql @inner_sql,
                N'@vt INT, @n INT, @dlq INT',
                @vt = @vt_seconds, @n = @max_count, @dlq = @max_dlq;
            BREAK;
        END TRY
        BEGIN CATCH
            IF ERROR_NUMBER() IN (41302, 41305, 41325, 41301)
                SET @attempts_b += 1;
            ELSE
                THROW;
        END CATCH
    END
    SELECT msg_id, read_ct, enqueued_at, vt, group_key, message_bin, headers
      FROM @captured_b ORDER BY msg_id;
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
        RETURN;
    END

    -- inmemory: per-queue native proc, INSERT-EXEC merge, retry on 41302.
    -- pop is single-row by design (max one DELETE TOP (1) inside the proc).
    DECLARE @inner_sql NVARCHAR(MAX) =
        N'EXEC sqlmq.' + QUOTENAME(N'_pop_inmem_' + @queue) + N';';

    IF @payload = 'json'
    BEGIN
        DECLARE @captured_j TABLE (
            msg_id BIGINT, read_ct INT, enqueued_at DATETIME2(7), vt DATETIME2(7),
            group_key NVARCHAR(255), message NVARCHAR(MAX), headers NVARCHAR(MAX)
        );
        DECLARE @attempts_j INT = 0;
        WHILE @attempts_j < 50
        BEGIN
            BEGIN TRY
                DELETE FROM @captured_j;
                INSERT INTO @captured_j (msg_id, read_ct, enqueued_at, vt, group_key, message, headers)
                EXEC sp_executesql @inner_sql;
                BREAK;
            END TRY
            BEGIN CATCH
                IF ERROR_NUMBER() IN (41302, 41305, 41325, 41301)
                    SET @attempts_j += 1;
                ELSE
                    THROW;
            END CATCH
        END
        SELECT msg_id, read_ct, enqueued_at, vt, group_key, message, headers
          FROM @captured_j ORDER BY msg_id;
        RETURN;
    END

    -- binary
    DECLARE @captured_b TABLE (
        msg_id BIGINT, read_ct INT, enqueued_at DATETIME2(7), vt DATETIME2(7),
        group_key NVARCHAR(255), message_bin VARBINARY(MAX), headers NVARCHAR(MAX)
    );
    DECLARE @attempts_b INT = 0;
    WHILE @attempts_b < 50
    BEGIN
        BEGIN TRY
            DELETE FROM @captured_b;
            INSERT INTO @captured_b (msg_id, read_ct, enqueued_at, vt, group_key, message_bin, headers)
            EXEC sp_executesql @inner_sql;
            BREAK;
        END TRY
        BEGIN CATCH
            IF ERROR_NUMBER() IN (41302, 41305, 41325, 41301)
                SET @attempts_b += 1;
            ELSE
                THROW;
        END CATCH
    END
    SELECT msg_id, read_ct, enqueued_at, vt, group_key, message_bin, headers
      FROM @captured_b ORDER BY msg_id;
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

    -- Grouped read serializes per queue via sp_getapplock on BOTH storage
    -- variants. Strict per-group FIFO ("at most one in-flight per group")
    -- requires the eligible/claim sequence to run under exclusive
    -- serialization across consumers; otherwise two consumers under
    -- different snapshots can each pick distinct rows of the same group
    -- without raising any row-level write conflict.
    --
    -- For ondisk we acquire the applock and call the unchanged
    -- _read_grouped_ondisk inner proc. For inmemory we acquire the same
    -- applock and call the per-queue natively compiled inner proc through
    -- INSERT-EXEC. SNAPSHOT isolation inside the atomic block is sufficient
    -- once the applock provides cross-consumer serialization.
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
    BEGIN
        EXEC sqlmq._read_grouped_ondisk @queue, @payload, @vt_seconds, @max_count, @max_dlq;
        COMMIT TRANSACTION;
        RETURN;
    END

    -- inmemory: per-queue native proc, INSERT-EXEC merge. The applock above
    -- prevents concurrent reads on this queue, so the SNAPSHOT inside the
    -- atomic block sees a stable view. 41302/41325 retries are kept as
    -- defense-in-depth (e.g. write-write conflict against an in-flight
    -- send under the same applock).
    DECLARE @inner_sql NVARCHAR(MAX) =
        N'EXEC sqlmq.' + QUOTENAME(N'_read_grouped_inmem_' + @queue) +
        N' @vt_seconds=@vt, @max_count=@n, @max_dlq=@dlq;';

    IF @payload = 'json'
    BEGIN
        DECLARE @captured_j TABLE (
            msg_id BIGINT, read_ct INT, enqueued_at DATETIME2(7), vt DATETIME2(7),
            group_key NVARCHAR(255), message NVARCHAR(MAX), headers NVARCHAR(MAX)
        );
        DECLARE @attempts_j INT = 0;
        WHILE @attempts_j < 50
        BEGIN
            BEGIN TRY
                DELETE FROM @captured_j;
                INSERT INTO @captured_j (msg_id, read_ct, enqueued_at, vt, group_key, message, headers)
                EXEC sp_executesql @inner_sql,
                    N'@vt INT, @n INT, @dlq INT',
                    @vt = @vt_seconds, @n = @max_count, @dlq = @max_dlq;
                BREAK;
            END TRY
            BEGIN CATCH
                IF ERROR_NUMBER() IN (41302, 41305, 41325, 41301)
                    SET @attempts_j += 1;
                ELSE
                    THROW;
            END CATCH
        END
        SELECT msg_id, read_ct, enqueued_at, vt, group_key, message, headers
          FROM @captured_j ORDER BY msg_id;
        COMMIT TRANSACTION;
        RETURN;
    END

    -- binary
    DECLARE @captured_b TABLE (
        msg_id BIGINT, read_ct INT, enqueued_at DATETIME2(7), vt DATETIME2(7),
        group_key NVARCHAR(255), message_bin VARBINARY(MAX), headers NVARCHAR(MAX)
    );
    DECLARE @attempts_b INT = 0;
    WHILE @attempts_b < 50
    BEGIN
        BEGIN TRY
            DELETE FROM @captured_b;
            INSERT INTO @captured_b (msg_id, read_ct, enqueued_at, vt, group_key, message_bin, headers)
            EXEC sp_executesql @inner_sql,
                N'@vt INT, @n INT, @dlq INT',
                @vt = @vt_seconds, @n = @max_count, @dlq = @max_dlq;
            BREAK;
        END TRY
        BEGIN CATCH
            IF ERROR_NUMBER() IN (41302, 41305, 41325, 41301)
                SET @attempts_b += 1;
            ELSE
                THROW;
        END CATCH
    END
    SELECT msg_id, read_ct, enqueued_at, vt, group_key, message_bin, headers
      FROM @captured_b ORDER BY msg_id;
    COMMIT TRANSACTION;
END
GO

-- ---------------------------------------------------------------------------
-- 5. Auto-upgrade existing in-memory queues created before V013.
--
-- Idempotent: each generator does CREATE OR ALTER (well, DROP+CREATE because
-- ALTER doesn't change SCHEMABINDING-pinned dependencies) so re-running V013
-- is safe.
-- ---------------------------------------------------------------------------

DECLARE upgrade_cursor CURSOR LOCAL FAST_FORWARD FOR
    SELECT queue_name, payload_type, is_grouped
      FROM sqlmq.meta
     WHERE storage_type = 'inmemory';
OPEN upgrade_cursor;
DECLARE @qname SYSNAME, @ptype VARCHAR(8), @grp BIT;
FETCH NEXT FROM upgrade_cursor INTO @qname, @ptype, @grp;
WHILE @@FETCH_STATUS = 0
BEGIN
    EXEC sqlmq._gen_inmem_read_proc @qname, @ptype;
    EXEC sqlmq._gen_inmem_pop_proc  @qname, @ptype;
    IF @grp = 1
        EXEC sqlmq._gen_inmem_read_grouped_proc @qname, @ptype;
    FETCH NEXT FROM upgrade_cursor INTO @qname, @ptype, @grp;
END
CLOSE upgrade_cursor;
DEALLOCATE upgrade_cursor;
GO
