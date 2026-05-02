-- V010: Serialize sqlmq.send and sqlmq.send_batch per queue with sp_getapplock.
--
-- Why: IDENTITY assigns msg_id at INSERT execution time, but row visibility depends on
-- COMMIT time. Under concurrent producers, commit order can differ from msg_id order,
-- so a reader of a grouped queue running with sp_getapplock can pick a higher msg_id
-- as the head of a group, then later see a lower (just-committed) msg_id of the same
-- group and deliver it AFTER the higher one. That violates the user-mandated invariant
-- "FIFO is maintained per queue".
--
-- Fix: have both send and send_batch acquire sp_getapplock @Resource = @queue
-- (Exclusive, Transaction-owned) BEFORE inserting. With LockOwner='Transaction', the
-- lock auto-releases when the wrapping transaction commits or rolls back. This makes
-- commit order match msg_id order per queue. Multi-queue throughput is unaffected.
--
-- The same resource name (@queue) is used by sqlmq._read_grouped_ondisk in V009, so
-- send and grouped reads serialize against each other on the same queue. This is the
-- intended behavior for strict FIFO.
--
-- Trade-off (acceptable per user directive): per-queue ingest throughput is now
-- bottlenecked by single-threaded send. Multiple queues still scale linearly.
--
-- Transactional contract: callers wrapping multiple sends in one transaction will
-- hold the applock for the whole transaction, which is correct (multi-send-in-one-txn
-- is atomic by intent).
--
-- Implementation note: an explicit BEGIN/COMMIT TRANSACTION wrapper is required because
-- @LockOwner='Transaction' refuses to attach to nothing. JDBC autocommit does not start
-- a transaction for an EXEC of a stored proc until the proc itself runs DML; by then it
-- is too late — the applock would attach to the auto-txn around the INSERT, not be held
-- BEFORE it. Wrapping the proc body in BEGIN+COMMIT ensures @@TRANCOUNT >= 1 from the
-- moment we call sp_getapplock through the moment we COMMIT.

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
    IF @storage <> 'ondisk' THROW 50031, 'In-memory send not implemented yet (V011).', 1;

    BEGIN TRANSACTION;

    -- sp_getapplock returns -1 (timeout), -2 (cancel), -3 (deadlock victim),
    -- -999 (param/other). 300s is a deliberately generous ceiling: under the
    -- "send-serialized per queue" contract a queue with N concurrent producers
    -- effectively gets N-deep wait queue, so timeouts must scale with the
    -- expected backlog rather than per-call latency. The bake-off in Phase 7
    -- will quantify p99 send latencies for typical workloads. If callers need
    -- a tighter SLA they should shard work across multiple queues.
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

    DECLARE @storage VARCHAR(16), @payload VARCHAR(8);
    SELECT @storage = storage_type, @payload = payload_type
      FROM sqlmq.meta WHERE queue_name = @queue;
    IF @storage IS NULL THROW 50032, 'Queue does not exist.', 1;
    IF @storage <> 'ondisk' THROW 50033, 'In-memory send_batch not implemented yet (V011).', 1;

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
