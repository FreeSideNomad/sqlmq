-- V001: sqlmq core schema, registry table, TVPs

CREATE SCHEMA sqlmq AUTHORIZATION dbo;
GO

-- Queue registry. One row per queue. Hot row, kept small.
CREATE TABLE sqlmq.meta (
    queue_name          SYSNAME       NOT NULL PRIMARY KEY,
    storage_type        VARCHAR(16)   NOT NULL,
    is_grouped          BIT           NOT NULL CONSTRAINT DF_sqlmq_meta_is_grouped DEFAULT (0),
    payload_type        VARCHAR(8)    NOT NULL,
    max_delivery_count  INT           NULL,
    created_at          DATETIME2(7)  NOT NULL CONSTRAINT DF_sqlmq_meta_created_at DEFAULT (SYSUTCDATETIME()),
    CONSTRAINT CK_sqlmq_meta_storage_type
        CHECK (storage_type IN ('ondisk', 'inmemory')),
    CONSTRAINT CK_sqlmq_meta_payload_type
        CHECK (payload_type IN ('json', 'binary'))
);
GO

-- TVP for batch send. Caller fills exactly one of (message, message_bin) per row
-- depending on the queue's payload_type; procs validate.
CREATE TYPE dbo.sqlmq_send_tvp AS TABLE (
    message       NVARCHAR(MAX) NULL,
    message_bin   VARBINARY(MAX) NULL,
    headers       NVARCHAR(MAX) NULL,
    delay_seconds INT NOT NULL DEFAULT 0
);
GO

-- TVP for batch delete / archive. Caller passes msg_ids.
CREATE TYPE dbo.sqlmq_msg_id_tvp AS TABLE (
    msg_id BIGINT NOT NULL PRIMARY KEY
);
GO
