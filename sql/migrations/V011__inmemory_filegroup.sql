-- V011: Add MEMORY_OPTIMIZED_DATA filegroup to the current database.
--
-- Required before any memory-optimized table can be created. The companion
-- migration V012 then extends sqlmq.create_queue / drop_queue and the public
-- dispatchers to support @storage='inmemory'.
--
-- ALTER DATABASE statements cannot run inside a multi-statement transaction,
-- so the companion file V011__inmemory_filegroup.sql.conf sets
-- executeInTransaction=false for this migration. Splitting filegroup setup
-- into its own migration keeps that exemption tightly scoped — V012 can
-- still run inside Flyway's default transaction.

DECLARE @db SYSNAME = DB_NAME();
DECLARE @data_dir NVARCHAR(500);

-- Find the directory of the primary data file (works on Windows and Linux).
SELECT TOP 1
    @data_dir = LEFT(physical_name,
        LEN(physical_name) -
        CASE
            WHEN CHARINDEX('/', REVERSE(physical_name)) > 0
                THEN CHARINDEX('/', REVERSE(physical_name))
            ELSE CHARINDEX('\', REVERSE(physical_name))
        END + 1)
FROM sys.master_files
WHERE database_id = DB_ID() AND type = 0;

-- Use a unique-ish suffix for the filegroup directory name to avoid collisions
-- when many test databases live on the same SQL Server instance (Testcontainers
-- creates and tears down dozens of databases over the course of a test run).
DECLARE @fg_dir NVARCHAR(200) = N'sqlmq_imoltp_' + REPLACE(CAST(NEWID() AS NVARCHAR(36)), N'-', N'');

DECLARE @sql NVARCHAR(MAX) = N'
ALTER DATABASE [' + @db + N'] ADD FILEGROUP sqlmq_imoltp CONTAINS MEMORY_OPTIMIZED_DATA;
ALTER DATABASE [' + @db + N'] ADD FILE (
    NAME = N''' + @fg_dir + N''',
    FILENAME = N''' + @data_dir + @fg_dir + N'''
) TO FILEGROUP sqlmq_imoltp;
ALTER DATABASE [' + @db + N'] SET MEMORY_OPTIMIZED_ELEVATE_TO_SNAPSHOT = ON;';

EXEC sp_executesql @sql;
GO
