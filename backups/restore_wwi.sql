/*
    restore_wwi.sql
    Restores the WideWorldImporters source database from
    WideWorldImporters-Full.bak so a clean clone can rebuild the source (NF-01).
    WITH REPLACE: overwrites any existing WideWorldImporters database,
    including rows added by sql/simulate_one_day.sql.

    Run (./backups is mounted at /var/opt/mssql/backup/ in wwi-sqlserver):
        docker exec wwi-sqlserver bash -c '/opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P "$MSSQL_SA_PASSWORD" -C -i /var/opt/mssql/backup/restore_wwi.sql'
    (Git Bash on Windows: prefix with MSYS_NO_PATHCONV=1 so /opt/... is not rewritten.)
*/

USE master;
GO

-- Kick off any open connections so the restore is not blocked.
IF DB_ID('WideWorldImporters') IS NOT NULL
    ALTER DATABASE WideWorldImporters SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
GO

RESTORE DATABASE WideWorldImporters
FROM DISK = N'/var/opt/mssql/backup/WideWorldImporters-Full.bak'
WITH REPLACE,
     MOVE N'WWI_Primary'         TO N'/var/opt/mssql/data/WideWorldImporters.mdf',
     MOVE N'WWI_UserData'        TO N'/var/opt/mssql/data/WideWorldImporters_UserData.ndf',
     MOVE N'WWI_Log'             TO N'/var/opt/mssql/data/WideWorldImporters.ldf',
     -- Type S (in-memory filegroup): target is a directory, not a file.
     MOVE N'WWI_InMemory_Data_1' TO N'/var/opt/mssql/data/WideWorldImporters_InMemory_Data_1',
     STATS = 10;
GO

ALTER DATABASE WideWorldImporters SET MULTI_USER;
GO
