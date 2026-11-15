/*
    restore_filelist.sql
    Lists the logical files inside WideWorldImporters-Full.bak (read-only,
    changes nothing). Use it to confirm the logical names that the MOVE
    clauses in restore_wwi.sql expect.

    Run (./backups is mounted at /var/opt/mssql/backup/ in wwi-sqlserver):
        docker exec wwi-sqlserver bash -c '/opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P "$MSSQL_SA_PASSWORD" -C -i /var/opt/mssql/backup/restore_filelist.sql'
    (Git Bash on Windows: prefix with MSYS_NO_PATHCONV=1 so /opt/... is not rewritten.)
*/

RESTORE FILELISTONLY
FROM DISK = N'/var/opt/mssql/backup/WideWorldImporters-Full.bak';
