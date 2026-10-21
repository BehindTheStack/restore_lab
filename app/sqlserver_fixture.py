"""Dangerous SQL Server fixture ADMIN actions, only for disposable lab source."""
import os
import sys
import time

import pymssql

HOST=os.getenv('MSSQL_HOST','remote-sqlserver')
PASSWORD=os.environ['MSSQL_SA_PASSWORD']


def admin(database='master'):
    last=None
    for attempt in range(45):
        try:
            conn=pymssql.connect(server=HOST,user='sa',password=PASSWORD,
                                 database=database,login_timeout=4,timeout=20)
            conn.autocommit(True)
            return conn
        except Exception as exc:
            last=exc
            time.sleep(2)
    raise RuntimeError(f'SQL Server admin unavailable: {last}')


def exec_sql(conn, statement):
    with conn.cursor() as cur:
        cur.execute(statement)


def run(action):
    if action=='init':
        with admin() as root:
            exec_sql(root,"IF DB_ID(N'lab_remote') IS NULL EXEC(N'CREATE DATABASE lab_remote')")
            exec_sql(root,"ALTER DATABASE lab_remote SET ALLOW_SNAPSHOT_ISOLATION ON")
            exec_sql(root,"IF NOT EXISTS(SELECT 1 FROM sys.sql_logins WHERE name='remote_reader') "
                "CREATE LOGIN remote_reader WITH PASSWORD='lab-reader', CHECK_POLICY=OFF")
        with admin('lab_remote') as root:
            exec_sql(root,"IF OBJECT_ID(N'dbo.contracts',N'U') IS NULL "
                "CREATE TABLE dbo.contracts (id BIGINT PRIMARY KEY, "
                "contract_code NVARCHAR(100) NOT NULL, amount DECIMAL(14,2) NOT NULL, "
                "status NVARCHAR(30) NOT NULL, description NVARCHAR(MAX) NULL)")
            exec_sql(root,"IF NOT EXISTS(SELECT 1 FROM sys.database_principals "
                "WHERE name='remote_reader') CREATE USER remote_reader FOR LOGIN remote_reader")
            exec_sql(root,'GRANT SELECT ON dbo.contracts TO remote_reader')
            exec_sql(root,"IF NOT EXISTS(SELECT 1 FROM dbo.contracts) "
                "INSERT INTO dbo.contracts(id,contract_code,amount,status,description) "
                "SELECT TOP (100) ROW_NUMBER() OVER (ORDER BY (SELECT NULL)), "
                "CONCAT('CT-',ROW_NUMBER() OVER (ORDER BY (SELECT NULL))), "
                "CONVERT(DECIMAL(14,2),100.00),'active',N'synthetic lab fixture' "
                "FROM sys.all_objects")
        print('SQL Server lab fixture initialized, SELECT-only reader created')
        return
    with admin('lab_remote') as root:
        if action=='add-column':
            exec_sql(root,"IF COL_LENGTH('dbo.contracts','currency_code') IS NULL "
                          'ALTER TABLE dbo.contracts ADD currency_code NVARCHAR(3) NULL')
        elif action=='change-length':
            exec_sql(root,'ALTER TABLE dbo.contracts ALTER COLUMN status NVARCHAR(100) NOT NULL')
        elif action=='drop-column':
            exec_sql(root,"IF COL_LENGTH('dbo.contracts','currency_code') IS NOT NULL "
                          'ALTER TABLE dbo.contracts DROP COLUMN currency_code')
        elif action=='mutate-insert':
            exec_sql(root,"IF NOT EXISTS(SELECT 1 FROM dbo.contracts WHERE id=101) "
                          "INSERT INTO dbo.contracts(id,contract_code,amount,status,description) "
                          "VALUES (101,N'CT-101',155.45,N'active',N'Unicode: ação')")
        elif action=='mutate-update':
            exec_sql(root,"UPDATE dbo.contracts SET amount=amount+1, "
                          "status=N'updated',description=N'alteração incremental' WHERE id=1")
        elif action=='mutate-delete':
            exec_sql(root,"DELETE FROM dbo.contracts WHERE id=2")
        else:
            raise ValueError('Unknown fixture action')
    print(f'SQL Server fixture DDL applied: {action}')


if __name__=='__main__':
    run(sys.argv[1] if len(sys.argv)>1 else 'init')
