# app/probes.py
import datetime as dt
import pyodbc

# Each probe returns (title: str, rows: list[tuple], columns: list[str])

def _fetch(cur, sql, params=()):
    cur.execute(sql, params)
    cols = [d[0] for d in cur.description] if cur.description else []
    rows = cur.fetchall() if cur.description else []
    return rows, cols

def probe_dup_key_sample(conn, max_rows=50):
    sql = """
    ;WITH x AS (
      SELECT TOP (?) ErrorID, ErrorNumber, ErrorMessage, OccurredAt
      FROM dbo.ErrorLog
      WHERE ErrorNumber IN (2601,2627)
      ORDER BY ErrorID DESC
    )
    SELECT TOP (?) t.Ref, t.TransactionID, t.CreatedAt
    FROM dbo.Transactions t
    JOIN x ON 1=1
    WHERE t.Ref IS NOT NULL
    GROUP BY t.Ref, t.TransactionID, t.CreatedAt
    HAVING COUNT(*) > 1
    ORDER BY t.CreatedAt DESC;
    """
    with conn.cursor() as cur:
        rows, cols = _fetch(cur, sql, (max_rows, max_rows))
    return ("Duplicate key sample (Transactions by Ref)", rows, cols)

def probe_index_info(conn, max_rows=50):
    sql = """
    SELECT TOP (?) i.name AS index_name, i.is_unique, i.is_unique_constraint,
           c.name AS column_name, ic.key_ordinal
    FROM sys.indexes i
    JOIN sys.index_columns ic ON ic.object_id=i.object_id AND ic.index_id=i.index_id
    JOIN sys.columns c ON c.object_id=ic.object_id AND c.column_id=ic.column_id
    WHERE i.object_id = OBJECT_ID('dbo.Transactions')
    ORDER BY i.is_unique DESC, ic.key_ordinal;
    """
    with conn.cursor() as cur:
        rows, cols = _fetch(cur, sql, (max_rows,))
    return ("Index info (dbo.Transactions)", rows, cols)

def probe_recent_deadlocks(conn, max_rows=10):
    # xp_readerrorlog deadlock lines (works on many editions)
    sql = "EXEC xp_readerrorlog 0, 1, N'deadlock', NULL;"
    with conn.cursor() as cur:
        try:
            rows, cols = _fetch(cur, sql)
        except pyodbc.Error:
            rows, cols = [], []
    return ("Recent deadlocks (error log)", rows[:max_rows], cols)

def probe_top_blocking(conn, max_rows=20):
    sql = """
    SELECT TOP (?) r.session_id, r.status, r.command, r.wait_type, r.blocking_session_id,
           r.cpu_time, r.total_elapsed_time, DB_NAME(r.database_id) AS db, t.text
    FROM sys.dm_exec_requests r
    CROSS APPLY sys.dm_exec_sql_text(r.sql_handle) t
    WHERE r.blocking_session_id <> 0
    ORDER BY r.total_elapsed_time DESC;
    """
    with conn.cursor() as cur:
        rows, cols = _fetch(cur, sql, (max_rows,))
    return ("Top blocking requests", rows, cols)

def probe_hot_objects(conn, max_rows=20):
    sql = """
    SELECT TOP (?) OBJECT_NAME(s.[object_id]) AS object_name, s.index_id,
           user_seeks+user_scans+user_lookups AS reads, user_updates
    FROM sys.dm_db_index_usage_stats s
    WHERE database_id = DB_ID()
    ORDER BY (user_seeks+user_scans+user_lookups) DESC;
    """
    with conn.cursor() as cur:
        rows, cols = _fetch(cur, sql, (max_rows,))
    return ("Hot objects (index usage)", rows, cols)

def probe_failed_logins(conn, max_rows=50):
    sql = "EXEC xp_readerrorlog 0, 1, N'Login failed', NULL;"
    with conn.cursor() as cur:
        try:
            rows, cols = _fetch(cur, sql)
        except pyodbc.Error:
            rows, cols = [], []
    return ("Recent 'Login failed' entries", rows[:max_rows], cols)

# registry
REGISTRY = {
    "dup_key_sample": probe_dup_key_sample,
    "index_info": probe_index_info,
    "recent_deadlocks": probe_recent_deadlocks,
    "top_blocking": probe_top_blocking,
    "hot_objects": probe_hot_objects,
    "failed_logins": probe_failed_logins,
}
