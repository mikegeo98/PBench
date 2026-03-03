"""Shared Databend execution helpers for metrics/replay code paths."""

from urllib.parse import urlencode

from databend_driver import BlockingDatabendClient


def split_sql_statements(query):
    """Split a SQL text into non-empty statements with trailing semicolons."""
    return [part.strip() + ";" for part in query.split(";") if part.strip()]


def apply_explain_mode(sql, explain_mode=None):
    """Prefix SQL with EXPLAIN mode unless already present."""
    if not explain_mode:
        return sql
    if sql.lstrip().upper().startswith(explain_mode.upper()):
        return sql
    return f"{explain_mode} {sql}"


def build_databend_dsn(host, port, database, settings=None, secure=False):
    """Build a Databend DSN from connection/session settings."""
    params = {}
    if not secure:
        params["sslmode"] = "disable"
    if settings:
        params.update({k: str(v) for k, v in settings.items() if v is not None})
    query = urlencode(params)
    return f"databend://root:@{host}:{port}/{database}" + (f"?{query}" if query else "")


def execute_databend_sql(host, port, database, sql, settings=None, secure=False):
    """Execute a single SQL statement and return legacy-compatible tuple payload."""
    dsn = build_databend_dsn(
        host=host,
        port=port,
        database=database,
        settings=settings,
        secure=secure,
    )
    conn = BlockingDatabendClient(dsn).get_conn()
    try:
        rows = [tuple(row.values()) for row in conn.query_iter(sql)]
        # Keep both index 1 and 2 compatible with older call-sites.
        return ([], rows, rows)
    except Exception as query_error:
        # Statements without result sets should still execute successfully.
        try:
            conn.exec(sql)
            return ([], [], [])
        except Exception:
            raise query_error
    finally:
        conn.close()


def execute_databend_query(
    host,
    port,
    database,
    query,
    settings=None,
    secure=False,
    explain_mode=None,
):
    """Execute one or more SQL statements, optionally under EXPLAIN mode."""
    results = []
    for sql in split_sql_statements(query):
        sql = apply_explain_mode(sql, explain_mode=explain_mode)
        results.append(
            execute_databend_sql(
                host=host,
                port=port,
                database=database,
                sql=sql,
                settings=settings,
                secure=secure,
            )
        )
    return results
