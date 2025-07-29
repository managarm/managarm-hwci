import sqlite3
import contextlib


def connect_autocommit(*args, **kwargs):
    assert "autocommit" not in kwargs
    assert "isolation_level" not in kwargs

    if hasattr(sqlite3, "LEGACY_TRANSACTION_CONTROL"):
        # Use the more modern autocommit=True if it is available.
        # This will continue working even when Python changes the default
        # to autocommit=False in the future (as indicated by the docs).
        return sqlite3.connect(*args, **kwargs, autocommit=True)
    else:
        # Pre-Python 3.12, we cannot set autocommit as it does not exist.
        return sqlite3.connect(*args, **kwargs, isolation_level=None)


@contextlib.contextmanager
def transaction(conn):
    conn.execute("BEGIN")
    commit = False
    try:
        yield
        commit = True
    finally:
        if commit:
            conn.execute("COMMIT")
        else:
            conn.execute("ROLLBACK")
