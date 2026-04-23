"""
db/__init__.py
Koneksi pool MySQL — dipakai oleh semua modul database.
"""

import math
import os
from contextlib import contextmanager

import mysql.connector
from dotenv import load_dotenv
from mysql.connector import pooling

load_dotenv()

DB_CONFIG = {
    "host":     os.getenv("DB_HOST", "localhost"),
    "port":     int(os.getenv("DB_PORT", 3306)),
    "user":     os.getenv("DB_USER", "root"),
    "password": os.getenv("DB_PASSWORD", ""),
    "database": os.getenv("DB_NAME", "atm_monitoring"),
}

_pool = pooling.MySQLConnectionPool(
    pool_name="atm_pool",
    pool_size=10,
    pool_reset_session=True,
    **DB_CONFIG,
)


@contextmanager
def get_conn():
    """Context manager koneksi DB. Auto commit / rollback."""
    conn = _pool.get_connection()
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def _s(v):
    """Sanitize float NaN/Inf → None."""
    if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
        return None
    return v