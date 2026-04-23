"""
db/upload_log.py
CRUD untuk tabel upload_log (riwayat upload file monitoring).

Fungsi publik:
    log_upload(filename, format_, rows, …)
    get_upload_log(limit)
    get_upload_log_today()
"""

import logging
from typing import Optional

from db import get_conn

logger = logging.getLogger("db.upload_log")


def log_upload(
    filename:    str,
    format_:     str,
    rows:        int,
    atm_count:   int,
    matched:     int,
    skipped:     int,
    predictions: int,
    retrain:     bool,
    notes:       Optional[str] = None,
):
    """Catat satu event upload ke tabel upload_log."""
    sql = """
        INSERT INTO upload_log
            (filename, format, total_rows, atm_count, matched, skipped,
             predictions, retrain, notes)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    with get_conn() as conn:
        conn.cursor().execute(
            sql,
            (filename, format_, rows, atm_count, matched, skipped,
             predictions, int(retrain), notes),
        )


def _fmt_log(rows: list) -> list:
    for r in rows:
        if r.get("uploaded_at"):
            r["uploaded_at"] = r["uploaded_at"].isoformat()
        r["retrain"] = bool(r.get("retrain", 0))
    return rows


def get_upload_log(limit: int = 50) -> list:
    """Ambil riwayat upload terbaru."""
    with get_conn() as conn:
        cur = conn.cursor(dictionary=True)
        cur.execute(
            """SELECT id, filename, format, total_rows, atm_count,
                      predictions, retrain, uploaded_at, status, notes
               FROM upload_log
               ORDER BY uploaded_at DESC LIMIT %s""",
            (limit,),
        )
        rows = cur.fetchall()
    return _fmt_log(rows)


def get_upload_log_today() -> list:
    """Ambil upload log hari ini."""
    with get_conn() as conn:
        cur = conn.cursor(dictionary=True)
        cur.execute(
            """SELECT id, filename, format, total_rows, atm_count,
                      predictions, retrain, uploaded_at, status, notes
               FROM upload_log
               WHERE DATE(uploaded_at)=CURDATE()
               ORDER BY uploaded_at DESC"""
        )
        rows = cur.fetchall()
    return _fmt_log(rows)