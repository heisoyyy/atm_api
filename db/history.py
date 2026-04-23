"""
db/history.py
CRUD untuk tabel atm_history (timeseries saldo per jam).

Fungsi publik:
    bulk_insert_history(df_history)
    get_atm_history(atm_id, last_n_days)  → dict | None
"""

import logging
from typing import Optional

import pandas as pd

from db import get_conn

logger = logging.getLogger("db.history")


def bulk_insert_history(df_history: pd.DataFrame):
    """Insert batch riwayat saldo. Pakai INSERT IGNORE untuk skip duplikat."""
    sql = """
        INSERT IGNORE INTO atm_history
            (id_atm, recorded_at, saldo, `limit`, penarikan, pct_saldo,
             is_refill, is_interpolated, status)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    batch = []
    for _, row in df_history.iterrows():
        try:
            recorded_at = pd.to_datetime(row.get("datetime", None))
        except Exception:
            continue

        id_atm = str(row.get("ID ATM", "")).strip().upper()
        if not id_atm or id_atm in ("", "NAN", "NONE", "NULL"):
            continue

        batch.append((
            id_atm,
            recorded_at,
            int(float(row.get("Sisa Saldo", 0) or 0)),
            int(float(row.get("Limit", 0) or 0)),
            int(float(row.get("Penarikan", 0) or 0)),
            float(row.get("Persentase", 0) or 0),
            int(row.get("Is Refill", 0) or 0),
            int(row.get("Is_Interpolated", 0) or 0),
            str(row.get("Status", "NO DATA")),
        ))

        if len(batch) >= 5000:
            with get_conn() as conn:
                conn.cursor().executemany(sql, batch)
            batch = []

    if batch:
        with get_conn() as conn:
            conn.cursor().executemany(sql, batch)

    logger.info("bulk_insert_history: %d baris diproses", len(df_history))


def get_atm_history(atm_id: str, last_n_days: int = 7) -> Optional[dict]:
    """
    Ambil riwayat saldo ATM N hari terakhir.
    Return None jika ATM tidak ditemukan.
    """
    sql = """
        SELECT
            h.recorded_at AS datetime,
            h.saldo,
            h.`limit`,
            h.penarikan,
            h.pct_saldo       AS pct,
            h.is_refill,
            h.is_interpolated,
            h.status,
            m.lokasi_atm      AS lokasi,
            m.wilayah,
            m.denom_options,
            m.merk_atm,
            UPPER(LEFT(h.id_atm, 3)) AS tipe
        FROM atm_history h
        INNER JOIN atm_masters m ON h.id_atm = m.id_atm
        WHERE h.id_atm = %s
          AND h.recorded_at >= NOW() - INTERVAL %s DAY
        ORDER BY h.recorded_at ASC
    """
    with get_conn() as conn:
        cur = conn.cursor(dictionary=True)
        cur.execute(sql, (atm_id.upper(), last_n_days))
        rows = cur.fetchall()

    if not rows:
        return None

    for r in rows:
        r["datetime"] = r["datetime"].isoformat() if r.get("datetime") else None

    saldos = [r["saldo"] for r in rows if r.get("saldo") is not None]

    return {
        "id_atm":       atm_id,
        "last_n_days":  last_n_days,
        "total_rows":   len(rows),
        "refill_count": sum(1 for r in rows if r.get("is_refill")),
        "saldo_min":    min(saldos) if saldos else 0,
        "saldo_max":    max(saldos) if saldos else 0,
        "saldo_latest": rows[-1]["saldo"] if rows else 0,
        "limit":        rows[-1]["limit"] if rows else 0,
        "lokasi":       rows[-1].get("lokasi") if rows else "-",
        "wilayah":      rows[-1].get("wilayah") if rows else "-",
        "denom_options": rows[-1].get("denom_options") if rows else "-",
        "merk_atm":     rows[-1].get("merk_atm") if rows else "-",
        "data":         rows,
    }