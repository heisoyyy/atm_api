"""
db/rekap.py
CRUD untuk tabel rekap_replacement (arsip historis pengisian ATM).

Fungsi publik:
    update_rekap(rekap_id, …)                → dict
    get_rekap(bulan, tahun, wilayah)          → list
    get_rekap_for_download(wilayah, bulan, tahun) → list
"""

import logging
import math
from typing import Optional

from db import get_conn

logger = logging.getLogger("db.rekap")


def update_rekap(
    rekap_id:    int,
    tgl_isi:     Optional[str] = None,
    jam_cash_in: Optional[str] = None,
    jam_cash_out:Optional[str] = None,
    denom:       Optional[int] = None,
) -> dict:
    """Simpan / edit detail rekap setelah pengisian selesai."""
    updates = {"is_saved": 1}
    if tgl_isi      is not None: updates["tgl_isi"]      = tgl_isi
    if jam_cash_in  is not None: updates["jam_cash_in"]  = jam_cash_in
    if jam_cash_out is not None: updates["jam_cash_out"] = jam_cash_out
    if denom        is not None:
        updates["denom"] = denom
        with get_conn() as conn:
            cur = conn.cursor(dictionary=True)
            cur.execute("SELECT jumlah_isi FROM rekap_replacement WHERE id=%s", (rekap_id,))
            row = cur.fetchone()
        if row:
            jumlah_isi = int(row["jumlah_isi"] or 0)
            updates["lembar"] = math.ceil(jumlah_isi / denom) if denom > 0 else 0

    set_parts = ", ".join(f"{k}=%s" for k in updates)
    vals      = list(updates.values()) + [rekap_id]

    with get_conn() as conn:
        conn.cursor().execute(
            f"UPDATE rekap_replacement SET {set_parts} WHERE id=%s", vals
        )
    return {"rekap_id": rekap_id, "saved": True}


def _fmt_rekap(rows: list) -> list:
    for r in rows:
        if r.get("done_at"): r["done_at"] = r["done_at"].isoformat()
        if r.get("tgl_isi"): r["tgl_isi"] = str(r["tgl_isi"])
        r["saldo_awal"] = int(r["saldo_awal"]) if r.get("saldo_awal") is not None else 0
        r["limit"]      = int(r["limit"])       if r.get("limit")      is not None else 0
        r["jumlah_isi"] = int(r["jumlah_isi"])  if r.get("jumlah_isi") is not None else 0
        r["is_saved"]   = bool(r.get("is_saved", 0))
    return rows


def get_rekap(
    bulan:   Optional[str] = None,
    tahun:   Optional[int] = None,
    wilayah: Optional[str] = None,
) -> list:
    """Ambil daftar rekap dengan filter bulan/tahun/wilayah."""
    where, params = [], []
    if bulan:   where.append("bulan=%s");   params.append(bulan)
    if tahun:   where.append("tahun=%s");   params.append(tahun)
    if wilayah and wilayah.lower() != "semua":
        where.append("wilayah=%s"); params.append(wilayah)

    where_sql = ("WHERE " + " AND ".join(where)) if where else ""
    with get_conn() as conn:
        cur = conn.cursor(dictionary=True)
        cur.execute(
            f"SELECT * FROM rekap_replacement {where_sql} ORDER BY done_at DESC",
            params,
        )
        rows = cur.fetchall()
    return _fmt_rekap(rows)


def get_rekap_for_download(
    wilayah: Optional[str] = None,
    bulan:   Optional[str] = None,
    tahun:   Optional[int] = None,
) -> list:
    """Ambil data rekap untuk ekspor Excel/CSV."""
    where, params = [], []
    if wilayah and wilayah.lower() != "semua":
        where.append("wilayah=%s"); params.append(wilayah)
    if bulan: where.append("bulan=%s");  params.append(bulan)
    if tahun: where.append("tahun=%s");  params.append(tahun)

    where_sql = ("WHERE " + " AND ".join(where)) if where else ""
    with get_conn() as conn:
        cur = conn.cursor(dictionary=True)
        cur.execute(
            f"""SELECT
                id_atm, lokasi, wilayah, tipe,
                denom_options, saldo_awal, `limit`,
                jumlah_isi, denom, lembar,
                status_awal, status_done, keterangan,
                tgl_isi, jam_isi, jam_cash_in, jam_cash_out,
                done_at, bulan, tahun
            FROM rekap_replacement {where_sql}
            ORDER BY done_at DESC""",
            params,
        )
        rows = cur.fetchall()

    return _fmt_rekap(rows)