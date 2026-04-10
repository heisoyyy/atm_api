"""
database.py — MySQL connection pool + helpers
Fix V6.4:
  - remove_cashplan_only: set status_done='REMOVED' agar bisa dibedakan
    dari BATAL (status_done='BATAL') di frontend
  - log_upload: kolom 'rows' → 'total_rows' (sesuai schema v6.3)
  - update_cashplan_status: bulan pakai nama Indonesia
  - add_to_cashplan: normalisasi id_atm uppercase
"""

from contextlib import contextmanager
from datetime import datetime
import math
import mysql.connector
import os
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
    if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
        return None
    return v


# ── Bulan Indonesia ────────────────────────────────────────
_BULAN_MAP = {
    "January": "Januari", "February": "Februari", "March": "Maret",
    "April": "April", "May": "Mei", "June": "Juni",
    "July": "Juli", "August": "Agustus", "September": "September",
    "October": "Oktober", "November": "November", "December": "Desember",
}

def _bulan_id(dt: datetime) -> str:
    return _BULAN_MAP.get(dt.strftime("%B"), dt.strftime("%B"))


# ══════════════════════════════════════════════════════════
#  PREDICTIONS
# ══════════════════════════════════════════════════════════

def upsert_predictions(predictions: list):
    if not predictions:
        return

    sql = """
    INSERT INTO predictions (
        id_atm, tipe, lokasi, wilayah,
        saldo, `limit`, pct_saldo, tarik_per_jam,
        cashout_harian, cashout_mingguan, cashout_bulanan,
        pred_saldo_6j, pred_saldo_12j, pred_saldo_24j, pred_saldo_48j, pred_saldo_72j,
        est_jam, est_hari,
        tgl_awas, jam_awas, tgl_habis, jam_habis, tgl_isi, jam_isi,
        rekomendasi_isi, status, skor_urgensi, ranking,
        atm_sepi, metode, last_update, generated_at
    ) VALUES (
        %(id_atm)s, %(tipe)s, %(lokasi)s, %(wilayah)s,
        %(saldo)s, %(limit)s, %(pct_saldo)s, %(tarik_per_jam)s,
        %(cashout_harian)s, %(cashout_mingguan)s, %(cashout_bulanan)s,
        %(pred_saldo_6j)s, %(pred_saldo_12j)s, %(pred_saldo_24j)s,
        %(pred_saldo_48j)s, %(pred_saldo_72j)s,
        %(est_jam)s, %(est_hari)s,
        %(tgl_awas)s, %(jam_awas)s, %(tgl_habis)s, %(jam_habis)s,
        %(tgl_isi)s, %(jam_isi)s,
        %(rekomendasi_isi)s, %(status)s, %(skor_urgensi)s, %(ranking)s,
        %(atm_sepi)s, %(metode)s, %(last_update)s, %(generated_at)s
    )
    ON DUPLICATE KEY UPDATE
        tipe=VALUES(tipe), lokasi=VALUES(lokasi), wilayah=VALUES(wilayah),
        saldo=VALUES(saldo), `limit`=VALUES(`limit`),
        pct_saldo=VALUES(pct_saldo), tarik_per_jam=VALUES(tarik_per_jam),
        cashout_harian=VALUES(cashout_harian),
        cashout_mingguan=VALUES(cashout_mingguan),
        cashout_bulanan=VALUES(cashout_bulanan),
        pred_saldo_6j=VALUES(pred_saldo_6j), pred_saldo_12j=VALUES(pred_saldo_12j),
        pred_saldo_24j=VALUES(pred_saldo_24j), pred_saldo_48j=VALUES(pred_saldo_48j),
        pred_saldo_72j=VALUES(pred_saldo_72j),
        est_jam=VALUES(est_jam), est_hari=VALUES(est_hari),
        tgl_awas=VALUES(tgl_awas), jam_awas=VALUES(jam_awas),
        tgl_habis=VALUES(tgl_habis), jam_habis=VALUES(jam_habis),
        tgl_isi=VALUES(tgl_isi), jam_isi=VALUES(jam_isi),
        rekomendasi_isi=VALUES(rekomendasi_isi),
        status=VALUES(status), skor_urgensi=VALUES(skor_urgensi),
        ranking=VALUES(ranking), atm_sepi=VALUES(atm_sepi),
        metode=VALUES(metode), last_update=VALUES(last_update),
        generated_at=VALUES(generated_at)
    """

    now = datetime.now().isoformat()
    rows = []
    for p in predictions:
        rows.append({
            "id_atm":           p.get("id_atm"),
            "tipe":             p.get("tipe", "-"),
            "lokasi":           p.get("lokasi", "-"),
            "wilayah":          p.get("wilayah", "-"),
            "saldo":            _s(p.get("saldo", 0)),
            "limit":            _s(p.get("limit", 0)),
            "pct_saldo":        _s(p.get("pct_saldo", 0)),
            "tarik_per_jam":    _s(p.get("tarik_per_jam", 0)),
            "cashout_harian":   _s(p.get("cashout_harian", 0)),
            "cashout_mingguan": _s(p.get("cashout_mingguan", 0)),
            "cashout_bulanan":  _s(p.get("cashout_bulanan", 0)),
            "pred_saldo_6j":    _s(p.get("pred_saldo_6j", 0)),
            "pred_saldo_12j":   _s(p.get("pred_saldo_12j", 0)),
            "pred_saldo_24j":   _s(p.get("pred_saldo_24j", 0)),
            "pred_saldo_48j":   _s(p.get("pred_saldo_48j", 0)),
            "pred_saldo_72j":   _s(p.get("pred_saldo_72j", 0)),
            "est_jam":          _s(p.get("est_jam")),
            "est_hari":         _s(p.get("est_hari")),
            "tgl_awas":         p.get("tgl_awas"),
            "jam_awas":         p.get("jam_awas"),
            "tgl_habis":        p.get("tgl_habis"),
            "jam_habis":        p.get("jam_habis"),
            "tgl_isi":          p.get("tgl_isi"),
            "jam_isi":          p.get("jam_isi"),
            "rekomendasi_isi":  p.get("rekomendasi_isi"),
            "status":           p.get("status", "NO DATA"),
            "skor_urgensi":     _s(p.get("skor_urgensi", 0)),
            "ranking":          p.get("ranking", 0),
            "atm_sepi":         int(bool(p.get("atm_sepi", False))),
            "metode":           p.get("metode", "NO DATA"),
            "last_update":      p.get("last_update"),
            "generated_at":     now,
        })

    with get_conn() as conn:
        cur = conn.cursor()
        cur.executemany(sql, rows)


def get_predictions_from_db(
    wilayah: str = None,
    status: str = None,
    tipe: str = None,
    limit: int = 100,
    offset: int = 0,
) -> dict:
    where, params = [], []
    if wilayah:
        where.append("wilayah LIKE %s"); params.append(f"%{wilayah}%")
    if status:
        where.append("LOWER(status) = %s"); params.append(status.lower())
    if tipe:
        where.append("UPPER(tipe) = %s"); params.append(tipe.upper())

    where_sql = ("WHERE " + " AND ".join(where)) if where else ""

    with get_conn() as conn:
        cur = conn.cursor(dictionary=True)
        cur.execute(f"SELECT COUNT(*) AS cnt FROM predictions {where_sql}", params)
        total = cur.fetchone()["cnt"]
        cur.execute(
            f"SELECT * FROM predictions {where_sql} ORDER BY skor_urgensi DESC LIMIT %s OFFSET %s",
            params + [limit, offset],
        )
        rows = cur.fetchall()

    gen_at = rows[0]["generated_at"].isoformat() if rows else None
    for r in rows:
        r["generated_at"] = r["generated_at"].isoformat() if r.get("generated_at") else None
        r["last_update"]  = str(r["last_update"])  if r.get("last_update")  else None
        r["tgl_awas"]     = str(r["tgl_awas"])      if r.get("tgl_awas")     else None
        r["tgl_habis"]    = str(r["tgl_habis"])     if r.get("tgl_habis")    else None
        r["tgl_isi"]      = str(r["tgl_isi"])       if r.get("tgl_isi")      else None
        r["atm_sepi"]     = bool(r.get("atm_sepi", 0))
        r["saldo"]        = int(r["saldo"])  if r.get("saldo")  is not None else 0
        r["limit"]        = int(r["limit"])  if r.get("limit")  is not None else 0
    return {"total": total, "data": rows, "generated_at": gen_at}


# ══════════════════════════════════════════════════════════
#  ATM HISTORY
# ══════════════════════════════════════════════════════════

def bulk_insert_history(df_history):
    import pandas as pd

    sql = """
    INSERT IGNORE INTO atm_history
        (id_atm, recorded_at, saldo, `limit`, penarikan, pct_saldo,
         is_refill, is_interpolated, status)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    cols = {
        "ID ATM": "id_atm", "datetime": "recorded_at",
        "Sisa Saldo": "saldo", "Limit": "limit",
        "Penarikan": "penarikan", "Persentase": "pct_saldo",
        "Is Refill": "is_refill", "Is_Interpolated": "is_interpolated",
        "Status": "status",
    }
    available = [c for c in cols if c in df_history.columns]
    df_sub = df_history[available].copy()

    batch = []
    for _, row in df_sub.iterrows():
        try:
            recorded_at = pd.to_datetime(row.get("datetime", None))
        except Exception:
            continue
        batch.append((
            str(row.get("ID ATM", "")).strip().upper(),
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


def get_atm_history_from_db(atm_id: str, last_n_days: int = 7) -> dict:
    sql = """
    SELECT recorded_at AS datetime, saldo, `limit`, penarikan,
           pct_saldo AS pct, is_refill, is_interpolated, status
    FROM atm_history
    WHERE id_atm=%s AND recorded_at >= NOW() - INTERVAL %s DAY
    ORDER BY recorded_at ASC
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
        "data":         rows,
    }


# ══════════════════════════════════════════════════════════
#  CASHPLAN
# ══════════════════════════════════════════════════════════

def add_to_cashplan(atm_data: dict) -> int:
    id_atm = str(atm_data.get("id_atm", "")).strip().upper()
    if not id_atm:
        raise ValueError("id_atm tidak boleh kosong")

    # Cek duplikasi PENDING
    with get_conn() as conn:
        cur = conn.cursor(dictionary=True)
        cur.execute(
            "SELECT id FROM cashplan WHERE id_atm=%s AND status_cashplan='PENDING'",
            (id_atm,)
        )
        existing = cur.fetchone()
        if existing:
            return existing["id"]

    jumlah = max(0, int(atm_data.get("limit", 0)) - int(atm_data.get("saldo", 0)))

    sql = """
    INSERT INTO cashplan
        (id_atm, lokasi, wilayah, tipe, saldo, `limit`, pct_saldo,
         status_awal, jumlah_isi, denom, tgl_isi, jam_isi, est_jam,
         skor_urgensi, added_by)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute(sql, (
            id_atm,
            atm_data.get("lokasi", "-"),
            atm_data.get("wilayah", "-"),
            atm_data.get("tipe", "-"),
            int(atm_data.get("saldo", 0)),
            int(atm_data.get("limit", 0)),
            float(atm_data.get("pct_saldo", 0)),
            atm_data.get("status", "AWAS"),
            jumlah,
            int(atm_data.get("denom", 100000)),
            atm_data.get("tgl_isi"),
            atm_data.get("jam_isi"),
            float(atm_data.get("est_jam", 0) or 0),
            float(atm_data.get("skor_urgensi", 0) or 0),
            atm_data.get("added_by", "system"),
        ))
        return cur.lastrowid


def get_cashplan_list(status: str = "PENDING") -> list:
    with get_conn() as conn:
        cur = conn.cursor(dictionary=True)
        cur.execute(
            "SELECT * FROM cashplan WHERE status_cashplan=%s ORDER BY skor_urgensi DESC",
            (status,)
        )
        rows = cur.fetchall()

    for r in rows:
        for f in ["added_at", "done_at", "removed_at"]:
            if r.get(f):
                r[f] = r[f].isoformat()
        if r.get("tgl_isi"):
            r["tgl_isi"] = str(r["tgl_isi"])
        r["saldo"] = int(r["saldo"]) if r.get("saldo") is not None else 0
        r["limit"] = int(r["limit"]) if r.get("limit") is not None else 0
    return rows


def update_cashplan_status(
    cashplan_id: int,
    new_status: str,
    keterangan: str = None,
    denom: int = None,
) -> dict:
    now = datetime.now()

    with get_conn() as conn:
        cur = conn.cursor(dictionary=True)
        cur.execute("SELECT * FROM cashplan WHERE id=%s", (cashplan_id,))
        item = cur.fetchone()

    if not item:
        raise ValueError(f"Cashplan id {cashplan_id} tidak ditemukan")

    # Label display: DONE → SELESAI, REMOVED (via tombol Batal) → BATAL
    status_done_label = "SELESAI" if new_status == "DONE" else "BATAL"

    updates = {
        "status_cashplan": new_status,
        "status_done":     status_done_label,
    }
    if keterangan is not None:
        updates["keterangan"] = keterangan
    if denom is not None:
        updates["denom"] = denom

    jumlah_isi = int(item.get("jumlah_isi", 0))
    denom_val  = denom or int(item.get("denom", 100000))
    lembar     = math.ceil(jumlah_isi / denom_val) if denom_val > 0 else 0
    bulan_str  = _bulan_id(now)

    if new_status == "DONE":
        updates["done_at"] = now
    elif new_status == "REMOVED":
        updates["removed_at"] = now

    # Kedua status (DONE dan REMOVED via Batal) masuk ke rekap_replacement
    rekap_sql = """
    INSERT INTO rekap_replacement
        (cashplan_id, id_atm, lokasi, wilayah, tipe,
         saldo_awal, `limit`, jumlah_isi, denom, lembar,
         keterangan, status_awal, status_done, tgl_isi, jam_isi, done_at,
         bulan, tahun)
    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute(rekap_sql, (
            cashplan_id,
            item["id_atm"],
            item.get("lokasi", "-"),
            item.get("wilayah", "-"),
            item.get("tipe", "-"),
            int(item["saldo"]),
            int(item["limit"]),
            jumlah_isi,
            denom_val,
            lembar,
            keterangan or item.get("keterangan"),
            item.get("status_awal", "AWAS"),
            status_done_label,
            item.get("tgl_isi"),
            item.get("jam_isi"),
            now,
            bulan_str,
            now.year,
        ))

    # Update cashplan
    set_parts = ", ".join(f"{k}=%s" for k in updates)
    vals = list(updates.values()) + [cashplan_id]
    with get_conn() as conn:
        conn.cursor().execute(
            f"UPDATE cashplan SET {set_parts} WHERE id=%s", vals
        )

    return {"cashplan_id": cashplan_id, "new_status": new_status, "status_done": status_done_label}


def remove_cashplan_only(cashplan_id: int):
    """
    Hapus item dari antrian cashplan via tombol ✕ Remove.
    - status_cashplan = 'REMOVED'
    - status_done     = 'REMOVED'   ← marker khusus, berbeda dari 'BATAL'
    - TIDAK insert ke rekap_replacement
    
    Perbedaan dengan update_cashplan_status(REMOVED):
      update_cashplan_status → dipanggil tombol Batal → status_done='BATAL' → masuk rekap
      remove_cashplan_only   → dipanggil tombol Remove → status_done='REMOVED' → tidak masuk rekap
    """
    with get_conn() as conn:
        cur = conn.cursor(dictionary=True)
        cur.execute("SELECT id FROM cashplan WHERE id=%s", (cashplan_id,))
        item = cur.fetchone()

    if not item:
        raise ValueError(f"Cashplan id {cashplan_id} tidak ditemukan")

    with get_conn() as conn:
        conn.cursor().execute(
            """UPDATE cashplan
               SET status_cashplan='REMOVED',
                   status_done='REMOVED',
                   removed_at=%s
               WHERE id=%s""",
            (datetime.now(), cashplan_id)
        )


# ══════════════════════════════════════════════════════════
#  REKAP REPLACEMENT
# ══════════════════════════════════════════════════════════

def update_rekap_replacement(
    rekap_id: int,
    tgl_isi: str = None,
    jam_cash_in: str = None,
    jam_cash_out: str = None,
    denom: int = None,
) -> dict:
    updates = {"is_saved": 1}
    if tgl_isi      is not None: updates["tgl_isi"]      = tgl_isi
    if jam_cash_in  is not None: updates["jam_cash_in"]  = jam_cash_in
    if jam_cash_out is not None: updates["jam_cash_out"] = jam_cash_out
    if denom        is not None: updates["denom"]        = denom

    set_parts = ", ".join(f"{k}=%s" for k in updates)
    vals      = list(updates.values()) + [rekap_id]

    with get_conn() as conn:
        conn.cursor().execute(
            f"UPDATE rekap_replacement SET {set_parts} WHERE id=%s", vals
        )
    return {"rekap_id": rekap_id, "saved": True}


def get_rekap_replacement(
    bulan: str = None,
    tahun: int = None,
    wilayah: str = None,
) -> list:
    where, params = [], []
    if bulan:
        where.append("bulan=%s"); params.append(bulan)
    if tahun:
        where.append("tahun=%s"); params.append(tahun)
    if wilayah and wilayah != "Semua":
        where.append("wilayah=%s"); params.append(wilayah)

    where_sql = ("WHERE " + " AND ".join(where)) if where else ""

    with get_conn() as conn:
        cur = conn.cursor(dictionary=True)
        cur.execute(
            f"SELECT * FROM rekap_replacement {where_sql} ORDER BY done_at DESC",
            params
        )
        rows = cur.fetchall()

    for r in rows:
        if r.get("done_at"): r["done_at"] = r["done_at"].isoformat()
        if r.get("tgl_isi"): r["tgl_isi"] = str(r["tgl_isi"])
        r["saldo_awal"] = int(r["saldo_awal"]) if r.get("saldo_awal") is not None else 0
        r["limit"]      = int(r["limit"])       if r.get("limit")      is not None else 0
        r["jumlah_isi"] = int(r["jumlah_isi"])  if r.get("jumlah_isi") is not None else 0
        r["is_saved"]   = bool(r.get("is_saved", 0))
    return rows


# ══════════════════════════════════════════════════════════
#  UPLOAD LOG
# ══════════════════════════════════════════════════════════

def log_upload(
    filename: str,
    format_: str,
    rows: int,
    atm_count: int,
    predictions: int,
    retrain: bool,
    notes: str = None,
):
    sql = """
    INSERT INTO upload_log
        (filename, format, total_rows, atm_count, predictions, retrain, notes)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    """
    with get_conn() as conn:
        conn.cursor().execute(
            sql,
            (filename, format_, rows, atm_count, predictions, int(retrain), notes)
        )