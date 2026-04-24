"""
database.py — v8
Semua query predictions/cashplan/notif JOIN ke atm_masters.
Kolom tipe, denom_options, lokasi, wilayah, limit tidak lagi
disimpan di predictions/cashplan/notif — diambil dari atm_masters.

FIXED:
- add_to_cashplan: denom diambil dari atm_masters.denom_options
- _parse_denom_options: handle "100","50","100 & 50","50000"
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


_BULAN_MAP = {
    "January": "Januari", "February": "Februari", "March": "Maret",
    "April": "April", "May": "Mei", "June": "Juni",
    "July": "Juli", "August": "Agustus", "September": "September",
    "October": "Oktober", "November": "November", "December": "Desember",
}

def _bulan_id(dt: datetime) -> str:
    return _BULAN_MAP.get(dt.strftime("%B"), dt.strftime("%B"))


# ── Helper: ambil data master untuk satu ATM ─────────────────
def _get_master(conn, id_atm: str) -> dict:
    """Ambil satu baris atm_masters. Return {} jika tidak ada."""
    cur = conn.cursor(dictionary=True)
    cur.execute(
        "SELECT * FROM atm_masters WHERE id_atm = %s",
        (id_atm.upper(),)
    )
    row = cur.fetchone()
    return row or {}


# ── Helper: parse denom_options → nilai rupiah penuh ─────────
def _parse_denom_options(denom_options_str: str) -> int:
    """
    Parse kolom denom_options dari atm_masters ke nilai rupiah penuh.
    Ambil nilai TERBESAR (untuk default denom di cashplan).

    Format yang ada di data:
      "100"      → 100_000  (dalam ribuan)
      "50"       → 50_000   (dalam ribuan)
      "100 & 50" → 100_000  (ambil pecahan terbesar)
      "50000"    → 50_000   (sudah full rupiah ≥ 1000)
      "" / None  → 100_000  (default)
    """
    if not denom_options_str:
        return 100_000

    raw = str(denom_options_str).strip()

    # Handle "100 & 50" atau "50 & 100" — ambil yang terbesar
    if "&" in raw or "," in raw or "/" in raw:
        parts = [p.strip() for p in raw.replace("&", ",").replace("/", ",").split(",")]
        candidates = []
        for p in parts:
            try:
                candidates.append(int(float(p.replace(".", "").replace(",", ""))))
            except ValueError:
                continue
        if candidates:
            best = max(candidates)
            return best * 1_000 if best <= 1_000 else best
        return 100_000

    # Single value
    try:
        val = int(float(raw.replace(".", "").replace(",", "")))
    except ValueError:
        return 100_000

    return val * 1_000 if val <= 1_000 else val


def _build_denom_string(denom_options_str: str) -> str:
    """
    Build string denom untuk disimpan di cashplan (VARCHAR).
    Semua denom yang tersedia digabung dengan koma, sort descending.

    "50"       → "50000"
    "100"      → "100000"
    "100 & 50" → "100000,50000"
    "50000"    → "50000"
    "100000"   → "100000"
    "" / None  → "100000"
    """
    if not denom_options_str:
        return "100000"

    raw = str(denom_options_str).strip()

    # Split by &, koma, atau /
    if any(c in raw for c in ["&", ",", "/"]):
        parts = [p.strip() for p in raw.replace("&", ",").replace("/", ",").split(",")]
    else:
        parts = [raw]

    results = []
    for p in parts:
        try:
            val = int(float(p.replace(".", "").replace(",", "").strip()))
            full = val * 1_000 if val <= 1_000 else val
            if full > 0 and full not in results:
                results.append(full)
        except ValueError:
            continue

    if not results:
        return "100000"

    # Sort descending (100000 dulu, 50000 belakang)
    results.sort(reverse=True)
    return ",".join(str(v) for v in results)

def _build_denom_string(denom_options_str: str) -> str:
    """
    Build string denom untuk disimpan di cashplan.
    "50"       → "50000"
    "100"      → "100000"
    "100 & 50" → "100000,50000"
    "50000"    → "50000"
    """
    if not denom_options_str:
        return "100000"

    raw = str(denom_options_str).strip()

    # Split by koma, &, atau /
    if any(c in raw for c in ["&", ",", "/"]):
        parts = [p.strip() for p in raw.replace("&", ",").replace("/", ",").split(",")]
    else:
        parts = [raw]

    results = []
    for p in parts:
        try:
            val = int(float(p.replace(".", "").replace(",", "").strip()))
            full = val * 1_000 if val <= 1_000 else val
            if full not in results:
                results.append(full)
        except ValueError:
            continue

    if not results:
        return "100000"

    # Sort descending (100000 dulu)
    results.sort(reverse=True)
    return ",".join(str(v) for v in results)


# ══════════════════════════════════════════════════════════════
#  PREDICTIONS
# ══════════════════════════════════════════════════════════════

_PRED_JOIN_SELECT = """
    SELECT
        p.*,
        m.lokasi_atm      AS lokasi,
        m.wilayah,
        m.denom_options,
        m.`limit`,
        m.merk_atm,
        m.alamat_atm,
        m.nama_vendor,
        m.kode_cabang,
        UPPER(LEFT(p.id_atm, 3)) AS tipe
    FROM predictions p
    INNER JOIN atm_masters m ON p.id_atm = m.id_atm
"""


def _fmt_pred(r: dict) -> dict:
    for f in ["generated_at", "last_update", "tgl_awas", "tgl_habis", "tgl_isi"]:
        if r.get(f):
            r[f] = str(r[f])
    r["atm_sepi"] = bool(r.get("atm_sepi", 0))
    r["saldo"]    = int(r["saldo"])  if r.get("saldo")  is not None else 0
    r["limit"]    = int(r["limit"])  if r.get("limit")  is not None else 0
    return r


def upsert_predictions(predictions: list):
    if not predictions:
        return

    sql = """
        INSERT INTO predictions (
            id_atm,
            saldo, pct_saldo, tarik_per_jam,
            cashout_harian, cashout_mingguan, cashout_bulanan,
            pred_saldo_6j, pred_saldo_12j, pred_saldo_24j,
            pred_saldo_48j, pred_saldo_72j,
            est_jam, est_hari,
            tgl_awas, jam_awas, tgl_habis, jam_habis,
            tgl_isi, jam_isi, rekomendasi_isi,
            status, skor_urgensi, ranking,
            atm_sepi, metode, last_update, generated_at
        ) VALUES (
            %(id_atm)s,
            %(saldo)s, %(pct_saldo)s, %(tarik_per_jam)s,
            %(cashout_harian)s, %(cashout_mingguan)s, %(cashout_bulanan)s,
            %(pred_saldo_6j)s, %(pred_saldo_12j)s, %(pred_saldo_24j)s,
            %(pred_saldo_48j)s, %(pred_saldo_72j)s,
            %(est_jam)s, %(est_hari)s,
            %(tgl_awas)s, %(jam_awas)s, %(tgl_habis)s, %(jam_habis)s,
            %(tgl_isi)s, %(jam_isi)s, %(rekomendasi_isi)s,
            %(status)s, %(skor_urgensi)s, %(ranking)s,
            %(atm_sepi)s, %(metode)s, %(last_update)s, %(generated_at)s
        )
        ON DUPLICATE KEY UPDATE
            saldo            = VALUES(saldo),
            pct_saldo        = VALUES(pct_saldo),
            tarik_per_jam    = VALUES(tarik_per_jam),
            cashout_harian   = VALUES(cashout_harian),
            cashout_mingguan = VALUES(cashout_mingguan),
            cashout_bulanan  = VALUES(cashout_bulanan),
            pred_saldo_6j    = VALUES(pred_saldo_6j),
            pred_saldo_12j   = VALUES(pred_saldo_12j),
            pred_saldo_24j   = VALUES(pred_saldo_24j),
            pred_saldo_48j   = VALUES(pred_saldo_48j),
            pred_saldo_72j   = VALUES(pred_saldo_72j),
            est_jam          = VALUES(est_jam),
            est_hari         = VALUES(est_hari),
            tgl_awas         = VALUES(tgl_awas),
            jam_awas         = VALUES(jam_awas),
            tgl_habis        = VALUES(tgl_habis),
            jam_habis        = VALUES(jam_habis),
            tgl_isi          = VALUES(tgl_isi),
            jam_isi          = VALUES(jam_isi),
            rekomendasi_isi  = VALUES(rekomendasi_isi),
            status           = VALUES(status),
            skor_urgensi     = VALUES(skor_urgensi),
            ranking          = VALUES(ranking),
            atm_sepi         = VALUES(atm_sepi),
            metode           = VALUES(metode),
            last_update      = VALUES(last_update),
            generated_at     = VALUES(generated_at)
    """

    now = datetime.now().isoformat()
    rows = []
    for p in predictions:
        rows.append({
            "id_atm":           p.get("id_atm"),
            "saldo":            _s(p.get("saldo", 0)),
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
        conn.cursor().executemany(sql, rows)


def get_predictions_from_db(
    wilayah: str = None,
    status:  str = None,
    tipe:    str = None,
    limit:   int = 100,
    offset:  int = 0,
) -> dict:
    where, params = [], []
    if wilayah:
        where.append("m.wilayah LIKE %s")
        params.append(f"%{wilayah}%")
    if status:
        where.append("LOWER(p.status) = %s")
        params.append(status.lower())
    if tipe:
        where.append("UPPER(LEFT(p.id_atm, 3)) = %s")
        params.append(tipe.upper())

    where_sql = ("WHERE " + " AND ".join(where)) if where else ""

    sql_count = f"""
        SELECT COUNT(*) AS cnt
        FROM predictions p
        INNER JOIN atm_masters m ON p.id_atm = m.id_atm
        {where_sql}
    """
    sql_data = f"""
        {_PRED_JOIN_SELECT}
        {where_sql}
        ORDER BY p.skor_urgensi DESC
        LIMIT %s OFFSET %s
    """

    with get_conn() as conn:
        cur = conn.cursor(dictionary=True)
        cur.execute(sql_count, params)
        total = cur.fetchone()["cnt"]
        cur.execute(sql_data, params + [limit, offset])
        rows = cur.fetchall()

    gen_at = rows[0]["generated_at"] if rows else None
    if gen_at and not isinstance(gen_at, str):
        gen_at = gen_at.isoformat()

    return {
        "total": total,
        "data":  [_fmt_pred(r) for r in rows],
        "generated_at": gen_at,
    }


# ══════════════════════════════════════════════════════════════
#  ATM HISTORY
# ══════════════════════════════════════════════════════════════

def bulk_insert_history(df_history):
    import pandas as pd

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


def get_atm_history_from_db(atm_id: str, last_n_days: int = 7) -> dict:
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
        "denom_options":rows[-1].get("denom_options") if rows else "-",
        "merk_atm":     rows[-1].get("merk_atm") if rows else "-",
        "data":         rows,
    }


# ══════════════════════════════════════════════════════════════
#  CASHPLAN
# ══════════════════════════════════════════════════════════════

def add_to_cashplan(atm_data: dict) -> int:
    """
    Tambah ATM ke cashplan.
    Data statis (lokasi/wilayah/limit/denom_options) diambil dari atm_masters.
    Jika sudah ada PENDING → return id yang ada.

    Urutan prioritas denom:
      1. atm_data["denom"] jika dikirim eksplisit oleh user (string)
      2. denom_options dari atm_masters → _build_denom_string
      3. Default "100000"

    denom disimpan sebagai VARCHAR: "100000" | "50000" | "100000,50000"
    """
    id_atm = str(atm_data.get("id_atm", "")).strip().upper()
    if not id_atm:
        raise ValueError("id_atm tidak boleh kosong")

    with get_conn() as conn:
        # Cek sudah PENDING?
        cur = conn.cursor(dictionary=True)
        cur.execute(
            "SELECT id FROM cashplan WHERE id_atm=%s AND status_cashplan='PENDING'",
            (id_atm,)
        )
        existing = cur.fetchone()
        if existing:
            return existing["id"]

        # Ambil limit & denom_options dari master
        master = _get_master(conn, id_atm)

    limit_val = int(master.get("limit") or 0)

    # ── Tentukan denom ────────────────────────────────────────
    caller_denom = atm_data.get("denom")
    caller_denom_str = str(caller_denom).strip() if caller_denom is not None else ""

    if caller_denom_str and caller_denom_str not in ("0", "100000", ""):
        # User eksplisit set denom → pakai langsung sebagai string
        denom_val = caller_denom_str
    else:
        # Ambil dari master → build string semua denom tersedia
        master_denom_str = master.get("denom_options") or ""
        denom_val = _build_denom_string(master_denom_str)

    saldo  = max(0, int(atm_data.get("saldo", 0)))
    jumlah = max(0, limit_val - saldo)

    sql = """
        INSERT INTO cashplan
            (id_atm, saldo, pct_saldo,
             status_awal, jumlah_isi, denom,
             tgl_isi, jam_isi, est_jam, skor_urgensi, added_by)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute(sql, (
            id_atm,
            saldo,
            float(atm_data.get("pct_saldo", 0)),
            atm_data.get("status", "AWAS"),
            jumlah,
            denom_val,  # string: "100000" | "50000" | "100000,50000"
            atm_data.get("tgl_isi"),
            atm_data.get("jam_isi"),
            float(atm_data.get("est_jam", 0) or 0),
            float(atm_data.get("skor_urgensi", 0) or 0),
            atm_data.get("added_by", "system"),
        ))
        return cur.lastrowid


_CP_JOIN_SELECT = """
    SELECT
        c.*,
        m.lokasi_atm      AS lokasi,
        m.wilayah,
        m.denom_options,
        m.`limit`,
        m.merk_atm,
        UPPER(LEFT(c.id_atm, 3)) AS tipe
    FROM cashplan c
    INNER JOIN atm_masters m ON c.id_atm = m.id_atm
"""


def _fmt_cp(r: dict) -> dict:
    for f in ["added_at", "done_at", "removed_at"]:
        if r.get(f):
            r[f] = r[f].isoformat() if hasattr(r[f], 'isoformat') else str(r[f])
    if r.get("tgl_isi"):
        r["tgl_isi"] = str(r["tgl_isi"])
    r["saldo"] = int(r["saldo"]) if r.get("saldo") is not None else 0
    r["limit"] = int(r["limit"]) if r.get("limit") is not None else 0
    return r


def get_cashplan_list(status: str = "PENDING") -> list:
    with get_conn() as conn:
        cur = conn.cursor(dictionary=True)
        cur.execute(
            f"{_CP_JOIN_SELECT} WHERE c.status_cashplan=%s ORDER BY c.skor_urgensi DESC",
            (status,)
        )
        rows = cur.fetchall()
    return [_fmt_cp(r) for r in rows]


def update_cashplan_status(
    cashplan_id: int,
    new_status:  str,
    keterangan:  str = None,
    denom:       str = None,
) -> dict:
    now = datetime.now()

    with get_conn() as conn:
        cur = conn.cursor(dictionary=True)
        cur.execute("SELECT * FROM cashplan WHERE id=%s", (cashplan_id,))
        item = cur.fetchone()

    if not item:
        raise ValueError(f"Cashplan id {cashplan_id} tidak ditemukan")

    id_atm = item["id_atm"]

    with get_conn() as conn:
        master = _get_master(conn, id_atm)

    lokasi       = master.get("lokasi_atm") or "-"
    wilayah      = master.get("wilayah") or "-"
    tipe         = str(id_atm[:3]).upper() if id_atm else "-"
    denom_opts   = master.get("denom_options") or "100000"
    limit_val    = int(master.get("limit") or 0)

    status_done_label = "SELESAI" if new_status == "DONE" else "BATAL"

    jumlah_isi = int(item.get("jumlah_isi", 0))

    # denom sekarang VARCHAR: "100000" | "50000" | "100000,50000"
    raw_denom  = denom or item.get("denom", "100000")
    denom_str  = str(raw_denom).strip()

    # Untuk hitung lembar → pakai denom terkecil (worst case / max lembar)
    denom_parts = []
    for x in denom_str.split(","):
        x = x.strip()
        if x.isdigit():
            denom_parts.append(int(x))
    denom_for_lembar = min(denom_parts) if denom_parts else 100_000
    lembar    = math.ceil(jumlah_isi / denom_for_lembar) if denom_for_lembar > 0 else 0
    denom_val = denom_str  # simpan string ke rekap


    bulan_str  = _bulan_id(now)

    rekap_sql = """
        INSERT INTO rekap_replacement
            (cashplan_id, id_atm, lokasi, wilayah, tipe, denom_options,
             saldo_awal, `limit`, jumlah_isi, denom, lembar,
             keterangan, status_awal, status_done,
             tgl_isi, jam_isi, done_at, bulan, tahun)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute(rekap_sql, (
            cashplan_id, id_atm, lokasi, wilayah, tipe, denom_opts,
            int(item["saldo"]), limit_val, jumlah_isi, denom_val, lembar,
            keterangan or item.get("keterangan"),
            item.get("status_awal", "AWAS"), status_done_label,
            item.get("tgl_isi"), item.get("jam_isi"), now, bulan_str, now.year,
        ))

    updates = {
        "status_cashplan": new_status,
        "status_done":     status_done_label,
    }
    if keterangan is not None: updates["keterangan"] = keterangan
    if denom      is not None: updates["denom"]      = denom
    if new_status == "DONE":   updates["done_at"]    = now
    else:                      updates["removed_at"] = now

    set_parts = ", ".join(f"{k}=%s" for k in updates)
    vals      = list(updates.values()) + [cashplan_id]
    with get_conn() as conn:
        conn.cursor().execute(
            f"UPDATE cashplan SET {set_parts} WHERE id=%s", vals
        )

    return {
        "cashplan_id": cashplan_id,
        "new_status":  new_status,
        "status_done": status_done_label,
    }


def remove_cashplan_only(cashplan_id: int):
    """Hapus dari antrian cashplan (tombol ✕). TIDAK insert ke rekap."""
    with get_conn() as conn:
        cur = conn.cursor(dictionary=True)
        cur.execute("SELECT id FROM cashplan WHERE id=%s", (cashplan_id,))
        if not cur.fetchone():
            raise ValueError(f"Cashplan id {cashplan_id} tidak ditemukan")

    with get_conn() as conn:
        conn.cursor().execute(
            """UPDATE cashplan
               SET status_cashplan='REMOVED', status_done='REMOVED', removed_at=%s
               WHERE id=%s""",
            (datetime.now(), cashplan_id)
        )


# ══════════════════════════════════════════════════════════════
#  NOTIF CASHPLAN
# ══════════════════════════════════════════════════════════════

def upsert_notif_cashplan(atm_data: dict):
    id_atm = str(atm_data.get("id_atm", "")).strip().upper()

    with get_conn() as conn:
        cur = conn.cursor(dictionary=True)
        cur.execute(
            "SELECT id FROM notif_cashplan WHERE id_atm=%s AND status_notif='PENDING'",
            (id_atm,)
        )
        existing = cur.fetchone()

    if existing:
        with get_conn() as conn:
            conn.cursor().execute(
                """UPDATE notif_cashplan
                   SET saldo=%s, pct_saldo=%s, skor_urgensi=%s,
                       est_jam=%s, status_awal=%s, created_at=%s
                   WHERE id=%s""",
                (
                    int(atm_data.get("saldo", 0)),
                    float(atm_data.get("pct_saldo", 0)),
                    float(atm_data.get("skor_urgensi", 0) or 0),
                    _s(atm_data.get("est_jam")),
                    atm_data.get("status", "AWAS"),
                    datetime.now(),
                    existing["id"],
                )
            )
        return existing["id"]

    sql = """
        INSERT INTO notif_cashplan
            (id_atm, saldo, pct_saldo, skor_urgensi, est_jam,
             status_awal, status_notif, sumber)
        VALUES (%s, %s, %s, %s, %s, %s, 'PENDING', 'system')
    """
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute(sql, (
            id_atm,
            int(atm_data.get("saldo", 0)),
            float(atm_data.get("pct_saldo", 0)),
            float(atm_data.get("skor_urgensi", 0) or 0),
            _s(atm_data.get("est_jam")),
            atm_data.get("status", "AWAS"),
        ))
        return cur.lastrowid


_NOTIF_JOIN_SELECT = """
    SELECT
        n.*,
        m.lokasi_atm      AS lokasi,
        m.wilayah,
        m.denom_options,
        m.`limit`,
        m.merk_atm,
        UPPER(LEFT(n.id_atm, 3)) AS tipe
    FROM notif_cashplan n
    INNER JOIN atm_masters m ON n.id_atm = m.id_atm
"""


def _fmt_notif(r: dict) -> dict:
    for f in ["created_at", "decided_at"]:
        if r.get(f):
            r[f] = r[f].isoformat() if hasattr(r[f], 'isoformat') else str(r[f])
    r["saldo"] = int(r["saldo"]) if r.get("saldo") is not None else 0
    r["limit"] = int(r["limit"]) if r.get("limit") is not None else 0
    return r


def get_notif_pending() -> list:
    with get_conn() as conn:
        cur = conn.cursor(dictionary=True)
        cur.execute(
            f"{_NOTIF_JOIN_SELECT} WHERE n.status_notif='PENDING' ORDER BY n.skor_urgensi DESC"
        )
        rows = cur.fetchall()
    return [_fmt_notif(r) for r in rows]


def approve_notif(notif_id: int) -> int:
    """User approve notif → masuk cashplan dengan added_by='notif'."""
    with get_conn() as conn:
        cur = conn.cursor(dictionary=True)
        cur.execute(
            f"{_NOTIF_JOIN_SELECT} WHERE n.id=%s",
            (notif_id,)
        )
        item = cur.fetchone()

    if not item:
        raise ValueError(f"Notif id {notif_id} tidak ditemukan")

    # Tidak pass denom → add_to_cashplan akan ambil dari master otomatis
    cp_id = add_to_cashplan({
        "id_atm":       item["id_atm"],
        "saldo":        item.get("saldo", 0),
        "pct_saldo":    item.get("pct_saldo", 0),
        "status":       item.get("status_awal", "AWAS"),
        "est_jam":      item.get("est_jam"),
        "skor_urgensi": item.get("skor_urgensi", 0),
        "added_by":     "notif",
    })

    with get_conn() as conn:
        conn.cursor().execute(
            "UPDATE notif_cashplan SET status_notif='APPROVED', decided_at=%s WHERE id=%s",
            (datetime.now(), notif_id)
        )
    return cp_id


def dismiss_notif(notif_id: int):
    with get_conn() as conn:
        cur = conn.cursor(dictionary=True)
        cur.execute("SELECT id FROM notif_cashplan WHERE id=%s", (notif_id,))
        if not cur.fetchone():
            raise ValueError(f"Notif id {notif_id} tidak ditemukan")

    with get_conn() as conn:
        conn.cursor().execute(
            "UPDATE notif_cashplan SET status_notif='DISMISSED', decided_at=%s WHERE id=%s",
            (datetime.now(), notif_id)
        )


# ══════════════════════════════════════════════════════════════
#  REKAP REPLACEMENT
# ══════════════════════════════════════════════════════════════

def update_rekap_replacement(
    rekap_id:    int,
    tgl_isi:     str = None,
    jam_cash_in: str = None,
    jam_cash_out:str = None,
    denom:       int = None,
) -> dict:
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


def get_rekap_replacement(
    bulan:   str = None,
    tahun:   int = None,
    wilayah: str = None,
) -> list:
    where, params = [], []
    if bulan:
        where.append("bulan=%s"); params.append(bulan)
    if tahun:
        where.append("tahun=%s"); params.append(tahun)
    if wilayah and wilayah.lower() != "semua":
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


def get_rekap_for_download(
    wilayah: str = None,
    bulan:   str = None,
    tahun:   int = None,
) -> list:
    where, params = [], []
    if wilayah and wilayah.lower() != "semua":
        where.append("wilayah=%s"); params.append(wilayah)
    if bulan:
        where.append("bulan=%s"); params.append(bulan)
    if tahun:
        where.append("tahun=%s"); params.append(tahun)

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
            params
        )
        rows = cur.fetchall()

    for r in rows:
        if r.get("done_at"): r["done_at"] = r["done_at"].isoformat()
        if r.get("tgl_isi"): r["tgl_isi"] = str(r["tgl_isi"])
        r["saldo_awal"] = int(r["saldo_awal"]) if r.get("saldo_awal") is not None else 0
        r["limit"]      = int(r["limit"])       if r.get("limit")      is not None else 0
        r["jumlah_isi"] = int(r["jumlah_isi"])  if r.get("jumlah_isi") is not None else 0
    return rows


# ══════════════════════════════════════════════════════════════
#  UPLOAD LOG
# ══════════════════════════════════════════════════════════════

def log_upload(
    filename:  str,
    format_:   str,
    rows:      int,
    atm_count: int,
    matched:   int,
    skipped:   int,
    predictions: int,
    retrain:   bool,
    notes:     str = None,
):
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
             predictions, int(retrain), notes)
        )