"""
db/cashplan.py
CRUD untuk tabel cashplan (antrian pengisian ATM).

Fungsi publik:
    add_to_cashplan(atm_data)              → int  (cashplan_id)
    get_cashplan_list(status)              → list
    update_cashplan_status(id, status, …)  → dict
    remove_cashplan_only(cashplan_id)
"""

import logging
import math
from datetime import datetime
from typing import Optional

from db import get_conn, _s
from db.atm_masters import get_master_row

logger = logging.getLogger("db.cashplan")

# ── JOIN SELECT standar cashplan + atm_masters ───────────────
_JOIN_SELECT = """
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

_BULAN_MAP = {
    "January": "Januari", "February": "Februari", "March": "Maret",
    "April": "April", "May": "Mei", "June": "Juni",
    "July": "Juli", "August": "Agustus", "September": "September",
    "October": "Oktober", "November": "November", "December": "Desember",
}


def _bulan_id(dt: datetime) -> str:
    return _BULAN_MAP.get(dt.strftime("%B"), dt.strftime("%B"))


def _parse_denom_options(denom_options_str: str) -> int:
    """
    Parse kolom denom_options → nilai rupiah penuh.
    "100" → 100_000 | "50" → 50_000 | "100 & 50" → 100_000 | "50000" → 50_000
    """
    if not denom_options_str:
        return 100_000

    raw = str(denom_options_str).strip()

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

    try:
        val = int(float(raw.replace(".", "").replace(",", "")))
    except ValueError:
        return 100_000

    return val * 1_000 if val <= 1_000 else val


def _fmt(r: dict) -> dict:
    for f in ["added_at", "done_at", "removed_at"]:
        if r.get(f):
            r[f] = r[f].isoformat() if hasattr(r[f], "isoformat") else str(r[f])
    if r.get("tgl_isi"):
        r["tgl_isi"] = str(r["tgl_isi"])
    r["saldo"] = int(r["saldo"]) if r.get("saldo") is not None else 0
    r["limit"] = int(r["limit"]) if r.get("limit") is not None else 0
    return r


# ═══════════════════════════════════════════════════════════════
#  PUBLIC FUNCTIONS
# ═══════════════════════════════════════════════════════════════

def add_to_cashplan(atm_data: dict) -> int:
    """
    Tambah ATM ke antrian cashplan.
    Jika sudah ada PENDING → kembalikan id yang ada.
    Denom diambil otomatis dari atm_masters.
    """
    id_atm = str(atm_data.get("id_atm", "")).strip().upper()
    if not id_atm:
        raise ValueError("id_atm tidak boleh kosong")

    with get_conn() as conn:
        cur = conn.cursor(dictionary=True)
        cur.execute(
            "SELECT id FROM cashplan WHERE id_atm=%s AND status_cashplan='PENDING'",
            (id_atm,),
        )
        existing = cur.fetchone()
        if existing:
            return existing["id"]

        master = get_master_row(conn, id_atm)

    limit_val = int(master.get("limit") or 0)

    caller_denom = atm_data.get("denom")
    if caller_denom and int(caller_denom) not in (0, 100_000):
        denom_val = int(caller_denom)
    else:
        denom_val = _parse_denom_options(master.get("denom_options") or "")

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
            denom_val,
            atm_data.get("tgl_isi"),
            atm_data.get("jam_isi"),
            float(atm_data.get("est_jam", 0) or 0),
            float(atm_data.get("skor_urgensi", 0) or 0),
            atm_data.get("added_by", "system"),
        ))
        return cur.lastrowid


def get_cashplan_list(status: str = "PENDING") -> list:
    """Ambil daftar cashplan berdasarkan status."""
    with get_conn() as conn:
        cur = conn.cursor(dictionary=True)
        cur.execute(
            f"{_JOIN_SELECT} WHERE c.status_cashplan=%s ORDER BY c.skor_urgensi DESC",
            (status,),
        )
        rows = cur.fetchall()
    return [_fmt(r) for r in rows]


def update_cashplan_status(
    cashplan_id: int,
    new_status:  str,
    keterangan:  Optional[str] = None,
    denom:       Optional[int] = None,
) -> dict:
    """
    Ubah status cashplan ke DONE atau REMOVED.
    Otomatis insert snapshot ke rekap_replacement.
    """
    now = datetime.now()

    with get_conn() as conn:
        cur = conn.cursor(dictionary=True)
        cur.execute("SELECT * FROM cashplan WHERE id=%s", (cashplan_id,))
        item = cur.fetchone()
    if not item:
        raise ValueError(f"Cashplan id {cashplan_id} tidak ditemukan")

    id_atm = item["id_atm"]
    with get_conn() as conn:
        master = get_master_row(conn, id_atm)

    lokasi     = master.get("lokasi_atm") or "-"
    wilayah    = master.get("wilayah") or "-"
    tipe       = str(id_atm[:3]).upper() if id_atm else "-"
    denom_opts = master.get("denom_options") or "100000"
    limit_val  = int(master.get("limit") or 0)

    status_done_label = "SELESAI" if new_status == "DONE" else "BATAL"
    jumlah_isi = int(item.get("jumlah_isi", 0))
    denom_val  = denom or int(item.get("denom", 100_000))
    lembar     = math.ceil(jumlah_isi / denom_val) if denom_val > 0 else 0
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
        conn.cursor().execute(rekap_sql, (
            cashplan_id, id_atm, lokasi, wilayah, tipe, denom_opts,
            int(item["saldo"]), limit_val, jumlah_isi, denom_val, lembar,
            keterangan or item.get("keterangan"),
            item.get("status_awal", "AWAS"), status_done_label,
            item.get("tgl_isi"), item.get("jam_isi"), now, bulan_str, now.year,
        ))

    updates = {"status_cashplan": new_status, "status_done": status_done_label}
    if keterangan is not None: updates["keterangan"] = keterangan
    if denom      is not None: updates["denom"]      = denom
    if new_status == "DONE":   updates["done_at"]    = now
    else:                      updates["removed_at"] = now

    set_parts = ", ".join(f"{k}=%s" for k in updates)
    vals      = list(updates.values()) + [cashplan_id]
    with get_conn() as conn:
        conn.cursor().execute(f"UPDATE cashplan SET {set_parts} WHERE id=%s", vals)

    return {
        "cashplan_id": cashplan_id,
        "new_status":  new_status,
        "status_done": status_done_label,
    }


def remove_cashplan_only(cashplan_id: int):
    """Hapus dari antrian tanpa mencatat ke rekap (tombol ✕)."""
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
            (datetime.now(), cashplan_id),
        )