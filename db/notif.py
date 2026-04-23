"""
db/notif.py
CRUD untuk tabel notif_cashplan (bell-notif rekomendasi sistem).

Fungsi publik:
    upsert_notif(atm_data)     → int  (notif_id)
    get_notif_pending()        → list
    approve_notif(notif_id)    → int  (cashplan_id)
    dismiss_notif(notif_id)
    dismiss_all_notif()
"""

import logging
from datetime import datetime
from typing import Optional

from db import get_conn, _s

logger = logging.getLogger("db.notif")

_JOIN_SELECT = """
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


def _fmt(r: dict) -> dict:
    for f in ["created_at", "decided_at"]:
        if r.get(f):
            r[f] = r[f].isoformat() if hasattr(r[f], "isoformat") else str(r[f])
    r["saldo"] = int(r["saldo"]) if r.get("saldo") is not None else 0
    r["limit"] = int(r["limit"]) if r.get("limit") is not None else 0
    return r


def upsert_notif(atm_data: dict) -> int:
    """
    Buat atau update notif PENDING untuk sebuah ATM.
    Jika sudah ada PENDING → update data saldo/skor.
    """
    id_atm = str(atm_data.get("id_atm", "")).strip().upper()

    with get_conn() as conn:
        cur = conn.cursor(dictionary=True)
        cur.execute(
            "SELECT id FROM notif_cashplan WHERE id_atm=%s AND status_notif='PENDING'",
            (id_atm,),
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
                ),
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


def get_notif_pending() -> list:
    """Ambil semua notif yang masih PENDING."""
    with get_conn() as conn:
        cur = conn.cursor(dictionary=True)
        cur.execute(
            f"{_JOIN_SELECT} WHERE n.status_notif='PENDING' ORDER BY n.skor_urgensi DESC"
        )
        rows = cur.fetchall()
    return [_fmt(r) for r in rows]


def approve_notif(notif_id: int) -> int:
    """
    User approve notif → ATM masuk cashplan.
    Return cashplan_id.
    """
    from db.cashplan import add_to_cashplan  # local import hindari circular

    with get_conn() as conn:
        cur = conn.cursor(dictionary=True)
        cur.execute(f"{_JOIN_SELECT} WHERE n.id=%s", (notif_id,))
        item = cur.fetchone()
    if not item:
        raise ValueError(f"Notif id {notif_id} tidak ditemukan")

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
            (datetime.now(), notif_id),
        )
    return cp_id


def dismiss_notif(notif_id: int):
    """Dismiss satu notif."""
    with get_conn() as conn:
        cur = conn.cursor(dictionary=True)
        cur.execute("SELECT id FROM notif_cashplan WHERE id=%s", (notif_id,))
        if not cur.fetchone():
            raise ValueError(f"Notif id {notif_id} tidak ditemukan")

    with get_conn() as conn:
        conn.cursor().execute(
            "UPDATE notif_cashplan SET status_notif='DISMISSED', decided_at=%s WHERE id=%s",
            (datetime.now(), notif_id),
        )


def dismiss_all_notif():
    """Dismiss semua notif PENDING sekaligus."""
    with get_conn() as conn:
        conn.cursor().execute(
            "UPDATE notif_cashplan SET status_notif='DISMISSED', decided_at=%s "
            "WHERE status_notif='PENDING'",
            (datetime.now(),),
        )