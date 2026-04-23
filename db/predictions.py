"""
db/predictions.py
CRUD untuk tabel predictions.

Fungsi publik:
    upsert_predictions(predictions)
    get_predictions(wilayah, status, tipe, limit, offset)  → dict
    get_prediction_by_id(atm_id)                           → dict | None
"""

import logging
from datetime import datetime
from typing import Optional

from db import get_conn, _s

logger = logging.getLogger("db.predictions")

# ── JOIN SELECT standar predictions + atm_masters ────────────
_JOIN_SELECT = """
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


def _fmt(r: dict) -> dict:
    for f in ["generated_at", "last_update", "tgl_awas", "tgl_habis", "tgl_isi"]:
        if r.get(f):
            r[f] = str(r[f])
    r["atm_sepi"] = bool(r.get("atm_sepi", 0))
    r["saldo"]    = int(r["saldo"])  if r.get("saldo")  is not None else 0
    r["limit"]    = int(r["limit"])  if r.get("limit")  is not None else 0
    return r


# ═══════════════════════════════════════════════════════════════
#  UPSERT
# ═══════════════════════════════════════════════════════════════

def upsert_predictions(predictions: list):
    """Simpan / update list prediksi ke DB."""
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
    rows = [
        {
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
        }
        for p in predictions
    ]

    with get_conn() as conn:
        conn.cursor().executemany(sql, rows)


# ═══════════════════════════════════════════════════════════════
#  READ
# ═══════════════════════════════════════════════════════════════

def get_predictions(
    wilayah: Optional[str] = None,
    status:  Optional[str] = None,
    tipe:    Optional[str] = None,
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
        {_JOIN_SELECT}
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
        "data":  [_fmt(r) for r in rows],
        "generated_at": gen_at,
    }


def get_prediction_by_id(atm_id: str) -> Optional[dict]:
    """Ambil prediksi satu ATM. Return None jika tidak ada."""
    with get_conn() as conn:
        cur = conn.cursor(dictionary=True)
        cur.execute("SELECT * FROM predictions WHERE id_atm=%s", (atm_id.upper(),))
        row = cur.fetchone()
    if not row:
        return None
    for f in ["generated_at", "last_update", "tgl_awas", "tgl_habis", "tgl_isi"]:
        if row.get(f):
            row[f] = str(row[f])
    row["atm_sepi"] = bool(row.get("atm_sepi", 0))
    return row