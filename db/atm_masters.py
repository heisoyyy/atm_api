"""
db/atm_masters.py
CRUD lengkap untuk tabel atm_masters.

Fungsi publik:
    get_all_masters(search, wilayah, limit, offset)  → dict
    get_master_by_id(id_atm)                         → dict | None
    create_master(data)                              → str   (id_atm)
    update_master(id_atm, data)                      → str
    patch_master(id_atm, fields)                     → str
    delete_master(id_atm)                            → str
    import_masters(df)                               → dict  (hasil import)
    get_master_row(conn, id_atm)                     → dict  (raw, pakai koneksi yg ada)
"""

import logging
import math
import re
import traceback
from datetime import datetime
from typing import Any, Dict, Optional

import pandas as pd

from db import get_conn, _s

logger = logging.getLogger("db.atm_masters")

# ── Kolom DB (urutan harus sama persis dengan tabel) ─────────
DB_COLUMNS = [
    "id_atm", "kode_cabang", "merk_atm", "lokasi_atm", "sn",
    "denom_options", "join", "limit", "pct_saldo", "wilayah",
    "alamat_atm", "tipe_mesin", "off_on_bank", "status_pemilik",
    "nama_vendor", "maintenance", "vendor_maintenance",
    "last_maintenance", "cit_mulai", "cit_akhir", "sisa_hari",
    "nama_asuransi", "link_komunikasi", "bw", "media", "isp",
    "no_inventaris", "nilai_inventaris", "unit_pengisian",
    "is_vendor", "lembar", "is_tms", "no", "nomor",
]

# ── Kolom → mapping nama Excel/CSV ───────────────────────────
COL_MAP = {
    "id_atm": "id_atm", "kode_cabang": "kode_cabang",
    "merk_atm": "merk_atm", "lokasi_atm": "lokasi_atm",
    "sn": "sn", "denom_options": "denom_options",
    "join": "join", "limit": "limit",
    "pct_saldo": "pct_saldo", "wilayah": "wilayah",
    "alamat_atm": "alamat_atm", "tipe_mesin": "tipe_mesin",
    "off_on_bank": "off_on_bank", "status_pemilik": "status_pemilik",
    "nama_vendor": "nama_vendor", "maintenance": "maintenance",
    "vendor_maintenance": "vendor_maintenance",
    "last_maintenance": "last_maintenance",
    "cit_mulai": "cit_mulai", "cit_akhir": "cit_akhir",
    "sisa_hari": "sisa_hari", "nama_asuransi": "nama_asuransi",
    "link_komunikasi": "link_komunikasi", "bw": "bw",
    "media": "media", "isp": "isp",
    "no_inventaris": "no_inventaris", "nilai_inventaris": "nilai_inventaris",
    "unit_pengisian": "unit_pengisian", "is_vendor": "is_vendor",
    "lembar": "lembar", "is_tms": "is_tms",
    "no": "no", "nomor": "nomor",
    # alias
    "id atm": "id_atm", "kode cabang": "kode_cabang",
    "merk atm": "merk_atm", "lokasi atm": "lokasi_atm",
    "denom": "denom_options", "join date": "join", "join_date": "join",
    "persentase": "pct_saldo", "alamat atm": "alamat_atm",
    "tipe mesin": "tipe_mesin", "type mesin": "tipe_mesin",
    "off on bank": "off_on_bank", "status pemilik": "status_pemilik",
    "status pemilik atm": "status_pemilik", "nama vendor": "nama_vendor",
    "vendor maintenance": "vendor_maintenance",
    "jadwal terakhir": "last_maintenance", "jadwal_terakhir": "last_maintenance",
    "last maintenance": "last_maintenance",
    "periode cit cis mulai": "cit_mulai", "periode cit/cis mulai": "cit_mulai",
    "periode cit cis akhir": "cit_akhir", "periode cit/cis akhir": "cit_akhir",
    "sisa waktu": "sisa_hari", "sisa waktu hari": "sisa_hari",
    "nama asuransi": "nama_asuransi", "link komunikasi": "link_komunikasi",
    "no inventaris": "no_inventaris", "nilai inventaris": "nilai_inventaris",
    "unit pengisian": "unit_pengisian", "is vendor": "is_vendor", "is tms": "is_tms",
}

# ═══════════════════════════════════════════════════════════════
#  SANITIZER HELPERS
# ═══════════════════════════════════════════════════════════════

def _clean_pct(v) -> Optional[float]:
    if v is None:
        return None
    if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
        return None
    try:
        s = str(v).strip().replace("%", "").replace(",", ".")
        if s in ("", "-"):
            return None
        return round(min(max(float(s), -9999.99), 9999.99), 2)
    except Exception:
        return None


def _clean_bigint(v) -> Optional[int]:
    if v is None:
        return None
    if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
        return None
    try:
        s = str(v).strip().replace(".", "").replace(",", "").replace(" ", "")
        return None if s in ("", "-") else int(float(s))
    except Exception:
        return None


def _clean_int(v) -> Optional[int]:
    if v is None:
        return None
    if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
        return None
    try:
        return int(float(str(v).strip()))
    except Exception:
        return None


def _clean_str(v) -> Optional[str]:
    if v is None:
        return None
    if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
        return None
    s = str(v).strip()
    return s if s and s.upper() not in ("NAN", "NONE", "NULL", "NA", "-") else None


def _clean_date(v) -> Optional[str]:
    """Konversi berbagai format tanggal → 'YYYY-MM-DD'."""
    if v is None:
        return None
    if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
        return None
    if hasattr(v, "strftime"):
        return v.strftime("%Y-%m-%d")
    s = str(v).strip()
    if not s or s.upper() in ("NAN", "NONE", "NULL", "NA", "-", ""):
        return None

    BULAN = {
        "jan": "01", "feb": "02", "mar": "03", "apr": "04",
        "may": "05", "mei": "05", "jun": "06", "jul": "07",
        "aug": "08", "agu": "08", "sep": "09", "oct": "10",
        "okt": "10", "nov": "11", "dec": "12", "des": "12",
    }
    m = re.match(r"^(\d{1,2})[-/]([A-Za-z]{3})[-/](\d{2,4})$", s)
    if m:
        day, mon_str, yr = m.group(1), m.group(2).lower(), m.group(3)
        mon = BULAN.get(mon_str)
        if mon:
            year = (2000 + int(yr)) if len(yr) == 2 else int(yr)
            return f"{year}-{mon}-{day.zfill(2)}"

    m = re.match(r"^(\d{1,2})/(\d{1,2})/(\d{4})$", s)
    if m:
        return f"{m.group(3)}-{m.group(2).zfill(2)}-{m.group(1).zfill(2)}"

    m = re.match(r"^(\d{4})-(\d{2})-(\d{2})$", s)
    if m:
        return s

    m = re.match(r"^(\d{4})/(\d{2})/(\d{2})$", s)
    if m:
        return f"{m.group(1)}-{m.group(2)}-{m.group(3)}"

    try:
        return pd.to_datetime(s, dayfirst=True).strftime("%Y-%m-%d")
    except Exception:
        return None


def _clean_lembar(v) -> Optional[int]:
    if v is None:
        return None
    if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
        return None
    try:
        s = str(v).strip()
        if not s or s.upper() in ("NAN", "NONE", "NULL", "NA", "-"):
            return None
        return int(float(s.replace(",", "").replace(".", "")))
    except Exception:
        return None


_COL_SANITIZERS = {
    "kode_cabang": _clean_int, "join": _clean_int,
    "no": _clean_int, "nomor": _clean_int, "is_vendor": _clean_int,
    "lembar": _clean_lembar,
    "pct_saldo": _clean_pct,
    "limit": _clean_bigint,
    "last_maintenance": _clean_date,
    "cit_mulai": _clean_date, "cit_akhir": _clean_date,
    "nilai_inventaris": lambda v: (
        str(int(float(str(v).strip().replace(".", "").replace(",", ""))))
        if v is not None
        and not (isinstance(v, float) and math.isnan(v))
        and str(v).strip() not in ("", "-", "NaN", "None")
        else None
    ),
}

_STR_COLS = {
    "id_atm", "merk_atm", "lokasi_atm", "sn", "denom_options",
    "wilayah", "alamat_atm", "tipe_mesin", "off_on_bank", "status_pemilik",
    "nama_vendor", "maintenance", "vendor_maintenance", "sisa_hari",
    "nama_asuransi", "link_komunikasi", "bw", "media", "isp",
    "no_inventaris", "unit_pengisian", "is_tms",
}


def _sanitize(col: str, v):
    if col in _COL_SANITIZERS:
        return _COL_SANITIZERS[col](v)
    if col in _STR_COLS:
        return _clean_str(v)
    if v is None:
        return None
    if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
        return None
    return v


def _serialize(row: dict) -> dict:
    return {k: _s(v) for k, v in row.items()}


# ═══════════════════════════════════════════════════════════════
#  CRUD PUBLIC FUNCTIONS
# ═══════════════════════════════════════════════════════════════

def get_all_masters(
    search: Optional[str] = None,
    wilayah: Optional[str] = None,
    limit: int = 20,
    offset: int = 0,
) -> dict:
    """Ambil daftar ATM master dengan filter & paginasi."""
    where, params = [], []
    if search:
        where.append("(id_atm LIKE %s OR lokasi_atm LIKE %s OR nama_vendor LIKE %s)")
        params += [f"%{search}%"] * 3
    if wilayah:
        where.append("wilayah = %s")
        params.append(wilayah)

    where_sql = ("WHERE " + " AND ".join(where)) if where else ""

    with get_conn() as conn:
        cur = conn.cursor(dictionary=True)
        cur.execute(f"SELECT COUNT(*) AS cnt FROM atm_masters {where_sql}", params)
        total = cur.fetchone()["cnt"]

        cur.execute(
            f"SELECT * FROM atm_masters {where_sql} ORDER BY id_atm LIMIT %s OFFSET %s",
            params + [limit, offset],
        )
        rows = cur.fetchall()

        cur.execute(
            "SELECT DISTINCT wilayah FROM atm_masters "
            "WHERE wilayah IS NOT NULL AND wilayah != '' ORDER BY wilayah"
        )
        wilayah_opts = [r["wilayah"] for r in cur.fetchall()]

    return {
        "total": total,
        "limit": limit,
        "offset": offset,
        "data": [_serialize(r) for r in rows],
        "wilayah_options": wilayah_opts,
    }


def get_master_by_id(id_atm: str) -> Optional[dict]:
    """Ambil satu ATM master. Return None jika tidak ada."""
    with get_conn() as conn:
        cur = conn.cursor(dictionary=True)
        cur.execute("SELECT * FROM atm_masters WHERE id_atm = %s", (id_atm.upper(),))
        row = cur.fetchone()
    return _serialize(row) if row else None


def get_master_row(conn, id_atm: str) -> dict:
    """Ambil raw dict dari master pakai koneksi yang sudah ada. Return {} jika tidak ada."""
    cur = conn.cursor(dictionary=True)
    cur.execute("SELECT * FROM atm_masters WHERE id_atm = %s", (id_atm.upper(),))
    return cur.fetchone() or {}


def create_master(data: dict) -> str:
    """Insert ATM baru. Raise ValueError jika duplikat."""
    data["id_atm"] = str(data["id_atm"]).strip().upper()
    if data.get("pct_saldo") is not None:
        data["pct_saldo"] = _clean_pct(data["pct_saldo"])
    if data.get("limit") is not None:
        data["limit"] = _clean_bigint(data["limit"])

    cols = [c for c in DB_COLUMNS if data.get(c) is not None]
    vals = [data[c] for c in cols]
    col_sql = ", ".join(f"`{c}`" for c in cols)
    placeholders = ", ".join(["%s"] * len(cols))

    with get_conn() as conn:
        try:
            conn.cursor().execute(
                f"INSERT INTO atm_masters ({col_sql}) VALUES ({placeholders})", vals
            )
        except Exception as e:
            if "Duplicate entry" in str(e):
                raise ValueError(f"ID ATM '{data['id_atm']}' sudah ada.")
            raise

    return data["id_atm"]


def update_master(id_atm: str, data: dict) -> str:
    """Full update (PUT) ATM master."""
    data["id_atm"] = id_atm.strip().upper()
    if data.get("pct_saldo") is not None:
        data["pct_saldo"] = _clean_pct(data["pct_saldo"])
    if data.get("limit") is not None:
        data["limit"] = _clean_bigint(data["limit"])

    update_cols = [c for c in DB_COLUMNS if c != "id_atm"]
    set_parts = ", ".join(f"`{c}`=%s" for c in update_cols)
    vals = [data.get(c) for c in update_cols] + [data["id_atm"]]

    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute(f"UPDATE atm_masters SET {set_parts} WHERE id_atm=%s", vals)
        if cur.rowcount == 0:
            raise ValueError(f"ATM {id_atm} tidak ditemukan.")

    return data["id_atm"]


def patch_master(id_atm: str, fields: Dict[str, Any]) -> str:
    """Partial update (PATCH) ATM master."""
    allowed = set(DB_COLUMNS) - {"id_atm"}
    sanitized = {
        k: _sanitize(k, v)
        for k, v in fields.items()
        if k in allowed
    }
    if not sanitized:
        raise ValueError("Tidak ada field valid yang bisa diupdate.")

    set_parts = ", ".join(f"`{k}`=%s" for k in sanitized)
    vals = list(sanitized.values()) + [id_atm.strip().upper()]

    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute(f"UPDATE atm_masters SET {set_parts} WHERE id_atm=%s", vals)
        if cur.rowcount == 0:
            raise ValueError(f"ATM {id_atm} tidak ditemukan.")

    return id_atm.upper()


def delete_master(id_atm: str) -> str:
    """Hapus ATM master."""
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute("DELETE FROM atm_masters WHERE id_atm=%s", (id_atm.strip().upper(),))
        if cur.rowcount == 0:
            raise ValueError(f"ATM {id_atm} tidak ditemukan.")
    return id_atm.upper()


def import_masters(df: pd.DataFrame) -> dict:
    """
    Bulk UPSERT dari DataFrame hasil baca file.
    Return: { total, inserted, errors[] }
    """
    # Normalize kolom
    df.columns = [str(c).strip() for c in df.columns]
    rename_map = {
        col: COL_MAP[col.lower().strip()]
        for col in df.columns
        if col.lower().strip() in COL_MAP
    }
    df = df.rename(columns=rename_map)

    if "id_atm" not in df.columns:
        raise ValueError(f"Kolom 'ID ATM' tidak ditemukan. Kolom tersedia: {list(df.columns)}")

    df["id_atm"] = df["id_atm"].astype(str).str.strip().str.upper()
    df = df[
        df["id_atm"].notna()
        & (df["id_atm"] != "")
        & (~df["id_atm"].str.upper().isin(["NAN", "NONE", "NULL", "NA"]))
    ]

    if df.empty:
        raise ValueError("Tidak ada baris valid setelah filter ID ATM.")

    upsert_sql = (
        f"INSERT INTO atm_masters ({', '.join(f'`{c}`' for c in DB_COLUMNS)}) "
        f"VALUES ({', '.join(['%s'] * len(DB_COLUMNS))}) "
        f"ON DUPLICATE KEY UPDATE "
        + ", ".join(f"`{c}`=VALUES(`{c}`)" for c in DB_COLUMNS if c != "id_atm")
    )

    batch_vals, row_errors = [], []

    for idx, row in df.iterrows():
        try:
            vals = [_sanitize(col, row.get(col)) for col in DB_COLUMNS]
            batch_vals.append(vals)
        except Exception as e:
            row_errors.append({
                "id_atm": str(row.get("id_atm", "?")),
                "row": int(idx),
                "error": str(e),
            })

    inserted = 0
    CHUNK = 500
    for start in range(0, len(batch_vals), CHUNK):
        chunk = batch_vals[start: start + CHUNK]
        try:
            with get_conn() as conn:
                cur = conn.cursor()
                cur.executemany(upsert_sql, chunk)
                inserted += len(chunk)
        except Exception as e:
            logger.error("Batch chunk %d-%d gagal: %s", start, start + CHUNK, e)
            for i, vals in enumerate(chunk):
                atm_id = vals[0] if vals else "?"
                try:
                    with get_conn() as conn:
                        conn.cursor().execute(upsert_sql, vals)
                    inserted += 1
                except Exception as e2:
                    row_errors.append({
                        "id_atm": str(atm_id),
                        "row": start + i,
                        "error": str(e2),
                    })

    inserted = max(0, inserted - len(row_errors))
    logger.info("Import selesai: total=%d inserted≈%d errors=%d", len(df), inserted, len(row_errors))

    return {
        "total": len(df),
        "inserted": inserted,
        "errors": row_errors[:20],
    }