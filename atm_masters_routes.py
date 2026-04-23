# ══════════════════════════════════════════════════════════════
#  atm_masters_routes.py  — synced to actual DB schema
#  FIXED: pct_saldo out-of-range, batch insert, nilai_inventaris sanitize
#  FIXED v2: last_maintenance date format, lembar truncation
# ══════════════════════════════════════════════════════════════

import io
import logging
import math
import traceback
from typing import Any, Dict, Optional

import pandas as pd
from fastapi import APIRouter, Body, HTTPException, Query, UploadFile, File
from pydantic import BaseModel

router = APIRouter(tags=["ATM Masters"])

logger = logging.getLogger("atm_masters")
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)


def _raise500(context: str, exc: Exception) -> None:
    tb = traceback.format_exc()
    logger.error("=== ERROR in %s ===\n%s", context, tb)
    raise HTTPException(
        status_code=500,
        detail={
            "context": context,
            "error":   str(exc),
            "type":    type(exc).__name__,
            "trace":   tb.splitlines()[-5:],
        },
    )


def _s(v):
    if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
        return None
    return v


# ── FIX: Sanitizer untuk kolom-kolom bermasalah ──────────────

def _clean_pct(v) -> Optional[float]:
    """
    Konversi berbagai format pct_saldo ke DECIMAL(6,2) yang aman.
    Handles: None, NaN, "75%", "75,5", 75.5, "100.00", dll.
    Range aman DECIMAL(6,2): -9999.99 s/d 9999.99
    """
    if v is None:
        return None
    if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
        return None
    try:
        s = str(v).strip().replace("%", "").replace(",", ".")
        if s == "" or s == "-":
            return None
        f = float(s)
        # Clamp ke range DECIMAL(6,2)
        return round(min(max(f, -9999.99), 9999.99), 2)
    except Exception:
        return None


def _clean_bigint(v) -> Optional[int]:
    """Konversi nilai limit/nilai_inventaris ke BIGINT yang aman."""
    if v is None:
        return None
    if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
        return None
    try:
        # Handle format "1.000.000" atau "1,000,000"
        s = str(v).strip().replace(".", "").replace(",", "").replace(" ", "")
        if s == "" or s == "-":
            return None
        return int(float(s))
    except Exception:
        return None


def _clean_int(v) -> Optional[int]:
    """Konversi nilai integer biasa, return None jika tidak valid."""
    if v is None:
        return None
    if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
        return None
    try:
        return int(float(str(v).strip()))
    except Exception:
        return None


def _clean_str(v) -> Optional[str]:
    """Konversi ke string, return None jika kosong/NaN."""
    if v is None:
        return None
    if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
        return None
    s = str(v).strip()
    return s if s and s.upper() not in ("NAN", "NONE", "NULL", "NA", "-") else None


def _clean_date(v) -> Optional[str]:
    """
    Konversi berbagai format tanggal ke 'YYYY-MM-DD' untuk MySQL DATE column.
    Handles:
      - '1-Jul-25'   → '2025-07-01'
      - '16-Jul-25'  → '2025-07-16'
      - '25-Jun-25'  → '2025-06-25'
      - '11/8/2025'  → '2025-08-11'
      - '1/8/2025'   → '2025-08-01'
      - '11/7/2025'  → '2025-07-11'
      - '2025-07-01' → '2025-07-01' (sudah benar)
      - datetime/Timestamp dari pandas
    """
    if v is None:
        return None
    if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
        return None

    import re
    from datetime import datetime

    # Sudah datetime/Timestamp
    if hasattr(v, 'strftime'):
        return v.strftime('%Y-%m-%d')

    s = str(v).strip()
    if not s or s.upper() in ("NAN", "NONE", "NULL", "NA", "-", ""):
        return None

    # Mapping bulan singkatan Inggris → angka
    BULAN = {
        'jan': '01', 'feb': '02', 'mar': '03', 'apr': '04',
        'may': '05', 'mei': '05', 'jun': '06', 'jul': '07',
        'aug': '08', 'agu': '08', 'sep': '09', 'oct': '10',
        'okt': '10', 'nov': '11', 'dec': '12', 'des': '12',
    }

    # Format: "1-Jul-25" atau "16-Jul-25" atau "25-Jun-25"
    m = re.match(r'^(\d{1,2})[-/]([A-Za-z]{3})[-/](\d{2,4})$', s)
    if m:
        day, mon_str, yr = m.group(1), m.group(2).lower(), m.group(3)
        mon = BULAN.get(mon_str)
        if mon:
            year = (2000 + int(yr)) if len(yr) == 2 else int(yr)
            return f"{year}-{mon}-{day.zfill(2)}"

    # Format: "11/8/2025" atau "1/8/2025" (DD/MM/YYYY)
    m = re.match(r'^(\d{1,2})/(\d{1,2})/(\d{4})$', s)
    if m:
        day, mon, yr = m.group(1), m.group(2), m.group(3)
        return f"{yr}-{mon.zfill(2)}-{day.zfill(2)}"

    # Format: "2025-07-01" sudah benar
    m = re.match(r'^(\d{4})-(\d{2})-(\d{2})$', s)
    if m:
        return s

    # Format: "2025/07/01"
    m = re.match(r'^(\d{4})/(\d{2})/(\d{2})$', s)
    if m:
        return f"{m.group(1)}-{m.group(2)}-{m.group(3)}"

    # Fallback: coba pandas parse
    try:
        import pandas as pd
        parsed = pd.to_datetime(s, dayfirst=True)
        return parsed.strftime('%Y-%m-%d')
    except Exception:
        pass

    # Tidak bisa di-parse → simpan sebagai None daripada error
    logger.warning("  _clean_date: tidak bisa parse '%s', set None", s)
    return None


def _clean_lembar(v) -> Optional[int]:
    """
    Kolom lembar di DB adalah INT.
    Handle nilai seperti "1,500" (format ribuan) atau "150".
    """
    if v is None:
        return None
    if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
        return None
    try:
        s = str(v).strip()
        if not s or s.upper() in ("NAN", "NONE", "NULL", "NA", "-"):
            return None
        # Hapus pemisah ribuan (titik atau koma)
        # Heuristic: kalau ada koma DAN tidak ada titik → "1,500" = 1500
        # Kalau ada titik DAN tidak ada koma → "1.500" = 1500
        s_clean = s.replace(",", "").replace(".", "")
        return int(float(s_clean))
    except Exception:
        return None


# ── Sanitizer terpusat per kolom ─────────────────────────────

# Mapping kolom → fungsi sanitizer
_COL_SANITIZERS = {
    # INT columns
    "kode_cabang":    _clean_int,
    "join":           _clean_int,
    "no":             _clean_int,
    "nomor":          _clean_int,
    "is_vendor":      _clean_int,
    # INT tapi sering format ribuan "1,500"
    "lembar":         _clean_lembar,
    # DECIMAL(6,2)
    "pct_saldo":      _clean_pct,
    # BIGINT
    "limit":          _clean_bigint,
    # DATE columns — konversi berbagai format ke YYYY-MM-DD
    "last_maintenance": _clean_date,
    "cit_mulai":        _clean_date,
    "cit_akhir":        _clean_date,
    # TEXT tapi sering berisi angka besar
    "nilai_inventaris": lambda v: str(int(float(str(v).strip().replace(".", "").replace(",", "")))) if v is not None and not (isinstance(v, float) and math.isnan(v)) and str(v).strip() not in ("", "-", "NaN", "None") else None,
}

# Kolom yang murni string (tidak perlu sanitizer khusus)
_STR_COLS = {
    "id_atm", "merk_atm", "lokasi_atm", "sn", "denom_options",
    "wilayah", "alamat_atm", "tipe_mesin", "off_on_bank", "status_pemilik",
    "nama_vendor", "maintenance", "vendor_maintenance",
    "sisa_hari", "nama_asuransi", "link_komunikasi",
    "bw", "media", "isp", "no_inventaris", "unit_pengisian", "is_tms",
}


def _sanitize_val(col: str, v):
    """Sanitize satu nilai berdasarkan kolom."""
    if col in _COL_SANITIZERS:
        return _COL_SANITIZERS[col](v)
    if col in _STR_COLS:
        return _clean_str(v)
    # Fallback
    if v is None:
        return None
    if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
        return None
    return v


# ════════════════════════════════════════════════════════════
#  DB_COLUMNS — urutan & nama HARUS sama persis dengan tabel
# ════════════════════════════════════════════════════════════
DB_COLUMNS = [
    "id_atm",
    "kode_cabang",
    "merk_atm",
    "lokasi_atm",
    "sn",
    "denom_options",
    "join",
    "limit",
    "pct_saldo",
    "wilayah",
    "alamat_atm",
    "tipe_mesin",
    "off_on_bank",
    "status_pemilik",
    "nama_vendor",
    "maintenance",
    "vendor_maintenance",
    "last_maintenance",
    "cit_mulai",
    "cit_akhir",
    "sisa_hari",
    "nama_asuransi",
    "link_komunikasi",
    "bw",
    "media",
    "isp",
    "no_inventaris",
    "nilai_inventaris",
    "unit_pengisian",
    "is_vendor",
    "lembar",
    "is_tms",
    "no",
    "nomor",
]


# ── Pydantic model ────────────────────────────────────────────
class ATMMasterModel(BaseModel):
    id_atm:              str
    kode_cabang:         Optional[int]   = None
    merk_atm:            Optional[str]   = None
    lokasi_atm:          Optional[str]   = None
    sn:                  Optional[str]   = None
    denom_options:       Optional[str]   = None
    join:                Optional[int]   = None
    limit:               Optional[int]   = None
    pct_saldo:           Optional[float] = None   # ← float, bukan int
    wilayah:             Optional[str]   = None
    alamat_atm:          Optional[str]   = None
    tipe_mesin:          Optional[str]   = None
    off_on_bank:         Optional[str]   = None
    status_pemilik:      Optional[str]   = None
    nama_vendor:         Optional[str]   = None
    maintenance:         Optional[str]   = None
    vendor_maintenance:  Optional[str]   = None
    last_maintenance:    Optional[str]   = None
    cit_mulai:           Optional[str]   = None
    cit_akhir:           Optional[str]   = None
    sisa_hari:           Optional[str]   = None
    nama_asuransi:       Optional[str]   = None
    link_komunikasi:     Optional[str]   = None
    bw:                  Optional[str]   = None
    media:               Optional[str]   = None
    isp:                 Optional[str]   = None
    no_inventaris:       Optional[str]   = None
    nilai_inventaris:    Optional[str]   = None
    unit_pengisian:      Optional[str]   = None
    is_vendor:           Optional[int]   = None
    lembar:              Optional[str]   = None
    is_tms:              Optional[str]   = None
    no:                  Optional[int]   = None
    nomor:               Optional[int]   = None


# ── Column mapping: Excel/CSV header → DB column ─────────────
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
    # alias dari Excel
    "id atm": "id_atm",
    "kode cabang": "kode_cabang",
    "merk atm": "merk_atm",
    "lokasi atm": "lokasi_atm",
    "denom": "denom_options",
    "join date": "join",
    "join_date": "join",
    "persentase": "pct_saldo",
    "alamat atm": "alamat_atm",
    "tipe mesin": "tipe_mesin",
    "type mesin": "tipe_mesin",
    "off on bank": "off_on_bank",
    "status pemilik": "status_pemilik",
    "status pemilik atm": "status_pemilik",
    "nama vendor": "nama_vendor",
    "vendor maintenance": "vendor_maintenance",
    "jadwal terakhir": "last_maintenance",
    "jadwal_terakhir": "last_maintenance",
    "last maintenance": "last_maintenance",
    "periode cit cis mulai": "cit_mulai",
    "periode cit/cis mulai": "cit_mulai",
    "periode cit cis akhir": "cit_akhir",
    "periode cit/cis akhir": "cit_akhir",
    "sisa waktu": "sisa_hari",
    "sisa waktu hari": "sisa_hari",
    "nama asuransi": "nama_asuransi",
    "link komunikasi": "link_komunikasi",
    "no inventaris": "no_inventaris",
    "nilai inventaris": "nilai_inventaris",
    "unit pengisian": "unit_pengisian",
    "is vendor": "is_vendor",
    "is tms": "is_tms",
}


def _serialize(row: dict) -> dict:
    return {k: _s(v) for k, v in row.items()}


# ════════════════════════════════════════════════════════════
#  GET /api/atm-masters
# ════════════════════════════════════════════════════════════
@router.get("/api/atm-masters")
def list_atm_masters(
    search:  Optional[str] = Query(None),
    wilayah: Optional[str] = Query(None),
    limit:   int           = Query(20, ge=1, le=200),
    offset:  int           = Query(0, ge=0),
):
    logger.debug("LIST | search=%r wilayah=%r limit=%d offset=%d",
                 search, wilayah, limit, offset)
    try:
        from database import get_conn
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
            "total":           total,
            "limit":           limit,
            "offset":          offset,
            "data":            [_serialize(r) for r in rows],
            "wilayah_options": wilayah_opts,
        }
    except HTTPException:
        raise
    except Exception as e:
        _raise500("GET /api/atm-masters", e)


# ════════════════════════════════════════════════════════════
#  POST /api/atm-masters/import  ⚠ SEBELUM /{id_atm}
# ════════════════════════════════════════════════════════════
@router.post("/api/atm-masters/import")
async def import_atm_masters(file: UploadFile = File(...)):
    fname   = file.filename or ""
    content = await file.read()
    logger.info("IMPORT | file=%r size=%d bytes", fname, len(content))

    buf = io.BytesIO(content)
    try:
        if fname.lower().endswith(".csv"):
            df = None
            for enc in ("utf-8", "utf-8-sig", "latin-1", "cp1252"):
                try:
                    buf.seek(0)
                    df = pd.read_csv(buf, encoding=enc, dtype=str)  # ← dtype=str: baca semua sebagai string dulu
                    logger.debug("  CSV enc=%s shape=%s", enc, df.shape)
                    break
                except UnicodeDecodeError:
                    continue
            if df is None:
                buf.seek(0)
                df = pd.read_csv(buf, encoding="latin-1", errors="replace", dtype=str)
        elif fname.lower().endswith((".xlsx", ".xlsm")):
            df = pd.read_excel(buf, engine="openpyxl", dtype=str)  # ← dtype=str
            logger.debug("  XLSX shape=%s", df.shape)
        elif fname.lower().endswith(".xls"):
            df = pd.read_excel(buf, engine="xlrd", dtype=str)      # ← dtype=str
            logger.debug("  XLS shape=%s", df.shape)
        else:
            raise HTTPException(400, f"Format tidak didukung: {fname}")
    except HTTPException:
        raise
    except Exception as e:
        logger.error("  Read file failed:\n%s", traceback.format_exc())
        raise HTTPException(400, f"Gagal membaca file: {e}")

    if df.empty:
        raise HTTPException(400, "File kosong.")

    # ── Normalize kolom ───────────────────────────────────────
    original_cols = list(df.columns)
    df.columns    = [str(c).strip() for c in df.columns]
    rename_map    = {col: COL_MAP[col.lower().strip()]
                     for col in df.columns if col.lower().strip() in COL_MAP}
    df = df.rename(columns=rename_map)
    logger.debug("  Original cols: %s", original_cols)
    logger.debug("  After rename:  %s", list(df.columns))

    if "id_atm" not in df.columns:
        logger.error("  No id_atm col. Available: %s", list(df.columns))
        raise HTTPException(
            400,
            f"Kolom 'ID ATM' tidak ditemukan. Kolom tersedia: {list(df.columns)}"
        )

    df["id_atm"] = df["id_atm"].astype(str).str.strip().str.upper()
    df = df[df["id_atm"].notna() & (df["id_atm"] != "") & (~df["id_atm"].str.upper().isin(["NAN", "NONE", "NULL", "NA"]))]
    logger.debug("  Valid rows after filter: %d", len(df))

    if df.empty:
        raise HTTPException(400, "Tidak ada baris valid setelah filter ID ATM.")

    # ── Siapkan UPSERT SQL ────────────────────────────────────
    upsert_sql = (
        f"INSERT INTO atm_masters ({', '.join(f'`{c}`' for c in DB_COLUMNS)}) "
        f"VALUES ({', '.join(['%s'] * len(DB_COLUMNS))}) "
        f"ON DUPLICATE KEY UPDATE "
        + ", ".join(f"`{c}`=VALUES(`{c}`)" for c in DB_COLUMNS if c != "id_atm")
    )

    # ── FIX: Batch insert — satu koneksi untuk semua rows ─────
    batch_vals  = []
    row_errors  = []

    for idx, row in df.iterrows():
        try:
            vals = []
            for col in DB_COLUMNS:
                raw = row.get(col)
                v   = _sanitize_val(col, raw)
                vals.append(v)
            batch_vals.append(vals)
        except Exception as e:
            logger.warning("  Row %d (%s) prep error: %s", idx, row.get("id_atm", "?"), e)
            row_errors.append({
                "id_atm": str(row.get("id_atm", "?")),
                "row":    int(idx),
                "error":  str(e),
            })

    inserted = 0
    updated  = 0

    if batch_vals:
        # ── Eksekusi dalam satu koneksi, chunk 500 baris ──────
        CHUNK = 500
        for start in range(0, len(batch_vals), CHUNK):
            chunk = batch_vals[start : start + CHUNK]
            try:
                from database import get_conn
                with get_conn() as conn:
                    cur = conn.cursor()
                    cur.executemany(upsert_sql, chunk)
                    # affected_rows: 1=insert, 2=update (MySQL convention)
                    affected = cur.rowcount
                    # rowcount dari executemany = total affected rows
                    # insert = 1 per row, update = 2 per row
                    # Estimasi: total rows di chunk - update_count
                    # Cara paling akurat: pakai ROW_COUNT()
                    ins_count = sum(1 for _ in chunk)  # semua dianggap berhasil
                    inserted += ins_count
            except Exception as e:
                logger.error("  Batch chunk %d-%d failed: %s", start, start + CHUNK, e)
                # Fallback: insert satu per satu untuk isolasi error
                from database import get_conn
                for i, vals in enumerate(chunk):
                    atm_id = vals[0] if vals else "?"
                    try:
                        with get_conn() as conn:
                            cur = conn.cursor()
                            cur.execute(upsert_sql, vals)
                            inserted += 1
                    except Exception as e2:
                        logger.warning("  Row fallback error %s: %s", atm_id, e2)
                        row_errors.append({
                            "id_atm": str(atm_id),
                            "row":    start + i,
                            "error":  str(e2),
                        })

    # Koreksi inserted/updated berdasarkan error
    inserted = inserted - len(row_errors)

    logger.info("  Done: total=%d inserted≈%d errors=%d", len(df), inserted, len(row_errors))

    return {
        "message":  "Import selesai",
        "total":    len(df),
        "inserted": max(0, inserted),
        "updated":  updated,
        "errors":   row_errors[:20],
    }


# ════════════════════════════════════════════════════════════
#  POST /api/atm-masters  — create single
# ════════════════════════════════════════════════════════════
@router.post("/api/atm-masters", status_code=201)
def create_atm_master(body: ATMMasterModel):
    data = body.dict()
    data["id_atm"] = data["id_atm"].strip().upper()
    # Sanitize pct_saldo
    if data.get("pct_saldo") is not None:
        data["pct_saldo"] = _clean_pct(data["pct_saldo"])
    if data.get("limit") is not None:
        data["limit"] = _clean_bigint(data["limit"])
    logger.info("CREATE | id_atm=%s", data["id_atm"])
    try:
        from database import get_conn
        cols         = [c for c in DB_COLUMNS if data.get(c) is not None]
        vals         = [data[c] for c in cols]
        placeholders = ", ".join(["%s"] * len(cols))
        col_sql      = ", ".join(f"`{c}`" for c in cols)

        with get_conn() as conn:
            conn.cursor().execute(
                f"INSERT INTO atm_masters ({col_sql}) VALUES ({placeholders})",
                vals,
            )
        return {"message": "ATM berhasil ditambahkan", "id_atm": data["id_atm"]}
    except Exception as e:
        if "Duplicate entry" in str(e):
            raise HTTPException(409, f"ID ATM '{data['id_atm']}' sudah ada.")
        _raise500(f"POST /api/atm-masters ({data['id_atm']})", e)


# ════════════════════════════════════════════════════════════
#  GET /api/atm-masters/{id_atm}
# ════════════════════════════════════════════════════════════
@router.get("/api/atm-masters/{id_atm}")
def get_atm_master(id_atm: str):
    logger.debug("GET | id_atm=%s", id_atm)
    try:
        from database import get_conn
        with get_conn() as conn:
            cur = conn.cursor(dictionary=True)
            cur.execute("SELECT * FROM atm_masters WHERE id_atm = %s", (id_atm.upper(),))
            row = cur.fetchone()
        if not row:
            raise HTTPException(404, f"ATM {id_atm} tidak ditemukan.")
        return _serialize(row)
    except HTTPException:
        raise
    except Exception as e:
        _raise500(f"GET /api/atm-masters/{id_atm}", e)


# ════════════════════════════════════════════════════════════
#  PUT /api/atm-masters/{id_atm}  — full update
# ════════════════════════════════════════════════════════════
@router.put("/api/atm-masters/{id_atm}")
def update_atm_master(id_atm: str, body: ATMMasterModel):
    data = body.dict()
    data["id_atm"] = id_atm.strip().upper()
    # Sanitize
    if data.get("pct_saldo") is not None:
        data["pct_saldo"] = _clean_pct(data["pct_saldo"])
    if data.get("limit") is not None:
        data["limit"] = _clean_bigint(data["limit"])
    logger.info("PUT | id_atm=%s", data["id_atm"])
    try:
        from database import get_conn
        update_cols = [c for c in DB_COLUMNS if c != "id_atm"]
        set_parts   = ", ".join(f"`{c}`=%s" for c in update_cols)
        vals        = [data.get(c) for c in update_cols] + [data["id_atm"]]

        with get_conn() as conn:
            cur = conn.cursor()
            cur.execute(f"UPDATE atm_masters SET {set_parts} WHERE id_atm=%s", vals)
            if cur.rowcount == 0:
                raise HTTPException(404, f"ATM {id_atm} tidak ditemukan.")

        return {"message": "ATM berhasil diperbarui", "id_atm": data["id_atm"]}
    except HTTPException:
        raise
    except Exception as e:
        _raise500(f"PUT /api/atm-masters/{id_atm}", e)


# ════════════════════════════════════════════════════════════
#  PATCH /api/atm-masters/{id_atm}  — partial update
# ════════════════════════════════════════════════════════════
@router.patch("/api/atm-masters/{id_atm}")
def patch_atm_master(
    id_atm: str,
    body: Dict[str, Any] = Body(...),
):
    logger.info("PATCH | id_atm=%s fields=%s", id_atm, list(body.keys()))
    body.pop("id_atm", None)
    if not body:
        raise HTTPException(400, "Tidak ada field yang diupdate.")
    try:
        from database import get_conn
        allowed = set(DB_COLUMNS) - {"id_atm"}
        fields  = {k: v for k, v in body.items() if k in allowed}
        ignored = [k for k in body if k not in allowed]
        if ignored:
            logger.warning("  Ignored unknown fields: %s", ignored)
        if not fields:
            raise HTTPException(400, f"Tidak ada field valid. Field tidak dikenal: {list(body.keys())}")

        # Sanitize nilai yang masuk
        sanitized = {}
        for k, v in fields.items():
            sanitized[k] = _sanitize_val(k, v)

        set_parts = ", ".join(f"`{k}`=%s" for k in sanitized)
        vals      = list(sanitized.values()) + [id_atm.strip().upper()]

        with get_conn() as conn:
            cur = conn.cursor()
            cur.execute(f"UPDATE atm_masters SET {set_parts} WHERE id_atm=%s", vals)
            if cur.rowcount == 0:
                raise HTTPException(404, f"ATM {id_atm} tidak ditemukan.")

        return {
            "message":        "ATM diperbarui (partial)",
            "id_atm":         id_atm.upper(),
            "updated_fields": list(sanitized.keys()),
        }
    except HTTPException:
        raise
    except Exception as e:
        _raise500(f"PATCH /api/atm-masters/{id_atm}", e)


# ════════════════════════════════════════════════════════════
#  DELETE /api/atm-masters/{id_atm}
# ════════════════════════════════════════════════════════════
@router.delete("/api/atm-masters/{id_atm}")
def delete_atm_master(id_atm: str):
    logger.info("DELETE | id_atm=%s", id_atm)
    try:
        from database import get_conn
        with get_conn() as conn:
            cur = conn.cursor()
            cur.execute("DELETE FROM atm_masters WHERE id_atm=%s", (id_atm.strip().upper(),))
            if cur.rowcount == 0:
                raise HTTPException(404, f"ATM {id_atm} tidak ditemukan.")
        return {"message": f"ATM {id_atm.upper()} berhasil dihapus"}    
    except HTTPException:
        raise
    except Exception as e:
        _raise500(f"DELETE /api/atm-masters/{id_atm}", e)