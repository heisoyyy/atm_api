import asyncio
import io
import math
import re
import zipfile
from datetime import datetime
from typing import Optional
import traceback
import pandas as pd
from fastapi import FastAPI, UploadFile, File, BackgroundTasks, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, StreamingResponse
from pydantic import BaseModel

from config import (
    PROCESSED_CSV, PRED_CACHE, MODEL_PATH, FITUR_PATH, WILAYAH_LIST,
    AUTO_CASHPLAN_PCT, TRIGGER_CASHPLAN_PCT,
)
from processing import process_dataframe
from predictor import build_predictions, save_cache, load_cache
from trainer import train
from database import (
    upsert_predictions,
    get_predictions_from_db,
    bulk_insert_history,
    get_atm_history_from_db,
    add_to_cashplan,
    get_cashplan_list,
    update_cashplan_status,
    remove_cashplan_only,
    get_rekap_replacement,
    get_rekap_for_download,
    update_rekap_replacement,
    log_upload,
    upsert_notif_cashplan,
    get_notif_pending,
    approve_notif,
    dismiss_notif,
)


def _sanitize(obj):
    if isinstance(obj, float):
        if math.isnan(obj) or math.isinf(obj):
            return None
        return obj
    if isinstance(obj, dict):
        return {k: _sanitize(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_sanitize(i) for i in obj]
    return obj


app = FastAPI(
    title="Smart ATM Dashboard API",
    description="Backend monitoring & prediksi saldo ATM BRK Syariah — V7",
    version="7.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

from atm_masters_routes import router as masters_router
app.include_router(masters_router)

_train_state = {
    "status":       "idle",
    "progress":     0,
    "message":      "",
    "last_trained": None,
    "last_result":  None,
}


# ════════════════════════════════════════════════════════════════════════════════
#  HEALTH
# ════════════════════════════════════════════════════════════════════════════════

@app.get("/", tags=["Health"])
def root():
    return {
        "service":    "Smart ATM Dashboard API",
        "version":    "7.0.0",
        "status":     "running",
        "time":       datetime.now().isoformat(),
    }


@app.get("/api/health/db", tags=["Health"])
def health_db():
    try:
        from database import get_conn
        with get_conn() as conn:
            cur = conn.cursor()
            cur.execute("SELECT 1")
        return {"status": "ok", "message": "MySQL connected"}
    except Exception as e:
        return JSONResponse(status_code=503, content={"status": "error", "message": str(e)})


# ════════════════════════════════════════════════════════════════════════════════
#  STATUS
# ════════════════════════════════════════════════════════════════════════════════

@app.get("/api/status", tags=["Status"])
def get_status():
    has_data  = PROCESSED_CSV.exists()
    has_model = MODEL_PATH.exists() and FITUR_PATH.exists()
    has_cache = PRED_CACHE.exists()

    info = {
        "has_data":     has_data,
        "has_model":    has_model,
        "has_cache":    has_cache,
        "model_path":   str(MODEL_PATH) if has_model else None,
        "train_status": _train_state["status"],
        "last_trained": _train_state["last_trained"],
        "version":      "7.0.0",
        "config": {
            "AUTO_CASHPLAN_PCT":    AUTO_CASHPLAN_PCT,
            "TRIGGER_CASHPLAN_PCT": TRIGGER_CASHPLAN_PCT,
        },
    }

    if has_data:
        try:
            df = pd.read_csv(PROCESSED_CSV, usecols=['ID ATM', 'Tanggal'], low_memory=False)
            df['ID ATM'] = df['ID ATM'].astype(str).str.strip().str.upper()
            df = df[~df['ID ATM'].isin(['', 'NAN', 'NONE', 'NULL', 'ID ATM'])]
            info["total_rows"] = len(df)
            info["total_atm"]  = df['ID ATM'].nunique()
            info["date_range"] = {
                "from": str(df['Tanggal'].min()),
                "to":   str(df['Tanggal'].max()),
            }
        except Exception as e:
            info["data_error"] = str(e)

    if not has_data or not has_model or not has_cache:
        try:
            from database import get_conn
            with get_conn() as conn:
                cur = conn.cursor(dictionary=True)
                cur.execute("SELECT COUNT(*) AS cnt FROM predictions")
                pred_count = cur.fetchone()["cnt"]

                if pred_count > 0:
                    info["has_data"]  = True
                    info["has_cache"] = True
                    cur.execute("""
                        SELECT COUNT(*) AS total_atm, MAX(generated_at) AS generated_at
                        FROM predictions
                    """)
                    pred_row = cur.fetchone()
                    info["total_atm"]           = int(pred_row["total_atm"] or 0)
                    info["predictions_count"]   = pred_count
                    info["predictions_updated"] = (
                        pred_row["generated_at"].isoformat()
                        if pred_row.get("generated_at") else None
                    )

                cur.execute("SELECT COUNT(*) AS cnt FROM atm_history")
                hist_count = cur.fetchone()["cnt"]
                if hist_count > 0:
                    info["has_data"]   = True
                    info["total_rows"] = int(hist_count)
                    cur.execute("""
                        SELECT MIN(DATE(recorded_at)) AS date_from, MAX(DATE(recorded_at)) AS date_to
                        FROM atm_history
                    """)
                    date_row = cur.fetchone()
                    info["date_range"] = {
                        "from": str(date_row["date_from"]) if date_row["date_from"] else "-",
                        "to":   str(date_row["date_to"])   if date_row["date_to"]   else "-",
                    }

                cur.execute("SELECT COUNT(*) AS cnt FROM notif_cashplan WHERE status_notif='PENDING'")
                info["notif_pending"] = cur.fetchone()["cnt"]

                # ── Info ATM Master vs Upload ──────────────────────────────
                cur.execute("SELECT COUNT(*) AS cnt FROM atm_masters WHERE unit_pengisian='SSI'")
                info["atm_master_ssi_count"] = cur.fetchone()["cnt"]

        except Exception as e:
            info["db_error"] = str(e)

    if has_cache:
        try:
            cache = load_cache()
            info["predictions_count"]   = cache.get("count", 0)
            info["predictions_updated"] = cache.get("generated_at")
            info["total_atm"]           = cache.get("count", info.get("total_atm", 0))
        except Exception:
            pass

    return _sanitize(info)


# ════════════════════════════════════════════════════════════════════════════════
#  ATM MASTER vs MONITORING — endpoint baru untuk dashboard comparison
# ════════════════════════════════════════════════════════════════════════════════

@app.get("/api/dashboard/master-vs-monitoring", tags=["Dashboard"])
def master_vs_monitoring():
    """
    Perbandingan antara ATM di master (SSI) dan ATM yang termonitor (ada di predictions).
    Digunakan untuk fitur analisis di Dashboard.
    """
    try:
        from database import get_conn
        with get_conn() as conn:
            cur = conn.cursor(dictionary=True)

            # Semua ATM SSI di master
            cur.execute("""
                SELECT id_atm, lokasi_atm, wilayah, denom_options, `limit`, merk_atm
                FROM atm_masters
                WHERE unit_pengisian = 'SSI'
                ORDER BY wilayah, id_atm
            """)
            master_rows = cur.fetchall()

            # ATM yang ada di predictions (sudah pernah diupload)
            cur.execute("""
                SELECT id_atm, saldo, pct_saldo, status, last_update
                FROM predictions
            """)
            pred_rows = cur.fetchall()

            print("DEBUG MASTER ROWS:", len(master_rows))
            print("DEBUG PRED ROWS:", len(pred_rows))

            # cek contoh isi data pertama
            if master_rows:
                print("SAMPLE MASTER:", master_rows[0])
            if pred_rows:
                print("SAMPLE PRED:", pred_rows[0])

        master_ids = {r["id_atm"] for r in master_rows}
        pred_ids   = {r["id_atm"] for r in pred_rows}
        pred_map   = {r["id_atm"]: r for r in pred_rows}

        # ATM di master tapi belum pernah ada data upload
        not_monitored = [
            {
                "id_atm":       r["id_atm"],
                "lokasi_atm":   r["lokasi_atm"],
                "wilayah":      r["wilayah"],
                "denom_options":r["denom_options"],
                "limit":        r["limit"],
                "merk_atm":     r["merk_atm"],
                "reason":       "Belum ada data upload",
            }
            for r in master_rows if r["id_atm"] not in pred_ids
        ]

        # ATM di predictions tapi tidak ada di master SSI
        not_in_master = [
            {
                "id_atm":     r["id_atm"],
                "lokasi":     r["lokasi"],
                "wilayah":    r["wilayah"],
                "saldo":      r["saldo"],
                "pct_saldo":  r["pct_saldo"],
                "status":     r["status"],
                "last_update":str(r["last_update"]) if r["last_update"] else None,
                "reason":     "Tidak ditemukan di ATM Master SSI",
            }
            for r in pred_rows if r["id_atm"] not in master_ids
        ]

        # ATM yang normal (ada di keduanya)
        matched = len(master_ids & pred_ids)

        # Breakdown per wilayah
        wilayah_breakdown = {}
        for r in master_rows:
            w = r["wilayah"] or "Unknown"
            if w not in wilayah_breakdown:
                wilayah_breakdown[w] = {"master": 0, "monitored": 0, "not_monitored": 0}
            wilayah_breakdown[w]["master"] += 1
            if r["id_atm"] in pred_ids:
                wilayah_breakdown[w]["monitored"] += 1
            else:
                wilayah_breakdown[w]["not_monitored"] += 1

        return {
            "summary": {
                "total_master_ssi": len(master_ids),
                "total_monitored": len(pred_ids),
                "matched": matched,
                "not_monitored": len(not_monitored),
                "not_in_master": len(not_in_master),
                "coverage_pct": round(matched / max(len(master_ids), 1) * 100, 1),
            },
            "not_monitored": not_monitored,
            "not_in_master": not_in_master,
            "wilayah_breakdown": [
                {"wilayah": w, **v} for w, v in wilayah_breakdown.items()
            ],
        }
    

    except Exception as e:
        print("\n=== ERROR master-vs-monitoring ===")
        print("Error message:", str(e))
        traceback.print_exc()
        print("=================================\n")
        raise HTTPException(500, str(e))


# ════════════════════════════════════════════════════════════════════════════════
#  HELPERS — file parsing
# ════════════════════════════════════════════════════════════════════════════════

BULAN_ID = {
    'januari':'01','februari':'02','maret':'03','april':'04',
    'mei':'05','juni':'06','juli':'07','agustus':'08',
    'september':'09','oktober':'10','november':'11','desember':'12',
    'jan':'01','feb':'02','mar':'03','apr':'04','jun':'06',
    'jul':'07','agu':'08','agt':'08','sep':'09','okt':'10','nov':'11','des':'12',
}


def _extract_tanggal(path: str) -> Optional[str]:
    m = re.search(r'(\d{4})-(\d{2})-(\d{2})', path)
    if m: return f"{m.group(1)}-{m.group(2)}-{m.group(3)}"
    m = re.search(r'(\d{4})_(\d{2})_(\d{2})', path)
    if m: return f"{m.group(1)}-{m.group(2)}-{m.group(3)}"
    m = re.search(r'(\d{1,2})[-/](\d{2})[-/](\d{4})', path)
    if m: return f"{m.group(3)}-{m.group(2)}-{m.group(1).zfill(2)}"
    m = re.search(r'(\d{1,2})[\s_-]+([A-Za-z]+)[\s_-]+(\d{4})', path)
    if m:
        bln = BULAN_ID.get(m.group(2).lower())
        if bln: return f"{m.group(3)}-{bln}-{m.group(1).zfill(2)}"
    return None


def _extract_jam(basename: str) -> Optional[str]:
    base = re.sub(r'\.(csv|xlsx|xls)$', '', basename, flags=re.IGNORECASE).strip()
    matches = re.findall(r'(\d{1,2})[\.:](\d{2})', base)
    if matches:
        hh, _ = matches[-1]
        if 0 <= int(hh) <= 23:
            return f"{int(hh):02d}:00"
    m = re.search(r'(\d{1,2})$', base)
    if m:
        hh_int = int(m.group(1))
        if 0 <= hh_int <= 23:
            return f"{hh_int:02d}:00"
    return None


def _read_tabular(zf: zipfile.ZipFile, name: str) -> pd.DataFrame:
    lower = name.lower()
    with zf.open(name) as raw:
        data = raw.read()
    buf = io.BytesIO(data)
    if lower.endswith(".csv"):
        for enc in ("utf-8", "utf-8-sig", "latin-1", "cp1252"):
            try:
                buf.seek(0)
                return pd.read_csv(buf, encoding=enc)
            except UnicodeDecodeError:
                continue
        buf.seek(0)
        return pd.read_csv(buf, encoding="latin-1", errors="replace")
    elif lower.endswith(".xlsx"):
        return pd.read_excel(buf, engine="openpyxl")
    elif lower.endswith(".xls"):
        return pd.read_excel(buf, engine="xlrd")
    raise ValueError(f"Format tidak didukung: {name}")


def _normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize kolom dari file upload — hanya perlu ID ATM & Sisa Saldo."""
    df = df.copy()
    df.columns = [str(c).strip() for c in df.columns]
    col_rename = {}
    for col in df.columns:
        c = col.lower().strip()
        if 'id' in c and 'atm' in c:
            col_rename[col] = 'ID ATM'
        elif 'sisa' in c and 'saldo' in c:
            col_rename[col] = 'Sisa Saldo'
        elif c in ('tanggal', 'date', 'tgl'):
            col_rename[col] = 'Tanggal'
        elif c in ('jam', 'time', 'waktu'):
            col_rename[col] = 'Jam'
    return df.rename(columns=col_rename)


def _read_excel_or_csv(content: bytes, filename: str) -> pd.DataFrame:
    buf = io.BytesIO(content)
    fname = filename.lower()
    if fname.endswith('.csv'):
        for enc in ('utf-8', 'utf-8-sig', 'latin-1', 'cp1252'):
            try:
                buf.seek(0)
                return pd.read_csv(buf, encoding=enc)
            except UnicodeDecodeError:
                continue
        buf.seek(0)
        return pd.read_csv(buf, encoding='latin-1', errors='replace')
    elif fname.endswith('.xlsx') or fname.endswith('.xlsm'):
        buf.seek(0)
        return pd.read_excel(buf, engine='openpyxl')
    elif fname.endswith('.xls'):
        buf.seek(0)
        return pd.read_excel(buf, engine='xlrd')
    raise HTTPException(400, f"Format file tidak didukung: {filename}")


def _parse_zip(zip_bytes: bytes):
    SUPPORTED = {".csv", ".xlsx", ".xls"}
    frames, errors = [], []
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
        for name in zf.namelist():
            if name.endswith("/") or "/." in name or name.startswith(".__"):
                continue
            ext = ("." + name.rsplit(".", 1)[-1].lower()) if "." in name else ""
            if ext not in SUPPORTED:
                continue
            tanggal_str = _extract_tanggal(name)
            if not tanggal_str:
                errors.append(f"Skip (tanggal tidak terdeteksi): {name}")
                continue
            basename = name.split("/")[-1]
            jam_str  = _extract_jam(basename)
            if not jam_str:
                errors.append(f"Skip (jam tidak terdeteksi): {name}")
                continue
            try:
                df_file = _read_tabular(zf, name)
            except Exception as e:
                errors.append(f"Gagal baca {name}: {e}")
                continue
            df_file = _normalize_columns(df_file)
            if "ID ATM" not in df_file.columns or "Sisa Saldo" not in df_file.columns:
                errors.append(f"Skip (kolom ID ATM/Sisa Saldo tidak ditemukan): {name}")
                continue
            df_file["Tanggal"] = tanggal_str
            df_file["Jam"]     = jam_str
            frames.append(df_file)

    if not frames:
        detail = "; ".join(errors[:10]) if errors else "Tidak ada file valid di dalam ZIP."
        raise HTTPException(400, f"ZIP tidak mengandung file valid. {detail}")

    df_combined = pd.concat(frames, ignore_index=True)
    df_combined = df_combined.drop_duplicates(subset=["ID ATM", "Tanggal", "Jam"])
    return df_combined.sort_values(["ID ATM", "Tanggal", "Jam"]).reset_index(drop=True), errors


# ════════════════════════════════════════════════════════════════════════════════
#  ENRICH FROM ATM MASTER — INTI PERUBAHAN
#  Ambil data ATM dari atm_masters (unit_pengisian=SSI),
#  cocokkan dengan ID ATM dari file upload.
#  Hanya Sisa Saldo yang diambil dari file.
# ════════════════════════════════════════════════════════════════════════════════

def _enrich_from_master(df_upload: pd.DataFrame) -> tuple[pd.DataFrame, list, list]:
    """
    Input  : df_upload — minimal punya kolom: ID ATM, Sisa Saldo, Tanggal, Jam
    Output : (df_enriched, warnings, skipped_ids)

    - df_enriched : DataFrame lengkap siap masuk processing
    - warnings    : list string pesan warning untuk response
    - skipped_ids : list ID ATM yang tidak ditemukan di master SSI
    """
    from database import get_conn

    # Ambil semua ATM SSI dari master
    with get_conn() as conn:
        cur = conn.cursor(dictionary=True)
        cur.execute("""
            SELECT
                id_atm,
                merk_atm,
                lokasi_atm,
                alamat_atm,
                denom_options,
                lembar,
                wilayah,
                `limit`,
                nama_vendor
            FROM atm_masters
            WHERE unit_pengisian = 'SSI'
        """)
        master_rows = cur.fetchall()

    if not master_rows:
        raise HTTPException(500, "Tidak ada data ATM Master dengan unit_pengisian='SSI'. Import ATM Master terlebih dahulu.")

    # Buat dict id_atm → row master (case-insensitive)
    master_map = {r["id_atm"].strip().upper(): r for r in master_rows}

    # Normalize ID ATM dari file upload
    df_upload = df_upload.copy()
    df_upload["ID ATM"] = df_upload["ID ATM"].astype(str).str.strip().str.upper()
    df_upload = df_upload[~df_upload["ID ATM"].isin(["", "NAN", "NONE", "NULL", "ID ATM"])]

    upload_ids  = df_upload["ID ATM"].unique().tolist()
    matched_ids = []
    skipped_ids = []

    for atm_id in upload_ids:
        if atm_id in master_map:
            matched_ids.append(atm_id)
        else:
            skipped_ids.append(atm_id)

    warnings = []
    if skipped_ids:
        for sid in skipped_ids:
            warnings.append(f"ID ATM '{sid}' tidak ditemukan di ATM Master SSI — dilewati")

    if not matched_ids:
        raise HTTPException(
            400,
            f"Tidak ada ID ATM dari file yang cocok dengan ATM Master SSI. "
            f"ID dari file: {upload_ids[:10]}. Pastikan ATM Master sudah diimport."
        )

    # Filter hanya ID yang match
    df_matched = df_upload[df_upload["ID ATM"].isin(matched_ids)].copy()

    # Enrich dengan data dari master
    enriched_rows = []
    for _, row in df_matched.iterrows():
        atm_id  = row["ID ATM"]
        master  = master_map[atm_id]

        # Parse limit dari master
        try:
            limit_val = float(str(master["limit"]).replace(",", "").replace(".", "").strip()) if master["limit"] else 0
        except Exception:
            limit_val = 0

        # Wilayah → format untuk Vendor field (dipakai processing._clean_vendor)
        wilayah_raw = master.get("wilayah") or "Unknown"

        # Mapping wilayah → nomor vendor SSI (sesuai format lama)
        wilayah_vendor_map = {
            "Pekanbaru":     "1 - SSI Wilayah Pekanbaru",
            "Batam":         "2 - SSI Wilayah Batam",
            "Tanjung Pinang":"3 - SSI Wilayah Tanjung Pinang",
            "Tanjungpinang": "3 - SSI Wilayah Tanjung Pinang",
            "Dumai":         "4 - SSI Wilayah Dumai",
        }
        vendor_str = wilayah_vendor_map.get(wilayah_raw, f"1 - SSI Wilayah {wilayah_raw}")

        enriched_rows.append({
            "ID ATM":    atm_id,
            "Sisa Saldo": row["Sisa Saldo"],
            "Tanggal":   row.get("Tanggal", datetime.now().strftime("%Y-%m-%d")),
            "Jam":       row.get("Jam", datetime.now().strftime("%H:00")),
            # Data dari master
            "Merk ATM":  master.get("merk_atm") or "-",
            "Lokasi ATM":master.get("lokasi_atm") or "-",
            "Alamat ATM":master.get("alamat_atm") or "-",
            "Denom":     master.get("denom_options") or "100",
            "Limit":     limit_val,
            "Vendor":    vendor_str,
            "Wilayah":   wilayah_raw,
        })

    df_enriched = pd.DataFrame(enriched_rows)

    return df_enriched, warnings, skipped_ids


# ════════════════════════════════════════════════════════════════════════════════
#  UPLOAD
# ════════════════════════════════════════════════════════════════════════════════

@app.post("/api/upload", tags=["Data"])
async def upload_data(
    background_tasks: BackgroundTasks,
    file: UploadFile = File(...),
    retrain: bool = Query(True),
):
    fname   = file.filename or ""
    content = await file.read()
    parse_warnings = []
    skipped_ids    = []

    # ── 1. Baca file → ambil ID ATM + Sisa Saldo saja ────────────────────────
    if fname.lower().endswith(".zip"):
        df_raw, parse_warnings = _parse_zip(content)
        # df_raw dari ZIP sudah ada Tanggal & Jam dari nama file
    elif fname.lower().endswith((".xlsx", ".xls", ".xlsm")):
        df_raw = _read_excel_or_csv(content, fname)
        df_raw = _normalize_columns(df_raw)
        _now = datetime.now()
        if "Tanggal" not in df_raw.columns:
            df_raw["Tanggal"] = _now.strftime("%Y-%m-%d")
        if "Jam" not in df_raw.columns:
            df_raw["Jam"] = _now.strftime("%H:00")
    elif fname.lower().endswith(".csv"):
        df_raw = _read_excel_or_csv(content, fname)
        df_raw = _normalize_columns(df_raw)
        _now = datetime.now()
        if "Tanggal" not in df_raw.columns:
            df_raw["Tanggal"] = _now.strftime("%Y-%m-%d")
        if "Jam" not in df_raw.columns:
            df_raw["Jam"] = _now.strftime("%H:00")
    else:
        raise HTTPException(400, f"Format tidak didukung: {fname}. Gunakan ZIP, XLSX, atau CSV.")

    if df_raw.empty:
        raise HTTPException(400, "File kosong atau tidak ada data yang bisa dibaca.")

    # Validasi kolom minimal
    for col in ["ID ATM", "Sisa Saldo"]:
        if col not in df_raw.columns:
            raise HTTPException(
                400,
                f"Kolom wajib '{col}' tidak ditemukan. "
                f"Kolom yang ada: {list(df_raw.columns)}. "
                f"File monitoring hanya memerlukan kolom: ID ATM dan Sisa Saldo."
            )

    # ── 2. Enrich dari ATM Master ─────────────────────────────────────────────
    try:
        df_new, master_warnings, skipped_ids = _enrich_from_master(df_raw)
        parse_warnings.extend(master_warnings)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(500, f"Gagal mengambil data dari ATM Master: {str(e)}")

    if df_new.empty:
        raise HTTPException(400, "Tidak ada data valid setelah pencocokan dengan ATM Master SSI.")

    # ── 3. Proses data (sama seperti sebelumnya) ──────────────────────────────
    try:
        if PROCESSED_CSV.exists():
            df_old = pd.read_csv(PROCESSED_CSV, low_memory=False)
            is_old_processed = "Avg Penarikan 6j" in df_old.columns
            df_old["ID ATM"] = df_old["ID ATM"].astype(str).str.strip().str.upper()
            df_new["ID ATM"] = df_new["ID ATM"].astype(str).str.strip().str.upper()

            if is_old_processed:
                tanggal_baru = set(df_new["Tanggal"].astype(str).str[:10].unique())
                raw_cols = ["ID ATM", "Sisa Saldo", "Limit", "Tanggal", "Jam",
                            "Merk ATM", "Lokasi ATM", "Alamat ATM", "Vendor", "Denom",
                            "Wilayah"]
                raw_cols_available = [c for c in raw_cols if c in df_old.columns]
                df_old_raw = df_old[raw_cols_available].copy()
                df_old_raw = df_old_raw[
                    ~df_old_raw["Tanggal"].astype(str).str[:10].isin(tanggal_baru)
                ]
                df_combined = pd.concat([df_old_raw, df_new], ignore_index=True)
                df_combined = df_combined.drop_duplicates(subset=["ID ATM", "Tanggal", "Jam"])
                df_final = process_dataframe(df_combined)
            else:
                tanggal_baru = set(df_new["Tanggal"].astype(str).str[:10].unique())
                df_old = df_old[
                    ~df_old["Tanggal"].astype(str).str[:10].isin(tanggal_baru)
                ]
                df_combined = pd.concat([df_old, df_new], ignore_index=True)
                df_combined = df_combined.drop_duplicates(subset=["ID ATM", "Tanggal", "Jam"])
                df_final = process_dataframe(df_combined)
        else:
            df_final = process_dataframe(df_new)

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(500, f"Gagal memproses data: {str(e)}")

    try:
        df_final.to_csv(PROCESSED_CSV, index=False)
    except Exception as e:
        raise HTTPException(500, f"Gagal menyimpan CSV: {str(e)}")

    def _clean_pred(p):
        return {
            k: (None if isinstance(v, float) and (math.isnan(v) or math.isinf(v)) else v)
            for k, v in p.items()
        }

    try:
        predictions = [_clean_pred(p) for p in build_predictions(df_final)]
        save_cache(predictions)
    except Exception as e:
        parse_warnings.append(f"[Prediction Warning] {str(e)}")
        predictions = []

    db_synced = False
    try:
        if predictions:
            upsert_predictions(predictions)
        bulk_insert_history(df_final)
        db_synced = True
        _sync_notif_from_predictions(predictions)
    except Exception as e:
        parse_warnings.append(f"[DB Warning] {str(e)}")

    try:
        log_upload(
            fname,
            "ZIP" if fname.lower().endswith(".zip") else "CSV/Excel",
            len(df_final),
            df_final["ID ATM"].nunique() if "ID ATM" in df_final.columns else 0,
            matched=int(df_new["ID ATM"].nunique()) if "ID ATM" in df_new.columns else 0,
            skipped=len(skipped_ids),
            predictions=len(predictions),
            retrain=retrain,
            notes=f"Skipped IDs: {skipped_ids}" if skipped_ids else None,
        )
    except Exception as e:
        parse_warnings.append(f"[Log Warning] {str(e)}")

    resp = {
        "message":      "Upload berhasil",
        "version":      "V7",
        "format":       "ZIP" if fname.lower().endswith(".zip") else "Excel/CSV",
        "total_file":   int(df_raw["ID ATM"].nunique()) if "ID ATM" in df_raw.columns else 0,
        "matched":      int(df_new["ID ATM"].nunique()) if "ID ATM" in df_new.columns else 0,
        "skipped":      len(skipped_ids),
        "skipped_ids":  skipped_ids,
        "rows":         len(df_final),
        "atm_count":    int(df_final["ID ATM"].nunique()) if "ID ATM" in df_final.columns else 0,
        "predictions":  len(predictions),
        "db_synced":    db_synced,
        "source":       "Data ATM (Merk, Lokasi, Limit, Denom, Wilayah) diambil dari ATM Master SSI",
    }
    if parse_warnings:
        resp["warnings"] = parse_warnings[:15]

    if retrain:
        background_tasks.add_task(_do_retrain, df_final)
        resp["retrain"] = "Dimulai di background — cek GET /api/train/status"

    return resp


def _sync_notif_from_predictions(predictions: list):
    from database import get_conn
    with get_conn() as conn:
        cur = conn.cursor(dictionary=True)
        cur.execute("SELECT id_atm FROM cashplan WHERE status_cashplan='PENDING'")
        pending_ids = {r["id_atm"] for r in cur.fetchall()}

    for p in predictions:
        atm_id = p.get("id_atm", "")
        pct    = float(p.get("pct_saldo", 100) or 100)

        if atm_id in pending_ids:
            continue

        if pct <= AUTO_CASHPLAN_PCT * 100:
            try:
                add_to_cashplan({**p, "added_by": "system"})
            except Exception:
                pass
        elif pct <= TRIGGER_CASHPLAN_PCT * 100:
            try:
                upsert_notif_cashplan(p)
            except Exception:
                pass


# ════════════════════════════════════════════════════════════════════════════════
#  TRAINING
# ════════════════════════════════════════════════════════════════════════════════

@app.post("/api/train", tags=["Training"])
async def trigger_train(background_tasks: BackgroundTasks):
    if _train_state["status"] == "running":
        raise HTTPException(409, "Training sedang berjalan.")
    if not PROCESSED_CSV.exists():
        raise HTTPException(404, "Belum ada data. Upload terlebih dahulu.")
    df = pd.read_csv(PROCESSED_CSV, low_memory=False)
    background_tasks.add_task(_do_retrain, df)
    return {"message": "Training V7 dimulai", "monitor": "GET /api/train/status"}


@app.get("/api/train/status", tags=["Training"])
def get_train_status():
    return _train_state


# ════════════════════════════════════════════════════════════════════════════════
#  PREDICTIONS
# ════════════════════════════════════════════════════════════════════════════════

@app.get("/api/predictions", tags=["Predictions"])
def get_predictions(
    wilayah: Optional[str] = Query(None),
    status:  Optional[str] = Query(None),
    tipe:    Optional[str] = Query(None),
    limit:   int = Query(100, ge=1, le=500),
    offset:  int = Query(0, ge=0),
):
    try:
        result = get_predictions_from_db(wilayah=wilayah, status=status, tipe=tipe, limit=limit, offset=offset)
        return _sanitize({"generated_at": result["generated_at"], "total": result["total"],
                          "offset": offset, "limit": limit, "data": result["data"]})
    except Exception:
        cache = load_cache()
        if cache is None:
            raise HTTPException(404, "Belum ada prediksi. Upload data terlebih dahulu.")
        data = cache["data"]
        if wilayah: data = [d for d in data if wilayah.lower() in d.get("wilayah", "").lower()]
        if status:  data = [d for d in data if d.get("status", "").lower() == status.lower()]
        if tipe:    data = [d for d in data if d.get("tipe", "").upper() == tipe.upper()]
        return _sanitize({"generated_at": cache.get("generated_at"), "total": len(data),
                          "offset": offset, "limit": limit, "data": data[offset: offset + limit]})


@app.get("/api/predictions/{atm_id}", tags=["Predictions"])
def get_prediction_detail(atm_id: str):
    """
    FIX v7.1: LEFT JOIN ke atm_masters agar lokasi/wilayah/limit/tipe/denom_options
    ikut dalam response — dibutuhkan oleh modal Konfirmasi Cash Plan (manual).
    Pakai LEFT JOIN (bukan INNER) agar ATM yang belum ada di master tetap bisa dibuka.
    """
    try:
        from database import get_conn, _fmt_pred

        _JOIN_DETAIL = """
            SELECT
                p.*,
                COALESCE(m.lokasi_atm, '-')   AS lokasi,
                COALESCE(m.wilayah,    '-')   AS wilayah,
                COALESCE(m.denom_options, '100000') AS denom_options,
                COALESCE(m.`limit`, 0)         AS `limit`,
                m.merk_atm,
                m.alamat_atm,
                m.nama_vendor,
                m.kode_cabang,
                UPPER(LEFT(p.id_atm, 3))       AS tipe
            FROM predictions p
            LEFT JOIN atm_masters m ON p.id_atm = m.id_atm
            WHERE p.id_atm = %s
        """

        with get_conn() as conn:
            cur = conn.cursor(dictionary=True)
            cur.execute(_JOIN_DETAIL, (atm_id.strip().upper(),))
            row = cur.fetchone()

        if not row:
            raise HTTPException(404, f"ATM {atm_id} tidak ditemukan di database.")

        return _sanitize(_fmt_pred(row))

    except HTTPException:
        raise
    except Exception:
        # fallback cache
        cache = load_cache()
        if cache is None:
            raise HTTPException(404, "Belum ada prediksi.")
        match = [d for d in cache["data"] if d["id_atm"] == atm_id.strip().upper()]
        if not match:
            raise HTTPException(404, f"ATM {atm_id} tidak ditemukan.")
        return _sanitize(match[0])

# ════════════════════════════════════════════════════════════════════════════════
#  ALERTS & SUMMARY
# ════════════════════════════════════════════════════════════════════════════════

@app.get("/api/alerts", tags=["Alerts"])
def get_alerts(level: Optional[str] = Query(None)):
    alert_statuses = ["BONGKAR", "AWAS"]
    if level:
        lvl = level.upper()
        if lvl not in alert_statuses:
            raise HTTPException(400, f"Level harus BONGKAR atau AWAS, bukan '{level}'")
        alert_statuses = [lvl]
    try:
        results = []
        for st in alert_statuses:
            r = get_predictions_from_db(status=st, limit=500)
            results.extend(r["data"])
        results.sort(key=lambda x: x.get("skor_urgensi", 0) or 0, reverse=True)
        gen_at = results[0].get("generated_at") if results else None
        return _sanitize({"generated_at": gen_at, "total_alerts": len(results),
                          "breakdown": {s: sum(1 for d in results if d["status"] == s) for s in alert_statuses},
                          "data": results})
    except Exception:
        cache = load_cache()
        if cache is None:
            raise HTTPException(404, "Belum ada prediksi.")
        alerts = [d for d in cache["data"] if d.get("status") in alert_statuses]
        alerts.sort(key=lambda x: x.get("skor_urgensi", 0) or 0, reverse=True)
        return _sanitize({"generated_at": cache.get("generated_at"), "total_alerts": len(alerts),
                          "breakdown": {s: sum(1 for d in alerts if d.get("status") == s) for s in alert_statuses},
                          "data": alerts})


@app.get("/api/summary", tags=["Summary"])
def get_summary():
    try:
        from database import get_conn
        with get_conn() as conn:
            cur = conn.cursor(dictionary=True)
            cur.execute("""
                SELECT COUNT(*) AS total_atm,
                    SUM(status='BONGKAR') AS bongkar, SUM(status='AWAS') AS awas,
                    SUM(status='PERLU PANTAU') AS perlu_pantau, SUM(status='AMAN') AS aman,
                    SUM(status='OVERFUND') AS overfund, SUM(atm_sepi=1) AS atm_sepi,
                    ROUND(AVG(pct_saldo),1) AS avg_pct_saldo, MAX(generated_at) AS generated_at
                FROM predictions
            """)
            ov_row = cur.fetchone()
            cur.execute("""
                SELECT wilayah, COUNT(*) AS total,
                    SUM(status='BONGKAR') AS bongkar, SUM(status='AWAS') AS awas,
                    SUM(status='PERLU PANTAU') AS perlu_pantau, SUM(status='AMAN') AS aman,
                    SUM(status='OVERFUND') AS overfund, SUM(atm_sepi=1) AS atm_sepi,
                    ROUND(AVG(pct_saldo),1) AS avg_pct_saldo, ROUND(AVG(skor_urgensi),1) AS avg_skor
                FROM predictions GROUP BY wilayah ORDER BY AVG(skor_urgensi) DESC
            """)
            wilayah_rows = cur.fetchall()
            cur.execute("SELECT status, COUNT(*) AS cnt FROM predictions GROUP BY status")
            status_breakdown = {r["status"]: r["cnt"] for r in cur.fetchall()}

            # Tambahan: total master SSI
            cur.execute("SELECT COUNT(*) AS cnt FROM atm_masters WHERE unit_pengisian='SSI'")
            master_ssi_count = cur.fetchone()["cnt"]

        def _i(v): return int(v or 0)
        def _f(v): return float(v or 0.0)

        overall = {
            "total_atm":         _i(ov_row["total_atm"]),
            "bongkar":           _i(ov_row["bongkar"]),
            "awas":              _i(ov_row["awas"]),
            "perlu_pantau":      _i(ov_row["perlu_pantau"]),
            "aman":              _i(ov_row["aman"]),
            "overfund":          _i(ov_row["overfund"]),
            "atm_sepi":          _i(ov_row["atm_sepi"]),
            "avg_pct_saldo":     _f(ov_row["avg_pct_saldo"]),
            "status_breakdown":  status_breakdown,
            "kritis":            _i(ov_row["bongkar"]),
            "total_master_ssi":  int(master_ssi_count),
        }
        gen_at = ov_row["generated_at"].isoformat() if ov_row.get("generated_at") else None
        per_wilayah = [{
            "wilayah": w["wilayah"], "total": _i(w["total"]),
            "bongkar": _i(w["bongkar"]), "awas": _i(w["awas"]),
            "perlu_pantau": _i(w["perlu_pantau"]), "aman": _i(w["aman"]),
            "overfund": _i(w["overfund"]), "atm_sepi": _i(w["atm_sepi"]),
            "avg_pct_saldo": _f(w["avg_pct_saldo"]), "avg_skor": _f(w["avg_skor"]),
        } for w in wilayah_rows]

        return _sanitize({"generated_at": gen_at, "overall": overall, "per_wilayah": per_wilayah})

    except Exception:
        cache = load_cache()
        if cache is None:
            raise HTTPException(404, "Belum ada prediksi.")
        data = cache["data"]
        def _n(v, default=0.0):
            if v is None: return default
            try:
                f = float(v)
                return default if (math.isnan(f) or math.isinf(f)) else f
            except: return default
        status_counts = {}
        for d in data:
            status_counts[d.get("status", "NO DATA")] = status_counts.get(d.get("status", "NO DATA"), 0) + 1
        pct_values = [_n(d.get("pct_saldo")) for d in data]
        overall = {
            "total_atm": len(data), "bongkar": status_counts.get("BONGKAR", 0),
            "awas": status_counts.get("AWAS", 0), "perlu_pantau": status_counts.get("PERLU PANTAU", 0),
            "aman": status_counts.get("AMAN", 0), "overfund": status_counts.get("OVERFUND", 0),
            "atm_sepi": sum(1 for d in data if d.get("atm_sepi")),
            "avg_pct_saldo": round(sum(pct_values) / max(len(pct_values), 1), 1),
            "status_breakdown": status_counts, "kritis": status_counts.get("BONGKAR", 0),
            "total_master_ssi": 0,
        }
        wilayah_map = {}
        for d in data:
            wilayah_map.setdefault(d.get("wilayah", "Unknown"), []).append(d)
        per_wilayah = []
        for w, items in wilayah_map.items():
            pcts = [_n(i.get("pct_saldo")) for i in items]
            per_wilayah.append({"wilayah": w, "total": len(items),
                "bongkar": sum(1 for i in items if i.get("status") == "BONGKAR"),
                "awas": sum(1 for i in items if i.get("status") == "AWAS"),
                "perlu_pantau": sum(1 for i in items if i.get("status") == "PERLU PANTAU"),
                "aman": sum(1 for i in items if i.get("status") == "AMAN"),
                "overfund": sum(1 for i in items if i.get("status") == "OVERFUND"),
                "atm_sepi": sum(1 for i in items if i.get("atm_sepi")),
                "avg_pct_saldo": round(sum(pcts) / max(len(pcts), 1), 1)})
        return _sanitize({"generated_at": cache.get("generated_at"), "overall": overall, "per_wilayah": per_wilayah})


# ════════════════════════════════════════════════════════════════════════════════
#  HISTORY / ATM LIST / WILAYAH
# ════════════════════════════════════════════════════════════════════════════════

@app.get("/api/history/{atm_id}", tags=["History"])
def get_atm_history(atm_id: str, last_n_days: int = Query(7, ge=1, le=30)):
    try:
        result = get_atm_history_from_db(atm_id.strip().upper(), last_n_days)
        if result is None:
            raise HTTPException(404, f"ATM {atm_id} tidak ditemukan.")
        return _sanitize(result)
    except HTTPException:
        raise
    except Exception:
        raise HTTPException(500, "Gagal mengambil history ATM.")


@app.get("/api/atm-list", tags=["ATM"])
def get_atm_list(wilayah: Optional[str] = Query(None), status: Optional[str] = Query(None)):
    try:
        from database import get_conn
        where, params = [], []
        if wilayah: where.append("wilayah LIKE %s"); params.append(f"%{wilayah}%")
        if status:  where.append("LOWER(status) = %s"); params.append(status.lower())
        where_sql = ("WHERE " + " AND ".join(where)) if where else ""
        with get_conn() as conn:
            cur = conn.cursor(dictionary=True)
            cur.execute(
                f"SELECT id_atm, lokasi, wilayah, tipe, denom_options, pct_saldo, status, skor_urgensi "
                f"FROM predictions {where_sql} ORDER BY skor_urgensi DESC", params)
            rows = cur.fetchall()
        return {"total": len(rows), "data": rows}
    except Exception:
        cache = load_cache()
        if cache is None:
            raise HTTPException(404, "Belum ada data.")
        data = cache["data"]
        if wilayah: data = [d for d in data if wilayah.lower() in d.get("wilayah", "").lower()]
        if status:  data = [d for d in data if d.get("status", "").lower() == status.lower()]
        return {"total": len(data), "data": [{k: d[k] for k in ["id_atm","lokasi","wilayah","tipe","denom_options","pct_saldo","status","skor_urgensi"] if k in d} for d in data]}


@app.get("/api/wilayah", tags=["Wilayah"])
def get_wilayah():
    try:
        from database import get_conn
        with get_conn() as conn:
            cur = conn.cursor(dictionary=True)
            cur.execute("SELECT id_atm,lokasi,tipe,denom_options,pct_saldo,status,skor_urgensi,atm_sepi,wilayah FROM predictions ORDER BY skor_urgensi DESC")
            rows = cur.fetchall()
        result = {}
        for d in rows:
            d["atm_sepi"] = bool(d.get("atm_sepi", 0))
            result.setdefault(d["wilayah"], []).append(d)
        return _sanitize({"wilayah_list": list(result.keys()), "data": result})
    except Exception:
        cache = load_cache()
        if cache is None:
            raise HTTPException(404, "Belum ada prediksi.")
        result = {}
        for d in cache["data"]:
            w = d.get("wilayah", "Unknown")
            result.setdefault(w, []).append({k: d[k] for k in ["id_atm","lokasi","tipe","denom_options","pct_saldo","status","skor_urgensi","atm_sepi"] if k in d})
        return _sanitize({"wilayah_list": list(result.keys()), "data": result})


# ════════════════════════════════════════════════════════════════════════════════
#  CASHPLAN
# ════════════════════════════════════════════════════════════════════════════════

class CashplanAddRequest(BaseModel):
    id_atm:        str
    lokasi:        str = "-"
    wilayah:       str = "-"
    tipe:          str = "-"
    denom_options: str = "100000"
    saldo:         int = 0
    limit:         int = 0
    pct_saldo:     float = 0
    status:        str = "AWAS"
    tgl_isi:       Optional[str] = None
    jam_isi:       Optional[str] = None
    est_jam:       Optional[float] = None
    skor_urgensi:  float = 0
    denom:         str = "100000"
    added_by:      str = "user"


class CashplanStatusUpdate(BaseModel):
    status:      str
    keterangan:  Optional[str] = None
    denom:       Optional[str] = None


@app.post("/api/cashplan", tags=["CashPlan"])
def api_add_cashplan(req: CashplanAddRequest):
    try:
        data = req.dict()
        # denom sudah string, add_to_cashplan expects string — langsung pass
        cp_id = add_to_cashplan(data)
        return {"message": "Berhasil ditambahkan", "cashplan_id": cp_id}
    except Exception as e:
        raise HTTPException(500, str(e))


@app.get("/api/cashplan", tags=["CashPlan"])
def api_get_cashplan(status: str = Query("PENDING")):
    try:
        items = get_cashplan_list(status.upper())
        return {"total": len(items), "data": items}
    except Exception as e:
        raise HTTPException(500, str(e))


@app.patch("/api/cashplan/{cashplan_id}/status", tags=["CashPlan"])
def api_update_cashplan_status(cashplan_id: int, body: CashplanStatusUpdate):
    STATUS_MAP = {"DONE": "DONE", "REMOVED": "REMOVED", "SELESAI": "DONE", "BATAL": "REMOVED"}
    mapped = STATUS_MAP.get(body.status.upper())
    if not mapped:
        raise HTTPException(400, f"Status harus salah satu dari: {set(STATUS_MAP.keys())}")
    try:
        result = update_cashplan_status(cashplan_id, mapped, keterangan=body.keterangan, denom=body.denom)
        return {"message": f"Status diubah ke {body.status.upper()}", **result}
    except ValueError as e:
        raise HTTPException(404, str(e))
    except Exception as e:
        raise HTTPException(500, str(e))


@app.delete("/api/cashplan/{cashplan_id}", tags=["CashPlan"])
def api_remove_cashplan(cashplan_id: int):
    try:
        remove_cashplan_only(cashplan_id)
        return {"message": "Cashplan dihapus dari antrian", "cashplan_id": cashplan_id}
    except ValueError as e:
        raise HTTPException(404, str(e))
    except Exception as e:
        raise HTTPException(500, str(e))


# ════════════════════════════════════════════════════════════════════════════════
#  NOTIF CASHPLAN
# ════════════════════════════════════════════════════════════════════════════════

@app.get("/api/notif-cashplan", tags=["Notif"])
def api_get_notif():
    try:
        items = get_notif_pending()
        return {"total": len(items), "data": items}
    except Exception as e:
        raise HTTPException(500, str(e))


@app.post("/api/notif-cashplan/{notif_id}/approve", tags=["Notif"])
def api_approve_notif(notif_id: int):
    try:
        cp_id = approve_notif(notif_id)
        return {"message": "ATM berhasil ditambahkan ke cashplan", "cashplan_id": cp_id}
    except ValueError as e:
        raise HTTPException(404, str(e))
    except Exception as e:
        raise HTTPException(500, str(e))


@app.post("/api/notif-cashplan/{notif_id}/dismiss", tags=["Notif"])
def api_dismiss_notif(notif_id: int):
    try:
        dismiss_notif(notif_id)
        return {"message": "Notif berhasil di-dismiss"}
    except ValueError as e:
        raise HTTPException(404, str(e))
    except Exception as e:
        raise HTTPException(500, str(e))


@app.post("/api/notif-cashplan/dismiss-all", tags=["Notif"])
def api_dismiss_all_notif():
    try:
        from database import get_conn
        with get_conn() as conn:
            conn.cursor().execute(
                "UPDATE notif_cashplan SET status_notif='DISMISSED', decided_at=%s WHERE status_notif='PENDING'",
                (datetime.now(),)
            )
        return {"message": "Semua notif berhasil di-dismiss"}
    except Exception as e:
        raise HTTPException(500, str(e))


# ════════════════════════════════════════════════════════════════════════════════
#  REKAP REPLACEMENT
# ════════════════════════════════════════════════════════════════════════════════

class RekapUpdateRequest(BaseModel):
    tgl_isi:      Optional[str] = None
    jam_cash_in:  Optional[str] = None
    jam_cash_out: Optional[str] = None
    denom:        Optional[int] = None


@app.get("/api/rekap-replacement", tags=["Rekap"])
def api_get_rekap(
    bulan:   Optional[str] = Query(None),
    tahun:   Optional[int] = Query(None),
    wilayah: Optional[str] = Query(None),
):
    try:
        items = get_rekap_replacement(bulan=bulan, tahun=tahun, wilayah=wilayah)
        return _sanitize({"total": len(items), "data": items})
    except Exception as e:
        raise HTTPException(500, str(e))


@app.patch("/api/rekap-replacement/{rekap_id}", tags=["Rekap"])
def api_update_rekap(rekap_id: int, body: RekapUpdateRequest):
    try:
        result = update_rekap_replacement(
            rekap_id, tgl_isi=body.tgl_isi,
            jam_cash_in=body.jam_cash_in, jam_cash_out=body.jam_cash_out, denom=body.denom,
        )
        return {"message": "Rekap berhasil disimpan", **result}
    except Exception as e:
        raise HTTPException(500, str(e))


@app.get("/api/rekap-replacement/download", tags=["Rekap"])
def api_download_rekap(
    wilayah: Optional[str] = Query(None),
    bulan:   Optional[str] = Query(None),
    tahun:   Optional[int] = Query(None),
    format:  str = Query("xlsx"),
):
    try:
        rows = get_rekap_for_download(wilayah=wilayah, bulan=bulan, tahun=tahun)
    except Exception as e:
        raise HTTPException(500, str(e))

    if not rows:
        raise HTTPException(404, "Tidak ada data rekap untuk filter yang dipilih.")

    df = pd.DataFrame(rows)
    col_map = {
        "id_atm": "ID ATM", "lokasi": "Lokasi ATM", "wilayah": "Wilayah",
        "tipe": "Tipe", "denom_options": "Denom Tersedia",
        "saldo_awal": "Saldo Awal (Rp)", "limit": "Limit (Rp)",
        "jumlah_isi": "Jumlah Isi (Rp)", "denom": "Denom Dipakai",
        "lembar": "Lembar", "status_awal": "Status Awal", "status_done": "Status",
        "keterangan": "Keterangan", "tgl_isi": "Tanggal Isi", "jam_isi": "Jam Isi",
        "jam_cash_in": "Jam Cash In", "jam_cash_out": "Jam Cash Out",
        "done_at": "Waktu Selesai", "bulan": "Bulan", "tahun": "Tahun",
    }
    df = df.rename(columns={k: v for k, v in col_map.items() if k in df.columns})

    wilayah_label = (wilayah or "semua").lower().replace(" ", "_")
    bulan_label   = f"_{bulan.lower()}" if bulan else ""
    tahun_label   = f"_{tahun}" if tahun else ""
    filename      = f"rekap_{wilayah_label}{bulan_label}{tahun_label}"

    buf = io.BytesIO()
    if format.lower() == "csv":
        df.to_csv(buf, index=False, encoding="utf-8-sig")
        buf.seek(0)
        return StreamingResponse(buf, media_type="text/csv",
            headers={"Content-Disposition": f'attachment; filename="{filename}.csv"'})
    else:
        with pd.ExcelWriter(buf, engine="openpyxl") as writer:
            df.to_excel(writer, index=False, sheet_name="Rekap Replacement")
        buf.seek(0)
        return StreamingResponse(buf,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={"Content-Disposition": f'attachment; filename="{filename}.xlsx"'})


# ════════════════════════════════════════════════════════════════════════════════
#  ADMIN / DEBUG
# ════════════════════════════════════════════════════════════════════════════════

@app.post("/api/clear-cache", tags=["Admin"])
async def clear_cache():
    if PRED_CACHE.exists():
        PRED_CACHE.unlink()
        return {"message": "Cache dibersihkan"}
    return {"message": "Cache sudah kosong"}


@app.delete("/api/data", tags=["Admin"])
def reset_all():
    removed = []
    for p in [PROCESSED_CSV, PRED_CACHE, MODEL_PATH, FITUR_PATH]:
        if p.exists():
            p.unlink()
            removed.append(str(p))
    _train_state.update({"status": "idle", "progress": 0, "message": "", "last_trained": None, "last_result": None})
    return {"message": "Reset berhasil", "removed_files": removed}


@app.get("/api/upload-log/today", tags=["Admin"])
def api_get_upload_log_today():
    try:
        from database import get_conn
        with get_conn() as conn:
            cur = conn.cursor(dictionary=True)
            cur.execute("""SELECT id, filename, format, total_rows, atm_count, predictions, retrain, uploaded_at, status, notes
                FROM upload_log WHERE DATE(uploaded_at)=CURDATE() ORDER BY uploaded_at DESC""")
            rows = cur.fetchall()
        for r in rows:
            if r.get("uploaded_at"): r["uploaded_at"] = r["uploaded_at"].isoformat()
            r["retrain"] = bool(r.get("retrain", 0))
        return {"total": len(rows), "data": rows}
    except Exception as e:
        raise HTTPException(500, str(e))


@app.get("/api/upload-log", tags=["Admin"])
def api_get_upload_log(limit: int = Query(50, ge=1, le=200)):
    try:
        from database import get_conn
        with get_conn() as conn:
            cur = conn.cursor(dictionary=True)
            cur.execute("""SELECT id, filename, format, total_rows, atm_count, predictions, retrain, uploaded_at, status, notes
                FROM upload_log ORDER BY uploaded_at DESC LIMIT %s""", (limit,))
            rows = cur.fetchall()
        for r in rows:
            if r.get("uploaded_at"): r["uploaded_at"] = r["uploaded_at"].isoformat()
            r["retrain"] = bool(r.get("retrain", 0))
        return {"total": len(rows), "data": rows}
    except Exception as e:
        raise HTTPException(500, str(e))


# ════════════════════════════════════════════════════════════════════════════════
#  BACKGROUND TASK — training
# ════════════════════════════════════════════════════════════════════════════════

async def _do_retrain(df: pd.DataFrame):
    _train_state.update({"status": "running", "progress": 0, "message": "Memulai training V7..."})

    def _cb(pct, msg):
        _train_state["progress"] = pct
        _train_state["message"]  = msg

    try:
        if "Avg Penarikan 6j" not in df.columns:
            _cb(5, "Processing data...")
            df = process_dataframe(df)

        loop   = asyncio.get_event_loop()
        result = await loop.run_in_executor(None, lambda: train(df, _cb))

        _cb(95, "Rebuild prediksi cache V7...")
        predictions = build_predictions(df)
        save_cache(predictions)

        try:
            upsert_predictions(predictions)
            _sync_notif_from_predictions(predictions)
        except Exception as e:
            _train_state["message"] += f" | DB sync warning: {e}"

        _train_state.update({
            "status":       "done",
            "progress":     100,
            "message":      f"Training V7 selesai ✅ MAE={result.get('mae_avg')} jam | R²={result.get('r2_avg')}",
            "last_trained": datetime.now().isoformat(),
            "last_result":  result,
        })
    except Exception as e:
        _train_state.update({"status": "error", "progress": 0, "message": f"Error training: {str(e)}"})