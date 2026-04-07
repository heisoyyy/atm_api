"""
main.py — Smart ATM Dashboard API v6
=====================================
Status V6: AMAN >25% | AWAS 20-25% | BONGKAR <20%

Endpoints:
  GET  /                        health check
  GET  /api/status              status model + data
  POST /api/upload              upload CSV/Excel/ZIP, proses, simpan
  POST /api/train               trigger retrain XGBoost (background)
  GET  /api/train/status        progress retrain
  GET  /api/predictions         seluruh prediksi (+ filter)
  GET  /api/predictions/{id}    detail 1 ATM
  GET  /api/summary             ringkasan per wilayah
  GET  /api/alerts              ATM dengan status BONGKAR / AWAS
  GET  /api/history/{id}        data historis saldo 1 ATM
  GET  /api/wilayah             list wilayah + statistik
  GET  /api/debug/atm_ids       debug ID ATM & vendor
  DELETE /api/data              hapus semua data & model (reset)
"""

import asyncio
import io
import math
import re
import zipfile
from datetime import datetime
from typing import Optional

import pandas as pd
from fastapi import FastAPI, UploadFile, File, BackgroundTasks, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from config import PROCESSED_CSV, PRED_CACHE, MODEL_PATH, FITUR_PATH, WILAYAH_LIST
from processing import process_dataframe
from predictor import build_predictions, save_cache, load_cache
from trainer import train


# ── App ──────────────────────────────────────────────

def _sanitize(obj):
    """Rekursif ganti NaN/Inf dengan None agar JSON-safe."""
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
    description="Backend monitoring & prediksi saldo ATM BRK Syariah — V6 (AMAN >25% | AWAS 20-25% | BONGKAR <20%)",
    version="6.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
@app.post("/api/clear-cache")
async def clear_cache():
    if PRED_CACHE.exists():
        PRED_CACHE.unlink()
        return {"message": "Cache berhasil dibersihkan"}
    return {"message": "Cache sudah kosong"}

# ── Training state (in-memory) ───────────────────────
_train_state = {
    "status":       "idle",   # idle | running | done | error
    "progress":     0,
    "message":      "",
    "last_trained": None,
    "last_result":  None,
}


# ════════════════════════════════════════════════════
#  HEALTH
# ════════════════════════════════════════════════════

@app.get("/", tags=["Health"])
def root():
    return {
        "service": "Smart ATM Dashboard API",
        "version": "6.0.0",
        "status":  "running",
        "time":    datetime.now().isoformat(),
        "thresholds": {
            "aman":    "> 25%",
            "awas":    "20–25%",
            "bongkar": "< 20%",
        },
    }


# ════════════════════════════════════════════════════
#  STATUS
# ════════════════════════════════════════════════════

@app.get("/api/status", tags=["Status"])
def get_status():
    """Cek apakah data & model sudah ada."""
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
        "version":      "6.0.0",
    }

    if has_data:
        df = pd.read_csv(PROCESSED_CSV, usecols=['ID ATM', 'Tanggal'], low_memory=False)
        df['ID ATM'] = df['ID ATM'].astype(str).str.strip().str.upper()
        df = df[~df['ID ATM'].isin(['', 'NAN', 'NONE', 'NULL', 'ID ATM'])]
        info["total_rows"] = len(df)
        info["total_atm"]  = df['ID ATM'].nunique()
        info["date_range"] = {
            "from": str(df['Tanggal'].min()),
            "to":   str(df['Tanggal'].max()),
        }

    if has_cache:
        cache = load_cache()
        info["predictions_count"]   = cache.get("count", 0)
        info["predictions_updated"] = cache.get("generated_at")
        info["total_atm"]           = cache.get("count", info.get("total_atm", 0))

    return _sanitize(info)


# ════════════════════════════════════════════════════
#  HELPERS — parse ZIP & tabular files
# ════════════════════════════════════════════════════

BULAN_ID = {
    'januari':'01','februari':'02','maret':'03','april':'04',
    'mei':'05','juni':'06','juli':'07','agustus':'08',
    'september':'09','oktober':'10','november':'11','desember':'12',
    'jan':'01','feb':'02','mar':'03','apr':'04','jun':'06',
    'jul':'07','agu':'08','agt':'08','sep':'09','okt':'10','nov':'11','des':'12',
}

BULAN_NAMA = [
    'Januari','Februari','Maret','April','Mei','Juni',
    'Juli','Agustus','September','Oktober','November','Desember'
]


def _extract_tanggal(path: str) -> Optional[str]:
    """
    Ekstrak tanggal dari path ZIP. Support berbagai format:
      2026-03-22 / 2026_03_22 → YYYY-MM-DD
      22-03-2026 / 22/03/2026 → DD-MM-YYYY
      22 Maret 2026 / 22_Maret_2026 → Indonesia
    Return: string YYYY-MM-DD atau None.
    """
    # YYYY-MM-DD
    m = re.search(r'(\d{4})-(\d{2})-(\d{2})', path)
    if m:
        return f"{m.group(1)}-{m.group(2)}-{m.group(3)}"
    # YYYY_MM_DD
    m = re.search(r'(\d{4})_(\d{2})_(\d{2})', path)
    if m:
        return f"{m.group(1)}-{m.group(2)}-{m.group(3)}"
    # DD-MM-YYYY atau DD/MM/YYYY
    m = re.search(r'(\d{1,2})[-/](\d{2})[-/](\d{4})', path)
    if m:
        return f"{m.group(3)}-{m.group(2)}-{m.group(1).zfill(2)}"
    # DD Bulan YYYY — Indonesia (spasi atau underscore)
    m = re.search(r'(\d{1,2})[\s_-]+([A-Za-z]+)[\s_-]+(\d{4})', path)
    if m:
        bln = BULAN_ID.get(m.group(2).lower())
        if bln:
            return f"{m.group(3)}-{bln}-{m.group(1).zfill(2)}"
    return None


def _extract_jam(basename: str) -> Optional[str]:
    """
    Ekstrak jam dari nama file.
    Mendukung:
      '21.01.xlsx'                           → '21:00'
      '09.00.xlsx'                           → '09:00'
      'Monitoring Saldo ATM BRKS 21.01.xlsx' → '21:00'
      '09.xlsx' / '9.csv'                    → '09:00'
    """
    base = re.sub(r'\.(csv|xlsx|xls)$', '', basename, flags=re.IGNORECASE).strip()
    # Cari semua pasangan HH.MM atau HH:MM di bagian akhir nama
    matches = re.findall(r'(\d{1,2})[\.:](\d{2})', base)
    if matches:
        hh, _ = matches[-1]
        hh_int = int(hh)
        if 0 <= hh_int <= 23:
            return f"{hh_int:02d}:00"
    # Coba angka tunggal (jam tanpa menit)
    m = re.search(r'(\d{1,2})$', base)
    if m:
        hh_int = int(m.group(1))
        if 0 <= hh_int <= 23:
            return f"{hh_int:02d}:00"
    return None


def _read_tabular(zf: "zipfile.ZipFile", name: str) -> pd.DataFrame:
    """Baca satu file di dalam ZipFile sebagai DataFrame (CSV/XLSX/XLS)."""
    lower = name.lower()
    with zf.open(name) as raw:
        data = raw.read()
    buf = io.BytesIO(data)

    if lower.endswith(".csv"):
        for enc in ("utf-8", "utf-8-sig", "latin-1", "cp1252"):
            try:
                buf.seek(0)
                df = pd.read_csv(buf, encoding=enc, dtype_backend="numpy_nullable")
                break
            except UnicodeDecodeError:
                continue
        else:
            buf.seek(0)
            df = pd.read_csv(buf, encoding="latin-1", errors="replace",
                             dtype_backend="numpy_nullable")
    elif lower.endswith(".xlsx"):
        df = pd.read_excel(buf, engine="openpyxl", dtype_backend="numpy_nullable")
    elif lower.endswith(".xls"):
        df = pd.read_excel(buf, engine="xlrd")
    else:
        raise ValueError(f"Format tidak didukung: {name}")

    df = df.convert_dtypes(dtype_backend="numpy_nullable")
    for col in df.select_dtypes(include=["object", "string"]).columns:
        df[col] = df[col].astype(str).replace("nan", pd.NA).replace("<NA>", pd.NA)
    return df


def _normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Rename kolom dari berbagai format ke nama standar.
    Sama persis dengan V6 notebook Cell 3.
    """
    df.columns = df.columns.str.strip()
    col_rename = {}
    for col in df.columns:
        c = col.lower().strip()
        if 'id' in c and 'atm' in c:
            col_rename[col] = 'ID ATM'
        elif 'merk' in c:
            col_rename[col] = 'Merk ATM'
        elif 'lokasi' in c:
            col_rename[col] = 'Lokasi ATM'
        elif 'alamat' in c:
            col_rename[col] = 'Alamat ATM'
        elif 'vendor' in c:
            col_rename[col] = 'Vendor'
        elif 'limit' in c:
            col_rename[col] = 'Limit'
        elif 'sisa' in c and 'saldo' in c:
            col_rename[col] = 'Sisa Saldo'
        elif 'denom' in c:
            col_rename[col] = 'Denom'
        elif 'lembar' in c:
            col_rename[col] = 'Lembar'
    return df.rename(columns=col_rename)


def _parse_zip(zip_bytes: bytes):
    """
    Parse ZIP V6:
      - Folder = tanggal (berbagai format Indonesia didukung)
      - File   = jam snapshot (HH.MM.xlsx / Monitoring ... HH.MM.xlsx)
    Return (DataFrame gabungan, list warnings)
    """
    SUPPORTED = {".csv", ".xlsx", ".xls"}
    REQUIRED  = {"ID ATM", "Sisa Saldo", "Limit"}

    frames = []
    errors = []

    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
        names = zf.namelist()

        for name in names:
            if name.endswith("/") or "/." in name or name.startswith("."):
                continue
            ext = "." + name.rsplit(".", 1)[-1].lower() if "." in name else ""
            if ext not in SUPPORTED:
                continue

            tanggal_str = _extract_tanggal(name)
            if not tanggal_str:
                errors.append(f"Skip (tidak ada tanggal di path): {name}")
                continue

            basename = name.split("/")[-1]
            jam_str  = _extract_jam(basename)
            if not jam_str:
                errors.append(f"Skip (nama file bukan format jam): {name}")
                continue

            try:
                df_file = _read_tabular(zf, name)
            except Exception as e:
                errors.append(f"Gagal baca {name}: {e}")
                continue

            df_file = _normalize_columns(df_file)

            missing = REQUIRED - set(df_file.columns)
            if missing:
                errors.append(f"Skip {name}: kolom kurang {missing}")
                continue

            df_file["Tanggal"] = tanggal_str
            df_file["Jam"]     = jam_str
            frames.append(df_file)

    if not frames:
        detail = "ZIP tidak mengandung file tabular valid (CSV/XLSX/XLS). "
        if errors:
            detail += "Detail: " + "; ".join(errors[:5])
        raise HTTPException(400, detail)

    df_combined = pd.concat(frames, ignore_index=True)
    if "Tanggal" in df_combined.columns and "Jam" in df_combined.columns:
        df_combined = df_combined.drop_duplicates(subset=["ID ATM", "Tanggal", "Jam"])
    df_combined = df_combined.sort_values(["ID ATM", "Tanggal", "Jam"]).reset_index(drop=True)
    return df_combined, errors


# ════════════════════════════════════════════════════
#  UPLOAD DATA
# ════════════════════════════════════════════════════

@app.post("/api/upload", tags=["Data"])
async def upload_data(
    background_tasks: BackgroundTasks,
    file: UploadFile = File(...),
    retrain: bool = Query(True, description="Langsung retrain setelah upload?"),
):
    """
    Upload file data ATM (ZIP / CSV / XLSX).
    ZIP: folder tanggal Indonesia → file Excel per jam (format V6).
    CSV/XLSX: bisa raw maupun processed (auto-detect).

    Alur:
      1. Baca & parse file
      2. Merge incremental dengan data lama
      3. Process (jika belum processed)
      4. Rebuild prediction cache
      5. Retrain XGBoost (opsional, background)
    """
    fname   = file.filename or ""
    content = await file.read()

    parse_warnings = []

    # ── 1. Baca file ─────────────────────────────────────
    if fname.lower().endswith(".zip"):
        try:
            df_new, parse_warnings = _parse_zip(content)
        except HTTPException:
            raise
        except Exception as e:
            raise HTTPException(400, f"Gagal membaca ZIP: {e}")
        is_processed = False

    elif fname.lower().endswith(".csv"):
        try:
            df_new = pd.read_csv(io.BytesIO(content))
        except Exception as e:
            raise HTTPException(400, f"Gagal membaca CSV: {e}")
        missing = {"ID ATM", "Sisa Saldo", "Limit"} - set(df_new.columns)
        if missing:
            raise HTTPException(400, f"Kolom wajib tidak ditemukan: {missing}")
        df_new = _normalize_columns(df_new)
        # Inject Tanggal & Jam dari waktu sekarang jika tidak ada di file
        _now = datetime.now()
        if "Tanggal" not in df_new.columns:
            df_new["Tanggal"] = _now.strftime("%Y-%m-%d")
            parse_warnings.append(f"Kolom 'Tanggal' tidak ditemukan — diisi otomatis: {_now.strftime('%Y-%m-%d')}")
        if "Jam" not in df_new.columns:
            df_new["Jam"] = _now.strftime("%H:00")
            parse_warnings.append(f"Kolom 'Jam' tidak ditemukan — diisi otomatis: {_now.strftime('%H:00')}")
        is_processed = "Avg Penarikan 6j" in df_new.columns

    elif fname.lower().endswith((".xlsx", ".xls")):
        try:
            df_new = pd.read_excel(io.BytesIO(content), engine="openpyxl")
        except Exception as e:
            raise HTTPException(400, f"Gagal membaca Excel: {e}")
        missing = {"ID ATM", "Sisa Saldo", "Limit"} - set(df_new.columns)
        if missing:
            raise HTTPException(400, f"Kolom wajib tidak ditemukan: {missing}")
        df_new = _normalize_columns(df_new)
        # Inject Tanggal & Jam dari waktu sekarang jika tidak ada di file
        _now = datetime.now()
        if "Tanggal" not in df_new.columns:
            df_new["Tanggal"] = _now.strftime("%Y-%m-%d")
            parse_warnings.append(f"Kolom 'Tanggal' tidak ditemukan — diisi otomatis: {_now.strftime('%Y-%m-%d')}")
        if "Jam" not in df_new.columns:
            df_new["Jam"] = _now.strftime("%H:00")
            parse_warnings.append(f"Kolom 'Jam' tidak ditemukan — diisi otomatis: {_now.strftime('%H:00')}")
        is_processed = "Avg Penarikan 6j" in df_new.columns

    else:
        raise HTTPException(400, "Format file tidak didukung. Gunakan .zip, .csv, atau .xlsx")

    # ── 2. Merge incremental ──────────────────────────────
    if PROCESSED_CSV.exists():
        df_old = pd.read_csv(PROCESSED_CSV)

        if is_processed:
            # CSV processed → langsung merge
            df_old["ID ATM"] = df_old["ID ATM"].astype(str).str.strip().str.upper()
            df_new["ID ATM"] = df_new["ID ATM"].astype(str).str.strip().str.upper()
            df_merged = pd.concat([df_old, df_new], ignore_index=True)
            if "datetime" in df_merged.columns:
                df_merged["datetime"] = pd.to_datetime(df_merged["datetime"])
                df_merged = df_merged.drop_duplicates(subset=["ID ATM", "datetime"])
            elif "Tanggal" in df_merged.columns and "Jam" in df_merged.columns:
                df_merged["Tanggal"] = df_merged["Tanggal"].astype(str).str[:10]
                df_merged["Jam"]     = df_merged["Jam"].astype(str).str[:5]
                df_merged = df_merged.drop_duplicates(subset=["ID ATM", "Tanggal", "Jam"])
            df_final = df_merged

        else:
            # ZIP / raw → ambil kolom raw dari data lama, dedup per tanggal baru
            RAW_COLS   = ["ID ATM", "Sisa Saldo", "Limit", "Tanggal", "Jam",
                          "Merk ATM", "Lokasi ATM", "Alamat ATM", "Vendor"]
            cols_keep  = [c for c in RAW_COLS if c in df_old.columns]
            df_old_raw = df_old[cols_keep].copy()

            df_old_raw["ID ATM"] = df_old_raw["ID ATM"].astype(str).str.strip().str.upper()
            df_new["ID ATM"]     = df_new["ID ATM"].astype(str).str.strip().str.upper()

            df_old_raw["Tanggal"] = df_old_raw["Tanggal"].astype(str).str[:10]
            df_old_raw["Jam"]     = df_old_raw["Jam"].astype(str).str[:5]
            df_new["Tanggal"]     = df_new["Tanggal"].astype(str).str[:10]
            df_new["Jam"]         = df_new["Jam"].astype(str).str[:5]

            # Buang data lama yang tanggalnya sama dengan data baru
            tanggal_baru  = set(df_new["Tanggal"].unique())
            df_old_raw    = df_old_raw[~df_old_raw["Tanggal"].isin(tanggal_baru)]

            df_combined   = pd.concat([df_old_raw, df_new], ignore_index=True)
            df_combined   = df_combined.drop_duplicates(subset=["ID ATM", "Tanggal", "Jam"])
            df_combined   = df_combined.sort_values(["ID ATM", "Tanggal", "Jam"]).reset_index(drop=True)
            df_final      = process_dataframe(df_combined)
    else:
        # Data pertama kali
        df_final = df_new if is_processed else process_dataframe(df_new)

    # ── 3. Simpan processed data ──────────────────────────
    df_final.to_csv(PROCESSED_CSV, index=False)

    # ── 4. Rebuild prediction cache ───────────────────────
    def _clean_pred(p):
        return {k: (None if isinstance(v, float) and (math.isnan(v) or math.isinf(v)) else v)
                for k, v in p.items()}

    predictions = [_clean_pred(p) for p in build_predictions(df_final)]
    save_cache(predictions)

    resp = {
        "message":      "Upload berhasil",
        "version":      "V6",
        "format":       "ZIP" if fname.lower().endswith(".zip") else "CSV/Excel",
        "rows":         len(df_final),
        "atm_count":    df_final["ID ATM"].nunique() if "ID ATM" in df_final.columns else 0,
        "predictions":  len(predictions),
    }
    if parse_warnings:
        resp["warnings"] = parse_warnings[:10]

    if retrain:
        background_tasks.add_task(_do_retrain, df_final)
        resp["retrain"] = "Dimulai di background — pantau GET /api/train/status"

    return resp


# ════════════════════════════════════════════════════
#  TRAINING
# ════════════════════════════════════════════════════

@app.post("/api/train", tags=["Training"])
async def trigger_train(background_tasks: BackgroundTasks):
    """Trigger retrain XGBoost secara manual dari data yang sudah ada."""
    if _train_state["status"] == "running":
        raise HTTPException(409, "Training sedang berjalan, tunggu selesai dulu.")
    if not PROCESSED_CSV.exists():
        raise HTTPException(404, "Tidak ada data. Upload dulu via POST /api/upload")

    df = pd.read_csv(PROCESSED_CSV)
    background_tasks.add_task(_do_retrain, df)
    return {"message": "Training V6 dimulai", "monitor": "GET /api/train/status"}


@app.get("/api/train/status", tags=["Training"])
def get_train_status():
    """Cek progress training."""
    return _train_state


# ════════════════════════════════════════════════════
#  PREDICTIONS
# ════════════════════════════════════════════════════

@app.get("/api/predictions", tags=["Predictions"])
def get_predictions(
    wilayah: Optional[str] = Query(None, description="Filter by wilayah"),
    status:  Optional[str] = Query(None,
        description="Filter by status: BONGKAR | AWAS | PERLU PANTAU | AMAN | OVERFUND"),
    tipe:    Optional[str] = Query(None, description="Filter by tipe ATM (EMV / CRM)"),
    limit:   int           = Query(100, ge=1, le=500),
    offset:  int           = Query(0, ge=0),
):
    """
    Seluruh prediksi ATM, diurutkan by skor_urgensi DESC.
    Status V6: BONGKAR | AWAS | PERLU PANTAU | AMAN | OVERFUND
    """
    cache = load_cache()
    if cache is None:
        raise HTTPException(404, "Belum ada prediksi. Upload data terlebih dahulu.")

    data = cache["data"]

    if wilayah:
        data = [d for d in data if wilayah.lower() in d["wilayah"].lower()]
    if status:
        data = [d for d in data if d["status"].lower() == status.lower()]
    if tipe:
        data = [d for d in data if d["tipe"].upper() == tipe.upper()]

    total = len(data)
    paged = data[offset: offset + limit]

    return _sanitize({
        "generated_at": cache.get("generated_at"),
        "total":        total,
        "offset":       offset,
        "limit":        limit,
        "data":         paged,
    })


@app.get("/api/predictions/{atm_id}", tags=["Predictions"])
def get_prediction_detail(atm_id: str):
    """Detail prediksi 1 ATM, termasuk field baru V6 (tgl_awas, cashout, pred_saldo_Xj, dll)."""
    cache = load_cache()
    if cache is None:
        raise HTTPException(404, "Belum ada prediksi.")

    match = [d for d in cache["data"] if d["id_atm"] == atm_id.strip().upper()]
    if not match:
        raise HTTPException(404, f"ATM {atm_id} tidak ditemukan.")

    return _sanitize(match[0])


# ════════════════════════════════════════════════════
#  ALERTS
# ════════════════════════════════════════════════════

@app.get("/api/alerts", tags=["Alerts"])
def get_alerts(
    level: Optional[str] = Query(
        None,
        description="BONGKAR | AWAS | PERLU PANTAU  (default: BONGKAR + AWAS)"
    ),
):
    """
    ATM yang butuh perhatian segera.
    V6 default: BONGKAR + AWAS (menggantikan KRITIS + SEGERA ISI dari V5)
    """
    cache = load_cache()
    if cache is None:
        raise HTTPException(404, "Belum ada prediksi.")

    # V6 alert statuses
    alert_statuses = ["BONGKAR", "AWAS"]
    if level:
        alert_statuses = [level.upper()]

    alerts = [d for d in cache["data"] if d["status"] in alert_statuses]

    return _sanitize({
        "generated_at": cache.get("generated_at"),
        "total_alerts": len(alerts),
        "breakdown":    {s: sum(1 for d in alerts if d["status"] == s) for s in alert_statuses},
        "data":         alerts,
    })


# ════════════════════════════════════════════════════
#  SUMMARY
# ════════════════════════════════════════════════════

@app.get("/api/summary", tags=["Summary"])
def get_summary():
    """Ringkasan keseluruhan + per wilayah. Status V6."""
    cache = load_cache()
    if cache is None:
        raise HTTPException(404, "Belum ada prediksi.")

    data = cache["data"]

    # Hitung distribusi status
    status_counts = {}
    for d in data:
        status_counts[d["status"]] = status_counts.get(d["status"], 0) + 1

    metode_counts = {}
    for d in data:
        metode_counts[d["metode"]] = metode_counts.get(d["metode"], 0) + 1

    def _n(v, default=0.0):
        if v is None: return default
        try:
            f = float(v)
            return default if math.isnan(f) or math.isinf(f) else f
        except: return default

    pct_values = [_n(d.get("pct_saldo")) for d in data]

    overall = {
        "total_atm":     len(data),
        # V6 status labels
        "bongkar":       status_counts.get("BONGKAR", 0),
        "awas":          status_counts.get("AWAS", 0),
        "perlu_pantau":  status_counts.get("PERLU PANTAU", 0),
        "aman":          status_counts.get("AMAN", 0),
        "overfund":      status_counts.get("OVERFUND", 0),
        "no_data":       status_counts.get("NO DATA", 0),
        "atm_sepi":      sum(1 for d in data if d.get("atm_sepi")),
        "avg_pct_saldo": round(sum(pct_values) / max(len(pct_values), 1), 1),
        "status_breakdown": status_counts,
        "metode_breakdown": metode_counts,
    }

    # Per wilayah
    wilayah_map: dict = {}
    for d in data:
        w = d["wilayah"]
        wilayah_map.setdefault(w, []).append(d)

    per_wilayah = []
    for w, items in wilayah_map.items():
        pct_w  = [_n(i.get("pct_saldo"))    for i in items]
        skor_w = [_n(i.get("skor_urgensi")) for i in items]
        est_w  = [_n(i["est_jam"]) for i in items if i.get("est_jam") is not None]
        per_wilayah.append({
            "wilayah":       w,
            "total":         len(items),
            # V6 status labels
            "bongkar":       sum(1 for i in items if i.get("status") == "BONGKAR"),
            "awas":          sum(1 for i in items if i.get("status") == "AWAS"),
            "perlu_pantau":  sum(1 for i in items if i.get("status") == "PERLU PANTAU"),
            "aman":          sum(1 for i in items if i.get("status") == "AMAN"),
            "atm_sepi":      sum(1 for i in items if i.get("atm_sepi")),
            "avg_pct_saldo": round(sum(pct_w)  / max(len(pct_w),  1), 1),
            "avg_est_jam":   round(sum(est_w)   / max(len(est_w),  1), 1),
            "avg_skor":      round(sum(skor_w)  / max(len(skor_w), 1), 1),
        })
    per_wilayah.sort(key=lambda x: x["avg_skor"], reverse=True)

    return _sanitize({
        "generated_at": cache.get("generated_at"),
        "overall":      overall,
        "per_wilayah":  per_wilayah,
    })


# ════════════════════════════════════════════════════
#  HISTORY
# ════════════════════════════════════════════════════

@app.get("/api/history/{atm_id}", tags=["History"])
def get_atm_history(
    atm_id: str,
    last_n_days: int = Query(7, ge=1, le=30),
):
    """
    Data historis saldo + penarikan untuk 1 ATM.
    Status kolom menggunakan label V6 (AMAN / AWAS / BONGKAR).
    """
    if not PROCESSED_CSV.exists():
        raise HTTPException(404, "Belum ada data.")

    df     = pd.read_csv(PROCESSED_CSV)
    atm_id = atm_id.strip().upper()
    df["ID ATM"] = df["ID ATM"].astype(str).str.strip().str.upper()
    atm_df = df[df['ID ATM'] == atm_id].copy()

    if atm_df.empty:
        raise HTTPException(404, f"ATM {atm_id} tidak ditemukan.")

    atm_df['datetime'] = pd.to_datetime(atm_df['datetime'])
    cutoff  = atm_df['datetime'].max() - pd.Timedelta(days=last_n_days)
    atm_df  = atm_df[atm_df['datetime'] >= cutoff].sort_values('datetime')

    cols = ['datetime', 'Sisa Saldo', 'Limit', 'Penarikan', 'Persentase',
            'Is Refill', 'Is_Interpolated', 'Status']
    cols = [c for c in cols if c in atm_df.columns]

    records = atm_df[cols].rename(columns={
        'Sisa Saldo':      'saldo',
        'Limit':           'limit',
        'Penarikan':       'penarikan',
        'Persentase':      'pct',
        'Is Refill':       'is_refill',
        'Is_Interpolated': 'is_interpolated',
        'Status':          'status',
    }).to_dict(orient='records')

    refill_count = int(atm_df['Is Refill'].sum()) if 'Is Refill' in atm_df.columns else 0

    return _sanitize({
        "id_atm":       atm_id,
        "last_n_days":  last_n_days,
        "total_rows":   len(records),
        "refill_count": refill_count,
        "saldo_min":    round(float(atm_df['Sisa Saldo'].min()), 0),
        "saldo_max":    round(float(atm_df['Sisa Saldo'].max()), 0),
        "saldo_latest": round(float(atm_df['Sisa Saldo'].iloc[-1]), 0),
        "limit":        round(float(atm_df['Limit'].iloc[-1]), 0),
        "data":         records,
    })


# ════════════════════════════════════════════════════
#  WILAYAH
# ════════════════════════════════════════════════════

@app.get("/api/wilayah", tags=["Wilayah"])
def get_wilayah():
    """List ATM per wilayah dengan status V6."""
    cache = load_cache()
    if cache is None:
        raise HTTPException(404, "Belum ada prediksi.")

    result: dict = {}
    for d in cache["data"]:
        w = d["wilayah"]
        result.setdefault(w, []).append({
            "id_atm":       d["id_atm"],
            "lokasi":       d["lokasi"],
            "tipe":         d["tipe"],
            "pct_saldo":    d["pct_saldo"],
            "status":       d["status"],
            "skor_urgensi": d["skor_urgensi"],
            "atm_sepi":     d.get("atm_sepi", False),
        })

    return _sanitize({
        "wilayah_list": list(result.keys()),
        "data":         result,
    })


# ════════════════════════════════════════════════════
#  DEBUG
# ════════════════════════════════════════════════════

@app.get("/api/debug/atm_ids", tags=["Debug"])
def debug_atm_ids():
    """Cek ID ATM, Vendor, Wilayah di processed_data.csv."""
    if not PROCESSED_CSV.exists():
        raise HTTPException(404, "Belum ada data.")

    df = pd.read_csv(PROCESSED_CSV, low_memory=False)
    df["ID ATM"] = df["ID ATM"].astype(str).str.strip().str.upper()
    df = df[~df["ID ATM"].isin(["", "NAN", "NONE", "NULL", "ID ATM"])]

    result = {
        "total_rows": len(df),
        "total_atm":  df["ID ATM"].nunique(),
        "date_range": {
            "from": str(df["Tanggal"].min()) if "Tanggal" in df.columns else "-",
            "to":   str(df["Tanggal"].max()) if "Tanggal" in df.columns else "-",
        },
    }

    if "Vendor" in df.columns:
        result["vendor_unique"] = sorted(df["Vendor"].dropna().unique().tolist())

    if "Wilayah" in df.columns:
        result["wilayah_atm_count"] = df.groupby("Wilayah")["ID ATM"].nunique().to_dict()

    if "Tanggal" in df.columns:
        tgl_counts = df.groupby("Tanggal")["ID ATM"].nunique().to_dict()
        result["atm_per_tanggal_sample"] = dict(list(tgl_counts.items())[:10])

    # Distribusi status V6
    if "Status" in df.columns:
        # Ambil snapshot terbaru per ATM
        df["datetime"] = pd.to_datetime(df.get("datetime", pd.NaT), errors='coerce')
        latest = df.loc[df.groupby("ID ATM")["datetime"].idxmax()]
        result["status_distribution_v6"] = latest["Status"].value_counts().to_dict()

    return result


# ════════════════════════════════════════════════════
#  RESET
# ════════════════════════════════════════════════════

@app.delete("/api/data", tags=["Admin"])
def reset_all():
    """Hapus semua data, model, dan cache. HATI-HATI!"""
    removed = []
    for p in [PROCESSED_CSV, PRED_CACHE, MODEL_PATH, FITUR_PATH]:
        if p.exists():
            p.unlink()
            removed.append(str(p))

    _train_state.update({
        "status": "idle", "progress": 0,
        "message": "", "last_trained": None, "last_result": None,
    })

    return {"message": "Reset berhasil", "removed_files": removed}


# ════════════════════════════════════════════════════
#  BACKGROUND TASK
# ════════════════════════════════════════════════════

async def _do_retrain(df: pd.DataFrame):
    """Background task untuk training XGBoost V6."""
    _train_state.update({"status": "running", "progress": 0, "message": "Memulai training V6..."})

    def _cb(pct, msg):
        _train_state["progress"] = pct
        _train_state["message"]  = msg

    try:
        if 'Avg Penarikan 6j' not in df.columns:
            _cb(5, "Processing data...")
            df = process_dataframe(df)

        loop   = asyncio.get_event_loop()
        result = await loop.run_in_executor(None, lambda: train(df, _cb))

        _cb(95, "Rebuild prediksi cache V6...")
        predictions = build_predictions(df)
        save_cache(predictions)

        _train_state.update({
            "status":       "done",
            "progress":     100,
            "message":      (
                f"Training V6 selesai ✅ MAE={result.get('mae_avg')} jam | R²={result.get('r2_avg')}"
                if result.get('mae_avg') is not None
                else "Training V6 selesai ✅ (Rule-Based — data kurang)"
            ),
            "last_trained": datetime.now().isoformat(),
            "last_result":  result,
        })
    except Exception as e:
        _train_state.update({
            "status":   "error",
            "progress": 0,
            "message":  f"Error: {str(e)}",
        })