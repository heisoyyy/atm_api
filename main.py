"""
main.py — Smart ATM Dashboard API v5
=====================================
Endpoints:
  GET  /                        health check
  GET  /api/status              status model + data
  POST /api/upload              upload CSV/Excel, proses, simpan
  POST /api/train               trigger retrain XGBoost (background)
  GET  /api/train/status        progress retrain
  GET  /api/predictions         seluruh prediksi (+ filter)
  GET  /api/predictions/{id}    detail 1 ATM
  GET  /api/summary             ringkasan per wilayah
  GET  /api/alerts              ATM dengan status KRITIS / SEGERA ISI
  GET  /api/history/{id}        data historis saldo 1 ATM
  GET  /api/wilayah             list wilayah + statistik
  DELETE /api/data              hapus semua data & model (reset)
"""

import asyncio
import io
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
app = FastAPI(
    title="Smart ATM Dashboard API",
    description="Backend untuk monitoring & prediksi saldo ATM BRK Syariah",
    version="5.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── Training state (in-memory) ───────────────────────
_train_state = {
    "status":       "idle",      # idle | running | done | error
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
        "version": "5.0.0",
        "status":  "running",
        "time":    datetime.now().isoformat(),
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
        "has_data":       has_data,
        "has_model":      has_model,
        "has_cache":      has_cache,
        "model_path":     str(MODEL_PATH) if has_model else None,
        "train_status":   _train_state["status"],
        "last_trained":   _train_state["last_trained"],
    }

    if has_data:
        df = pd.read_csv(PROCESSED_CSV, usecols=['ID ATM', 'Tanggal'])
        info["total_rows"] = len(df)
        info["total_atm"]  = df['ID ATM'].nunique()
        info["date_range"] = {
            "from": str(df['Tanggal'].min()),
            "to":   str(df['Tanggal'].max()),
        }

    if has_cache:
        cache = load_cache()
        info["predictions_count"]  = cache.get("count", 0)
        info["predictions_updated"] = cache.get("generated_at")

    return info


# ════════════════════════════════════════════════════
#  UPLOAD DATA
# ════════════════════════════════════════════════════

# ════════════════════════════════════════════════════
#  HELPERS
# ════════════════════════════════════════════════════

def _read_tabular(zf: "zipfile.ZipFile", name: str) -> pd.DataFrame:
    """
    Baca satu file di dalam ZipFile sebagai DataFrame.
    Support: .csv, .xlsx, .xls
    """
    lower = name.lower()
    with zf.open(name) as raw:
        data = raw.read()
    buf = io.BytesIO(data)
    if lower.endswith(".csv"):
        # Coba beberapa encoding umum
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
    else:
        raise ValueError(f"Format tidak didukung: {name}")


def _parse_zip(zip_bytes: bytes) -> pd.DataFrame:
    """
    Parse ZIP berisi folder tanggal (YYYY-MM-DD) yang di dalamnya
    ada file per jam. Format file yang didukung: .csv, .xlsx, .xls

    Struktur yang didukung:
        data.zip
        └── 2026-03-31/
            ├── 00.csv   (atau 00.xlsx / 00.xls)
            ├── 01.csv
            ├── ...
            └── 23.csv

    Nama file = jam (0-23). Tanggal diambil dari nama folder (YYYY-MM-DD).
    Kolom wajib per file: ID ATM, Sisa Saldo, Limit

    Return: (DataFrame gabungan, list warning/skip)
    """
    DATE_RE  = re.compile(r"(\d{4}-\d{2}-\d{2})")
    # Format didukung:
    #   "00.csv" / "01.xlsx"                        → jam sederhana
    #   "Monitoring Saldo ATM BRKS 00.58.xlsx"       → jam.menit.ext
    HOUR_RE  = re.compile(
        r'(?:^|[\s_\-])(\d{1,2})\.\d{2}\.(csv|xlsx|xls)$'
        r'|^(\d{1,2})\.(csv|xlsx|xls)$',
        re.IGNORECASE
    )
    SUPPORTED = {".csv", ".xlsx", ".xls"}
    REQUIRED  = {"ID ATM", "Sisa Saldo", "Limit"}

    frames = []
    errors = []

    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
        names = zf.namelist()

        for name in names:
            # Abaikan folder, file tersembunyi, file non-tabular
            if name.endswith("/") or "/." in name or name.startswith("."):
                continue
            ext = "." + name.rsplit(".", 1)[-1].lower() if "." in name else ""
            if ext not in SUPPORTED:
                continue

            # Ekstrak tanggal dari nama folder / path
            date_match = DATE_RE.search(name)
            if not date_match:
                errors.append(f"Skip (tidak ada tanggal YYYY-MM-DD di path): {name}")
                continue
            tanggal_str = date_match.group(1)

            # Ekstrak jam dari nama file
            basename   = name.split("/")[-1]
            hour_match = HOUR_RE.search(basename)
            if not hour_match:
                errors.append(f"Skip (nama file bukan format jam 0-23): {name}")
                continue
            # group(1) = format "BRKS 00.58.xlsx", group(3) = format "00.csv"
            jam_raw = hour_match.group(1) or hour_match.group(3)
            jam_int = int(jam_raw)
            if jam_int > 23:
                errors.append(f"Skip (jam {jam_int} tidak valid): {name}")
                continue
            jam_str = f"{jam_int:02d}:00"

            # Baca file
            try:
                df_file = _read_tabular(zf, name)
            except Exception as e:
                errors.append(f"Gagal baca {name}: {e}")
                continue

            # Validasi kolom wajib
            missing = REQUIRED - set(df_file.columns)
            if missing:
                errors.append(f"Skip {name}: kolom kurang {missing}")
                continue

            # Tambahkan Tanggal dan Jam dari nama folder/file
            df_file["Tanggal"] = tanggal_str
            df_file["Jam"]     = jam_str

            frames.append(df_file)

    if not frames:
        detail = "ZIP tidak mengandung file tabular valid (CSV/XLSX/XLS). "
        if errors:
            detail += "Detail: " + "; ".join(errors[:5])
        raise HTTPException(400, detail)

    df_combined = pd.concat(frames, ignore_index=True)

    # Buang duplikat
    if "Tanggal" in df_combined.columns and "Jam" in df_combined.columns:
        df_combined = df_combined.drop_duplicates(subset=["ID ATM", "Tanggal", "Jam"])

    df_combined = df_combined.sort_values(["ID ATM", "Tanggal", "Jam"]).reset_index(drop=True)
    return df_combined, errors


@app.post("/api/upload", tags=["Data"])
async def upload_data(
    background_tasks: BackgroundTasks,
    file: UploadFile = File(...),
    retrain: bool = Query(True, description="Langsung retrain setelah upload?"),
):
    """
    Upload file processed_data.csv (output dari Colab V5).
    Setelah upload, otomatis:
    1. Merge dengan data lama (incremental)
    2. Rebuild prediksi cache
    3. Retrain XGBoost (opsional, background)
    """
    fname   = file.filename or ""
    content = await file.read()

    # ── 1. Baca file masuk ───────────────────────────────
    parse_warnings = []

    if fname.lower().endswith(".zip"):
        # ZIP berisi folder tanggal → 24 CSV per hari
        try:
            df_new, parse_warnings = _parse_zip(content)
        except HTTPException:
            raise
        except Exception as e:
            raise HTTPException(400, f"Gagal membaca ZIP: {e}")
        is_processed = False   # ZIP selalu raw data, harus di-process

    elif fname.lower().endswith(".csv"):
        try:
            df_new = pd.read_csv(io.BytesIO(content))
        except Exception as e:
            raise HTTPException(400, f"Gagal membaca CSV: {e}")
        # Validasi kolom wajib untuk CSV
        missing = {"ID ATM", "Sisa Saldo", "Limit"} - set(df_new.columns)
        if missing:
            raise HTTPException(400, f"Kolom wajib tidak ditemukan: {missing}")
        is_processed = "Avg Penarikan 6j" in df_new.columns

    elif fname.lower().endswith((".xlsx", ".xls")):
        try:
            df_new = pd.read_excel(io.BytesIO(content), engine="openpyxl")
        except Exception as e:
            raise HTTPException(400, f"Gagal membaca Excel: {e}")
        missing = {"ID ATM", "Sisa Saldo", "Limit"} - set(df_new.columns)
        if missing:
            raise HTTPException(400, f"Kolom wajib tidak ditemukan: {missing}")
        is_processed = "Avg Penarikan 6j" in df_new.columns

    else:
        raise HTTPException(400, "Format file tidak didukung. Gunakan .zip, .csv, atau .xlsx")

    # ── 2. Merge incremental dengan data lama ────────────
    if PROCESSED_CSV.exists():
        df_old = pd.read_csv(PROCESSED_CSV)

        if is_processed:
            # CSV processed (sudah punya semua fitur) → langsung merge
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
            # ZIP / raw CSV → ambil HANYA kolom raw dari df_old supaya bisa
            # di-concat dengan df_new (yang belum punya fitur turunan)
            RAW_COLS = ["ID ATM", "Sisa Saldo", "Limit", "Tanggal", "Jam",
                        "Merk ATM", "Lokasi ATM", "Vendor"]
            cols_keep = [c for c in RAW_COLS if c in df_old.columns]
            df_old_raw = df_old[cols_keep].copy()

            # Normalisasi format Tanggal & Jam di data lama
            df_old_raw["Tanggal"] = df_old_raw["Tanggal"].astype(str).str[:10]
            df_old_raw["Jam"]     = df_old_raw["Jam"].astype(str).str[:5]

            # Normalisasi data baru
            df_new["Tanggal"] = df_new["Tanggal"].astype(str).str[:10]
            df_new["Jam"]     = df_new["Jam"].astype(str).str[:5]

            df_combined = pd.concat([df_old_raw, df_new], ignore_index=True)
            df_combined = df_combined.drop_duplicates(subset=["ID ATM", "Tanggal", "Jam"])
            df_combined = df_combined.sort_values(["ID ATM", "Tanggal", "Jam"]).reset_index(drop=True)

            df_final = process_dataframe(df_combined)
    else:
        # Data pertama kali
        df_final = df_new if is_processed else process_dataframe(df_new)

    # Simpan processed data
    df_final.to_csv(PROCESSED_CSV, index=False)

    # Rebuild cache prediksi
    predictions = build_predictions(df_final)
    save_cache(predictions)

    resp = {
        "message":    "Upload berhasil",
        "format":     "ZIP" if fname.lower().endswith(".zip") else "CSV/Excel",
        "rows":       len(df_final),
        "atm_count":  df_final["ID ATM"].nunique() if "ID ATM" in df_final.columns else 0,
        "predictions": len(predictions),
    }
    if parse_warnings:
        resp["warnings"] = parse_warnings[:10]  # maks 10 warning

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

    return {"message": "Training dimulai", "monitor": "GET /api/train/status"}


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
    status:  Optional[str] = Query(None, description="Filter by status (KRITIS, SEGERA ISI, dll)"),
    tipe:    Optional[str] = Query(None, description="Filter by tipe ATM (EMV / CRM)"),
    limit:   int           = Query(100, ge=1, le=500),
    offset:  int           = Query(0, ge=0),
):
    """
    Seluruh prediksi ATM, sudah diurutkan by skor_urgensi DESC.
    Mendukung filter wilayah, status, tipe.
    """
    cache = load_cache()
    if cache is None:
        raise HTTPException(404, "Belum ada prediksi. Upload data terlebih dahulu.")

    data = cache["data"]

    # Filter
    if wilayah:
        data = [d for d in data if wilayah.lower() in d["wilayah"].lower()]
    if status:
        data = [d for d in data if d["status"].lower() == status.lower()]
    if tipe:
        data = [d for d in data if d["tipe"].upper() == tipe.upper()]

    total   = len(data)
    paged   = data[offset: offset + limit]

    return {
        "generated_at": cache.get("generated_at"),
        "total":        total,
        "offset":       offset,
        "limit":        limit,
        "data":         paged,
    }


@app.get("/api/predictions/{atm_id}", tags=["Predictions"])
def get_prediction_detail(atm_id: str):
    """Detail prediksi 1 ATM."""
    cache = load_cache()
    if cache is None:
        raise HTTPException(404, "Belum ada prediksi.")

    match = [d for d in cache["data"] if d["id_atm"] == atm_id]
    if not match:
        raise HTTPException(404, f"ATM {atm_id} tidak ditemukan.")

    return match[0]


# ════════════════════════════════════════════════════
#  ALERTS
# ════════════════════════════════════════════════════

@app.get("/api/alerts", tags=["Alerts"])
def get_alerts(
    level: Optional[str] = Query(None, description="KRITIS | SEGERA ISI | PERLU DIPANTAU"),
):
    """
    ATM yang butuh perhatian segera.
    Default: KRITIS + SEGERA ISI
    """
    cache = load_cache()
    if cache is None:
        raise HTTPException(404, "Belum ada prediksi.")

    alert_statuses = ["KRITIS", "SEGERA ISI"]
    if level:
        alert_statuses = [level.upper()]

    alerts = [d for d in cache["data"] if d["status"] in alert_statuses]

    return {
        "generated_at": cache.get("generated_at"),
        "total_alerts": len(alerts),
        "breakdown": {
            s: sum(1 for d in alerts if d["status"] == s)
            for s in alert_statuses
        },
        "data": alerts,
    }


# ════════════════════════════════════════════════════
#  SUMMARY
# ════════════════════════════════════════════════════

@app.get("/api/summary", tags=["Summary"])
def get_summary():
    """Ringkasan keseluruhan + per wilayah."""
    cache = load_cache()
    if cache is None:
        raise HTTPException(404, "Belum ada prediksi.")

    data = cache["data"]

    # Overall
    status_counts = {}
    for d in data:
        status_counts[d["status"]] = status_counts.get(d["status"], 0) + 1

    metode_counts = {}
    for d in data:
        metode_counts[d["metode"]] = metode_counts.get(d["metode"], 0) + 1

    overall = {
        "total_atm":    len(data),
        "kritis":       status_counts.get("KRITIS", 0),
        "segera_isi":   status_counts.get("SEGERA ISI", 0),
        "perlu_pantau": status_counts.get("PERLU DIPANTAU", 0),
        "aman":         status_counts.get("AMAN", 0),
        "overfund":     status_counts.get("BONGKAR (OVERFUND)", 0),
        "no_data":      status_counts.get("NO DATA", 0),
        "atm_sepi":     sum(1 for d in data if d.get("atm_sepi")),
        "avg_pct_saldo": round(sum(d["pct_saldo"] for d in data) / max(len(data), 1), 1),
        "status_breakdown": status_counts,
        "metode_breakdown": metode_counts,
    }

    # Per wilayah
    wilayah_map = {}
    for d in data:
        w = d["wilayah"]
        if w not in wilayah_map:
            wilayah_map[w] = []
        wilayah_map[w].append(d)

    per_wilayah = []
    for w, items in wilayah_map.items():
        per_wilayah.append({
            "wilayah":       w,
            "total":         len(items),
            "kritis":        sum(1 for i in items if i["status"] == "KRITIS"),
            "segera_isi":    sum(1 for i in items if i["status"] == "SEGERA ISI"),
            "atm_sepi":      sum(1 for i in items if i.get("atm_sepi")),
            "avg_pct_saldo": round(sum(i["pct_saldo"] for i in items) / len(items), 1),
            "avg_est_jam":   round(
                sum(i["est_jam"] for i in items if i["est_jam"] is not None) /
                max(sum(1 for i in items if i["est_jam"] is not None), 1), 1
            ),
            "avg_skor":      round(sum(i["skor_urgensi"] for i in items) / len(items), 1),
        })
    per_wilayah.sort(key=lambda x: x["avg_skor"], reverse=True)

    return {
        "generated_at": cache.get("generated_at"),
        "overall":      overall,
        "per_wilayah":  per_wilayah,
    }


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
    Dipakai untuk grafik tren di frontend.
    """
    if not PROCESSED_CSV.exists():
        raise HTTPException(404, "Belum ada data.")

    df = pd.read_csv(PROCESSED_CSV)
    atm_df = df[df['ID ATM'] == atm_id].copy()

    if atm_df.empty:
        raise HTTPException(404, f"ATM {atm_id} tidak ditemukan.")

    atm_df['datetime'] = pd.to_datetime(atm_df['datetime'])
    cutoff  = atm_df['datetime'].max() - pd.Timedelta(days=last_n_days)
    atm_df  = atm_df[atm_df['datetime'] >= cutoff].sort_values('datetime')

    # Hanya kirim kolom yang dibutuhkan frontend
    cols = ['datetime', 'Sisa Saldo', 'Limit', 'Penarikan', 'Persentase',
            'Is Refill', 'Is_Interpolated', 'Status']
    cols = [c for c in cols if c in atm_df.columns]

    records = atm_df[cols].rename(columns={
        'Sisa Saldo':       'saldo',
        'Limit':            'limit',
        'Penarikan':        'penarikan',
        'Persentase':       'pct',
        'Is Refill':        'is_refill',
        'Is_Interpolated':  'is_interpolated',
        'Status':           'status',
    }).to_dict(orient='records')

    # Statistik
    real_data = atm_df[atm_df.get('Is_Interpolated', pd.Series(0)) == 0] \
        if 'Is_Interpolated' in atm_df.columns else atm_df

    refill_count = int(atm_df['Is Refill'].sum()) if 'Is Refill' in atm_df.columns else 0

    return {
        "id_atm":       atm_id,
        "last_n_days":  last_n_days,
        "total_rows":   len(records),
        "refill_count": refill_count,
        "saldo_min":    round(float(atm_df['Sisa Saldo'].min()), 0),
        "saldo_max":    round(float(atm_df['Sisa Saldo'].max()), 0),
        "saldo_latest": round(float(atm_df['Sisa Saldo'].iloc[-1]), 0),
        "limit":        round(float(atm_df['Limit'].iloc[-1]), 0),
        "data":         records,
    }


# ════════════════════════════════════════════════════
#  WILAYAH
# ════════════════════════════════════════════════════

@app.get("/api/wilayah", tags=["Wilayah"])
def get_wilayah():
    """List ATM per wilayah."""
    cache = load_cache()
    if cache is None:
        raise HTTPException(404, "Belum ada prediksi.")

    result = {}
    for d in cache["data"]:
        w = d["wilayah"]
        if w not in result:
            result[w] = []
        result[w].append({
            "id_atm":       d["id_atm"],
            "lokasi":       d["lokasi"],
            "tipe":         d["tipe"],
            "pct_saldo":    d["pct_saldo"],
            "status":       d["status"],
            "skor_urgensi": d["skor_urgensi"],
            "atm_sepi":     d.get("atm_sepi", False),
        })

    return {
        "wilayah_list": list(result.keys()),
        "data":         result,
    }


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
    """Background task untuk training XGBoost."""
    _train_state.update({"status": "running", "progress": 0, "message": "Memulai training..."})

    def _cb(pct, msg):
        _train_state["progress"] = pct
        _train_state["message"]  = msg

    try:
        # Pastikan df sudah diproses
        if 'Avg Penarikan 6j' not in df.columns:
            _cb(5, "Processing data...")
            df = process_dataframe(df)

        loop   = asyncio.get_event_loop()
        result = await loop.run_in_executor(None, lambda: train(df, _cb))

        # Rebuild prediksi cache dengan model baru
        _cb(95, "Rebuild prediksi cache...")
        predictions = build_predictions(df)
        save_cache(predictions)

        _train_state.update({
            "status":       "done",
            "progress":     100,
            "message":      f"Training selesai ✅ MAE={result['mae_avg']} jam | R²={result['r2_avg']}",
            "last_trained": datetime.now().isoformat(),
            "last_result":  result,
        })
    except Exception as e:
        _train_state.update({
            "status":   "error",
            "progress": 0,
            "message":  f"Error: {str(e)}",
        })