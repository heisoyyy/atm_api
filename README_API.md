# Smart ATM Dashboard API — v5.0
## Panduan Setup & Test Postman

---

## 1. Instalasi & Jalankan

```bash
# Masuk ke folder backend
cd atm-dashboard/backend

# Install dependencies
pip install -r requirements.txt

# Jalankan server
uvicorn main:app --reload --host 0.0.0.0 --port 8000
```

Server berjalan di: `http://localhost:8000`
Swagger UI docs: `http://localhost:8000/docs`
ReDoc docs: `http://localhost:8000/redoc`

---

## 2. Struktur Folder

```
backend/
├── main.py           ← FastAPI app + semua endpoint
├── processing.py     ← Pipeline V5 (sama dengan notebook Cell 4)
├── trainer.py        ← XGBoost training
├── predictor.py      ← Build prediksi dari snapshot terbaru
├── config.py         ← Konstanta & path
├── requirements.txt
└── data/             ← Auto-created saat pertama kali jalan
    ├── processed_data.csv      ← Data setelah diproses
    ├── predictions_cache.json  ← Cache prediksi terbaru
    ├── xgboost_atm_v5.pkl      ← Model XGBoost
    └── xgboost_fitur_v5.pkl    ← List fitur model
```

---

## 3. Alur Kerja Normal

```
1. Upload processed_data.csv dari Colab
        ↓
2. API otomatis merge + rebuild prediksi
        ↓
3. XGBoost retrain di background
        ↓
4. GET /api/predictions → data terbaru
```

---

## 4. Semua Endpoint

### 🟢 Health Check
```
GET http://localhost:8000/
```

---

### 📊 Status Sistem
```
GET http://localhost:8000/api/status
```
**Response:**
```json
{
  "has_data": true,
  "has_model": true,
  "has_cache": true,
  "total_rows": 55132,
  "total_atm": 87,
  "date_range": { "from": "2026-03-01", "to": "2026-03-16" },
  "train_status": "done",
  "last_trained": "2026-03-16T10:30:00",
  "predictions_count": 87,
  "predictions_updated": "2026-03-16T10:31:00"
}
```

---

### 📤 Upload Data (paling penting)
```
POST http://localhost:8000/api/upload
```
- **Body**: form-data
  - Key: `file`, Type: File
  - Value: pilih `processed_data.csv` dari output Colab V5
- **Query params** (opsional):
  - `retrain=true` (default) → otomatis retrain setelah upload
  - `retrain=false` → skip retrain

**Response:**
```json
{
  "message": "Upload berhasil",
  "rows": 55132,
  "atm_count": 87,
  "predictions": 87,
  "retrain": "Dimulai di background — pantau GET /api/train/status"
}
```

---

### 🤖 Training Manual
```
POST http://localhost:8000/api/train
```
Trigger retrain dari data yang sudah ada (tanpa upload baru).

---

### 📈 Status Training
```
GET http://localhost:8000/api/train/status
```
**Response saat berjalan:**
```json
{
  "status": "running",
  "progress": 45,
  "message": "Fold 2/3 — MAE: 8.43 jam | R²: 0.8721",
  "last_trained": null,
  "last_result": null
}
```
**Response setelah selesai:**
```json
{
  "status": "done",
  "progress": 100,
  "message": "Training selesai ✅ MAE=7.82 jam | R²=0.8934",
  "last_trained": "2026-03-16T10:30:00",
  "last_result": {
    "mae_avg": 7.82,
    "r2_avg": 0.8934,
    "n_train": 28000,
    "n_hari": 16,
    "top_features": [...]
  }
}
```

---

### 🏆 Semua Prediksi
```
GET http://localhost:8000/api/predictions
```
**Query params:**
| Param | Tipe | Contoh | Keterangan |
|-------|------|--------|------------|
| `wilayah` | string | `Pekanbaru` | Filter wilayah |
| `status` | string | `KRITIS` | Filter status |
| `tipe` | string | `EMV` | Filter tipe ATM |
| `limit` | int | `20` | Jumlah hasil (default 100) |
| `offset` | int | `0` | Pagination |

**Contoh:**
```
GET /api/predictions?wilayah=Pekanbaru&status=KRITIS&limit=10
GET /api/predictions?tipe=CRM&limit=50
```

**Response:**
```json
{
  "generated_at": "2026-03-16T10:31:00",
  "total": 5,
  "offset": 0,
  "limit": 10,
  "data": [
    {
      "ranking": 1,
      "id_atm": "EMV10104",
      "tipe": "EMV",
      "lokasi": "...",
      "wilayah": "Pekanbaru",
      "saldo": 85000000,
      "limit": 500000000,
      "pct_saldo": 17.0,
      "tarik_per_jam": 2500000,
      "pred_saldo_12j": 55000000,
      "est_jam": 34.0,
      "est_hari": 1.42,
      "tgl_habis": "2026-03-17",
      "jam_habis": "20:00",
      "tgl_isi": "2026-03-17",
      "jam_isi": "14:00",
      "status": "SEGERA ISI",
      "skor_urgensi": 72.5,
      "atm_sepi": false,
      "metode": "XGBoost",
      "last_update": "2026-03-16 05:00:00"
    }
  ]
}
```

---

### 🔍 Detail 1 ATM
```
GET http://localhost:8000/api/predictions/EMV10614
```

---

### 🚨 Alerts
```
GET http://localhost:8000/api/alerts
GET http://localhost:8000/api/alerts?level=KRITIS
GET http://localhost:8000/api/alerts?level=SEGERA ISI
```

---

### 📋 Summary
```
GET http://localhost:8000/api/summary
```
**Response:**
```json
{
  "generated_at": "...",
  "overall": {
    "total_atm": 87,
    "kritis": 3,
    "segera_isi": 8,
    "perlu_pantau": 22,
    "aman": 54,
    "atm_sepi": 5,
    "avg_pct_saldo": 63.2,
    "status_breakdown": { ... },
    "metode_breakdown": { ... }
  },
  "per_wilayah": [
    {
      "wilayah": "Pekanbaru",
      "total": 42,
      "kritis": 2,
      "segera_isi": 4,
      "avg_pct_saldo": 58.1,
      "avg_est_jam": 67.3,
      "avg_skor": 38.2
    }
  ]
}
```

---

### 📈 Historis 1 ATM (untuk grafik)
```
GET http://localhost:8000/api/history/EMV10614
GET http://localhost:8000/api/history/EMV10614?last_n_days=3
```

---

### 🗺️ Per Wilayah
```
GET http://localhost:8000/api/wilayah
```

---

### 🗑️ Reset (HATI-HATI)
```
DELETE http://localhost:8000/api/data
```
Hapus semua data, model, cache.

---

## 5. Postman Collection — Import JSON

Simpan sebagai `SmartATM.postman_collection.json` lalu import ke Postman:

```json
{
  "info": { "name": "Smart ATM API v5", "schema": "https://schema.getpostman.com/json/collection/v2.1.0/collection.json" },
  "variable": [{ "key": "base_url", "value": "http://localhost:8000" }],
  "item": [
    { "name": "Health Check",   "request": { "method": "GET", "url": "{{base_url}}/" } },
    { "name": "Status",         "request": { "method": "GET", "url": "{{base_url}}/api/status" } },
    { "name": "Upload Data",    "request": { "method": "POST","url": "{{base_url}}/api/upload", "body": { "mode": "formdata", "formdata": [{ "key": "file", "type": "file" }] } } },
    { "name": "Train Manual",   "request": { "method": "POST","url": "{{base_url}}/api/train" } },
    { "name": "Train Status",   "request": { "method": "GET", "url": "{{base_url}}/api/train/status" } },
    { "name": "Predictions All","request": { "method": "GET", "url": "{{base_url}}/api/predictions?limit=20" } },
    { "name": "Alerts",         "request": { "method": "GET", "url": "{{base_url}}/api/alerts" } },
    { "name": "Summary",        "request": { "method": "GET", "url": "{{base_url}}/api/summary" } },
    { "name": "History ATM",    "request": { "method": "GET", "url": "{{base_url}}/api/history/EMV10614?last_n_days=7" } },
    { "name": "Wilayah",        "request": { "method": "GET", "url": "{{base_url}}/api/wilayah" } }
  ]
}
```

---

## 6. Status Codes

| Code | Arti |
|------|------|
| 200 | OK |
| 400 | Bad request (file salah / kolom kurang) |
| 404 | Data / ATM tidak ditemukan |
| 409 | Conflict (training sedang berjalan) |
| 500 | Internal server error |
