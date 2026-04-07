from pathlib import Path

BASE_DIR      = Path(__file__).parent
DATA_DIR      = BASE_DIR / "data"

# Update ke V6 agar tidak bentrok dengan model lama
MODEL_PATH    = DATA_DIR / "xgboost_atm_v6.pkl"
FITUR_PATH    = DATA_DIR / "xgboost_fitur_v6.pkl"
PROCESSED_CSV = DATA_DIR / "processed_data.csv"
PRED_CACHE    = DATA_DIR / "predictions_cache.json"

DATA_DIR.mkdir(exist_ok=True)

# ── Thresholds Status V6 (Tambahkan Bagian Ini) ──
# Sesuai deskripsi main.py: AMAN >25% | AWAS 20-25% | BONGKAR <20%
STATUS_AMAN_PCT    = 0.25
STATUS_AWAS_PCT    = 0.20
STATUS_BONGKAR_PCT = 0.20

# ── Processing thresholds (Sesuai V6) ──
REFILL_ABS          = -5_000_000
REFILL_PCT          = 0.05
ATM_SEPI_AVG72      = 50_000
INTERPOLASI_MAX_GAP = 3
CAP_JAM             = 168

# Tambahkan ini di config.py
STATUS_AMAN_PCT = 0.25    # > 25%
STATUS_AWAS_PCT = 0.20    # 20% - 25%
# BONGKAR otomatis di bawah 20%

WILAYAH_LIST = ["Pekanbaru", "Batam", "Dumai", "Tanjung Pinang"]