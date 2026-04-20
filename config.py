from pathlib import Path

BASE_DIR      = Path(__file__).parent
DATA_DIR      = BASE_DIR / "data"

MODEL_PATH    = DATA_DIR / "xgboost_atm_v6.pkl"
FITUR_PATH    = DATA_DIR / "xgboost_fitur_v6.pkl"
PROCESSED_CSV = DATA_DIR / "processed_data.csv"
PRED_CACHE    = DATA_DIR / "predictions_cache.json"

DATA_DIR.mkdir(exist_ok=True)

# ── Threshold Status V6 ──────────────────────────────────────
# AMAN >35% | PERLU PANTAU 30-35% | AWAS 20-30% | BONGKAR <=20%
STATUS_AMAN_PCT    = 0.35   # > 35%
STATUS_AWAS_PCT    = 0.20   # 20% - 30%
STATUS_BONGKAR_PCT = 0.20   # <= 20%

# ── Auto / Trigger cashplan ──────────────────────────────────
AUTO_CASHPLAN_PCT    = 0.25  # pct <= 25% → auto masuk cashplan
TRIGGER_CASHPLAN_PCT = 0.35  # 25% < pct <= 35% → trigger notif

# ── Processing thresholds ────────────────────────────────────
REFILL_ABS          = -5_000_000
REFILL_PCT          = 0.05
ATM_SEPI_AVG72      = 50_000
INTERPOLASI_MAX_GAP = 3
CAP_JAM             = 168

WILAYAH_LIST = ["Pekanbaru", "Batam", "Dumai", "Tanjung Pinang"]