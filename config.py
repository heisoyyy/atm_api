from pathlib import Path

BASE_DIR      = Path(__file__).parent
DATA_DIR      = BASE_DIR / "data"
MODEL_PATH    = DATA_DIR / "xgboost_atm_v5.pkl"
FITUR_PATH    = DATA_DIR / "xgboost_fitur_v5.pkl"
PROCESSED_CSV = DATA_DIR / "processed_data.csv"
PRED_CACHE    = DATA_DIR / "predictions_cache.json"

DATA_DIR.mkdir(exist_ok=True)

# Processing thresholds (sama persis dengan V5 notebook)
REFILL_ABS          = -5_000_000
REFILL_PCT          = 0.05
ATM_SEPI_AVG72      = 50_000
INTERPOLASI_MAX_GAP = 3
CAP_JAM             = 168

WILAYAH_LIST = ["Pekanbaru", "Batam", "Dumai", "Tanjung Pinang"]
