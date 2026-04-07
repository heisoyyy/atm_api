"""
trainer.py
Training XGBoost dari processed DataFrame — V6.
Dijalankan sebagai background task saat ada data baru.

Perubahan V6 vs V5:
  - Model disimpan sebagai xgboost_atm_v6.pkl / xgboost_fitur_v6.pkl
  - Tidak ada perubahan arsitektur model (hiperparameter identik)
"""

import numpy as np
import pandas as pd
import joblib
from sklearn.model_selection import TimeSeriesSplit
from sklearn.metrics import mean_absolute_error, r2_score
from xgboost import XGBRegressor

from config import MODEL_PATH, FITUR_PATH, CAP_JAM


FITUR_LIST = [
    'Jam Int', 'Hari Minggu', 'Is Weekend', 'Is Jam Sibuk',
    'Persentase',
    'Avg Penarikan 6j', 'Avg Penarikan 24j', 'Avg Penarikan 72j', 'Std Penarikan 24j',
    'Saldo Lag 1j', 'Saldo Lag 2j', 'Saldo Lag 3j', 'Saldo Lag 6j', 'Saldo Lag 12j',
    'Tarik Lag 1j', 'Tarik Lag 2j', 'Tarik Lag 3j', 'Tarik Lag 6j',
    'Is_ATM_Sepi', 'Is_Interpolated',
]


def train(df: pd.DataFrame, status_callback=None) -> dict:
    """
    Train XGBoost dari processed df.
    status_callback(pct, msg) dipanggil tiap progress update.
    Return dict hasil training.
    """

    def _cb(pct, msg):
        if status_callback:
            status_callback(pct, msg)

    _cb(5, "Menyiapkan fitur training...")

    FITUR = [f for f in FITUR_LIST if f in df.columns]

    df_train = df[FITUR + ['Est Jam Habis Rule']].copy()

    for col in [c for c in FITUR if any(k in c for k in ['Lag', 'Avg', 'Std'])]:
        df_train[col] = df_train[col].fillna(0)

    # Fallback target pakai Avg24j jika cascade None
    mask_none = df_train['Est Jam Habis Rule'].isna()
    if mask_none.any():
        avg24_est = (
            df['Sisa Saldo'] / df['Avg Penarikan 24j'].replace(0, np.nan)
        ).clip(upper=CAP_JAM)
        df_train.loc[mask_none, 'Est Jam Habis Rule'] = avg24_est[mask_none]

    df_train = df_train.dropna(subset=['Est Jam Habis Rule'])
    df_train = df_train[df_train['Est Jam Habis Rule'] < CAP_JAM]

    X = df_train[FITUR].values
    y = df_train['Est Jam Habis Rule'].values

    n_hari   = df['Tanggal'].nunique()
    n_splits = min(5, max(2, n_hari // 5))

    _cb(15, f"Data training: {len(X):,} baris | {n_hari} hari | {n_splits}-fold CV")

    if len(X) < 100:
        _cb(100, "⚠️ Data terlalu sedikit — model tidak dilatih, gunakan Rule-Based cascade")
        return {
            "mae_avg":      None,
            "r2_avg":       None,
            "n_train":      int(len(X)),
            "n_hari":       int(n_hari),
            "n_folds":      0,
            "top_features": [],
            "model_path":   None,
            "note":         "Data < 100 baris, model tidak dilatih",
        }

    model = XGBRegressor(
        n_estimators=400,
        learning_rate=0.05,
        max_depth=5,
        subsample=0.8,
        colsample_bytree=0.8,
        min_child_weight=3,
        reg_alpha=0.1,
        reg_lambda=1.0,
        random_state=42,
        early_stopping_rounds=20,
        eval_metric='mae',
        verbosity=0,
    )

    tscv     = TimeSeriesSplit(n_splits=n_splits)
    mae_list = []
    r2_list  = []

    for fold, (tr, val) in enumerate(tscv.split(X)):
        model.fit(
            X[tr], y[tr],
            eval_set=[(X[val], y[val])],
            verbose=False,
        )
        p = model.predict(X[val])
        mae_list.append(mean_absolute_error(y[val], p))
        r2_list.append(r2_score(y[val], p))

        pct = 15 + int((fold + 1) / n_splits * 60)
        _cb(pct, f"Fold {fold+1}/{n_splits} — MAE: {mae_list[-1]:.2f} jam | R²: {r2_list[-1]:.4f}")

    # Final training semua data
    _cb(80, "Final training semua data...")
    model.fit(X, y, eval_set=[(X, y)], verbose=False)

    # Feature importance
    fi = sorted( 
        zip(FITUR, model.feature_importances_),
        key=lambda x: x[1], reverse=True
    )
    top10 = [{"fitur": f, "importance": round(float(v), 4)} for f, v in fi[:10]]

    # Simpan model V6
    joblib.dump(model, MODEL_PATH)
    joblib.dump(FITUR,  FITUR_PATH)

    _cb(100, "Model V6 disimpan ✅")

    return {
        "mae_avg":      round(float(np.mean(mae_list)), 2),
        "r2_avg":       round(float(np.mean(r2_list)), 4),
        "n_train":      int(len(X)),
        "n_hari":       int(n_hari),
        "n_folds":      n_splits,
        "top_features": top10,
        "model_path":   str(MODEL_PATH),
    }