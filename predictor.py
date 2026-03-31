"""
predictor.py
Bangun prediction list dari snapshot terbaru tiap ATM.
"""

import json
import numpy as np
import pandas as pd
import joblib
from datetime import datetime, timedelta
from typing import Optional

from config import MODEL_PATH, FITUR_PATH, PRED_CACHE, CAP_JAM
from processing import est_jam_cascade, pred_status


def load_model():
    """Load model dari disk. Return (model, fitur) atau (None, [])."""
    if MODEL_PATH.exists() and FITUR_PATH.exists():
        return joblib.load(MODEL_PATH), joblib.load(FITUR_PATH)
    return None, []


def build_predictions(df: pd.DataFrame) -> list:
    """
    Ambil snapshot terbaru tiap ATM, jalankan prediksi, return list dict.
    """
    model_xgb, fitur_aktif = load_model()

    result      = []
    latest_snap = df.loc[df.groupby('ID ATM')['datetime'].idxmax()].copy()

    for _, row in latest_snap.iterrows():
        atm     = row['ID ATM']
        saldo   = float(row['Sisa Saldo'])
        limit   = float(row['Limit'])
        pct     = float(row['Persentase'])
        avg6j   = max(float(row.get('Avg Penarikan 6j',  0) or 0), 0)
        avg24j  = max(float(row.get('Avg Penarikan 24j', 0) or 0), 0)
        avg72j  = max(float(row.get('Avg Penarikan 72j', 0) or 0), 0)
        is_sepi = int(row.get('Is_ATM_Sepi', 0) or 0)

        # ── Prediksi ──────────────────────────────────────
        if model_xgb is not None and fitur_aktif:
            try:
                fitur_ok = [f for f in fitur_aktif if f in row.index]
                X_pred   = row[fitur_ok].fillna(0).values.reshape(1, -1)
                est_jam  = float(model_xgb.predict(X_pred)[0])
                est_jam  = max(0.0, min(est_jam, float(CAP_JAM)))
                metode   = 'XGBoost'
                # Blend untuk ATM sepi
                if is_sepi and avg72j > 0:
                    est_rb  = min(saldo / avg72j, float(CAP_JAM))
                    est_jam = 0.3 * est_jam + 0.7 * est_rb
                    metode  = 'XGBoost+Sepi72j'
            except Exception as e:
                est_jam, metode = _rule_based(saldo, avg6j, avg24j, avg72j)
        else:
            est_jam, metode = _rule_based(saldo, avg6j, avg24j, avg72j)

        # ── Effective rate ────────────────────────────────
        avg_eff  = avg6j if avg6j > 0 else (avg24j if avg24j > 0 else avg72j)
        pred_12j = max(0.0, saldo - avg_eff * 12) if avg_eff > 0 else saldo

        # ── Waktu habis & jadwal isi ──────────────────────
        now_dt    = pd.to_datetime(row['datetime'])
        tgl_habis = jam_habis = tgl_isi = jam_isi = None
        if est_jam is not None:
            habis_dt  = now_dt + timedelta(hours=est_jam)
            isi_dt    = habis_dt - timedelta(hours=6)
            tgl_habis = habis_dt.strftime('%Y-%m-%d')
            jam_habis = habis_dt.strftime('%H:%M')
            tgl_isi   = isi_dt.strftime('%Y-%m-%d')
            jam_isi   = isi_dt.strftime('%H:%M')

        # ── Status & Skor ─────────────────────────────────
        status_pred = pred_status(est_jam, pct)
        skor_pct    = (1 - pct / 100) * 40
        skor_laju   = (avg_eff / max(limit, 1)) * 100 * 30
        skor_est    = (1 - min(est_jam, CAP_JAM) / CAP_JAM) * 30 if est_jam is not None else 0
        skor_total  = round(min(skor_pct + skor_laju + skor_est, 100), 1)

        result.append({
            "id_atm":          atm,
            "tipe":            str(row.get('Tipe ATM', '-') or '-'),
            "lokasi":          str(row.get('Lokasi ATM', '-') or '-'),
            "wilayah":         str(row.get('Wilayah', '-') or '-'),
            "saldo":           saldo,
            "limit":           limit,
            "pct_saldo":       round(pct, 1),
            "tarik_per_jam":   round(avg_eff, 0),
            "pred_saldo_12j":  round(pred_12j, 0),
            "est_jam":         round(est_jam, 1) if est_jam is not None else None,
            "est_hari":        round(est_jam / 24, 2) if est_jam is not None else None,
            "tgl_habis":       tgl_habis,
            "jam_habis":       jam_habis,
            "tgl_isi":         tgl_isi,
            "jam_isi":         jam_isi,
            "status":          status_pred,
            "skor_urgensi":    float(skor_total),
            "atm_sepi":        bool(is_sepi),
            "metode":          metode,
            "last_update":     str(row['datetime']),
        })

    result.sort(key=lambda x: x['skor_urgensi'], reverse=True)
    for i, r in enumerate(result):
        r['ranking'] = i + 1

    return result


def save_cache(predictions: list):
    payload = {
        "generated_at": datetime.now().isoformat(),
        "count":        len(predictions),
        "data":         predictions,
    }
    with open(PRED_CACHE, 'w') as f:
        json.dump(payload, f, default=str)


def load_cache() -> Optional[dict]:
    if PRED_CACHE.exists():
        with open(PRED_CACHE) as f:
            return json.load(f)
    return None


# ── Private ───────────────────────────────────────────

def _rule_based(saldo, avg6j, avg24j, avg72j):
    """Cascade rule-based fallback."""
    for avg, label in [(avg6j, 'Rule-6j'), (avg24j, 'Rule-24j'), (avg72j, 'Rule-72j')]:
        if avg > 0:
            est = saldo / avg
            if est <= CAP_JAM:
                return est, label
    return None, 'NO DATA'
