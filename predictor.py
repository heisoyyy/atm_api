"""
predictor.py — v8
Perubahan vs v6/v7:
  - Entry dict TIDAK lagi menyertakan: tipe, lokasi, wilayah, limit, denom_options
    (kolom ini ada di atm_masters, diambil via JOIN saat query)
  - Sisanya sama: saldo, pct_saldo, kalkulasi, prediksi, status, skor
"""

import json
import numpy as np
import pandas as pd
import joblib
from datetime import datetime, timedelta
from typing import Optional

from config import MODEL_PATH, FITUR_PATH, PRED_CACHE, CAP_JAM, STATUS_AMAN_PCT
from processing import est_jam_cascade, pred_status


def load_model():
    if MODEL_PATH.exists() and FITUR_PATH.exists():
        return joblib.load(MODEL_PATH), joblib.load(FITUR_PATH)
    return None, []


def build_predictions(df: pd.DataFrame) -> list:
    """
    Bangun list prediksi dari snapshot terbaru tiap ATM.
    Output hanya kolom kalkulasi — data statis diambil dari atm_masters via JOIN.
    """
    model_xgb, fitur_aktif = load_model()

    result = []

    df = df.copy()
    df['ID ATM'] = df['ID ATM'].astype(str).str.strip().str.upper()
    df = df[~df['ID ATM'].isin(['', 'NAN', 'NONE', 'NULL', 'ID ATM'])]

    df['Sisa Saldo'] = pd.to_numeric(df['Sisa Saldo'], errors='coerce')
    df['Limit']      = pd.to_numeric(df['Limit'],      errors='coerce')
    df = df.dropna(subset=['Sisa Saldo', 'Limit'])
    df = df[df['Limit'] > 0]

    df['datetime'] = pd.to_datetime(df['datetime'], errors='coerce')
    df = df.dropna(subset=['datetime'])

    latest_snap = df.loc[df.groupby('ID ATM')['datetime'].idxmax()].copy()

    for _, row in latest_snap.iterrows():
        atm   = row['ID ATM']
        saldo = float(row['Sisa Saldo'])
        limit = float(row['Limit'])

        try:
            pct = float(row['Persentase'])
            if pct != pct:
                pct = round(saldo / limit * 100, 1) if limit > 0 else 0.0
        except Exception:
            pct = round(saldo / limit * 100, 1) if limit > 0 else 0.0

        avg6j   = max(float(row.get('Avg Penarikan 6j',  0) or 0), 0)
        avg24j  = max(float(row.get('Avg Penarikan 24j', 0) or 0), 0)
        avg72j  = max(float(row.get('Avg Penarikan 72j', 0) or 0), 0)
        is_sepi = int(row.get('Is_ATM_Sepi', 0) or 0)

        # ── Cash out analytics ──────────────────────────────────────────────
        data_atm  = df[df['ID ATM'] == atm]
        data_asli = (
            data_atm[data_atm['Is_Interpolated'] == 0]
            if 'Is_Interpolated' in data_atm.columns else data_atm
        )

        cashout_per_hari = (
            data_asli.groupby('Tanggal')['Penarikan'].sum()
            if 'Tanggal' in data_asli.columns else pd.Series(dtype=float)
        )
        cashout_per_minggu = (
            data_asli.groupby('Hari Minggu')['Penarikan'].mean()
            if 'Hari Minggu' in data_asli.columns else pd.Series(dtype=float)
        )

        cashout_bulanan = 0.0
        if 'Tanggal' in data_asli.columns and len(data_asli) > 0:
            tmp = data_asli.copy()
            tmp['Bulan'] = pd.to_datetime(tmp['Tanggal']).dt.to_period('M')
            cashout_bulanan = float(tmp.groupby('Bulan')['Penarikan'].sum().mean())

        avg_cashout_harian   = float(cashout_per_hari.mean())       if len(cashout_per_hari)   > 0 else 0.0
        avg_cashout_mingguan = float(cashout_per_minggu.mean()) * 7  if len(cashout_per_minggu) > 0 else 0.0

        # ── Prediksi jam habis ──────────────────────────────────────────────
        if model_xgb is not None and fitur_aktif:
            try:
                fitur_ok = [f for f in fitur_aktif if f in row.index]
                X_pred   = row[fitur_ok].fillna(0).values.reshape(1, -1)
                est_jam  = float(model_xgb.predict(X_pred)[0])
                est_jam  = max(0.0, min(est_jam, float(CAP_JAM)))
                metode   = 'XGBoost'
                if is_sepi and avg72j > 0:
                    est_rb  = min(saldo / avg72j, float(CAP_JAM))
                    est_jam = 0.3 * est_jam + 0.7 * est_rb
                    metode  = 'XGBoost+Sepi72j'
            except Exception:
                est_jam, metode = _rule_based(saldo, avg6j, avg24j, avg72j)
        else:
            est_jam, metode = _rule_based(saldo, avg6j, avg24j, avg72j)

        avg_eff = avg6j if avg6j > 0 else (avg24j if avg24j > 0 else avg72j)

        # ── Prediksi saldo per jam ke depan ────────────────────────────────
        pred_saldo = {}
        for jam_ke in [6, 12, 24, 48, 72]:
            saldo_pred = max(0.0, saldo - avg_eff * jam_ke) if avg_eff > 0 else saldo
            pred_saldo[f'pred_saldo_{jam_ke}j'] = round(saldo_pred, 0)

        # ── Waktu habis, AWAS, jadwal isi ──────────────────────────────────
        now_dt    = pd.to_datetime(row['datetime'])
        tgl_habis = jam_habis = tgl_isi = jam_isi = tgl_awas = jam_awas = None
        est_hari  = None

        if est_jam is not None:
            habis_dt = now_dt + timedelta(hours=est_jam)
            est_hari = est_jam / 24

            saldo_awas_thr = limit * STATUS_AMAN_PCT
            if avg_eff > 0 and saldo > saldo_awas_thr:
                jam_ke_awas = (saldo - saldo_awas_thr) / avg_eff
                awas_dt     = now_dt + timedelta(hours=jam_ke_awas)
            else:
                awas_dt = now_dt

            isi_dt    = awas_dt - timedelta(hours=2)
            tgl_habis = habis_dt.strftime('%Y-%m-%d')
            jam_habis = habis_dt.strftime('%H:%M')
            tgl_awas  = awas_dt.strftime('%Y-%m-%d')
            jam_awas  = awas_dt.strftime('%H:%M')
            tgl_isi   = isi_dt.strftime('%Y-%m-%d')
            jam_isi   = isi_dt.strftime('%H:%M')

        # ── Status & Rekomendasi ────────────────────────────────────────────
        status_pred = pred_status(est_jam, pct)

        if status_pred in ['BONGKAR', 'AWAS']:
            jumlah_isi      = max(0.0, limit - saldo)
            rekomendasi_isi = f'Segera isi Rp {jumlah_isi:,.0f} (target 100% limit)'
        elif status_pred == 'PERLU PANTAU':
            jumlah_isi      = max(0.0, limit - saldo)
            rekomendasi_isi = (
                f'Jadwalkan isi Rp {jumlah_isi:,.0f} sebelum {tgl_isi} {jam_isi}'
                if tgl_isi else f'Jadwalkan isi Rp {jumlah_isi:,.0f}'
            )
        else:
            rekomendasi_isi = 'Tidak perlu isi saat ini'

        # ── Skor Urgensi ────────────────────────────────────────────────────
        skor_pct   = (1 - pct / 100) * 40
        skor_laju  = (avg_eff / max(limit, 1)) * 100 * 30
        skor_est   = (1 - min(est_jam, CAP_JAM) / CAP_JAM) * 30 if est_jam is not None else 0
        skor_total = round(min(skor_pct + skor_laju + skor_est, 100), 1)

        # ── Entry — HANYA kolom kalkulasi ──────────────────────────────────
        # tipe, lokasi, wilayah, limit, denom_options TIDAK disimpan di sini
        # → diambil dari atm_masters via JOIN
        entry = {
            "id_atm":           atm,
            "saldo":            saldo,
            "pct_saldo":        round(pct, 1),
            "tarik_per_jam":    round(avg_eff, 0),
            "cashout_harian":   round(avg_cashout_harian,   0),
            "cashout_mingguan": round(avg_cashout_mingguan, 0),
            "cashout_bulanan":  round(cashout_bulanan,      0),
            **pred_saldo,
            "est_jam":          round(est_jam, 1) if est_jam is not None else None,
            "est_hari":         round(est_hari, 2) if est_hari is not None else None,
            "tgl_awas":         tgl_awas,
            "jam_awas":         jam_awas,
            "tgl_habis":        tgl_habis,
            "jam_habis":        jam_habis,
            "tgl_isi":          tgl_isi,
            "jam_isi":          jam_isi,
            "rekomendasi_isi":  rekomendasi_isi,
            "status":           status_pred,
            "skor_urgensi":     float(skor_total),
            "atm_sepi":         bool(is_sepi),
            "metode":           metode,
            "last_update":      str(row['datetime']),
        }

        result.append(entry)

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


def _rule_based(saldo, avg6j, avg24j, avg72j):
    for avg, label in [(avg6j, 'Rule-6j'), (avg24j, 'Rule-24j'), (avg72j, 'Rule-72j')]:
        if avg > 0:
            est = saldo / avg
            if est <= CAP_JAM:
                return est, label
    return None, 'NO DATA'