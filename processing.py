"""
processing.py
Semua logic dari Smart ATM V5 Notebook Cell 4 — agar konsisten dengan Colab.
"""

import re
import numpy as np
import pandas as pd
from config import REFILL_ABS, REFILL_PCT, ATM_SEPI_AVG72, INTERPOLASI_MAX_GAP, CAP_JAM


# ── Helpers ──────────────────────────────────────────

def parse_rupiah(val):
    """
    Parse berbagai format angka rupiah ke float.
    Contoh yang didukung:
      'Rp. 500.000.000 ,-'  → 500000000.0
      'Rp 41.475.000,-'     → 41475000.0
      '41475000.0'          → 41475000.0   (float string, titik = desimal)
      506250000.0           → 506250000.0  (sudah float, langsung return)
    """
    # Sudah numerik → langsung return
    if isinstance(val, (int, float)):
        return float(val) if not (isinstance(val, float) and np.isnan(val)) else np.nan
    if val is None:
        return np.nan
    try:
        if pd.isna(val): return np.nan
    except: pass
    s = str(val).strip()
    # Hapus prefix Rp dan variasinya
    s = re.sub(r'(?i)rp\.?\s*', '', s).strip()
    # Hapus suffix ,- dan spasi di akhir
    s = re.sub(r'[\s,\-]+$', '', s).strip()
    # Format ribuan titik: "41.475.000" atau "500.000.000"
    if re.match(r'^\d{1,3}(\.\d{3})+$', s):
        s = s.replace('.', '')
    else:
        # Hapus semua non-digit kecuali titik
        s = re.sub(r'[^\d\.]', '', s)
        # Lebih dari satu titik → bukan desimal, hapus semua titik
        if s.count('.') > 1:
            s = s.replace('.', '')
    try:
        return float(s)
    except:
        return np.nan


def est_jam_cascade(row):
    """
    FIX #4: Cascade fallback 6j → 24j → 72j.
    Jika hasil > CAP_JAM, turun ke window lebih panjang.
    Return None jika semua window → saldo aman > 7 hari.
    """
    s = row['Sisa Saldo']
    if s <= 0:
        return 0.0
    for col in ['Avg Penarikan 6j', 'Avg Penarikan 24j', 'Avg Penarikan 72j']:
        avg = row.get(col, 0)
        if avg > 0:
            est = s / avg
            if est <= CAP_JAM:
                return est
    return None  # saldo aman > 7 hari


def pred_status(est_jam, pct):
    """
    FIX #5: BONGKAR (OVERFUND) hanya dari pct > 100, bukan dari est_jam >= 168.
    est_jam = None berarti AMAN (tidak ada histori tarik / saldo sangat aman).
    """
    if pct > 100:
        return 'BONGKAR (OVERFUND)'
    if est_jam is None:
        return 'AMAN'
    elif est_jam < 6:
        return 'KRITIS'
    elif est_jam < 24:
        return 'SEGERA ISI'
    elif est_jam < 72:
        return 'PERLU DIPANTAU'
    else:
        return 'AMAN'


def status_saldo(pct):
    if pct > 40:
        return 'AMAN'
    elif pct > 25:
        return 'PERHATIAN'
    elif pct > 20:
        return 'AWAS'
    else:
        return 'BONGKAR'


# ── Main pipeline ─────────────────────────────────────

def process_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Full processing pipeline persis sama dengan V5 Cell 4.
    Input  : df mentah (kolom: ID ATM, Sisa Saldo, Limit, Tanggal, Jam, ...)
    Output : df lengkap dengan semua fitur siap pakai
    """

    # ── 1. Parse Rupiah jika masih string ───────────────
    for col in ['Sisa Saldo', 'Limit']:
        if col in df.columns and df[col].dtype == object:
            df[col] = df[col].apply(parse_rupiah)

    df = df.dropna(subset=['ID ATM', 'Sisa Saldo', 'Limit'])

    # ── 2. Normalisasi kategori ──────────────────────────
    if 'Merk ATM' in df.columns:
        df['Merk ATM'] = df['Merk ATM'].str.upper().str.strip()
        df['Tipe ATM'] = df['ID ATM'].str[:3].str.upper()
    if 'Vendor' in df.columns:
        df['Wilayah'] = df['Vendor'].str.extract(r'Wilayah\s+(.+)$', expand=False).str.strip()
        df['Wilayah'] = df['Wilayah'].fillna(df['Vendor'])

    # ── 3. Datetime ──────────────────────────────────────
    if 'datetime' not in df.columns:
        df['datetime'] = pd.to_datetime(
            df['Tanggal'].astype(str) + ' ' + df['Jam'].astype(str) + ':00'
        )
    else:
        df['datetime'] = pd.to_datetime(df['datetime'])

    df = df.drop_duplicates(subset=['ID ATM', 'datetime'])
    df = df.sort_values(['ID ATM', 'datetime']).reset_index(drop=True)

    # ── 4. FIX #2: Deteksi Refill dual-threshold ────────
    df['Selisih']   = df.groupby('ID ATM')['Sisa Saldo'].diff(1) * -1
    saldo_prev      = df.groupby('ID ATM')['Sisa Saldo'].shift(1)
    refill_abs_mask = df['Selisih'] < REFILL_ABS
    refill_pct_mask = (df['Selisih'] < 0) & (
        (-df['Selisih']) / saldo_prev.clip(lower=1) > REFILL_PCT
    )
    df['Is Refill'] = (refill_abs_mask | refill_pct_mask).astype(int)
    df['Penarikan'] = df.apply(
        lambda r: r['Selisih'] if r['Selisih'] > 0 and r['Is Refill'] == 0 else 0,
        axis=1
    )

    # ── 5. FIX #3: Interpolasi cerdas + flag + clip ──────
    hasil_interp = []
    full_range   = pd.date_range(df['datetime'].min(), df['datetime'].max(), freq='1h')

    for atm_id, grp in df.groupby('ID ATM'):
        grp = grp.set_index('datetime').sort_index()
        grp['Is_Interpolated'] = 0
        grp = grp.reindex(full_range)
        grp['Is_Interpolated'] = grp['Is_Interpolated'].fillna(1).astype(int)

        for col in ['ID ATM', 'Merk ATM', 'Lokasi ATM', 'Vendor', 'Wilayah', 'Tipe ATM']:
            if col in grp.columns:
                grp[col] = grp[col].ffill().bfill()
        grp['ID ATM'] = atm_id
        grp['Limit']  = grp['Limit'].ffill().bfill()

        # Gap detection: pendek (<3j) → linear, panjang (≥3j) → ffill
        is_nan   = grp['Sisa Saldo'].isna()
        gap_size = is_nan.astype(int).groupby(
            (is_nan != is_nan.shift()).cumsum()
        ).transform('sum')

        saldo_orig   = grp['Sisa Saldo'].copy()
        saldo_linear = saldo_orig.interpolate('linear')
        saldo_ffill  = saldo_orig.ffill()
        use_linear   = is_nan & (gap_size < INTERPOLASI_MAX_GAP)
        use_ffill    = is_nan & (gap_size >= INTERPOLASI_MAX_GAP)
        saldo_filled = saldo_orig.copy()
        saldo_filled[use_linear] = saldo_linear[use_linear]
        saldo_filled[use_ffill]  = saldo_ffill[use_ffill]

        # FIX #3: Clip saldo ≤ Limit
        grp['Sisa Saldo'] = saldo_filled.clip(lower=0)
        grp['Sisa Saldo'] = grp[['Sisa Saldo', 'Limit']].min(axis=1)

        # Recalculate
        grp['Selisih']  = grp['Sisa Saldo'].diff(1) * -1
        sp2             = grp['Sisa Saldo'].shift(1)
        r_abs2          = grp['Selisih'] < REFILL_ABS
        r_pct2          = (grp['Selisih'] < 0) & (
            (-grp['Selisih']) / sp2.clip(lower=1) > REFILL_PCT
        )
        grp['Is Refill'] = (r_abs2 | r_pct2).astype(int)
        grp['Penarikan'] = grp.apply(
            lambda r: r['Selisih'] if r['Selisih'] > 0 and r['Is Refill'] == 0 else 0,
            axis=1
        )

        grp.index.name = 'datetime'
        grp = grp.reset_index()
        grp['Tanggal'] = grp['datetime'].dt.date
        grp['Jam']     = grp['datetime'].dt.strftime('%H:%M')
        hasil_interp.append(grp)

    df = pd.concat(hasil_interp, ignore_index=True)

    # ── 6. Fitur turunan ────────────────────────────────
    df['Persentase']   = (df['Sisa Saldo'] / df['Limit'] * 100).clip(0, 100)
    df['Jam Int']      = df['datetime'].dt.hour
    df['Hari Minggu']  = df['datetime'].dt.dayofweek
    df['Is Weekend']   = (df['Hari Minggu'] >= 5).astype(int)
    df['Is Jam Sibuk'] = df['Jam Int'].apply(
        lambda h: 1 if (7 <= h <= 12) or (16 <= h <= 21) else 0
    )

    # ── 7. Rolling windows ───────────────────────────────
    for w, col in [(6, '6j'), (24, '24j'), (72, '72j')]:
        df[f'Avg Penarikan {col}'] = df.groupby('ID ATM')['Penarikan'].transform(
            lambda x: x.rolling(w, min_periods=1).mean()
        )
    df['Std Penarikan 24j'] = df.groupby('ID ATM')['Penarikan'].transform(
        lambda x: x.rolling(24, min_periods=1).std().fillna(0)
    )

    # ── 8. FIX #4: Flag ATM Sepi ─────────────────────────
    df['Is_ATM_Sepi'] = (df['Avg Penarikan 72j'] < ATM_SEPI_AVG72).astype(int)

    # ── 9. Lag features ──────────────────────────────────
    for lag in [1, 2, 3, 6, 12]:
        df[f'Saldo Lag {lag}j'] = df.groupby('ID ATM')['Sisa Saldo'].shift(lag)
        df[f'Tarik Lag {lag}j'] = df.groupby('ID ATM')['Penarikan'].shift(lag)

    # FIX #5: Explicit fillna lag
    lag_cols = [c for c in df.columns if 'Lag' in c]
    df[lag_cols] = df[lag_cols].fillna(0)

    # ── 10. FIX #1 + #4: Est Jam Habis cascade ───────────
    df['Est Jam Habis Rule'] = df.apply(est_jam_cascade, axis=1)
    df['Status']             = df['Persentase'].apply(status_saldo)

    return df