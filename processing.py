"""
processing.py
Semua logic dari Smart ATM V6 Notebook Cell 4 — agar konsisten dengan Colab.

Perubahan V6 vs V5:
  - Status threshold: AMAN >30% | AWAS 20–30% | BONGKAR <20%  (sebelumnya 25/20)
  - status_v6() menggantikan status_saldo()
  - Tambah kolom 'Alamat ATM' di ffill list interpolasi
  - Vendor cleaning menggunakan format "X - SSI WILAYAH YYYY" (uppercase)
"""

import re
import numpy as np
import pandas as pd
from config import (
    REFILL_ABS, REFILL_PCT, ATM_SEPI_AVG72,
    INTERPOLASI_MAX_GAP, CAP_JAM,
    STATUS_AMAN_PCT, STATUS_AWAS_PCT,
)


# ── Helpers ──────────────────────────────────────────

def parse_rupiah(val):
    """
    Parse berbagai format angka rupiah ke float.
    Contoh yang didukung:
      'Rp. 500.000.000 ,-'  → 500000000.0
      'Rp 41.475.000,-'     → 41475000.0
      '41475000.0'          → 41475000.0
      506250000.0           → 506250000.0
    """
    if isinstance(val, (int, float)):
        return float(val) if not (isinstance(val, float) and np.isnan(val)) else np.nan
    if val is None:
        return np.nan
    try:
        if pd.isna(val): return np.nan
    except: pass
    s = str(val).strip()
    s = re.sub(r'(?i)rp\.?\s*', '', s).strip()
    s = re.sub(r'[\s,\-]+$', '', s).strip()
    if re.match(r'^\d{1,3}(\.\d{3})+$', s):
        s = s.replace('.', '')
    else:
        s = re.sub(r'[^\d\.]', '', s)
        if s.count('.') > 1:
            s = s.replace('.', '')
    try:
        return float(s)
    except:
        return np.nan


def est_jam_cascade(row):
    """
    Cascade fallback 6j → 24j → 72j.
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
    Status prediksi V6:
      OVERFUND   : pct > 100
      BONGKAR    : pct <= 20  ATAU est_jam < 6 jam
      AWAS       : pct <= 30  ATAU est_jam < 24 jam
      PERLU PANTAU: est_jam < 72 jam
      AMAN       : est_jam >= 72 jam atau tidak ada histori
    """
    if pct > 100:   return 'OVERFUND'
    if pct <= 20:   return 'BONGKAR'
    if pct <= 30:   return 'AWAS'
    if est_jam is None: return 'AMAN'
    elif est_jam < 6:   return 'BONGKAR'
    elif est_jam < 24:  return 'AWAS'
    elif est_jam < 72:  return 'PERLU PANTAU'
    else:               return 'AMAN'


def status_saldo(pct):
    """
    Status saldo V6 berdasarkan persentase saldo terhadap limit.
    AMAN >30% | AWAS 20–30% | BONGKAR <20%
    """
    if pct > 100:   return 'OVERFUND'
    elif pct > 30:  return 'AMAN'
    elif pct > 20:  return 'AWAS'
    else:           return 'BONGKAR'


# ── Vendor cleaning ───────────────────────────────────

VALID_VENDOR = [
    '1 - SSI WILAYAH PEKANBARU',
    '2 - SSI WILAYAH BATAM',
    '3 - SSI WILAYAH TANJUNG PINANG',
    '4 - SSI WILAYAH DUMAI',
]

WILAYAH_KNOWN = [
    'Pekanbaru', 'Batam', 'Dumai', 'Tanjung Pinang',
    'Tanjungpinang', 'Tanjung pinang',
]


def _clean_vendor(df: pd.DataFrame) -> pd.DataFrame:
    """
    Filter ATM hanya yang memiliki Vendor valid (4 wilayah SSI).
    ATM tanpa vendor valid dibuang agar tidak mencemari prediksi.
    Jika tidak ada vendor valid sama sekali, fallback ke normalisasi biasa.
    """
    if 'Vendor' not in df.columns:
        return df

    df['Vendor'] = df['Vendor'].astype(str).str.upper().str.strip()

    df_valid = df[df['Vendor'].isin(VALID_VENDOR)].copy()

    if len(df_valid) > 0:
        # Petakan setiap ID ATM ke vendor paling sering muncul
        vendor_map = (
            df_valid.groupby('ID ATM')['Vendor']
            .agg(lambda x: x.mode()[0])
            .to_dict()
        )
        df['Vendor_Final'] = df['ID ATM'].map(vendor_map)
        df = df[df['Vendor_Final'].notna()].copy()
        df['Vendor'] = df['Vendor_Final']
        df = df.drop(columns=['Vendor_Final'])

    # Ekstrak nama wilayah dari Vendor
    df['Wilayah'] = df['Vendor'].str.extract(r'WILAYAH\s+(.+)$', expand=False).str.strip()

    # Normalize Tanjungpinang → Tanjung Pinang
    df['Wilayah'] = df['Wilayah'].str.replace(
        r'(?i)tanjungpinang|tanjung\s*pinang', 'Tanjung Pinang', regex=True
    )

    # Fallback: cek apakah Vendor sendiri adalah nama wilayah
    mask_null = df['Wilayah'].isna()
    if mask_null.any():
        vendor_clean = df.loc[mask_null, 'Vendor'].astype(str).str.strip()
        for w in WILAYAH_KNOWN:
            matched = vendor_clean.str.contains(w, case=False, na=False)
            df.loc[mask_null & matched, 'Wilayah'] = w

    df['Wilayah'] = df['Wilayah'].fillna(df['Vendor'].astype(str).str.strip())

    return df


# ── Main pipeline ─────────────────────────────────────

def process_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Full processing pipeline V6.
    Input  : df mentah (kolom: ID ATM, Sisa Saldo, Limit, Tanggal, Jam, ...)
    Output : df lengkap dengan semua fitur siap pakai

    Status threshold V6: AMAN >30% | AWAS 20–30% | BONGKAR <20%
    """

    # ── 0. Normalisasi ID ATM ────────────────────────────
    if 'ID ATM' in df.columns:
        df['ID ATM'] = df['ID ATM'].astype(str).str.strip().str.upper()
        df = df[~df['ID ATM'].isin(['', 'NAN', 'NONE', 'NULL'])]

    # ── 1. Pastikan Sisa Saldo & Limit numerik ───────────
    for col in ['Sisa Saldo', 'Limit']:
        if col not in df.columns:
            continue
        converted = pd.to_numeric(df[col], errors='coerce')
        mask_failed = converted.isna() & df[col].notna()
        if mask_failed.any():
            converted[mask_failed] = df.loc[mask_failed, col].apply(parse_rupiah)
        df[col] = converted

    df = df.dropna(subset=['ID ATM', 'Sisa Saldo', 'Limit'])

    # ── 2. Normalisasi kategori ──────────────────────────
    if 'Merk ATM' in df.columns:
        df['Merk ATM'] = df['Merk ATM'].astype(str).str.upper().str.strip()
        df['Tipe ATM'] = df['ID ATM'].str[:3].str.upper()

    # Vendor cleaning V6 (filter 4 wilayah SSI)
    df = _clean_vendor(df)

    # ── 3. Datetime ──────────────────────────────────────
    if 'datetime' not in df.columns:
        df['datetime'] = pd.to_datetime(
            df['Tanggal'].astype(str) + ' ' + df['Jam'].astype(str) + ':00'
        )
    else:
        df['datetime'] = pd.to_datetime(df['datetime'])

    df = df.drop_duplicates(subset=['ID ATM', 'datetime'])
    df = df.sort_values(['ID ATM', 'datetime']).reset_index(drop=True)
    df['ID ATM'] = df['ID ATM'].astype(str).str.strip().str.upper()

    # ── 4. Deteksi Refill dual-threshold ─────────────────
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

    # ── 5. Interpolasi cerdas + flag + clip ──────────────
    hasil_interp = []
    full_range   = pd.date_range(df['datetime'].min(), df['datetime'].max(), freq='1h')

    for atm_id, grp in df.groupby('ID ATM'):
        grp = grp.set_index('datetime').sort_index()
        grp['Is_Interpolated'] = 0
        grp = grp.reindex(full_range)
        grp['Is_Interpolated'] = grp['Is_Interpolated'].fillna(1).astype(int)

        # V6: tambah 'Alamat ATM' di list ffill
        for col in ['ID ATM', 'Merk ATM', 'Lokasi ATM', 'Alamat ATM',
                    'Vendor', 'Wilayah', 'Tipe ATM']:
            if col in grp.columns:
                grp[col] = grp[col].ffill().bfill()
        grp['ID ATM'] = atm_id
        grp['Limit']  = grp['Limit'].ffill().bfill()

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

        # Clip saldo ≤ Limit
        grp['Sisa Saldo'] = saldo_filled.clip(lower=0)
        grp['Sisa Saldo'] = grp[['Sisa Saldo', 'Limit']].min(axis=1)

        # Recalculate refill & penarikan setelah interpolasi
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

    # ── 6. Fitur turunan ─────────────────────────────────
    df['Persentase']   = (df['Sisa Saldo'] / df['Limit'] * 100).clip(0, 100)
    df['Jam Int']      = df['datetime'].dt.hour
    df['Hari Minggu']  = df['datetime'].dt.dayofweek
    df['Is Weekend']   = (df['Hari Minggu'] >= 5).astype(int)
    df['Is Jam Sibuk'] = df['Jam Int'].apply(
        lambda h: 1 if (7 <= h <= 12) or (16 <= h <= 21) else 0
    )

    # ── 7. Rolling windows ────────────────────────────────
    for w, col in [(6, '6j'), (24, '24j'), (72, '72j')]:
        df[f'Avg Penarikan {col}'] = df.groupby('ID ATM')['Penarikan'].transform(
            lambda x: x.rolling(w, min_periods=1).mean()
        )
    df['Std Penarikan 24j'] = df.groupby('ID ATM')['Penarikan'].transform(
        lambda x: x.rolling(24, min_periods=1).std().fillna(0)
    )

    # ── 8. Flag ATM Sepi ─────────────────────────────────
    df['Is_ATM_Sepi'] = (df['Avg Penarikan 72j'] < ATM_SEPI_AVG72).astype(int)

    # ── 9. Lag features ───────────────────────────────────
    for lag in [1, 2, 3, 6, 12]:
        df[f'Saldo Lag {lag}j'] = df.groupby('ID ATM')['Sisa Saldo'].shift(lag)
        df[f'Tarik Lag {lag}j'] = df.groupby('ID ATM')['Penarikan'].shift(lag)

    lag_cols = [c for c in df.columns if 'Lag' in c]
    df[lag_cols] = df[lag_cols].fillna(0)

    # ── 10. Est Jam Habis cascade & Status V6 ────────────
    df['Est Jam Habis Rule'] = df.apply(est_jam_cascade, axis=1)
    df['Status']             = df['Persentase'].apply(status_saldo)

    return df