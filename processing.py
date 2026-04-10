"""
processing.py
Semua logic dari Smart ATM V6 — Full pipeline robust.

Fix V6.1:
  - Fix ValueError: cannot convert NA to integer (nullable boolean pandas 2.x)
  - Fix Vendor matching: case-insensitive (file pakai mixed case "SSI Wilayah Pekanbaru")
  - Fix parse_rupiah: handle pandas StringDtype / NA
  - Fix interpolasi: handle ATM dengan hanya 1 baris data
  - Fix rolling: min_periods robust
  - Tambah validasi: Limit > 0, Sisa Saldo >= 0 sebelum processing

Status threshold V6: AMAN >30% | AWAS 20-30% | BONGKAR <=20%
"""

import re
import numpy as np
import pandas as pd
from config import (
    REFILL_ABS, REFILL_PCT, ATM_SEPI_AVG72,
    INTERPOLASI_MAX_GAP, CAP_JAM,
    STATUS_AMAN_PCT, STATUS_AWAS_PCT,
)


# ── Helpers ──────────────────────────────────────────────────────────────────

def parse_rupiah(val) -> float:
    """
    Parse berbagai format angka rupiah ke float.
    Mendukung:
      'Rp. 500.000.000 ,-'  → 500000000.0
      'Rp 41.475.000,-'     → 41475000.0
      '41475000.0'          → 41475000.0
      506250000.0           → 506250000.0
      pd.NA / None          → np.nan
    """
    if val is None:
        return np.nan
    # Handle pandas NA / NAType
    try:
        if pd.isna(val):
            return np.nan
    except (TypeError, ValueError):
        pass
    if isinstance(val, (int, float)):
        return float(val) if not (isinstance(val, float) and np.isnan(val)) else np.nan

    s = str(val).strip()
    if not s or s.lower() in ('nan', 'none', 'null', 'na', '-'):
        return np.nan
    # Buang prefix Rp
    s = re.sub(r'(?i)rp\.?\s*', '', s).strip()
    # Buang trailing spasi, koma, tanda minus, titik koma
    s = re.sub(r'[\s,\-;]+$', '', s).strip()
    # Format ribuan: 600.000.000
    if re.match(r'^\d{1,3}(\.\d{3})+$', s):
        s = s.replace('.', '')
    else:
        # Buang semua non-digit kecuali titik desimal
        s = re.sub(r'[^\d\.]', '', s)
        if s.count('.') > 1:
            s = s.replace('.', '')
    if not s:
        return np.nan
    try:
        return float(s)
    except ValueError:
        return np.nan


def est_jam_cascade(row) -> float | None:
    """
    Cascade fallback 6j → 24j → 72j.
    Jika hasil > CAP_JAM, turun ke window lebih panjang.
    Return None jika semua window → saldo aman > 7 hari.
    """
    s = row.get('Sisa Saldo', 0)
    if pd.isna(s) or s <= 0:
        return 0.0
    for col in ['Avg Penarikan 6j', 'Avg Penarikan 24j', 'Avg Penarikan 72j']:
        avg = row.get(col, 0)
        if pd.isna(avg) or avg <= 0:
            continue
        est = s / avg
        if est <= CAP_JAM:
            return float(est)
    return None  # saldo aman > 7 hari


def pred_status(est_jam, pct) -> str:
    """
    Status prediksi V6:
      OVERFUND    : pct > 100
      BONGKAR     : pct <= 20  ATAU est_jam < 6 jam
      AWAS        : pct <= 30  ATAU est_jam < 24 jam
      PERLU PANTAU: est_jam < 72 jam
      AMAN        : est_jam >= 72 jam atau tidak ada histori
    """
    if pct is None or pd.isna(pct):
        return 'NO DATA'
    pct = float(pct)
    if pct > 100:
        return 'OVERFUND'
    if pct <= 20:
        return 'BONGKAR'
    if pct <= 30:
        return 'AWAS'
    if est_jam is None or pd.isna(est_jam):
        return 'AMAN'
    est_jam = float(est_jam)
    if est_jam < 6:
        return 'BONGKAR'
    elif est_jam < 24:
        return 'AWAS'
    elif est_jam < 72:
        return 'PERLU PANTAU'
    else:
        return 'AMAN'


def status_saldo(pct) -> str:
    """
    Status saldo V6 berdasarkan persentase saldo terhadap limit.
    AMAN >30% | AWAS 20-30% | BONGKAR <=20%
    """
    if pct is None or pd.isna(pct):
        return 'NO DATA'
    pct = float(pct)
    if pct > 100:
        return 'OVERFUND'
    elif pct > 30:
        return 'AMAN'
    elif pct > 20:
        return 'AWAS'
    else:
        return 'BONGKAR'


# ── Vendor cleaning ───────────────────────────────────────────────────────────

# Pattern wilayah yang dikenali (case-insensitive match pada kode vendor)
WILAYAH_VENDOR_MAP = {
    'PEKANBARU':      'Pekanbaru',
    'BATAM':          'Batam',
    'DUMAI':          'Dumai',
    'TANJUNG PINANG': 'Tanjung Pinang',
    'TANJUNGPINANG':  'Tanjung Pinang',
}

WILAYAH_KNOWN = [
    'Pekanbaru', 'Batam', 'Dumai', 'Tanjung Pinang', 'Tanjungpinang',
]

# Nomor vendor yang valid (1-4 sesuai 4 wilayah SSI)
VALID_VENDOR_NUMBERS = {'1', '2', '3', '4'}


def _extract_wilayah_from_vendor(vendor_str: str) -> str | None:
    """
    Ekstrak nama wilayah dari string vendor.
    Contoh input: '1 - SSI Wilayah Pekanbaru', '2 - SSI WILAYAH BATAM'
    Return: 'Pekanbaru', 'Batam', dll. atau None jika tidak dikenali.
    """
    if not vendor_str or pd.isna(vendor_str):
        return None
    s = str(vendor_str).upper().strip()
    # Cari setelah kata WILAYAH
    m = re.search(r'WILAYAH\s+(.+)$', s)
    if m:
        raw = m.group(1).strip()
        # Normalize
        for key, val in WILAYAH_VENDOR_MAP.items():
            if raw.startswith(key):
                return val
        return raw.title()
    # Fallback: cari nama wilayah langsung
    for key, val in WILAYAH_VENDOR_MAP.items():
        if key in s:
            return val
    return None


def _is_valid_vendor(vendor_str: str) -> bool:
    """
    Cek apakah vendor string termasuk 4 wilayah SSI yang valid.
    Toleran terhadap mixed case: '1 - SSI Wilayah Pekanbaru' valid.
    """
    if not vendor_str or pd.isna(vendor_str):
        return False
    s = str(vendor_str).strip()
    # Cek nomor di awal (1-4)
    m = re.match(r'^([1-4])\s*[-–]\s*', s)
    if m:
        return True
    # Fallback: ada kata SSI dan WILAYAH
    su = s.upper()
    if 'SSI' in su and 'WILAYAH' in su:
        return True
    return False


def _clean_vendor(df: pd.DataFrame) -> pd.DataFrame:
    """
    Filter & normalize vendor. Toleran terhadap mixed case.
    Jika tidak ada vendor valid sama sekali, tambahkan Wilayah = 'Unknown'.
    """
    if 'Vendor' not in df.columns:
        df['Wilayah'] = 'Unknown'
        return df

    # Normalize ke string
    df['Vendor'] = df['Vendor'].fillna('').astype(str).str.strip()

    # Flag vendor valid
    df['_vendor_valid'] = df['Vendor'].apply(_is_valid_vendor)

    df_valid = df[df['_vendor_valid']].copy()

    if len(df_valid) > 0:
        # Petakan tiap ID ATM ke vendor paling sering muncul (dari baris valid)
        vendor_map = (
            df_valid.groupby('ID ATM')['Vendor']
            .agg(lambda x: x.mode().iloc[0])
            .to_dict()
        )
        df['Vendor_Final'] = df['ID ATM'].map(vendor_map)
        # Buang ATM yang tidak punya vendor valid sama sekali
        df = df[df['Vendor_Final'].notna()].copy()
        df['Vendor'] = df['Vendor_Final']
        df = df.drop(columns=['Vendor_Final'], errors='ignore')
    else:
        # Tidak ada vendor valid — tetap proses semua tapi tandai Wilayah Unknown
        pass

    df = df.drop(columns=['_vendor_valid'], errors='ignore')

    # Ekstrak Wilayah dari Vendor
    df['Wilayah'] = df['Vendor'].apply(_extract_wilayah_from_vendor)

    # Fallback Wilayah untuk baris yang masih kosong
    mask_null = df['Wilayah'].isna() | (df['Wilayah'] == '')
    if mask_null.any():
        df.loc[mask_null, 'Wilayah'] = df.loc[mask_null, 'Vendor'].apply(
            lambda v: next(
                (wv for wk, wv in WILAYAH_VENDOR_MAP.items() if wk in str(v).upper()),
                'Unknown'
            )
        )

    df['Wilayah'] = df['Wilayah'].fillna('Unknown')

    return df


# ── Boolean helper (fix nullable boolean → int) ───────────────────────────────

def _bool_to_int(series: pd.Series) -> pd.Series:
    """
    Convert boolean/nullable-boolean Series ke int (0/1) dengan aman.
    NA → 0.
    """
    return series.fillna(False).astype(bool).astype(int)


# ── Main pipeline ─────────────────────────────────────────────────────────────

def process_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Full processing pipeline V6.
    Input  : df mentah (kolom: ID ATM, Sisa Saldo, Limit, Tanggal, Jam, ...)
    Output : df lengkap dengan semua fitur siap pakai.

    Robust terhadap:
    - Nullable boolean (pandas 2.x dtype_backend="numpy_nullable")
    - Rupiah format string ('Rp. 600.000.000 ,-')
    - Vendor mixed case
    - ATM dengan hanya 1 baris data
    - Kolom tidak lengkap
    """

    df = df.copy()

    # ── 0. Normalisasi ID ATM ─────────────────────────────────────────────────
    if 'ID ATM' in df.columns:
        df['ID ATM'] = df['ID ATM'].astype(str).str.strip().str.upper()
        df = df[~df['ID ATM'].isin(['', 'NAN', 'NONE', 'NULL', 'ID ATM'])]

    if df.empty:
        raise ValueError("Tidak ada data valid setelah normalisasi ID ATM.")

    # ── 1. Pastikan Sisa Saldo & Limit numerik ────────────────────────────────
    for col in ['Sisa Saldo', 'Limit']:
        if col not in df.columns:
            if col == 'Limit':
                raise ValueError(f"Kolom '{col}' tidak ditemukan dalam data.")
            df[col] = 0
            continue

        # Coba to_numeric dulu (cepat untuk angka)
        converted = pd.to_numeric(df[col], errors='coerce')
        # Untuk yang gagal (string rupiah), parse manual
        mask_failed = converted.isna() & df[col].notna() & (df[col].astype(str).str.strip() != '')
        if mask_failed.any():
            converted[mask_failed] = df.loc[mask_failed, col].apply(parse_rupiah)
        df[col] = pd.to_numeric(converted, errors='coerce')

    # Buang baris tanpa ID/Saldo/Limit valid
    df = df.dropna(subset=['ID ATM', 'Sisa Saldo', 'Limit'])
    df = df[df['Limit'] > 0]
    df = df[df['Sisa Saldo'] >= 0]

    if df.empty:
        raise ValueError("Tidak ada data valid setelah filter Saldo/Limit.")

    # ── 2. Normalisasi kategori ───────────────────────────────────────────────
    if 'Merk ATM' in df.columns:
        df['Merk ATM'] = df['Merk ATM'].astype(str).str.upper().str.strip()

    # Tipe ATM dari 3 huruf pertama ID ATM
    df['Tipe ATM'] = df['ID ATM'].str[:3].str.upper()

    # Vendor cleaning (case-insensitive, extract Wilayah)
    df = _clean_vendor(df)

    # ── 3. Datetime ───────────────────────────────────────────────────────────
    if 'datetime' not in df.columns:
        # Pastikan Tanggal dan Jam ada
        if 'Tanggal' not in df.columns:
            df['Tanggal'] = pd.Timestamp.now().strftime('%Y-%m-%d')
        if 'Jam' not in df.columns:
            df['Jam'] = pd.Timestamp.now().strftime('%H:00')

        df['datetime'] = pd.to_datetime(
            df['Tanggal'].astype(str).str.strip() + ' '
            + df['Jam'].astype(str).str.strip() + ':00',
            errors='coerce',
        )
    else:
        df['datetime'] = pd.to_datetime(df['datetime'], errors='coerce')

    df = df.dropna(subset=['datetime'])
    df = df.drop_duplicates(subset=['ID ATM', 'datetime'])
    df = df.sort_values(['ID ATM', 'datetime']).reset_index(drop=True)

    # ── 4. Deteksi Refill dual-threshold (pre-interpolasi) ────────────────────
    df['Selisih'] = df.groupby('ID ATM')['Sisa Saldo'].diff(1) * -1
    saldo_prev    = df.groupby('ID ATM')['Sisa Saldo'].shift(1)

    refill_abs_mask = df['Selisih'] < REFILL_ABS
    # Hindari divide by zero
    saldo_prev_safe = saldo_prev.clip(lower=1).fillna(1)
    refill_pct_mask = (df['Selisih'] < 0) & (
        (-df['Selisih']) / saldo_prev_safe > REFILL_PCT
    )

    # FIX UTAMA: fillna(False) sebelum astype(int) — untuk nullable boolean
    df['Is Refill'] = _bool_to_int(refill_abs_mask | refill_pct_mask)
    # Hitung Penarikan secara vectorized (hindari apply dengan NA)
    selisih_clean = pd.to_numeric(df['Selisih'], errors='coerce').fillna(0)
    df['Penarikan'] = np.where(
        (selisih_clean > 0) & (df['Is Refill'] == 0),
        selisih_clean,
        0,
    )

    # ── 5. Interpolasi per ATM + flag ─────────────────────────────────────────
    hasil_interp = []
    dt_min = df['datetime'].min()
    dt_max = df['datetime'].max()

    # Jika range < 1 jam, buat range minimal 1 jam agar tidak error
    if dt_min == dt_max:
        dt_max = dt_min + pd.Timedelta(hours=1)

    full_range = pd.date_range(dt_min, dt_max, freq='1h')

    # Kolom yang perlu di-ffill (metadata ATM)
    FFILL_COLS = ['ID ATM', 'Merk ATM', 'Tipe ATM', 'Lokasi ATM', 'Alamat ATM',
                  'Vendor', 'Wilayah']

    for atm_id, grp in df.groupby('ID ATM'):
        grp = grp.set_index('datetime').sort_index()
        grp['Is_Interpolated'] = 0

        # Reindex ke full hourly range
        grp = grp.reindex(full_range)
        grp['Is_Interpolated'] = grp['Is_Interpolated'].fillna(1).astype(int)

        # ffill metadata
        for col in FFILL_COLS:
            if col in grp.columns:
                grp[col] = grp[col].ffill().bfill()
        grp['ID ATM'] = atm_id  # Pastikan tidak hilang
        grp['Limit']  = grp['Limit'].ffill().bfill()

        # Hitung gap size untuk memilih metode interpolasi
        is_nan   = grp['Sisa Saldo'].isna()
        gap_id   = (is_nan != is_nan.shift()).cumsum()
        gap_size = is_nan.astype(int).groupby(gap_id).transform('sum')

        saldo_linear = grp['Sisa Saldo'].interpolate('linear')
        saldo_ffill  = grp['Sisa Saldo'].ffill()
        use_linear   = is_nan & (gap_size < INTERPOLASI_MAX_GAP)
        use_ffill    = is_nan & (gap_size >= INTERPOLASI_MAX_GAP)

        saldo_filled = grp['Sisa Saldo'].copy()
        saldo_filled[use_linear] = saldo_linear[use_linear]
        saldo_filled[use_ffill]  = saldo_ffill[use_ffill]

        # Clip: 0 <= saldo <= limit
        grp['Sisa Saldo'] = saldo_filled.clip(lower=0)
        grp['Sisa Saldo'] = grp[['Sisa Saldo', 'Limit']].min(axis=1)

        # Recalculate refill & penarikan setelah interpolasi
        grp['Selisih']    = grp['Sisa Saldo'].diff(1) * -1
        sp2               = grp['Sisa Saldo'].shift(1).clip(lower=1).fillna(1)
        r_abs2            = grp['Selisih'] < REFILL_ABS
        r_pct2            = (grp['Selisih'] < 0) & ((-grp['Selisih']) / sp2 > REFILL_PCT)
        grp['Is Refill']  = _bool_to_int(r_abs2 | r_pct2)   # FIX: nullable boolean
        # Vectorized — hindari apply dengan NA
        selisih_c = pd.to_numeric(grp['Selisih'], errors='coerce').fillna(0)
        grp['Penarikan'] = np.where(
            (selisih_c > 0) & (grp['Is Refill'] == 0),
            selisih_c,
            0,
        )

        grp.index.name = 'datetime'
        grp = grp.reset_index()
        grp['Tanggal']     = grp['datetime'].dt.date
        grp['Jam']         = grp['datetime'].dt.strftime('%H:%M')
        grp['Hari Minggu'] = grp['datetime'].dt.dayofweek
        hasil_interp.append(grp)

    if not hasil_interp:
        raise ValueError("Tidak ada data berhasil diproses setelah interpolasi.")

    df = pd.concat(hasil_interp, ignore_index=True)

    # ── 6. Fitur turunan ──────────────────────────────────────────────────────
    df['Persentase']   = (df['Sisa Saldo'] / df['Limit'] * 100).clip(0, 100)
    df['Jam Int']      = df['datetime'].dt.hour
    df['Is Weekend']   = _bool_to_int(df['Hari Minggu'] >= 5)
    df['Is Jam Sibuk'] = df['Jam Int'].apply(
        lambda h: 1 if (7 <= h <= 12) or (16 <= h <= 21) else 0
    )

    # ── 7. Rolling windows ────────────────────────────────────────────────────
    for w, col in [(6, '6j'), (24, '24j'), (72, '72j')]:
        df[f'Avg Penarikan {col}'] = (
            df.groupby('ID ATM')['Penarikan']
            .transform(lambda x: x.rolling(w, min_periods=1).mean())
        )

    df['Std Penarikan 24j'] = (
        df.groupby('ID ATM')['Penarikan']
        .transform(lambda x: x.rolling(24, min_periods=2).std().fillna(0))
    )

    # ── 8. Flag ATM Sepi ──────────────────────────────────────────────────────
    df['Is_ATM_Sepi'] = _bool_to_int(df['Avg Penarikan 72j'] < ATM_SEPI_AVG72)

    # ── 9. Lag features ───────────────────────────────────────────────────────
    for lag in [1, 2, 3, 6, 12]:
        df[f'Saldo Lag {lag}j'] = df.groupby('ID ATM')['Sisa Saldo'].shift(lag).fillna(0)
        df[f'Tarik Lag {lag}j'] = df.groupby('ID ATM')['Penarikan'].shift(lag).fillna(0)

    # ── 10. Est Jam Habis cascade & Status V6 ────────────────────────────────
    df['Est Jam Habis Rule'] = df.apply(est_jam_cascade, axis=1)
    df['Status']             = df['Persentase'].apply(status_saldo)

    # ── 11. Final cleanup ─────────────────────────────────────────────────────
    # Pastikan tidak ada nullable dtype yang bisa menyebabkan masalah saat save/load
    for col in df.select_dtypes(include=['boolean']).columns:
        df[col] = _bool_to_int(df[col])

    # Konversi Int64 nullable ke int biasa untuk kolom integer
    for col in df.select_dtypes(include=['Int8', 'Int16', 'Int32', 'Int64']).columns:
        df[col] = df[col].fillna(0).astype(int)

    # String nullable → object
    for col in df.columns:
        if hasattr(df[col], 'dtype') and pd.api.types.is_string_dtype(df[col]) and str(df[col].dtype) in ('string', 'StringDtype'):
            df[col] = df[col].astype(str)

    return df