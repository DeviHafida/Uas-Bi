import pandas as pd
import numpy as np
import random

# =========================
# KONFIGURASI FILE
# =========================
FILE_WEBTOON = 'D:/UAS_BI/Data_Staging/webtoon_originals_id.csv'
FILE_MANGA = 'D:/UAS_BI/Data_Staging/Manga_Details.csv'
OUTPUT_FILE = 'D:/UAS_BI/Data_Staging/data_gabungan.csv'

VALID_WEEKDAYS = [
    'MONDAY', 'TUESDAY', 'WEDNESDAY',
    'THURSDAY', 'FRIDAY', 'SATURDAY', 'SUNDAY'
]

VALID_LENGTH = ['SHORT', 'MEDIUM', 'LONG']

TARGET_COLUMNS = [
    'title', 'genre', 'author', 'weekdays', 'length',
    'subscribers', 'status', 'rating', 'year',
    'source_type', 'synopsis'
]

# =========================
# FUNGSI UNTUK KAPITALISASI JUDUL
# =========================
def format_title(title):
    """
    Format judul menjadi Title Case (setiap awal kata kapital)
    dan handle kasus khusus.
    """
    if pd.isna(title):
        return title
    
    title = str(title)
    
    # Kata-kata yang tidak perlu dikapitalisasi penuh (kecuali di awal)
    small_words = {'a', 'an', 'and', 'as', 'at', 'but', 'by', 'for', 
                   'in', 'nor', 'of', 'on', 'or', 'so', 'the', 'to', 
                   'up', 'yet', 'with'}
    
    # Split judul menjadi kata-kata
    words = title.split()
    formatted_words = []
    
    for i, word in enumerate(words):
        # Jangan ubah jika semua huruf kapital (singkatan)
        if word.isupper():
            formatted_words.append(word)
        # Ubah ke Title Case untuk kata biasa
        else:
            # Kata pertama selalu kapital
            if i == 0:
                formatted_words.append(word.title())
            # Kata-kata kecil tidak dikapitalisasi (kecuali penting)
            elif word.lower() in small_words:
                formatted_words.append(word.lower())
            else:
                formatted_words.append(word.title())
    
    return ' '.join(formatted_words)

# =========================
# PRE-ETL HARMONIZATION
# =========================
def pre_etl_harmonization():
    print("=== MULAI PROSES HARMONISASI DATA ===")

    # 1. LOAD DATA
    df_webtoon = pd.read_csv(FILE_WEBTOON, encoding='latin-1')
    df_manga = pd.read_csv(FILE_MANGA, encoding='latin-1')

    df_webtoon.columns = df_webtoon.columns.str.lower().str.strip()
    df_manga.columns = df_manga.columns.str.lower().str.strip()

    # 2. PASTIKAN KOLOM AUTHOR ADA
    if 'authors' not in df_manga.columns:
        df_manga['authors'] = np.nan

    df_manga.rename(columns={'authors': 'author'}, inplace=True)
    df_webtoon.rename(columns={'authors': 'author'}, inplace=True)

    # 3. CLEANING DATA MANGA
    df_manga['title_original'] = df_manga['title'].copy()
    
    # Clean judul: lower case dulu untuk konsistensi
    df_manga['title'] = df_manga['title'].astype(str).str.lower().str.strip()
    
    # Format judul menjadi Title Case
    df_manga['title'] = df_manga['title'].apply(format_title)
    
    # Author cleaning
    df_manga['author'] = df_manga['author'].astype(str).str.strip()

    df_manga['author'].replace(
        ["", " ", "nan", "NaN", "-", "_"], np.nan, inplace=True
    )
    df_manga.dropna(subset=['author'], inplace=True)

    # LENGTH
    if 'length' not in df_manga.columns:
        df_manga['length'] = np.nan

    df_manga['length'] = df_manga['length'].astype(str).str.upper().str.strip()
    df_manga['length'].replace(
        ["", " ", "NAN", "NA", "-", "_"], np.nan, inplace=True
    )

    mask_length_na = df_manga['length'].isna()
    df_manga.loc[mask_length_na, 'length'] = [
        random.choice(VALID_LENGTH) for _ in range(mask_length_na.sum())
    ]

    # WEEKDAYS RANDOM
    df_manga['weekdays'] = [
        random.choice(VALID_WEEKDAYS) for _ in range(len(df_manga))
    ]

    df_manga['source_type'] = 'MANGA/WEBTOON ID'

    # 4. CLEANING DATA WEBTOON
    df_webtoon['title_original'] = df_webtoon['title'].copy()
    
    df_webtoon.drop(
        columns=[c for c in ['views', 'likes'] if c in df_webtoon.columns],
        inplace=True
    )

    # Clean judul: lower case dulu untuk konsistensi
    df_webtoon['title'] = df_webtoon['title'].astype(str).str.lower().str.strip()
    
    # Format judul menjadi Title Case
    df_webtoon['title'] = df_webtoon['title'].apply(format_title)
    
    df_webtoon['genre'] = df_webtoon['genre'].astype(str).str.upper().str.strip()
    df_webtoon['weekdays'] = df_webtoon['weekdays'].astype(str).str.upper().str.strip()

    df_webtoon['source_type'] = 'WEBTOON ORIGINALS'

    # 5. SAMAKAN STRUKTUR KOLOM
    df_webtoon = df_webtoon.reindex(columns=TARGET_COLUMNS)
    df_manga = df_manga.reindex(columns=TARGET_COLUMNS)

    # 6. GABUNGKAN DATA
    df_combined = pd.concat([df_webtoon, df_manga], ignore_index=True)

    # 7. HAPUS STATUS CANCELLED
    if 'status' in df_combined.columns:
        before = len(df_combined)
        df_combined = df_combined[
            df_combined['status'].astype(str).str.upper() != 'CANCELLED'
        ]
        print(f"Data CANCELLED dihapus: {before - len(df_combined)} baris")

    # 8. NORMALISASI RATING (1–10 → 1–5)
    df_combined['rating'] = pd.to_numeric(
        df_combined['rating'], errors='coerce'
    )

    def normalize_rating(r):
        if pd.isna(r):
            return np.nan
        if r <= 5:
            return r
        return r / 2

    df_combined['rating'] = df_combined['rating'].apply(normalize_rating)
    df_combined['rating'] = df_combined['rating'].round(2)

    # 9. FINAL CLEANING
    before_dedup = len(df_combined)
    df_combined.drop_duplicates(subset=['title'], inplace=True)
    print(f"Duplikat dihapus: {before_dedup - len(df_combined)} baris")
    
    df_combined.reset_index(drop=True, inplace=True)

    df_combined['title_id'] = df_combined.index + 1

    final_cols = ['title_id'] + TARGET_COLUMNS
    df_combined = df_combined[final_cols]

    # 9. SIMPAN FILE
    print(f"\nTOTAL DATA AKHIR: {len(df_combined)} baris")
    df_combined.to_csv(OUTPUT_FILE, index=False)
    print(f"FILE DISIMPAN KE: {OUTPUT_FILE}")

    return df_combined

# MAIN
if __name__ == "__main__":
    pre_etl_harmonization()