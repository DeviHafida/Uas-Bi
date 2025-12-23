# data_etl.py
import pandas as pd
from sqlalchemy import create_engine, text
from sklearn.preprocessing import MultiLabelBinarizer

INPUT_FILE = "D:/UAS_BI/Data_Staging/data_gabungan.csv"
DB_URI = "postgresql+psycopg2://postgres:2004@localhost:5432/datawarehouse"
engine = create_engine(DB_URI)

def etl_process():
    print("=== MULAI ETL DIMENSI & FAKTA (ML-FRIENDLY) ===")

    df = pd.read_csv(INPUT_FILE, encoding="latin-1")
    print("Kolom input:", df.columns.tolist())

    # --- Drop fact_predictions dulu supaya tidak error constraint ---
    with engine.begin() as conn:
        conn.execute(text("DROP TABLE IF EXISTS fact_predictions CASCADE;"))

    # --- Normalisasi teks ---
    df['genre'] = df['genre'].astype(str).str.upper().fillna("UNKNOWN")
    df['weekdays'] = df['weekdays'].astype(str).str.upper().fillna("UNKNOWN")
    df['status'] = df['status'].astype(str).str.capitalize().fillna("Ongoing")

    # --- Simpan kolom original untuk ML ---
    df['genre_original'] = df['genre']
    df['status_original'] = df['status']
    df['length_original'] = df['length']

    # --- MultiLabel Binarizer untuk genre & weekdays ---
    df['genre_list'] = df['genre'].apply(lambda x: [g.strip() for g in x.split(",")])
    df['weekdays_list'] = df['weekdays'].apply(lambda x: [d.strip() for d in x.split(",")])

    mlb_genre = MultiLabelBinarizer()
    genre_matrix = mlb_genre.fit_transform(df['genre_list'])
    genre_df = pd.DataFrame(genre_matrix, columns=[f"genre_{g}" for g in mlb_genre.classes_])

    mlb_days = MultiLabelBinarizer()
    days_matrix = mlb_days.fit_transform(df['weekdays_list'])
    days_df = pd.DataFrame(days_matrix, columns=[f"weekday_{d}" for d in mlb_days.classes_])

    # --- Gabung semua, jangan drop kolom original ---
    df_ml = pd.concat([df.drop(columns=['genre_list','weekdays_list']) , genre_df, days_df], axis=1)

    # --- Encode fitur kategori lain untuk ML ---
    for col in ['author','status','length']:
        df_ml[f"{col}_id"] = df_ml[col].astype('category').cat.codes

    # --- Simpan ke PostgreSQL ---
    df_ml.to_sql('dim_comics', con=engine, if_exists='replace', index=False)
    print("✅ ETL selesai: tabel dim_comics diperbarui di PostgreSQL.")

if __name__ == "__main__":
    etl_process()
