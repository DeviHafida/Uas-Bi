# scripts/dw_schema.py
from db_connector import connect_db

def create_dw_schema():
    conn = None
    try:
        conn = connect_db()
        cur = conn.cursor()

        print("\n=== MEMBUAT SCHEMA DATA WAREHOUSE ===")
        cur.execute("""
            DROP TABLE IF EXISTS fact_predictions CASCADE;
            DROP TABLE IF EXISTS dim_comics CASCADE;
        """)
        print("✅ Tabel lama dihapus")

        # Tabel dimensi / ML-ready
        cur.execute("""
            CREATE TABLE dim_comics (
                title_id SERIAL PRIMARY KEY,
                title TEXT,
                genre TEXT,
                author TEXT,
                weekdays TEXT,
                length TEXT,
                status TEXT,
                rating NUMERIC(5,2),
                subscribers BIGINT,
                year INTEGER
            );
        """)
        print("✅ dim_comics dibuat")

        # Tabel prediksi
        cur.execute("""
            CREATE TABLE fact_predictions (
                pred_id SERIAL PRIMARY KEY,
                title_id INTEGER,
                Target_Audience_Pred TEXT,
                Popularity_Pred TEXT,
                Viral_Potential_Pred TEXT,
                FOREIGN KEY (title_id) REFERENCES dim_comics(title_id)
            );
        """)
        print("✅ fact_predictions dibuat")

        conn.commit()
        print("\n🎉 SCHEMA DATA WAREHOUSE BERHASIL DIBUAT!")

    except Exception as e:
        print("\n❌ ERROR:", e)
        if conn:
            conn.rollback()
    finally:
        if conn:
            conn.close()
            print("\n🔒 Koneksi database ditutup")

if __name__ == "__main__":
    create_dw_schema()
