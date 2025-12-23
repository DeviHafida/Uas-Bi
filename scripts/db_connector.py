import psycopg2

def connect_db():
    """Membuat dan mengembalikan koneksi ke PostgreSQL."""
    conn = psycopg2.connect(
        host="localhost",
        database="datawarehouse",
        user="postgres",
        password="2004"
    )
    return conn

if __name__ == "__main__":
    try:
        conn = connect_db()
        print("✅ Koneksi ke PostgreSQL berhasil!") 
        conn.close()
    except Exception as e:
        print("❌ Koneksi GAGAL. Cek kredensial Anda.")
        print(f"Detail Error: {e}")
