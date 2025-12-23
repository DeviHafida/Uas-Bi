import streamlit as st
import pandas as pd
import plotly.express as px
from sqlalchemy import create_engine

# =============================
# DATABASE CONFIG
# =============================
DB_URI = "postgresql+psycopg2://postgres:2004@localhost:5432/datawarehouse"
engine = create_engine(DB_URI)

# =============================
# PAGE CONFIG
# =============================
st.set_page_config(
    page_title="Dashboard Komik Digital",
    layout="wide"
)

st.title("📊 Dashboard Analisis Komik Digital & Rekomendasi Webtoon")

# =============================
# LOAD DATA
# =============================
@st.cache_data
def load_data():
    df = pd.read_sql("SELECT * FROM fact_predictions", con=engine)
    return df

@st.cache_data
def load_metrics():
    try:
        return pd.read_sql("SELECT * FROM ml_metrics", con=engine)
    except:
        return pd.DataFrame()

df = load_data()
metrics_df = load_metrics()

# =============================
# METRICS MODEL (AKURASI dll.)
# =============================
st.subheader("🤖 Performance Model Machine Learning")

if metrics_df.empty:
    st.warning("Belum ada tabel *ml_metrics* di database.")
else:
    for _, row in metrics_df.iterrows():
        with st.expander(f"🔹 Model {row['target']} — {row['algorithm']}"):
            colA, colB, colC, colD = st.columns(4)
            colA.metric("Accuracy", f"{row['accuracy']:.3f}")
            colB.metric("F1 Score", f"{row['f1_score']:.3f}")
            colC.metric("ROC AUC", f"{row['roc_auc']:.3f}")
            colD.metric("RMSE", f"{row['rmse']:.3f}")

st.markdown("---")

# =============================
# SIDEBAR
# =============================
st.sidebar.header("⚙️ Pengaturan")

TOP_N = st.sidebar.slider(
    "Tampilkan Top N Data",
    min_value=5,
    max_value=30,
    value=10
)

page = st.sidebar.radio(
    "Pilih Analisis",
    ["Target Audience", "Popularity", "Viral Potential"]
)

# =============================
# METRICS KOMIK (STATISTIK DASAR)
# =============================
col1, col2, col3, col4 = st.columns(4)

col1.metric("Total Komik", len(df))
col2.metric("Rata-rata Rating", round(df["rating"].mean(), 2))
col3.metric("Jumlah Author", df["author"].nunique())
col4.metric("Total Subscribers", int(df["subscribers"].sum()))

st.markdown("---")

# =====================================================
# TARGET AUDIENCE
# =====================================================
if page == "Target Audience":

    st.subheader("🎯 Distribusi Target Audience")

    vc = (
        df["Target_Audience_Pred"]
        .dropna()
        .value_counts()
        .head(TOP_N)
        .reset_index()
    )
    vc.columns = ["Kategori", "Jumlah"]

    fig = px.bar(vc, x="Kategori", y="Jumlah", text="Jumlah")
    st.plotly_chart(fig, use_container_width=True)

    st.markdown("### 📌 Rekomendasi Komik")

    options = sorted(df["Target_Audience_Pred"].dropna().astype(str).unique())
    selected = st.selectbox("Pilih Target Audience", options)

    filtered_df = (
        df[df["Target_Audience_Pred"] == selected]
        .sort_values(by=["rating", "subscribers"], ascending=False)
        .head(TOP_N)
        .reset_index(drop=True)
    )

    st.dataframe(
        filtered_df[["title", "author", "genre_original", "rating", "subscribers"]],
        height=400
    )

# =====================================================
# POPULARITY
# =====================================================
elif page == "Popularity":

    st.subheader("🔥 Distribusi Popularity")

    vc = (
        df["Popularity_Pred"]
        .dropna()
        .value_counts()
        .head(TOP_N)
        .reset_index()
    )
    vc.columns = ["Kategori", "Jumlah"]

    fig = px.bar(vc, x="Kategori", y="Jumlah", text="Jumlah")
    st.plotly_chart(fig, use_container_width=True)

    st.markdown("### 📌 Rekomendasi Komik")

    options = sorted(df["Popularity_Pred"].dropna().astype(str).unique())
    selected = st.selectbox("Pilih Popularity", options)

    filtered_df = (
        df[df["Popularity_Pred"] == selected]
        .sort_values(by="subscribers", ascending=False)
        .head(TOP_N)
        .reset_index(drop=True)
    )

    st.dataframe(
        filtered_df[["title", "author", "genre_original", "rating", "subscribers"]],
        height=400
    )

# =====================================================
# VIRAL POTENTIAL
# =====================================================
elif page == "Viral Potential":

    st.subheader("🚀 Distribusi Viral Potential")

    vc = (
        df["Viral_Potential_Pred"]
        .dropna()
        .value_counts()
        .head(TOP_N)
        .reset_index()
    )
    vc.columns = ["Kategori", "Jumlah"]

    fig = px.bar(vc, x="Kategori", y="Jumlah", text="Jumlah")
    st.plotly_chart(fig, use_container_width=True)

    st.markdown("### 📌 Rekomendasi Komik")

    options = sorted(df["Viral_Potential_Pred"].dropna().astype(str).unique())
    selected = st.selectbox("Pilih Level Viral Potential", options)

    filtered_df = (
        df[df["Viral_Potential_Pred"] == selected]
        .sort_values(by="subscribers", ascending=False)
        .head(TOP_N)
        .reset_index(drop=True)
    )

    st.dataframe(
        filtered_df[["title", "author", "genre_original", "rating", "subscribers"]],
        height=400
    )
