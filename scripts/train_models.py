import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from sklearn.preprocessing import LabelEncoder
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier, GradientBoostingClassifier
from sklearn.metrics import accuracy_score, f1_score, roc_auc_score, mean_squared_error

# ===============================
# DATABASE CONNECTION
# ===============================
DB_URI = "postgresql+psycopg2://postgres:2004@localhost:5432/datawarehouse"
engine = create_engine(DB_URI)

# ===============================
# LOAD DATA
# ===============================
df = pd.read_sql("SELECT * FROM dim_comics", con=engine)

# ===============================
# CLEAN NUMERIC FIELDS
# ===============================
for col in ['rating', 'subscribers', 'year']:
    df[col] = pd.to_numeric(df[col], errors='coerce')

df['rating'].fillna(df['rating'].median(), inplace=True)
df['subscribers'].fillna(df['subscribers'].median(), inplace=True)
df['year'].fillna(df['year'].median(), inplace=True)

# ===============================
# TARGET AUDIENCE MAPPING
# ===============================
def map_target_audience(genre):
    genre = str(genre).upper()
    if any(g in genre for g in ['SCHOOL', 'SLICE OF LIFE', 'COMEDY']):
        return 'Teen'
    elif any(g in genre for g in ['ROMANCE', 'ACTION', 'FANTASY', 'DRAMA']):
        return 'Young Adult'
    else:
        return 'Adult'

df['Target_Audience'] = df['genre'].apply(map_target_audience)

# ===============================
# POPULARITY (COMPLETED)
# VIRAL POTENTIAL (ONGOING)
# ===============================
df['subscriber_rank'] = df['subscribers'].rank(method='first')

df_completed = df[df['status'].str.upper() == 'COMPLETED'].copy()
if len(df_completed) > 0:
    df_completed['Popularity'] = pd.qcut(df_completed['subscriber_rank'], 3, labels=['Low','Medium','High'])

df_ongoing = df[df['status'].str.upper() == 'ONGOING'].copy()
if len(df_ongoing) > 0:
    df_ongoing['Viral_Potential'] = pd.qcut(df_ongoing['subscriber_rank'], 3, labels=['Low','Medium','High'])

# ===============================
# ENCODE CATEGORICAL
# ===============================
for col in ['genre', 'author', 'length', 'status']:
    le = LabelEncoder()
    df[col] = le.fit_transform(df[col].astype(str))
    if len(df_completed) > 0:
        df_completed[col] = le.transform(df_completed[col].astype(str))
    if len(df_ongoing) > 0:
        df_ongoing[col] = le.transform(df_ongoing[col].astype(str))

# ===============================
# TRAINING FUNCTION
# ===============================
def train_model(X, y, model_type="gb"):
    le_y = LabelEncoder()
    y_enc = le_y.fit_transform(y)

    X_train, X_test, y_train, y_test = train_test_split(
        X, y_enc, test_size=0.2, random_state=42, stratify=y_enc
    )

    if model_type == "gb":
        model = GradientBoostingClassifier(
            n_estimators=400,
            learning_rate=0.05,
            max_depth=10,
            random_state=42
        )
        algo = "Gradient Boosting"

    else:
        model = RandomForestClassifier(
            n_estimators=500,
            random_state=42
        )
        algo = "Random Forest"

    model.fit(X_train, y_train)
    y_pred = model.predict(X_test)
    y_proba = model.predict_proba(X_test)

    metrics = {
        "target": y.name,
        "algorithm": algo,
        "accuracy": float(accuracy_score(y_test, y_pred)),
        "f1_score": float(f1_score(y_test, y_pred, average="weighted")),
        "roc_auc": float(
            roc_auc_score(y_test, y_proba, multi_class="ovr", average="weighted")
        ),
        "rmse": float(
            np.sqrt(mean_squared_error(pd.get_dummies(y_test).values, y_proba))
        )
    }

    return model, le_y, metrics

# ===============================
# TRAIN ALL MODELS
# ===============================
metrics_list = []

# -------- Target Audience --------
X_ta = df[['genre','rating','subscribers','year','length','author']]
y_ta = df['Target_Audience']
model_ta, le_ta, m_ta = train_model(X_ta, y_ta, "gb")
df['Target_Audience_Pred'] = le_ta.inverse_transform(model_ta.predict(X_ta))
metrics_list.append(m_ta)

# -------- Popularity --------
if len(df_completed) > 10:
    X_pop = df_completed[['genre','rating','year','length','author']]
    y_pop = df_completed['Popularity']
    model_pop, le_pop, m_pop = train_model(X_pop, y_pop, "rf")
    df.loc[df_completed.index, 'Popularity_Pred'] = le_pop.inverse_transform(model_pop.predict(X_pop))
    metrics_list.append(m_pop)

# -------- Viral Potential --------
if len(df_ongoing) > 10:
    X_viral = df_ongoing[['genre','rating','year','length','author']]
    y_viral = df_ongoing['Viral_Potential']
    model_viral, le_viral, m_viral = train_model(X_viral, y_viral, "rf")
    df.loc[df_ongoing.index, 'Viral_Potential_Pred'] = le_viral.inverse_transform(model_viral.predict(X_viral))
    metrics_list.append(m_viral)

# ===============================
# SAVE PREDICTIONS & METRICS
# ===============================
df.to_sql("fact_predictions", con=engine, if_exists="replace", index=False)
pd.DataFrame(metrics_list).to_sql("ml_metrics", con=engine, if_exists="replace", index=False)

print("✅ Semua prediksi & metrik model berhasil disimpan ke database!")
