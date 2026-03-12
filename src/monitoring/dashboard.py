import sys
import os
from pathlib import Path

# Résolution des imports depuis la racine du projet
ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from dotenv import load_dotenv
load_dotenv(ROOT / ".env")

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from src.monitoring.metrics import MonitoringMetrics

st.set_page_config(
    page_title="CheckIT_AI — Monitoring ETL",
    layout="wide",
    page_icon="🔍",
)

st.title("🔍 CheckIT_AI — Monitoring ETL")

# ---------------------------------------------------------------------------
# Chargement des données
# ---------------------------------------------------------------------------
@st.cache_data(ttl=300)  # Rafraîchit toutes les 5 minutes
def load_data():
    m = MonitoringMetrics()
    data = m.compute_all()
    alerts = m.check_alerts()
    return data, alerts

data, alerts = load_data()

# ---------------------------------------------------------------------------
# SECTION 1 : Alertes
# ---------------------------------------------------------------------------
if alerts:
    st.subheader("⚠️ Alertes")
    for alert in alerts:
        if alert["level"] == "error":
            st.error(alert["message"])
        else:
            st.warning(alert["message"])
else:
    st.success("✅ Tous les indicateurs sont dans les seuils normaux.")

st.divider()

# ---------------------------------------------------------------------------
# SECTION 2 : KPI Cards
# ---------------------------------------------------------------------------
st.subheader("📊 Indicateurs clés (dernier run)")

col1, col2, col3, col4 = st.columns(4)

df_src = data.get("articles_by_source", pd.DataFrame())
total_articles = int(df_src["count"].sum()) if not df_src.empty else 0

df_img = data.get("articles_with_images", pd.DataFrame())
taux_images = float(df_img.iloc[0]["pct"]) if not df_img.empty else 0.0

df_dup = data.get("duplicates", pd.DataFrame())
taux_doublons = float(df_dup.iloc[0]["dup_pct"]) if not df_dup.empty else 0.0

df_runs = data.get("pipeline_runs_7days", pd.DataFrame())
taux_valid_moyen = round(df_runs["taux_valid"].mean(), 1) if not df_runs.empty else 0.0

with col1:
    st.metric("Articles totaux", total_articles)
with col2:
    st.metric("Taux multimodalité", f"{taux_images}%", help="% d'articles avec ≥ 1 image")
with col3:
    st.metric("Taux doublons", f"{taux_doublons}%", help="% de titres en doublon")
with col4:
    st.metric("Taux validation moyen (7j)", f"{taux_valid_moyen}%", help="% d'articles passant les 3 gates")

st.divider()

# ---------------------------------------------------------------------------
# SECTION 3 : Volume par source + Distribution des labels
# ---------------------------------------------------------------------------
col_left, col_right = st.columns(2)

with col_left:
    st.subheader("📦 Articles par source")
    if not df_src.empty:
        fig = px.bar(
            df_src,
            x="source",
            y="count",
            text="pct",
            color="source",
            labels={"count": "Nombre d'articles", "source": "Source"},
        )
        fig.update_traces(texttemplate="%{text}%", textposition="outside")
        fig.update_layout(showlegend=False, margin=dict(t=20))
        st.plotly_chart(fig, width='stretch')
    else:
        st.info("Pas de données disponibles.")

with col_right:
    st.subheader("🏷️ Distribution des labels")
    df_labels = data.get("label_distribution", pd.DataFrame())
    if not df_labels.empty:
        fig = px.pie(
            df_labels,
            names="label",
            values="count",
            hole=0.4,
        )
        fig.update_layout(margin=dict(t=20))
        st.plotly_chart(fig, width='stretch')
    else:
        st.info("Pas de données disponibles.")

st.divider()

# ---------------------------------------------------------------------------
# SECTION 4 : Évolution du taux de validation sur 7 jours
# ---------------------------------------------------------------------------
st.subheader("📈 Évolution du taux de validation (7 derniers jours)")

if not df_runs.empty:
    fig = px.line(
        df_runs,
        x="run_date",
        y="taux_valid",
        color="source",
        markers=True,
        labels={"run_date": "Date", "taux_valid": "Taux de validation (%)", "source": "Source"},
    )
    fig.update_layout(yaxis_range=[0, 100], margin=dict(t=20))
    st.plotly_chart(fig, width='stretch')
else:
    st.info("Pas d'historique disponible (table pipeline_runs vide).")

st.divider()

# ---------------------------------------------------------------------------
# SECTION 5 : Temps d'exécution des runs (7 derniers jours)
# ---------------------------------------------------------------------------
st.subheader("⏱️ Temps d'exécution des runs (7 derniers jours)")

df_exec = data.get("execution_time_7days", pd.DataFrame())
if not df_exec.empty:
    df_exec['run_date'] = pd.to_datetime(df_exec['run_date'])

    # Grille complète : 7 derniers jours × toutes les sources
    today = pd.Timestamp.now().normalize()
    date_range = pd.date_range(end=today, periods=7, freq='D')
    sources = sorted(df_exec['source'].unique())
    grid = pd.MultiIndex.from_product([date_range, sources], names=['run_date', 'source']).to_frame(index=False)

    df_exec_full = grid.merge(df_exec[['run_date', 'source', 'avg_duration']], on=['run_date', 'source'], how='left')

    fig = px.bar(
        df_exec_full,
        x="run_date",
        y="avg_duration",
        color="source",
        barmode="group",
        labels={"run_date": "Date", "avg_duration": "Durée moyenne (s)", "source": "Source"},
    )
    fig.update_layout(xaxis_tickformat="%d/%m", margin=dict(t=20))
    st.plotly_chart(fig, width='stretch')
else:
    st.info("Pas de données disponibles (duration_seconds non renseigné ou table vide).")

st.divider()

# ---------------------------------------------------------------------------
# SECTION 6 : Entonnoir de validation du dernier run
# ---------------------------------------------------------------------------
st.subheader("🔽 Entonnoir de validation (dernier run)")

df_funnel = data.get("validation_funnel", pd.DataFrame())
if not df_funnel.empty:
    # Agrège toutes sources confondues
    total_row = df_funnel[["extraits", "multimodal", "label_valide", "images_ok"]].sum()
    stages = ["Extraits", "Multimodal ✓", "Label ✓", "Images ✓"]
    values = [
        int(total_row["extraits"]),
        int(total_row["multimodal"]),
        int(total_row["label_valide"]),
        int(total_row["images_ok"]),
    ]
    fig = go.Figure(go.Funnel(
        y=stages,
        x=values,
        textinfo="value+percent initial",
        marker_color=["#4C72B0", "#55A868", "#C44E52", "#8172B2"],
    ))
    fig.update_layout(margin=dict(t=20))
    st.plotly_chart(fig, width='stretch')

    # Détail par source
    with st.expander("Détail par source"):
        st.dataframe(df_funnel, width='stretch')
else:
    st.info("Pas de données disponibles (table pipeline_runs vide).")

st.caption(f"Données rafraîchies toutes les 5 min — CheckIT_AI Monitoring")