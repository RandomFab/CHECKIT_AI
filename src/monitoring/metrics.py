from datetime import datetime, timezone, date
from . import queries
from . import config
from config.logger import logger


class MonitoringMetrics:
    def __init__(self):
        self.data = {}

    def compute_all(self):
        """Charge tous les KPIs depuis la base de données."""
        self.data["articles_by_source"]    = queries.get_articles_by_source()
        self.data["articles_with_images"]  = queries.get_articles_with_images_pct()
        self.data["duplicates"]            = queries.get_duplicates_pct()
        self.data["pipeline_runs_7days"]   = queries.get_pipeline_runs_7days()
        self.data["validation_funnel"]     = queries.get_validation_funnel_latest()
        self.data["label_distribution"]    = queries.get_label_distribution()
        self.data["execution_time_7days"]  = queries.get_execution_time_7days()
        return self.data

    def check_alerts(self):
        """Vérifie les seuils et retourne une liste d'alertes (dict avec level + message)."""
        alerts = []
        thresholds = config.KPI_THRESHOLDS

        # --- Alerte 1 : volume minimum d'articles par source ---
        df_src = self.data.get("articles_by_source")
        if df_src is not None and not df_src.empty:
            for _, row in df_src.iterrows():
                if row["count"] < thresholds["min_articles_per_day"]:
                    alerts.append({
                        "level": "error",
                        "message": (
                            f"Source '{row['source']}' : seulement {int(row['count'])} articles"
                            f" (seuil min : {thresholds['min_articles_per_day']})"
                        ),
                    })

        # --- Alerte 2 : taux de multimodalité (articles avec images) ---
        df_img = self.data.get("articles_with_images")
        if df_img is not None and not df_img.empty:
            pct = float(df_img.iloc[0]["pct"])
            threshold_pct = thresholds["min_articles_with_images"] * 100
            if pct < threshold_pct:
                alerts.append({
                    "level": "warning",
                    "message": (
                        f"Taux de multimodalité : {pct}%"
                        f" (seuil min : {threshold_pct:.0f}%)"
                    ),
                })

        # --- Alerte 3 : taux de doublons ---
        df_dup = self.data.get("duplicates")
        if df_dup is not None and not df_dup.empty:
            dup_pct = float(df_dup.iloc[0]["dup_pct"])
            threshold_dup = thresholds["max_duplicates_pct"] * 100
            if dup_pct > threshold_dup:
                alerts.append({
                    "level": "warning",
                    "message": (
                        f"Taux de doublons : {dup_pct}%"
                        f" (seuil max : {threshold_dup:.0f}%)"
                    ),
                })

        # --- Alerte 4 : fraîcheur des données (dernier run) ---
        df_runs = self.data.get("pipeline_runs_7days")
        if df_runs is not None and not df_runs.empty:
            last_run = df_runs["run_date"].max()
            # Convertir en datetime si c'est un date
            if hasattr(last_run, "to_pydatetime"):
                last_run = last_run.to_pydatetime()
            if isinstance(last_run, date) and not isinstance(last_run, datetime):
                # Convertir date → datetime
                from datetime import datetime as dt
                last_run = dt.combine(last_run, dt.min.time())
            if last_run.tzinfo is None:
                last_run = last_run.replace(tzinfo=timezone.utc)
            hours_since = (datetime.now(timezone.utc) - last_run).total_seconds() / 3600
            if hours_since > thresholds["max_data_age_hours"]:
                alerts.append({
                    "level": "error",
                    "message": (
                        f"Données trop anciennes : dernier run il y a {hours_since:.0f}h"
                        f" (seuil max : {thresholds['max_data_age_hours']}h)"
                    ),
                })
        else:
            alerts.append({
                "level": "error",
                "message": "Aucun run détecté dans les 7 derniers jours.",
            })

        return alerts