# Plan de Monitoring — CheckIT_AI

Pipeline ETL `get_multimodal_news_dataset` : extraction quotidienne multi-sources (AFP, France24, GoogleFactCheck) → PostgreSQL. Trois couches de surveillance : logs structurés, alertes KPI, dashboard Streamlit.

---

## Architecture

```
DAG Airflow (@daily)
    ├── config/logger.py         → logs structurés
    ├── pipeline_runs (table PG) → historique métriques par run/source
    └── src/monitoring/
            ├── queries.py       → 7 requêtes SQL
            ├── metrics.py       → MonitoringMetrics.compute_all() + check_alerts()
            ├── config.py        → seuils KPI centralisés
            └── dashboard.py     → Streamlit (TTL = 5 min)
```

---

## Fréquences de vérification

| Mécanisme | Fréquence |
|---|---|
| DAG Airflow | 1×/jour (`@daily`) |
| Refresh dashboard | Toutes les 5 min (`@st.cache_data(ttl=300)`) |
| Rapport Markdown | À la demande (`generate_monitoring_report.py`) |

---

## Seuils d'alerte (`KPI_THRESHOLDS`)

| KPI | Seuil | Niveau |
|---|---|---|
| Articles extraits/source | < 50 | `error` |
| Taux de multimodalité | < 80 % | `warning` |
| Taux de doublons | > 15 % | `warning` |
| Fraîcheur des données | > 48 h depuis dernier run | `error` |
| Aucun run sur 7 jours | `pipeline_runs` vide | `error` |

Alertes affichées dans le dashboard (rouge = `error`, orange = `warning`).

---

## KPIs suivis (7 queries SQL)

| Indicateur | Table(s) |
|---|---|
| Volume articles par source | `articles` |
| Taux de multimodalité (mesuré sur `pipeline_runs`) | `pipeline_runs` |
| Taux de doublons (titres identiques) | `articles` |
| Distribution des labels | `articles` |
| Taux de validation par run — 7j | `pipeline_runs` |
| Entonnoir `extraits → multimodal → label_valide → images_ok` | `pipeline_runs` |
| Temps d'exécution moyen/max par source — 7j | `pipeline_runs` |

---

## Gestion des erreurs

**DAG** : `retries=3`, `retry_delay=5 min`, `max_active_runs=1`. Les sources sont traitées indépendamment — l'échec de l'une ne bloque pas les autres.

**Monitoring** : `_execute_query()` retourne un `DataFrame` vide en cas d'erreur DB (pas de crash du dashboard). `check_alerts()` teste `df is not None and not df.empty` avant tout calcul. Rôle `reader` (lecture seule) pour toutes les queries.

**Logs** : `config/logger.py` trace chaque query (`[Queries] {nom} -> N row(s)`), chaque erreur SQL, et les étapes pipeline (scraping, nettoyage, chargement).

---

## Réponse aux incidents

| Alerte | Action |
|---|---|
| < 50 articles/source | Vérifier disponibilité source + logs scraper |
| Multimodalité < 80 % | Vérifier `image_handler.py`, accès images |
| Doublons > 15 % | Vérifier `cleaning.py` |
| Run > 48 h | Vérifier Airflow (scheduler, tâche en échec) |
| Métriques vides | Vérifier connexion Postgres + variables d'env |
