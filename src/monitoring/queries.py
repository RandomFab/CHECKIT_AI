import os
import pandas as pd
from sqlalchemy import create_engine
from config.logger import logger


# ============================================================================
# HELPER : Engine SQLAlchemy réutilisable
# ============================================================================

def _get_engine():
    """Retourne un engine SQLAlchemy connecté à Postgres (role 'reader' pour SELECT).
    
    Si DB_HOST vaut 'postgres' (nom Docker interne), on bascule sur 'localhost'
    pour les exécutions locales hors Docker.
    """
    db_host = os.getenv("DB_HOST", "localhost")
    if db_host == "postgres":
        db_host = "localhost"

    db_url = (
        f"postgresql://"
        f"{os.getenv('DB_READER_USER', 'reader')}:"
        f"{os.getenv('DB_READER_PASSWORD', '')}@"
        f"{db_host}:"
        f"{os.getenv('DB_PORT', '5432')}/"
        f"{os.getenv('DB_NAME', 'checkit')}"
    )
    return create_engine(db_url)


def _execute_query(query: str, query_name: str = "query") -> pd.DataFrame:
    """Exécute une query SQL générique et retourne un DataFrame.
    
    Args:
        query: requête SQL à exécuter
        query_name: nom pour les logs
        
    Returns:
        pd.DataFrame avec les résultats, ou DataFrame() vide si erreur
    """
    try:
        engine = _get_engine()
        df = pd.read_sql_query(query, engine)
        logger.info(f"[Queries] {query_name} -> {len(df)} row(s)")
        return df
    except Exception as e:
        logger.error(f"[Queries] Erreur {query_name}: {e}")
        return pd.DataFrame()


# ============================================================================
# QUERY 1 : Articles par source (EXEMPLE COMPLET)
# ============================================================================

def get_articles_by_source():
    """Articles extraits par source + % du total.
    
    Retourne : DataFrame avec colonnes [source, count, pct]
    """
    query = """
    SELECT source, COUNT(*) as count, 
           ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER(), 1) as pct
    FROM articles
    GROUP BY source
    ORDER BY count DESC;
    """
    return _execute_query(query, "get_articles_by_source()")


# ============================================================================
# QUERY 2 : Taux de multimodalité (% articles avec images)
# ============================================================================

def get_articles_with_images_pct():
    """% articles multimodaux (avec ≥1 image) au moment de l'extraction — dernier run.

    On lit pipeline_runs plutôt que la table articles, car articles ne contient
    que les articles déjà filtrés (le taux y serait toujours ~100%).
    skipped_not_multimodal = articles rejetés faute d'image à l'extraction.

    Retourne : DataFrame avec colonnes [total_articles, with_images, pct]
    """
    query = """
    SELECT 
        SUM(total) as total_articles,
        SUM(total - skipped_not_multimodal) as with_images,
        ROUND(
            100.0 * SUM(total - skipped_not_multimodal) / NULLIF(SUM(total), 0),
            1
        ) as pct
    FROM pipeline_runs
    WHERE DATE(run_at) = DATE((SELECT MAX(run_at) FROM pipeline_runs));
    """
    return _execute_query(query, "get_articles_with_images_pct()")

# ============================================================================
# QUERY 3 : Taux de doublons
# ============================================================================

def get_duplicates_pct():
    """Taux de doublons (titres identiques).

    Retourne : DataFrame avec colonnes [unique_titles, total_articles, dup_pct]
    """
    query = """
    SELECT 
        COUNT(DISTINCT title) as unique_titles,
        COUNT(*) as total_articles,
        ROUND(100.0 * (COUNT(*) - COUNT(DISTINCT title)) / COUNT(*), 1) as dup_pct
    FROM articles;
    """
    return _execute_query(query, "get_duplicates_pct()")

# ============================================================================
# QUERY 4 : Historique des runs (7 derniers jours)
# ============================================================================

def get_pipeline_runs_7days():
    """Historique des 7 derniers runs par source (pour line chart).

    Retourne : DataFrame avec colonnes [run_date, source, total, valid, taux_valid, ...]
    """
    query = """
    SELECT 
        DATE(run_at) as run_date,
        source,
        total,
        valid,
        skipped_not_multimodal,
        skipped_bad_label,
        skipped_no_image,
        ROUND(100.0 * valid / NULLIF(total, 0), 1) as taux_valid
    FROM pipeline_runs
    WHERE run_at >= NOW() - INTERVAL '7 days'
    ORDER BY run_at DESC, source;
    """
    return _execute_query(query, "get_pipeline_runs_7days()")


# ============================================================================
# QUERY 5 : Entonnoir de validation (dernier run)
# ============================================================================

def get_validation_funnel_latest():
    """Entonnoir : extraits → multimodal → label_ok → images_ok (dernier run).

    Retourne : DataFrame avec colonnes [source, extraits, multimodal, label_valide, images_ok]
    """
    query = """
    SELECT 
        source,
        COALESCE(SUM(total), 0) as extraits,
        COALESCE(SUM(valid), 0) as multimodal,
        COALESCE(SUM(valid) - SUM(skipped_bad_label), 0) as label_valide,
        COALESCE(SUM(valid) - SUM(skipped_bad_label) - SUM(skipped_no_image), 0) as images_ok
    FROM pipeline_runs
    WHERE DATE(run_at) = DATE((SELECT MAX(run_at) FROM pipeline_runs))
    GROUP BY source
    ORDER BY source;
    """
    return _execute_query(query, "get_validation_funnel_latest()")


# ============================================================================
# QUERY 6 : Temps d'exécution des runs (7 derniers jours)
# ============================================================================

def get_execution_time_7days():
    """Temps d'exécution moyen par jour et par source sur les 7 derniers jours.

    Retourne : DataFrame avec colonnes [run_date, source, avg_duration, max_duration, run_count]
    """
    query = """
    SELECT 
        DATE(run_at) as run_date,
        source,
        AVG(duration_seconds) as avg_duration,
        MAX(duration_seconds) as max_duration,
        COUNT(*) as run_count
    FROM pipeline_runs
    WHERE run_at >= NOW() - INTERVAL '7 days'
      AND duration_seconds IS NOT NULL
    GROUP BY DATE(run_at), source
    ORDER BY run_date ASC, source;
    """
    return _execute_query(query, "get_execution_time_7days()")


# ============================================================================
# QUERY 7 : Distribution des labels
# ============================================================================

def get_label_distribution():
    """Répartition des labels : faux / partiellement_faux / vrai / inconnu.

    Retourne : DataFrame avec colonnes [label, count, pct]
    """
    query = """
    SELECT 
        label,
        COUNT(*) as count,
        ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER(), 1) as pct
    FROM articles
    WHERE label IS NOT NULL AND label != ''
    GROUP BY label
    ORDER BY count DESC;
    """
    return _execute_query(query, "get_label_distribution()")