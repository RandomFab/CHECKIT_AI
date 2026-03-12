"""
DAG : get_multimodal_news_dataset

Extraction, nettoyage et chargement de news factuelles multi-sources.

Flux Airflow :
    scrap_AfpScraper ──────────┐
    scrap_France24Scraper ─────┤→ process_{source} (parallèle) → merge → parquet → rapport
    scrap_GoogleFactCheck ─────┘

"""
from dotenv import load_dotenv

load_dotenv()
from datetime import datetime, timedelta
from pathlib import Path
from config.logger import logger

from airflow.sdk import dag, task


@dag(
    dag_id="get_multimodal_news_dataset",
    description="Extraction, nettoyage et chargement de news factuelles multi-sources",
    start_date=datetime(2026, 2, 1),
    catchup=False,
    schedule="@daily",
    max_active_runs=1,
    default_args={
        "owner": "RandomFab",
        "retries": 3,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["extraction"],
)
def extract_news_workflow():

    # ------------------------------------------------------------------ #
    # TÂCHE 1 : Scraping (une tâche par source, en parallèle)            #
    # ------------------------------------------------------------------ #
    @task
    def scrap_news_task(scraper_name: str) -> dict:
        """Lance le scraper de la source et retourne le chemin du dossier raw."""
        from src.extraction.scrapers.afp_scraper import AfpScraper
        from src.extraction.scrapers.france24_scraper import France24Scraper
        from src.extraction.scrapers.google_factcheck_client import GoogleFactCheckScraper
        from src.extraction.scrapers.fakenewsnet_scraper import FakeNewsNetScraper

        scrapers = {
            "AfpScraper":             AfpScraper,
            "France24Scraper":        France24Scraper,
            "GoogleFactCheckScraper": GoogleFactCheckScraper,
            # "FakeNewsNetScraper":   FakeNewsNetScraper,  # désactivé
        }

        scraper_cls = scrapers[scraper_name]
        scraper = scraper_cls()
        scraper.run()

        return {
            "scraper_name": scraper_name,
            "source_dir":   str(scraper.output_dir),
            "already_in_db": scraper.already_in_db,
        }

    # ------------------------------------------------------------------ #
    # TÂCHE 2 : Transformation (une tâche par source, en parallèle)      #
    # ------------------------------------------------------------------ #
    @task
    def process_source_task(scrap_result: dict) -> dict:
        """Transforme les articles bruts d'une source et écrit un JSON tmp sur disque.
        Ne retourne que les stats via XCom (pas les données brutes).
        """
        from config.config import PROCESSED_DATA_DIR
        from src.processing.pipeline import process_source

        stats = process_source(
            source_dir=Path(scrap_result["source_dir"]),
            output_dir=PROCESSED_DATA_DIR,
            already_in_db=scrap_result.get("already_in_db", 0),
        )
        return stats

    # ------------------------------------------------------------------ #
    # TÂCHE 3 : Merge — attend TOUTES les sources                        #
    # ------------------------------------------------------------------ #
    @task
    def merge_sources_task(all_stats: list[dict]) -> dict:
        """Fusionne les fichiers JSON tmp de chaque source en une seule liste.
        Lit depuis disque, écrit merged_*.json sur disque.
        Ne retourne que le summary via XCom.
        """
        from src.processing.pipeline import merge_sources

        summary = merge_sources(all_stats)
        return summary

    # ------------------------------------------------------------------ #
    # TÂCHE 4 : Sauvegarde Parquet                                       #
    # ------------------------------------------------------------------ #
    @task
    def save_to_parquet_task(summary: dict) -> dict:
        """Lit les fichiers fusionnés et les sauvegarde en Parquet."""
        from config.config import PROCESSED_DATA_DIR
        from src.processing.pipeline import save_to_parquet

        final_stats = save_to_parquet(
            summary=summary,
            output_dir=PROCESSED_DATA_DIR,
        )
        # On enrichit avec le summary pour le rapport final
        final_stats["summary"] = summary
        return final_stats

    # ------------------------------------------------------------------ #
    # TÂCHE 5 : Chargement Postgres                                      #
    # ------------------------------------------------------------------ #
    @task
    def load_to_postgres_task(stats: dict) -> str:
        """Charge les fichiers Parquet dans Postgres."""
        import os
        from sqlalchemy import create_engine, text
        from config.config import PROCESSED_DATA_DIR
        import pandas as pd

        # Construire la connection string depuis les variables d'env
        db_url = (
            f"postgresql://"
            f"{os.getenv('DB_WRITER_USER', 'writer')}:"
            f"{os.getenv('DB_WRITER_PASSWORD', '')}@"
            f"{os.getenv('DB_HOST', 'localhost')}:"
            f"{os.getenv('DB_PORT', '5432')}/"
            f"{os.getenv('DB_NAME', 'checkit')}"
        )
        engine = create_engine(db_url)

        articles_path = PROCESSED_DATA_DIR / "articles.parquet"
        images_path   = PROCESSED_DATA_DIR / "images.parquet"

        # Vider les tables AVANT d'insérer (TRUNCATE CASCADE)
        try:
            with engine.begin() as conn:
                conn.execute(text("TRUNCATE TABLE articles CASCADE"))
                conn.execute(text("TRUNCATE TABLE images CASCADE"))
            logger.info("[Postgres] Tables vidées (TRUNCATE)")
        except Exception as e:
            logger.warning(f"[Postgres] Erreur lors du TRUNCATE : {e}")

        articles_loaded = 0
        images_loaded = 0

        if articles_path.exists():
            df = pd.read_parquet(articles_path)
            df.to_sql("articles", engine, if_exists="append", index=False)
            articles_loaded = len(df)
            logger.info(f"[Postgres] {articles_loaded} articles chargés")
        else:
            logger.warning("[Postgres] Fichier articles.parquet introuvable — chargement ignoré.")

        if images_path.exists():
            df = pd.read_parquet(images_path)
            if "size" in df.columns:
                df["size"] = df["size"].apply(lambda x: x.tolist() if hasattr(x, "tolist") else x)
            df.to_sql("images", engine, if_exists="append", index=False)
            images_loaded = len(df)
            logger.info(f"[Postgres] {images_loaded} images chargées")
        else:
            logger.warning("[Postgres] Fichier images.parquet introuvable — chargement ignoré.")

        return (
            f"Postgres chargé : "
            f"{articles_loaded} articles, "
            f"{images_loaded} images."
        )

    # ------------------------------------------------------------------ #
    # TÂCHE 6 : Rapport final                                            #
    # ------------------------------------------------------------------ #
    @task
    def report_summary_task(postgres_result: str, parquet_stats: dict) -> None:
        """Affiche un rapport de synthèse lisible dans les logs Airflow."""
        from config.logger import logger

        summary = parquet_stats.get("summary", {})
        by_source = summary.get("by_source", {})

        logger.info("=" * 60)
        logger.info("  RAPPORT FINAL — CheckIt.AI Pipeline")
        logger.info("=" * 60)
        logger.info(f"  Articles valides    : {summary.get('valid_multimodal', 0)}/{summary.get('total_articles', 0)}")
        logger.info(f"  Articles ignorés    : {summary.get('skipped', 0)}")
        logger.info(f"  Déjà en base (skip) : {summary.get('already_in_db', 0)}")
        logger.info(f"  Erreurs             : {summary.get('errors', 0)}")
        logger.info(f"  Images téléchargées : {summary.get('total_images', 0)}")
        logger.info("")
        logger.info("  Détail par source :")
        logger.info(f"  {'Source':<25} {'Articles':>10} {'Taux':>6} {'Images':>8} {'Taux img':>9}")
        logger.info(f"  {'-'*25} {'-'*10} {'-'*6} {'-'*8} {'-'*9}")
        for source, s in by_source.items():
            logger.info(
                f"  {source:<25} "
                f"{s['valid']:>4}/{s['total']:<5} "
                f"{s['taux_articles']:>6} "
                f"{s['images']:>8} "
                f"{s['taux_images']:>9} "
                f"  db_skip={s.get('already_in_db', 0)}"
            )
        logger.info("=" * 60)
        logger.info(f"  Postgres : {postgres_result}")
        logger.info("=" * 60)

    # ------------------------------------------------------------------ #
    # ORCHESTRATION                                                       #
    # ------------------------------------------------------------------ #
    scrapers = [
        "AfpScraper",
        "France24Scraper",
        "GoogleFactCheckScraper",
        # "FakeNewsNetScraper",  # désactivé
    ]

    # Étape 1+2 : Scraping + transformation en parallèle pour chaque source
    all_process_results = []
    for scraper_name in scrapers:
        scrap_result    = scrap_news_task.override(task_id=f"scrap_{scraper_name}")(scraper_name)
        process_result  = process_source_task.override(task_id=f"process_{scraper_name}")(scrap_result)
        all_process_results.append(process_result)

    # Étape 3 : Merge quand toutes les sources sont prêtes
    merged = merge_sources_task(all_process_results)

    # Étape 4 : Sauvegarde Parquet
    parquet_stats = save_to_parquet_task(merged)

    # Étape 5 : Chargement Postgres
    postgres_result = load_to_postgres_task(parquet_stats)

    # Étape 6 : Rapport final
    report_summary_task(postgres_result, parquet_stats)


extract_news_workflow()