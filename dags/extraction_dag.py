"""
## CheckIt.AI - Pipeline ETL

Orchestre l'extraction (scraping), la transformation et la sauvegarde
des articles de fact-checking.
"""

from airflow.sdk import dag, task
from pendulum import datetime


@dag(
    start_date=datetime(2025, 7, 1),
    schedule="@weekly",
    catchup=False,
    doc_md=__doc__,
    default_args={"owner": "checkit", "retries": 1},
    tags=["checkit", "etl"],
)
def checkit_pipeline():

    @task()
    def extract_afp() -> str:
        """Scrape AFP Factuel et sauvegarde dans data/raw/afp/data.json"""
        from src.extraction.scrapers.afp_scraper import AfpScraper

        scraper = AfpScraper(headless=True)
        try:
            data = scraper.extract()
            scraper.save(data)
            return f"AFP: {len(data)} articles extraits"
        finally:
            scraper.close()

    @task()
    def extract_france24() -> str:
        """Scrape France 24 Les Observateurs"""
        from src.extraction.scrapers.france24_scraper import France24Scraper

        scraper = France24Scraper(headless=True)
        try:
            data = scraper.extract()
            scraper.save(data)
            return f"F24: {len(data)} articles extraits"
        finally:
            scraper.close()

    @task()
    def extract_google_factcheck() -> str:
        """Interroge l'API Google Fact Check"""
        from src.extraction.scrapers.google_factcheck_client import GoogleFactCheckScraper

        scraper = GoogleFactCheckScraper()
        data = scraper.extract()
        scraper.save(data)
        return f"GFC: {len(data)} articles extraits"

    @task()
    def extract_fakenewsnet() -> str:
        """Télécharge le dataset FakeNewsNet depuis Kaggle"""
        from src.extraction.scrapers.fakenewsnet_scraper import FakeNewsNetScraper

        scraper = FakeNewsNetScraper()
        data = scraper.extract()
        scraper.save(data)
        return f"FNN: {len(data)} articles extraits"

    @task()
    def transform_and_save(extraction_results: list[str]) -> str:
        """Transforme toutes les sources et sauvegarde en Parquet"""
        from src.processing.pipeline import process_all_sources, save_to_parquet
        from config.config import RAW_DATA_DIR, PROCESSED_DATA_DIR

        articles, images, summary = process_all_sources(RAW_DATA_DIR, PROCESSED_DATA_DIR)
        save_to_parquet(articles, images, PROCESSED_DATA_DIR)

        return (
            f"Articles: {summary['total_articles']} lus, "
            f"{summary['valid_multimodal']} valides, "
            f"{summary['total_images']} images"
        )

    # --- Orchestration ---
    # Les extractions tournent en parallèle
    afp = extract_afp()
    f24 = extract_france24()
    gfc = extract_google_factcheck()
    fnn = extract_fakenewsnet()

    # La transformation attend que TOUTES les extractions soient terminées
    transform_and_save(extraction_results=[afp, f24, gfc, fnn])


checkit_pipeline()