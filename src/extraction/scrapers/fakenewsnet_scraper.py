import kagglehub
import pandas as pd
import ast
from config.logger import logger
from config.config import BASE_DIR
from pathlib import Path
from typing import Optional
from src.extraction.core.base_scraper import BaseScraper
from src.extraction.scrapers.article_schema import ArticleSchema


class FakeNewsNetScraper(BaseScraper):

    DATASETS = {
        "BuzzFeed_fake_news_content.csv": False,
        "BuzzFeed_real_news_content.csv": True,
        "PolitiFact_fake_news_content.csv": False,
        "PolitiFact_real_news_content.csv": True,
    }

    def __init__(self, output_dir: Path = BASE_DIR / "data" / "raw" / "fakenewsnet"):
        super().__init__(source_name="FakeNewsNet", output_dir=output_dir)
        self._base_path = None

    def _download_dataset(self) -> Path:
        if not self._base_path:
            logger.info("[FakeNewsNet] Téléchargement du dataset Kaggle...")
            self._base_path = Path(kagglehub.dataset_download("mdepak/fakenewsnet"))
        return self._base_path

    def _parse_date(self, raw_date) -> Optional[str]:
        """Convertit le format MongoDB {'$date': timestamp_ms} en chaîne ISO 8601."""
        if pd.isna(raw_date) or raw_date == "":
            return None
        try:
            date_dict = ast.literal_eval(raw_date)
            timestamp_ms = date_dict.get("$date")
            if timestamp_ms:
                return pd.to_datetime(timestamp_ms, unit="ms").isoformat()
        except (ValueError, SyntaxError):
            pass
        return None

    def _get_content_blocks(self, row: pd.Series) -> list[dict]:
        blocks = []
        if pd.notnull(row.get("text")):
            blocks.append({"type": "paragraphe", "content": row["text"]})

        images_str = row.get("images")
        if isinstance(images_str, str) and images_str.strip():
            for url in [img.strip() for img in images_str.split(",") if img.strip()]:
                blocks.append({"type": "image", "content": url})
        return blocks

    def _parse_row(self, row: pd.Series, label: bool) -> Optional[dict]:
        try:
            article = ArticleSchema(
                id=str(row["id"]),
                source=self.source_name,
                title=row.get("title", ""),
                url=row.get("url", ""),
                label="Vrai" if label else "Faux",
                main_image=row.get("top_img") if pd.notnull(row.get("top_img")) else None,
                content_blocks=self._get_content_blocks(row),
                date=self._parse_date(row.get("publish_date")),
            )
            return article.model_dump()
        except Exception as e:
            logger.warning(f"[FakeNewsNet] Ligne ignorée : {e}")
            return None

    # --- Méthode principale imposée par BaseScraper ---

    def extract(self) -> list[dict]:
        path = self._download_dataset()
        results = []

        for filename, label in self.DATASETS.items():
            file_path = path / filename
            logger.info(f"[FakeNewsNet] Traitement de {filename}...")
            count_before = len(results)
            try:
                df = pd.read_csv(file_path, encoding="utf-8")
                for _, row in df.iterrows():
                    article = self._parse_row(row, label)
                    if article:
                        results.append(article)
                logger.info(f"[FakeNewsNet] {len(results) - count_before} articles extraits de {filename}")
            except Exception as e:
                logger.error(f"[FakeNewsNet] Erreur sur {filename} : {e}")

        return results


if __name__ == "__main__":
    scraper = FakeNewsNetScraper()
    scraper.run()
