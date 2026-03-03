from dotenv import load_dotenv
from src.extraction.core.api_client import APIClient
from src.extraction.scrapers.article_schema import ArticleSchema, ContentBlock
from config.config import BASE_DIR
from config.logger import logger
import hashlib
import os
import requests
from bs4 import BeautifulSoup

load_dotenv()


class GoogleFactCheckScrapper(APIClient):

    def __init__(self, queries: list[str], lang: str = "fr"):
        super().__init__(
            source_name="google_fact_check",
            output_dir=BASE_DIR / "data" / "raw" / "google_fact_check",
            base_url="https://factchecktools.googleapis.com/v1alpha1/",
        )
        self.api_key = os.getenv("GOOGLE_FACT_CHECK_API_KEY")
        self.queries = queries
        self.lang = lang

    def _get_og_image(self, url: str) -> str | None:
        """Tente de récupérer l'image Open Graph d'une page HTML."""
        try:
            response = requests.get(url, timeout=10)
            if response.status_code == 200:
                soup = BeautifulSoup(response.text, "html.parser")
                og_image = soup.find("meta", property="og:image")
                if og_image and og_image.get("content"):
                    return og_image["content"]
        except Exception as e:
            logger.warning(f"Impossible de récupérer l'image OG pour {url}: {e}")
        return None

    def extract(self) -> list[dict]:
        urls = []
        results = []

        # Niveau 1
        for query in self.queries:

            logger.info(f"[GoogleFactCheck] Recherche : '{query}'")

            params = {
                "query": query,
                "languageCode": self.lang,
                "key": self.api_key,
                "pageSize": 100,
            }

            page = 1
            while True:
                logger.info(f"[GoogleFactCheck] Page {page} pour '{query}'")

                data = self.get(endpoint="claims:search", params=params)

                for claim in data.get("claims", []):
                    claim_review = claim["claimReview"][0]
                    claim_url = claim_review["url"]

                    if claim_url not in urls:
                        urls.append(claim_url)
                        claim_title = claim_review.get("title", "")
                        claim_date = claim_review.get("reviewDate")
                        claim_label = claim_review.get("textualRating", "")
                        claim_text = claim.get("text", "")

                        logger.info(f"[GoogleFactCheck] Récupération de l'image pour {claim_url}")
                        main_image = self._get_og_image(claim_url)

                        article = ArticleSchema(
                            id=f"gfc_{hashlib.md5(claim_url.encode()).hexdigest()[:8]}",
                            source=self.source_name,
                            title=claim_title,
                            date=claim_date,
                            url=claim_url,
                            label=claim_label,
                            main_image=main_image,
                            content_blocks=[
                                ContentBlock(type="paragraphe", content=claim_text)
                            ],
                        )
                        results.append(article.model_dump())

                next_page_token = data.get("nextPageToken")
                if not next_page_token:
                    logger.info(f"[GoogleFactCheck] Fin de la pagination pour '{query}' ({page} page(s))")
                    break

                params["pageToken"] = next_page_token
                page += 1

        return results


if __name__ == "__main__":
    scraper = GoogleFactCheckScrapper(queries=["politique", "vaccin", "élection"])
    scraper.run()
