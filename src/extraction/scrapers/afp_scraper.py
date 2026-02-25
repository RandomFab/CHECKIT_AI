import requests
from bs4 import BeautifulSoup
from src.extraction.core.base_scraper import BaseScraper
from config.config import BASE_DIR


class AfpScrapper(BaseScraper):
    BASE_URL = "https://factuel.afp.com/"

    def __init__(self):
        super().__init__(
            source_name="AFP Factuel", output_dir=BASE_DIR / "data" / "raw" / "afp"
        )

    def extract(self) -> list[dict]:
        # NIVEAU 1
        response = requests.get(self.BASE_URL)
        soup1 = BeautifulSoup(response.text, "html.parser")
        articles = soup1.select(".node--type-article.node--view-mode-teaser")
        print(f"[AFP] {len(articles)} articles trouvés")

        results = []
        urls = [article.select_one("a").get("href") for article in articles]

        # NIVEAU 2
        for url in urls:
            article_data = self._scrape_article(url)
            if article_data:
                results.append(article_data)

        return results

    def _scrape_article(self, url: str) -> dict | None:

        try:

            response = requests.get(url, timeout=10)
            response.raise_for_status()
            soup2 = BeautifulSoup(response.text, "html.parser")

            # metadata

            ## Titre
            title = soup2.select_one("h1").text.strip()

            ## date
            date = soup2.select_one('meta[property="og:updated_time"]').get("content")

            ## Image principale
            main_image = None
            try:
                image_div = soup2.find("div", class_="image-wrapper")
                if image_div:
                    img = image_div.find("img")
                    if img:
                        main_image = img.get("src")
            except Exception as e:
                print(f"[AFP] Erreur extraction image : {e}")

            ## content blocks
            content_blocks = self._parse_body_wrapper(soup2)

            return {
                "id": f"afp_{hash(url) % 100000:05d}",
                "source": self.source_name,
                "title": title,
                "date": date,
                "url": url,
                "label": "Faux",
                "main_image": main_image,
                "content_blocks": content_blocks,
            }
        
        except Exception as e:
            print(f"[AFP] Erreur sur {url} : {e}")
            return None

    def _parse_body_wrapper(self, soup: BeautifulSoup) -> list[dict]:

        blocks = []
        body_wrapper = soup.find("div", class_="wrapper-body")

        if body_wrapper:
            content_body_wrapper = body_wrapper.find("div")
            
            if not content_body_wrapper:
                print("[AFP] body-wrapper est introuvable.")
        else : print("[AFP] content-body-wrapper est introuvable.")

        for element in body_wrapper.children:

            if element.name == "p":
                text = element.get_text(strip=True)
                if text:
                    blocks.append({"type": "paragraphe", "content": text})

            elif element.name == "div" and "wrapper-image" in (element.get("class") or []):
                img = element.find("img")
                if img and img.get("src"):
                    blocks.append({"type": "image", "url": img.get("src")})

            elif element.name in ["h1", "h2", "h3", "h4"]:
                text = element.get_text(strip=True)
                if text:
                    blocks.append({"type": "heading", "content": text})

        return blocks

    def close(self):
        """Pas de ressources à fermer (utilise BeautifulSoup/requests)."""
        pass
