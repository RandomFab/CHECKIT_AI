from src.extraction.core.selenium_scraper import SeleniumScraper
from config.config import BASE_DIR
from config.logger import logger
from bs4 import BeautifulSoup, Tag
from src.extraction.scrapers.article_schema import ArticleSchema
import hashlib
import time
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC



class France24Scraper(SeleniumScraper):
    BASE_URL = "https://www.france24.com/fr/%C3%A9missions/observateurs/"

    def __init__(self, headless: bool = False):
        super().__init__(
            source_name="France_24",
            output_dir=BASE_DIR / "data" / "raw" / "france24",
            headless=headless,
        )

    def _get_soup(self, url: str) -> BeautifulSoup | None:
        """Charge une URL avec Selenium (anti-bot) et retourne un objet BeautifulSoup."""
        try:
            self.driver.get(url)
            self._handle_cookies()
            return BeautifulSoup(self.driver.page_source, "html.parser")
        except Exception as e:
            logger.error(f"[F24] Impossible de charger : {url} → {e}")
            return None

    def _handle_cookies(self):
        """Tente de fermer la pop-up cookies si elle apparaît."""
        try:
            # On attend très peu de temps (2s) car si elle n'est pas là, on ne veut pas bloquer
            wait = WebDriverWait(self.driver, 2)
            # Sélecteur Didomi (France 24, AFP, etc.)
            button = wait.until(EC.element_to_be_clickable((By.ID, "didomi-notice-agree-button")))
            button.click()
            logger.debug("[F24] Pop-up cookies acceptée")
            # Petit temps pour que la pop-up disparaisse physiquement
            time.sleep(1)
        except:
            # Si pas de bouton, on ignore
            pass

    def extract(self) -> list[dict]:

        logger.info(f"[F24] Chargement de la page d'accueil : {self.BASE_URL}")

        soup = self._get_soup(self.BASE_URL)

        if not soup:
            return []

        logger.debug(f"[F24] Taille du HTML reçu : {len(str(soup))} caractères")

        articles = soup.select(".o-layout-list .o-layout-list__item")
        logger.info(f"[F24] {len(articles)} articles trouvés sur la page d'accueil")

        if not articles:
            logger.warning(
                "[F24] Aucun article trouvé — le sélecteur CSS est peut-être incorrect ou la page a changé"
            )
            # logger.debug(f"[F24] Début du HTML reçu :\n{response.text[:500]}")
            return []

        urls = []

        for article in articles:
            a_tag = article.select_one("a")
            if a_tag and a_tag.get("href"):
                url = a_tag.get("href")
                if url.startswith("/"):
                    url = "https://www.france24.com" + url
                urls.append(url)
            else:
                logger.warning(f"[F24] Une vignette sans lien ignorée")
                
        logger.info(f"[F24] {len(urls)} URLs extraites")

        # Déduplication : filtre les URLs déjà présentes en base
        existing_urls = self._fetch_existing_urls()
        filtered_urls = [url for url in urls if url not in existing_urls]
        self.already_in_db = len(urls) - len(filtered_urls)
        if self.already_in_db > 0:
            logger.info(f"[F24] {self.already_in_db} article(s) déjà en base → ignorés")
        urls = filtered_urls

        results = []

        for i, url in enumerate(urls):
            logger.info(f"[F24] Scraping article {i+1}/{len(urls)} : {url}")
            article_data = self._scrape_f24_article(url)
            if article_data:
                results.append(article_data)
                logger.info(f"[F24]   ✓ Réussi")
            else:
                logger.info(f"[F24]   ✗ Raté")

        logger.info(f"[F24] Extraction terminée : {len(results)}/{len(urls)} articles récupérés")
        return results

    def _scrape_f24_article(self, url: str) -> dict | None:
        try:
            soup = self._get_soup(url)
            if not soup:
                return None

            # --- Titre ---
            h1 = soup.find("h1", class_="t-content__title a-page-title")
            if not h1:
                logger.warning(f"[F24] Pas de balise <h1> sur : {url}")
                return None
            title = h1.text.strip()
            logger.debug(f"[F24] Titre : {title}")

            # --- Date ---
            date_meta = soup.select_one('meta[property="article:modified_time"]')
            if not date_meta:
                logger.warning(f"[F24] Pas de meta article:modified_time sur : {url}")
            date = date_meta.get("content") if date_meta else None

            # --- Image principale ---
            main_image = None
            image_meta = soup.select_one('meta[property="og:image"]')
            if image_meta:
                main_image = image_meta.get('content')
                if main_image and main_image.startswith("/"):
                    main_image = "https://www.france24.com" + main_image
                logger.debug(f"[F24] Image principale : {main_image}")
            else:
                logger.warning(f"[F24] Pas de meta og:image sur : {url}")

            # --- Content blocks ---
            content_blocks = self._parse_body_wrapper(soup)
            logger.debug(f"[F24] {len(content_blocks)} blocs extraits pour : {url}")

            article = ArticleSchema(
                id = f"F24_{hashlib.md5(url.encode()).hexdigest()[:8]}",
                source = self.source_name,
                title = title,
                date = date,
                url = url,
                label = "Faux",
                main_image = main_image,
                content_blocks = content_blocks)
            
            return article.model_dump() 

        except Exception as e:
            logger.error(f"[F24] Erreur inattendue sur {url} : {e}")
            return None
        
    def _parse_body_wrapper(self, soup: BeautifulSoup) -> list[dict]:

        blocks = []
        body_wrapper = soup.find("div", class_="t-content__body u-clearfix")

        if not body_wrapper:
            logger.warning("[F24] div.t-content__body u-clearfix introuvable — le sélecteur est peut-être incorrect")
            return blocks  # ← retour immédiat, évite le crash ci-dessous

        children = [e for e in body_wrapper.children if isinstance(e, Tag)]
        logger.debug(f"[F24] {len(children)} éléments enfants dans t-content__body")

        for element in children:

            if element.name == "p":
                text = element.get_text(strip=True)
                if text:
                    blocks.append({"type": "paragraphe", "content": text})
            
            elif element.name == "blockquote":
                for child in element.find_all(["p", "div"], recursive=False):
                    if child.name == "p":
                        text = child.get_text(strip=True)
                        if text:
                            blocks.append({"type": "paragraphe", "content": text})
                    elif child.name == "div" and "m-em-image" in (child.get("class") or []):
                        img = child.find("img")
                        if img and img.get("src"):
                            img_url = img.get("src")
                            if img_url and img_url.startswith("/"):
                                img_url = "https://www.france24.com" + img_url
                            blocks.append({"type": "image", "content": img_url})

            elif element.name == "div" and "m-em-image" in (element.get("class") or []):
                img = element.find("img")
                if img and img.get("src"):
                    img_url = img.get("src")
                    if img_url and img_url.startswith("/"):
                        img_url = "https://www.france24.com" + img_url
                    blocks.append({"type": "image", "content": img_url})

            elif element.name in ["h1", "h2", "h3", "h4"]:
                text = element.get_text(strip=True)
                if text:
                    blocks.append({"type": "heading", "content": text})

        return blocks


if __name__ == "__main__":
    scraper = France24Scraper()
    scraper.run()