from bs4 import BeautifulSoup
from src.extraction.core.selenium_scrapper import SeleniumScraper
from config.config import BASE_DIR
from config.logger import logger


class AfpScrapper(SeleniumScraper):
    BASE_URL = "https://factuel.afp.com/"

    def __init__(self, headless: bool = False):
        super().__init__(
            source_name="AFP Factuel",
            output_dir=BASE_DIR / "data" / "raw" / "afp",
            headless=headless,
        )

    def _get_soup(self, url: str) -> BeautifulSoup | None:
        """Charge une URL avec Selenium (anti-bot) et retourne un objet BeautifulSoup."""
        try:
            self.driver.get(url)
            return BeautifulSoup(self.driver.page_source, "html.parser")
        except Exception as e:
            logger.error(f"[AFP] Impossible de charger : {url} → {e}")
            return None

    def extract(self) -> list[dict]:
        # NIVEAU 1
        logger.info(f"[AFP] Chargement de la page d'accueil : {self.BASE_URL}")

        soup1 = self._get_soup(self.BASE_URL)
        if not soup1:
            return []

        logger.debug(f"[AFP] Taille du HTML reçu : {len(str(soup1))} caractères")

        articles = soup1.select(".node--type-article.node--view-mode-teaser")
        logger.info(f"[AFP] {len(articles)} articles trouvés sur la page d'accueil")

        if not articles:
            logger.warning("[AFP] Aucun article trouvé — le sélecteur CSS est peut-être incorrect ou la page a changé")
            # logger.debug(f"[AFP] Début du HTML reçu :\n{response.text[:500]}")
            return []

        urls = []
        for article in articles:
            a_tag = article.select_one("a")
            if a_tag and a_tag.get("href"):
                urls.append(a_tag.get("href"))
            else:
                logger.warning(f"[AFP] Une vignette sans lien ignorée")

        logger.info(f"[AFP] {len(urls)} URLs extraites")

        results = []
        # NIVEAU 2
        for i, url in enumerate(urls):
            logger.info(f"[AFP] Scraping article {i+1}/{len(urls)} : {url}")
            article_data = self._scrape_article(url)
            if article_data:
                results.append(article_data)
            else:
                logger.warning(f"[AFP] Article ignoré (erreur) : {url}")

        logger.info(f"[AFP] Extraction terminée : {len(results)}/{len(urls)} articles récupérés")
        return results

    def _scrape_article(self, url: str) -> dict | None:
        try:
            soup2 = self._get_soup(url)
            if not soup2:
                return None

            # --- Titre ---
            h1 = soup2.select_one("h1")
            if not h1:
                logger.warning(f"[AFP] Pas de balise <h1> sur : {url}")
                return None
            title = h1.text.strip()
            logger.debug(f"[AFP] Titre : {title}")

            # --- Date ---
            date_meta = soup2.select_one('meta[property="og:updated_time"]')
            if not date_meta:
                logger.warning(f"[AFP] Pas de meta og:updated_time sur : {url}")
            date = date_meta.get("content") if date_meta else None

            # --- Image principale ---
            main_image = None
            image_div = soup2.find("div", class_="image-wrapper")
            if image_div:
                img = image_div.find("img")
                if img:
                    main_image = img.get("src")
                    logger.debug(f"[AFP] Image principale : {main_image}")
                else:
                    logger.warning(f"[AFP] div.image-wrapper trouvée mais pas d'<img> dedans sur : {url}")
            else:
                logger.warning(f"[AFP] Pas de div.image-wrapper sur : {url}")

            # --- Content blocks ---
            content_blocks = self._parse_body_wrapper(soup2)
            logger.debug(f"[AFP] {len(content_blocks)} blocs extraits pour : {url}")

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
            logger.error(f"[AFP] Erreur inattendue sur {url} : {e}")
            return None

    def _parse_body_wrapper(self, soup: BeautifulSoup) -> list[dict]:

        blocks = []
        body_wrapper = soup.find("div", class_="wrapper-body")

        if not body_wrapper:
            logger.warning("[AFP] div.wrapper-body introuvable — le sélecteur est peut-être incorrect")
            return blocks  # ← retour immédiat, évite le crash ci-dessous

        children = [e for e in body_wrapper.children if not (isinstance(e, str) and not e.strip())]
        logger.debug(f"[AFP] {len(children)} éléments enfants dans wrapper-body")

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
        """Ferme le driver Selenium (hérité de SeleniumScraper)."""
        super().close()
