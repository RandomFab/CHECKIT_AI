from selenium.webdriver.common.by import By
from src.extraction.core.selenium_scrapper import SeleniumScraper
from config.config import BASE_DIR


class AfpScrapper(SeleniumScraper):
    BASE_URL = "https://factuel.afp.com/"

    def __init__(self):
        super().__init__(
            source_name="AFP Factuel", output_dir=BASE_DIR / "data" / "raw" / "afp"
        )

    def extract(self) -> list[dict]:
        self.driver.get(self.BASE_URL)
        print(f"[AFP] URL chargée : {self.driver.current_url}")
        print(f"[AFP] Titre de la page : {self.driver.title}")
        print(f"[AFP] Taille du DOM : {len(self.driver.page_source)} caractères")

        # NIVEAU 1 : Accès aux données des articles

        articles = self.driver.find_elements(
            By.CSS_SELECTOR,
            ".node--type-homepage .view-content .node--type-article.node--view-mode-teaser",
        )

        urls = [a.find_element(By.CSS_SELECTOR,"a").get_attribute("href") for a in articles]

        return urls