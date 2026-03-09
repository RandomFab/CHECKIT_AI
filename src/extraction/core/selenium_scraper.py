import os
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from src.extraction.core.base_scraper import BaseScraper
from pathlib import Path
from config.logger import logger

# Importe undetected_chromedriver uniquement en local
try:
    import undetected_chromedriver as uc
    HAS_UC = True
except ImportError:
    HAS_UC = False


class SeleniumScraper(BaseScraper):
    def __init__(self, source_name: str, output_dir: Path, headless: bool = True):
        super().__init__(source_name, output_dir)
        self.driver = self._init_driver(headless)

    def _init_driver(self, headless: bool):
        """
        Initialise un WebDriver Selenium.
        
        Mode 1 (Docker/Airflow) : Si SELENIUM_REMOTE_URL est défini,
        connecte à un conteneur Selenium distant.
        
        Mode 2 (Local) : Utilise undetected_chromedriver pour contourner
        les mesures anti-bot (WAF Akamai, etc.)
        """
        remote_url = os.getenv("SELENIUM_REMOTE_URL")

        if remote_url:
            # --- Mode REMOTE (Docker / Airflow) ---
            logger.info(f"[{self.source_name}] Initialisation du WebDriver REMOTE : {remote_url}")
            options = Options()
            options.add_argument("--no-sandbox")
            options.add_argument("--disable-dev-shm-usage")
            options.add_argument("--disable-blink-features=AutomationControlled")
            options.add_argument("--disable-gpu")
            options.add_argument(
                "--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
            )
            if headless:
                options.add_argument("--headless=new")

            driver = webdriver.Remote(
                command_executor=remote_url,
                options=options,
            )
            driver.execute_script(
                "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
            )
            logger.info(f"[{self.source_name}] WebDriver REMOTE connecté")
            return driver

        else:
            # --- Mode LOCAL (dev sur ta machine) ---
            if not HAS_UC:
                raise ImportError(
                    "undetected_chromedriver est requis en mode local. "
                    "Installe: pip install undetected-chromedriver"
                )

            logger.info(f"[{self.source_name}] Initialisation du WebDriver LOCAL (undetected_chromedriver)")
            options = uc.ChromeOptions()
            options.add_argument(
                "--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
            )
            options.add_argument("--disable-blink-features=AutomationControlled")

            driver = uc.Chrome(options=options, version_main=145)
            driver.execute_script(
                "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
            )
            logger.info(f"[{self.source_name}] WebDriver LOCAL prêt")
            return driver

    def close(self):
        if self.driver:
            try:
                self.driver.quit()
                self.driver = None
            except Exception as e:
                logger.warning(f"[{self.source_name}] Erreur lors de la fermeture du driver : {e}")

    def run(self):
        try:
            super().run()
        finally:
            self.close()
