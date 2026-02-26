from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from src.extraction.core.base_scraper import BaseScraper
from pathlib import Path
import undetected_chromedriver as uc


class SeleniumScraper(BaseScraper):
    def __init__(self, source_name: str, output_dir: Path, headless: bool = True):
        super().__init__(source_name, output_dir)
        self.driver = self._init_driver(headless)

    def _init_driver(self, headless: bool):
        options = uc.ChromeOptions()
        
        # Simule un vrai navigateur
        options.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
        options.add_argument("--disable-blink-features=AutomationControlled")
        
        driver = uc.Chrome(options=options, version_main=145)
        
        # Supprime la propriété webdriver
        driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
    
        return driver

    def close(self):
        if self.driver:
            try:
                self.driver.quit()
                self.driver = None  # marque comme fermé
            except:
                pass 

    def run(self):
        try:
            super().run()
        finally:
            self.close()
