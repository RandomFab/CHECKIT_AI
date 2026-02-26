from src.extraction.core.base_scraper import BaseScraper
from pathlib import Path
import requests

class APIClient(BaseScraper):

    def __init__(self, source_name:str, output_dir:Path,base_url:str, api_key: str = None):

        super().__init__(source_name, output_dir)

        self.base_url = base_url.rstrip("/")
        self.session = requests.Session()
        self.session.headers.update({
            'Accept' : "application/json",
            'Content-Type' : 'application/json'
        })

        if api_key:
            self.session.headers.update({"Authorization":f"Bearer {api_key}"})
    
    def get(self, endpoint:str, params: dict = None) -> dict:
        url = f"{self.base_url}/{endpoint.lstrip('/')}"
        response = self.session.get(url, params = params)
        response.raise_for_status()
        return response.json()
    
    def extract(self) -> list[dict]:
        raise NotImplementedError("Implémenter exteact() dans la classe fille dédiée")