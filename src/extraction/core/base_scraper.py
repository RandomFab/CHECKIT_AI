from config.config import BASE_DIR
from abc import ABC, abstractmethod
from pathlib import Path
import json


class BaseScraper(ABC):

    def __init__(self, source_name: str, output_dir: Path):

        self.source_name = source_name
        self.output_dir = output_dir
        self.output_dir.mkdir(parents=True, exist_ok=True)

    @abstractmethod
    def extract(self) -> list[dict]:
        """
        CONTRAT : chaque scraper enfant DOIT écrire cette méthode.
        Elle doit retourner une liste de dictionnaires comme :
        [
            {
                "id": "afp_001",
                "title": "...",
                "text": "...",
                "image_url": "...",
                "label": "Faux",
                "date": "2026-02-24"
            },
            ...
        ]
        """
        pass

    def save(self, data: list[dict], filename: str = "data.json"):
        """
        Enregistre au format json les informations extraites
        """
        filepath = self.output_dir / filename
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        print(f"[{self.source_name}] {len(data)} entrée → {filepath}")

    def run(self):
        """
        Chef d'orechestre : appelle extract() puis save()
        """
        print(f"[{self.source_name}] Début de l'extraction...")
        data = self.extract()
        self.save(data)
        print(f"[{self.source_name}] Terminé")
