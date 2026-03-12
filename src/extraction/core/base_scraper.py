import json
import os
from abc import ABC, abstractmethod
from pathlib import Path

from config.config import BASE_DIR
from config.logger import logger


class BaseScraper(ABC):

    def __init__(self, source_name: str, output_dir: Path):

        self.source_name = source_name
        self.output_dir = output_dir
        self.output_dir.mkdir(parents=True, exist_ok=True)
        # Nombre d'URLs ignorées car déjà présentes en base
        self.already_in_db: int = 0

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

    def _fetch_existing_urls(self) -> set:
        """Retourne l'ensemble des URLs déjà présentes en base pour cette source.

        Interroge la table `articles` filtrée par `source_name`.
        Retourne un set vide si la base est inaccessible (pas d'erreur fatale).
        """
        try:
            import psycopg2
            conn = psycopg2.connect(
                host=os.getenv("DB_HOST", "localhost"),
                port=int(os.getenv("DB_PORT", "5432")),
                dbname=os.getenv("DB_NAME", "checkit"),
                user=os.getenv("DB_WRITER_USER", "writer"),
                password=os.getenv("DB_WRITER_PASSWORD", ""),
            )
            with conn.cursor() as cur:
                cur.execute("SELECT url FROM articles WHERE source = %s", (self.source_name,))
                existing = {row[0] for row in cur.fetchall()}
            conn.close()
            logger.info(f"[{self.source_name}] {len(existing)} URL(s) déjà en base → déduplication activée")
            return existing
        except Exception as e:
            logger.warning(f"[{self.source_name}] Base inaccessible, déduplication désactivée : {e}")
            return set()

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
    