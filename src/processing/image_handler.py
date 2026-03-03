from pathlib import Path
import requests
from PIL import Image
from config.logger import logger


def is_url_reachable(url: str) -> bool:
    """Vérifie si l'URL répond avec un code succès (200)."""
    try:

        response = requests.head(url, timeout=5, allow_redirects=True)
        return response.status_code == 200
    except requests.RequestException as e:
        logger.error(f"URL non joignable : {url} - Erreur: {e}")
        return False


def download_image(image_url: str, output_dir: Path, filename: str) -> str | None:
    """Télécharge image et retourne le chemin local
    Ex: "http://..." → "data/processed/images/img_abc123.jpg"
    Gère les erreurs (timeout, format invalide, 404)"""
    output_dir.mkdir(parents=True, exist_ok=True)
    file_path = output_dir / filename

    try:
        response = requests.get(image_url, timeout=10)
        response.raise_for_status()
        with open(file_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)

        return str(file_path)
    except Exception as e:
        logger.error(f"Erreur lors du téléchargement de {image_url} : {e}")
        return ""


def validate_image_file(image_path: Path) -> bool:
    """Vérifie que le fichier existe et est lisible"""
    try:
        with Image.open(image_path) as img:
            img.verify()
            return True, img.format, img.size

    except Exception as e:
        logger.error(f"Fichier image invalide ou corrompu : {image_path} - Erreur: {e}")
        return False, None, None
