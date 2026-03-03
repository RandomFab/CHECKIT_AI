from pathlib import Path
from src.processing.image_handler import (
    download_image,
    validate_image_file,
)

from src.processing.cleaning import (
    clean_text_content,
    is_url_reachable,
    normalize_label,
)

from config.logger import logger


def extract_images_from_blocks(
    content_blocks: list[dict],
    main_image: str | None,
    article_id: str,
    output_dir: Path,
) -> list[dict]:
    """Traite main_image + images dans content_blocks.
    Retourne liste d'enregistrements IMAGE téléchargés"""

    extracted_images = []

    # Collecte toutes les URLs à traiter (main_image + images dans les blocs)
    urls_to_process = []
    if main_image:
        urls_to_process.append(main_image)

    for block in content_blocks:
        if block.get("type") == "image" and block.get("content"):
            urls_to_process.append(block["content"])

    # Traitement unique pour chaque URL
    for idx, url in enumerate(urls_to_process):
        try:
            # Générer un nom de fichier unique basé sur l'article_id et un index
            filename = f"{article_id}_img_{idx}"

            # Téléchargement (download_image doit gérer les erreurs HTTP en interne)
            file_path = download_image(url, output_dir=output_dir, filename=filename)

            if file_path:
                status, img_format, img_size = validate_image_file(file_path)

                if status:
                    extracted_images.append(
                        {
                            "article_id": article_id,
                            "image_url": url,
                            "local_path": str(file_path),
                            "format": img_format,
                            "size": img_size,
                        }
                    )
        except Exception as e:
            # On passe à l'image suivante en cas d'erreur de téléchargement ou validation
            logger.error(
                f"Erreur lors de l'extraction de l'image {url} pour l'article {article_id}: {e}"
            )
            continue

    return extracted_images


def extract_text_from_blocks(
    content_blocks: list[dict],
    article_id: str,
) -> list[dict]:
    """Extrait et nettoie les paragraphes des content_blocks.
    Retourne une liste de dicts avec le texte brut et nettoyé."""
    extracted_texts = []

    for idx, block in enumerate(content_blocks):
        # On ne traite que les blocs de type 'heading' ou 'paragraphe' avec du contenu
        if block.get("type") in ["heading", "paragraphe"] and block.get("content"):
            raw_text = block["content"]
            try:
                cleaned_text = clean_text_content(raw_text)
                extracted_texts.append({
                    "article_id": article_id,
                    "block_index": idx,
                    "raw_text": raw_text,
                    "cleaned_text": cleaned_text,
                })
            except Exception as e:
                logger.error(f"Erreur nettoyage texte bloc {idx} pour article {article_id}: {e}")
                continue

    return extracted_texts


def normalize_article_label(label: str) -> str:
    """Normalise le label brut d'un article en 'vrai', 'faux' ou 'partiellement_faux'.
    Retourne 'inconnu' si le label ne peut pas être mappé avec confiance."""
    normalized = normalize_label(label)
    logger.info(f"Label '{label}' → '{normalized}'")
    return normalized


def validate_article_url(url: str) -> bool:
    """Vérifie que l'URL d'un article est valide syntaxiquement ET accessible en ligne.
    Retourne True uniquement si les deux conditions sont remplies."""
    if not url or not isinstance(url, str):
        return False
    # Vérification syntaxique (pas de requête réseau)
    if not url.startswith(("http://", "https://")):
        logger.warning(f"URL syntaxiquement invalide : {url}")
        return False
    # Vérification réseau (requête HTTP HEAD)
    reachable = is_url_reachable(url)
    if not reachable:
        logger.warning(f"URL inaccessible : {url}")
    return reachable


def validate_article_is_multimodal(article: dict) -> bool:
    """Vérifie qu'un article contient au moins une image (bloc ou image principale) ET un bloc texte.
    Un article multimodal est requis pour l'entraînement du modèle."""
    has_image = (
        any(b.get("type") == "image" for b in article.get("content_blocks", []))
        or bool(article.get("main_image"))
    )
    has_text = any(b.get("type") == "paragraphe" for b in article.get("content_blocks", []))
    return has_image and has_text


def validate_article_has_label(article: dict) -> bool:
    """Vérifie qu'un article a un label normalisable (pas 'inconnu')."""
    raw_label = article.get("label", "")
    return normalize_label(raw_label) != "inconnu"
