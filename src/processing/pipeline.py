"""
pipeline.py - Orchestration principale du pipeline de transformation.

Ce fichier est le "chef d'orchestre" : il ne fait RIEN lui-même,
il appelle les fonctions des autres modules dans le bon ordre.

Flux :
    data/raw/{source}/data.json
        → read_raw_json()
        → _transform_one_article()   [pour chaque article]
        → process_source()           [pour chaque source]
        → process_all_sources()      [agrège tout]
        → save_to_parquet()
    data/processed/articles.parquet
    data/processed/images.parquet
"""

import json
import uuid
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from config.config import PROCESSED_DATA_DIR, RAW_DATA_DIR
from config.logger import logger
from src.processing.cleaning import clean_text_content
from src.processing.date_normalizer import normalize_date
from src.processing.schema_transformer import (
    extract_images_from_blocks,
    extract_text_from_blocks,
    normalize_article_label,
    validate_article_has_label,
    validate_article_is_multimodal,
)


# ---------------------------------------------------------------------------
# 1. LECTURE
# ---------------------------------------------------------------------------

def read_raw_json(source_dir: Path) -> list[dict]:
    """Lit le fichier data.json d'une source et retourne la liste d'articles bruts.

    Pourquoi cette fonction existe séparément ?
    → Principe de responsabilité unique (SRP) : une seule chose = lire le fichier.
      Si demain le format change (CSV, API...), on ne touche QUE cette fonction.

    Args:
        source_dir: Chemin vers le dossier d'une source, ex: data/raw/afp/

    Returns:
        Liste de dicts bruts. Retourne [] si le fichier est absent ou corrompu.
    """
    json_path = source_dir / "data.json"

    if not json_path.exists():
        logger.warning(f"[Pipeline] Fichier introuvable : {json_path}")
        return []

    try:
        with open(json_path, encoding="utf-8") as f:
            data = json.load(f)
        logger.info(f"[Pipeline] {len(data)} articles lus depuis {json_path}")
        return data
    except json.JSONDecodeError as e:
        logger.error(f"[Pipeline] JSON invalide dans {json_path} : {e}")
        return []


# ---------------------------------------------------------------------------
# 2. TRANSFORMATION (1 article)
# ---------------------------------------------------------------------------

def _transform_one_article(raw_article: dict, images_output_dir: Path) -> tuple[dict | None, list[dict]]:
    """Transforme 1 article brut (ArticleSchema) en 2 entités du schéma conceptuel.

    C'est la fonction CENTRALE du pipeline. Elle prend un article tel que
    scrappé (avec content_blocks mélangés) et produit :
      - 1 enregistrement ARTICLE (texte nettoyé, date normalisée, label normalisé)
      - N enregistrements IMAGE (téléchargées, validées, avec métadonnées)

    Pourquoi retourner un tuple (article, images) ?
    → Un article est lié à plusieurs images (relation 1:N du schéma).
      On les traite ensemble pour garantir la cohérence article_id ↔ image.article_id

    Args:
        raw_article:       Dict brut lu depuis data.json (respecte ArticleSchema)
        images_output_dir: Dossier où télécharger les images (data/processed/images/)

    Returns:
        (article_row, images_rows) ou (None, []) si l'article doit être ignoré.
    """
    article_id = raw_article.get("id", "")

    # --- Validation 1 : article multimodal ? (texte ET image) ---
    # Un article sans image ou sans texte ne sert pas à notre cas d'usage IA.
    if not validate_article_is_multimodal(raw_article):
        logger.warning(f"[Pipeline] Article ignoré (non multimodal) : {article_id}")
        return None, []

    # --- Validation 2 : label normalisable ? ---
    # On ne veut pas garder un article avec un label "inconnu" qu'on ne peut pas classer.
    if not validate_article_has_label(raw_article):
        logger.warning(f"[Pipeline] Article ignoré (label non mappable) : {article_id}")
        return None, []

    # --- Traitement du texte ---
    # extract_text_from_blocks() nettoie et sépare les paragraphes depuis content_blocks.
    # On concatène ensuite tous les paragraphes nettoyés en un seul string "content".
    text_blocks = extract_text_from_blocks(raw_article.get("content_blocks", []), article_id)
    content = " ".join(block["cleaned_text"] for block in text_blocks)

    # --- Traitement des images ---
    # extract_images_from_blocks() gère : main_image + images dans content_blocks.
    # Pour chaque URL : téléchargement → validation → métadonnées (width, height, size).
    images_rows = extract_images_from_blocks(
        content_blocks=raw_article.get("content_blocks", []),
        main_image=raw_article.get("main_image"),
        article_id=article_id,
        output_dir=images_output_dir,
    )

    # Si aucune image n'a pu être téléchargée (toutes en erreur 404, etc.)
    # → l'article n'est plus multimodal en pratique, on l'ignore.
    if not images_rows:
        logger.warning(f"[Pipeline] Article ignoré (0 image téléchargée) : {article_id}")
        return None, []

    # --- Construction de l'enregistrement ARTICLE ---
    # On mappe les champs du ArticleSchema vers le schéma conceptuel final.
    article_row = {
        "id":               article_id,
        "source":           raw_article.get("source", ""),
        # clean_text_content() applique le même pipeline que pour les paragraphes :
        # réparation encodage, suppression HTML, suppression URLs, multi-espaces
        "title":            clean_text_content(raw_article.get("title", "")),

        "content":          content,
        # normalize_label() convertit "Faux", "false", "incorrect"... → "faux" / "vrai" / "partiellement_faux"
        "label":            normalize_article_label(raw_article.get("label", "")),
        # normalize_date() convertit n'importe quel format ISO en format uniforme
        "publication_date": normalize_date(raw_article.get("date")),
        "author":           raw_article.get("author"),
        "url":              raw_article.get("url", ""),
        # created_at = timestamp de notre traitement (pas de la publication)
        "created_at":       datetime.now(timezone.utc).isoformat(),
    }

    # On ajoute un id unique à chaque image (les images n'en ont pas dans le schéma brut)
    for img in images_rows:
        img["id"] = str(uuid.uuid4())

    return article_row, images_rows


# ---------------------------------------------------------------------------
# 3. TRAITEMENT D'UNE SOURCE
# ---------------------------------------------------------------------------

def process_source(source_dir: Path, output_dir: Path) -> tuple[list, list, dict]:
    """Traite une source complète (ex: data/raw/afp/).

    C'est une boucle sur tous les articles d'une source.
    Elle délègue chaque transformation à _transform_one_article().

    Pourquoi ne pas tout mettre dans process_all_sources() ?
    → Pour pouvoir traiter et tester une source seule, sans dépendre des autres.

    Args:
        source_dir: Chemin vers le dossier source (ex: data/raw/afp/)
        output_dir: Dossier de sortie (data/processed/)

    Returns:
        (articles, images, stats) pour cette source.
    """
    source_name = source_dir.name
    images_dir = output_dir / "images"

    articles = []
    images = []
    stats = {"total": 0, "valid": 0, "skipped": 0, "errors": 0}

    raw_articles = read_raw_json(source_dir)
    stats["total"] = len(raw_articles)

    for raw_article in raw_articles:
        article_id = raw_article.get("id", "?")
        try:
            article_row, images_rows = _transform_one_article(raw_article, images_dir)

            if article_row is None:
                # Article ignoré volontairement (non multimodal, label inconnu, etc.)
                stats["skipped"] += 1
                continue

            articles.append(article_row)
            images.extend(images_rows)   # extend = ajouter les éléments de la liste (pas la liste elle-même)
            stats["valid"] += 1

        except Exception as e:
            # Sécurité : si _transform_one_article() lève une exception inattendue,
            # on log et on continue. On ne veut pas qu'un seul article casse tout.
            logger.error(f"[{source_name}] Erreur inattendue sur article {article_id} : {e}")
            stats["errors"] += 1

    logger.info(
        f"[{source_name}] Terminé — "
        f"valid={stats['valid']}, skipped={stats['skipped']}, errors={stats['errors']}"
    )
    return articles, images, stats


# ---------------------------------------------------------------------------
# 4. TRAITEMENT DE TOUTES LES SOURCES
# ---------------------------------------------------------------------------

def process_all_sources(raw_dir: Path, output_dir: Path) -> tuple[list, list, dict]:
    """Scanne data/raw/ et traite chaque sous-dossier comme une source.

    Pourquoi scanner les dossiers plutôt qu'une liste codée en dur ?
    → Si tu ajoutes une nouvelle source (ex: twitter/), le pipeline la détecte
      automatiquement sans modifier le code.

    Args:
        raw_dir:    Dossier racine des données brutes (data/raw/)
        output_dir: Dossier de sortie (data/processed/)

    Returns:
        (all_articles, all_images, summary) agrégés de toutes les sources.
    """
    all_articles = []
    all_images = []
    summary = {
        "total_articles": 0,
        "valid_multimodal": 0,
        "total_images": 0,
        "skipped": 0,
        "errors": 0,
        "by_source": {},   # Détail par source pour le rapport
    }

    # Itération sur chaque sous-dossier de data/raw/
    # .iterdir() liste le contenu du dossier ; .is_dir() filtre les fichiers
    source_dirs = [d for d in raw_dir.iterdir() if d.is_dir()]

    if not source_dirs:
        logger.warning(f"[Pipeline] Aucune source trouvée dans {raw_dir}")
        return all_articles, all_images, summary

    logger.info(f"[Pipeline] {len(source_dirs)} source(s) détectée(s) : {[d.name for d in source_dirs]}")

    for source_dir in source_dirs:
        articles, images, stats = process_source(source_dir, output_dir)

        # Accumulation des résultats
        all_articles.extend(articles)
        all_images.extend(images)

        # Mise à jour du résumé global
        summary["total_articles"]  += stats["total"]
        summary["valid_multimodal"] += stats["valid"]
        summary["total_images"]    += len(images)
        summary["skipped"]         += stats["skipped"]
        summary["errors"]          += stats["errors"]
        summary["by_source"][source_dir.name] = stats

    logger.info(
        f"[Pipeline] Toutes sources traitées — "
        f"articles valides={summary['valid_multimodal']}, "
        f"images={summary['total_images']}, "
        f"ignorés={summary['skipped']}, erreurs={summary['errors']}"
    )
    return all_articles, all_images, summary


# ---------------------------------------------------------------------------
# 5. SAUVEGARDE
# ---------------------------------------------------------------------------

def save_to_parquet(articles: list, images: list, output_dir: Path) -> None:
    """Convertit les listes Python en DataFrames pandas et les sauvegarde en Parquet.

    Pourquoi Parquet et pas CSV ?
    → Parquet conserve les types (int, datetime...), est compressé (~5x plus léger)
      et est directement lisible par pandas, Spark, DuckDB, etc.

    Args:
        articles:   Liste de dicts (un dict = une ligne de la table ARTICLE)
        images:     Liste de dicts (un dict = une ligne de la table IMAGE)
        output_dir: Dossier de sortie (data/processed/)
    """
    output_dir.mkdir(parents=True, exist_ok=True)

    articles_path = output_dir / "articles.parquet"
    images_path   = output_dir / "images.parquet"

    if articles:
        df_articles = pd.DataFrame(articles)
        df_articles.to_parquet(articles_path, index=False)
        logger.info(f"[Pipeline] {len(df_articles)} articles sauvegardés → {articles_path}")
    else:
        logger.warning("[Pipeline] Aucun article à sauvegarder.")

    if images:
        df_images = pd.DataFrame(images)
        df_images.to_parquet(images_path, index=False)
        logger.info(f"[Pipeline] {len(df_images)} images sauvegardées → {images_path}")
    else:
        logger.warning("[Pipeline] Aucune image à sauvegarder.")


# ---------------------------------------------------------------------------
# 6. POINT D'ENTRÉE
# ---------------------------------------------------------------------------

def main() -> None:
    """Point d'entrée du pipeline de transformation.

    Appelle les fonctions dans l'ordre :
        1. process_all_sources()  → traitement complet
        2. save_to_parquet()      → sauvegarde

    Pourquoi séparer main() du reste ?
    → Permet d'importer les fonctions sans déclencher le pipeline
      (important pour les tests unitaires et Airflow à l'étape 4).
    """
    logger.info("=" * 60)
    logger.info("[Pipeline] Démarrage du pipeline de transformation")
    logger.info("=" * 60)

    articles, images, summary = process_all_sources(RAW_DATA_DIR, PROCESSED_DATA_DIR)

    save_to_parquet(articles, images, PROCESSED_DATA_DIR)

    # Rapport final de synthèse
    logger.info("[Pipeline] Résumé de l'exécution :")
    logger.info(f"  Articles totaux lus    : {summary['total_articles']}")
    logger.info(f"  Articles valides       : {summary['valid_multimodal']}")
    logger.info(f"  Articles ignorés       : {summary['skipped']}")
    logger.info(f"  Erreurs                : {summary['errors']}")
    logger.info(f"  Images téléchargées    : {summary['total_images']}")
    logger.info("  Détail par source :")
    for source, stats in summary["by_source"].items():
        logger.info(f"    {source}: {stats}")
    logger.info("[Pipeline] Terminé.")


# Ce bloc garantit que main() n'est appelé QUE si on lance ce fichier directement.
# Si un autre fichier fait "from pipeline import process_source", main() ne s'exécute pas.
if __name__ == "__main__":
    main()