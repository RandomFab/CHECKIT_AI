"""
pipeline.py - Orchestration principale du pipeline de transformation.

Flux :
    data/raw/{source}/data.json
        → read_raw_json()
        → _transform_one_article()   [pour chaque article]
        → process_source()           [pour chaque source, appelé individuellement]
        → merge_sources()            [fusionne les résultats de chaque source]
        → save_to_parquet()          [sauvegarde en Parquet]
    data/processed/articles.parquet
    data/processed/images.parquet

    Dans Airflow, chaque étape correspond à une tâche distincte :
        process_source  (x N sources, en parallèle)
        merge_sources   (attend toutes les sources)
        save_to_parquet (puis chargement Postgres)
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
    """Lit le fichier data.json d'une source et retourne la liste d'articles bruts."""
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
    """Transforme 1 article brut en 2 entités : ARTICLE + liste d'IMAGES."""
    article_id = raw_article.get("id", "")

    if not validate_article_is_multimodal(raw_article):
        logger.warning(f"[Pipeline] ✗ Article ignoré (non multimodal) : {article_id}")
        return None, []

    if not validate_article_has_label(raw_article):
        logger.warning(f"[Pipeline] ✗ Article ignoré (label non mappable) : {article_id}")
        return None, []

    text_blocks = extract_text_from_blocks(raw_article.get("content_blocks", []), article_id)
    content = " ".join(block["cleaned_text"] for block in text_blocks)

    images_rows = extract_images_from_blocks(
        content_blocks=raw_article.get("content_blocks", []),
        main_image=raw_article.get("main_image"),
        article_id=article_id,
        output_dir=images_output_dir,
    )

    if not images_rows:
        logger.warning(f"[Pipeline] ✗ Article ignoré (0 image téléchargée) : {article_id}")
        return None, []

    article_row = {
        "id":               article_id,
        "source":           raw_article.get("source", ""),
        "title":            clean_text_content(raw_article.get("title", "")),
        "content":          content,
        "label":            normalize_article_label(raw_article.get("label", "")),
        "publication_date": normalize_date(raw_article.get("date")),
        "author":           raw_article.get("author"),
        "url":              raw_article.get("url", ""),
        "created_at":       datetime.now(timezone.utc).isoformat(),
    }

    for img in images_rows:
        img["id"] = str(uuid.uuid4())

    return article_row, images_rows


# ---------------------------------------------------------------------------
# 3. TRAITEMENT D'UNE SOURCE
# ---------------------------------------------------------------------------

def process_source(source_dir: Path, output_dir: Path) -> dict:
    """Traite une source complète et écrit un JSON intermédiaire sur disque.

    Pourquoi écrire sur disque plutôt que retourner les données ?
    → Dans Airflow, les résultats entre tâches passent via XCom.
      XCom est limité en taille (~48KB par défaut avec Postgres).
      Écrire sur disque évite de saturer XCom avec des milliers d'articles.
      On ne retourne que les stats (légères) via XCom.

    Returns:
        stats dict avec total/valid/skipped/errors + chemins des fichiers intermédiaires.
    """
    source_name = source_dir.name
    images_dir = output_dir / "images"
    images_dir.mkdir(parents=True, exist_ok=True)

    # Dossier temporaire pour les résultats intermédiaires par source
    tmp_dir = output_dir / "tmp"
    tmp_dir.mkdir(parents=True, exist_ok=True)

    articles = []
    images = []
    stats = {
        "source": source_name,
        "total": 0,
        "valid": 0,
        "skipped": 0,
        "errors": 0,
        "images_count": 0,
        # Chemins des fichiers intermédiaires pour la tâche merge
        "articles_tmp": str(tmp_dir / f"{source_name}_articles.json"),
        "images_tmp":   str(tmp_dir / f"{source_name}_images.json"),
    }

    raw_articles = read_raw_json(source_dir)
    stats["total"] = len(raw_articles)

    if stats["total"] == 0:
        logger.warning(f"[{source_name}] Aucun article à traiter.")
        _write_tmp([], stats["articles_tmp"])
        _write_tmp([], stats["images_tmp"])
        return stats

    logger.info(f"[{source_name}] Début du traitement — {stats['total']} articles")

    for raw_article in raw_articles:
        article_id = raw_article.get("id", "?")
        url = raw_article.get("url", article_id)
        logger.info(f"[{source_name}] Traitement : {url}")
        try:
            article_row, images_rows = _transform_one_article(raw_article, images_dir)

            if article_row is None:
                stats["skipped"] += 1
                logger.info(f"[{source_name}] ✗ Raté  : {url}")
                continue

            articles.append(article_row)
            images.extend(images_rows)
            stats["valid"] += 1
            logger.info(f"[{source_name}] ✓ Réussi : {url} ({len(images_rows)} image(s))")

        except Exception as e:
            logger.error(f"[{source_name}] ✗ Erreur inattendue sur {url} : {e}")
            stats["errors"] += 1

    stats["images_count"] = len(images)

    # Écriture sur disque — XCom ne reçoit que les stats
    _write_tmp(articles, stats["articles_tmp"])
    _write_tmp(images,   stats["images_tmp"])

    taux = round(100 * stats["valid"] / max(stats["total"], 1))
    taux_img = round(100 * stats["images_count"] / max(stats["valid"] * 1, 1))

    logger.info(
        f"[{source_name}] ✓ Terminé — "
        f"valid={stats['valid']}/{stats['total']} ({taux}%), "
        f"images={stats['images_count']} ({taux_img}% des articles valides), "
        f"skipped={stats['skipped']}, errors={stats['errors']}"
    )
    return stats


# ---------------------------------------------------------------------------
# 4. FUSION DES RÉSULTATS DE TOUTES LES SOURCES
# ---------------------------------------------------------------------------

def merge_sources(all_stats: list[dict]) -> dict:
    """Fusionne les fichiers intermédiaires de chaque source en une seule liste.

    Lit les fichiers JSON temporaires écrits par process_source()
    et les consolide. Ne retourne que le summary via XCom.

    Args:
        all_stats: Liste de stats retournées par chaque process_source().

    Returns:
        summary dict avec totaux globaux + chemins des fichiers fusionnés.
    """
    all_articles = []
    all_images = []
    summary = {
        "total_articles": 0,
        "valid_multimodal": 0,
        "total_images": 0,
        "skipped": 0,
        "errors": 0,
        "by_source": {},
    }

    for stats in all_stats:
        source_name = stats["source"]

        # Lecture des fichiers intermédiaires
        articles = _read_tmp(stats["articles_tmp"])
        images   = _read_tmp(stats["images_tmp"])

        all_articles.extend(articles)
        all_images.extend(images)

        summary["total_articles"]   += stats["total"]
        summary["valid_multimodal"] += stats["valid"]
        summary["total_images"]     += stats["images_count"]
        summary["skipped"]          += stats["skipped"]
        summary["errors"]           += stats["errors"]
        summary["by_source"][source_name] = {
            "total":        stats["total"],
            "valid":        stats["valid"],
            "skipped":      stats["skipped"],
            "errors":       stats["errors"],
            "images":       stats["images_count"],
            "taux_articles": f"{round(100 * stats['valid'] / max(stats['total'], 1))}%",
            "taux_images":  f"{round(100 * stats['images_count'] / max(stats['valid'], 1))}%",
        }

    logger.info(
        f"[Pipeline] ✓ Fusion terminée — "
        f"articles={summary['valid_multimodal']}, "
        f"images={summary['total_images']}, "
        f"ignorés={summary['skipped']}, erreurs={summary['errors']}"
    )

    # Écriture des fichiers fusionnés
    from config.config import PROCESSED_DATA_DIR
    tmp_dir = PROCESSED_DATA_DIR / "tmp"
    tmp_dir.mkdir(parents=True, exist_ok=True)

    merged_articles_path = str(tmp_dir / "merged_articles.json")
    merged_images_path   = str(tmp_dir / "merged_images.json")

    _write_tmp(all_articles, merged_articles_path)
    _write_tmp(all_images,   merged_images_path)

    summary["merged_articles_path"] = merged_articles_path
    summary["merged_images_path"]   = merged_images_path

    return summary


# ---------------------------------------------------------------------------
# 5. SAUVEGARDE
# ---------------------------------------------------------------------------

def save_to_parquet(summary: dict, output_dir: Path) -> dict:
    """Lit les fichiers fusionnés et les sauvegarde en Parquet.

    Args:
        summary:    Summary retourné par merge_sources() (contient les chemins).
        output_dir: Dossier de sortie (data/processed/)

    Returns:
        stats finales pour le rapport.
    """
    output_dir.mkdir(parents=True, exist_ok=True)

    articles = _read_tmp(summary["merged_articles_path"])
    images   = _read_tmp(summary["merged_images_path"])

    articles_path = output_dir / "articles.parquet"
    images_path   = output_dir / "images.parquet"

    if articles:
        df_articles = pd.DataFrame(articles)
        df_articles.to_parquet(articles_path, index=False)
        logger.info(f"[Pipeline] ✓ {len(df_articles)} articles sauvegardés → {articles_path}")
    else:
        logger.warning("[Pipeline] Aucun article à sauvegarder.")

    if images:
        df_images = pd.DataFrame(images)
        df_images.to_parquet(images_path, index=False)
        logger.info(f"[Pipeline] ✓ {len(df_images)} images sauvegardées → {images_path}")
    else:
        logger.warning("[Pipeline] Aucune image à sauvegarder.")

    return {
        "articles_count": len(articles),
        "images_count":   len(images),
    }


# ---------------------------------------------------------------------------
# HELPERS INTERNES
# ---------------------------------------------------------------------------

def _write_tmp(data: list, path: str) -> None:
    """Écrit une liste de dicts dans un fichier JSON temporaire."""
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, default=str)


def _read_tmp(path: str) -> list:
    """Lit un fichier JSON temporaire et retourne une liste de dicts."""
    p = Path(path)
    if not p.exists():
        logger.warning(f"[Pipeline] Fichier tmp introuvable : {path}")
        return []
    with open(p, encoding="utf-8") as f:
        return json.load(f)


# ---------------------------------------------------------------------------
# 6. POINT D'ENTRÉE LOCAL
# ---------------------------------------------------------------------------

def main() -> None:
    """Point d'entrée pour exécution locale (hors Airflow)."""
    logger.info("=" * 60)
    logger.info("[Pipeline] Démarrage du pipeline de transformation")
    logger.info("=" * 60)

    source_dirs = [d for d in RAW_DATA_DIR.iterdir() if d.is_dir()]
    if not source_dirs:
        logger.warning(f"[Pipeline] Aucune source trouvée dans {RAW_DATA_DIR}")
        return

    logger.info(f"[Pipeline] {len(source_dirs)} source(s) : {[d.name for d in source_dirs]}")

    all_stats = [process_source(source_dir, PROCESSED_DATA_DIR) for source_dir in source_dirs]
    summary   = merge_sources(all_stats)
    final     = save_to_parquet(summary, PROCESSED_DATA_DIR)

    logger.info("=" * 60)
    logger.info("[Pipeline] Résumé final :")
    logger.info(f"  Articles valides    : {summary['valid_multimodal']}/{summary['total_articles']}")
    logger.info(f"  Articles ignorés    : {summary['skipped']}")
    logger.info(f"  Erreurs             : {summary['errors']}")
    logger.info(f"  Images téléchargées : {summary['total_images']}")
    logger.info("  Détail par source :")
    for source, stats in summary["by_source"].items():
        logger.info(
            f"    [{source}] articles={stats['valid']}/{stats['total']} "
            f"({stats['taux_articles']}), images={stats['images']} ({stats['taux_images']})"
        )
    logger.info("=" * 60)


if __name__ == "__main__":
    main()