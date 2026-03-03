import html
import unicodedata
import re
from config.logger import logger
import requests

from src.utils.decorators import log_text_modification

# --- Title, Paragraphe content blocks ---
@log_text_modification
def _clean_bad_encoded_text(text: str) -> str:
    """Répare les problèmes d'encodage (ex: &eacute; ou Ã©) et normalise l'Unicode."""
    if not text:
        return ""
    # Décode les entités HTML (ex: &amp; -> &)
    text = html.unescape(text)
    # Répare les erreurs d'encodage UTF-8 mal interprété (courant dans les fichiers JSON)
    try:
        text = text.encode("latin-1").decode("utf-8")
    except (UnicodeEncodeError, UnicodeDecodeError):
        pass
    # Normalise les caractères (ex: accents combinés)
    return unicodedata.normalize("NFKC", text)


@log_text_modification
def _clean_multispaces(text: str) -> str:
    """Supprime les espaces doubles, tabulations et espaces de début/fin."""
    if not text:
        return ""
    # Remplace tous les types d'espaces (tab, saut de ligne, etc.) par un seul espace
    text = re.sub(r"\s+", " ", text)
    return text.strip()


@log_text_modification
def _remove_urls(text: str) -> str:
    """Supprime les liens HTTP/HTTPS."""
    return re.sub(r"https?://\S+|www\.\S+", "", text)


@log_text_modification
def _remove_html_tags(text: str) -> str:
    """Supprime les balises HTML résiduelles (ex: <p>, <div>)."""
    return re.sub(r"<.*?>", "", text)


def clean_text_content(content_text: str) -> str:
    """Pipeline principal de nettoyage."""
    if not isinstance(content_text, str):
        return ""

    text = _clean_bad_encoded_text(content_text)
    text = _remove_html_tags(text)
    text = _remove_urls(text)
    text = _clean_multispaces(text)

    return text

# --- URL --- 
def is_url_reachable(url: str) -> bool:
    """Vérifie si l'URL répond avec un code succès (200)."""
    try:

        response = requests.head(url, timeout=5, allow_redirects=True)
        return response.status_code == 200
    except requests.RequestException as e:
        logger.error(f"URL non joignable : {url} - Erreur: {e}")
        return False

# --- label ---


from thefuzz import fuzz


LABEL_REFERENCES = {
    "faux": [
        "faux", "false", "incorrect", "infondé", "erroné",
        "trompeur", "fausse information", "sans fondement",
        "inexact", "c'est faux", "unproven"
    ],
    "partiellement_faux": [
        "plutôt faux", "partiellement faux", "partiellement correct",
        "manque de contexte", "contexte manquant", "à nuancer",
        "a nuancer", "en partie", "c'est plus compliqué",
        "discutable", "trompeur et infondé", "très exagéré"
    ],
    "vrai": [
        "vrai", "true", "correct", "vérifié",
        "plutôt vrai", "en partie vrai", "partiellement correct",
        "authentique", "exact"
    ]
}

def normalize_label(label: str, threshold: int = 70) -> str:
    """
    Normalise un label brut en 3 catégories : 'faux', 'partiellement_faux', 'vrai'.
    
    Args:
        label: Le label brut à normaliser (ex: "Plutôt Faux", "C'est faux", etc.)
        threshold: Score minimum (0-100) pour accepter une correspondance. 
                   En dessous, le label est considéré comme 'inconnu'.
    """
    if not isinstance(label, str) or not label.strip():
        return "inconnu"
    
    # Étape 1 : Nettoyage basique pour uniformiser la comparaison
    label_clean = label.strip().lower()
    
    best_category = "inconnu"
    best_score = 0
    
    # Étape 2 : Pour chaque catégorie et ses mots-clés de référence...
    for category, references in LABEL_REFERENCES.items():
        for ref in references:
   
            score = fuzz.partial_ratio(label_clean, ref)
            
            # On garde le meilleur score observé toutes catégories confondues
            if score > best_score:
                best_score = score
                best_category = category
    
    # Étape 3 : Si le meilleur score est trop bas, on ne se risque pas à mapper
    if best_score < threshold:
        logger.warning(f"Label non mappé (score={best_score}) : '{label}'")
        return "inconnu"
    
    return best_category