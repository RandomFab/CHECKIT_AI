from pathlib import Path
import time
import urllib.parse
import requests
from PIL import Image
from config.logger import logger

# Mapping MIME type → extension de fichier
_MIME_TO_EXT: dict[str, str] = {
    "image/jpeg":  ".jpg",
    "image/png":   ".png",
    "image/gif":   ".gif",
    "image/webp":  ".webp",
    "image/bmp":   ".bmp",
    "image/tiff":  ".tiff",
    "image/svg+xml": ".svg",
}
_KNOWN_IMG_EXTS = set(_MIME_TO_EXT.values()) | {".jpeg"}


def _origin(url: str) -> str:
    p = urllib.parse.urlparse(url)
    return f"{p.scheme}://{p.netloc}"


# ---------------------------------------------------------------------------
# Session HTTP avec warm-up Selenium par domaine
# ---------------------------------------------------------------------------
# Certains sites (AFP, ...) utilisent un WAF (Akamai) qui bloque les requêtes
# directes python-requests, même avec de vrais headers browser.
# Solution : on ouvre une vraie session Chrome sur la page d'accueil du domaine,
# on récupère les cookies de session, et on les injecte dans requests.
# La session est mise en cache par domaine pour ne faire le warm-up qu'une fois.

_session_cache: dict[str, requests.Session] = {}


def _get_session(url: str) -> requests.Session:
    """Retourne une requests.Session prête à télécharger des images du domaine de `url`.

    - Premier appel pour un domaine : ouvre Chrome, visite la page d'accueil,
      transfère les cookies dans une requests.Session, met en cache.
    - Appels suivants : retourne la session mise en cache directement.
    """
    domain = _origin(url)

    if domain in _session_cache:
        return _session_cache[domain]

    logger.info(f"[ImageHandler] Warm-up Selenium pour le domaine : {domain}")
    session = requests.Session()

    try:
        from selenium import webdriver
        from selenium.webdriver.chrome.options import Options

        options = Options()
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--disable-blink-features=AutomationControlled")
        options.add_experimental_option("excludeSwitches", ["enable-automation"])
        options.add_experimental_option("useAutomationExtension", False)

        driver = webdriver.Chrome(options=options)
        driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {
            "source": "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
        })

        driver.get(domain + "/")
        time.sleep(2)  # laisse Akamai/JS fixer les cookies

        ua = driver.execute_script("return navigator.userAgent")
        session.headers.update({
            "User-Agent": ua,
            "Referer": domain + "/",
            "Accept": "image/avif,image/webp,image/apng,image/*,*/*;q=0.8",
            "Accept-Language": "fr-FR,fr;q=0.9,en;q=0.8",
        })
        for c in driver.get_cookies():
            session.cookies.set(c["name"], c["value"],
                                domain=c.get("domain", "").lstrip("."))

        logger.info(f"[ImageHandler] Cookies obtenus : {list(session.cookies.keys())}")
        driver.quit()

    except Exception as e:
        logger.warning(f"[ImageHandler] Warm-up Selenium échoué pour {domain} : {e} — fallback headers simples")
        session.headers.update({
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
            ),
            "Referer": domain + "/",
            "Accept": "image/avif,image/webp,image/apng,image/*,*/*;q=0.8",
        })

    _session_cache[domain] = session
    return session



def _resolve_extension(url: str, response: requests.Response) -> str:
    """Détermine l'extension d'image depuis l'URL ou le header Content-Type.

    Ordre de priorité :
    1. Extension présente dans le chemin de l'URL (ex: .jpg, .png)
    2. Header Content-Type de la réponse HTTP
    3. Fallback : .jpg
    """
    # 1. Extension dans l'URL (avant les query params)
    url_path = urllib.parse.urlparse(url).path
    ext = Path(url_path).suffix.lower()
    if ext in _KNOWN_IMG_EXTS:
        return ext

    # 2. Content-Type
    content_type = response.headers.get("Content-Type", "").split(";")[0].strip()
    if content_type in _MIME_TO_EXT:
        return _MIME_TO_EXT[content_type]

    # 3. Fallback
    logger.warning(f"Extension introuvable pour {url} (Content-Type={content_type!r}), fallback .jpg")
    return ".jpg"


def download_image(image_url: str, output_dir: Path, filename: str) -> str | None:
    """Télécharge image et retourne le chemin local.

    - Ajoute automatiquement l'extension si `filename` n'en a pas.
    - Suit les redirections et log l'URL finale en cas d'erreur.
    Ex: "http://..." → "data/processed/images/img_abc123.jpg"
    Gère les erreurs (timeout, format invalide, 404)
    """
    output_dir.mkdir(parents=True, exist_ok=True)

    try:
        session = _get_session(image_url)
        response = session.get(image_url, timeout=10, allow_redirects=True)

        # Log l'URL finale si une redirection a eu lieu
        final_url = response.url
        if final_url != image_url:
            logger.debug(f"Redirection : {image_url} → {final_url}")

        response.raise_for_status()

        # Ajout de l'extension si absente
        if not Path(filename).suffix:
            ext = _resolve_extension(final_url, response)
            filename = filename + ext

        file_path = output_dir / filename
        with open(file_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)

        return str(file_path)
    except requests.HTTPError as e:
        final_url = e.response.url if e.response is not None else image_url
        logger.error(
            f"Erreur lors du téléchargement de {image_url} : {e}"
            + (f" (URL finale après redirection : {final_url})" if final_url != image_url else "")
        )
        return ""
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
