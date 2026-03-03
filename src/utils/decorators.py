from config.logger import logger

import difflib
from functools import wraps


def log_text_modification(func):
    @wraps(func)
    def wrapper(text, *args, **kwargs):
        if not isinstance(text, str):
            return func(text, *args, **kwargs)

        initial_len = len(text)
        transformed_text = func(text, *args, **kwargs)
        final_len = len(transformed_text)

        if text != transformed_text:
            # Comparaison par caractères pour voir les espaces/lettres modifiés
            diff = difflib.ndiff(text, transformed_text)
            # On ne garde que ce qui a été supprimé (-) ou ajouté (+)
            changes = "".join(
                [d for d in diff if d.startswith("-") or d.startswith("+")]
            )

            logger.info(
                f"[{func.__name__}] Modification : {initial_len} -> {final_len} cars."
            )
            logger.info(f"  Détail des modifs : {changes}")

        return transformed_text

    return wrapper
