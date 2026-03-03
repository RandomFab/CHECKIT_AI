from dateutil import parser
from datetime import datetime
import pandas as pd
from config.logger import logger


def normalize_date(date_str: str, output_format: str = "%d/%m/%y") -> str:

    if pd.isna(date_str) or not isinstance(date_str, str) or date_str.strip() == "":
        return ""

    try:
        # 1. Analyse automatique de la date
        # dayfirst=True est crucial pour le format français
        dt = parser.parse(date_str, dayfirst=True)

        # 2. Export au format souhaité
        return dt.strftime(output_format)
    except (ValueError, OverflowError) as e:
        # Si le format est inconnu (ex: "date inconnue")
        logger.warning(
            f"Format de date invalide ou inconnu : '{date_str}' - Erreur: {e}"
        )
        return ""
