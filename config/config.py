import os
from pathlib import Path

# --- Chemin base projet ---
BASE_DIR = Path(__file__).resolve().parent.parent

# --- Chemins data ---
EXTERNAL_DATA_DIR = BASE_DIR / 'data' / 'external'
RAW_DATA_DIR = BASE_DIR / 'data' / 'raw'
PROCESSED_DATA_DIR = BASE_DIR / 'data' / 'processed'

# --- Déclaration couleurs ---
VIOLET_CLAIR = '#99abf7'
VIOLET_FONCE = '#7451eb'
GRIS_CLAIR = '#3f3f3f'
JAUNE_FONCE = '#f9ab2d'
JAUNE_CLAIR = '#f7e096ff'

PALETTE = [VIOLET_CLAIR, VIOLET_FONCE, GRIS_CLAIR, JAUNE_FONCE, JAUNE_CLAIR,'#212E53','#4A919E','#BED3C3','#EBACA2','#226D68','#CE6A6B','#08C5D1','#D46F4D', '#430C05','#18534F','#696969']

# --- Déclaration tracking URI MLflow ---
MLFLOW_TRACKING_URI='http://127.0.0.1:5000'
