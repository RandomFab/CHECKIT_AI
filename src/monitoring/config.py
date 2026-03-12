import os

# Seuils d'alerte pour chaque KPI
KPI_THRESHOLDS = {
    'min_articles_per_day': 50,
    'min_articles_with_images': 0.80,  # 80%
    'max_duplicates_pct': 0.15,        # 15%
    'max_data_age_hours': 48,
}

# Sources monitoring
SOURCES = ['afp', 'france24', 'google_factcheck']

# DB Connection
DB_CONFIG = {
    'host': os.getenv('DB_HOST'),
    'user': 'reader',
    'password': os.getenv('DB_READER_PASSWORD'),
    'database': 'checkit'
}