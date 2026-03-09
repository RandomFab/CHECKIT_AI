#!/usr/bin/env python
"""Script d'initialisation de la BD avec variables d'env."""
import os
import sys
from pathlib import Path
from dotenv import load_dotenv
import subprocess

# Charger les variables d'environnement
load_dotenv()

# Créer la BD si elle n'existe pas
print("Vérification de la BD 'checkit'...")
result = subprocess.run(
    'psql -U postgres -c "CREATE DATABASE checkit;" 2>&1',
    shell=True,
    capture_output=True,
    text=True
)
if "already exists" in result.stderr or result.returncode == 0:
    print("✓ BD 'checkit' prête")
else:
    print(f"⚠ Attention : {result.stderr}")

# Vérifier que les passwords existent
admin_pwd = os.getenv("DB_ADMIN_PASSWORD")
writer_pwd = os.getenv("DB_WRITER_PASSWORD")
reader_pwd = os.getenv("DB_READER_PASSWORD")

if not all([admin_pwd, writer_pwd, reader_pwd]):
    print("❌ Erreur : DB_ADMIN_PASSWORD, DB_WRITER_PASSWORD et DB_READER_PASSWORD doivent être définis dans .env")
    sys.exit(1)

# Lire le template
template_path = Path("sql/init_roles.sql.example")
if not template_path.exists():
    print(f"❌ Fichier non trouvé : {template_path}")
    sys.exit(1)

with open(template_path, "r", encoding="utf-8") as f:
    sql = f.read()

# Remplacer les placeholders par les variables d'env
sql = sql.replace("CHANGE_ME_admin", admin_pwd)
sql = sql.replace("CHANGE_ME_writer", writer_pwd)
sql = sql.replace("CHANGE_ME_reader", reader_pwd)

# Écrire le vrai fichier (non committé)
output_path = Path("sql/init_roles.sql")
with open(output_path, "w", encoding="utf-8") as f:
    f.write(sql)

print(f"✓ {output_path} généré avec succès")

# Exécuter le script SQL
print("Exécution du script SQL...")
result = subprocess.run(
    f'psql -U postgres -d checkit -f {output_path}',
    shell=True,
    capture_output=True,
    text=True
)

if result.returncode == 0:
    print("✓ Rôles créés avec succès!")
else:
    print(f"❌ Erreur : {result.stderr}")
    sys.exit(1)
