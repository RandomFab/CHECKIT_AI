import json
from src.extraction.scrapers.google_factcheck_client import GoogleFactCheckScrapper

scraper = GoogleFactCheckScrapper(
    queries=[
                # Politique & société
        "politique",
        # "élection",
        # "gouvernement",
        
        # # Santé
        # "vaccin",
        # "covid",
        # "cancer",
        # "médicament",
        
        # # Sciences & environnement
        # "climatique",
        # "5G",
        # "OGM",
        
        # # Conflits & géopolitique
        # "ukraine",
        # "guerre",
        # "terrorisme",
        
        # # Complotisme classique
        # "complot",
       
        # # Immigration & société
        # "immigration",
        # "islam",
        "crime",],
        lang="fr")

data = scraper.extract()

print(json.dumps(data, indent=2, ensure_ascii=False))
