from src.extraction.scrapers.afp_scraper import AfpScrapper

scraper = AfpScrapper()

data = scraper.extract()

print(f"premier article  : {data[0]}")
print(f"Nombre d'article : {len(data)}")

scraper.close()