from src.extraction.scrapers.afp_scraper import AfpScrapper

scraper = AfpScrapper()

data = scraper.extract()

print(f"premier article  : {data}")

scraper.close()