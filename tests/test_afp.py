from src.extraction.scrapers.afp_scraper import AfpScraper

scraper = AfpScraper()

data = scraper.extract()

print(f"premier article  : {data}")

scraper.close()