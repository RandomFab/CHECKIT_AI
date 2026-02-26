from src.extraction.scrapers.france24_scraper import France24Scraper

scraper = France24Scraper()

data = scraper.extract()

print(f"premier article  : {data}")

scraper.close()