"""MIIM Scrapers Module."""
from scrapers.base_scraper import BaseScraper
from scrapers.medias24_scraper import Medias24Scraper
from scrapers.leseco_scraper import LesecoScraper
from scrapers.mcinet_scraper import McinetScraper

__all__ = [
    "BaseScraper",
    "Medias24Scraper",
    "LesecoScraper",
    "McinetScraper",
]
