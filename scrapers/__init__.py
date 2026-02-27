"""MIIM Scrapers Module."""
from scrapers.base_scraper import BaseScraper
from scrapers.challenge_scraper import ChallengeScraper
from scrapers.fnh_scraper import FnhScraper
from scrapers.leconomiste_scraper import LeconomisteScraper
from scrapers.leseco_scraper import LesecoScraper
from scrapers.mapbusiness_scraper import MapBusinessScraper
from scrapers.mcinet_scraper import McinetScraper

__all__ = [
    "BaseScraper",
    "ChallengeScraper",
    "FnhScraper",
    "LeconomisteScraper",
    "LesecoScraper",
    "MapBusinessScraper",
    "McinetScraper",
]
