"""MIIM Scrapers Module."""
from scrapers.base_scraper import BaseScraper
from scrapers.challenge_scraper import ChallengeScraper
from scrapers.fnh_scraper import FnhScraper
from scrapers.hespress_scraper import HespressScraper
from scrapers.lavieeco_scraper import LavieEcoScraper
from scrapers.leconomiste_scraper import LeconomisteScraper
from scrapers.leseco_scraper import LesecoScraper
from scrapers.mapbusiness_scraper import MapBusinessScraper
from scrapers.mcinet_scraper import McinetScraper
from scrapers.medias24_scraper import Medias24Scraper
from scrapers.telquel_scraper import TelquelScraper

__all__ = [
    "BaseScraper",
    "ChallengeScraper",
    "FnhScraper",
    "HespressScraper",
    "LavieEcoScraper",
    "LeconomisteScraper",
    "LesecoScraper",
    "MapBusinessScraper",
    "McinetScraper",
    "Medias24Scraper",
    "TelquelScraper",
]
