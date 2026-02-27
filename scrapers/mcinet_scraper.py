"""Scraper for Ministry of Industry press releases."""
import logging
from typing import Any, Dict, List, Optional

from supabase import Client

from scrapers.base_scraper import BaseScraper
from scrapers.scraper_utils import clean_article_text, parse_french_date

logger = logging.getLogger(__name__)


class McinetScraper(BaseScraper):
    """Scraper for https://www.mcinet.gov.ma/fr press releases."""

    BASE_URL = "https://www.mcinet.gov.ma/fr"

    def __init__(self, supabase_client: Client, rate_limit: float = 3.0):
        """
        Initialize McinetScraper.

        Args:
            supabase_client: Supabase client instance
            rate_limit: Seconds between requests (higher for government site)
        """
        super().__init__("MCINET", supabase_client, rate_limit=rate_limit)

    def get_article_urls(self) -> List[str]:
        """
        Get list of press release URLs from MCINET.

        Returns:
            List of press release URLs
        """
        urls = []
        max_pages = 3

        for page in range(1, max_pages + 1):
            try:
                # Build URL for press releases
                press_url = f"{self.BASE_URL}/actualites?page={page}"
                logger.debug(f"Fetching: {press_url}")

                response = self.session.get(press_url)
                if not response:
                    logger.warning(f"Failed to fetch {press_url}")
                    continue

                from bs4 import BeautifulSoup
                soup = BeautifulSoup(response.text, "html.parser")

                # Try multiple selectors for press release links
                selectors = [
                    "article h2 a",
                    "div.view-content a",
                    "div.views-row a",
                    "a[href*='/actualites/']",
                    "a[href*='/fr/content/']",
                    "h3 a",
                ]

                found_on_page = 0
                for selector in selectors:
                    links = soup.select(selector)
                    if links:
                        for link in links:
                            href = link.get("href")
                            if href:
                                # Ensure absolute URL
                                if not href.startswith("http"):
                                    href = self.BASE_URL.split("/fr")[0] + href
                                if href not in urls:
                                    urls.append(href)
                                    found_on_page += 1
                        if found_on_page > 0:
                            break

                if found_on_page == 0:
                    logger.warning(f"No press releases found on page {page} with any selector")

            except Exception as e:
                logger.error(f"Error fetching press releases page {page}: {str(e)}")
                continue

        logger.info(f"Found {len(urls)} press release URLs from MCINET")
        return urls[:50]  # Limit to 50 URLs

    def scrape_article(self, url: str) -> Optional[Dict[str, Any]]:
        """
        Scrape a single press release from MCINET.

        Args:
            url: Press release URL

        Returns:
            Dictionary with press release data or None if scraping fails
        """
        try:
            response = self.session.get(url)
            if not response:
                logger.warning(f"Failed to fetch press release: {url}")
                return None

            from bs4 import BeautifulSoup
            soup = BeautifulSoup(response.text, "html.parser")

            # Extract title
            title = None
            for selector in ["h1.press-title", "h1", "div.content h1"]:
                elem = soup.select_one(selector)
                if elem:
                    title = elem.get_text(strip=True)
                    break

            if not title:
                logger.warning(f"Could not extract title from {url}")
                return None

            # Extract publication date
            published_date = None
            for selector in ["time", "span.press-date", "div.publish-date", "span[datetime]", "span.date"]:
                elem = soup.select_one(selector)
                if elem:
                    date_str = elem.get_text(strip=True) or elem.get("datetime", "")
                    published_date = parse_french_date(date_str)
                    if published_date:
                        break

            # Extract press release text
            article_text = None
            for selector in ["div.press-content", "div.content", "article", "div.node-content", "div.field-body"]:
                elem = soup.select_one(selector)
                if elem:
                    article_text = clean_article_text(str(elem))
                    if article_text and len(article_text) > 100:
                        break

            if not article_text or len(article_text) < 100:
                logger.warning(f"Could not extract press release text from {url}")
                return None

            logger.debug(f"Successfully scraped: {title}")

            return {
                "title": title[:500],
                "published_date": published_date.isoformat() if published_date else None,
                "article_text": article_text,
                "language": "fr",
            }

        except Exception as e:
            logger.error(f"Error scraping press release {url}: {str(e)}")
            return None
