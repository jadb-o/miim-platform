"""Scraper for Leseco news site."""
import logging
from typing import Any, Dict, List, Optional

from supabase import Client

from scrapers.base_scraper import BaseScraper
from scrapers.scraper_utils import clean_article_text, parse_french_date

logger = logging.getLogger(__name__)


class LesecoScraper(BaseScraper):
    """Scraper for https://leseco.ma articles."""

    BASE_URL = "https://leseco.ma"

    def __init__(self, supabase_client: Client, rate_limit: float = 2.0):
        """
        Initialize LesecoScraper.

        Args:
            supabase_client: Supabase client instance
            rate_limit: Seconds between requests
        """
        super().__init__("Leseco", supabase_client, rate_limit=rate_limit)
        self.categories = ["/business", "/maroc"]

    def get_article_urls(self) -> List[str]:
        """
        Get list of article URLs from Leseco.

        Returns:
            List of article URLs
        """
        urls = []
        max_pages = 3

        for category in self.categories:
            for page in range(1, max_pages + 1):
                try:
                    # Build URL for category
                    category_url = f"{self.BASE_URL}{category}?page={page}"
                    logger.debug(f"Fetching: {category_url}")

                    response = self.session.get(category_url)
                    if not response:
                        logger.warning(f"Failed to fetch {category_url}")
                        continue

                    from bs4 import BeautifulSoup
                    soup = BeautifulSoup(response.text, "html.parser")

                    # Try multiple selectors for article links
                    selectors = [
                        "div.article-item a.article-link",
                        "article a",
                        "h3.article-title a",
                        "div.post-item a",
                        "a[href*='/article/']",
                        "a[href*='/news/']",
                    ]

                    found_on_page = 0
                    for selector in selectors:
                        links = soup.select(selector)
                        if links:
                            for link in links:
                                href = link.get("href")
                                if href and href not in urls:
                                    # Ensure absolute URL
                                    if not href.startswith("http"):
                                        href = self.BASE_URL + href
                                    if href not in urls:
                                        urls.append(href)
                                        found_on_page += 1
                            if found_on_page > 0:
                                break

                    if found_on_page == 0:
                        logger.warning(f"No articles found on {category_url} with any selector")

                except Exception as e:
                    logger.error(f"Error fetching {category} page {page}: {str(e)}")
                    continue

        logger.info(f"Found {len(urls)} article URLs from Leseco")
        return urls[:50]  # Limit to 50 URLs

    def scrape_article(self, url: str) -> Optional[Dict[str, Any]]:
        """
        Scrape a single article from Leseco.

        Args:
            url: Article URL

        Returns:
            Dictionary with article data or None if scraping fails
        """
        try:
            response = self.session.get(url)
            if not response:
                logger.warning(f"Failed to fetch article: {url}")
                return None

            from bs4 import BeautifulSoup
            soup = BeautifulSoup(response.text, "html.parser")

            # Extract title
            title = None
            for selector in ["h1.article-title", "h1", "div.entry-header h1"]:
                elem = soup.select_one(selector)
                if elem:
                    title = elem.get_text(strip=True)
                    break

            if not title:
                logger.warning(f"Could not extract title from {url}")
                return None

            # Extract publication date
            published_date = None
            for selector in ["time", "span.article-date", "div.post-date", "span.publish-date", "span[datetime]"]:
                elem = soup.select_one(selector)
                if elem:
                    date_str = elem.get_text(strip=True) or elem.get("datetime", "")
                    published_date = parse_french_date(date_str)
                    if published_date:
                        break

            # Extract article text
            article_text = None
            for selector in ["div.article-content", "div.post-content", "article", "div.entry-content", "div.content"]:
                elem = soup.select_one(selector)
                if elem:
                    article_text = clean_article_text(str(elem))
                    if article_text and len(article_text) > 100:
                        break

            if not article_text or len(article_text) < 100:
                logger.warning(f"Could not extract article text from {url}")
                return None

            logger.debug(f"Successfully scraped: {title}")

            return {
                "title": title[:500],
                "published_date": published_date.isoformat() if published_date else None,
                "article_text": article_text,
                "language": "fr",
            }

        except Exception as e:
            logger.error(f"Error scraping article {url}: {str(e)}")
            return None
