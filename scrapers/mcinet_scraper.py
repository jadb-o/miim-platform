"""Scraper for Ministry of Industry press releases."""
import logging
from typing import Any, Dict, List, Optional

from supabase import Client

from scrapers.base_scraper import BaseScraper
from scrapers.scraper_utils import clean_article_text, parse_french_date

logger = logging.getLogger(__name__)


class McinetScraper(BaseScraper):
    """Scraper for https://www.mcinet.gov.ma/fr press releases."""

    BASE_URL = "https://www.mcinet.gov.ma"

    def __init__(self, supabase_client: Client, rate_limit: float = 3.0):
        super().__init__("MCINET", supabase_client, rate_limit=rate_limit)

    def get_article_urls(self) -> List[str]:
        urls = []
        max_pages = 3

        for page in range(0, max_pages):
            try:
                # MCINET uses 0-based pagination
                press_url = f"{self.BASE_URL}/fr/actualites" if page == 0 else f"{self.BASE_URL}/fr/actualites?page={page}"
                logger.debug(f"Fetching: {press_url}")

                response = self.session.get(press_url)
                if not response:
                    logger.warning(f"Failed to fetch {press_url}")
                    continue

                from bs4 import BeautifulSoup
                soup = BeautifulSoup(response.text, "html.parser")

                # Find all links to actualites articles
                found_on_page = 0
                for link in soup.find_all("a", href=True):
                    href = link["href"]
                    if "/fr/actualites/" in href and href != "/fr/actualites" and href not in urls:
                        if not href.startswith("http"):
                            href = self.BASE_URL + href
                        # Skip pagination and anchor links
                        if "?page=" not in href and "#" not in href:
                            urls.append(href)
                            found_on_page += 1

                logger.info(f"Found {found_on_page} URLs on page {page}")

                if found_on_page == 0:
                    logger.warning(f"No press releases found on page {page}")

            except Exception as e:
                logger.error(f"Error fetching press releases page {page}: {str(e)}")
                continue

        logger.info(f"Found {len(urls)} press release URLs from MCINET")
        return urls[:50]

    def scrape_article(self, url: str) -> Optional[Dict[str, Any]]:
        try:
            response = self.session.get(url)
            if not response:
                logger.warning(f"Failed to fetch press release: {url}")
                return None

            from bs4 import BeautifulSoup
            soup = BeautifulSoup(response.text, "html.parser")

            # Extract title - try multiple approaches for Drupal sites
            title = None
            # Try standard h1
            h1 = soup.find("h1")
            if h1:
                title = h1.get_text(strip=True)

            # If no h1, try page-title class or title tag
            if not title:
                for selector in ["h1.page-title", ".page-title", "title"]:
                    elem = soup.select_one(selector)
                    if elem:
                        title = elem.get_text(strip=True)
                        if title:
                            break

            # Last resort: extract from URL slug
            if not title:
                slug = url.rstrip("/").split("/")[-1]
                title = slug.replace("-", " ").capitalize()
                logger.info(f"Extracted title from URL slug: {title}")

            if not title:
                logger.warning(f"Could not extract title from {url}")
                return None

            # Extract publication date
            published_date = None
            # Try time element first
            time_elem = soup.find("time")
            if time_elem:
                date_str = time_elem.get("datetime", "") or time_elem.get_text(strip=True)
                published_date = parse_french_date(date_str)

            if not published_date:
                # Look for date patterns in text
                import re
                date_pattern = re.compile(r'\d{1,2}\s+(?:janvier|février|mars|avril|mai|juin|juillet|août|septembre|octobre|novembre|décembre)\s+\d{4}', re.IGNORECASE)
                text = soup.get_text()
                match = date_pattern.search(text)
                if match:
                    published_date = parse_french_date(match.group())

            # Extract article text - try multiple content selectors
            article_text = None
            content_selectors = [
                "div.field--name-body",
                "div.node__content",
                "article",
                "main .region--content",
                "div.content",
                "main",
            ]
            for selector in content_selectors:
                elem = soup.select_one(selector)
                if elem:
                    article_text = clean_article_text(str(elem))
                    if article_text and len(article_text) > 100:
                        break

            # Fallback: get all paragraph text from the page
            if not article_text or len(article_text) < 100:
                paragraphs = soup.find_all("p")
                if paragraphs:
                    combined = " ".join(p.get_text(strip=True) for p in paragraphs if len(p.get_text(strip=True)) > 20)
                    if len(combined) > 100:
                        article_text = combined

            if not article_text or len(article_text) < 50:
                logger.warning(f"Could not extract press release text from {url}")
                return None

            logger.info(f"Successfully scraped: {title[:60]}...")

            return {
                "title": title[:500],
                "published_date": published_date.isoformat() if published_date else None,
                "article_text": article_text,
                "language": "fr",
            }

        except Exception as e:
            logger.error(f"Error scraping press release {url}: {str(e)}")
            return None
