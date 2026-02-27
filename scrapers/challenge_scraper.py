"""Scraper for Challenge.ma business news site."""
import logging
from typing import Any, Dict, List, Optional

from supabase import Client

from scrapers.base_scraper import BaseScraper
from scrapers.scraper_utils import clean_article_text, parse_french_date

logger = logging.getLogger(__name__)


class ChallengeScraper(BaseScraper):
    """Scraper for https://www.challenge.ma articles (replaces Medias24 which blocks cloud IPs)."""

    BASE_URL = "https://www.challenge.ma"

    def __init__(self, supabase_client: Client, rate_limit: float = 2.0):
        super().__init__("Challenge", supabase_client, rate_limit=rate_limit)
        self.categories = ["/category/economie", "/category/entreprises"]

    def get_article_urls(self) -> List[str]:
        urls = []
        max_pages = 3

        for category in self.categories:
            for page in range(1, max_pages + 1):
                try:
                    if page == 1:
                        cat_url = f"{self.BASE_URL}{category}/"
                    else:
                        cat_url = f"{self.BASE_URL}{category}/page/{page}/"
                    logger.debug(f"Fetching: {cat_url}")

                    response = self.session.get(cat_url)
                    if not response:
                        logger.warning(f"Failed to fetch {cat_url}")
                        continue

                    from bs4 import BeautifulSoup
                    soup = BeautifulSoup(response.text, "html.parser")

                    found_on_page = 0

                    # Challenge.ma uses article tags
                    articles = soup.find_all("article")
                    if articles:
                        for article_elem in articles:
                            link = article_elem.find("a", href=True)
                            if link:
                                href = link["href"]
                                if not href.startswith("http"):
                                    href = self.BASE_URL + href
                                if href not in urls and "challenge.ma" in href:
                                    urls.append(href)
                                    found_on_page += 1

                    # Fallback: look for links with article-like URL patterns
                    if found_on_page == 0:
                        for link in soup.find_all("a", href=True):
                            href = link["href"]
                            if not href.startswith("http"):
                                href = self.BASE_URL + href
                            # Challenge.ma articles end with -NNNNNN/
                            import re
                            if re.search(r'-\d{4,}/?$', href) and href not in urls:
                                urls.append(href)
                                found_on_page += 1

                    logger.info(f"Found {found_on_page} URLs on {cat_url}")

                    if found_on_page == 0:
                        logger.warning(f"No articles found on {cat_url}")

                except Exception as e:
                    logger.error(f"Error fetching {category} page {page}: {str(e)}")
                    continue

        logger.info(f"Found {len(urls)} article URLs from Challenge")
        return urls[:50]

    def scrape_article(self, url: str) -> Optional[Dict[str, Any]]:
        try:
            response = self.session.get(url)
            if not response:
                logger.warning(f"Failed to fetch article: {url}")
                return None

            from bs4 import BeautifulSoup
            soup = BeautifulSoup(response.text, "html.parser")

            # Extract title
            title = None
            article = soup.find("article")
            if article:
                h1 = article.find("h1")
                if h1:
                    title = h1.get_text(strip=True)

            if not title:
                h1 = soup.find("h1")
                if h1:
                    title = h1.get_text(strip=True)

            if not title:
                logger.warning(f"Could not extract title from {url}")
                return None

            # Extract publication date
            published_date = None
            time_elem = soup.find("time")
            if time_elem:
                date_str = time_elem.get("datetime", "") or time_elem.get_text(strip=True)
                published_date = parse_french_date(date_str)

            if not published_date:
                import re
                date_pattern = re.compile(
                    r'\d{1,2}\s+(?:janvier|février|mars|avril|mai|juin|juillet|août|septembre|octobre|novembre|décembre)\s+\d{4}',
                    re.IGNORECASE
                )
                text = soup.get_text()
                match = date_pattern.search(text)
                if match:
                    published_date = parse_french_date(match.group())

            # Extract article text
            article_text = None
            if article:
                # Remove non-content elements
                for tag in article.find_all(["header", "footer", "nav", "aside", "script", "style", "figure"]):
                    tag.decompose()
                paragraphs = article.find_all("p")
                if paragraphs:
                    article_text = " ".join(
                        p.get_text(strip=True) for p in paragraphs
                        if len(p.get_text(strip=True)) > 20
                    )

            # Fallback
            if not article_text or len(article_text) < 100:
                for selector in ["div.entry-content", "div.post-content", "div.article-body", "main"]:
                    elem = soup.select_one(selector)
                    if elem:
                        article_text = clean_article_text(str(elem))
                        if article_text and len(article_text) > 100:
                            break

            if not article_text or len(article_text) < 50:
                logger.warning(f"Could not extract article text from {url}")
                return None

            logger.info(f"Successfully scraped: {title[:60]}...")

            return {
                "title": title[:500],
                "published_date": published_date.isoformat() if published_date else None,
                "article_text": article_text,
                "language": "fr",
            }

        except Exception as e:
            logger.error(f"Error scraping article {url}: {str(e)}")
            return None
