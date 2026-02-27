"""Scraper for L'Economiste (leconomiste.com) business news site."""
import logging
import re
from typing import Any, Dict, List, Optional

from supabase import Client

from scrapers.base_scraper import BaseScraper
from scrapers.scraper_utils import clean_article_text, parse_french_date

logger = logging.getLogger(__name__)


class LeconomisteScraper(BaseScraper):
    """Scraper for https://www.leconomiste.com articles."""

    BASE_URL = "https://www.leconomiste.com"

    def __init__(self, supabase_client: Client, rate_limit: float = 3.0):
        super().__init__("L'Economiste", supabase_client, rate_limit=rate_limit)
        self.categories = ["/economie", "/entreprises"]

    def get_article_urls(self) -> List[str]:
        urls = []
        max_pages = 3

        for category in self.categories:
            for page in range(0, max_pages):
                try:
                    # L'Economiste uses ?page= query param (0-based)
                    if page == 0:
                        cat_url = f"{self.BASE_URL}{category}"
                    else:
                        cat_url = f"{self.BASE_URL}{category}?page={page}"
                    logger.debug(f"Fetching: {cat_url}")

                    response = self.session.get(cat_url)
                    if not response:
                        logger.warning(f"Failed to fetch {cat_url}")
                        continue

                    from bs4 import BeautifulSoup
                    soup = BeautifulSoup(response.text, "html.parser")

                    found_on_page = 0

                    # L'Economiste uses article tags or card-like divs
                    articles = soup.find_all("article")
                    if articles:
                        for article_elem in articles:
                            link = article_elem.find("a", href=True)
                            if link:
                                href = link["href"]
                                if not href.startswith("http"):
                                    href = self.BASE_URL + href
                                if href not in urls and "leconomiste.com" in href:
                                    urls.append(href)
                                    found_on_page += 1

                    # Fallback: look for teaser/node links (Drupal-based site)
                    if found_on_page == 0:
                        for link in soup.find_all("a", href=True):
                            href = link["href"]
                            if not href.startswith("http"):
                                href = self.BASE_URL + href
                            # L'Economiste article URLs contain /article/ or have long slugs
                            if (
                                "/article/" in href
                                or re.search(r'/\d{4}/\d{2}/', href)
                                or (len(href.split("/")[-1]) > 20 and category.lstrip("/") in href)
                            ) and href not in urls and "leconomiste.com" in href:
                                urls.append(href)
                                found_on_page += 1

                    # Another fallback: h2/h3 title links
                    if found_on_page == 0:
                        for heading in soup.find_all(["h2", "h3"]):
                            link = heading.find("a", href=True)
                            if link:
                                href = link["href"]
                                if not href.startswith("http"):
                                    href = self.BASE_URL + href
                                if href not in urls and "leconomiste.com" in href:
                                    urls.append(href)
                                    found_on_page += 1

                    logger.info(f"Found {found_on_page} URLs on {cat_url}")

                    if found_on_page == 0:
                        logger.warning(f"No articles found on {cat_url}")

                except Exception as e:
                    logger.error(f"Error fetching {category} page {page}: {str(e)}")
                    continue

        logger.info(f"Found {len(urls)} article URLs from L'Economiste")
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
                # Try meta og:title
                og_title = soup.find("meta", property="og:title")
                if og_title and og_title.get("content"):
                    title = og_title["content"]

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
                # Look for meta article:published_time
                meta_date = soup.find("meta", property="article:published_time")
                if meta_date and meta_date.get("content"):
                    published_date = parse_french_date(meta_date["content"])

            if not published_date:
                date_pattern = re.compile(
                    r'\d{1,2}\s+(?:janvier|février|mars|avril|mai|juin|juillet|août|septembre|octobre|novembre|décembre)\s+\d{4}',
                    re.IGNORECASE,
                )
                text = soup.get_text()
                match = date_pattern.search(text)
                if match:
                    published_date = parse_french_date(match.group())

            # Extract article text
            article_text = None

            # Try article body first
            if article:
                for tag in article.find_all(["header", "footer", "nav", "aside", "script", "style", "figure"]):
                    tag.decompose()
                paragraphs = article.find_all("p")
                if paragraphs:
                    article_text = " ".join(
                        p.get_text(strip=True) for p in paragraphs if len(p.get_text(strip=True)) > 20
                    )

            # Fallback content selectors
            if not article_text or len(article_text) < 100:
                for selector in [
                    "div.field--name-body",
                    "div.entry-content",
                    "div.article-content",
                    "div.post-content",
                    "div.node__content",
                    "main",
                ]:
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
