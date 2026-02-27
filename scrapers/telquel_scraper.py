"""Scraper for TelQuel (telquel.ma) news site."""
import logging
import re
from typing import Any, Dict, List, Optional

from supabase import Client

from scrapers.base_scraper import BaseScraper
from scrapers.scraper_utils import clean_article_text, parse_french_date

logger = logging.getLogger(__name__)


class TelquelScraper(BaseScraper):
    """Scraper for https://telquel.ma articles."""

    BASE_URL = "https://telquel.ma"

    def __init__(self, supabase_client: Client, rate_limit: float = 3.0):
        super().__init__("TelQuel", supabase_client, rate_limit=rate_limit)
        self.categories = ["/economie", "/entreprises"]

    def get_article_urls(self) -> List[str]:
        urls = []
        max_pages = 3

        for category in self.categories:
            for page in range(1, max_pages + 1):
                try:
                    if page == 1:
                        cat_url = f"{self.BASE_URL}{category}"
                    else:
                        cat_url = f"{self.BASE_URL}{category}/page/{page}"
                    logger.debug(f"Fetching: {cat_url}")

                    response = self.session.get(cat_url)
                    if not response:
                        logger.warning(f"Failed to fetch {cat_url}")
                        continue

                    from bs4 import BeautifulSoup
                    soup = BeautifulSoup(response.text, "html.parser")
                    found_on_page = 0

                    # Article tags
                    for article_elem in soup.find_all("article"):
                        link = article_elem.find("a", href=True)
                        if link:
                            href = link["href"]
                            if not href.startswith("http"):
                                href = self.BASE_URL + href
                            if href not in urls and "telquel.ma" in href:
                                urls.append(href)
                                found_on_page += 1

                    # Fallback: heading links
                    if found_on_page == 0:
                        for heading in soup.find_all(["h2", "h3"]):
                            link = heading.find("a", href=True)
                            if link:
                                href = link["href"]
                                if not href.startswith("http"):
                                    href = self.BASE_URL + href
                                if href not in urls and "telquel.ma" in href and "/category/" not in href:
                                    urls.append(href)
                                    found_on_page += 1

                    # Fallback: long slug links
                    if found_on_page == 0:
                        for link in soup.find_all("a", href=True):
                            href = link["href"]
                            if not href.startswith("http"):
                                href = self.BASE_URL + href
                            slug = href.rstrip("/").split("/")[-1]
                            if (
                                "telquel.ma" in href
                                and href not in urls
                                and len(slug) > 20
                                and "/category/" not in href
                                and "/tag/" not in href
                                and "/page/" not in href
                            ):
                                urls.append(href)
                                found_on_page += 1

                    logger.info(f"Found {found_on_page} URLs on {cat_url}")
                except Exception as e:
                    logger.error(f"Error fetching {category} page {page}: {str(e)}")

        logger.info(f"Found {len(urls)} article URLs from TelQuel")
        return urls[:50]

    def scrape_article(self, url: str) -> Optional[Dict[str, Any]]:
        try:
            response = self.session.get(url)
            if not response:
                return None

            from bs4 import BeautifulSoup
            soup = BeautifulSoup(response.text, "html.parser")

            # Extract title
            title = None
            for sel in ["h1.entry-title", "h1.article-title", "h1.post-title", "h1"]:
                elem = soup.select_one(sel)
                if elem:
                    title = elem.get_text(strip=True)
                    break
            if not title:
                og = soup.find("meta", property="og:title")
                if og and og.get("content"):
                    title = og["content"]
            if not title:
                return None

            # Extract date
            published_date = None
            time_elem = soup.find("time")
            if time_elem:
                published_date = parse_french_date(time_elem.get("datetime", "") or time_elem.get_text(strip=True))
            if not published_date:
                meta_date = soup.find("meta", property="article:published_time")
                if meta_date and meta_date.get("content"):
                    published_date = parse_french_date(meta_date["content"])
            if not published_date:
                match = re.search(
                    r'\d{1,2}\s+(?:janvier|février|mars|avril|mai|juin|juillet|août|septembre|octobre|novembre|décembre)\s+\d{4}',
                    soup.get_text(), re.IGNORECASE,
                )
                if match:
                    published_date = parse_french_date(match.group())

            # Extract article text
            article_text = None
            article = soup.find("article")
            if article:
                for tag in article.find_all(["header", "footer", "nav", "aside", "script", "style", "figure"]):
                    tag.decompose()
                paragraphs = article.find_all("p")
                if paragraphs:
                    article_text = " ".join(p.get_text(strip=True) for p in paragraphs if len(p.get_text(strip=True)) > 20)

            if not article_text or len(article_text) < 100:
                for sel in ["div.entry-content", "div.post-content", "div.article-body", "div.single-content", "main"]:
                    elem = soup.select_one(sel)
                    if elem:
                        article_text = clean_article_text(str(elem))
                        if article_text and len(article_text) > 100:
                            break

            if not article_text or len(article_text) < 50:
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
