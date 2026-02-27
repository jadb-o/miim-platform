"""Scraper for MAP Business (mapbusiness.ma) — Morocco's official press agency business wire."""
import logging
import re
from typing import Any, Dict, List, Optional

from supabase import Client

from scrapers.base_scraper import BaseScraper
from scrapers.scraper_utils import clean_article_text, parse_french_date

logger = logging.getLogger(__name__)


class MapBusinessScraper(BaseScraper):
    """Scraper for https://www.mapbusiness.ma articles."""

    BASE_URL = "https://www.mapbusiness.ma"

    def __init__(self, supabase_client: Client, rate_limit: float = 3.0):
        super().__init__("MAP Business", supabase_client, rate_limit=rate_limit)
        self.sections = ["/economie", "/entreprises", "/industrie"]

    def get_article_urls(self) -> List[str]:
        urls = []
        max_pages = 3

        for section in self.sections:
            for page in range(1, max_pages + 1):
                try:
                    if page == 1:
                        section_url = f"{self.BASE_URL}{section}"
                    else:
                        section_url = f"{self.BASE_URL}{section}?page={page}"
                    logger.debug(f"Fetching: {section_url}")

                    response = self.session.get(section_url)
                    if not response:
                        logger.warning(f"Failed to fetch {section_url}")
                        continue

                    from bs4 import BeautifulSoup
                    soup = BeautifulSoup(response.text, "html.parser")

                    found_on_page = 0

                    # MAP Business uses article tags
                    articles = soup.find_all("article")
                    if articles:
                        for article_elem in articles:
                            link = article_elem.find("a", href=True)
                            if link:
                                href = link["href"]
                                if not href.startswith("http"):
                                    href = self.BASE_URL + href
                                if href not in urls and "mapbusiness.ma" in href:
                                    urls.append(href)
                                    found_on_page += 1

                    # Fallback: h2/h3 heading links
                    if found_on_page == 0:
                        for heading in soup.find_all(["h2", "h3", "h4"]):
                            link = heading.find("a", href=True)
                            if link:
                                href = link["href"]
                                if not href.startswith("http"):
                                    href = self.BASE_URL + href
                                if href not in urls and "mapbusiness.ma" in href:
                                    urls.append(href)
                                    found_on_page += 1

                    # Another fallback: links with article-like patterns
                    if found_on_page == 0:
                        for link in soup.find_all("a", href=True):
                            href = link["href"]
                            if not href.startswith("http"):
                                href = self.BASE_URL + href
                            # MAP articles usually have a numeric ID or long slug
                            if (
                                "mapbusiness.ma" in href
                                and href not in urls
                                and (re.search(r'/\d+[-/]', href) or len(href.split("/")[-1]) > 20)
                                and "/page/" not in href
                                and "#" not in href
                            ):
                                urls.append(href)
                                found_on_page += 1

                    logger.info(f"Found {found_on_page} URLs on {section_url}")

                    if found_on_page == 0:
                        logger.warning(f"No articles found on {section_url}")

                except Exception as e:
                    logger.error(f"Error fetching {section} page {page}: {str(e)}")
                    continue

        logger.info(f"Found {len(urls)} article URLs from MAP Business")
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
                og_title = soup.find("meta", property="og:title")
                if og_title and og_title.get("content"):
                    title = og_title["content"]

            if not title:
                # Extract from URL slug as last resort
                slug = url.rstrip("/").split("/")[-1]
                if len(slug) > 10:
                    title = slug.replace("-", " ").capitalize()
                    logger.info(f"Extracted title from URL slug: {title}")

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

            # Try article body
            if article:
                for tag in article.find_all(["header", "footer", "nav", "aside", "script", "style", "figure"]):
                    tag.decompose()
                paragraphs = article.find_all("p")
                if paragraphs:
                    article_text = " ".join(
                        p.get_text(strip=True) for p in paragraphs if len(p.get_text(strip=True)) > 20
                    )

            # Fallback selectors
            if not article_text or len(article_text) < 100:
                for selector in [
                    "div.article-body",
                    "div.entry-content",
                    "div.post-content",
                    "div.field--name-body",
                    "div.content-article",
                    "div.node__content",
                    "main",
                ]:
                    elem = soup.select_one(selector)
                    if elem:
                        article_text = clean_article_text(str(elem))
                        if article_text and len(article_text) > 100:
                            break

            # Last resort: all paragraph text
            if not article_text or len(article_text) < 100:
                paragraphs = soup.find_all("p")
                if paragraphs:
                    combined = " ".join(
                        p.get_text(strip=True) for p in paragraphs if len(p.get_text(strip=True)) > 20
                    )
                    if len(combined) > 100:
                        article_text = combined

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
