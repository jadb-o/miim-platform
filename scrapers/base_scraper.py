"""Base scraper class with common functionality."""
import logging
import time
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any, Dict, List, Optional

from supabase import Client

from scrapers.scraper_utils import PoliteSession, content_hash, is_industry_relevant

logger = logging.getLogger(__name__)


class BaseScraper(ABC):
    """Abstract base class for all web scrapers."""

    def __init__(self, source_name: str, supabase_client: Client, rate_limit: float = 2.0):
        """
        Initialize BaseScraper.

        Args:
            source_name: Name of the news source
            supabase_client: Supabase client instance
            rate_limit: Seconds between requests
        """
        self.source_name = source_name
        self.supabase = supabase_client
        self.session = PoliteSession(rate_limit=rate_limit)
        logger.info(f"Initialized scraper for {source_name}")

    @abstractmethod
    def get_article_urls(self) -> List[str]:
        """
        Get list of article URLs to scrape.

        Returns:
            List of article URLs
        """
        pass

    @abstractmethod
    def scrape_article(self, url: str) -> Optional[Dict[str, Any]]:
        """
        Scrape a single article.

        Args:
            url: Article URL

        Returns:
            Dictionary with keys: title, published_date, article_text, or None if scraping fails
        """
        pass

    def scrape(self) -> List[Dict[str, Any]]:
        """
        Scrape all articles from the source.

        Returns:
            List of article dictionaries
        """
        logger.info(f"Starting scrape for {self.source_name}")
        articles = []

        try:
            urls = self.get_article_urls()
            logger.info(f"Found {len(urls)} article URLs from {self.source_name}")

            for i, url in enumerate(urls, 1):
                logger.debug(f"Scraping article {i}/{len(urls)}: {url}")
                article = self.scrape_article(url)

                if article:
                    # Add metadata
                    article["source_name"] = self.source_name
                    article["source_url"] = url
                    article["scraped_date"] = datetime.utcnow().isoformat()

                    # Check if industry relevant
                    if is_industry_relevant(article.get("article_text", "")):
                        articles.append(article)
                        logger.debug(f"Article accepted (industry relevant): {article.get('title', 'N/A')}")
                    else:
                        logger.debug(f"Article rejected (not industry relevant): {article.get('title', 'N/A')}")
                else:
                    logger.debug(f"Failed to scrape article: {url}")

        except Exception as e:
            logger.error(f"Error during scraping: {str(e)}", exc_info=True)

        logger.info(f"Scraping complete for {self.source_name}. Collected {len(articles)} articles.")
        return articles

    def save_articles(self, articles: List[Dict[str, Any]]) -> int:
        """
        Save articles to database, avoiding duplicates.

        Args:
            articles: List of article dictionaries

        Returns:
            Number of new articles inserted
        """
        new_count = 0

        for article in articles:
            try:
                source_url = article.get("source_url")
                article_text = article.get("article_text", "")

                # Check for duplicates by URL and hash
                if self._is_duplicate(source_url, article_text):
                    logger.debug(f"Article already exists, skipping: {source_url}")
                    continue

                # Prepare data for insertion
                insert_data = {
                    "source_name": article.get("source_name"),
                    "source_url": source_url,
                    "title": article.get("title", "")[:500],
                    "published_date": article.get("published_date"),
                    "scraped_date": article.get("scraped_date"),
                    "article_text": article_text,
                    "language": article.get("language", "fr"),
                    "processing_status": "pending",
                    "raw_content_hash": content_hash(article_text),
                }

                # Insert into database
                response = self.supabase.table("articles").insert(insert_data).execute()

                if response.data:
                    new_count += 1
                    logger.debug(f"Inserted article: {source_url}")
                else:
                    logger.warning(f"Failed to insert article: {source_url}")

            except Exception as e:
                logger.error(f"Error saving article {article.get('source_url', 'N/A')}: {str(e)}", exc_info=True)

        logger.info(f"Saved {new_count} new articles for {self.source_name}")
        return new_count

    def _is_duplicate(self, source_url: str, article_text: str) -> bool:
        """
        Check if article is a duplicate.

        Args:
            source_url: Article URL
            article_text: Article text

        Returns:
            True if duplicate exists
        """
        try:
            text_hash = content_hash(article_text)

            # Check by source_url (UNIQUE constraint)
            response = self.supabase.table("articles").select("id").eq("source_url", source_url).limit(1).execute()

            if response.data and len(response.data) > 0:
                return True

            # Check by hash (same content from different source)
            response = self.supabase.table("articles").select("id").eq("raw_content_hash", text_hash).limit(1).execute()

            if response.data and len(response.data) > 0:
                return True

            return False

        except Exception as e:
            logger.error(f"Error checking for duplicates: {str(e)}")
            return False

    def log_run(
        self,
        found: int,
        new: int,
        dupes: int,
        duration: float,
        status: str = "success",
        error: Optional[str] = None,
    ) -> None:
        """
        Log scraper run to database.

        Args:
            found: Total articles found
            new: New articles inserted
            dupes: Duplicate articles skipped
            duration: Execution time in seconds
            status: Run status (success/error)
            error: Error message if applicable
        """
        try:
            log_data = {
                "source_name": self.source_name,
                "run_date": datetime.utcnow().isoformat(),
                "articles_found": found,
                "articles_new": new,
                "articles_duplicate": dupes,
                "processing_time_seconds": round(duration, 2),
                "status": status,
                "error_message": error,
            }

            self.supabase.table("scraper_runs").insert(log_data).execute()
            logger.info(f"Logged scraper run for {self.source_name}")

        except Exception as e:
            logger.error(f"Error logging scraper run: {str(e)}")

    def run(self) -> Dict[str, Any]:
        """
        Execute full scraping pipeline.

        Returns:
            Dictionary with run statistics
        """
        start_time = time.time()
        status = "success"
        error_msg = None

        try:
            # Scrape articles
            articles = self.scrape()
            found = len(articles)

            # Save to database
            new = self.save_articles(articles)
            dupes = found - new

            # Log run
            duration = time.time() - start_time
            self.log_run(found, new, dupes, duration, status=status)

            logger.info(
                f"Scraper run complete for {self.source_name}: "
                f"found={found}, new={new}, dupes={dupes}, duration={duration:.1f}s"
            )

            return {
                "source": self.source_name,
                "found": found,
                "new": new,
                "duplicates": dupes,
                "duration": round(duration, 2),
                "status": status,
            }

        except Exception as e:
            duration = time.time() - start_time
            error_msg = str(e)
            status = "error"
            self.log_run(0, 0, 0, duration, status=status, error=error_msg)
            logger.error(f"Scraper run failed for {self.source_name}: {error_msg}")

            return {
                "source": self.source_name,
                "found": 0,
                "new": 0,
                "duplicates": 0,
                "duration": round(duration, 2),
                "status": status,
                "error": error_msg,
            }

        finally:
            self.session.close()
