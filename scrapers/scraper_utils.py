"""Utility functions for web scrapers."""
import hashlib
import logging
import random
import time
from datetime import datetime
from typing import Optional

import requests
from bs4 import BeautifulSoup

from config.settings import INDUSTRY_KEYWORDS, SCRAPER_CONFIG

logger = logging.getLogger(__name__)


class PoliteSession:
    """HTTP session with rate limiting, user-agent rotation, and retry logic."""

    def __init__(self, rate_limit: float = 2.0, max_retries: int = 3):
        """
        Initialize PoliteSession.

        Args:
            rate_limit: Seconds to wait between requests
            max_retries: Maximum number of retries for failed requests
        """
        self.rate_limit = rate_limit
        self.max_retries = max_retries
        self.backoff_factor = SCRAPER_CONFIG.get("backoff_factor", 1.5)
        self.timeout = SCRAPER_CONFIG.get("timeout", 15)
        self.last_request_time = 0
        self.session = requests.Session()

    def get(self, url: str, **kwargs) -> Optional[requests.Response]:
        """
        Fetch URL with rate limiting, user-agent rotation, and retries.

        Args:
            url: URL to fetch
            **kwargs: Additional arguments for requests.get

        Returns:
            Response object or None if all retries failed
        """
        # Apply rate limiting
        elapsed = time.time() - self.last_request_time
        if elapsed < self.rate_limit:
            time.sleep(self.rate_limit - elapsed)

        # Add random delay (2-5 seconds)
        delay = random.uniform(2, 5)
        time.sleep(delay)

        # Set random user-agent
        headers = kwargs.get("headers", {})
        headers["User-Agent"] = random.choice(SCRAPER_CONFIG["user_agents"])
        kwargs["headers"] = headers

        # Set timeout
        kwargs["timeout"] = kwargs.get("timeout", self.timeout)

        # Retry logic with exponential backoff
        for attempt in range(self.max_retries):
            try:
                self.last_request_time = time.time()
                response = self.session.get(url, **kwargs)
                response.raise_for_status()
                return response
            except requests.RequestException as e:
                if attempt < self.max_retries - 1:
                    wait_time = self.backoff_factor ** attempt
                    logger.warning(
                        f"Request failed for {url} (attempt {attempt + 1}/{self.max_retries}). "
                        f"Retrying in {wait_time}s. Error: {str(e)}"
                    )
                    time.sleep(wait_time)
                else:
                    logger.error(f"Failed to fetch {url} after {self.max_retries} retries. Error: {str(e)}")
                    return None

    def close(self):
        """Close the session."""
        self.session.close()


def content_hash(text: str) -> str:
    """
    Generate SHA256 hash of content.

    Args:
        text: Text content to hash

    Returns:
        Hexadecimal hash string
    """
    return hashlib.sha256(text.encode()).hexdigest()


def parse_french_date(date_str: str) -> Optional[datetime]:
    """
    Parse French date strings in various formats.

    Handles formats like:
    - "27 février 2026"
    - "27/02/2026"
    - "2026-02-27"

    Args:
        date_str: Date string in French or standard format

    Returns:
        datetime object or None if parsing fails
    """
    if not date_str:
        return None

    date_str = date_str.strip()

    # French month mapping
    french_months = {
        "janvier": 1, "février": 2, "mars": 3, "avril": 4,
        "mai": 5, "juin": 6, "juillet": 7, "août": 8,
        "septembre": 9, "octobre": 10, "novembre": 11, "décembre": 12,
    }

    # Try ISO format first (2026-02-27)
    try:
        return datetime.fromisoformat(date_str)
    except (ValueError, TypeError):
        pass

    # Try DD/MM/YYYY format
    try:
        return datetime.strptime(date_str, "%d/%m/%Y")
    except (ValueError, TypeError):
        pass

    # Try French format (27 février 2026)
    try:
        parts = date_str.lower().split()
        if len(parts) >= 3:
            day = int(parts[0])
            month_str = parts[1]
            year = int(parts[2])
            month = french_months.get(month_str)
            if month:
                return datetime(year, month, day)
    except (ValueError, TypeError, IndexError):
        pass

    # Try other common formats
    formats = [
        "%d-%m-%Y",
        "%d.%m.%Y",
        "%Y-%m-%d",
        "%d %B %Y",
        "%B %d, %Y",
    ]
    for fmt in formats:
        try:
            return datetime.strptime(date_str, fmt)
        except (ValueError, TypeError):
            continue

    logger.warning(f"Could not parse date: {date_str}")
    return None


def clean_article_text(html: str) -> str:
    """
    Extract and clean plain text from HTML.

    Args:
        html: HTML content

    Returns:
        Clean plain text
    """
    soup = BeautifulSoup(html, "html.parser")

    # Remove script and style elements
    for script in soup(["script", "style"]):
        script.decompose()

    # Get text
    text = soup.get_text()

    # Clean up whitespace
    lines = (line.strip() for line in text.splitlines())
    chunks = (phrase.strip() for line in lines for phrase in line.split("  "))
    text = " ".join(chunk for chunk in chunks if chunk)

    return text


def is_industry_relevant(text: str) -> bool:
    """
    Check if text is relevant to Moroccan industrial sector.

    Args:
        text: Text to check

    Returns:
        True if text contains industry-relevant keywords
    """
    if not text:
        return False

    text_lower = text.lower()

    # Check for keyword matches
    keyword_count = sum(1 for keyword in INDUSTRY_KEYWORDS if keyword in text_lower)

    # Need at least 2 keyword matches to be considered relevant
    return keyword_count >= 2
