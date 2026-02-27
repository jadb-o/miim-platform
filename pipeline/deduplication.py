"""Deduplication and matching utilities."""
import logging
from typing import Optional

from supabase import Client

from scrapers.scraper_utils import content_hash

logger = logging.getLogger(__name__)


def is_article_duplicate(supabase_client: Client, source_url: str, article_text: str) -> bool:
    """
    Check if article is a duplicate.

    Args:
        supabase_client: Supabase client instance
        source_url: Article source URL
        article_text: Article text content

    Returns:
        True if duplicate exists
    """
    try:
        text_hash = content_hash(article_text)

        # Check by source_url (UNIQUE constraint)
        response = supabase_client.table("articles").select("id").eq("source_url", source_url).limit(1).execute()

        if response.data and len(response.data) > 0:
            return True

        # Check by hash (same content from different source)
        response = supabase_client.table("articles").select("id").eq("raw_content_hash", text_hash).limit(1).execute()

        if response.data and len(response.data) > 0:
            return True

        return False

    except Exception as e:
        logger.error(f"Error checking for article duplicates: {str(e)}")
        return False


def find_matching_company(
    supabase_client: Client, company_name: str, city: Optional[str] = None
) -> Optional[str]:
    """
    Find matching company in database using fuzzy matching.

    Args:
        supabase_client: Supabase client instance
        company_name: Company name to search for
        city: Optional city to narrow search

    Returns:
        company_id if match found, None otherwise
    """
    if not company_name or not company_name.strip():
        return None

    try:
        company_name_lower = company_name.lower().strip()

        # Build query with ILIKE for case-insensitive matching
        query = supabase_client.table("companies").select("company_id, company_name")

        # Apply ILIKE filter on company_name
        query = query.ilike("company_name", f"%{company_name_lower}%")

        response = query.execute()

        if not response.data:
            logger.debug(f"No exact match found for company: {company_name}")
            return None

        # If multiple matches, prefer exact case match
        for company in response.data:
            if company["company_name"].lower() == company_name_lower:
                logger.debug(f"Found exact match for: {company_name}")
                return company["company_id"]

        # Return first match if no exact case match
        if response.data:
            logger.debug(f"Found fuzzy match for: {company_name}")
            return response.data[0]["company_id"]

        return None

    except Exception as e:
        logger.error(f"Error finding matching company for '{company_name}': {str(e)}")
        return None
