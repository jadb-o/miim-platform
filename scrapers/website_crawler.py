"""Company Website Crawler — auto-enriches companies by visiting their websites.

Targets companies where website_url IS NOT NULL and description IS NULL.
For each: fetches homepage + /about + /contact, extracts useful data.

Usage:
  python -m scrapers.website_crawler [--limit 50] [--dry-run] [--use-llm]
"""
import argparse
import logging
import os
import re
import time
from typing import Any, Dict, List, Optional
from urllib.parse import urljoin, urlparse

from supabase import Client, create_client

from config.settings import SUPABASE_ANON_KEY, SUPABASE_URL
from scrapers.scraper_utils import PoliteSession

logger = logging.getLogger(__name__)


class WebsiteCrawler:
    """Crawl company websites to extract descriptions, contact info, and team data."""

    ABOUT_PATHS = ["/about", "/about-us", "/a-propos", "/qui-sommes-nous", "/notre-entreprise", "/en/about"]
    CONTACT_PATHS = ["/contact", "/contact-us", "/contactez-nous", "/en/contact"]

    def __init__(self, supabase_client: Client, dry_run: bool = False, use_llm: bool = False):
        self.supabase = supabase_client
        self.dry_run = dry_run
        self.use_llm = use_llm
        self.session = PoliteSession(rate_limit=7.0)
        self.openai_api_key = os.environ.get("OPENAI_API_KEY")

    def _get_companies_to_enrich(self, limit: int = 50) -> List[Dict[str, Any]]:
        """Get companies with website_url but missing description."""
        try:
            response = (
                self.supabase.table("companies")
                .select("id, company_name, website_url, description, headquarters_city, phone, email")
                .not_.is_("website_url", "null")
                .is_("description", "null")
                .limit(limit)
                .execute()
            )
            return response.data or []
        except Exception as e:
            logger.error(f"Error fetching companies: {e}")
            return []

    def _fetch_page(self, url: str) -> Optional[str]:
        """Fetch a page and return its HTML content."""
        try:
            response = self.session.get(url)
            if response and response.status_code == 200:
                return response.text
            return None
        except Exception as e:
            logger.debug(f"Failed to fetch {url}: {e}")
            return None

    def _extract_meta(self, soup) -> Dict[str, str]:
        """Extract meta description and og:description."""
        data = {}

        meta_desc = soup.find("meta", attrs={"name": "description"})
        if meta_desc and meta_desc.get("content"):
            data["meta_description"] = meta_desc["content"].strip()

        og_desc = soup.find("meta", property="og:description")
        if og_desc and og_desc.get("content"):
            data["og_description"] = og_desc["content"].strip()

        return data

    def _extract_contact_info(self, soup) -> Dict[str, str]:
        """Extract email and phone from page content."""
        data = {}
        text = soup.get_text()

        # Email
        emails = re.findall(r'[\w.+-]+@[\w-]+\.[\w.-]+', text)
        # Filter out common non-company emails
        for email in emails:
            if not any(skip in email.lower() for skip in ["example.com", "test.com", "noreply", "wordpress"]):
                data["email"] = email
                break

        # Phone (Moroccan format: +212, 05, 06, 07, 08)
        phones = re.findall(r'(?:\+212|0)[\s.-]?[5-8][\s.-]?\d{2}[\s.-]?\d{2}[\s.-]?\d{2}[\s.-]?\d{2}', text)
        if phones:
            data["phone"] = phones[0].strip()

        return data

    def _extract_team_names(self, soup) -> List[str]:
        """Extract team/management names from about page."""
        names = []

        # Look for team sections
        for section in soup.find_all(["div", "section"], class_=re.compile(
            r'team|equipe|direction|management|leadership|fondateur|founder', re.IGNORECASE
        )):
            for heading in section.find_all(["h3", "h4", "h5", "strong"]):
                name = heading.get_text(strip=True)
                # Basic name validation: 2-4 words, no numbers, reasonable length
                if name and 5 < len(name) < 60 and not re.search(r'\d', name):
                    words = name.split()
                    if 2 <= len(words) <= 4:
                        names.append(name)

        return names[:10]

    def _extract_body_text(self, soup) -> Optional[str]:
        """Extract main body text from page."""
        # Remove scripts, styles, nav, footer
        for tag in soup.find_all(["script", "style", "nav", "footer", "header", "aside"]):
            tag.decompose()

        # Try main content areas
        for sel in ["main", "article", "#content", ".content", ".main-content", "#main"]:
            elem = soup.select_one(sel)
            if elem:
                paragraphs = elem.find_all("p")
                if paragraphs:
                    text = " ".join(p.get_text(strip=True) for p in paragraphs if len(p.get_text(strip=True)) > 30)
                    if len(text) > 100:
                        return text[:2000]

        # Fallback: all paragraphs
        paragraphs = soup.find_all("p")
        if paragraphs:
            text = " ".join(p.get_text(strip=True) for p in paragraphs if len(p.get_text(strip=True)) > 30)
            if len(text) > 100:
                return text[:2000]

        return None

    def _generate_summary(self, company_name: str, raw_text: str) -> Optional[str]:
        """Use GPT-4o-mini to generate a clean 2-sentence company summary."""
        if not self.openai_api_key or not self.use_llm:
            # Fallback: use meta description or first 200 chars
            return raw_text[:300] if raw_text else None

        try:
            import openai
            client = openai.OpenAI(api_key=self.openai_api_key)

            response = client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[
                    {
                        "role": "system",
                        "content": "You are a business data assistant. Given raw text from a company website, write a concise 2-sentence description of the company in English. Focus on what the company does, its sector, and its location in Morocco if mentioned.",
                    },
                    {
                        "role": "user",
                        "content": f"Company: {company_name}\n\nWebsite text:\n{raw_text[:1500]}",
                    },
                ],
                max_tokens=150,
                temperature=0.3,
            )

            summary = response.choices[0].message.content.strip()
            return summary[:500]

        except Exception as e:
            logger.error(f"LLM summary generation failed: {e}")
            return raw_text[:300] if raw_text else None

    def crawl_company(self, company: Dict[str, Any]) -> Optional[Dict[str, str]]:
        """Crawl a single company's website and extract enrichment data."""
        website_url = company.get("website_url", "")
        company_name = company.get("company_name", "")

        if not website_url:
            return None

        # Ensure URL has protocol
        if not website_url.startswith("http"):
            website_url = "https://" + website_url

        parsed = urlparse(website_url)
        base = f"{parsed.scheme}://{parsed.netloc}"

        from bs4 import BeautifulSoup
        enrichment = {}

        # 1. Fetch homepage
        homepage_html = self._fetch_page(website_url)
        if not homepage_html:
            logger.warning(f"Could not fetch homepage for {company_name}: {website_url}")
            return None

        soup = BeautifulSoup(homepage_html, "html.parser")
        meta = self._extract_meta(soup)
        contact = self._extract_contact_info(soup)
        body_text = self._extract_body_text(soup)

        enrichment.update(contact)
        if meta.get("meta_description"):
            enrichment["raw_description"] = meta["meta_description"]
        elif meta.get("og_description"):
            enrichment["raw_description"] = meta["og_description"]

        # 2. Fetch /about page
        for path in self.ABOUT_PATHS:
            about_url = urljoin(base, path)
            about_html = self._fetch_page(about_url)
            if about_html:
                about_soup = BeautifulSoup(about_html, "html.parser")
                about_text = self._extract_body_text(about_soup)
                if about_text and len(about_text) > len(enrichment.get("raw_description", "")):
                    enrichment["raw_description"] = about_text

                team = self._extract_team_names(about_soup)
                if team:
                    enrichment["team_names"] = team

                about_contact = self._extract_contact_info(about_soup)
                enrichment.update({k: v for k, v in about_contact.items() if k not in enrichment})
                break

        # 3. Fetch /contact page
        for path in self.CONTACT_PATHS:
            contact_url = urljoin(base, path)
            contact_html = self._fetch_page(contact_url)
            if contact_html:
                contact_soup = BeautifulSoup(contact_html, "html.parser")
                page_contact = self._extract_contact_info(contact_soup)
                enrichment.update({k: v for k, v in page_contact.items() if k not in enrichment})
                break

        # 4. Generate clean summary
        raw_desc = enrichment.get("raw_description") or body_text
        if raw_desc:
            enrichment["description"] = self._generate_summary(company_name, raw_desc)

        return enrichment if enrichment else None

    def _update_company(self, company_id: str, company_name: str, enrichment: Dict[str, Any]) -> bool:
        """Update company record with crawled data (only fill NULLs)."""
        try:
            resp = self.supabase.table("companies").select("*").eq("id", company_id).single().execute()
            current = resp.data
            if not current:
                return False

            update_data = {}

            if enrichment.get("description") and not current.get("description"):
                update_data["description"] = enrichment["description"][:500]

            if enrichment.get("email") and not current.get("email"):
                update_data["email"] = enrichment["email"]

            if enrichment.get("phone") and not current.get("phone"):
                update_data["phone"] = enrichment["phone"]

            if not update_data:
                return False

            if self.dry_run:
                logger.info(f"[DRY-RUN] Would update {company_name}: {list(update_data.keys())}")
                return True

            self.supabase.table("companies").update(update_data).eq("id", company_id).execute()
            logger.info(f"Updated {company_name} with website data: {list(update_data.keys())}")
            return True

        except Exception as e:
            logger.error(f"Error updating company {company_id}: {e}")
            return False

    def run(self, limit: int = 50) -> Dict[str, Any]:
        """Run the website crawler on companies missing descriptions."""
        results = {
            "total": 0,
            "crawled": 0,
            "enriched": 0,
            "errors": 0,
        }

        companies = self._get_companies_to_enrich(limit)
        results["total"] = len(companies)
        logger.info(f"Found {len(companies)} companies to crawl")

        for company in companies:
            try:
                enrichment = self.crawl_company(company)
                results["crawled"] += 1

                if enrichment:
                    if self._update_company(company["id"], company.get("company_name", ""), enrichment):
                        results["enriched"] += 1

            except Exception as e:
                logger.error(f"Error crawling {company.get('company_name')}: {e}")
                results["errors"] += 1

        logger.info(
            f"Website crawl complete: total={results['total']}, "
            f"crawled={results['crawled']}, enriched={results['enriched']}, "
            f"errors={results['errors']}"
        )
        return results


def main():
    """CLI entry point."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    parser = argparse.ArgumentParser(description="MIIM Company Website Crawler")
    parser.add_argument("--limit", type=int, default=50, help="Max companies to process")
    parser.add_argument("--dry-run", action="store_true", default=False, help="Don't save changes")
    parser.add_argument("--use-llm", action="store_true", default=False, help="Use GPT-4o-mini for summaries")

    args = parser.parse_args()

    supabase = create_client(SUPABASE_URL, SUPABASE_ANON_KEY)
    crawler = WebsiteCrawler(supabase, dry_run=args.dry_run, use_llm=args.use_llm)
    results = crawler.run(limit=args.limit)
    print(f"\nResults: {results}")


if __name__ == "__main__":
    main()
