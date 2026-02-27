"""Charika.ma registry scraper — enriches existing companies with official data.

Requires:
- CHARIKA_EMAIL and CHARIKA_PASSWORD environment variables
- Playwright installed (pip install playwright && playwright install chromium)

Usage:
  python -m scrapers.charika_scraper [--limit 20] [--dry-run]
"""
import argparse
import logging
import os
import re
import time
from typing import Any, Dict, List, Optional

from supabase import Client, create_client

from config.settings import SUPABASE_ANON_KEY, SUPABASE_URL

logger = logging.getLogger(__name__)

# Fields to extract from Charika company profiles
CHARIKA_FIELDS = [
    "legal_form",
    "capital_mad",
    "ice_number",
    "date_registered",
    "headquarters_address",
    "phone",
    "email",
    "sector",
    "description",
]


class CharikaScraper:
    """Scraper for Charika.ma — enriches companies with official registry data."""

    BASE_URL = "https://www.charika.ma"
    SEARCH_URL = "https://www.charika.ma/recherche"

    def __init__(self, supabase_client: Client, email: str, password: str, dry_run: bool = False):
        self.supabase = supabase_client
        self.email = email
        self.password = password
        self.dry_run = dry_run
        self.browser = None
        self.page = None
        self.rate_limit = 5.0  # 5 seconds between requests
        self._last_request_time = 0.0

    def _wait_rate_limit(self) -> None:
        """Enforce rate limiting between requests."""
        elapsed = time.time() - self._last_request_time
        if elapsed < self.rate_limit:
            time.sleep(self.rate_limit - elapsed)
        self._last_request_time = time.time()

    async def _init_browser(self) -> None:
        """Initialize Playwright browser."""
        from playwright.async_api import async_playwright

        self._playwright = await async_playwright().start()
        self.browser = await self._playwright.chromium.launch(headless=True)
        self.context = await self.browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            viewport={"width": 1280, "height": 720},
        )
        self.page = await self.context.new_page()
        logger.info("Browser initialized")

    async def _close_browser(self) -> None:
        """Close Playwright browser."""
        if self.browser:
            await self.browser.close()
        if self._playwright:
            await self._playwright.stop()
        logger.info("Browser closed")

    async def login(self) -> bool:
        """Log in to Charika.ma."""
        try:
            await self.page.goto(f"{self.BASE_URL}/connexion", wait_until="networkidle")
            self._wait_rate_limit()

            # Fill login form
            email_input = await self.page.query_selector('input[type="email"], input[name="email"], #email')
            password_input = await self.page.query_selector('input[type="password"], input[name="password"], #password')

            if not email_input or not password_input:
                # Try broader selectors
                inputs = await self.page.query_selector_all('input')
                for inp in inputs:
                    inp_type = await inp.get_attribute("type")
                    if inp_type == "email" or (await inp.get_attribute("name") or "").lower() == "email":
                        email_input = inp
                    elif inp_type == "password":
                        password_input = inp

            if not email_input or not password_input:
                logger.error("Could not find login form fields")
                return False

            await email_input.fill(self.email)
            await password_input.fill(self.password)

            # Submit form
            submit_btn = await self.page.query_selector('button[type="submit"], input[type="submit"]')
            if submit_btn:
                await submit_btn.click()
            else:
                await password_input.press("Enter")

            await self.page.wait_for_load_state("networkidle")
            self._wait_rate_limit()

            # Check if login succeeded (look for logout link or user profile)
            is_logged_in = await self.page.query_selector(
                'a[href*="deconnexion"], a[href*="logout"], .user-menu, .user-profile'
            )
            if is_logged_in:
                logger.info("Successfully logged in to Charika.ma")
                return True

            # Check if we're still on the login page
            current_url = self.page.url
            if "connexion" not in current_url.lower():
                logger.info("Login appears successful (redirected away from login page)")
                return True

            logger.error("Login failed — still on login page")
            return False

        except Exception as e:
            logger.error(f"Login error: {e}")
            return False

    async def search_company(self, company_name: str) -> Optional[str]:
        """Search for a company on Charika.ma and return the profile URL."""
        try:
            self._wait_rate_limit()
            await self.page.goto(
                f"{self.SEARCH_URL}?q={company_name.replace(' ', '+')}",
                wait_until="networkidle",
            )

            # Look for the first result link
            result_link = await self.page.query_selector(
                'a[href*="/entreprise/"], .search-result a, .company-link, .result-item a'
            )

            if result_link:
                href = await result_link.get_attribute("href")
                if href:
                    if not href.startswith("http"):
                        href = self.BASE_URL + href
                    logger.info(f"Found Charika profile for '{company_name}': {href}")
                    return href

            # Fallback: look for any link containing the company name
            all_links = await self.page.query_selector_all("a[href]")
            for link in all_links:
                text = await link.inner_text()
                href = await link.get_attribute("href")
                if text and company_name.lower()[:10] in text.lower() and "/entreprise/" in (href or ""):
                    full_url = href if href.startswith("http") else self.BASE_URL + href
                    logger.info(f"Found Charika profile (fuzzy) for '{company_name}': {full_url}")
                    return full_url

            logger.warning(f"No Charika profile found for '{company_name}'")
            return None

        except Exception as e:
            logger.error(f"Error searching for '{company_name}': {e}")
            return None

    async def scrape_profile(self, profile_url: str) -> Optional[Dict[str, Any]]:
        """Scrape a company profile page for structured data."""
        try:
            self._wait_rate_limit()
            await self.page.goto(profile_url, wait_until="networkidle")

            data = {}

            # Extract text content of the page
            content = await self.page.content()

            from bs4 import BeautifulSoup
            soup = BeautifulSoup(content, "html.parser")

            # Extract structured fields from tables or definition lists
            # Charika typically uses tables or key-value lists
            for row in soup.find_all("tr"):
                cells = row.find_all(["td", "th"])
                if len(cells) >= 2:
                    key = cells[0].get_text(strip=True).lower()
                    value = cells[1].get_text(strip=True)

                    if not value or value == "-":
                        continue

                    if "forme juridique" in key or "legal form" in key:
                        data["legal_form"] = value
                    elif "capital" in key:
                        # Extract numeric capital
                        amount = re.search(r'[\d\s,.]+', value)
                        if amount:
                            try:
                                data["capital_mad"] = float(amount.group().replace(" ", "").replace(",", "."))
                            except ValueError:
                                pass
                    elif "ice" in key:
                        data["ice_number"] = value
                    elif "date" in key and ("création" in key or "immatriculation" in key or "registration" in key):
                        data["date_registered"] = value
                    elif "adresse" in key or "address" in key or "siège" in key:
                        data["headquarters_address"] = value
                    elif "téléphone" in key or "phone" in key or "tél" in key:
                        data["phone"] = value
                    elif "email" in key or "e-mail" in key:
                        data["email"] = value
                    elif "activité" in key or "secteur" in key or "activity" in key:
                        data["sector"] = value

            # Also try definition lists (dl/dt/dd)
            for dl in soup.find_all("dl"):
                terms = dl.find_all("dt")
                defs = dl.find_all("dd")
                for dt, dd in zip(terms, defs):
                    key = dt.get_text(strip=True).lower()
                    value = dd.get_text(strip=True)
                    if not value or value == "-":
                        continue
                    if "forme" in key:
                        data.setdefault("legal_form", value)
                    elif "capital" in key:
                        amount = re.search(r'[\d\s,.]+', value)
                        if amount:
                            try:
                                data.setdefault("capital_mad", float(amount.group().replace(" ", "").replace(",", ".")))
                            except ValueError:
                                pass
                    elif "ice" in key:
                        data.setdefault("ice_number", value)

            # Try to get description from meta or page content
            meta_desc = soup.find("meta", {"name": "description"})
            if meta_desc and meta_desc.get("content"):
                data["description"] = meta_desc["content"][:500]

            # Extract people (legal representatives, directors)
            people = []
            for section in soup.find_all(["div", "section"], class_=re.compile(r"dirigeant|manager|representative|personnel")):
                for item in section.find_all(["li", "tr", "div"]):
                    name = item.get_text(strip=True)
                    if name and len(name) > 3 and len(name) < 100:
                        people.append(name)

            if people:
                data["people"] = people[:20]

            if data:
                logger.info(f"Extracted {len(data)} fields from {profile_url}")
            else:
                logger.warning(f"No structured data extracted from {profile_url}")

            return data if data else None

        except Exception as e:
            logger.error(f"Error scraping profile {profile_url}: {e}")
            return None

    def _update_company(self, company_id: str, charika_data: Dict[str, Any]) -> bool:
        """Update a company record with Charika data (only fill NULLs)."""
        try:
            # Fetch current company data
            resp = self.supabase.table("companies").select("*").eq("id", company_id).single().execute()
            company = resp.data
            if not company:
                return False

            update_data = {}
            field_mapping = {
                "legal_form": "legal_form",
                "capital_mad": "capital_mad",
                "ice_number": "ice_number",
                "headquarters_address": "headquarters_address",
                "sector": "sector",
                "description": "description",
            }

            for charika_field, db_field in field_mapping.items():
                if charika_data.get(charika_field) and not company.get(db_field):
                    update_data[db_field] = charika_data[charika_field]

            if not update_data:
                logger.info(f"No new data to update for company {company_id}")
                return False

            if self.dry_run:
                logger.info(f"[DRY-RUN] Would update {company.get('company_name')}: {list(update_data.keys())}")
                return True

            self.supabase.table("companies").update(update_data).eq("id", company_id).execute()
            logger.info(f"Updated {company.get('company_name')} with Charika data: {list(update_data.keys())}")
            return True

        except Exception as e:
            logger.error(f"Error updating company {company_id}: {e}")
            return False

    async def run(self, limit: int = 100) -> Dict[str, Any]:
        """Run the Charika enrichment pipeline."""
        results = {
            "searched": 0,
            "found": 0,
            "enriched": 0,
            "errors": 0,
        }

        try:
            await self._init_browser()

            # Login
            if not await self.login():
                logger.error("Failed to login to Charika.ma, aborting")
                return results

            # Get companies that could benefit from enrichment
            response = self.supabase.table("companies").select(
                "id, company_name, legal_form, ice_number, capital_mad"
            ).is_("ice_number", "null").limit(limit).execute()

            companies = response.data or []
            logger.info(f"Found {len(companies)} companies to enrich via Charika")

            for company in companies:
                company_name = company.get("company_name", "")
                if not company_name:
                    continue

                results["searched"] += 1

                # Search for the company
                profile_url = await self.search_company(company_name)
                if not profile_url:
                    continue

                results["found"] += 1

                # Scrape the profile
                charika_data = await self.scrape_profile(profile_url)
                if not charika_data:
                    continue

                # Update the company
                if self._update_company(company["id"], charika_data):
                    results["enriched"] += 1

        except Exception as e:
            logger.error(f"Error in Charika enrichment pipeline: {e}")
            results["errors"] += 1
        finally:
            await self._close_browser()

        logger.info(
            f"Charika enrichment complete: searched={results['searched']}, "
            f"found={results['found']}, enriched={results['enriched']}"
        )
        return results


async def main():
    """CLI entry point for Charika scraper."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    parser = argparse.ArgumentParser(description="MIIM Charika.ma Registry Scraper")
    parser.add_argument("--limit", type=int, default=100, help="Max companies to process")
    parser.add_argument("--dry-run", action="store_true", default=False, help="Don't save changes")

    args = parser.parse_args()

    email = os.environ.get("CHARIKA_EMAIL")
    password = os.environ.get("CHARIKA_PASSWORD")

    if not email or not password:
        logger.error("CHARIKA_EMAIL and CHARIKA_PASSWORD environment variables required")
        return

    supabase = create_client(SUPABASE_URL, SUPABASE_ANON_KEY)
    scraper = CharikaScraper(supabase, email, password, dry_run=args.dry_run)
    results = await scraper.run(limit=args.limit)
    print(f"\nResults: {results}")


if __name__ == "__main__":
    import asyncio
    asyncio.run(main())
