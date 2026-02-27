"""Data Quality Assurance module for MIIM pipeline.

Performs:
1. Deduplication — merge companies with near-identical names (Levenshtein ≤ 2)
2. NULL filling — fill missing fields from related articles/relationships
3. Name normalization — standardize city names, trim whitespace, title-case
4. Quality report — generate summary of data issues

Default: dry-run mode. Use --commit to apply changes.
"""
import argparse
import logging
import re
import uuid
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from supabase import Client, create_client

from config.settings import SUPABASE_ANON_KEY, SUPABASE_URL

logger = logging.getLogger(__name__)

# --- City name normalization mappings ---
CITY_NORMALIZATION = {
    # French → Standardized
    "tanger": "Tanger",
    "tangier": "Tanger",
    "tangers": "Tanger",
    "fès": "Fès",
    "fes": "Fès",
    "fez": "Fès",
    "marrakech": "Marrakech",
    "marrakesh": "Marrakech",
    "casablanca": "Casablanca",
    "casa": "Casablanca",
    "rabat": "Rabat",
    "agadir": "Agadir",
    "oujda": "Oujda",
    "kénitra": "Kénitra",
    "kenitra": "Kénitra",
    "meknès": "Meknès",
    "meknes": "Meknès",
    "mekness": "Meknès",
    "tétouan": "Tétouan",
    "tetouan": "Tétouan",
    "el jadida": "El Jadida",
    "el-jadida": "El Jadida",
    "mohammedia": "Mohammedia",
    "mohamedia": "Mohammedia",
    "safi": "Safi",
    "settat": "Settat",
    "beni mellal": "Béni Mellal",
    "béni mellal": "Béni Mellal",
    "nador": "Nador",
    "taza": "Taza",
    "laayoune": "Laâyoune",
    "laâyoune": "Laâyoune",
    "dakhla": "Dakhla",
    "berrechid": "Berrechid",
    "khouribga": "Khouribga",
    "temara": "Témara",
    "témara": "Témara",
    "salé": "Salé",
    "sale": "Salé",
}

# Legal suffixes to strip for comparison
LEGAL_SUFFIXES = re.compile(
    r'\b(SA|SARL|SAS|SASU|SCA|SNC|SARL AU|GIE|S\.A\.?|S\.A\.R\.L\.?|S\.A\.S\.?)\s*$',
    re.IGNORECASE,
)


def normalize_company_name(name: str) -> str:
    """Normalize a company name for comparison: strip legal suffixes, lowercase, trim."""
    if not name:
        return ""
    cleaned = name.strip()
    cleaned = LEGAL_SUFFIXES.sub("", cleaned).strip()
    cleaned = re.sub(r'\s+', ' ', cleaned)
    return cleaned.lower()


def levenshtein_distance(s1: str, s2: str) -> int:
    """Compute Levenshtein distance between two strings."""
    if len(s1) < len(s2):
        return levenshtein_distance(s2, s1)
    if len(s2) == 0:
        return len(s1)

    prev_row = range(len(s2) + 1)
    for i, c1 in enumerate(s1):
        curr_row = [i + 1]
        for j, c2 in enumerate(s2):
            insertions = prev_row[j + 1] + 1
            deletions = curr_row[j] + 1
            substitutions = prev_row[j] + (c1 != c2)
            curr_row.append(min(insertions, deletions, substitutions))
        prev_row = curr_row

    return prev_row[-1]


def normalize_city(city: str) -> Optional[str]:
    """Normalize city name using the mapping."""
    if not city:
        return None
    city_stripped = city.strip()
    lookup = city_stripped.lower()
    if lookup in CITY_NORMALIZATION:
        return CITY_NORMALIZATION[lookup]
    # Title-case as fallback
    return city_stripped.title()


class QualityAssurance:
    """Run data quality checks and fixes on the MIIM database."""

    def __init__(self, supabase_client: Client, commit: bool = False):
        self.supabase = supabase_client
        self.commit = commit
        self.run_id = str(uuid.uuid4())
        self.report: Dict[str, Any] = {
            "run_id": self.run_id,
            "mode": "commit" if commit else "dry-run",
            "started_at": datetime.now(timezone.utc).isoformat(),
            "duplicates_found": 0,
            "merges_applied": 0,
            "nulls_filled": 0,
            "names_normalized": 0,
            "issues": [],
        }

    def _log_action(self, action_type: str, entity_id: Optional[str], details: dict) -> None:
        """Log a quality action to the database."""
        try:
            self.supabase.table("data_quality_log").insert({
                "run_id": self.run_id,
                "action_type": action_type,
                "entity_type": "company",
                "entity_id": entity_id,
                "details": details,
                "applied": self.commit,
            }).execute()
        except Exception as e:
            logger.error(f"Failed to log quality action: {e}")

    def _get_all_companies(self) -> List[Dict[str, Any]]:
        """Fetch all companies from the database."""
        try:
            response = self.supabase.table("companies").select("*").execute()
            return response.data or []
        except Exception as e:
            logger.error(f"Failed to fetch companies: {e}")
            return []

    # ── 1. Deduplication ──────────────────────────────────────────────

    def find_duplicates(self) -> List[Tuple[Dict, Dict, int]]:
        """Find companies with near-identical names (Levenshtein ≤ 2 after normalization)."""
        companies = self._get_all_companies()
        if not companies:
            return []

        duplicates = []
        seen_pairs = set()

        for i, c1 in enumerate(companies):
            name1 = normalize_company_name(c1.get("company_name", ""))
            if not name1 or len(name1) < 3:
                continue

            for j, c2 in enumerate(companies):
                if j <= i:
                    continue
                name2 = normalize_company_name(c2.get("company_name", ""))
                if not name2 or len(name2) < 3:
                    continue

                # Quick length check to skip obvious non-matches
                if abs(len(name1) - len(name2)) > 2:
                    continue

                dist = levenshtein_distance(name1, name2)
                if dist <= 2 and dist < max(len(name1), len(name2)):
                    pair_key = tuple(sorted([c1["id"], c2["id"]]))
                    if pair_key not in seen_pairs:
                        seen_pairs.add(pair_key)
                        duplicates.append((c1, c2, dist))
                        logger.info(
                            f"Duplicate found: '{c1['company_name']}' ↔ '{c2['company_name']}' (dist={dist})"
                        )

        self.report["duplicates_found"] = len(duplicates)
        return duplicates

    def merge_duplicates(self, duplicates: List[Tuple[Dict, Dict, int]]) -> int:
        """Merge duplicate companies: keep highest confidence, fill NULLs from duplicate."""
        merged_count = 0

        for c1, c2, dist in duplicates:
            # Keep the one with higher confidence or more complete data
            conf1 = c1.get("data_confidence") or 0
            conf2 = c2.get("data_confidence") or 0
            fields1 = sum(1 for v in c1.values() if v is not None)
            fields2 = sum(1 for v in c2.values() if v is not None)

            if conf1 >= conf2 or (conf1 == conf2 and fields1 >= fields2):
                keeper, duplicate = c1, c2
            else:
                keeper, duplicate = c2, c1

            # Fill NULLs in keeper from duplicate
            update_data = {}
            fillable_fields = [
                "sector", "headquarters_city", "headquarters_address",
                "ownership_type", "parent_company", "website_url",
                "description", "employee_count", "year_founded",
                "capital_mad", "ice_number", "legal_form",
            ]
            for field in fillable_fields:
                if keeper.get(field) is None and duplicate.get(field) is not None:
                    update_data[field] = duplicate[field]

            details = {
                "keeper_id": keeper["id"],
                "keeper_name": keeper.get("company_name"),
                "duplicate_id": duplicate["id"],
                "duplicate_name": duplicate.get("company_name"),
                "distance": dist,
                "fields_filled": list(update_data.keys()),
            }

            self._log_action("merge", keeper["id"], details)

            if self.commit and update_data:
                try:
                    self.supabase.table("companies").update(update_data).eq("id", keeper["id"]).execute()
                    logger.info(f"Merged: kept '{keeper['company_name']}', filled {list(update_data.keys())}")
                    merged_count += 1
                except Exception as e:
                    logger.error(f"Failed to merge {keeper['id']}: {e}")
            elif update_data:
                logger.info(f"[DRY-RUN] Would merge: kept '{keeper['company_name']}', fill {list(update_data.keys())}")
                merged_count += 1

        self.report["merges_applied"] = merged_count
        return merged_count

    # ── 2. NULL Filling ───────────────────────────────────────────────

    def fill_nulls(self) -> int:
        """Fill missing sector/city/ownership from related articles or extraction results."""
        companies = self._get_all_companies()
        filled_count = 0

        for company in companies:
            company_id = company["id"]
            update_data = {}

            # Check if sector is missing
            if not company.get("sector"):
                # Try to infer from extraction results
                try:
                    resp = self.supabase.table("extraction_results").select(
                        "raw_extraction"
                    ).contains(
                        "raw_extraction", {"entities": [{"company_name": company.get("company_name", "")}]}
                    ).limit(5).execute()

                    for result in (resp.data or []):
                        raw = result.get("raw_extraction", {})
                        for entity in raw.get("entities", []):
                            if entity.get("company_name", "").lower() == company.get("company_name", "").lower():
                                if entity.get("sector"):
                                    update_data["sector"] = entity["sector"]
                                    break
                        if "sector" in update_data:
                            break
                except Exception:
                    pass

            # Check if city is missing
            if not company.get("headquarters_city"):
                try:
                    resp = self.supabase.table("extraction_results").select(
                        "raw_extraction"
                    ).contains(
                        "raw_extraction", {"entities": [{"company_name": company.get("company_name", "")}]}
                    ).limit(5).execute()

                    for result in (resp.data or []):
                        raw = result.get("raw_extraction", {})
                        for entity in raw.get("entities", []):
                            if entity.get("company_name", "").lower() == company.get("company_name", "").lower():
                                if entity.get("headquarters_city"):
                                    update_data["headquarters_city"] = normalize_city(entity["headquarters_city"])
                                    break
                        if "headquarters_city" in update_data:
                            break
                except Exception:
                    pass

            if update_data:
                details = {
                    "company_id": company_id,
                    "company_name": company.get("company_name"),
                    "fields_filled": update_data,
                }
                self._log_action("null_fill", company_id, details)

                if self.commit:
                    try:
                        self.supabase.table("companies").update(update_data).eq("id", company_id).execute()
                        filled_count += 1
                        logger.info(f"Filled NULLs for '{company.get('company_name')}': {list(update_data.keys())}")
                    except Exception as e:
                        logger.error(f"Failed to fill NULLs for {company_id}: {e}")
                else:
                    filled_count += 1
                    logger.info(f"[DRY-RUN] Would fill NULLs for '{company.get('company_name')}': {update_data}")

        self.report["nulls_filled"] = filled_count
        return filled_count

    # ── 3. Name Normalization ─────────────────────────────────────────

    def normalize_names(self) -> int:
        """Standardize city names, trim whitespace, title-case company names."""
        companies = self._get_all_companies()
        normalized_count = 0

        for company in companies:
            company_id = company["id"]
            update_data = {}

            # Normalize city
            city = company.get("headquarters_city")
            if city:
                normalized = normalize_city(city)
                if normalized and normalized != city:
                    update_data["headquarters_city"] = normalized

            # Trim and clean company name
            name = company.get("company_name", "")
            if name:
                cleaned = re.sub(r'\s+', ' ', name.strip())
                if cleaned != name:
                    update_data["company_name"] = cleaned

            if update_data:
                details = {
                    "company_id": company_id,
                    "company_name": company.get("company_name"),
                    "changes": update_data,
                }
                self._log_action("name_normalize", company_id, details)

                if self.commit:
                    try:
                        self.supabase.table("companies").update(update_data).eq("id", company_id).execute()
                        normalized_count += 1
                        logger.info(f"Normalized '{company.get('company_name')}': {update_data}")
                    except Exception as e:
                        logger.error(f"Failed to normalize {company_id}: {e}")
                else:
                    normalized_count += 1
                    logger.info(f"[DRY-RUN] Would normalize '{company.get('company_name')}': {update_data}")

        self.report["names_normalized"] = normalized_count
        return normalized_count

    # ── 4. Quality Report ─────────────────────────────────────────────

    def generate_report(self) -> Dict[str, Any]:
        """Generate a summary report of data quality issues."""
        companies = self._get_all_companies()

        # Completeness scores
        completeness = defaultdict(int)
        key_fields = [
            "sector", "headquarters_city", "ownership_type",
            "description", "employee_count", "website_url",
        ]

        for company in companies:
            for field in key_fields:
                if company.get(field):
                    completeness[field] += 1

        total = len(companies) or 1
        completeness_pct = {
            field: round(count / total * 100, 1)
            for field, count in completeness.items()
        }

        # Orphaned parent references
        orphaned_parents = []
        company_names = {c.get("company_name", "").lower() for c in companies}
        for company in companies:
            parent = company.get("parent_company")
            if parent and parent.lower() not in company_names:
                orphaned_parents.append({
                    "company": company.get("company_name"),
                    "parent_reference": parent,
                })

        # Sector distribution
        sector_dist = defaultdict(int)
        for company in companies:
            sector = company.get("sector") or "Unknown"
            sector_dist[sector] += 1

        self.report["completed_at"] = datetime.now(timezone.utc).isoformat()
        self.report["total_companies"] = total
        self.report["completeness"] = completeness_pct
        self.report["orphaned_parents"] = orphaned_parents
        self.report["sector_distribution"] = dict(sector_dist)

        # Log report as info action
        self._log_action("info", None, {
            "type": "quality_report",
            "total_companies": total,
            "completeness": completeness_pct,
            "orphaned_parents_count": len(orphaned_parents),
        })

        return self.report

    # ── Main runner ───────────────────────────────────────────────────

    def run(self) -> Dict[str, Any]:
        """Run all quality checks."""
        logger.info(f"Starting quality assurance run (mode={'commit' if self.commit else 'dry-run'})")

        # 1. Find and merge duplicates
        duplicates = self.find_duplicates()
        if duplicates:
            self.merge_duplicates(duplicates)

        # 2. Fill NULL values
        self.fill_nulls()

        # 3. Normalize names
        self.normalize_names()

        # 4. Generate report
        report = self.generate_report()

        logger.info(
            f"Quality assurance complete: "
            f"duplicates={report['duplicates_found']}, "
            f"merges={report['merges_applied']}, "
            f"nulls_filled={report['nulls_filled']}, "
            f"names_normalized={report['names_normalized']}"
        )

        return report


def main():
    """CLI entry point for quality assurance."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    parser = argparse.ArgumentParser(description="MIIM Data Quality Assurance")
    parser.add_argument("--commit", action="store_true", default=False, help="Apply changes (default: dry-run)")

    args = parser.parse_args()

    supabase = create_client(SUPABASE_URL, SUPABASE_ANON_KEY)
    qa = QualityAssurance(supabase, commit=args.commit)
    report = qa.run()

    print("\n" + "=" * 60)
    print("QUALITY ASSURANCE REPORT")
    print("=" * 60)
    print(f"Mode: {report['mode']}")
    print(f"Total companies: {report.get('total_companies', 'N/A')}")
    print(f"Duplicates found: {report['duplicates_found']}")
    print(f"Merges {'applied' if args.commit else 'would apply'}: {report['merges_applied']}")
    print(f"NULLs filled: {report['nulls_filled']}")
    print(f"Names normalized: {report['names_normalized']}")

    if report.get("completeness"):
        print(f"\nField completeness:")
        for field, pct in report["completeness"].items():
            print(f"  {field}: {pct}%")

    if report.get("orphaned_parents"):
        print(f"\nOrphaned parent references ({len(report['orphaned_parents'])}):")
        for item in report["orphaned_parents"][:10]:
            print(f"  {item['company']} → {item['parent_reference']}")

    print("=" * 60)


if __name__ == "__main__":
    main()
