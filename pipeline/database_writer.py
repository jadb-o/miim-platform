"""Database writer for storing v2 extraction results with comprehensive profiles."""
import logging
import uuid
from datetime import datetime
from typing import Any, Dict, List, Optional

from supabase import Client

from config.settings import GPT_4O_PRICING
from pipeline.deduplication import find_matching_company

logger = logging.getLogger(__name__)


class DatabaseWriter:
    """Write extraction results to Supabase database (v2 — multi-entity)."""

    def __init__(self, supabase_client: Client):
        self.supabase = supabase_client

    # ── Company CRUD ─────────────────────────────────────

    def upsert_company(
        self,
        entity: Dict[str, Any],
        article_id: str,
        confidence: float,
    ) -> Optional[str]:
        """
        Insert or update a company from an extracted entity.
        Now handles v2 fields: description, activities, value_chain_position, etc.

        Returns company_id or None.
        """
        try:
            company_name = (entity.get("company_name") or "").strip()
            if not company_name:
                logger.warning("No company name in entity data")
                return None

            # Try to find existing company by fuzzy match
            existing_id = find_matching_company(
                self.supabase, company_name, city=entity.get("city"),
            )

            sector_id = self._get_sector_id(entity.get("sector"))
            value_chain_stage_id = self._get_value_chain_stage_id(
                sector_id, entity.get("value_chain_position")
            )

            company_data = {
                "company_name": company_name,
                "sector_id": sector_id,
                "sub_sector": entity.get("sub_sector"),
                "headquarters_city": entity.get("city"),
                "description": entity.get("description"),
                "activities": entity.get("activities"),
                "value_chain_stage_id": value_chain_stage_id,
                "website_url": entity.get("website_url"),
                "parent_company": entity.get("parent_company"),
                "ownership_type": entity.get("ownership_type", "Unknown"),
                "data_confidence": confidence,
                "updated_at": datetime.utcnow().isoformat(),
            }

            # Add optional numeric fields only if provided
            if entity.get("employee_count") is not None:
                company_data["employee_count"] = entity["employee_count"]
            if entity.get("revenue_mad") is not None:
                company_data["annual_revenue_mad"] = entity["revenue_mad"]

            if existing_id:
                # Update: only fill fields that are currently empty
                existing = self.supabase.table("companies").select("*").eq("company_id", existing_id).execute()
                if existing.data:
                    current = existing.data[0]
                    update_data = {"updated_at": datetime.utcnow().isoformat()}
                    for key, value in company_data.items():
                        if key in ("updated_at", "data_confidence"):
                            continue
                        if value and not current.get(key):
                            update_data[key] = value
                    # Always update confidence if higher
                    if confidence > (current.get("data_confidence") or 0):
                        update_data["data_confidence"] = confidence

                    self.supabase.table("companies").update(update_data).eq("company_id", existing_id).execute()
                    logger.info(f"Updated company: {company_name} ({existing_id})")

                    # Link article
                    self._link_article_to_company(existing_id, article_id, entity.get("mention_type", "mentioned"))

                    # Save management mentions
                    self._save_management(existing_id, entity.get("management_mentions", []))

                    return existing_id
            else:
                # Insert new company
                company_data["company_id"] = str(uuid.uuid4())
                company_data["created_at"] = datetime.utcnow().isoformat()
                # Clean None values for insert
                company_data = {k: v for k, v in company_data.items() if v is not None}

                response = self.supabase.table("companies").insert(company_data).execute()
                if response.data:
                    company_id = response.data[0]["company_id"]
                    logger.info(f"Inserted new company: {company_name} ({company_id})")

                    # Link article
                    self._link_article_to_company(company_id, article_id, entity.get("mention_type", "mentioned"))

                    # Save management mentions
                    self._save_management(company_id, entity.get("management_mentions", []))

                    return company_id
                else:
                    logger.error(f"Failed to insert company: {company_name}")
                    return None

        except Exception as e:
            logger.error(f"Error upserting company: {str(e)}", exc_info=True)
            return None

    # ── Events ───────────────────────────────────────────

    def insert_event(
        self,
        entity: Dict[str, Any],
        company_id: str,
        article_url: str,
        confidence: float,
    ) -> Optional[str]:
        """Insert an event record from an entity."""
        try:
            event_type = (entity.get("event_type") or "other").lower()
            event_type = self._normalize_event_type(event_type)

            summary = entity.get("source_summary") or entity.get("description") or ""
            title = summary[:200] if summary else f"Event: {entity.get('company_name', '')}"

            event_data = {
                "event_id": str(uuid.uuid4()),
                "company_id": company_id,
                "event_type": event_type,
                "title": title,
                "description": summary,
                "event_date": datetime.utcnow().isoformat(),
                "city": entity.get("city", ""),
                "investment_amount_mad": entity.get("investment_amount_mad"),
                "source_url": article_url,
                "confidence_score": confidence,
                "created_at": datetime.utcnow().isoformat(),
            }

            response = self.supabase.table("events").insert(event_data).execute()
            if response.data:
                event_id = response.data[0]["event_id"]
                logger.info(f"Inserted event: {event_type} for company {company_id}")
                return event_id
            return None

        except Exception as e:
            logger.error(f"Error inserting event: {str(e)}", exc_info=True)
            return None

    # ── Relationships (v2 — unified) ─────────────────────

    def insert_relationships(
        self,
        relationships: List[Dict[str, Any]],
        company_name_to_id: Dict[str, str],
        source_url: str,
        confidence: float,
    ) -> int:
        """
        Insert relationship records from the v2 extraction.

        Args:
            relationships: List of {source_company, target_company, relationship_type, description}
            company_name_to_id: Mapping of company names to their IDs
            source_url: Source article URL
            confidence: Confidence score

        Returns:
            Number of relationships inserted
        """
        inserted = 0
        for rel in relationships:
            try:
                source_name = rel.get("source_company", "").strip()
                target_name = rel.get("target_company", "").strip()
                rel_type = rel.get("relationship_type", "partner")

                # Resolve company IDs
                source_id = company_name_to_id.get(source_name)
                target_id = company_name_to_id.get(target_name)

                if not source_id:
                    source_id = find_matching_company(self.supabase, source_name)
                if not target_id:
                    target_id = find_matching_company(self.supabase, target_name)

                if not source_id or not target_id:
                    logger.debug(f"Skipping relationship: missing company ID for {source_name} -> {target_name}")
                    continue
                if source_id == target_id:
                    continue

                # Check if relationship already exists
                existing = (
                    self.supabase.table("company_relationships")
                    .select("id")
                    .eq("source_company_id", source_id)
                    .eq("target_company_id", target_id)
                    .eq("relationship_type", rel_type)
                    .execute()
                )
                if existing.data:
                    continue  # Already exists

                rel_data = {
                    "id": str(uuid.uuid4()),
                    "source_company_id": source_id,
                    "target_company_id": target_id,
                    "relationship_type": rel_type,
                    "description": rel.get("description", ""),
                    "source_url": source_url,
                    "confidence_score": confidence,
                    "status": "active",
                    "created_at": datetime.utcnow().isoformat(),
                }

                response = self.supabase.table("company_relationships").insert(rel_data).execute()
                if response.data:
                    inserted += 1
                    logger.info(f"Inserted relationship: {source_name} --[{rel_type}]--> {target_name}")

                # Also insert into legacy partnerships table for backward compatibility
                if rel_type in ("partner", "joint_venture"):
                    self._insert_legacy_partnership(source_id, target_id, rel_type, rel.get("description", ""), source_url, confidence)

            except Exception as e:
                logger.error(f"Error inserting relationship: {str(e)}", exc_info=True)

        return inserted

    # ── Article linking ──────────────────────────────────

    def _link_article_to_company(self, company_id: str, article_id: str, mention_type: str = "mentioned") -> None:
        """Create a company-article link for media mention tracking."""
        try:
            link_data = {
                "id": str(uuid.uuid4()),
                "company_id": company_id,
                "article_id": article_id,
                "mention_type": mention_type,
                "created_at": datetime.utcnow().isoformat(),
            }
            self.supabase.table("company_articles").insert(link_data).execute()
        except Exception as e:
            # Unique constraint violation is expected for duplicate links
            if "unique" not in str(e).lower() and "duplicate" not in str(e).lower():
                logger.error(f"Error linking article to company: {str(e)}")

    # ── Management team ──────────────────────────────────

    def _save_management(self, company_id: str, management_mentions: List[Dict]) -> None:
        """Save management/people mentions for a company."""
        if not management_mentions:
            return

        for person in management_mentions:
            try:
                name = (person.get("name") or "").strip()
                role = (person.get("role") or "").strip()
                if not name:
                    continue

                # Check if person already exists for this company
                existing = (
                    self.supabase.table("company_people")
                    .select("id")
                    .eq("company_id", company_id)
                    .ilike("person_name", f"%{name}%")
                    .execute()
                )
                if existing.data:
                    continue  # Already exists

                # Classify role type
                role_type = self._classify_role(role)

                person_data = {
                    "id": str(uuid.uuid4()),
                    "company_id": company_id,
                    "person_name": name,
                    "role_title": role,
                    "role_type": role_type,
                    "created_at": datetime.utcnow().isoformat(),
                }

                self.supabase.table("company_people").insert(person_data).execute()
                logger.debug(f"Saved person: {name} ({role}) for company {company_id}")

            except Exception as e:
                logger.error(f"Error saving person {person}: {str(e)}")

    # ── Extraction results & review queue ────────────────

    def save_extraction_result(
        self,
        article_id: str,
        extraction_data: Dict[str, Any],
        confidence: float,
        input_tokens: int,
        output_tokens: int,
        processing_time_ms: int,
    ) -> Optional[str]:
        """Save extraction result to database."""
        try:
            result_data = {
                "id": str(uuid.uuid4()),
                "article_id": article_id,
                "extraction_data": extraction_data,
                "model_used": "gpt-4o",
                "prompt_version": 2,
                "input_tokens": input_tokens,
                "output_tokens": output_tokens,
                "confidence_score": confidence,
                "processing_time_ms": processing_time_ms,
                "created_at": datetime.utcnow().isoformat(),
            }

            response = self.supabase.table("extraction_results").insert(result_data).execute()
            if response.data:
                result_id = response.data[0]["id"]
                logger.info(f"Saved extraction result: {result_id}")
                return result_id
            return None

        except Exception as e:
            logger.error(f"Error saving extraction result: {str(e)}", exc_info=True)
            return None

    def add_to_review_queue(
        self,
        article_id: str,
        extraction_result_id: str,
        extraction_data: Dict[str, Any],
        confidence: float,
        reason: str,
    ) -> Optional[str]:
        """Add extraction result to review queue."""
        try:
            review_data = {
                "id": str(uuid.uuid4()),
                "article_id": article_id,
                "extraction_result_id": extraction_result_id,
                "extracted_data": extraction_data,
                "confidence_score": confidence,
                "reason_flagged": reason,
                "status": "pending",
                "created_at": datetime.utcnow().isoformat(),
            }

            response = self.supabase.table("review_queue").insert(review_data).execute()
            if response.data:
                review_id = response.data[0]["id"]
                logger.info(f"Added to review queue: {review_id}")
                return review_id
            return None

        except Exception as e:
            logger.error(f"Error adding to review queue: {str(e)}", exc_info=True)
            return None

    def log_cost(
        self,
        article_id: Optional[str],
        extraction_result_id: Optional[str],
        model: str,
        input_tokens: int,
        output_tokens: int,
    ) -> bool:
        """Log API costs."""
        try:
            cost_usd = (
                input_tokens * GPT_4O_PRICING["input"] + output_tokens * GPT_4O_PRICING["output"]
            )

            cost_data = {
                "id": str(uuid.uuid4()),
                "article_id": article_id,
                "extraction_result_id": extraction_result_id,
                "model": model,
                "input_tokens": input_tokens,
                "output_tokens": output_tokens,
                "cost_usd": round(cost_usd, 6),
                "logged_at": datetime.utcnow().isoformat(),
            }

            self.supabase.table("pipeline_costs").insert(cost_data).execute()
            return True

        except Exception as e:
            logger.error(f"Error logging cost: {str(e)}", exc_info=True)
            return False

    # ── Legacy support ───────────────────────────────────

    def _insert_legacy_partnership(
        self, company_a_id: str, company_b_id: str,
        rel_type: str, description: str, source_url: str, confidence: float,
    ) -> None:
        """Insert into the old partnerships table for backward compatibility."""
        try:
            # Check for existing
            existing = (
                self.supabase.table("partnerships")
                .select("partnership_id")
                .eq("company_a_id", company_a_id)
                .eq("company_b_id", company_b_id)
                .execute()
            )
            if existing.data:
                return

            partnership_data = {
                "partnership_id": str(uuid.uuid4()),
                "company_a_id": company_a_id,
                "company_b_id": company_b_id,
                "partnership_type": "Joint Venture" if rel_type == "joint_venture" else "Other",
                "description": description,
                "status": "Active",
                "source_url": source_url,
                "confidence_score": confidence,
                "created_at": datetime.utcnow().isoformat(),
            }
            self.supabase.table("partnerships").insert(partnership_data).execute()
        except Exception as e:
            logger.debug(f"Legacy partnership insert skipped: {str(e)}")

    def insert_partnerships(
        self, extraction_data: Dict[str, Any], company_id: str, source_url: str, confidence: float,
    ) -> List[str]:
        """Legacy v1 compatibility: insert partnerships from partner_companies list."""
        partnership_ids = []
        partners = extraction_data.get("partner_companies", [])
        if not partners:
            return partnership_ids

        for partner_name in partners:
            partner_name = (partner_name or "").strip()
            if not partner_name:
                continue

            partner_id = find_matching_company(self.supabase, partner_name)
            if not partner_id:
                new_partner = {
                    "company_id": str(uuid.uuid4()),
                    "company_name": partner_name,
                    "data_confidence": confidence,
                    "created_at": datetime.utcnow().isoformat(),
                }
                response = self.supabase.table("companies").insert(new_partner).execute()
                if response.data:
                    partner_id = response.data[0]["company_id"]
                else:
                    continue

            self._insert_legacy_partnership(company_id, partner_id, "partner", "", source_url, confidence)

        return partnership_ids

    # ── Helpers ───────────────────────────────────────────

    def _get_sector_id(self, sector_name: Optional[str]) -> Optional[str]:
        if not sector_name:
            return None
        try:
            response = (
                self.supabase.table("sectors")
                .select("sector_id")
                .ilike("sector_name", f"%{sector_name.strip()}%")
                .limit(1)
                .execute()
            )
            return response.data[0]["sector_id"] if response.data else None
        except Exception as e:
            logger.error(f"Error getting sector_id for '{sector_name}': {str(e)}")
            return None

    def _get_value_chain_stage_id(self, sector_id: Optional[str], position: Optional[str]) -> Optional[str]:
        """Look up value_chain_stage_id from sector and position description."""
        if not sector_id or not position:
            return None
        try:
            response = (
                self.supabase.table("value_chain_stages")
                .select("id")
                .eq("sector_id", sector_id)
                .ilike("stage_name", f"%{position.strip()}%")
                .limit(1)
                .execute()
            )
            return response.data[0]["id"] if response.data else None
        except Exception as e:
            logger.debug(f"Value chain stage lookup failed: {str(e)}")
            return None

    def _normalize_event_type(self, event_type: str) -> str:
        mapping = {
            "new_factory": "New Factory",
            "partnership": "Partnership",
            "investment": "Investment",
            "acquisition": "Acquisition",
            "export_milestone": "Export Milestone",
            "government_incentive": "Government Incentive",
            "expansion": "Expansion",
            "closure": "Closure",
            "ipo": "IPO",
            "hiring": "Other",
            "product_launch": "Other",
            "certification": "Other",
            "other": "Other",
        }
        return mapping.get(event_type.lower(), "Other")

    def _classify_role(self, role_title: str) -> str:
        """Classify a role title into a role_type enum value."""
        role_lower = role_title.lower()
        if any(k in role_lower for k in ["ceo", "pdg", "directeur général", "director general", "chief executive"]):
            return "CEO"
        if any(k in role_lower for k in ["cfo", "directeur financier", "chief financial"]):
            return "CFO"
        if any(k in role_lower for k in ["coo", "directeur des opérations", "chief operating"]):
            return "COO"
        if any(k in role_lower for k in ["cto", "directeur technique", "chief technology"]):
            return "CTO"
        if any(k in role_lower for k in ["fondateur", "founder", "co-fondateur", "co-founder"]):
            return "Founder"
        if any(k in role_lower for k in ["directeur", "director", "directrice"]):
            return "Director"
        if any(k in role_lower for k in ["board", "administrateur", "conseil"]):
            return "Board"
        if any(k in role_lower for k in ["manager", "responsable", "chef"]):
            return "Manager"
        return "Other"
