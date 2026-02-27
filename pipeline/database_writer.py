"""Database writer for storing extraction results and linked data."""
import logging
import uuid
from datetime import datetime
from typing import Any, Dict, List, Optional

from supabase import Client

from config.settings import GPT_4O_PRICING
from pipeline.deduplication import find_matching_company

logger = logging.getLogger(__name__)


class DatabaseWriter:
    """Write extraction results to Supabase database."""

    def __init__(self, supabase_client: Client):
        """
        Initialize DatabaseWriter.

        Args:
            supabase_client: Supabase client instance
        """
        self.supabase = supabase_client

    def upsert_company(
        self,
        extraction_data: Dict[str, Any],
        article_id: str,
        confidence: float,
    ) -> Optional[str]:
        """
        Insert or update company based on extraction data.

        Args:
            extraction_data: Extracted company data dictionary
            article_id: Source article ID
            confidence: Confidence score

        Returns:
            company_id or None if failed
        """
        try:
            company_name = extraction_data.get("company_name", "").strip()
            if not company_name:
                logger.warning("No company name in extraction data")
                return None

            # Try to find existing company by fuzzy match
            existing_id = find_matching_company(
                self.supabase,
                company_name,
                city=extraction_data.get("city"),
            )

            sector_id = self._get_sector_id(extraction_data.get("sector"))
            city = extraction_data.get("city", "")

            company_data = {
                "company_name": company_name,
                "sector_id": sector_id,
                "sub_sector": extraction_data.get("sub_sector"),
                "headquarters_city": city,
                "data_confidence": confidence,
                "updated_at": datetime.utcnow().isoformat(),
            }

            if existing_id:
                # Update existing company (only update None/empty fields)
                existing = self.supabase.table("companies").select("*").eq("company_id", existing_id).execute()
                if existing.data:
                    current = existing.data[0]
                    # Only update fields that are empty/None in existing record
                    for key, value in company_data.items():
                        if value and not current.get(key):
                            pass  # Will be included in update
                        elif not value:
                            company_data.pop(key, None)

                response = (
                    self.supabase.table("companies").update(company_data).eq("company_id", existing_id).execute()
                )

                if response.data:
                    logger.info(f"Updated company: {company_name} ({existing_id})")
                    return existing_id
                else:
                    logger.error(f"Failed to update company: {company_name}")
                    return None

            else:
                # Insert new company
                company_data["company_id"] = str(uuid.uuid4())
                company_data["created_at"] = datetime.utcnow().isoformat()

                response = self.supabase.table("companies").insert(company_data).execute()

                if response.data:
                    company_id = response.data[0]["company_id"]
                    logger.info(f"Inserted new company: {company_name} ({company_id})")
                    return company_id
                else:
                    logger.error(f"Failed to insert company: {company_name}")
                    return None

        except Exception as e:
            logger.error(f"Error upserting company: {str(e)}", exc_info=True)
            return None

    def insert_event(
        self,
        extraction_data: Dict[str, Any],
        company_id: str,
        article_url: str,
        confidence: float,
    ) -> Optional[str]:
        """
        Insert event record.

        Args:
            extraction_data: Extracted event data
            company_id: Associated company ID
            article_url: Source article URL
            confidence: Confidence score

        Returns:
            event_id or None if failed
        """
        try:
            event_type = extraction_data.get("event_type", "other").lower()
            event_type = self._normalize_event_type(event_type)

            event_data = {
                "event_id": str(uuid.uuid4()),
                "company_id": company_id,
                "event_type": event_type,
                "title": extraction_data.get("company_name", ""),
                "description": extraction_data.get("source_summary", ""),
                "event_date": datetime.utcnow().isoformat(),
                "city": extraction_data.get("city", ""),
                "investment_amount_mad": extraction_data.get("investment_amount_mad"),
                "source_url": article_url,
                "confidence_score": confidence,
                "created_at": datetime.utcnow().isoformat(),
            }

            response = self.supabase.table("events").insert(event_data).execute()

            if response.data:
                event_id = response.data[0]["event_id"]
                logger.info(f"Inserted event: {event_type} for company {company_id}")
                return event_id
            else:
                logger.error(f"Failed to insert event for company {company_id}")
                return None

        except Exception as e:
            logger.error(f"Error inserting event: {str(e)}", exc_info=True)
            return None

    def insert_partnerships(
        self,
        extraction_data: Dict[str, Any],
        company_id: str,
        source_url: str,
        confidence: float,
    ) -> List[str]:
        """
        Insert partnership records for partner companies.

        Args:
            extraction_data: Extracted partnership data
            company_id: Main company ID
            source_url: Source article URL
            confidence: Confidence score

        Returns:
            List of partnership_ids
        """
        partnership_ids = []
        partners = extraction_data.get("partner_companies", [])

        if not partners:
            return partnership_ids

        try:
            for partner_name in partners:
                partner_name = partner_name.strip()
                if not partner_name:
                    continue

                # Find or create partner company
                partner_id = find_matching_company(self.supabase, partner_name)

                if not partner_id:
                    # Create new partner company
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
                        logger.warning(f"Failed to create partner company: {partner_name}")
                        continue

                # Insert partnership
                partnership_data = {
                    "partnership_id": str(uuid.uuid4()),
                    "company_a_id": company_id,
                    "company_b_id": partner_id,
                    "partnership_type": "Other",
                    "description": extraction_data.get("source_summary", ""),
                    "status": "Active",
                    "source_url": source_url,
                    "confidence_score": confidence,
                    "created_at": datetime.utcnow().isoformat(),
                }

                response = self.supabase.table("partnerships").insert(partnership_data).execute()

                if response.data:
                    partnership_ids.append(response.data[0]["partnership_id"])
                    logger.info(f"Inserted partnership between {company_id} and {partner_id}")
                else:
                    logger.warning(f"Failed to insert partnership for {partner_name}")

        except Exception as e:
            logger.error(f"Error inserting partnerships: {str(e)}", exc_info=True)

        return partnership_ids

    def save_extraction_result(
        self,
        article_id: str,
        extraction_data: Dict[str, Any],
        confidence: float,
        input_tokens: int,
        output_tokens: int,
        processing_time_ms: int,
    ) -> Optional[str]:
        """
        Save extraction result to database.

        Args:
            article_id: Source article ID
            extraction_data: Extracted data dictionary
            confidence: Confidence score
            input_tokens: Input token count
            output_tokens: Output token count
            processing_time_ms: Processing time in milliseconds

        Returns:
            extraction_result_id or None if failed
        """
        try:
            result_data = {
                "id": str(uuid.uuid4()),
                "article_id": article_id,
                "extraction_data": extraction_data,
                "model_used": "gpt-4o",
                "prompt_version": "v1",
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
            else:
                logger.error(f"Failed to save extraction result for article {article_id}")
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
        """
        Add extraction result to review queue.

        Args:
            article_id: Source article ID
            extraction_result_id: Extraction result ID
            extraction_data: Extracted data
            confidence: Confidence score
            reason: Reason for flagging

        Returns:
            review_queue_id or None if failed
        """
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
            else:
                logger.error(f"Failed to add to review queue for article {article_id}")
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
        """
        Log API costs.

        Args:
            article_id: Associated article ID
            extraction_result_id: Associated extraction result ID
            model: Model used
            input_tokens: Input token count
            output_tokens: Output token count

        Returns:
            True if successful
        """
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

            response = self.supabase.table("pipeline_costs").insert(cost_data).execute()

            if response.data:
                logger.debug(f"Logged cost: ${cost_usd:.6f} for extraction")
                return True
            else:
                logger.warning("Failed to log cost")
                return False

        except Exception as e:
            logger.error(f"Error logging cost: {str(e)}", exc_info=True)
            return False

    def approve_review_item(self, review_id: str) -> bool:
        """
        Approve a review queue item and insert associated data.

        Args:
            review_id: Review queue item ID

        Returns:
            True if successful
        """
        try:
            # Get review item
            response = self.supabase.table("review_queue").select("*").eq("id", review_id).execute()

            if not response.data:
                logger.error(f"Review item not found: {review_id}")
                return False

            review_item = response.data[0]
            extraction_data = review_item.get("extracted_data", {})
            article_id = review_item.get("article_id")
            confidence = review_item.get("confidence_score", 0.5)

            # Insert company if not already linked
            company_id = review_item.get("linked_company_id")
            if not company_id:
                company_id = self.upsert_company(extraction_data, article_id, confidence)

            # Insert event if not already linked
            event_id = review_item.get("linked_event_id")
            if not event_id and company_id:
                event_id = self.insert_event(extraction_data, company_id, review_item.get("article_id"), confidence)

            # Update review status
            update_data = {
                "status": "approved",
                "reviewed_at": datetime.utcnow().isoformat(),
                "linked_company_id": company_id,
                "linked_event_id": event_id,
            }

            self.supabase.table("review_queue").update(update_data).eq("id", review_id).execute()

            logger.info(f"Approved review item: {review_id}")
            return True

        except Exception as e:
            logger.error(f"Error approving review item: {str(e)}", exc_info=True)
            return False

    def reject_review_item(self, review_id: str, notes: Optional[str] = None) -> bool:
        """
        Reject a review queue item.

        Args:
            review_id: Review queue item ID
            notes: Optional rejection notes

        Returns:
            True if successful
        """
        try:
            update_data = {
                "status": "rejected",
                "reviewed_at": datetime.utcnow().isoformat(),
                "reviewer_notes": notes,
            }

            response = self.supabase.table("review_queue").update(update_data).eq("id", review_id).execute()

            if response.data:
                logger.info(f"Rejected review item: {review_id}")
                return True
            else:
                logger.error(f"Failed to reject review item: {review_id}")
                return False

        except Exception as e:
            logger.error(f"Error rejecting review item: {str(e)}", exc_info=True)
            return False

    def _get_sector_id(self, sector_name: Optional[str]) -> Optional[str]:
        """
        Get sector_id from sector name.

        Args:
            sector_name: Sector name to look up

        Returns:
            sector_id or None if not found
        """
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

            if response.data:
                return response.data[0]["sector_id"]

            return None

        except Exception as e:
            logger.error(f"Error getting sector_id for '{sector_name}': {str(e)}")
            return None

    def _normalize_event_type(self, event_type: str) -> str:
        """
        Normalize event_type string to database enum value.

        Args:
            event_type: Lowercase event type string

        Returns:
            Normalized event type with proper capitalization
        """
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
            "other": "Other",
        }

        normalized = mapping.get(event_type.lower(), "Other")
        return normalized
