"""
MIIM — Review Queue Helper Functions

Database queries and actions for the human-in-the-loop review interface.
"""

import logging
from datetime import datetime, timedelta
from typing import Optional

logger = logging.getLogger("miim.review")


def load_review_items(supabase_client, status: str = "pending", limit: int = 50) -> list[dict]:
    """Load review queue items with article details."""
    try:
        response = (
            supabase_client.table("review_queue")
            .select("*, articles!review_queue_article_id_fkey(title, source_url, source_name, article_text, published_date)")
            .eq("status", status)
            .order("created_at", desc=True)
            .limit(limit)
            .execute()
        )
        items = []
        for row in response.data or []:
            article = row.get("articles", {}) or {}
            items.append({
                "id": row["id"],
                "article_id": row["article_id"],
                "extraction_result_id": row.get("extraction_result_id"),
                "extracted_data": row["extracted_data"],
                "confidence_score": row["confidence_score"],
                "reason_flagged": row.get("reason_flagged", "low_confidence"),
                "status": row["status"],
                "created_at": row["created_at"],
                "article_title": article.get("title", "Unknown"),
                "source_url": article.get("source_url", ""),
                "source_name": article.get("source_name", "unknown"),
                "article_text": article.get("article_text", ""),
                "published_date": article.get("published_date", ""),
            })
        return items
    except Exception as e:
        logger.error(f"Failed to load review items: {e}")
        return []


def get_review_stats(supabase_client, days: int = 7) -> dict:
    """Get review queue statistics."""
    try:
        pending = supabase_client.table("review_queue").select("id", count="exact").eq("status", "pending").execute()
        cutoff = (datetime.utcnow() - timedelta(days=days)).isoformat()
        approved = (
            supabase_client.table("review_queue")
            .select("id", count="exact")
            .eq("status", "approved")
            .gte("reviewed_at", cutoff)
            .execute()
        )
        rejected = (
            supabase_client.table("review_queue")
            .select("id", count="exact")
            .eq("status", "rejected")
            .gte("reviewed_at", cutoff)
            .execute()
        )
        avg_resp = (
            supabase_client.table("review_queue")
            .select("confidence_score")
            .eq("status", "pending")
            .execute()
        )
        scores = [r["confidence_score"] for r in (avg_resp.data or []) if r.get("confidence_score")]
        avg_confidence = sum(scores) / len(scores) if scores else 0.0

        return {
            "pending": pending.count or 0,
            "approved_7d": approved.count or 0,
            "rejected_7d": rejected.count or 0,
            "avg_confidence": avg_confidence,
        }
    except Exception as e:
        logger.error(f"Failed to get review stats: {e}")
        return {"pending": 0, "approved_7d": 0, "rejected_7d": 0, "avg_confidence": 0.0}


def approve_item(supabase_client, review_id: str, edited_data: Optional[dict] = None) -> bool:
    """Approve a review queue item and insert into main tables."""
    try:
        # Get the review item
        item = supabase_client.table("review_queue").select("*").eq("id", review_id).single().execute()
        if not item.data:
            return False

        data = edited_data or item.data["extracted_data"]
        status = "edited" if edited_data else "approved"

        # Find or create the company
        company_name = data.get("company_name")
        if not company_name:
            logger.warning(f"No company_name in review item {review_id}")
            return False

        # Check if company exists
        existing = (
            supabase_client.table("companies")
            .select("company_id")
            .ilike("company_name", f"%{company_name}%")
            .execute()
        )

        company_id = None
        if existing.data:
            company_id = existing.data[0]["company_id"]
        else:
            # Map sector name to sector_id
            sector_id = None
            sector_name = data.get("sector")
            if sector_name:
                sector_resp = (
                    supabase_client.table("sectors")
                    .select("sector_id")
                    .ilike("sector_name", f"%{sector_name}%")
                    .execute()
                )
                if sector_resp.data:
                    sector_id = sector_resp.data[0]["sector_id"]

            new_company = {
                "company_name": company_name,
                "sector_id": sector_id,
                "sub_sector": data.get("sub_sector"),
                "headquarters_city": data.get("city"),
                "ownership_type": "Unknown",
                "tier_level": "Unknown",
                "data_confidence": item.data["confidence_score"],
            }
            resp = supabase_client.table("companies").insert(new_company).execute()
            if resp.data:
                company_id = resp.data[0]["company_id"]

        # Insert event
        event_id = None
        if company_id:
            event_type_map = {
                "new_factory": "New Factory",
                "partnership": "Partnership",
                "investment": "Investment",
                "acquisition": "Acquisition",
                "export_milestone": "Export Milestone",
                "other": "Other",
            }
            event = {
                "company_id": company_id,
                "event_type": event_type_map.get(data.get("event_type", "other"), "Other"),
                "title": data.get("source_summary", f"Event: {company_name}")[:200],
                "description": data.get("source_summary"),
                "city": data.get("city"),
                "investment_amount_mad": data.get("investment_amount_mad"),
                "source_url": item.data.get("articles", {}).get("source_url") if isinstance(item.data.get("articles"), dict) else None,
                "source_summary": data.get("source_summary"),
                "confidence_score": item.data["confidence_score"],
            }
            event_resp = supabase_client.table("events").insert(event).execute()
            if event_resp.data:
                event_id = event_resp.data[0]["event_id"]

        # Update review queue status
        supabase_client.table("review_queue").update({
            "status": status,
            "reviewed_at": datetime.utcnow().isoformat(),
            "linked_company_id": company_id,
            "linked_event_id": event_id,
        }).eq("id", review_id).execute()

        # Update article status
        supabase_client.table("articles").update({
            "processing_status": "reviewed"
        }).eq("id", item.data["article_id"]).execute()

        return True

    except Exception as e:
        logger.error(f"Failed to approve review item {review_id}: {e}")
        return False


def reject_item(supabase_client, review_id: str, notes: str = "") -> bool:
    """Reject a review queue item."""
    try:
        supabase_client.table("review_queue").update({
            "status": "rejected",
            "reviewer_notes": notes,
            "reviewed_at": datetime.utcnow().isoformat(),
        }).eq("id", review_id).execute()

        # Get article_id to update status
        item = supabase_client.table("review_queue").select("article_id").eq("id", review_id).single().execute()
        if item.data:
            supabase_client.table("articles").update({
                "processing_status": "skipped"
            }).eq("id", item.data["article_id"]).execute()

        return True
    except Exception as e:
        logger.error(f"Failed to reject review item {review_id}: {e}")
        return False


def get_pipeline_stats(supabase_client) -> dict:
    """Get overall pipeline statistics for the dashboard."""
    try:
        articles = supabase_client.table("articles").select("id", count="exact").execute()
        pending = supabase_client.table("articles").select("id", count="exact").eq("processing_status", "pending").execute()
        extracted = supabase_client.table("articles").select("id", count="exact").eq("processing_status", "extracted").execute()

        costs = supabase_client.table("pipeline_costs").select("cost_usd").execute()
        total_cost = sum(r["cost_usd"] for r in (costs.data or []) if r.get("cost_usd"))

        runs = (
            supabase_client.table("scraper_runs")
            .select("*")
            .order("run_date", desc=True)
            .limit(5)
            .execute()
        )

        return {
            "total_articles": articles.count or 0,
            "pending_extraction": pending.count or 0,
            "extracted": extracted.count or 0,
            "total_cost_usd": round(total_cost, 4),
            "recent_runs": runs.data or [],
        }
    except Exception as e:
        logger.error(f"Failed to get pipeline stats: {e}")
        return {
            "total_articles": 0,
            "pending_extraction": 0,
            "extracted": 0,
            "total_cost_usd": 0.0,
            "recent_runs": [],
        }
