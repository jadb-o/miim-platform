"""
MIIM Admin Dashboard — Database CRUD Helpers
All direct Supabase operations for the admin interface.
"""

import logging
import pandas as pd
from datetime import datetime
from typing import Optional

logger = logging.getLogger("miim.admin")


# ─── Companies ────────────────────────────────────────────

def load_companies_admin(sb) -> pd.DataFrame:
    """Load all companies with sector names for admin table."""
    try:
        resp = (
            sb.table("companies")
            .select("*, sectors(sector_name)")
            .order("company_name")
            .execute()
        )
        rows = []
        for r in resp.data or []:
            sector = r.pop("sectors", None) or {}
            r["sector_name"] = sector.get("sector_name", "")
            rows.append(r)
        return pd.DataFrame(rows) if rows else pd.DataFrame()
    except Exception as e:
        logger.error(f"load_companies_admin: {e}")
        return pd.DataFrame()


def create_company(sb, data: dict) -> Optional[str]:
    """Insert a new company. Returns company_id or None."""
    try:
        resp = sb.table("companies").insert(data).execute()
        return resp.data[0]["company_id"] if resp.data else None
    except Exception as e:
        logger.error(f"create_company: {e}")
        return None


def update_company(sb, company_id: str, data: dict) -> bool:
    """Update a company by ID."""
    try:
        sb.table("companies").update(data).eq("company_id", company_id).execute()
        return True
    except Exception as e:
        logger.error(f"update_company: {e}")
        return False


def delete_company(sb, company_id: str) -> bool:
    """Delete a company by ID."""
    try:
        sb.table("companies").delete().eq("company_id", company_id).execute()
        return True
    except Exception as e:
        logger.error(f"delete_company: {e}")
        return False


# ─── Sectors ──────────────────────────────────────────────

def load_sectors_admin(sb) -> pd.DataFrame:
    try:
        resp = sb.table("sectors").select("*").order("sector_name").execute()
        return pd.DataFrame(resp.data) if resp.data else pd.DataFrame()
    except Exception as e:
        logger.error(f"load_sectors_admin: {e}")
        return pd.DataFrame()


def get_sector_options(sb) -> list[dict]:
    """Return list of {sector_id, sector_name} for dropdowns."""
    try:
        resp = sb.table("sectors").select("sector_id, sector_name").order("sector_name").execute()
        return resp.data or []
    except Exception as e:
        logger.error(f"get_sector_options: {e}")
        return []


def create_sector(sb, data: dict) -> Optional[str]:
    try:
        resp = sb.table("sectors").insert(data).execute()
        return resp.data[0]["sector_id"] if resp.data else None
    except Exception as e:
        logger.error(f"create_sector: {e}")
        return None


def update_sector(sb, sector_id: str, data: dict) -> bool:
    try:
        sb.table("sectors").update(data).eq("sector_id", sector_id).execute()
        return True
    except Exception as e:
        logger.error(f"update_sector: {e}")
        return False


def delete_sector(sb, sector_id: str) -> bool:
    try:
        sb.table("sectors").delete().eq("sector_id", sector_id).execute()
        return True
    except Exception as e:
        logger.error(f"delete_sector: {e}")
        return False


# ─── Relationships ────────────────────────────────────────

def load_relationships_admin(sb) -> pd.DataFrame:
    """Load relationships with company names resolved."""
    try:
        resp = (
            sb.table("company_relationships")
            .select("*, source:companies!company_relationships_source_company_id_fkey(company_name), target:companies!company_relationships_target_company_id_fkey(company_name)")
            .order("created_at", desc=True)
            .execute()
        )
        rows = []
        for r in resp.data or []:
            src = r.pop("source", None) or {}
            tgt = r.pop("target", None) or {}
            r["source_name"] = src.get("company_name", "?")
            r["target_name"] = tgt.get("company_name", "?")
            rows.append(r)
        return pd.DataFrame(rows) if rows else pd.DataFrame()
    except Exception as e:
        logger.error(f"load_relationships_admin: {e}")
        return pd.DataFrame()


def create_relationship(sb, data: dict) -> Optional[str]:
    try:
        resp = sb.table("company_relationships").insert(data).execute()
        return resp.data[0]["id"] if resp.data else None
    except Exception as e:
        logger.error(f"create_relationship: {e}")
        return None


def delete_relationship(sb, rel_id: str) -> bool:
    try:
        sb.table("company_relationships").delete().eq("id", rel_id).execute()
        return True
    except Exception as e:
        logger.error(f"delete_relationship: {e}")
        return False


# ─── People ───────────────────────────────────────────────

def load_people_admin(sb, company_id: str = None) -> pd.DataFrame:
    try:
        q = sb.table("company_people").select("*, companies(company_name)")
        if company_id:
            q = q.eq("company_id", company_id)
        resp = q.order("person_name").execute()
        rows = []
        for r in resp.data or []:
            co = r.pop("companies", None) or {}
            r["company_name"] = co.get("company_name", "")
            rows.append(r)
        return pd.DataFrame(rows) if rows else pd.DataFrame()
    except Exception as e:
        logger.error(f"load_people_admin: {e}")
        return pd.DataFrame()


def create_person(sb, data: dict) -> Optional[str]:
    try:
        resp = sb.table("company_people").insert(data).execute()
        return resp.data[0]["id"] if resp.data else None
    except Exception as e:
        logger.error(f"create_person: {e}")
        return None


def delete_person(sb, person_id: str) -> bool:
    try:
        sb.table("company_people").delete().eq("id", person_id).execute()
        return True
    except Exception as e:
        logger.error(f"delete_person: {e}")
        return False


# ─── Events ───────────────────────────────────────────────

def load_events_admin(sb) -> pd.DataFrame:
    try:
        resp = (
            sb.table("events")
            .select("*, companies(company_name)")
            .order("event_date", desc=True)
            .execute()
        )
        rows = []
        for r in resp.data or []:
            co = r.pop("companies", None) or {}
            r["company_name"] = co.get("company_name", "")
            rows.append(r)
        return pd.DataFrame(rows) if rows else pd.DataFrame()
    except Exception as e:
        logger.error(f"load_events_admin: {e}")
        return pd.DataFrame()


def create_event(sb, data: dict) -> Optional[str]:
    try:
        resp = sb.table("events").insert(data).execute()
        return resp.data[0]["event_id"] if resp.data else None
    except Exception as e:
        logger.error(f"create_event: {e}")
        return None


def update_event(sb, event_id: str, data: dict) -> bool:
    try:
        sb.table("events").update(data).eq("event_id", event_id).execute()
        return True
    except Exception as e:
        logger.error(f"update_event: {e}")
        return False


def delete_event(sb, event_id: str) -> bool:
    try:
        sb.table("events").delete().eq("event_id", event_id).execute()
        return True
    except Exception as e:
        logger.error(f"delete_event: {e}")
        return False


# ─── Articles ─────────────────────────────────────────────

def load_articles_admin(sb, source_filter: str = None, status_filter: str = None, limit: int = 200) -> pd.DataFrame:
    try:
        q = sb.table("articles").select("id, source_name, source_url, title, published_date, language, processing_status, scraped_date")
        if source_filter and source_filter != "All":
            q = q.eq("source_name", source_filter)
        if status_filter and status_filter != "All":
            q = q.eq("processing_status", status_filter)
        resp = q.order("scraped_date", desc=True).limit(limit).execute()
        return pd.DataFrame(resp.data) if resp.data else pd.DataFrame()
    except Exception as e:
        logger.error(f"load_articles_admin: {e}")
        return pd.DataFrame()


def get_article_text(sb, article_id: str) -> str:
    try:
        resp = sb.table("articles").select("article_text").eq("id", article_id).single().execute()
        return resp.data.get("article_text", "") if resp.data else ""
    except Exception as e:
        logger.error(f"get_article_text: {e}")
        return ""


def get_article_sources(sb) -> list[str]:
    """Get distinct source names."""
    try:
        resp = sb.table("articles").select("source_name").execute()
        return sorted(set(r["source_name"] for r in (resp.data or []) if r.get("source_name")))
    except Exception as e:
        logger.error(f"get_article_sources: {e}")
        return []


# ─── Extraction Results ───────────────────────────────────

def load_extractions_admin(sb, limit: int = 100) -> pd.DataFrame:
    try:
        resp = (
            sb.table("extraction_results")
            .select("id, article_id, model_used, prompt_version, confidence_score, input_tokens, output_tokens, processing_time_ms, created_at, articles(title, source_name)")
            .order("created_at", desc=True)
            .limit(limit)
            .execute()
        )
        rows = []
        for r in resp.data or []:
            art = r.pop("articles", None) or {}
            r["article_title"] = art.get("title", "")
            r["source_name"] = art.get("source_name", "")
            rows.append(r)
        return pd.DataFrame(rows) if rows else pd.DataFrame()
    except Exception as e:
        logger.error(f"load_extractions_admin: {e}")
        return pd.DataFrame()


def get_extraction_data(sb, extraction_id: str) -> dict:
    """Get the full extraction_data JSONB for one result."""
    try:
        resp = sb.table("extraction_results").select("extraction_data").eq("id", extraction_id).single().execute()
        return resp.data.get("extraction_data", {}) if resp.data else {}
    except Exception as e:
        logger.error(f"get_extraction_data: {e}")
        return {}


# ─── Review Queue ─────────────────────────────────────────

def load_review_queue_admin(sb, status: str = "pending", limit: int = 100) -> pd.DataFrame:
    try:
        resp = (
            sb.table("review_queue")
            .select("*, articles(title, source_name, source_url)")
            .eq("status", status)
            .order("created_at", desc=True)
            .limit(limit)
            .execute()
        )
        rows = []
        for r in resp.data or []:
            art = r.pop("articles", None) or {}
            r["article_title"] = art.get("title", "")
            r["source_name"] = art.get("source_name", "")
            r["article_url"] = art.get("source_url", "")
            rows.append(r)
        return pd.DataFrame(rows) if rows else pd.DataFrame()
    except Exception as e:
        logger.error(f"load_review_queue_admin: {e}")
        return pd.DataFrame()


def approve_review(sb, review_id: str) -> bool:
    """Quick approve — update status to approved."""
    try:
        sb.table("review_queue").update({
            "status": "approved",
            "reviewed_at": datetime.utcnow().isoformat(),
        }).eq("id", review_id).execute()
        return True
    except Exception as e:
        logger.error(f"approve_review: {e}")
        return False


def reject_review(sb, review_id: str) -> bool:
    """Quick reject — update status to rejected."""
    try:
        sb.table("review_queue").update({
            "status": "rejected",
            "reviewed_at": datetime.utcnow().isoformat(),
        }).eq("id", review_id).execute()
        return True
    except Exception as e:
        logger.error(f"reject_review: {e}")
        return False


# ─── Scraper Runs ─────────────────────────────────────────

def load_scraper_runs_admin(sb, limit: int = 50) -> pd.DataFrame:
    try:
        resp = (
            sb.table("scraper_runs")
            .select("*")
            .order("run_date", desc=True)
            .limit(limit)
            .execute()
        )
        return pd.DataFrame(resp.data) if resp.data else pd.DataFrame()
    except Exception as e:
        logger.error(f"load_scraper_runs_admin: {e}")
        return pd.DataFrame()


# ─── Pipeline Costs ───────────────────────────────────────

def load_pipeline_costs_admin(sb, limit: int = 100) -> pd.DataFrame:
    try:
        resp = (
            sb.table("pipeline_costs")
            .select("*")
            .order("logged_at", desc=True)
            .limit(limit)
            .execute()
        )
        return pd.DataFrame(resp.data) if resp.data else pd.DataFrame()
    except Exception as e:
        logger.error(f"load_pipeline_costs_admin: {e}")
        return pd.DataFrame()


def get_cost_summary(sb) -> dict:
    """Get total costs and token usage."""
    try:
        resp = sb.table("pipeline_costs").select("cost_usd, input_tokens, output_tokens").execute()
        data = resp.data or []
        total_cost = sum(r.get("cost_usd", 0) or 0 for r in data)
        total_input = sum(r.get("input_tokens", 0) or 0 for r in data)
        total_output = sum(r.get("output_tokens", 0) or 0 for r in data)
        return {
            "total_cost_usd": round(total_cost, 4),
            "total_input_tokens": total_input,
            "total_output_tokens": total_output,
            "total_calls": len(data),
        }
    except Exception as e:
        logger.error(f"get_cost_summary: {e}")
        return {"total_cost_usd": 0, "total_input_tokens": 0, "total_output_tokens": 0, "total_calls": 0}


# ─── Overview Stats ───────────────────────────────────────

def get_overview_stats(sb) -> dict:
    """Get all overview metrics in one call."""
    try:
        companies = sb.table("companies").select("company_id", count="exact").execute()
        articles = sb.table("articles").select("id", count="exact").execute()
        pending = sb.table("review_queue").select("id", count="exact").eq("status", "pending").execute()
        costs = get_cost_summary(sb)
        return {
            "total_companies": companies.count or 0,
            "total_articles": articles.count or 0,
            "pending_reviews": pending.count or 0,
            "total_cost_usd": costs["total_cost_usd"],
        }
    except Exception as e:
        logger.error(f"get_overview_stats: {e}")
        return {"total_companies": 0, "total_articles": 0, "pending_reviews": 0, "total_cost_usd": 0}


# ─── Company Options Helper ──────────────────────────────

def get_company_options(sb) -> list[dict]:
    """Return list of {company_id, company_name} for dropdowns."""
    try:
        resp = sb.table("companies").select("company_id, company_name").order("company_name").execute()
        return resp.data or []
    except Exception as e:
        logger.error(f"get_company_options: {e}")
        return []
