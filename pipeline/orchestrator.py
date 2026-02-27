"""Pipeline orchestrator for end-to-end data processing (v2 — multi-entity)."""
import argparse
import logging
import time
from typing import Any, Dict, List, Optional

from supabase import Client, create_client

from config.settings import (
    CONFIDENCE_THRESHOLDS,
    SUPABASE_ANON_KEY,
    SUPABASE_URL,
)
from extraction.extract_company_data import extract_company_data
from pipeline.database_writer import DatabaseWriter
from scrapers.leseco_scraper import LesecoScraper
from scrapers.mcinet_scraper import McinetScraper
from scrapers.challenge_scraper import ChallengeScraper

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


class PipelineOrchestrator:
    """Orchestrate the full MIIM data extraction pipeline (v2)."""

    def __init__(self, supabase_client: Client, openai_api_key: str):
        self.supabase = supabase_client
        self.openai_api_key = openai_api_key
        self.db_writer = DatabaseWriter(supabase_client)
        logger.info("Initialized PipelineOrchestrator v2")

    def run_scrapers(self) -> Dict[str, Any]:
        """Run all scrapers and collect articles."""
        logger.info("Starting scraper phase")
        results = {
            "total_found": 0,
            "total_new": 0,
            "total_duplicates": 0,
            "scrapers": [],
        }

        scrapers = [
            ChallengeScraper(self.supabase),
            LesecoScraper(self.supabase),
            McinetScraper(self.supabase),
        ]

        for scraper in scrapers:
            try:
                scraper_result = scraper.run()
                results["scrapers"].append(scraper_result)
                results["total_found"] += scraper_result.get("found", 0)
                results["total_new"] += scraper_result.get("new", 0)
                results["total_duplicates"] += scraper_result.get("duplicates", 0)

                logger.info(
                    f"Scraper {scraper_result['source']}: "
                    f"found={scraper_result.get('found', 0)}, "
                    f"new={scraper_result.get('new', 0)}"
                )
            except Exception as e:
                logger.error(f"Error running scraper {scraper.source_name}: {str(e)}", exc_info=True)

        logger.info(
            f"Scraper phase complete: found={results['total_found']}, "
            f"new={results['total_new']}, duplicates={results['total_duplicates']}"
        )
        return results

    def process_extraction_queue(self, limit: int = 50) -> Dict[str, Any]:
        """
        Process pending articles through v2 extraction pipeline.
        Now handles multiple entities and relationships per article.
        """
        logger.info(f"Starting extraction phase v2 (limit={limit})")

        results = {
            "processed": 0,
            "entities_created": 0,
            "relationships_created": 0,
            "approved": 0,
            "review_queue": 0,
            "failed": 0,
            "total_cost_usd": 0.0,
        }

        try:
            # Get pending articles
            response = (
                self.supabase.table("articles")
                .select("*")
                .eq("processing_status", "pending")
                .limit(limit)
                .execute()
            )

            articles = response.data or []
            logger.info(f"Found {len(articles)} pending articles for extraction")

            for article in articles:
                try:
                    article_id = article.get("id")
                    article_text = article.get("article_text", "")
                    article_url = article.get("source_url", "")

                    logger.debug(f"Extracting article: {article_id}")

                    # Extract (v2: returns entities + relationships)
                    start_time = time.time()
                    extraction = extract_company_data(article_text, api_key=self.openai_api_key)
                    processing_time_ms = int((time.time() - start_time) * 1000)

                    if not extraction or not extraction.get("entities"):
                        logger.warning(f"Extraction returned no entities for article {article_id}")
                        self._update_article_status(article_id, "failed", "No entities extracted")
                        results["failed"] += 1
                        continue

                    confidence = extraction.get("overall_confidence", 0.0)
                    input_tokens = extraction.get("input_tokens", 0)
                    output_tokens = extraction.get("output_tokens", 0)

                    # Save raw extraction result
                    result_id = self.db_writer.save_extraction_result(
                        article_id, extraction, confidence,
                        input_tokens, output_tokens, processing_time_ms,
                    )

                    if not result_id:
                        self._update_article_status(article_id, "failed", "Failed to save extraction result")
                        results["failed"] += 1
                        continue

                    # Log cost
                    self.db_writer.log_cost(article_id, result_id, "gpt-4o", input_tokens, output_tokens)
                    cost = (input_tokens * 2.50 / 1_000_000) + (output_tokens * 10.00 / 1_000_000)
                    results["total_cost_usd"] += cost

                    # Route based on confidence
                    threshold_auto_approve = CONFIDENCE_THRESHOLDS.get("auto_approve", 0.85)
                    threshold_review = CONFIDENCE_THRESHOLDS.get("review_queue", 0.65)

                    if confidence >= threshold_auto_approve:
                        # Auto-approve: process all entities and relationships
                        company_name_to_id = {}

                        for entity in extraction["entities"]:
                            entity_name = (entity.get("company_name") or "").strip()
                            if not entity_name:
                                continue

                            company_id = self.db_writer.upsert_company(entity, article_id, entity.get("confidence_score", confidence))

                            if company_id:
                                company_name_to_id[entity_name] = company_id
                                results["entities_created"] += 1

                                # Only create events for primary subjects
                                if entity.get("mention_type") == "primary_subject":
                                    self.db_writer.insert_event(entity, company_id, article_url, entity.get("confidence_score", confidence))

                        # Insert relationships
                        if extraction.get("relationships"):
                            rel_count = self.db_writer.insert_relationships(
                                extraction["relationships"], company_name_to_id, article_url, confidence
                            )
                            results["relationships_created"] += rel_count

                        self._update_article_status(article_id, "extracted")
                        results["approved"] += 1
                        logger.info(
                            f"Auto-approved: {article_id} "
                            f"(entities={len(extraction['entities'])}, "
                            f"relationships={len(extraction.get('relationships', []))})"
                        )

                    elif confidence >= threshold_review:
                        # Send to review queue
                        review_id = self.db_writer.add_to_review_queue(
                            article_id, result_id, extraction, confidence,
                            "Confidence score between thresholds"
                        )
                        if review_id:
                            self._update_article_status(article_id, "extracted")
                            results["review_queue"] += 1
                            logger.info(f"Sent to review queue: {article_id} (confidence={confidence:.2f})")
                        else:
                            self._update_article_status(article_id, "failed", "Failed to add to review queue")
                            results["failed"] += 1
                    else:
                        # Low confidence: skip
                        self._update_article_status(article_id, "skipped", "Confidence below minimum threshold")
                        logger.info(f"Skipped: {article_id} (confidence={confidence:.2f})")

                    results["processed"] += 1

                except Exception as e:
                    logger.error(f"Error processing article {article.get('id')}: {str(e)}", exc_info=True)
                    self._update_article_status(article.get("id"), "failed", f"Processing error: {str(e)}")
                    results["failed"] += 1

        except Exception as e:
            logger.error(f"Error in extraction queue processing: {str(e)}", exc_info=True)

        logger.info(
            f"Extraction v2 complete: processed={results['processed']}, "
            f"entities={results['entities_created']}, "
            f"relationships={results['relationships_created']}, "
            f"approved={results['approved']}, review_queue={results['review_queue']}, "
            f"failed={results['failed']}, cost=${results['total_cost_usd']:.4f}"
        )
        return results

    def run_full_pipeline(self, scrape: bool = True, extract: bool = True, limit: int = 50) -> Dict[str, Any]:
        """Run the complete pipeline: scrape and extract."""
        logger.info("Starting full pipeline execution (v2)")
        pipeline_results = {}

        try:
            if scrape:
                scraper_results = self.run_scrapers()
                pipeline_results["scraping"] = scraper_results

            if extract:
                extraction_results = self.process_extraction_queue(limit=limit)
                pipeline_results["extraction"] = extraction_results

            pipeline_results["status"] = "success"
            logger.info("Full pipeline execution complete")

        except Exception as e:
            logger.error(f"Pipeline execution failed: {str(e)}", exc_info=True)
            pipeline_results["status"] = "error"
            pipeline_results["error"] = str(e)

        return pipeline_results

    def _update_article_status(self, article_id: str, status: str, error_message: Optional[str] = None) -> None:
        try:
            update_data = {"processing_status": status}
            if error_message:
                update_data["error_message"] = error_message
            self.supabase.table("articles").update(update_data).eq("id", article_id).execute()
        except Exception as e:
            logger.error(f"Error updating article status: {str(e)}")


def main():
    """CLI entry point for pipeline orchestrator."""
    parser = argparse.ArgumentParser(description="MIIM Pipeline Orchestrator v2")
    parser.add_argument("--scrape", action="store_true", default=False, help="Run scrapers")
    parser.add_argument("--extract", action="store_true", default=False, help="Run extraction")
    parser.add_argument("--limit", type=int, default=50, help="Limit for extraction queue")
    parser.add_argument("--full", action="store_true", default=False, help="Run full pipeline")
    parser.add_argument(
        "--reprocess", action="store_true", default=False,
        help="Reset all articles to pending and re-extract with v2 prompt",
    )

    args = parser.parse_args()

    scrape = args.full or args.scrape
    extract = args.full or args.extract

    if not scrape and not extract and not args.reprocess:
        parser.print_help()
        return

    supabase = create_client(SUPABASE_URL, SUPABASE_ANON_KEY)

    import os
    openai_api_key = os.environ.get("OPENAI_API_KEY")
    if not openai_api_key:
        logger.error("OPENAI_API_KEY environment variable not set")
        return

    orchestrator = PipelineOrchestrator(supabase, openai_api_key)

    if args.reprocess:
        logger.info("Resetting all articles to pending for v2 re-extraction...")
        supabase.table("articles").update(
            {"processing_status": "pending", "error_message": None}
        ).neq("processing_status", "pending").execute()
        logger.info("All articles reset. Running extraction...")
        results = orchestrator.process_extraction_queue(limit=args.limit)
    else:
        results = orchestrator.run_full_pipeline(scrape=scrape, extract=extract, limit=args.limit)

    logger.info(f"Pipeline results: {results}")


if __name__ == "__main__":
    main()
