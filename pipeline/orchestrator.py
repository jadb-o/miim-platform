"""Pipeline orchestrator for end-to-end data processing."""
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
from scrapers.medias24_scraper import Medias24Scraper

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


class PipelineOrchestrator:
    """Orchestrate the full MIIM data extraction pipeline."""

    def __init__(self, supabase_client: Client, openai_api_key: str):
        """
        Initialize PipelineOrchestrator.

        Args:
            supabase_client: Supabase client instance
            openai_api_key: OpenAI API key for GPT-4o
        """
        self.supabase = supabase_client
        self.openai_api_key = openai_api_key
        self.db_writer = DatabaseWriter(supabase_client)
        logger.info("Initialized PipelineOrchestrator")

    def run_scrapers(self) -> Dict[str, Any]:
        """
        Run all scrapers and collect articles.

        Returns:
            Dictionary with aggregated statistics
        """
        logger.info("Starting scraper phase")
        results = {
            "total_found": 0,
            "total_new": 0,
            "total_duplicates": 0,
            "scrapers": [],
        }

        scrapers = [
            Medias24Scraper(self.supabase),
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
        Process pending articles through extraction pipeline.

        Args:
            limit: Maximum number of articles to process

        Returns:
            Dictionary with processing statistics
        """
        logger.info(f"Starting extraction phase (limit={limit})")

        results = {
            "processed": 0,
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

                    logger.debug(f"Extracting article: {article_id}")

                    # Extract company data
                    start_time = time.time()
                    extraction_data = extract_company_data(article_text, api_key=self.openai_api_key)
                    processing_time_ms = int((time.time() - start_time) * 1000)

                    if not extraction_data:
                        logger.warning(f"Extraction failed for article {article_id}")
                        self._update_article_status(article_id, "failed", "Extraction returned no data")
                        results["failed"] += 1
                        continue

                    confidence = extraction_data.get("confidence_score", 0.0)
                    input_tokens = extraction_data.get("input_tokens", 0)
                    output_tokens = extraction_data.get("output_tokens", 0)

                    # Save extraction result
                    result_id = self.db_writer.save_extraction_result(
                        article_id,
                        extraction_data,
                        confidence,
                        input_tokens,
                        output_tokens,
                        processing_time_ms,
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
                        # Auto-approve: insert to database
                        company_id = self.db_writer.upsert_company(extraction_data, article_id, confidence)

                        if company_id:
                            self.db_writer.insert_event(
                                extraction_data, company_id, article.get("source_url"), confidence
                            )
                            self.db_writer.insert_partnerships(
                                extraction_data, company_id, article.get("source_url"), confidence
                            )
                            self._update_article_status(article_id, "extracted")
                            results["approved"] += 1
                            logger.info(f"Auto-approved extraction: {article_id} (confidence={confidence:.2f})")
                        else:
                            self._update_article_status(article_id, "failed", "Failed to upsert company")
                            results["failed"] += 1

                    elif confidence >= threshold_review:
                        # Send to review queue
                        review_id = self.db_writer.add_to_review_queue(
                            article_id, result_id, extraction_data, confidence, "Confidence score between thresholds"
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
                        self._update_article_status(article_id, "skipped", "Confidence score below minimum threshold")
                        logger.info(f"Skipped extraction: {article_id} (confidence={confidence:.2f})")

                    results["processed"] += 1

                except Exception as e:
                    logger.error(f"Error processing article {article_id}: {str(e)}", exc_info=True)
                    self._update_article_status(article_id, "failed", f"Processing error: {str(e)}")
                    results["failed"] += 1

        except Exception as e:
            logger.error(f"Error in extraction queue processing: {str(e)}", exc_info=True)

        logger.info(
            f"Extraction phase complete: processed={results['processed']}, "
            f"approved={results['approved']}, review_queue={results['review_queue']}, "
            f"failed={results['failed']}, cost=${results['total_cost_usd']:.4f}"
        )

        return results

    def run_full_pipeline(self, scrape: bool = True, extract: bool = True, limit: int = 50) -> Dict[str, Any]:
        """
        Run the complete pipeline: scrape and extract.

        Args:
            scrape: Whether to run scrapers
            extract: Whether to run extraction
            limit: Maximum articles to extract

        Returns:
            Full pipeline results
        """
        logger.info("Starting full pipeline execution")
        pipeline_results = {}

        try:
            if scrape:
                scraper_results = self.run_scrapers()
                pipeline_results["scraping"] = scraper_results
            else:
                logger.info("Skipping scraper phase (scrape=False)")

            if extract:
                extraction_results = self.process_extraction_queue(limit=limit)
                pipeline_results["extraction"] = extraction_results
            else:
                logger.info("Skipping extraction phase (extract=False)")

            pipeline_results["status"] = "success"
            logger.info("Full pipeline execution complete")

        except Exception as e:
            logger.error(f"Pipeline execution failed: {str(e)}", exc_info=True)
            pipeline_results["status"] = "error"
            pipeline_results["error"] = str(e)

        return pipeline_results

    def _update_article_status(self, article_id: str, status: str, error_message: Optional[str] = None) -> None:
        """
        Update article processing status.

        Args:
            article_id: Article ID
            status: New status (pending, extracted, reviewed, failed, skipped)
            error_message: Optional error message
        """
        try:
            update_data = {
                "processing_status": status,
                "updated_at": time.time(),
            }

            if error_message:
                update_data["error_message"] = error_message

            self.supabase.table("articles").update(update_data).eq("id", article_id).execute()

        except Exception as e:
            logger.error(f"Error updating article status: {str(e)}")


def main():
    """CLI entry point for pipeline orchestrator."""
    parser = argparse.ArgumentParser(description="MIIM Pipeline Orchestrator")
    parser.add_argument(
        "--scrape",
        action="store_true",
        default=False,
        help="Run scrapers",
    )
    parser.add_argument(
        "--extract",
        action="store_true",
        default=False,
        help="Run extraction",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=50,
        help="Limit for extraction queue",
    )
    parser.add_argument(
        "--full",
        action="store_true",
        default=False,
        help="Run full pipeline (scrape + extract)",
    )

    args = parser.parse_args()

    # Determine what to run
    scrape = args.full or args.scrape
    extract = args.full or args.extract

    if not scrape and not extract:
        parser.print_help()
        return

    # Initialize Supabase client
    supabase = create_client(SUPABASE_URL, SUPABASE_ANON_KEY)

    # Get OpenAI API key from environment
    import os
    openai_api_key = os.environ.get("OPENAI_API_KEY")

    if not openai_api_key:
        logger.error("OPENAI_API_KEY environment variable not set")
        return

    # Run pipeline
    orchestrator = PipelineOrchestrator(supabase, openai_api_key)
    results = orchestrator.run_full_pipeline(scrape=scrape, extract=extract, limit=args.limit)

    # Print results
    logger.info(f"Pipeline results: {results}")


if __name__ == "__main__":
    main()
