"""
MIIM — Morocco Industry Intelligence Monitor
LLM-Powered Extraction Pipeline

This module takes raw French/Arabic news text about Moroccan industry
and returns structured JSON using the OpenAI GPT-4o API.

Usage:
    from extraction.extract_company_data import extract_company_data

    result = extract_company_data(article_text)
    print(result["company_name"], result["confidence_score"])

Environment:
    Requires OPENAI_API_KEY in environment or .env file.
"""

import json
import os
import time
import logging
from typing import Any

from openai import OpenAI, APIError, APIConnectionError, RateLimitError

# ── Logging ──────────────────────────────────────────────
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("miim.extraction")

# ── Constants ────────────────────────────────────────────
MODEL = "gpt-4o"
MAX_RETRIES = 3
RETRY_BACKOFF_BASE = 2  # seconds — exponential: 2, 4, 8

VALID_EVENT_TYPES = [
    "new_factory",
    "partnership",
    "investment",
    "acquisition",
    "export_milestone",
    "other",
]

SYSTEM_PROMPT = """You are a structured data extraction engine for the Morocco Industry Intelligence Monitor (MIIM).

Your task is to read a news article (in French, Arabic, or Darija) about Moroccan industry and extract structured data as a JSON object. You MUST return ONLY valid JSON — no markdown, no commentary, no explanation.

Return this exact JSON structure:
{
  "company_name": "Primary company mentioned (string or null)",
  "sector": "Industry sector (string or null). Use one of: Automotive, Aerospace, Agrifood, Textiles & Leather, Electronics, Pharmaceuticals, Renewable Energy, Mining & Phosphates, Construction Materials, Other",
  "sub_sector": "More specific classification (string or null), e.g. Wiring Harnesses, Vehicle Assembly, Metal Stamping",
  "event_type": "One of: new_factory, partnership, investment, acquisition, export_milestone, other",
  "partner_companies": ["List of other companies mentioned as partners, clients, or suppliers"],
  "investment_amount_mad": null or number in Moroccan Dirhams. Convert from EUR (multiply by ~11) or USD (multiply by ~10) if needed. Set null if not mentioned,
  "city": "Moroccan city where the event takes place (string or null)",
  "source_summary": "One-sentence summary of the article in English",
  "confidence_score": 0.0 to 1.0
}

CONFIDENCE RULES:
- Set confidence_score BELOW 0.7 if ANY field is uncertain or inferred rather than explicitly stated.
- Set confidence_score BELOW 0.5 if the article is not clearly about Moroccan industry.
- Set confidence_score ABOVE 0.85 only if ALL fields are directly stated in the text.

LANGUAGE HANDLING:
- The article may be in French, Modern Standard Arabic, or Moroccan Darija.
- Always output field VALUES in English, except for proper nouns (company names, city names) which should remain in their original form.
- If a company name appears in Arabic, also provide the Latin transliteration if recognizable.

OUTPUT: Return ONLY the JSON object. No other text."""


# ── Schema validation ────────────────────────────────────

def _validate_extracted_data(data: dict) -> dict:
    """
    Validates and normalizes the extracted data against the MIIM schema.
    Fills missing fields with None and clamps confidence_score to [0, 1].
    """
    schema_defaults = {
        "company_name": None,
        "sector": None,
        "sub_sector": None,
        "event_type": "other",
        "partner_companies": [],
        "investment_amount_mad": None,
        "city": None,
        "source_summary": None,
        "confidence_score": 0.5,
    }

    validated = {}
    for key, default in schema_defaults.items():
        validated[key] = data.get(key, default)

    # Normalize event_type
    if validated["event_type"] not in VALID_EVENT_TYPES:
        logger.warning(
            f"Invalid event_type '{validated['event_type']}' — defaulting to 'other'"
        )
        validated["event_type"] = "other"
        # Reduce confidence because the model misclassified
        validated["confidence_score"] = min(validated["confidence_score"], 0.6)

    # Ensure partner_companies is always a list
    if not isinstance(validated["partner_companies"], list):
        validated["partner_companies"] = []

    # Clamp confidence_score
    try:
        validated["confidence_score"] = max(0.0, min(1.0, float(validated["confidence_score"])))
    except (TypeError, ValueError):
        validated["confidence_score"] = 0.5

    # Round investment amount if present
    if validated["investment_amount_mad"] is not None:
        try:
            validated["investment_amount_mad"] = round(float(validated["investment_amount_mad"]), 2)
        except (TypeError, ValueError):
            validated["investment_amount_mad"] = None
            validated["confidence_score"] = min(validated["confidence_score"], 0.6)

    return validated


# ── Core extraction function ─────────────────────────────

def extract_company_data(
    article_text: str,
    *,
    api_key: str | None = None,
    model: str = MODEL,
    max_retries: int = MAX_RETRIES,
) -> dict[str, Any]:
    """
    Extract structured company/industry data from a French or Arabic
    news article about Moroccan industry.

    Args:
        article_text: Raw article text (French, Arabic, or Darija).
        api_key:      OpenAI API key. Falls back to OPENAI_API_KEY env var.
        model:        Model to use (default: gpt-4o).
        max_retries:  Number of retry attempts on transient failures.

    Returns:
        A validated dictionary with keys:
            company_name, sector, sub_sector, event_type,
            partner_companies, investment_amount_mad, city,
            source_summary, confidence_score

    Raises:
        ValueError: If article_text is empty or too short.
        RuntimeError: If all retry attempts are exhausted.
    """
    # ── Input validation ──
    if not article_text or not article_text.strip():
        raise ValueError("article_text cannot be empty.")

    cleaned_text = article_text.strip()
    if len(cleaned_text) < 20:
        raise ValueError(
            f"article_text is too short ({len(cleaned_text)} chars). "
            "Provide at least a few sentences for meaningful extraction."
        )

    # Truncate extremely long articles to stay within token limits
    MAX_CHARS = 12_000
    if len(cleaned_text) > MAX_CHARS:
        logger.warning(
            f"Article truncated from {len(cleaned_text)} to {MAX_CHARS} characters."
        )
        cleaned_text = cleaned_text[:MAX_CHARS]

    # ── API client ──
    resolved_key = api_key or os.environ.get("OPENAI_API_KEY")
    if not resolved_key:
        raise RuntimeError(
            "No OpenAI API key provided. Set the OPENAI_API_KEY environment "
            "variable or pass api_key= to extract_company_data()."
        )

    client = OpenAI(api_key=resolved_key)

    # ── Retry loop with exponential backoff ──
    last_exception = None

    for attempt in range(1, max_retries + 1):
        try:
            logger.info(f"Extraction attempt {attempt}/{max_retries} using {model}")

            response = client.chat.completions.create(
                model=model,
                temperature=0.1,  # Low temperature for deterministic extraction
                response_format={"type": "json_object"},
                messages=[
                    {"role": "system", "content": SYSTEM_PROMPT},
                    {"role": "user", "content": cleaned_text},
                ],
            )

            raw_content = response.choices[0].message.content
            logger.info(f"Received response ({len(raw_content)} chars)")

            # Parse JSON
            parsed = json.loads(raw_content)

            # Validate and normalize
            validated = _validate_extracted_data(parsed)

            logger.info(
                f"Extraction successful: company='{validated['company_name']}', "
                f"confidence={validated['confidence_score']}"
            )

            return validated

        except json.JSONDecodeError as e:
            logger.error(f"Attempt {attempt}: JSON parsing failed — {e}")
            last_exception = e

        except RateLimitError as e:
            wait_time = RETRY_BACKOFF_BASE ** attempt
            logger.warning(f"Attempt {attempt}: Rate limited. Waiting {wait_time}s...")
            time.sleep(wait_time)
            last_exception = e

        except APIConnectionError as e:
            wait_time = RETRY_BACKOFF_BASE ** attempt
            logger.warning(f"Attempt {attempt}: Connection error. Waiting {wait_time}s...")
            time.sleep(wait_time)
            last_exception = e

        except APIError as e:
            logger.error(f"Attempt {attempt}: OpenAI API error — {e}")
            last_exception = e

            # Don't retry on 4xx client errors (except 429 rate limit)
            if hasattr(e, "status_code") and 400 <= e.status_code < 500 and e.status_code != 429:
                break

    # All retries exhausted
    raise RuntimeError(
        f"Extraction failed after {max_retries} attempts. Last error: {last_exception}"
    )


# ── Convenience: batch extraction ────────────────────────

def extract_batch(
    articles: list[str],
    *,
    api_key: str | None = None,
    skip_errors: bool = True,
) -> list[dict[str, Any]]:
    """
    Extract data from multiple articles. Failed extractions are either
    skipped (skip_errors=True) or raise immediately.

    Returns a list of result dicts, each with an added '_source_index' field.
    """
    results = []
    for i, text in enumerate(articles):
        try:
            result = extract_company_data(text, api_key=api_key)
            result["_source_index"] = i
            results.append(result)
        except Exception as e:
            if skip_errors:
                logger.error(f"Article {i} failed: {e} — skipping.")
                results.append({
                    "_source_index": i,
                    "_error": str(e),
                    "confidence_score": 0.0,
                })
            else:
                raise
    return results


# ── CLI entry point ──────────────────────────────────────

if __name__ == "__main__":
    import sys

    sample = """
    Le groupe Renault a annoncé un investissement de 450 millions d'euros dans
    son usine de Tanger pour la production d'un nouveau modèle électrique. Cette
    expansion, réalisée en partenariat avec Yazaki Morocco pour le câblage et SNOP
    pour l'emboutissage, devrait créer 2 000 emplois supplémentaires d'ici 2027.
    L'usine de Tanger Automotive City produira 100 000 véhicules électriques par an.
    """

    if len(sys.argv) > 1:
        # Read article from file
        with open(sys.argv[1], "r", encoding="utf-8") as f:
            sample = f.read()

    try:
        result = extract_company_data(sample)
        print(json.dumps(result, indent=2, ensure_ascii=False))
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)
