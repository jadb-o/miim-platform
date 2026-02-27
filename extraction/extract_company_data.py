"""
MIIM — Morocco Industry Intelligence Monitor
LLM-Powered Extraction Pipeline v2

Extracts comprehensive company profiles and relationships from
French/Arabic news articles about Moroccan industry.

Usage:
    from extraction.extract_company_data import extract_company_data

    result = extract_company_data(article_text)
    for entity in result["entities"]:
        print(entity["company_name"], entity["description"])

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
    "expansion",
    "hiring",
    "product_launch",
    "certification",
    "other",
]

VALID_RELATIONSHIP_TYPES = [
    "client",
    "supplier",
    "partner",
    "subsidiary",
    "parent",
    "investor",
    "joint_venture",
    "competitor",
]

SYSTEM_PROMPT = """You are a structured data extraction engine for the Morocco Industry Intelligence Monitor (MIIM).

Your task is to read a news article (in French, Arabic, or Darija) about Moroccan industry and extract ALL companies mentioned along with comprehensive profiles and relationships. Return ONLY valid JSON — no markdown, no commentary.

Return this exact JSON structure:
{
  "entities": [
    {
      "company_name": "Company legal or trade name (string, required)",
      "description": "What the company does — one or two sentences (string or null)",
      "activities": "Core business activities, products, or services (string or null)",
      "sector": "One of: Automotive, Aerospace, Agrifood, Textiles & Leather, Electronics, Pharmaceuticals, Renewable Energy, Mining & Phosphates, Construction Materials, Fishing & Seafood, Other",
      "sub_sector": "More specific classification (string or null)",
      "value_chain_position": "Position in industry value chain (string or null). Examples: Raw Materials, Tier 3 Components, Tier 2 Sub-Assembly, Tier 1 Systems, OEM Assembly, Distribution, Services",
      "event_type": "One of: new_factory, partnership, investment, acquisition, export_milestone, expansion, hiring, product_launch, certification, other",
      "city": "Moroccan city (string or null)",
      "investment_amount_mad": null or number in Moroccan Dirhams. Convert EUR (×11) or USD (×10),
      "employee_count": null or integer if mentioned,
      "revenue_mad": null or number if annual revenue mentioned (convert to MAD),
      "website_url": "Company website if mentioned (string or null)",
      "parent_company": "Parent or holding company name if mentioned (string or null)",
      "ownership_type": "One of: Moroccan Private, Foreign Private, State-Owned, Joint Venture, Multinational, Public (Listed), Unknown",
      "management_mentions": [
        {"name": "Person name", "role": "Job title or role"}
      ],
      "mention_type": "primary_subject or mentioned",
      "confidence_score": 0.0 to 1.0
    }
  ],
  "relationships": [
    {
      "source_company": "Company A name",
      "target_company": "Company B name",
      "relationship_type": "One of: client, supplier, partner, subsidiary, parent, investor, joint_venture, competitor",
      "description": "Brief description of the relationship"
    }
  ],
  "article_summary": "Two-sentence English summary of the article",
  "overall_confidence": 0.0 to 1.0
}

EXTRACTION RULES:
1. Extract ALL companies mentioned in the article, not just the primary one.
2. For each company, fill in as many fields as possible from the text. Leave null if not mentioned.
3. The "entities" array must have at least one entry. If no company is identifiable, set company_name to null.
4. Relationships should capture explicit connections: who supplies whom, who partnered with whom, parent-subsidiary, etc.
5. "mention_type" should be "primary_subject" for the main company(ies) the article is about, and "mentioned" for others.

CONFIDENCE RULES:
- Below 0.7: Any field is uncertain or inferred rather than explicitly stated.
- Below 0.5: Article is not clearly about Moroccan industry or companies.
- Above 0.85: ALL key fields (company_name, sector, event_type) are directly stated.

LANGUAGE HANDLING:
- Articles may be in French, Arabic, or Darija.
- Output all field VALUES in English, except proper nouns (company names, city names, person names) which remain in original form.
- If a company name appears in Arabic, also provide the Latin transliteration if recognizable.

OUTPUT: Return ONLY the JSON object. No other text."""


# ── Schema validation ────────────────────────────────────

def _validate_entity(data: dict) -> dict:
    """Validate and normalize a single extracted entity."""
    schema_defaults = {
        "company_name": None,
        "description": None,
        "activities": None,
        "sector": None,
        "sub_sector": None,
        "value_chain_position": None,
        "event_type": "other",
        "city": None,
        "investment_amount_mad": None,
        "employee_count": None,
        "revenue_mad": None,
        "website_url": None,
        "parent_company": None,
        "ownership_type": "Unknown",
        "management_mentions": [],
        "mention_type": "mentioned",
        "confidence_score": 0.5,
    }

    validated = {}
    for key, default in schema_defaults.items():
        validated[key] = data.get(key, default)

    # Normalize event_type
    if validated["event_type"] not in VALID_EVENT_TYPES:
        logger.warning(f"Invalid event_type '{validated['event_type']}' — defaulting to 'other'")
        validated["event_type"] = "other"
        validated["confidence_score"] = min(validated.get("confidence_score", 0.5), 0.6)

    # Ensure management_mentions is a list of dicts
    if not isinstance(validated["management_mentions"], list):
        validated["management_mentions"] = []
    validated["management_mentions"] = [
        m for m in validated["management_mentions"]
        if isinstance(m, dict) and m.get("name")
    ]

    # Clamp confidence_score
    try:
        validated["confidence_score"] = max(0.0, min(1.0, float(validated["confidence_score"])))
    except (TypeError, ValueError):
        validated["confidence_score"] = 0.5

    # Validate numeric fields
    for field in ["investment_amount_mad", "revenue_mad"]:
        if validated[field] is not None:
            try:
                validated[field] = round(float(validated[field]), 2)
            except (TypeError, ValueError):
                validated[field] = None

    if validated["employee_count"] is not None:
        try:
            validated["employee_count"] = int(validated["employee_count"])
        except (TypeError, ValueError):
            validated["employee_count"] = None

    # Normalize mention_type
    if validated["mention_type"] not in ("primary_subject", "mentioned", "quoted"):
        validated["mention_type"] = "mentioned"

    return validated


def _validate_relationship(data: dict) -> dict | None:
    """Validate a single relationship entry."""
    source = data.get("source_company", "").strip()
    target = data.get("target_company", "").strip()
    rel_type = data.get("relationship_type", "").strip().lower()

    if not source or not target:
        return None
    if source == target:
        return None
    if rel_type not in VALID_RELATIONSHIP_TYPES:
        rel_type = "partner"  # Default to partner

    return {
        "source_company": source,
        "target_company": target,
        "relationship_type": rel_type,
        "description": data.get("description", ""),
    }


def _validate_extracted_data(data: dict) -> dict:
    """Validate the full v2 extraction output."""
    result = {
        "entities": [],
        "relationships": [],
        "article_summary": data.get("article_summary") or data.get("source_summary", ""),
        "overall_confidence": 0.5,
    }

    # Handle v1 format (single entity, no "entities" array) for backward compatibility
    if "entities" not in data and "company_name" in data:
        # Convert v1 to v2 format
        data["entities"] = [data]
        data["relationships"] = []
        data["overall_confidence"] = data.get("confidence_score", 0.5)

    # Validate entities
    raw_entities = data.get("entities", [])
    if not isinstance(raw_entities, list):
        raw_entities = [raw_entities] if raw_entities else []

    for entity in raw_entities:
        if isinstance(entity, dict):
            validated_entity = _validate_entity(entity)
            result["entities"].append(validated_entity)

    # Validate relationships
    raw_relationships = data.get("relationships", [])
    if not isinstance(raw_relationships, list):
        raw_relationships = []

    for rel in raw_relationships:
        if isinstance(rel, dict):
            validated_rel = _validate_relationship(rel)
            if validated_rel:
                result["relationships"].append(validated_rel)

    # Compute overall confidence
    if result["entities"]:
        confidences = [e["confidence_score"] for e in result["entities"]]
        result["overall_confidence"] = round(sum(confidences) / len(confidences), 2)
    else:
        try:
            result["overall_confidence"] = max(0.0, min(1.0, float(data.get("overall_confidence", 0.5))))
        except (TypeError, ValueError):
            result["overall_confidence"] = 0.5

    return result


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

    Returns a v2 result with:
        entities: list of company profiles
        relationships: list of inter-company relationships
        article_summary: English summary
        overall_confidence: average confidence
        input_tokens, output_tokens: token usage
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
        logger.warning(f"Article truncated from {len(cleaned_text)} to {MAX_CHARS} characters.")
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
                temperature=0.1,
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

            # Attach token usage
            usage = response.usage
            validated["input_tokens"] = usage.prompt_tokens if usage else 0
            validated["output_tokens"] = usage.completion_tokens if usage else 0

            primary = next(
                (e for e in validated["entities"] if e.get("mention_type") == "primary_subject"),
                validated["entities"][0] if validated["entities"] else None,
            )
            company_name = primary["company_name"] if primary else "N/A"

            logger.info(
                f"Extraction successful: primary='{company_name}', "
                f"entities={len(validated['entities'])}, "
                f"relationships={len(validated['relationships'])}, "
                f"confidence={validated['overall_confidence']}"
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

            if hasattr(e, "status_code") and 400 <= e.status_code < 500 and e.status_code != 429:
                break

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
                    "overall_confidence": 0.0,
                    "entities": [],
                    "relationships": [],
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
        with open(sys.argv[1], "r", encoding="utf-8") as f:
            sample = f.read()

    try:
        result = extract_company_data(sample)
        print(json.dumps(result, indent=2, ensure_ascii=False))
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)
