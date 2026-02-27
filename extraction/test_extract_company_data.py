"""
MIIM — Unit Tests for the LLM Extraction Pipeline.

These tests use unittest.mock to patch the OpenAI API call, so they run
WITHOUT an API key and WITHOUT network access.  Each test provides a
realistic French article snippet and a simulated GPT-4o JSON response,
then asserts that the full pipeline (parsing, validation, normalization)
works correctly.

Run:
    pytest extraction/test_extract_company_data.py -v
"""

import json
import sys
import pytest
from unittest.mock import patch, MagicMock

from extraction.extract_company_data import (
    extract_company_data,
    _validate_extracted_data,
    VALID_EVENT_TYPES,
)

# The module and function share the same name, so we use sys.modules
# to get a reference to the actual MODULE for patching.
_extract_module = sys.modules["extraction.extract_company_data"]


# ─────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────

def _mock_openai_response(json_body: dict) -> MagicMock:
    """Build a fake OpenAI ChatCompletion response object."""
    message = MagicMock()
    message.content = json.dumps(json_body, ensure_ascii=False)

    choice = MagicMock()
    choice.message = message

    response = MagicMock()
    response.choices = [choice]
    return response


# ─────────────────────────────────────────────────────────
# Test 1 — Renault Tangier EV Investment (Classic case)
# ─────────────────────────────────────────────────────────

ARTICLE_RENAULT = """
Automobile : Renault investit 450 millions d'euros à Tanger pour les véhicules électriques

Le groupe Renault a annoncé mercredi un investissement massif de 450 millions
d'euros dans son complexe industriel de Tanger pour lancer la production d'un
nouveau modèle 100% électrique. L'usine, située dans la zone Tanger Automotive
City, produira 100 000 véhicules par an dès 2027. Le projet est réalisé en
partenariat avec Yazaki Morocco pour le câblage électrique et SNOP pour les
pièces d'emboutissage. Selon le ministère de l'Industrie, cet investissement
portera le taux d'intégration locale du site à 75%. Près de 2 000 emplois
directs seront créés.
"""

MOCK_RESPONSE_RENAULT = {
    "company_name": "Renault Group",
    "sector": "Automotive",
    "sub_sector": "Vehicle Assembly",
    "event_type": "investment",
    "partner_companies": ["Yazaki Morocco", "SNOP"],
    "investment_amount_mad": 4950000000,  # 450M EUR * ~11
    "city": "Tanger",
    "source_summary": "Renault is investing 450 million euros in its Tangier plant to produce 100,000 electric vehicles annually starting 2027, partnering with Yazaki and SNOP.",
    "confidence_score": 0.92,
}


@patch.object(_extract_module, "OpenAI")
def test_renault_tangier_ev_investment(mock_openai_cls):
    """
    Scenario: A well-structured French article about Renault's EV factory
    investment in Tangier with clear partners and amounts.
    Expected: High confidence, all fields populated correctly.
    """
    # Wire up the mock
    mock_client = MagicMock()
    mock_openai_cls.return_value = mock_client
    mock_client.chat.completions.create.return_value = _mock_openai_response(
        MOCK_RESPONSE_RENAULT
    )

    result = extract_company_data(ARTICLE_RENAULT, api_key="test-key-fake")

    # Core assertions
    assert result["company_name"] == "Renault Group"
    assert result["sector"] == "Automotive"
    assert result["event_type"] == "investment"
    assert result["event_type"] in VALID_EVENT_TYPES
    assert "Yazaki Morocco" in result["partner_companies"]
    assert "SNOP" in result["partner_companies"]
    assert result["investment_amount_mad"] == 4950000000
    assert result["city"] == "Tanger"
    assert result["confidence_score"] >= 0.85
    assert result["source_summary"] is not None and len(result["source_summary"]) > 10

    # Verify the API was called exactly once (no retries needed)
    mock_client.chat.completions.create.assert_called_once()


# ─────────────────────────────────────────────────────────
# Test 2 — Stellantis-Lear Partnership Announcement
# ─────────────────────────────────────────────────────────

ARTICLE_STELLANTIS = """
Kénitra : Stellantis renforce sa chaîne d'approvisionnement locale

Le constructeur automobile Stellantis a signé un accord de partenariat
stratégique avec l'équipementier américain Lear Corporation pour
l'approvisionnement en systèmes de sièges dans son usine de Kénitra. Selon
des sources proches du dossier, ce contrat pluriannuel vise à augmenter le
contenu local des véhicules produits à l'Atlantic Free Zone. Le montant de
l'accord n'a pas été divulgué. Cette initiative s'inscrit dans la stratégie
de Stellantis d'atteindre un taux d'intégration de 60% au Maroc d'ici 2026.
"""

MOCK_RESPONSE_STELLANTIS = {
    "company_name": "Stellantis",
    "sector": "Automotive",
    "sub_sector": "Vehicle Assembly",
    "event_type": "partnership",
    "partner_companies": ["Lear Corporation"],
    "investment_amount_mad": None,
    "city": "Kenitra",
    "source_summary": "Stellantis signed a multi-year partnership with Lear Corporation for seating systems supply at its Kenitra plant to increase local content.",
    "confidence_score": 0.88,
}


@patch.object(_extract_module, "OpenAI")
def test_stellantis_lear_partnership(mock_openai_cls):
    """
    Scenario: A partnership announcement between Stellantis and Lear.
    No investment amount is disclosed in the article.
    Expected: investment_amount_mad should be None; event_type is 'partnership'.
    """
    mock_client = MagicMock()
    mock_openai_cls.return_value = mock_client
    mock_client.chat.completions.create.return_value = _mock_openai_response(
        MOCK_RESPONSE_STELLANTIS
    )

    result = extract_company_data(ARTICLE_STELLANTIS, api_key="test-key-fake")

    assert result["company_name"] == "Stellantis"
    assert result["event_type"] == "partnership"
    assert result["investment_amount_mad"] is None  # Not disclosed
    assert "Lear Corporation" in result["partner_companies"]
    assert result["city"] == "Kenitra"
    assert result["confidence_score"] > 0.7


# ─────────────────────────────────────────────────────────
# Test 3 — Vague / Ambiguous Article (Low Confidence)
# ─────────────────────────────────────────────────────────

ARTICLE_VAGUE = """
Industrie : Le Maroc accélère sa stratégie d'intégration industrielle

Plusieurs acteurs du secteur automobile au Maroc travaillent actuellement sur
des projets d'expansion. Selon des observateurs du marché, de nouveaux
investissements sont attendus dans les zones franches de Tanger et Kénitra,
mais aucun détail officiel n'a encore été communiqué. Le ministère de
l'Industrie prévoit d'annoncer de nouvelles mesures incitatives au cours du
prochain trimestre pour renforcer l'attractivité du Royaume auprès des
équipementiers internationaux.
"""

MOCK_RESPONSE_VAGUE = {
    "company_name": None,
    "sector": "Automotive",
    "sub_sector": None,
    "event_type": "other",
    "partner_companies": [],
    "investment_amount_mad": None,
    "city": None,
    "source_summary": "Morocco's Ministry of Industry plans to announce new incentives to boost automotive local integration, though no specific company or investment details were shared.",
    "confidence_score": 0.45,
}


@patch.object(_extract_module, "OpenAI")
def test_vague_article_low_confidence(mock_openai_cls):
    """
    Scenario: A general sector overview article with no specific company,
    no investment amount, and no concrete event.
    Expected: Low confidence (<0.7), company_name is None, event_type is 'other'.
    """
    mock_client = MagicMock()
    mock_openai_cls.return_value = mock_client
    mock_client.chat.completions.create.return_value = _mock_openai_response(
        MOCK_RESPONSE_VAGUE
    )

    result = extract_company_data(ARTICLE_VAGUE, api_key="test-key-fake")

    assert result["company_name"] is None
    assert result["event_type"] == "other"
    assert result["partner_companies"] == []
    assert result["investment_amount_mad"] is None
    assert result["confidence_score"] < 0.7  # Below the quality gate


# ─────────────────────────────────────────────────────────
# Test 4 — Validation logic (no API call)
# ─────────────────────────────────────────────────────────

class TestValidation:
    """Unit tests for the _validate_extracted_data() helper."""

    def test_missing_fields_get_defaults(self):
        """Incomplete data should be filled with safe defaults."""
        raw = {"company_name": "TestCo"}
        result = _validate_extracted_data(raw)
        assert result["sector"] is None
        assert result["event_type"] == "other"
        assert result["partner_companies"] == []
        assert result["confidence_score"] == 0.5

    def test_invalid_event_type_normalized(self):
        """An unrecognized event_type should fall back to 'other'."""
        raw = {"event_type": "something_invalid", "confidence_score": 0.9}
        result = _validate_extracted_data(raw)
        assert result["event_type"] == "other"
        assert result["confidence_score"] <= 0.6  # Penalized

    def test_confidence_clamped_to_range(self):
        """Confidence scores outside [0, 1] should be clamped."""
        assert _validate_extracted_data({"confidence_score": 1.5})["confidence_score"] == 1.0
        assert _validate_extracted_data({"confidence_score": -0.3})["confidence_score"] == 0.0

    def test_investment_amount_rounded(self):
        """Investment amounts should be rounded to 2 decimal places."""
        raw = {"investment_amount_mad": 1234567.891}
        result = _validate_extracted_data(raw)
        assert result["investment_amount_mad"] == 1234567.89

    def test_partner_companies_not_list_normalized(self):
        """If partner_companies is not a list, it should become an empty list."""
        raw = {"partner_companies": "not a list"}
        result = _validate_extracted_data(raw)
        assert result["partner_companies"] == []


# ─────────────────────────────────────────────────────────
# Test 5 — Input validation (no API call)
# ─────────────────────────────────────────────────────────

class TestInputValidation:
    """Tests that bad inputs raise clear errors before any API call."""

    def test_empty_string_raises(self):
        with pytest.raises(ValueError, match="cannot be empty"):
            extract_company_data("", api_key="test-key-fake")

    def test_whitespace_only_raises(self):
        with pytest.raises(ValueError, match="cannot be empty"):
            extract_company_data("   \n\t  ", api_key="test-key-fake")

    def test_too_short_raises(self):
        with pytest.raises(ValueError, match="too short"):
            extract_company_data("Bonjour", api_key="test-key-fake")

    def test_no_api_key_raises(self):
        """Without an API key, the function should raise before calling OpenAI."""
        with patch.dict("os.environ", {}, clear=True):
            with pytest.raises(RuntimeError, match="No OpenAI API key"):
                extract_company_data(
                    "Un article assez long pour passer la validation de longueur minimale.",
                    api_key=None,
                )
