"""MIIM Pipeline Configuration Settings."""
import os
from typing import List

# Supabase Configuration
SUPABASE_URL = os.environ.get("SUPABASE_URL", "https://rkqfjesnavbngtihffge.supabase.co")
SUPABASE_ANON_KEY = os.environ.get("SUPABASE_ANON_KEY", "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InJrcWZqZXNuYXZibmd0aWhmZmdlIiwicm9sZSI6ImFub24iLCJpYXQiOjE3Mzc0OTc0OTAsImV4cCI6MTc2OTAzMzQ5MH0.GpkJGVAKMo5NaPv2RL0AKcSKPCB2Zh0s1O3s8VIrZvw")

# Sector Configuration
MOROCCAN_SECTORS: List[str] = [
    "Automotive",
    "Aerospace & Defense",
    "Textile & Apparel",
    "Electronics",
    "Chemicals & Pharmaceuticals",
    "Food & Beverage",
    "Energy",
    "Mining",
    "Construction & BTP",
    "Agriculture",
    "ICT & Software",
    "Tourism & Hospitality",
    "Real Estate",
]

# Confidence Thresholds (0-1 scale)
CONFIDENCE_THRESHOLDS = {
    "auto_approve": 0.85,  # Directly insert to database
    "review_queue": 0.65,  # Send to human review
    "discard": 0.0,        # Below this is discarded
}

# Scraper Configuration
SCRAPER_CONFIG = {
    "rate_limit": 2.0,  # Seconds between requests
    "user_agents": [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Safari/605.1.15",
    ],
    "max_pages": 3,
    "timeout": 15,
    "max_retries": 3,
    "backoff_factor": 1.5,
}

# GPT-4o Pricing Constants (as of Feb 2025)
GPT_4O_PRICING = {
    "input": 2.50 / 1_000_000,      # $2.50 per 1M input tokens
    "output": 10.00 / 1_000_000,    # $10.00 per 1M output tokens
}

# Pipeline Configuration
PIPELINE_CONFIG = {
    "extraction_batch_size": 50,
    "extraction_timeout_seconds": 30,
    "keyword_filter_enabled": True,
}

# Industry-Relevant Keywords (for Moroccan context)
INDUSTRY_KEYWORDS = [
    "industrie", "industriel", "fabrication", "manufacture", "usine",
    "production", "secteur", "secteur économique", "export", "exportation",
    "pme", "tpe", "entreprise", "société", "groupe", "holding",
    "maroc", "marocain", "casablanca", "fès", "tanger", "rabat", "marrakech",
    "mdc", "zone industrielle", "zone franche", "parc industriel",
    "investissement", "partenariat", "alliance", "joint venture",
    "acquisition", "fusion", "croissance", "expansion",
    "apprentissage", "formation", "compétence", "ressource humaine",
    "innovation", "recherche", "développement", "r&d", "technologie",
    "supplier", "fournisseur", "client", "customer", "distributeur",
    "automobile", "aéronautique", "défense", "textile", "agroalimentaire",
    "électronique", "chimie", "pharmaceutique", "énergie", "mines",
    "tic", "logiciel", "digital", "transformation", "industrie 4.0",
    "ompic", "oca", "amdie", "maroc num", "invest in morocco",
]

# Logging Configuration
LOGGING_CONFIG = {
    "level": "INFO",
    "format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
}
