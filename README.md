# MIIM — Morocco Industry Intelligence Monitor

An open-source platform that maps the Moroccan industrial landscape: players, market share, partnerships, and local integration rates.

## Live Dashboard

Run locally:

```bash
pip install -r requirements.txt
streamlit run app.py
```

## Features

- **Company Directory** — Searchable, filterable table of Moroccan industrial companies
- **Integration Analysis** — Plotly charts showing local integration rates by sector and company
- **Partnership Network** — Interactive graph of supplier relationships between companies
- **Morocco Map** — Folium map with company locations sized by employee count
- **LLM Extraction Pipeline** — GPT-4o-powered extraction of structured data from French/Arabic news

## Tech Stack

| Layer | Tool |
|-------|------|
| Frontend | Streamlit |
| Database | Supabase (PostgreSQL) |
| Charts | Plotly |
| Map | Folium |
| LLM | OpenAI GPT-4o |
| CI/CD | GitHub Actions |

## License

MIT
