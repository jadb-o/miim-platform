"""
MIIM — Morocco Industry Intelligence Monitor
Streamlit Dashboard v2

Run:
    streamlit run app.py

Requires:
    SUPABASE_URL and SUPABASE_ANON_KEY environment variables (or hardcoded below).
"""

import os
import json
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import folium
from streamlit_folium import st_folium
import networkx as nx
from datetime import datetime
import streamlit.components.v1 as components

# ── Supabase client ──────────────────────────────────────
from supabase import create_client

SUPABASE_URL = os.environ.get(
    "SUPABASE_URL", "https://rkqfjesnavbngtihffge.supabase.co"
)
SUPABASE_ANON_KEY = os.environ.get(
    "SUPABASE_ANON_KEY",
    "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InJrcWZqZXNuYXZibmd0aWhmZmdlIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NzIxNTY0NzIsImV4cCI6MjA4NzczMjQ3Mn0.Djfr1UI1XzrUQmvo2rNi3rvMQIC0GXrMHpZiPnG6zfE",
)


@st.cache_resource
def get_supabase_client():
    return create_client(SUPABASE_URL, SUPABASE_ANON_KEY)


# ── Color palette ────────────────────────────────────────
NAVY = "#1B3A5C"
TEAL = "#2A9D8F"
LIGHT_BG = "#F7F9FB"
CORAL = "#E76F51"
GOLD = "#E9C46A"
SAND = "#F4A261"

TIER_COLORS = {
    "OEM": NAVY,
    "Tier 1": TEAL,
    "Tier 2": SAND,
    "Tier 3": CORAL,
    "Unknown": "#AAAAAA",
}

SECTOR_COLORS = {
    "Automotive": "#2A9D8F",
    "Aerospace": "#264653",
    "Textiles & Leather": "#E76F51",
    "Mining & Phosphates": "#F4A261",
    "Fishing & Seafood": "#457B9D",
    "Agrifood": "#8AB17D",
    "Renewable Energy": "#6A994E",
    "Electronics": "#E9C46A",
    "Pharmaceuticals": "#BC6C25",
    "Construction Materials": "#A8DADC",
    "Other": "#AAAAAA",
}

RELATIONSHIP_COLORS = {
    "partner": "#2A9D8F",
    "client": "#457B9D",
    "supplier": "#F4A261",
    "subsidiary": "#264653",
    "parent": "#264653",
    "investor": "#E9C46A",
    "joint_venture": "#6A994E",
    "competitor": "#E76F51",
}

# ── Moroccan city coordinates ────────────────────────────
CITY_COORDS = {
    "Tanger": (35.7595, -5.8340),
    "Kenitra": (34.2610, -6.5802),
    "Casablanca": (33.5731, -7.5898),
    "Rabat": (34.0209, -6.8416),
    "Marrakech": (31.6295, -7.9811),
    "Fes": (34.0181, -5.0078),
    "Agadir": (30.4278, -9.5981),
    "Oujda": (34.6814, -1.9086),
    "Meknes": (33.8935, -5.5473),
    "Mohammedia": (33.6866, -7.3830),
    "Settat": (33.0014, -7.6201),
    "El Jadida": (33.2316, -8.5007),
    "Nador": (35.1740, -2.9287),
    "Tetouan": (35.5889, -5.3626),
    "Safi": (32.2994, -9.2372),
    "Beni Mellal": (32.3373, -6.3498),
    "Khouribga": (32.8811, -6.9063),
    "Jorf Lasfar": (33.1167, -8.6333),
    "Laayoune": (27.1536, -13.2033),
    "Dakhla": (23.6848, -15.9580),
    "Guelmim": (28.9833, -10.0572),
    "Tan-Tan": (28.4379, -11.1033),
    "Tiznit": (29.6974, -9.8022),
    "Essaouira": (31.5085, -9.7595),
    "Errachidia": (31.9314, -4.4288),
    "Ouarzazate": (30.9189, -6.8936),
    "Taza": (34.2133, -4.0103),
    "Berrechid": (33.2654, -7.5876),
}


# ══════════════════════════════════════════════════════════
#  DATA LOADING (cached)
# ══════════════════════════════════════════════════════════


@st.cache_data(ttl=300)
def load_companies():
    """Load companies joined with sector names."""
    sb = get_supabase_client()
    resp = (
        sb.table("companies")
        .select("*, sectors(sector_name, target_integration_pct)")
        .order("company_name")
        .execute()
    )
    rows = resp.data
    for r in rows:
        sector_info = r.pop("sectors", None) or {}
        r["sector_name"] = sector_info.get("sector_name", "Unknown")
        r["sector_target_pct"] = sector_info.get("target_integration_pct")
    return pd.DataFrame(rows)


@st.cache_data(ttl=300)
def load_partnerships():
    """Load partnerships with company names resolved."""
    sb = get_supabase_client()
    resp = (
        sb.table("partnerships")
        .select(
            "*, company_a:companies!company_a_id(company_name, tier_level), "
            "company_b:companies!company_b_id(company_name, tier_level)"
        )
        .eq("status", "Active")
        .execute()
    )
    rows = resp.data
    for r in rows:
        a = r.pop("company_a", {}) or {}
        b = r.pop("company_b", {}) or {}
        r["company_a_name"] = a.get("company_name", "?")
        r["company_a_tier"] = a.get("tier_level", "Unknown")
        r["company_b_name"] = b.get("company_name", "?")
        r["company_b_tier"] = b.get("tier_level", "Unknown")
    return pd.DataFrame(rows)


@st.cache_data(ttl=300)
def load_events():
    sb = get_supabase_client()
    resp = (
        sb.table("events")
        .select("*, companies(company_name)")
        .order("event_date", desc=True)
        .execute()
    )
    rows = resp.data
    for r in rows:
        co = r.pop("companies", {}) or {}
        r["company_name"] = co.get("company_name", "?")
    return pd.DataFrame(rows)


@st.cache_data(ttl=300)
def load_relationships():
    """Load company relationships (v2 unified table)."""
    sb = get_supabase_client()
    try:
        resp = (
            sb.table("company_relationships")
            .select(
                "*, source:companies!source_company_id(id, company_name, headquarters_city, sector_id), "
                "target:companies!target_company_id(id, company_name, headquarters_city, sector_id)"
            )
            .execute()
        )
        rows = resp.data or []
        for r in rows:
            src = r.pop("source", {}) or {}
            tgt = r.pop("target", {}) or {}
            r["source_name"] = src.get("company_name", "?")
            r["source_city"] = src.get("headquarters_city")
            r["target_name"] = tgt.get("company_name", "?")
            r["target_city"] = tgt.get("headquarters_city")
        return pd.DataFrame(rows)
    except Exception:
        return pd.DataFrame()


@st.cache_data(ttl=300)
def load_company_people():
    """Load company people / management."""
    sb = get_supabase_client()
    try:
        resp = (
            sb.table("company_people")
            .select("*, companies(company_name)")
            .execute()
        )
        rows = resp.data or []
        for r in rows:
            co = r.pop("companies", {}) or {}
            r["company_name"] = co.get("company_name", "?")
        return pd.DataFrame(rows)
    except Exception:
        return pd.DataFrame()


@st.cache_data(ttl=300)
def load_company_articles():
    """Load company-article links."""
    sb = get_supabase_client()
    try:
        resp = (
            sb.table("company_articles")
            .select("*, companies(company_name), articles(title, source_url, source_name, published_date)")
            .execute()
        )
        rows = resp.data or []
        for r in rows:
            co = r.pop("companies", {}) or {}
            art = r.pop("articles", {}) or {}
            r["company_name"] = co.get("company_name", "?")
            r["article_title"] = art.get("title", "?")
            r["article_url"] = art.get("source_url", "")
            r["article_source"] = art.get("source_name", "")
            r["article_date"] = art.get("published_date", "")
        return pd.DataFrame(rows)
    except Exception:
        return pd.DataFrame()


@st.cache_data(ttl=300)
def load_sectors():
    """Load sectors table."""
    sb = get_supabase_client()
    resp = sb.table("sectors").select("*").execute()
    return pd.DataFrame(resp.data or [])


# ══════════════════════════════════════════════════════════
#  PAGE CONFIG
# ══════════════════════════════════════════════════════════

st.set_page_config(
    page_title="MIIM — Morocco Industry Intelligence Monitor",
    page_icon="🇲🇦",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Custom CSS ───────────────────────────────────────────
st.markdown(
    f"""
    <style>
        /* ── Global resets ── */
        .block-container {{
            padding: 1.5rem 2rem !important;
        }}

        /* ── Header ── */
        .main-header {{
            background: linear-gradient(135deg, {NAVY} 0%, {TEAL} 100%);
            padding: 2rem 2.5rem;
            border-radius: 14px;
            margin-bottom: 2rem;
        }}
        .main-header h1 {{
            color: white !important;
            margin: 0 !important;
            font-size: 1.6rem !important;
            font-weight: 600 !important;
        }}
        .main-header p {{
            color: rgba(255,255,255,0.8) !important;
            margin: 0.4rem 0 0 0 !important;
            font-size: 0.9rem !important;
            font-weight: 300 !important;
        }}

        /* ── Metric cards ── */
        div[data-testid="stMetric"] {{
            background-color: white;
            border-left: 4px solid {TEAL};
            padding: 1.2rem 1.5rem;
            border-radius: 14px;
            box-shadow: 0 1px 4px rgba(0,0,0,0.04);
            transition: box-shadow 0.2s ease;
        }}
        div[data-testid="stMetric"]:hover {{
            box-shadow: 0 4px 12px rgba(0,0,0,0.08);
        }}
        div[data-testid="stMetric"] label {{
            color: {NAVY} !important;
            font-weight: 500 !important;
        }}

        /* ── Sidebar ── */
        section[data-testid="stSidebar"] {{
            background-color: {LIGHT_BG};
        }}
        section[data-testid="stSidebar"] h1 {{
            color: {NAVY} !important;
            font-size: 1rem !important;
            font-weight: 600 !important;
        }}

        /* ── Tabs ── */
        button[data-baseweb="tab"] {{
            font-weight: 500 !important;
            font-size: 0.9rem !important;
            padding: 0.6rem 1.2rem !important;
            border-radius: 10px 10px 0 0 !important;
            transition: all 0.2s ease;
        }}
        button[data-baseweb="tab"]:hover {{
            background-color: rgba(42, 157, 143, 0.08) !important;
        }}

        /* ── Cards ── */
        .sector-card {{
            background: white;
            border-radius: 14px;
            padding: 1.5rem;
            border-left: 5px solid {TEAL};
            box-shadow: 0 1px 4px rgba(0,0,0,0.04);
            margin-bottom: 1rem;
            transition: box-shadow 0.2s ease, transform 0.2s ease;
        }}
        .sector-card:hover {{
            box-shadow: 0 4px 16px rgba(0,0,0,0.08);
            transform: translateY(-1px);
        }}
        .sector-card h3 {{
            color: {NAVY};
            margin: 0 0 0.5rem 0;
            font-size: 1rem;
            font-weight: 600;
        }}
        .sector-card .stat {{
            font-size: 0.85rem;
            color: #666;
        }}

        .profile-card {{
            background: white;
            border-radius: 14px;
            padding: 2rem;
            box-shadow: 0 2px 8px rgba(0,0,0,0.06);
            margin-bottom: 1.5rem;
            transition: box-shadow 0.2s ease;
        }}
        .profile-card:hover {{
            box-shadow: 0 4px 20px rgba(0,0,0,0.1);
        }}
        .profile-header {{
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 1.2rem;
        }}
        .profile-header h2 {{
            color: {NAVY};
            margin: 0;
            font-weight: 600;
        }}
        .profile-badge {{
            background: {TEAL};
            color: white;
            padding: 0.3rem 0.8rem;
            border-radius: 20px;
            font-size: 0.8rem;
            font-weight: 500;
        }}

        /* ── Tables ── */
        .stDataFrame {{
            border-radius: 14px !important;
            overflow: hidden;
            box-shadow: 0 1px 4px rgba(0,0,0,0.04);
        }}

        /* ── Buttons ── */
        .stButton > button {{
            border-radius: 10px !important;
            font-weight: 500 !important;
            transition: all 0.2s ease !important;
            padding: 0.5rem 1.2rem !important;
        }}
        .stButton > button:hover {{
            transform: translateY(-1px) !important;
            box-shadow: 0 4px 12px rgba(0,0,0,0.1) !important;
        }}

        /* ── Expanders ── */
        details {{
            border-radius: 14px !important;
            border: 1px solid #E8ECF0 !important;
        }}

        /* ── Softer typography ── */
        h1, h2, h3, h4 {{
            font-weight: 600 !important;
        }}
        .stMarkdown p {{
            font-weight: 400;
            line-height: 1.6;
        }}
    </style>
    """,
    unsafe_allow_html=True,
)

# ── Header ───────────────────────────────────────────────
st.markdown(
    """
    <div class="main-header">
        <h1>Morocco Industry Intelligence Monitor</h1>
        <p>Mapping Morocco's industrial landscape — companies, supply chains, partnerships, and market intelligence.</p>
    </div>
    """,
    unsafe_allow_html=True,
)


# ══════════════════════════════════════════════════════════
#  LOAD DATA
# ══════════════════════════════════════════════════════════

try:
    df_companies = load_companies()
    df_partnerships = load_partnerships()
    df_events = load_events()
    df_relationships = load_relationships()
    df_people = load_company_people()
    df_articles = load_company_articles()
    df_sectors = load_sectors()
    data_loaded = True
except Exception as e:
    st.error(f"Could not connect to database: {e}")
    data_loaded = False

if not data_loaded or df_companies.empty:
    st.warning("No data found. Please seed the database first.")
    st.stop()


# ══════════════════════════════════════════════════════════
#  SIDEBAR FILTERS
# ══════════════════════════════════════════════════════════

with st.sidebar:
    st.markdown("### Filters")

    all_sectors = sorted(df_companies["sector_name"].dropna().unique().tolist())
    selected_sectors = st.multiselect("Sector", options=all_sectors, default=all_sectors)

    all_cities = sorted(df_companies["headquarters_city"].dropna().unique().tolist())
    selected_cities = st.multiselect("City", options=all_cities, default=all_cities)

    all_ownership = sorted(df_companies["ownership_type"].dropna().unique().tolist())
    selected_ownership = st.multiselect("Ownership Type", options=all_ownership, default=all_ownership)

    all_tiers = sorted(df_companies["tier_level"].dropna().unique().tolist())
    selected_tiers = st.multiselect("Tier Level", options=all_tiers, default=all_tiers)

    st.markdown("---")
    search_query = st.text_input("🔍 Search companies", placeholder="e.g. Renault, Yazaki...")

    st.markdown("---")
    st.markdown(
        f'<p style="color:{NAVY}; font-size:0.8rem;">'
        f"Last refreshed: {datetime.now().strftime('%H:%M:%S')}<br>"
        f"Data cached for 5 min"
        f"</p>",
        unsafe_allow_html=True,
    )
    if st.button("🔄 Refresh data"):
        st.cache_data.clear()
        st.rerun()

# ── Apply filters ────────────────────────────────────────
mask = pd.Series(True, index=df_companies.index)

if selected_sectors and len(selected_sectors) < len(all_sectors):
    mask = mask & (df_companies["sector_name"].isin(selected_sectors) | df_companies["sector_name"].isna())

if selected_cities and len(selected_cities) < len(all_cities):
    mask = mask & (df_companies["headquarters_city"].isin(selected_cities) | df_companies["headquarters_city"].isna())

if selected_ownership and len(selected_ownership) < len(all_ownership):
    mask = mask & (df_companies["ownership_type"].isin(selected_ownership) | df_companies["ownership_type"].isna())

if selected_tiers and len(selected_tiers) < len(all_tiers):
    mask = mask & (df_companies["tier_level"].isin(selected_tiers) | df_companies["tier_level"].isna())

if search_query:
    mask = mask & df_companies["company_name"].str.contains(search_query, case=False, na=False)

df_filtered = df_companies[mask].copy()


# ══════════════════════════════════════════════════════════
#  KPI METRICS ROW
# ══════════════════════════════════════════════════════════

col1, col2, col3, col4, col5 = st.columns(5)
with col1:
    st.metric("Companies", len(df_filtered))
with col2:
    n_sectors = df_filtered["sector_name"].nunique() if not df_filtered.empty else 0
    st.metric("Sectors", n_sectors)
with col3:
    total_employees = df_filtered["employee_count"].astype(float).sum() if not df_filtered.empty else 0
    st.metric("Total Employees", f"{total_employees:,.0f}")
with col4:
    n_rels = len(df_relationships) if not df_relationships.empty else 0
    st.metric("Relationships", n_rels)
with col5:
    n_articles = len(df_articles) if not df_articles.empty else 0
    st.metric("Article Links", n_articles)


# ══════════════════════════════════════════════════════════
#  TAB LAYOUT
# ══════════════════════════════════════════════════════════

tab_sectors, tab_directory, tab_profile, tab_network, tab_map, tab_events, tab_review = st.tabs(
    ["Sectors", "Directory", "Company Profile", "Network Map", "Map", "Events", "Review Queue"]
)


# ─── TAB 1: Sectors Overview ─────────────────────────────
with tab_sectors:
    st.markdown("#### Sector Overview")
    st.caption("Morocco's priority industrial sectors and their key statistics.")

    if not df_filtered.empty:
        # Build aggregation dict based on available columns
        agg_dict = {
            "company_count": ("company_name", "count"),
            "total_employees": ("employee_count", lambda x: x.astype(float).sum()),
            "cities": ("headquarters_city", lambda x: x.dropna().nunique()),
        }
        if "investment_amount_mad" in df_filtered.columns:
            agg_dict["total_investment"] = ("investment_amount_mad", lambda x: x.astype(float).sum())

        sector_stats = (
            df_filtered.groupby("sector_name")
            .agg(**agg_dict)
            .reset_index()
            .sort_values("company_count", ascending=False)
        )
        if "total_investment" not in sector_stats.columns:
            sector_stats["total_investment"] = 0

        # Grid of sector cards
        cols = st.columns(3)
        for idx, (_, row) in enumerate(sector_stats.iterrows()):
            sector_name = row["sector_name"]
            color = SECTOR_COLORS.get(sector_name, TEAL)
            emp = row["total_employees"]
            inv = row["total_investment"]

            inv_str = ""
            if inv and inv > 0:
                if inv >= 1e9:
                    inv_str = f"{inv/1e9:.1f}B MAD"
                elif inv >= 1e6:
                    inv_str = f"{inv/1e6:.0f}M MAD"
                else:
                    inv_str = f"{inv:,.0f} MAD"

            with cols[idx % 3]:
                st.markdown(
                    f"""
                    <div class="sector-card" style="border-left-color: {color};">
                        <h3>{sector_name}</h3>
                        <div class="stat">🏢 <b>{int(row['company_count'])}</b> companies</div>
                        <div class="stat">👥 <b>{emp:,.0f}</b> employees</div>
                        {'<div class="stat">💰 <b>' + inv_str + '</b> invested</div>' if inv_str else ''}
                        <div class="stat">📍 <b>{int(row['cities'])}</b> cities</div>
                    </div>
                    """,
                    unsafe_allow_html=True,
                )

        st.markdown("---")

        # Charts row
        col_left, col_right = st.columns(2)

        with col_left:
            st.markdown("##### Companies per Sector")
            sector_counts = sector_stats.sort_values("company_count", ascending=True)
            fig_bar = px.bar(
                sector_counts, x="company_count", y="sector_name", orientation="h",
                text="company_count",
                labels={"company_count": "Companies", "sector_name": ""},
                color="sector_name",
                color_discrete_map=SECTOR_COLORS,
            )
            fig_bar.update_traces(textposition="outside")
            fig_bar.update_layout(
                showlegend=False, plot_bgcolor="white", paper_bgcolor="white",
                margin=dict(l=20, r=80, t=20, b=40), height=350,
                font=dict(family="Arial", color=NAVY),
            )
            st.plotly_chart(fig_bar, use_container_width=True)

        with col_right:
            st.markdown("##### Companies by Ownership Type")
            ownership_counts = (
                df_filtered["ownership_type"].fillna("Unknown").value_counts()
                .reset_index()
                .rename(columns={"index": "ownership_type", "count": "count"})
            )
            if "ownership_type" in ownership_counts.columns and "count" in ownership_counts.columns:
                fig_pie = px.pie(
                    ownership_counts, names="ownership_type", values="count", hole=0.4,
                    color_discrete_sequence=[TEAL, NAVY, SAND, CORAL, GOLD, "#AAAAAA", "#7FB3D8"],
                )
                fig_pie.update_layout(
                    margin=dict(l=20, r=20, t=20, b=20), height=350,
                    font=dict(family="Arial", color=NAVY), paper_bgcolor="white",
                )
                fig_pie.update_traces(textinfo="label+percent", textposition="outside")
                st.plotly_chart(fig_pie, use_container_width=True)

        # Sector integration targets
        if "sector_target_pct" in df_filtered.columns:
            sector_targets = (
                df_filtered.groupby("sector_name")
                .agg(target_pct=("sector_target_pct", "first"))
                .reset_index()
                .dropna(subset=["target_pct"])
                .sort_values("target_pct", ascending=True)
            )
            if not sector_targets.empty:
                st.markdown("##### Government Integration Targets by Sector")
                st.caption("Target local integration rates set by Morocco's industrial strategy.")
                sector_targets["target_pct"] = sector_targets["target_pct"].astype(float)
                fig_target = px.bar(
                    sector_targets, x="target_pct", y="sector_name", orientation="h",
                    text=sector_targets["target_pct"].map(lambda x: f"{x:.0f}%"),
                    labels={"target_pct": "Integration Target (%)", "sector_name": ""},
                    color="target_pct",
                    color_continuous_scale=[[0, CORAL], [0.5, SAND], [1, TEAL]],
                )
                fig_target.update_traces(textposition="outside")
                fig_target.update_layout(
                    showlegend=False, coloraxis_showscale=False,
                    plot_bgcolor="white", paper_bgcolor="white",
                    xaxis=dict(range=[0, 100], gridcolor="#E8E8E8"),
                    margin=dict(l=20, r=60, t=20, b=40), height=300,
                    font=dict(family="Arial", color=NAVY),
                )
                st.plotly_chart(fig_target, use_container_width=True)
    else:
        st.info("No data matches the current filters.")


# ─── TAB 2: Company Directory ────────────────────────────
with tab_directory:
    st.markdown(f"#### Company Directory ({len(df_filtered)} results)")
    st.caption("Select a company in the 'Company Profile' tab for full details.")

    display_cols = {
        "company_name": "Company",
        "sector_name": "Sector",
        "sub_sector": "Sub-Sector",
        "headquarters_city": "City",
        "ownership_type": "Ownership",
        "employee_count": "Employees",
        "description": "Description",
        "parent_company": "Parent",
        "website_url": "Website",
    }

    df_display = df_filtered[[c for c in display_cols.keys() if c in df_filtered.columns]].copy()
    df_display.rename(columns=display_cols, inplace=True)

    if "Employees" in df_display.columns:
        df_display["Employees"] = (
            df_display["Employees"].astype(float).map(lambda x: f"{x:,.0f}" if pd.notna(x) else "—")
        )
    if "Description" in df_display.columns:
        df_display["Description"] = df_display["Description"].fillna("").str[:80]

    st.dataframe(
        df_display, use_container_width=True, hide_index=True,
        height=min(500, 50 + len(df_display) * 35),
    )


# ─── TAB 3: Company Profile ──────────────────────────────
with tab_profile:
    st.markdown("#### Company Profile")

    company_names = sorted(df_filtered["company_name"].dropna().unique().tolist())
    selected_company = st.selectbox("Select a company", options=[""] + company_names, index=0)

    if selected_company:
        co = df_filtered[df_filtered["company_name"] == selected_company].iloc[0]
        co_id = co.get("id")

        # Header
        sector = co.get("sector_name", "Unknown")
        city = co.get("headquarters_city", "")
        color = SECTOR_COLORS.get(sector, TEAL)

        st.markdown(
            f"""
            <div class="profile-card">
                <div class="profile-header">
                    <h2>{selected_company}</h2>
                    <span class="profile-badge" style="background:{color};">{sector}</span>
                </div>
                <div style="display:flex; gap:2rem; flex-wrap:wrap; color:#555;">
                    {'<span>📍 ' + str(city) + '</span>' if city else ''}
                    {'<span>🏢 ' + str(co.get("ownership_type", "")) + '</span>' if co.get("ownership_type") else ''}
                    {'<span>🔗 <a href="' + str(co.get("website_url", "")) + '" target="_blank">' + str(co.get("website_url", "")) + '</a></span>' if co.get("website_url") else ''}
                </div>
            </div>
            """,
            unsafe_allow_html=True,
        )

        # Key metrics
        m1, m2, m3, m4 = st.columns(4)
        with m1:
            emp = co.get("employee_count")
            st.metric("Employees", f"{float(emp):,.0f}" if emp and pd.notna(emp) else "N/A")
        with m2:
            rev = co.get("revenue_mad")
            if rev and pd.notna(rev):
                rev_f = float(rev)
                rev_str = f"{rev_f/1e6:.0f}M MAD" if rev_f >= 1e6 else f"{rev_f:,.0f} MAD"
            else:
                rev_str = "N/A"
            st.metric("Revenue", rev_str)
        with m3:
            inv = co.get("investment_amount_mad")
            if inv and pd.notna(inv):
                inv_f = float(inv)
                inv_str = f"{inv_f/1e6:.0f}M MAD" if inv_f >= 1e6 else f"{inv_f:,.0f} MAD"
            else:
                inv_str = "N/A"
            st.metric("Investment", inv_str)
        with m4:
            cap = co.get("capital_mad")
            if cap and pd.notna(cap):
                cap_f = float(cap)
                cap_str = f"{cap_f/1e6:.0f}M MAD" if cap_f >= 1e6 else f"{cap_f:,.0f} MAD"
            else:
                cap_str = "N/A"
            st.metric("Capital", cap_str)

        # Description & activities
        col_l, col_r = st.columns(2)
        with col_l:
            desc = co.get("description")
            if desc and pd.notna(desc):
                st.markdown("**Description**")
                st.write(desc)
            activities = co.get("activities")
            if activities and pd.notna(activities):
                st.markdown("**Activities**")
                st.write(activities)

        with col_r:
            parent = co.get("parent_company")
            if parent and pd.notna(parent):
                st.markdown(f"**Parent Company**: {parent}")
            sub = co.get("sub_sector")
            if sub and pd.notna(sub):
                st.markdown(f"**Sub-sector**: {sub}")
            tier = co.get("tier_level")
            if tier and pd.notna(tier):
                st.markdown(f"**Tier Level**: {tier}")

        # Management team
        if not df_people.empty and co_id:
            co_people = df_people[df_people["company_id"] == co_id]
            if not co_people.empty:
                st.markdown("---")
                st.markdown("**👥 Management Team**")
                for _, p in co_people.iterrows():
                    role = p.get("role_title", "")
                    name = p.get("person_name", "")
                    st.markdown(f"- **{name}** — {role}")

        # Relationships
        if not df_relationships.empty and co_id:
            co_rels_out = df_relationships[df_relationships["source_company_id"] == co_id] if "source_company_id" in df_relationships.columns else pd.DataFrame()
            co_rels_in = df_relationships[df_relationships["target_company_id"] == co_id] if "target_company_id" in df_relationships.columns else pd.DataFrame()

            all_rels = []
            for _, r in co_rels_out.iterrows():
                all_rels.append({"Company": r.get("target_name", "?"), "Type": r.get("relationship_type", ""), "Description": r.get("description", ""), "Direction": "→"})
            for _, r in co_rels_in.iterrows():
                all_rels.append({"Company": r.get("source_name", "?"), "Type": r.get("relationship_type", ""), "Description": r.get("description", ""), "Direction": "←"})

            if all_rels:
                st.markdown("---")
                st.markdown("**🔗 Relationships**")
                st.dataframe(pd.DataFrame(all_rels), use_container_width=True, hide_index=True)

        # Recent articles
        if not df_articles.empty and co_id:
            co_arts = df_articles[df_articles["company_id"] == co_id]
            if not co_arts.empty:
                st.markdown("---")
                st.markdown("**📰 Media Mentions**")
                for _, a in co_arts.head(10).iterrows():
                    title = a.get("article_title", "Article")
                    url = a.get("article_url", "")
                    source = a.get("article_source", "")
                    date = str(a.get("article_date", ""))[:10]
                    mention = a.get("mention_type", "")
                    if url:
                        st.markdown(f"- [{title}]({url}) — {source} ({date}) *[{mention}]*")
                    else:
                        st.markdown(f"- {title} — {source} ({date}) *[{mention}]*")
    else:
        st.info("Select a company from the dropdown above to view its full profile.")


# ─── TAB 4: Interactive Network Map (vis.js) ─────────────
with tab_network:
    st.markdown("#### Industry Network Map")
    st.caption("Interactive visualization of company relationships. Drag nodes, zoom, and click to highlight connections.")

    # Build network data
    nodes = {}
    edges = []

    for _, co in df_filtered.iterrows():
        name = co["company_name"]
        sector = co.get("sector_name", "Unknown")
        city = co.get("headquarters_city", "")
        emp = co.get("employee_count", 0)
        try:
            emp = float(emp) if emp and pd.notna(emp) else 0
        except (TypeError, ValueError):
            emp = 0

        color = SECTOR_COLORS.get(sector, "#AAAAAA")
        size = max(15, min(50, 15 + (emp / 500)))

        nodes[name] = {
            "id": name, "label": name, "color": color, "size": size,
            "title": f"<b>{name}</b><br>Sector: {sector}<br>City: {city}<br>Employees: {emp:,.0f}",
            "sector": sector,
        }

    # Edges from company_relationships
    if not df_relationships.empty:
        for _, r in df_relationships.iterrows():
            src = r.get("source_name", "")
            tgt = r.get("target_name", "")
            rel_type = r.get("relationship_type", "partner")
            desc = r.get("description", "")

            if src and tgt:
                for n in [src, tgt]:
                    if n not in nodes:
                        nodes[n] = {"id": n, "label": n, "color": "#AAAAAA", "size": 15, "title": f"<b>{n}</b>", "sector": "Unknown"}

                edge_color = RELATIONSHIP_COLORS.get(rel_type, "#B0C4D8")
                edges.append({
                    "from": src, "to": tgt, "label": rel_type,
                    "color": {"color": edge_color, "opacity": 0.7},
                    "title": f"{src} → {tgt}<br>Type: {rel_type}<br>{desc}", "width": 2,
                })

    # Edges from partnerships
    if not df_partnerships.empty:
        for _, row in df_partnerships.iterrows():
            a = row.get("company_a_name", "")
            b = row.get("company_b_name", "")
            ptype = row.get("partnership_type", "partner")

            if a and b:
                for n in [a, b]:
                    if n not in nodes:
                        nodes[n] = {"id": n, "label": n, "color": "#AAAAAA", "size": 15, "title": f"<b>{n}</b>", "sector": "Unknown"}
                existing = {(e["from"], e["to"]) for e in edges}
                if (a, b) not in existing and (b, a) not in existing:
                    edges.append({
                        "from": a, "to": b, "label": ptype,
                        "color": {"color": RELATIONSHIP_COLORS.get("partner", "#B0C4D8"), "opacity": 0.7},
                        "title": f"{a} ↔ {b}<br>Type: {ptype}", "width": 2,
                    })

    if nodes and edges:
        nodes_json = json.dumps(list(nodes.values()))
        edges_json = json.dumps(edges)

        # Legend
        rel_types_used = set(e.get("label", "") for e in edges if e.get("label"))
        legend_items = "".join(
            f'<span style="display:inline-flex;align-items:center;gap:4px;margin-right:12px;"><span style="width:20px;height:3px;background:{RELATIONSHIP_COLORS.get(rt, "#B0C4D8")};display:inline-block;"></span>{rt}</span>'
            for rt in sorted(rel_types_used)
        )
        sectors_used = set(n.get("sector", "") for n in nodes.values() if n.get("sector") != "Unknown")
        sector_legend = "".join(
            f'<span style="display:inline-flex;align-items:center;gap:4px;margin-right:12px;"><span style="width:12px;height:12px;border-radius:50%;background:{SECTOR_COLORS.get(s, "#AAAAAA")};display:inline-block;"></span>{s}</span>'
            for s in sorted(sectors_used)
        )

        vis_html = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <script src="https://cdnjs.cloudflare.com/ajax/libs/vis-network/9.1.6/vis-network.min.js"></script>
            <link href="https://cdnjs.cloudflare.com/ajax/libs/vis-network/9.1.6/vis-network.min.css" rel="stylesheet">
            <style>
                body {{ margin: 0; padding: 0; font-family: Arial, sans-serif; }}
                #network {{ width: 100%; height: 600px; border: 1px solid #E8ECF0; border-radius: 14px; }}
                #legend {{ padding: 8px 12px; font-size: 12px; color: #555; }}
            </style>
        </head>
        <body>
            <div id="network"></div>
            <div id="legend">
                <div style="margin-bottom:4px;"><b>Sectors:</b> {sector_legend}</div>
                <div><b>Relationships:</b> {legend_items}</div>
            </div>
            <script>
                var nodes = new vis.DataSet({nodes_json});
                var edges = new vis.DataSet({edges_json});
                var container = document.getElementById('network');
                var data = {{ nodes: nodes, edges: edges }};
                var options = {{
                    physics: {{
                        forceAtlas2Based: {{
                            gravitationalConstant: -40,
                            centralGravity: 0.005,
                            springLength: 150,
                            springConstant: 0.08,
                            damping: 0.4
                        }},
                        solver: 'forceAtlas2Based',
                        stabilization: {{ iterations: 150 }}
                    }},
                    nodes: {{
                        shape: 'dot',
                        font: {{ size: 12, color: '{NAVY}', face: 'Arial' }},
                        borderWidth: 2,
                        borderWidthSelected: 3
                    }},
                    edges: {{
                        smooth: {{ type: 'continuous' }},
                        font: {{ size: 9, color: '#888', align: 'middle' }},
                        arrows: {{ to: {{ enabled: true, scaleFactor: 0.5 }} }}
                    }},
                    interaction: {{
                        hover: true,
                        tooltipDelay: 200,
                        navigationButtons: true,
                        keyboard: true
                    }}
                }};
                var network = new vis.Network(container, data, options);
                network.on("click", function(params) {{
                    if (params.nodes.length > 0) {{
                        var nodeId = params.nodes[0];
                        var connectedEdges = network.getConnectedEdges(nodeId);
                        var connectedNodes = network.getConnectedNodes(nodeId);
                        connectedNodes.push(nodeId);
                        network.selectNodes(connectedNodes);
                        network.selectEdges(connectedEdges);
                    }}
                }});
            </script>
        </body>
        </html>
        """

        components.html(vis_html, height=680)
    elif nodes:
        st.info("Companies loaded but no relationships found yet. Run the pipeline to extract relationships from articles.")
    else:
        st.info("No network data available.")


# ─── TAB 5: Map of Morocco ──────────────────────────────
with tab_map:
    st.markdown("#### Industrial Map of Morocco")
    st.caption("Dot size proportional to employee count. Click markers for details.")

    if not df_filtered.empty:
        city_data = (
            df_filtered.groupby("headquarters_city")
            .agg(
                total_employees=("employee_count", lambda x: x.astype(float).sum()),
                company_count=("company_name", "count"),
                companies_list=("company_name", lambda x: ", ".join(x.tolist())),
            )
            .reset_index()
        )

        m = folium.Map(location=[31.5, -7.0], zoom_start=6, tiles=None)
        folium.TileLayer(
            tiles="https://server.arcgisonline.com/ArcGIS/rest/services/Canvas/World_Light_Gray_Base/MapServer/tile/{z}/{y}/{x}",
            attr="Esri, HERE, Garmin",
            name="Esri Light Gray",
        ).add_to(m)

        for _, row in city_data.iterrows():
            city = row["headquarters_city"]
            coords = CITY_COORDS.get(city)
            if not coords:
                continue

            max_emp = city_data["total_employees"].max() if city_data["total_employees"].max() > 0 else 1
            radius = 8 + (row["total_employees"] / max_emp) * 32

            popup_html = f"""
            <div style="font-family:Arial; width:220px;">
                <h4 style="color:{NAVY}; margin:0 0 8px 0;">{city}</h4>
                <b>Companies:</b> {row['company_count']}<br>
                <b>Employees:</b> {row['total_employees']:,.0f}<br>
                <hr style="margin:6px 0;">
                <small>{row['companies_list']}</small>
            </div>
            """

            folium.CircleMarker(
                location=coords, radius=radius, color=NAVY,
                fill=True, fill_color=TEAL, fill_opacity=0.7, weight=2,
                popup=folium.Popup(popup_html, max_width=250),
                tooltip=f"{city}: {row['company_count']} companies, {row['total_employees']:,.0f} employees",
            ).add_to(m)

        # Relationship lines between cities
        if not df_relationships.empty:
            drawn_pairs = set()
            for _, r in df_relationships.iterrows():
                src_city = r.get("source_city")
                tgt_city = r.get("target_city")
                if src_city and tgt_city and src_city != tgt_city:
                    pair = tuple(sorted([src_city, tgt_city]))
                    if pair not in drawn_pairs:
                        src_coords = CITY_COORDS.get(src_city)
                        tgt_coords = CITY_COORDS.get(tgt_city)
                        if src_coords and tgt_coords:
                            folium.PolyLine(
                                [src_coords, tgt_coords],
                                color=TEAL, weight=1.5, opacity=0.4, dash_array="5 5",
                            ).add_to(m)
                            drawn_pairs.add(pair)

        st_folium(m, use_container_width=True, height=550)
    else:
        st.info("No data matches the current filters.")


# ─── TAB 6: Events ──────────────────────────────────────
with tab_events:
    st.markdown("#### Recent Industrial Events")

    if not df_events.empty:
        for _, ev in df_events.head(15).iterrows():
            event_icon = {
                "New Factory": "🏗️", "new_factory": "🏗️",
                "Partnership": "🤝", "partnership": "🤝",
                "Investment": "💰", "investment": "💰",
                "Acquisition": "🔄", "acquisition": "🔄",
                "Export Milestone": "📦", "export_milestone": "📦",
                "Expansion": "📈", "expansion": "📈",
                "hiring": "👥", "product_launch": "🚀",
                "certification": "✅", "other": "📌",
            }.get(ev.get("event_type", ""), "📌")

            amt_str = ""
            if ev.get("investment_amount_mad") and pd.notna(ev["investment_amount_mad"]):
                amt = float(ev["investment_amount_mad"])
                if amt >= 1_000_000_000:
                    amt_str = f" — {amt / 1_000_000_000:.1f}B MAD"
                elif amt >= 1_000_000:
                    amt_str = f" — {amt / 1_000_000:.0f}M MAD"

            date_str = ev.get("event_date", "")
            if date_str:
                try:
                    date_str = pd.to_datetime(date_str).strftime("%b %d, %Y")
                except Exception:
                    pass

            st.markdown(
                f"""
                <div style="background:white; padding:1.5rem; border-radius:14px;
                            border-left:4px solid {TEAL}; margin-bottom:1rem; box-shadow: 0 1px 4px rgba(0,0,0,0.04);">
                    <div style="display:flex; justify-content:space-between; align-items:center;">
                        <span style="font-size:1.1rem; font-weight:600; color:{NAVY};">
                            {event_icon} {ev.get('title', 'Event')}
                        </span>
                        <span style="color:#888; font-size:0.85rem;">{date_str}</span>
                    </div>
                    <p style="margin:0.4rem 0 0 0; color:#555; font-size:0.9rem;">
                        <b>{ev.get('company_name', '')}</b>{amt_str}
                        {' — ' + ev.get('city', '') if ev.get('city') else ''}
                    </p>
                    <p style="margin:0.3rem 0 0 0; color:#666; font-size:0.85rem;">
                        {ev.get('description', '')}
                    </p>
                </div>
                """,
                unsafe_allow_html=True,
            )
    else:
        st.info("No events recorded yet.")


# ─── TAB 7: Review Queue ─────────────────────────────────
with tab_review:
    from review_ui.review_helpers import load_review_items, get_review_stats, approve_item, reject_item, get_pipeline_stats

    st.markdown("#### Human-in-the-Loop Review Queue")
    st.markdown("Review and approve low-confidence extractions before they enter the database.")

    sb = get_supabase_client()

    p_stats = get_pipeline_stats(sb)
    pcol1, pcol2, pcol3, pcol4 = st.columns(4)
    with pcol1:
        st.metric("Total Articles Scraped", p_stats["total_articles"])
    with pcol2:
        st.metric("Pending Extraction", p_stats["pending_extraction"])
    with pcol3:
        st.metric("Extracted", p_stats["extracted"])
    with pcol4:
        st.metric("Pipeline Cost (USD)", f"${p_stats['total_cost_usd']:.2f}")

    st.divider()

    r_stats = get_review_stats(sb)
    rcol1, rcol2, rcol3, rcol4 = st.columns(4)
    with rcol1:
        st.metric("Pending Review", r_stats["pending"])
    with rcol2:
        st.metric("Approved (7d)", r_stats["approved_7d"])
    with rcol3:
        st.metric("Rejected (7d)", r_stats["rejected_7d"])
    with rcol4:
        st.metric("Avg Confidence", f"{r_stats['avg_confidence']:.0%}")

    st.divider()

    review_items = load_review_items(sb, status="pending", limit=20)

    if not review_items:
        st.success("No items pending review. The pipeline is running smoothly.")
    else:
        st.info(f"**{len(review_items)}** items awaiting your review")

        for idx, item in enumerate(review_items):
            ext = item["extracted_data"] if isinstance(item["extracted_data"], dict) else {}
            conf_pct = f"{item['confidence_score']:.0%}" if item["confidence_score"] else "N/A"
            company = ext.get("company_name", "Unknown Company")

            with st.expander(
                f"[{conf_pct}] **{company}** — {item['source_name']} ({item.get('published_date', '')[:10]})",
                expanded=(idx == 0),
            ):
                col_left, col_right = st.columns([3, 1])
                with col_left:
                    st.markdown(f"**Article**: {item['article_title']}")
                    if item["source_url"]:
                        st.markdown(f"**Source**: [{item['source_name']}]({item['source_url']})")
                    snippet = (item.get("article_text") or "")[:500]
                    if snippet:
                        st.text_area("Article Preview", value=snippet + "...", height=120, disabled=True, key=f"preview_{idx}")
                with col_right:
                    st.metric("Confidence", conf_pct)
                    st.markdown(f"**Flagged**: {item['reason_flagged']}")

                st.markdown("**Extracted Data:**")
                d1, d2 = st.columns(2)
                with d1:
                    st.markdown(f"- **Company**: {ext.get('company_name', 'N/A')}")
                    st.markdown(f"- **Sector**: {ext.get('sector', 'N/A')}")
                    st.markdown(f"- **City**: {ext.get('city', 'N/A')}")
                    st.markdown(f"- **Sub-sector**: {ext.get('sub_sector', 'N/A')}")
                with d2:
                    st.markdown(f"- **Event**: {ext.get('event_type', 'N/A')}")
                    amt = ext.get("investment_amount_mad")
                    st.markdown(f"- **Investment (MAD)**: {f'{amt:,.0f}' if amt else 'N/A'}")
                    partners = ext.get("partner_companies", [])
                    st.markdown(f"- **Partners**: {', '.join(partners) if partners else 'None'}")
                    st.markdown(f"- **Summary**: {ext.get('source_summary', ext.get('article_summary', 'N/A'))}")

                a1, a2, a3 = st.columns(3)
                with a1:
                    if st.button("✅ Approve", key=f"approve_{item['id']}"):
                        if approve_item(sb, item["id"]):
                            st.success(f"Approved! {company} added to database.")
                            st.rerun()
                        else:
                            st.error("Failed to approve. Check logs.")
                with a2:
                    reject_notes = st.text_input("Rejection reason", key=f"reject_notes_{item['id']}", placeholder="Optional...")
                    if st.button("❌ Reject", key=f"reject_{item['id']}"):
                        if reject_item(sb, item["id"], notes=reject_notes):
                            st.info(f"Rejected: {company}")
                            st.rerun()
                        else:
                            st.error("Failed to reject.")
                with a3:
                    st.markdown("*Edit & Approve coming soon*")

    if p_stats["recent_runs"]:
        st.divider()
        st.markdown("#### Recent Scraper Runs")
        runs_df = pd.DataFrame(p_stats["recent_runs"])
        if not runs_df.empty:
            display_cols = [c for c in ["source_name", "run_date", "articles_found", "articles_new", "articles_duplicate", "status"] if c in runs_df.columns]
            st.dataframe(runs_df[display_cols], use_container_width=True, hide_index=True)


# ── Footer ───────────────────────────────────────────────
st.markdown("---")
st.markdown(
    f"""
    <div style="text-align:center; padding:1rem 0; color:{NAVY};">
        <b>MIIM</b> — Morocco Industry Intelligence Monitor<br>
        <span style="font-size:0.85rem; color:#888;">
            Open-source &middot; Data-driven &middot; Transparent<br>
            Built with Streamlit, Supabase, and vis.js
        </span>
    </div>
    """,
    unsafe_allow_html=True,
)
