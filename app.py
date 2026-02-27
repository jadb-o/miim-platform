"""
MIIM — Morocco Industry Intelligence Monitor
Streamlit Dashboard

Run:
    streamlit run app.py

Requires:
    SUPABASE_URL and SUPABASE_ANON_KEY environment variables (or hardcoded below).
"""

import os
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import folium
from streamlit_folium import st_folium
import networkx as nx
from datetime import datetime

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
LIGHT_BG = "#F0F5F8"
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
}


# ══════════════════════════════════════════════════════════
#  DATA LOADING (cached)
# ══════════════════════════════════════════════════════════


@st.cache_data(ttl=300)  # refresh every 5 minutes
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


# ══════════════════════════════════════════════════════════
#  PAGE CONFIG
# ══════════════════════════════════════════════════════════

st.set_page_config(
    page_title="MIIM — Morocco Industry Intelligence Monitor",
    page_icon="🏭",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Custom CSS ───────────────────────────────────────────
st.markdown(
    f"""
    <style>
        /* Header bar */
        .main-header {{
            background: linear-gradient(135deg, {NAVY} 0%, {TEAL} 100%);
            padding: 1.5rem 2rem;
            border-radius: 10px;
            margin-bottom: 1.5rem;
        }}
        .main-header h1 {{
            color: white !important;
            margin: 0 !important;
            font-size: 1.8rem !important;
        }}
        .main-header p {{
            color: rgba(255,255,255,0.85) !important;
            margin: 0.3rem 0 0 0 !important;
            font-size: 0.95rem !important;
        }}
        /* Metric cards */
        div[data-testid="stMetric"] {{
            background-color: {LIGHT_BG};
            border-left: 4px solid {TEAL};
            padding: 0.8rem 1rem;
            border-radius: 6px;
        }}
        div[data-testid="stMetric"] label {{
            color: {NAVY} !important;
        }}
        /* Sidebar styling */
        section[data-testid="stSidebar"] {{
            background-color: {LIGHT_BG};
        }}
        section[data-testid="stSidebar"] h1 {{
            color: {NAVY} !important;
            font-size: 1.1rem !important;
        }}
    </style>
    """,
    unsafe_allow_html=True,
)

# ── Header ───────────────────────────────────────────────
st.markdown(
    """
    <div class="main-header">
        <h1>🏭 Morocco Industry Intelligence Monitor</h1>
        <p>Open-source platform mapping the Moroccan industrial landscape — players, market share, partnerships, and local integration rates.</p>
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
    st.markdown(f"### Filters")

    # ── Sector ──
    all_sectors = sorted(df_companies["sector_name"].dropna().unique().tolist())
    selected_sectors = st.multiselect(
        "Sector",
        options=all_sectors,
        default=all_sectors,
        help="Filter companies by industry sector",
    )

    # ── City ──
    all_cities = sorted(df_companies["headquarters_city"].dropna().unique().tolist())
    selected_cities = st.multiselect(
        "City",
        options=all_cities,
        default=all_cities,
    )

    # ── Ownership ──
    all_ownership = sorted(df_companies["ownership_type"].dropna().unique().tolist())
    selected_ownership = st.multiselect(
        "Ownership Type",
        options=all_ownership,
        default=all_ownership,
    )

    # ── Tier ──
    all_tiers = sorted(df_companies["tier_level"].dropna().unique().tolist())
    selected_tiers = st.multiselect(
        "Tier Level",
        options=all_tiers,
        default=all_tiers,
    )

    # ── Search ──
    st.markdown("---")
    search_query = st.text_input(
        "🔍 Search companies",
        placeholder="e.g. Renault, Yazaki...",
    )

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
# Only apply a filter if the user has actively narrowed the selection
# (i.e., deselected some options). When all options are selected or none
# are selected, show everything — including companies with NULL values.

mask = pd.Series(True, index=df_companies.index)

if selected_sectors and len(selected_sectors) < len(all_sectors):
    mask = mask & (
        df_companies["sector_name"].isin(selected_sectors)
        | df_companies["sector_name"].isna()
    )

if selected_cities and len(selected_cities) < len(all_cities):
    mask = mask & (
        df_companies["headquarters_city"].isin(selected_cities)
        | df_companies["headquarters_city"].isna()
    )

if selected_ownership and len(selected_ownership) < len(all_ownership):
    mask = mask & (
        df_companies["ownership_type"].isin(selected_ownership)
        | df_companies["ownership_type"].isna()
    )

if selected_tiers and len(selected_tiers) < len(all_tiers):
    mask = mask & (
        df_companies["tier_level"].isin(selected_tiers)
        | df_companies["tier_level"].isna()
    )

if search_query:
    mask = mask & df_companies["company_name"].str.contains(
        search_query, case=False, na=False
    )

df_filtered = df_companies[mask].copy()


# ══════════════════════════════════════════════════════════
#  KPI METRICS ROW
# ══════════════════════════════════════════════════════════

col1, col2, col3, col4 = st.columns(4)
with col1:
    st.metric("Companies Tracked", len(df_filtered))
with col2:
    n_sectors = df_filtered["sector_name"].nunique() if not df_filtered.empty else 0
    st.metric("Sectors Covered", n_sectors)
with col3:
    total_employees = (
        df_filtered["employee_count"].astype(float).sum()
        if not df_filtered.empty
        else 0
    )
    st.metric("Total Employees", f"{total_employees:,.0f}")
with col4:
    n_partnerships = len(df_partnerships) if not df_partnerships.empty else 0
    st.metric("Active Partnerships", n_partnerships)


# ══════════════════════════════════════════════════════════
#  TAB LAYOUT
# ══════════════════════════════════════════════════════════

tab_table, tab_chart, tab_network, tab_map, tab_events, tab_review = st.tabs(
    ["📋 Company Directory", "📊 Integration Analysis", "🔗 Partnership Network", "🗺️ Map", "📰 Events", "🔍 Review Queue"]
)

# ─── TAB 1: Company Directory Table ─────────────────────
with tab_table:
    st.markdown(f"#### Company Directory ({len(df_filtered)} results)")

    display_cols = {
        "company_name": "Company Name",
        "sector_name": "Sector",
        "sub_sector": "Sub-Sector",
        "headquarters_city": "City",
        "ownership_type": "Ownership",
        "employee_count": "Employees",
        "parent_company": "Parent Company",
        "website_url": "Website",
        "updated_at": "Last Updated",
    }

    df_display = df_filtered[
        [c for c in display_cols.keys() if c in df_filtered.columns]
    ].copy()
    df_display.rename(columns=display_cols, inplace=True)

    if "Employees" in df_display.columns:
        df_display["Employees"] = (
            df_display["Employees"]
            .astype(float)
            .map(lambda x: f"{x:,.0f}" if pd.notna(x) else "—")
        )
    if "Last Updated" in df_display.columns:
        df_display["Last Updated"] = pd.to_datetime(
            df_display["Last Updated"]
        ).dt.strftime("%Y-%m-%d")

    st.dataframe(
        df_display,
        use_container_width=True,
        hide_index=True,
        height=min(400, 50 + len(df_display) * 35),
    )


# ─── TAB 2: Sector Analysis ──────────────────────────────
with tab_chart:
    st.markdown("#### Sector Overview")
    st.caption("Integration rates are measured at the industry/sector level — not per company.")

    col_left, col_right = st.columns([3, 2])

    with col_left:
        st.markdown("##### Companies per Sector")

        if not df_filtered.empty:
            sector_counts = (
                df_filtered.groupby("sector_name")
                .agg(count=("company_name", "count"))
                .reset_index()
                .sort_values("count", ascending=True)
            )

            fig_bar = px.bar(
                sector_counts,
                x="count",
                y="sector_name",
                orientation="h",
                text="count",
                labels={"count": "Number of Companies", "sector_name": ""},
                color="count",
                color_continuous_scale=[[0, SAND], [1, TEAL]],
            )
            fig_bar.update_traces(textposition="outside")
            fig_bar.update_layout(
                showlegend=False,
                coloraxis_showscale=False,
                plot_bgcolor="white",
                paper_bgcolor="white",
                xaxis=dict(gridcolor="#E8E8E8"),
                yaxis=dict(gridcolor="#E8E8E8"),
                margin=dict(l=20, r=80, t=20, b=40),
                height=350,
                font=dict(family="Arial", color=NAVY),
            )
            st.plotly_chart(fig_bar, use_container_width=True)
        else:
            st.info("No data matches the current filters.")

    with col_right:
        st.markdown("##### Sector Integration Targets")
        st.caption("Government targets for local integration by sector.")

        # Show sector-level integration targets from the sectors table
        if not df_filtered.empty and "sector_target_pct" in df_filtered.columns:
            sector_targets = (
                df_filtered.groupby("sector_name")
                .agg(target_pct=("sector_target_pct", "first"))
                .reset_index()
                .dropna(subset=["target_pct"])
                .sort_values("target_pct", ascending=True)
            )
            if not sector_targets.empty:
                sector_targets["target_pct"] = sector_targets["target_pct"].astype(float)
                fig_target = px.bar(
                    sector_targets,
                    x="target_pct",
                    y="sector_name",
                    orientation="h",
                    text=sector_targets["target_pct"].map(lambda x: f"{x:.0f}%"),
                    labels={"target_pct": "Integration Target (%)", "sector_name": ""},
                    color="target_pct",
                    color_continuous_scale=[[0, CORAL], [0.5, SAND], [1, TEAL]],
                )
                fig_target.update_traces(textposition="outside")
                fig_target.update_layout(
                    showlegend=False,
                    coloraxis_showscale=False,
                    plot_bgcolor="white",
                    paper_bgcolor="white",
                    xaxis=dict(range=[0, 100], gridcolor="#E8E8E8"),
                    margin=dict(l=20, r=60, t=20, b=40),
                    height=350,
                    font=dict(family="Arial", color=NAVY),
                )
                st.plotly_chart(fig_target, use_container_width=True)
            else:
                st.info("No integration targets set for these sectors yet.")

    # ── Companies by ownership type ──
    st.markdown("##### Companies by Ownership Type")
    if not df_filtered.empty:
        ownership_counts = (
            df_filtered["ownership_type"]
            .fillna("Unknown")
            .value_counts()
            .reset_index()
            .rename(columns={"index": "ownership_type", "count": "count"})
        )
        if "ownership_type" in ownership_counts.columns and "count" in ownership_counts.columns:
            fig_pie = px.pie(
                ownership_counts,
                names="ownership_type",
                values="count",
                hole=0.4,
                color_discrete_sequence=[TEAL, NAVY, SAND, CORAL, GOLD, "#AAAAAA", "#7FB3D8"],
            )
            fig_pie.update_layout(
                margin=dict(l=20, r=20, t=20, b=20),
                height=350,
                font=dict(family="Arial", color=NAVY),
                paper_bgcolor="white",
            )
            fig_pie.update_traces(
                textinfo="label+percent",
                textposition="outside",
            )
            st.plotly_chart(fig_pie, use_container_width=True)


# ─── TAB 3: Partnership Network Graph ───────────────────
with tab_network:
    st.markdown("#### Partnership Network")
    st.caption("Nodes = companies (colored by tier), edges = active partnerships.")

    if not df_partnerships.empty:
        G = nx.Graph()

        # Add company nodes
        all_node_names = set()
        for _, row in df_partnerships.iterrows():
            for side, tier_col in [("company_a_name", "company_a_tier"), ("company_b_name", "company_b_tier")]:
                name = row[side]
                if name not in all_node_names:
                    tier = row[tier_col]
                    G.add_node(name, tier=tier, color=TIER_COLORS.get(tier, "#AAAAAA"))
                    all_node_names.add(name)

        # Add edges
        for _, row in df_partnerships.iterrows():
            G.add_edge(
                row["company_a_name"],
                row["company_b_name"],
                label=row["partnership_type"],
                description=row.get("description", ""),
            )

        # Use Plotly for the network graph (more reliable than pyvis in Streamlit)
        pos = nx.spring_layout(G, seed=42, k=2)

        # Edges
        edge_x, edge_y = [], []
        edge_labels_x, edge_labels_y, edge_texts = [], [], []
        for u, v, data in G.edges(data=True):
            x0, y0 = pos[u]
            x1, y1 = pos[v]
            edge_x.extend([x0, x1, None])
            edge_y.extend([y0, y1, None])
            mx, my = (x0 + x1) / 2, (y0 + y1) / 2
            edge_labels_x.append(mx)
            edge_labels_y.append(my)
            edge_texts.append(data.get("label", ""))

        edge_trace = go.Scatter(
            x=edge_x, y=edge_y,
            line=dict(width=2, color="#B0C4D8"),
            hoverinfo="none",
            mode="lines",
        )

        edge_label_trace = go.Scatter(
            x=edge_labels_x, y=edge_labels_y,
            text=edge_texts,
            mode="text",
            textfont=dict(size=9, color="#888888"),
            hoverinfo="none",
        )

        # Nodes
        node_x = [pos[n][0] for n in G.nodes()]
        node_y = [pos[n][1] for n in G.nodes()]
        node_colors = [G.nodes[n].get("color", "#AAAAAA") for n in G.nodes()]
        node_names = list(G.nodes())
        node_tiers = [G.nodes[n].get("tier", "Unknown") for n in G.nodes()]
        node_sizes = []
        for n in G.nodes():
            degree = G.degree(n)
            node_sizes.append(25 + degree * 10)

        node_trace = go.Scatter(
            x=node_x, y=node_y,
            mode="markers+text",
            text=node_names,
            textposition="top center",
            textfont=dict(size=11, color=NAVY, family="Arial"),
            hovertext=[
                f"<b>{name}</b><br>Tier: {tier}<br>Connections: {G.degree(name)}"
                for name, tier in zip(node_names, node_tiers)
            ],
            hoverinfo="text",
            marker=dict(
                size=node_sizes,
                color=node_colors,
                line=dict(width=2, color="white"),
            ),
        )

        fig_network = go.Figure(
            data=[edge_trace, edge_label_trace, node_trace],
            layout=go.Layout(
                showlegend=False,
                hovermode="closest",
                xaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
                yaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
                plot_bgcolor="white",
                paper_bgcolor="white",
                margin=dict(l=20, r=20, t=20, b=20),
                height=500,
            ),
        )
        st.plotly_chart(fig_network, use_container_width=True)

        # Legend
        st.markdown(
            f"""
            <div style="display:flex; gap:1.5rem; flex-wrap:wrap; margin-top:0.5rem;">
                <span style="display:flex;align-items:center;gap:0.3rem;"><span style="width:14px;height:14px;border-radius:50%;background:{NAVY};display:inline-block;"></span> OEM</span>
                <span style="display:flex;align-items:center;gap:0.3rem;"><span style="width:14px;height:14px;border-radius:50%;background:{TEAL};display:inline-block;"></span> Tier 1</span>
                <span style="display:flex;align-items:center;gap:0.3rem;"><span style="width:14px;height:14px;border-radius:50%;background:{SAND};display:inline-block;"></span> Tier 2</span>
                <span style="display:flex;align-items:center;gap:0.3rem;"><span style="width:14px;height:14px;border-radius:50%;background:{CORAL};display:inline-block;"></span> Tier 3</span>
            </div>
            """,
            unsafe_allow_html=True,
        )
    else:
        st.info("No partnerships data available.")


# ─── TAB 4: Map of Morocco ──────────────────────────────
with tab_map:
    st.markdown("#### Industrial Map of Morocco")
    st.caption("Dot size proportional to employee count. Click markers for details.")

    if not df_filtered.empty:
        # Aggregate by city
        city_data = (
            df_filtered.groupby("headquarters_city")
            .agg(
                total_employees=("employee_count", lambda x: x.astype(float).sum()),
                company_count=("company_name", "count"),
                companies_list=("company_name", lambda x: ", ".join(x.tolist())),
                avg_integration=(
                    "local_integration_pct",
                    lambda x: x.astype(float).mean(),
                ),
            )
            .reset_index()
        )

        # Center on Morocco
        m = folium.Map(
            location=[33.0, -6.5],
            zoom_start=6,
            tiles="CartoDB positron",
        )

        for _, row in city_data.iterrows():
            city = row["headquarters_city"]
            coords = CITY_COORDS.get(city)
            if not coords:
                continue

            # Scale radius: min 8, max 40, based on employee count
            max_emp = city_data["total_employees"].max() if city_data["total_employees"].max() > 0 else 1
            radius = 8 + (row["total_employees"] / max_emp) * 32

            popup_html = f"""
            <div style="font-family:Arial; width:220px;">
                <h4 style="color:{NAVY}; margin:0 0 8px 0;">{city}</h4>
                <b>Companies:</b> {row['company_count']}<br>
                <b>Employees:</b> {row['total_employees']:,.0f}<br>
                <b>Avg Integration:</b> {row['avg_integration']:.1f}%<br>
                <hr style="margin:6px 0;">
                <small>{row['companies_list']}</small>
            </div>
            """

            folium.CircleMarker(
                location=coords,
                radius=radius,
                color=NAVY,
                fill=True,
                fill_color=TEAL,
                fill_opacity=0.7,
                weight=2,
                popup=folium.Popup(popup_html, max_width=250),
                tooltip=f"{city}: {row['company_count']} companies, {row['total_employees']:,.0f} employees",
            ).add_to(m)

        st_folium(m, use_container_width=True, height=550)
    else:
        st.info("No data matches the current filters.")


# ─── TAB 5: Recent Events ───────────────────────────────
with tab_events:
    st.markdown("#### Recent Industrial Events")

    if not df_events.empty:
        for _, ev in df_events.head(10).iterrows():
            event_icon = {
                "New Factory": "🏗️",
                "Partnership": "🤝",
                "Investment": "💰",
                "Acquisition": "🔄",
                "Export Milestone": "📦",
                "Expansion": "📈",
                "Government Incentive": "🏛️",
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
                <div style="background:{LIGHT_BG}; padding:1rem; border-radius:8px;
                            border-left:4px solid {TEAL}; margin-bottom:0.8rem;">
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


# ─── TAB 6: Review Queue ─────────────────────────────────
with tab_review:
    from review_ui.review_helpers import load_review_items, get_review_stats, approve_item, reject_item, get_pipeline_stats

    st.markdown("#### Human-in-the-Loop Review Queue")
    st.markdown("Review and approve low-confidence extractions before they enter the database.")

    sb = get_supabase_client()

    # ── Pipeline stats row ──
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

    # ── Review stats row ──
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

    # ── Load pending items ──
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
                # ── Article info ──
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

                # ── Extracted data ──
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
                    st.markdown(f"- **Summary**: {ext.get('source_summary', 'N/A')}")

                # ── Actions ──
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

    # ── Recent scraper runs ──
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
            Built with Streamlit, Supabase, and Plotly
        </span>
    </div>
    """,
    unsafe_allow_html=True,
)
