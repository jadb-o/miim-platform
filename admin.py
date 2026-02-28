"""
MIIM Admin Dashboard — Full CRUD for all tables + pipeline visibility.
Run: streamlit run admin.py --server.port=8502
"""

import os
import json
import streamlit as st
import pandas as pd
from datetime import datetime
from supabase import create_client

from admin_helpers import (
    load_companies_admin, create_company, update_company, delete_company,
    load_sectors_admin, get_sector_options, create_sector, update_sector, delete_sector,
    load_relationships_admin, create_relationship, delete_relationship,
    load_people_admin, create_person, delete_person,
    load_events_admin, create_event, update_event, delete_event,
    load_articles_admin, get_article_text, get_article_sources,
    load_extractions_admin, get_extraction_data,
    load_review_queue_admin, approve_review, reject_review,
    load_scraper_runs_admin,
    load_pipeline_costs_admin, get_cost_summary,
    get_overview_stats, get_company_options,
)

# ── Page config ──────────────────────────────────────────
st.set_page_config(page_title="MIIM Admin", page_icon="⚙️", layout="wide")

# ── Color palette (match app.py) ─────────────────────────
NAVY = "#1B3A5C"
TEAL = "#2A9D8F"
CORAL = "#E76F51"
SAND = "#F4A261"
GOLD = "#E9C46A"

OWNERSHIP_TYPES = ["Moroccan Private", "Foreign Private", "State-Owned", "Joint Venture", "Multinational", "Public", "Unknown"]
TIER_LEVELS = ["OEM", "Tier 1", "Tier 2", "Tier 3", "Unknown"]
RELATIONSHIP_TYPES = ["partner", "client", "supplier", "subsidiary", "parent", "investor", "joint_venture", "competitor"]
EVENT_TYPES = ["New Factory", "Partnership", "Investment", "Acquisition", "Export Milestone", "Expansion", "Closure", "IPO", "Other"]
ROLE_TYPES = ["CEO", "CFO", "COO", "CTO", "Founder", "Director", "Board", "Manager", "Other"]

# ── Supabase client ──────────────────────────────────────
SUPABASE_URL = os.environ.get("SUPABASE_URL", "https://rkqfjesnavbngtihffge.supabase.co")
SUPABASE_ANON_KEY = os.environ.get(
    "SUPABASE_ANON_KEY",
    "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InJrcWZqZXNuYXZibmd0aWhmZmdlIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NzIxNTY0NzIsImV4cCI6MjA4NzczMjQ3Mn0.Djfr1UI1XzrUQmvo2rNi3rvMQIC0GXrMHpZiPnG6zfE",
)


@st.cache_resource
def get_sb():
    return create_client(SUPABASE_URL, SUPABASE_ANON_KEY)


sb = get_sb()

# ── Auth ─────────────────────────────────────────────────
if "admin_auth" not in st.session_state:
    st.session_state.admin_auth = False

if not st.session_state.admin_auth:
    st.markdown(f"""
    <div style="max-width:400px;margin:5rem auto;text-align:center;">
        <h2 style="color:{NAVY};">⚙️ MIIM Admin</h2>
        <p style="color:#666;">Enter admin password to continue.</p>
    </div>
    """, unsafe_allow_html=True)
    pwd = st.text_input("Password", type="password", key="admin_pwd")
    if st.button("Login", use_container_width=True):
        if pwd == os.environ.get("MIIM_ADMIN_PASSWORD", "miim2026"):
            st.session_state.admin_auth = True
            st.rerun()
        else:
            st.error("Incorrect password.")
    st.stop()

# ── Global styles ────────────────────────────────────────
st.markdown(f"""
<style>
    .block-container {{ padding-top: 1rem; }}
    [data-testid="stMetric"] {{
        background: white; padding: 1rem; border-radius: 10px;
        border-left: 4px solid {TEAL}; box-shadow: 0 1px 4px rgba(0,0,0,0.06);
    }}
    .stTabs [data-baseweb="tab-list"] {{ gap: 0.5rem; }}
    .stTabs [data-baseweb="tab"] {{
        border-radius: 8px 8px 0 0; padding: 0.6rem 1rem;
        font-weight: 600; color: {NAVY};
    }}
</style>
""", unsafe_allow_html=True)

# ── Header ───────────────────────────────────────────────
st.markdown(f"""
<div style="background:linear-gradient(135deg,{NAVY} 0%,{TEAL} 100%);
            padding:1.5rem 2rem;border-radius:14px;margin-bottom:1.5rem;">
    <h2 style="color:white;margin:0;">⚙️ MIIM Admin Dashboard</h2>
    <p style="color:rgba(255,255,255,0.8);margin:0.3rem 0 0;">
        Manage companies, sectors, relationships, events & pipeline data
    </p>
</div>
""", unsafe_allow_html=True)

# ── Tabs ─────────────────────────────────────────────────
tab_overview, tab_companies, tab_sectors, tab_rels, tab_people, tab_events, tab_scraped = st.tabs(
    ["📊 Overview", "🏢 Companies", "🏭 Sectors", "🔗 Relationships", "👔 People", "📅 Events", "📰 Scraped Data"]
)


# ═══════════════════════════════════════════════════════════
# TAB 1: Overview
# ═══════════════════════════════════════════════════════════
with tab_overview:
    stats = get_overview_stats(sb)
    m1, m2, m3, m4 = st.columns(4)
    m1.metric("🏢 Companies", stats["total_companies"])
    m2.metric("📰 Articles Scraped", stats["total_articles"])
    m3.metric("⏳ Pending Reviews", stats["pending_reviews"])
    m4.metric("💰 Pipeline Cost", f"${stats['total_cost_usd']:.2f}")

    col_l, col_r = st.columns(2)
    with col_l:
        st.markdown("##### 🔄 Recent Scraper Runs")
        df_runs = load_scraper_runs_admin(sb, limit=10)
        if not df_runs.empty:
            st.dataframe(
                df_runs[["source_name", "run_date", "articles_found", "articles_new", "status"]],
                use_container_width=True, hide_index=True,
            )
        else:
            st.info("No scraper runs yet.")

    with col_r:
        st.markdown("##### 📰 Recent Articles")
        df_art = load_articles_admin(sb, limit=10)
        if not df_art.empty:
            st.dataframe(
                df_art[["title", "source_name", "published_date", "processing_status"]],
                use_container_width=True, hide_index=True,
            )
        else:
            st.info("No articles yet.")


# ═══════════════════════════════════════════════════════════
# TAB 2: Companies
# ═══════════════════════════════════════════════════════════
with tab_companies:
    # Helpers for this tab
    sectors_list = get_sector_options(sb)
    sector_map = {s["sector_id"]: s["sector_name"] for s in sectors_list}
    sector_names = ["All"] + [s["sector_name"] for s in sectors_list]

    # ── Filters
    fc1, fc2, fc3, fc4 = st.columns(4)
    search = fc1.text_input("🔍 Search", placeholder="Company name…", key="co_search")
    sec_filter = fc2.selectbox("Sector", sector_names, key="co_sec")
    own_filter = fc3.selectbox("Ownership", ["All"] + OWNERSHIP_TYPES, key="co_own")
    tier_filter = fc4.selectbox("Tier", ["All"] + TIER_LEVELS, key="co_tier")

    df_co = load_companies_admin(sb)
    if not df_co.empty:
        if search:
            df_co = df_co[df_co["company_name"].str.contains(search, case=False, na=False)]
        if sec_filter != "All":
            df_co = df_co[df_co["sector_name"] == sec_filter]
        if own_filter != "All":
            df_co = df_co[df_co["ownership_type"] == own_filter]
        if tier_filter != "All":
            df_co = df_co[df_co["tier_level"] == tier_filter]

        st.caption(f"Showing {len(df_co)} companies")

        # ── Data table (read-only display)
        display_cols = ["company_name", "sector_name", "headquarters_city", "employee_count",
                        "ownership_type", "tier_level", "data_confidence"]
        available = [c for c in display_cols if c in df_co.columns]
        st.dataframe(df_co[available], use_container_width=True, hide_index=True, height=400)

        # ── Edit per company
        st.markdown("---")
        st.markdown("##### ✏️ Edit Company")
        co_names = df_co["company_name"].tolist()
        sel_co = st.selectbox("Select company to edit", co_names, key="edit_co_select")

        if sel_co:
            row = df_co[df_co["company_name"] == sel_co].iloc[0]
            co_id = row["company_id"]

            with st.form(f"edit_co_{co_id}"):
                e1, e2 = st.columns(2)
                name = e1.text_input("Company Name", value=row.get("company_name", ""))
                city = e2.text_input("City", value=row.get("headquarters_city", "") or "")

                e3, e4 = st.columns(2)
                cur_sector = row.get("sector_name", "")
                sec_idx = ([s["sector_name"] for s in sectors_list].index(cur_sector) if cur_sector in [s["sector_name"] for s in sectors_list] else 0)
                sector_sel = e3.selectbox("Sector", [s["sector_name"] for s in sectors_list], index=sec_idx)
                sector_id = next((s["sector_id"] for s in sectors_list if s["sector_name"] == sector_sel), None)
                own = e4.selectbox("Ownership", OWNERSHIP_TYPES, index=OWNERSHIP_TYPES.index(row["ownership_type"]) if row.get("ownership_type") in OWNERSHIP_TYPES else 6)

                e5, e6, e7, e8 = st.columns(4)
                emp = e5.number_input("Employees", value=int(row["employee_count"]) if pd.notna(row.get("employee_count")) else 0, min_value=0)
                rev = e6.number_input("Revenue (MAD)", value=float(row["annual_revenue_mad"]) if pd.notna(row.get("annual_revenue_mad")) else 0.0, min_value=0.0)
                inv = e7.number_input("Investment (MAD)", value=float(row["investment_amount_mad"]) if pd.notna(row.get("investment_amount_mad")) else 0.0, min_value=0.0)
                cap = e8.number_input("Capital (MAD)", value=float(row["capital_mad"]) if pd.notna(row.get("capital_mad")) else 0.0, min_value=0.0)

                e9, e10 = st.columns(2)
                tier = e9.selectbox("Tier", TIER_LEVELS, index=TIER_LEVELS.index(row["tier_level"]) if row.get("tier_level") in TIER_LEVELS else 4)
                website = e10.text_input("Website", value=row.get("website_url", "") or "")

                desc = st.text_area("Description", value=row.get("description", "") or "", height=80)
                activities = st.text_area("Activities", value=row.get("activities", "") or "", height=80)
                parent = st.text_input("Parent Company", value=row.get("parent_company", "") or "")
                sub = st.text_input("Sub-sector", value=row.get("sub_sector", "") or "")

                bc1, bc2 = st.columns(2)
                save = bc1.form_submit_button("💾 Save Changes", type="primary")
                del_check = bc2.checkbox("🗑️ Delete this company")

            if save:
                if del_check:
                    if delete_company(sb, co_id):
                        st.success(f"Deleted {sel_co}")
                        st.rerun()
                    else:
                        st.error("Delete failed.")
                else:
                    data = {
                        "company_name": name, "sector_id": sector_id, "headquarters_city": city or None,
                        "ownership_type": own, "tier_level": tier, "employee_count": emp or None,
                        "annual_revenue_mad": rev or None, "investment_amount_mad": inv or None,
                        "capital_mad": cap or None, "website_url": website or None,
                        "description": desc or None, "activities": activities or None,
                        "parent_company": parent or None, "sub_sector": sub or None,
                    }
                    if update_company(sb, co_id, data):
                        st.success(f"Updated {name}")
                        st.rerun()
                    else:
                        st.error("Update failed.")
    else:
        st.info("No companies found.")

    # ── Add new company
    st.markdown("---")
    st.markdown("##### ➕ Add New Company")
    with st.form("add_co_form"):
        a1, a2 = st.columns(2)
        new_name = a1.text_input("Company Name*", key="new_co_name")
        new_city = a2.text_input("City", key="new_co_city")
        a3, a4 = st.columns(2)
        new_sec = a3.selectbox("Sector*", [s["sector_name"] for s in sectors_list], key="new_co_sec")
        new_own = a4.selectbox("Ownership", OWNERSHIP_TYPES, key="new_co_own")
        a5, a6 = st.columns(2)
        new_emp = a5.number_input("Employees", min_value=0, value=0, key="new_co_emp")
        new_tier = a6.selectbox("Tier", TIER_LEVELS, key="new_co_tier")

        if st.form_submit_button("➕ Create Company", type="primary"):
            if not new_name.strip():
                st.error("Company name is required.")
            else:
                new_sec_id = next((s["sector_id"] for s in sectors_list if s["sector_name"] == new_sec), None)
                result = create_company(sb, {
                    "company_name": new_name.strip(), "sector_id": new_sec_id,
                    "headquarters_city": new_city or None, "ownership_type": new_own,
                    "tier_level": new_tier, "employee_count": new_emp or None,
                })
                if result:
                    st.success(f"Created {new_name}")
                    st.rerun()
                else:
                    st.error("Failed to create company.")


# ═══════════════════════════════════════════════════════════
# TAB 3: Sectors
# ═══════════════════════════════════════════════════════════
with tab_sectors:
    df_sec = load_sectors_admin(sb)
    if not df_sec.empty:
        st.dataframe(
            df_sec[["sector_name", "target_integration_pct", "current_integration_pct", "government_strategy", "source_url"]],
            use_container_width=True, hide_index=True,
        )

        st.markdown("---")
        st.markdown("##### ✏️ Edit Sector")
        sec_names = df_sec["sector_name"].tolist()
        sel_sec = st.selectbox("Select sector", sec_names, key="edit_sec")

        if sel_sec:
            srow = df_sec[df_sec["sector_name"] == sel_sec].iloc[0]
            sid = srow["sector_id"]

            with st.form(f"edit_sec_{sid}"):
                sn = st.text_input("Sector Name", value=srow["sector_name"])
                s1, s2 = st.columns(2)
                tgt = s1.number_input("Target Integration %", value=float(srow["target_integration_pct"]) if pd.notna(srow.get("target_integration_pct")) else 0.0, min_value=0.0, max_value=100.0)
                cur = s2.number_input("Current Integration %", value=float(srow["current_integration_pct"]) if pd.notna(srow.get("current_integration_pct")) else 0.0, min_value=0.0, max_value=100.0)
                strat = st.text_area("Government Strategy", value=srow.get("government_strategy", "") or "", height=80)
                surl = st.text_input("Source URL", value=srow.get("source_url", "") or "")
                sname_det = st.text_input("Source Name", value=srow.get("source_name_detail", "") or "")

                sc1, sc2 = st.columns(2)
                save_sec = sc1.form_submit_button("💾 Save", type="primary")
                del_sec = sc2.checkbox("🗑️ Delete sector")

            if save_sec:
                if del_sec:
                    if delete_sector(sb, sid):
                        st.success(f"Deleted {sel_sec}")
                        st.rerun()
                    else:
                        st.error("Delete failed.")
                else:
                    if update_sector(sb, sid, {
                        "sector_name": sn, "target_integration_pct": tgt,
                        "current_integration_pct": cur, "government_strategy": strat or None,
                        "source_url": surl or None, "source_name_detail": sname_det or None,
                    }):
                        st.success(f"Updated {sn}")
                        st.rerun()
                    else:
                        st.error("Update failed.")
    else:
        st.info("No sectors found.")

    # ── Add sector
    st.markdown("---")
    st.markdown("##### ➕ Add Sector")
    with st.form("add_sec_form"):
        ns_name = st.text_input("Sector Name*", key="new_sec_name")
        ns1, ns2 = st.columns(2)
        ns_tgt = ns1.number_input("Target %", 0.0, 100.0, 0.0, key="new_sec_tgt")
        ns_cur = ns2.number_input("Current %", 0.0, 100.0, 0.0, key="new_sec_cur")

        if st.form_submit_button("➕ Create Sector", type="primary"):
            if not ns_name.strip():
                st.error("Sector name required.")
            else:
                if create_sector(sb, {"sector_name": ns_name.strip(), "target_integration_pct": ns_tgt, "current_integration_pct": ns_cur}):
                    st.success(f"Created {ns_name}")
                    st.rerun()
                else:
                    st.error("Failed.")


# ═══════════════════════════════════════════════════════════
# TAB 4: Relationships
# ═══════════════════════════════════════════════════════════
with tab_rels:
    df_rel = load_relationships_admin(sb)
    if not df_rel.empty:
        display_rel = df_rel[["source_name", "target_name", "relationship_type", "confidence_score", "status", "description", "id"]].copy()
        display_rel.columns = ["Source", "Target", "Type", "Confidence", "Status", "Description", "id"]
        st.dataframe(display_rel.drop(columns=["id"]), use_container_width=True, hide_index=True, height=350)

        # Delete
        st.markdown("##### 🗑️ Delete Relationship")
        rel_labels = [f"{r['Source']} → {r['Target']} ({r['Type']})" for _, r in display_rel.iterrows()]
        sel_rel = st.selectbox("Select", rel_labels, key="del_rel")
        if sel_rel and st.button("Delete Selected Relationship"):
            idx = rel_labels.index(sel_rel)
            rid = display_rel.iloc[idx]["id"]
            if delete_relationship(sb, rid):
                st.success("Deleted.")
                st.rerun()
    else:
        st.info("No relationships found.")

    # ── Add relationship
    st.markdown("---")
    st.markdown("##### ➕ Add Relationship")
    co_opts = get_company_options(sb)
    co_labels = [c["company_name"] for c in co_opts]

    with st.form("add_rel_form"):
        r1, r2 = st.columns(2)
        src_co = r1.selectbox("Source Company*", co_labels, key="rel_src")
        tgt_co = r2.selectbox("Target Company*", co_labels, key="rel_tgt")
        r3, r4 = st.columns(2)
        rel_type = r3.selectbox("Type*", RELATIONSHIP_TYPES, key="rel_type")
        rel_conf = r4.slider("Confidence", 0.0, 1.0, 0.7, key="rel_conf")
        rel_desc = st.text_input("Description", key="rel_desc")
        rel_url = st.text_input("Source URL", key="rel_url")

        if st.form_submit_button("➕ Add Relationship", type="primary"):
            src_id = next((c["company_id"] for c in co_opts if c["company_name"] == src_co), None)
            tgt_id = next((c["company_id"] for c in co_opts if c["company_name"] == tgt_co), None)
            if src_id == tgt_id:
                st.error("Source and target must be different.")
            elif src_id and tgt_id:
                if create_relationship(sb, {
                    "source_company_id": src_id, "target_company_id": tgt_id,
                    "relationship_type": rel_type, "confidence_score": rel_conf,
                    "status": "active", "description": rel_desc or None,
                    "source_url": rel_url or None,
                }):
                    st.success("Added relationship.")
                    st.rerun()
                else:
                    st.error("Failed.")


# ═══════════════════════════════════════════════════════════
# TAB 5: People
# ═══════════════════════════════════════════════════════════
with tab_people:
    co_opts_p = get_company_options(sb)
    co_labels_p = ["All"] + [c["company_name"] for c in co_opts_p]
    filter_co = st.selectbox("Filter by company", co_labels_p, key="ppl_filter")

    co_id_filter = None
    if filter_co != "All":
        co_id_filter = next((c["company_id"] for c in co_opts_p if c["company_name"] == filter_co), None)

    df_ppl = load_people_admin(sb, company_id=co_id_filter)
    if not df_ppl.empty:
        display_ppl = df_ppl[["person_name", "role_title", "role_type", "company_name", "id"]].copy()
        st.dataframe(display_ppl.drop(columns=["id"]), use_container_width=True, hide_index=True, height=350)

        # Delete
        st.markdown("##### 🗑️ Delete Person")
        ppl_labels = [f"{r['person_name']} — {r['role_title']} ({r['company_name']})" for _, r in display_ppl.iterrows()]
        sel_ppl = st.selectbox("Select", ppl_labels, key="del_ppl")
        if sel_ppl and st.button("Delete Selected Person"):
            idx = ppl_labels.index(sel_ppl)
            pid = display_ppl.iloc[idx]["id"]
            if delete_person(sb, pid):
                st.success("Deleted.")
                st.rerun()
    else:
        st.info("No people found.")

    # ── Add person
    st.markdown("---")
    st.markdown("##### ➕ Add Person")
    with st.form("add_ppl_form"):
        p1, p2 = st.columns(2)
        ppl_co = p1.selectbox("Company*", [c["company_name"] for c in co_opts_p], key="ppl_co")
        ppl_name = p2.text_input("Person Name*", key="ppl_name")
        p3, p4 = st.columns(2)
        ppl_title = p3.text_input("Role Title*", key="ppl_title")
        ppl_type = p4.selectbox("Role Type", ROLE_TYPES, key="ppl_type")

        if st.form_submit_button("➕ Add Person", type="primary"):
            if not ppl_name.strip():
                st.error("Name required.")
            else:
                co_id_ins = next((c["company_id"] for c in co_opts_p if c["company_name"] == ppl_co), None)
                if co_id_ins and create_person(sb, {
                    "company_id": co_id_ins, "person_name": ppl_name.strip(),
                    "role_title": ppl_title.strip(), "role_type": ppl_type,
                }):
                    st.success(f"Added {ppl_name}")
                    st.rerun()
                else:
                    st.error("Failed.")


# ═══════════════════════════════════════════════════════════
# TAB 6: Events
# ═══════════════════════════════════════════════════════════
with tab_events:
    df_ev = load_events_admin(sb)
    if not df_ev.empty:
        display_ev = ["company_name", "event_type", "title", "event_date", "city", "investment_amount_mad", "confidence_score"]
        available_ev = [c for c in display_ev if c in df_ev.columns]
        st.dataframe(df_ev[available_ev], use_container_width=True, hide_index=True, height=400)

        # ── Edit event
        st.markdown("---")
        st.markdown("##### ✏️ Edit Event")
        ev_labels = [f"{r.get('title', '?')} ({r.get('company_name', '?')})" for _, r in df_ev.iterrows()]
        sel_ev = st.selectbox("Select event", ev_labels, key="edit_ev")

        if sel_ev:
            ev_idx = ev_labels.index(sel_ev)
            erow = df_ev.iloc[ev_idx]
            eid = erow["event_id"]

            with st.form(f"edit_ev_{eid}"):
                ev1, ev2 = st.columns(2)
                ev_title = ev1.text_input("Title", value=erow.get("title", ""))
                ev_type = ev2.selectbox("Type", EVENT_TYPES, index=EVENT_TYPES.index(erow["event_type"]) if erow.get("event_type") in EVENT_TYPES else 8)
                ev3, ev4 = st.columns(2)
                ev_city = ev3.text_input("City", value=erow.get("city", "") or "")
                ev_amt = ev4.number_input("Investment (MAD)", value=float(erow["investment_amount_mad"]) if pd.notna(erow.get("investment_amount_mad")) else 0.0, min_value=0.0)
                ev_desc = st.text_area("Description", value=erow.get("description", "") or "", height=80)
                ev_url = st.text_input("Source URL", value=erow.get("source_url", "") or "")
                ev_conf = st.slider("Confidence", 0.0, 1.0, float(erow["confidence_score"]) if pd.notna(erow.get("confidence_score")) else 0.5)

                evc1, evc2 = st.columns(2)
                save_ev = evc1.form_submit_button("💾 Save", type="primary")
                del_ev = evc2.checkbox("🗑️ Delete event")

            if save_ev:
                if del_ev:
                    if delete_event(sb, eid):
                        st.success("Deleted.")
                        st.rerun()
                else:
                    if update_event(sb, eid, {
                        "title": ev_title, "event_type": ev_type, "city": ev_city or None,
                        "investment_amount_mad": ev_amt or None, "description": ev_desc or None,
                        "source_url": ev_url or None, "confidence_score": ev_conf,
                    }):
                        st.success("Updated.")
                        st.rerun()
    else:
        st.info("No events found.")

    # ── Add event
    st.markdown("---")
    st.markdown("##### ➕ Add Event")
    co_opts_ev = get_company_options(sb)
    with st.form("add_ev_form"):
        ne1, ne2 = st.columns(2)
        ne_co = ne1.selectbox("Company*", [c["company_name"] for c in co_opts_ev], key="new_ev_co")
        ne_type = ne2.selectbox("Event Type*", EVENT_TYPES, key="new_ev_type")
        ne_title = st.text_input("Title*", key="new_ev_title")
        ne3, ne4 = st.columns(2)
        ne_city = ne3.text_input("City", key="new_ev_city")
        ne_amt = ne4.number_input("Investment (MAD)", min_value=0.0, value=0.0, key="new_ev_amt")
        ne_desc = st.text_area("Description", key="new_ev_desc")
        ne_url = st.text_input("Source URL", key="new_ev_url")

        if st.form_submit_button("➕ Add Event", type="primary"):
            if not ne_title.strip():
                st.error("Title required.")
            else:
                co_id_ev = next((c["company_id"] for c in co_opts_ev if c["company_name"] == ne_co), None)
                if co_id_ev and create_event(sb, {
                    "company_id": co_id_ev, "event_type": ne_type, "title": ne_title.strip(),
                    "city": ne_city or None, "investment_amount_mad": ne_amt or None,
                    "description": ne_desc or None, "source_url": ne_url or None,
                    "confidence_score": 1.0,
                }):
                    st.success(f"Created event: {ne_title}")
                    st.rerun()
                else:
                    st.error("Failed.")


# ═══════════════════════════════════════════════════════════
# TAB 7: Scraped Data
# ═══════════════════════════════════════════════════════════
with tab_scraped:
    sub_articles, sub_extractions, sub_review, sub_runs, sub_costs = st.tabs(
        ["📰 Articles", "🔍 Extractions", "✅ Review Queue", "🔄 Scraper Runs", "💰 Pipeline Costs"]
    )

    # ── Articles ──────────────────────────────────────────
    with sub_articles:
        ac1, ac2 = st.columns(2)
        sources = get_article_sources(sb)
        art_src = ac1.selectbox("Source", ["All"] + sources, key="art_src")
        art_status = ac2.selectbox("Status", ["All", "pending", "extracted", "reviewed", "failed", "skipped"], key="art_status")

        df_articles = load_articles_admin(sb, source_filter=art_src if art_src != "All" else None, status_filter=art_status if art_status != "All" else None)
        if not df_articles.empty:
            st.caption(f"{len(df_articles)} articles")
            st.dataframe(
                df_articles[["title", "source_name", "published_date", "processing_status", "language"]],
                use_container_width=True, hide_index=True, height=400,
            )

            # Expand to view full text
            st.markdown("##### 📖 View Article Text")
            art_titles = df_articles["title"].tolist()
            sel_art = st.selectbox("Select article", art_titles, key="view_art")
            if sel_art:
                art_id = df_articles[df_articles["title"] == sel_art].iloc[0]["id"]
                text = get_article_text(sb, art_id)
                if text:
                    st.text_area("Full Text", value=text, height=300, disabled=True, key="art_text_view")
                else:
                    st.info("No text available.")
        else:
            st.info("No articles match filters.")

    # ── Extractions ───────────────────────────────────────
    with sub_extractions:
        df_ext = load_extractions_admin(sb)
        if not df_ext.empty:
            display_ext = ["article_title", "source_name", "model_used", "confidence_score", "input_tokens", "output_tokens", "created_at"]
            available_ext = [c for c in display_ext if c in df_ext.columns]
            st.dataframe(df_ext[available_ext], use_container_width=True, hide_index=True, height=350)

            # View extraction JSON
            st.markdown("##### 🔍 View Extraction Data")
            ext_labels = [f"{r.get('article_title', '?')[:50]} (conf: {r.get('confidence_score', 0):.2f})" for _, r in df_ext.iterrows()]
            sel_ext = st.selectbox("Select extraction", ext_labels, key="view_ext")
            if sel_ext:
                ext_idx = ext_labels.index(sel_ext)
                ext_id = df_ext.iloc[ext_idx]["id"]
                ext_data = get_extraction_data(sb, ext_id)
                if ext_data:
                    st.json(ext_data)
                else:
                    st.info("No extraction data.")
        else:
            st.info("No extraction results yet.")

    # ── Review Queue ──────────────────────────────────────
    with sub_review:
        rq_status = st.selectbox("Status", ["pending", "approved", "rejected"], key="rq_status")
        df_rq = load_review_queue_admin(sb, status=rq_status)

        if not df_rq.empty:
            st.caption(f"{len(df_rq)} items ({rq_status})")

            for idx, rq_row in df_rq.iterrows():
                with st.expander(f"📄 {rq_row.get('article_title', 'Unknown')} — conf: {rq_row.get('confidence_score', 0):.2f}"):
                    st.markdown(f"**Source**: {rq_row.get('source_name', '?')} | **Reason**: {rq_row.get('reason_flagged', '?')}")

                    # Show extracted data
                    ext = rq_row.get("extracted_data")
                    if ext:
                        if isinstance(ext, str):
                            try:
                                ext = json.loads(ext)
                            except Exception:
                                pass
                        st.json(ext)

                    if rq_status == "pending":
                        bc1, bc2 = st.columns(2)
                        with bc1:
                            if st.button("✅ Approve", key=f"approve_{rq_row['id']}"):
                                if approve_review(sb, rq_row["id"]):
                                    st.success("Approved!")
                                    st.rerun()
                        with bc2:
                            if st.button("❌ Reject", key=f"reject_{rq_row['id']}"):
                                if reject_review(sb, rq_row["id"]):
                                    st.success("Rejected.")
                                    st.rerun()
        else:
            st.info(f"No {rq_status} items in the review queue.")

    # ── Scraper Runs ──────────────────────────────────────
    with sub_runs:
        df_sr = load_scraper_runs_admin(sb)
        if not df_sr.empty:
            st.dataframe(df_sr, use_container_width=True, hide_index=True, height=400)
        else:
            st.info("No scraper runs yet.")

    # ── Pipeline Costs ────────────────────────────────────
    with sub_costs:
        cost_stats = get_cost_summary(sb)
        cm1, cm2, cm3, cm4 = st.columns(4)
        cm1.metric("💵 Total Cost", f"${cost_stats['total_cost_usd']:.4f}")
        cm2.metric("📥 Input Tokens", f"{cost_stats['total_input_tokens']:,}")
        cm3.metric("📤 Output Tokens", f"{cost_stats['total_output_tokens']:,}")
        cm4.metric("🔢 API Calls", cost_stats["total_calls"])

        df_costs = load_pipeline_costs_admin(sb)
        if not df_costs.empty:
            st.dataframe(df_costs, use_container_width=True, hide_index=True, height=350)
        else:
            st.info("No cost data yet.")
