import json
import datetime
from pathlib import Path
import streamlit as st
import pandas as pd
from collections import defaultdict
import csv
# ── Config ────────────────────────────────────────────────────────────────────

BASE_DIR = Path(__file__).resolve().parent
SNAPSHOTS_DIR = BASE_DIR.parent / "data" / "outputs"
SNAPSHOTS_OLD = BASE_DIR.parent / "data" / "snapshots"  

EXCEL_PATH = Path("../data/list_of_banks_ref_new.xlsx")

st.set_page_config(page_title="Daily Market Watch", page_icon="📊", layout="wide")

# ── HELPERS ───────────────────────────────────────────────────────────────────

def load_json(path):
    if Path(path).exists():
        return json.loads(Path(path).read_text(encoding="utf-8"))
    return {}

def load_closest(prefix, target_date):
    for directory in [SNAPSHOTS_DIR, SNAPSHOTS_OLD]:
        files = sorted(directory.glob(f"{prefix}*.json"), reverse=True)
        if not files:
            continue
        for f in files:
            parts = f.stem.split("_")
            for part in parts:
                if len(part) == 10 and part.count("-") == 2:
                    if part <= target_date:
                        return load_json(f)
    return {}

def get_available_dates():
    files = sorted(SNAPSHOTS_DIR.glob("*_classified_test.json"), reverse=True)
    dates = []
    for f in files:
        parts = f.stem.split("_")
        for part in parts:
            if len(part) == 10 and part.count("-") == 2:
                dates.append(part)
                break
    return dates

def get_impact(change):
    raw = change.get("classification", {}).get("impact", "low")
    return raw.upper()

def get_description(change):
    return change.get("classification", {}).get("description", "")

def get_category(change):
    return change.get("classification", {}).get("taxonomy_category", "")

def get_group(change):
    group = change.get("classification", {}).get("taxonomy_group", "Other")
    if isinstance(group, list):
        group = group[0] if group else "Other"
    return group

def build_executive_summary(display_changes, discs):
    """Génère un résumé automatique depuis les descriptions du classifier."""
    high   = [c for c in display_changes if get_impact(c) == "HIGH"]
    medium = [c for c in display_changes if get_impact(c) == "MEDIUM"]

    sentences = []
    for c in high + medium[:2]:
        desc = get_description(c)
        if desc:
            sentences.append(desc)

    if not sentences:
        return "No significant changes detected today."

    summary = " ".join(sentences)
    if discs:
        summary += f" {len(discs)} data discrepanc{'y' if len(discs)==1 else 'ies'} flagged across sources."
    return summary

# ── CSS ───────────────────────────────────────────────────────────────────────

st.markdown("""
<style>
    .stApp { background-color: #1a1a1a; color: #e0e0e0; }
    .block-container { padding-top: 1.5rem; }

    .header-box {
        background: #242424; border-radius: 10px;
        padding: 16px 24px; margin-bottom: 20px;
        display: flex; align-items: center; gap: 16px;
    }
    .header-logo {
        background: #e05b2b; color: white; font-weight: bold;
        font-size: 18px; padding: 8px 12px; border-radius: 8px;
    }
    .header-title { font-size: 22px; font-weight: bold; color: #fff; }
    .header-sub { font-size: 13px; color: #888; }

    /* ── KPIs ── */
    .kpi-row { display:flex; gap:12px; margin-bottom:16px; flex-wrap:wrap; }
    .kpi-card {
        flex:1; min-width:110px;
        background:#1f1f1f; border-radius:10px;
        padding:14px 18px;
        border-left: 3px solid #333;
    }
    .kpi-card.neutral { border-left-color: #444; }
    .kpi-card.high    { background:#2a1515; border-left-color:#c0392b; }
    .kpi-card.medium  { background:#2a1f0a; border-left-color:#d4800a; }
    .kpi-card.disc    { background:#0f2a18; border-left-color:#27ae60; }
    .kpi-label { font-size:12px; color:#666; margin-bottom:6px; }
    .kpi-value { font-size:32px; font-weight:bold; color:#fff; line-height:1; }
    .kpi-card.high   .kpi-value { color:#c0392b; }
    .kpi-card.medium .kpi-value { color:#d4800a; }
    .kpi-card.disc   .kpi-value { color:#27ae60; }

    /* ── Executive summary ── */
    .exec-summary {
        background:#1f1f1f; border-radius:10px;
        padding:16px 20px;
        font-size:14px; color:#ccc; line-height:1.7;
        border-left:3px solid #e05b2b;
    }
    .exec-label {
        font-size:11px; font-weight:bold; letter-spacing:0.1em;
        text-transform:uppercase; color:#e05b2b;
        margin-bottom:8px;
    }

    .section-box { background: #242424; border-radius: 10px; padding: 20px 24px; margin-bottom: 16px; }
    .section-title { font-size: 16px; font-weight: bold; color: #fff; margin-bottom: 14px; }

    .group-label {
        font-size: 11px; font-weight: bold; letter-spacing: 0.1em;
        text-transform: uppercase; color: #555;
        padding: 8px 0 6px 0; margin-top: 6px;
        border-bottom: 1px solid #2a2a2a; margin-bottom: 4px;
    }

    .news-item { padding: 12px 0; border-bottom: 1px solid #333; }
    .news-title a { color: #4da6ff; text-decoration: none; font-weight: bold; font-size: 14px; }
    .news-meta { color: #666; font-size: 12px; margin-top: 3px; }

    .change-row { display:flex; gap:10px; padding:10px 0; border-bottom:1px solid #333; align-items:flex-start; }
    .change-row:last-child { border-bottom: none; }
    .badge { padding:2px 8px; border-radius:4px; font-size:11px; flex-shrink:0; margin-top:2px; }
    .badge-HIGH   { background:#c0392b; }
    .badge-MEDIUM { background:#d4800a; }
    .badge-LOW    { background:#555; }

    .change-body { display:flex; flex-direction:column; gap:3px; flex:1; }
    .change-main { font-size:14px; color:#e0e0e0; }
    .change-category { font-size:11px; color:#555; }
    .change-desc { font-size:12px; color:#888; font-style:italic; }
    .change-before { color:#c0392b; }
    .change-after  { color:#27ae60; }
            
    .change-after  { color:#27ae60; }

    .disc-row { padding: 10px 0; border-bottom: 1px solid #333; }
    .disc-row:last-child { border-bottom: none; }
    .disc-bank { font-size: 14px; color: #e0e0e0; margin-bottom: 4px; }
    .disc-detail { font-size: 12px; color: #888; }
    .disc-diff { color: #e05b2b; font-weight: bold; }

</style>
""", unsafe_allow_html=True)

# ── ADMIN SYSTEM ──────────────────────────────────────────────────────────────

ADMIN_PASSWORD = "admin123"

if "is_admin" not in st.session_state:
    st.session_state.is_admin = False

with st.sidebar:
    st.markdown("### 🔐 Admin")
    if not st.session_state.is_admin:
        pwd = st.text_input("Password", type="password")
        if st.button("Login"):
            if pwd == ADMIN_PASSWORD:
                st.session_state.is_admin = True
                st.success("Admin mode ON")
            else:
                st.error("Wrong password")
    else:
        st.success("Logged in as admin")

# ── SIDEBAR ───────────────────────────────────────────────────────────────────

with st.sidebar:
    st.markdown("### 📅 Date")
    dates = get_available_dates()
    if not dates:
        st.error("No data found")
        st.stop()
    selected_date = st.selectbox("Select date", dates)

# ── LOAD DATA ─────────────────────────────────────────────────────────────────

pdf_list_path = BASE_DIR.parent / "data" / "pdf_links_csv" / f"{selected_date}_pdf_list.csv"
pdf_fallback_url  = {}
pdf_fallback_bank = {}
if pdf_list_path.exists():
    with open(pdf_list_path, encoding="utf-8") as f:
        for row in csv.DictReader(f):
            if row.get("url") and row.get("pdf_source"):
                pdf_fallback_url[row["url"]] = row["pdf_source"]
            if row.get("bank") and row.get("pdf_source") and row["bank"] not in pdf_fallback_bank:
                pdf_fallback_bank[row["bank"]] = row["pdf_source"]

raw_classified = load_json(SNAPSHOTS_DIR / f"{selected_date}_classified_test.json")
changes = list(raw_classified.values())

lecho_ent = load_closest("lecho_entreprises_banques_", selected_date)
lecho_top = load_closest("lecho_top_stories_", selected_date)
articles  = lecho_ent.get("articles", []) + lecho_top.get("articles", [])

discrepancies = load_closest("discrepancies_", selected_date)
discs = discrepancies.get("discrepancies", [])

news_from_diff  = [c for c in changes if c.get("change_type") == "new_article"]
display_changes = [c for c in changes if c.get("change_type") != "new_article"]

groups = defaultdict(list)
for c in display_changes:
    groups[get_group(c)].append(c)

# KPI counts
n_total  = len(display_changes)
n_high   = len([c for c in display_changes if get_impact(c) == "HIGH"])
n_medium = len([c for c in display_changes if get_impact(c) == "MEDIUM"])
n_discs  = len(discs)

pdf_report  = load_json(SNAPSHOTS_DIR / f"{selected_date}_pdf_report.json")
pdf_summary = pdf_report.get("summary", {})

# ── HEADER ────────────────────────────────────────────────────────────────────

st.markdown(f"""
<div class="header-box">
    <div class="header-logo">MW</div>
    <div>
        <div class="header-title">Daily Market Watch</div>
        <div class="header-sub">{selected_date}</div>
    </div>
</div>
""", unsafe_allow_html=True)

# ── KPIs ──────────────────────────────────────────────────────────────────────

st.markdown(f"""
<div class="kpi-row">
    <div class="kpi-card neutral">
        <div class="kpi-label">Total changes</div>
        <div class="kpi-value">{n_total}</div>
    </div>
    <div class="kpi-card high">
        <div class="kpi-label">High impact</div>
        <div class="kpi-value">{n_high}</div>
    </div>
    <div class="kpi-card medium">
        <div class="kpi-label">Medium impact</div>
        <div class="kpi-value">{n_medium}</div>
    </div>
    <div class="kpi-card disc">
        <div class="kpi-label">Discrepancies</div>
        <div class="kpi-value">{n_discs}</div>
    </div>
</div>
""", unsafe_allow_html=True)

# ── EXECUTIVE SUMMARY ─────────────────────────────────────────────────────────

summary_text = build_executive_summary(display_changes, discs)

st.markdown(f"""
<div class="section-box">
    <div class="section-title">Executive summary</div>
    <div class="exec-summary">
        <div class="exec-label">Today's briefing</div>
        {summary_text}
    </div>
</div>
""", unsafe_allow_html=True)

# ── CHANGES ───────────────────────────────────────────────────────────────────

st.markdown('<div class="section-box"><div class="section-title">Changes detected</div>', unsafe_allow_html=True)

if not display_changes:
    st.markdown('<div style="color:#666">No changes