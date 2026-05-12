import json
import datetime
from pathlib import Path
import streamlit as st
import pandas as pd

# ── Config ────────────────────────────────────────────────────────────────────

BASE_DIR = Path(__file__).resolve().parent
SNAPSHOTS_DIR = BASE_DIR.parent / "data" / "snapshots"
EXCEL_PATH = Path("/Users/fefe/Desktop/Market_Watch_Agent/data/list_of_banks_ref_new.xlsx")

st.set_page_config(page_title="Daily Market Watch", page_icon="📊", layout="wide")

# ── HELPERS ───────────────────────────────────────────────────────────────────

def load_json(path):
    if Path(path).exists():
        return json.loads(Path(path).read_text(encoding="utf-8"))
    return {}

def load_closest(prefix, target_date):
    files = sorted(SNAPSHOTS_DIR.glob(f"{prefix}*.json"), reverse=True)
    if not files:
        return {}
    for f in files:
        parts = f.stem.split("_")
        for part in parts:
            if len(part) == 10 and part.count("-") == 2:
                if part <= target_date:
                    return load_json(f)
    return load_json(files[-1])

def get_available_dates():
    files = sorted(SNAPSHOTS_DIR.glob("diff_all_*.json"), reverse=True)
    return [f.stem.replace("diff_all_", "") for f in files]

def classify_impact(change):
    ct = change.get("change_type", "")
    if ct == "rate_change":
        diff = abs((change.get("after") or 0) - (change.get("before") or 0))
        return "HIGH" if diff >= 0.1 else "MED"
    if ct in ("product_added", "product_removed"):
        return "HIGH"
    return "LOW"

def describe_change(change):
    return f"{change.get('product_name', '')} - {change.get('change_type', '')}"

# ── CSS ───────────────────────────────────────────────────────────────────────

st.markdown("""
<style>

    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800&display=swap');

    :root {
        --bg: #0f1115;
        --card: #171a21;
        --border: #262b36;

        --text: #f3f4f6;
        --muted: #8b93a7;

        --accent: #60a5fa;
        --title: #ffffff;

        --high: #ef4444;
        --med: #f59e0b;
        --low: #3b82f6;
    }

    html, body, .stApp {
        background: var(--bg);
        color: var(--text);
        font-family: 'Inter', sans-serif;
    }

    .block-container {
        max-width: 1100px;
        padding-top: 2.5rem;
        padding-bottom: 4rem;
    }

    /* ── Sidebar ───────────────────── */

    section[data-testid="stSidebar"] {
        background: var(--card);
        border-right: 1px solid var(--border);
    }

    section[data-testid="stSidebar"] * {
        font-family: 'Inter', sans-serif !important;
    }

    /* ── Header ───────────────────── */

    .mw-header {
        display: flex;
        align-items: center;
        gap: 18px;

        margin-bottom: 54px;
    }

    .mw-logo {
        width: 48px;
        height: 48px;

        border-radius: 14px;

        background: var(--accent);

        color: white;

        display: flex;
        align-items: center;
        justify-content: center;

        font-weight: 800;
        font-size: 16px;

        box-shadow: 0 0 25px rgba(96,165,250,0.15);
    }

    .mw-title {
        font-size: 34px;
        font-weight: 800;

        color: var(--title);

        letter-spacing: -0.04em;

        line-height: 1;
    }

    .mw-date {
        font-size: 14px;
        color: var(--muted);

        margin-top: 7px;
    }

    /* ── Section Titles ───────────────────── */

    .mw-section-title {
        font-size: 30px;

        font-weight: 800;

        color: var(--title);

        letter-spacing: -0.04em;

        margin-top: 54px;
        margin-bottom: 22px;

        line-height: 1;

        position: relative;
    }

    .mw-section-title::after {
        content: "";

        display: block;

        width: 42px;
        height: 3px;

        border-radius: 999px;

        background: var(--accent);

        margin-top: 14px;
    }

    /* ── Content wrapper ───────────────────── */

    .mw-card {
        padding: 0;
    }

    /* ── News ───────────────────── */

    .news-item {
        padding: 18px 0;

        border-bottom: 1px solid rgba(255,255,255,0.06);
    }

    .news-item:last-child {
        border-bottom: none;
    }

    .news-link {
        color: #60a5fa !important;

        text-decoration: none !important;

        font-size: 16px;
        font-weight: 600;

        line-height: 1.6;

        transition: 0.2s ease;
    }

    .news-link:hover {
        color: #93c5fd !important;
    }

    .news-meta {
        margin-top: 7px;

        font-size: 12px;

        color: var(--muted);
    }

    /* ── Changes ───────────────────── */

    .change-row {
        display: flex;
        align-items: center;
        gap: 14px;

        padding: 16px 0;

        border-bottom: 1px solid rgba(255,255,255,0.06);
    }

    .change-row:last-child {
        border-bottom: none;
    }

    .change-label {
        font-size: 15px;
        color: var(--text);

        line-height: 1.5;
    }

    /* ── Badges ───────────────────── */

    .badge {
        padding: 5px 11px;

        border-radius: 999px;

        font-size: 11px;
        font-weight: 700;

        min-width: 58px;

        text-align: center;

        flex-shrink: 0;

        letter-spacing: 0.03em;
    }

    .badge-HIGH {
        background: rgba(239, 68, 68, 0.14);
        color: var(--high);
    }

    .badge-MED {
        background: rgba(245, 158, 11, 0.14);
        color: var(--med);
    }

    .badge-LOW {
        background: rgba(59, 130, 246, 0.14);
        color: var(--low);
    }

    /* ── Discrepancies ───────────────────── */

    .disc-row {
        display: flex;
        align-items: center;

        gap: 12px;

        padding: 16px 0;

        border-bottom: 1px solid rgba(255,255,255,0.06);
    }

    .disc-row:last-child {
        border-bottom: none;
    }

    .disc-dot {
        width: 8px;
        height: 8px;

        border-radius: 999px;

        background: var(--accent);

        flex-shrink: 0;
    }

    .disc-field {
        color: var(--muted);
        font-size: 13px;
    }

    /* ── Empty state ───────────────────── */

    .empty-state {
        color: var(--muted);

        font-size: 15px;

        padding: 10px 0;
    }

    /* ── Inputs ───────────────────── */

    .stSelectbox label,
    .stTextInput label {
        color: var(--muted) !important;
        font-size: 13px !important;
    }

    div[data-baseweb="select"] > div,
    .stTextInput input {
        background: var(--card) !important;
        color: var(--text) !important;

        border-radius: 12px !important;
        border: 1px solid var(--border) !important;
    }

    /* ── Buttons ───────────────────── */

    .stButton > button {
        border-radius: 12px !important;

        border: 1px solid var(--border) !important;

        background: var(--card) !important;

        color: var(--text) !important;

        font-weight: 600 !important;

        padding: 0.5rem 1rem !important;

        transition: 0.2s ease;
    }

    .stButton > button:hover {
        border-color: var(--accent) !important;
        color: var(--accent) !important;
    }

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

diff_all      = load_json(SNAPSHOTS_DIR / f"diff_all_{selected_date}.json")
discrepancies = load_closest("discrepancies_", selected_date)
lecho_ent     = load_closest("lecho_entreprises_banques_", selected_date)
lecho_top     = load_closest("lecho_top_stories_", selected_date)

changes  = diff_all.get("changes", [])
discs    = discrepancies.get("discrepancies", [])
articles = lecho_ent.get("articles", []) + lecho_top.get("articles", [])

# ── HEADER ────────────────────────────────────────────────────────────────────

st.markdown(f"""
<div class="mw-header">
    <div class="mw-logo">MW</div>
    <div>
        <div class="mw-title">Daily Market Watch</div>
        <div class="mw-date">{selected_date}</div>
    </div>
</div>
""", unsafe_allow_html=True)

# ── NEWS ──────────────────────────────────────────────────────────────────────

st.markdown('<div class="mw-card"><div class="mw-card-title">News headlines</div>', unsafe_allow_html=True)

if not articles:
    st.markdown('<div class="empty-state">— No news for this date.</div>', unsafe_allow_html=True)
else:
    for article in articles[:6]:
        title = article.get("title", "")
        link  = article.get("link", "#")
        pub   = article.get("pub_date", "")
        try:
            dt = datetime.datetime.strptime(pub, "%a, %d %b %Y %H:%M:%S %Z")
            pub_fmt = dt.strftime("%d %b %Y")
        except:
            pub_fmt = pub[:16] if pub else ""
        st.markdown(f"""
        <div class="news-item">
            <a class="news-link" href="{link}" target="_blank">{title}</a>
            <span class="news-meta">lecho.be &nbsp;·&nbsp; {pub_fmt}</span>
        </div>
        """, unsafe_allow_html=True)

st.markdown('</div>', unsafe_allow_html=True)

# ── CHANGES ───────────────────────────────────────────────────────────────────

st.markdown('<div class="mw-card"><div class="mw-card-title">Changes detected</div>', unsafe_allow_html=True)

if not changes:
    st.markdown('<div class="empty-state">— No changes detected.</div>', unsafe_allow_html=True)
else:
    for c in changes:
        impact = classify_impact(c)
        st.markdown(f"""
        <div class="change-row">
            <span class="badge badge-{impact}">{impact}</span>
            <span class="change-label">{describe_change(c)}</span>
        </div>
        """, unsafe_allow_html=True)

st.markdown('</div>', unsafe_allow_html=True)

# ── DISCREPANCIES ─────────────────────────────────────────────────────────────

st.markdown('<div class="mw-card"><div class="mw-card-title">Discrepancies</div>', unsafe_allow_html=True)

if not discs:
    st.markdown('<div class="empty-state">— No discrepancies detected.</div>', unsafe_allow_html=True)
else:
    for d in discs:
        product = d.get("product_name", "")
        field   = d.get("field", "")
        st.markdown(f"""
        <div class="disc-row">
            <div class="disc-dot"></div>
            <span>{product}</span>
            <span class="disc-field">· {field}</span>
        </div>
        """, unsafe_allow_html=True)

st.markdown('</div>', unsafe_allow_html=True)

# ── EXCEL (ADMIN ONLY) ────────────────────────────────────────────────────────

if st.session_state.is_admin:
    st.markdown('<div class="mw-card"><div class="mw-card-title">Excel Editor (Admin)</div>', unsafe_allow_html=True)

    if EXCEL_PATH.exists():
        excel_file = pd.ExcelFile(EXCEL_PATH)
        sheet = st.selectbox("Sheet", excel_file.sheet_names)
        df = pd.read_excel(EXCEL_PATH, sheet_name=sheet)
        edited = st.data_editor(df, use_container_width=True, num_rows="dynamic")

        col1, col2 = st.columns(2)
        with col1:
            if st.button("💾 Save"):
                with pd.ExcelWriter(EXCEL_PATH, engine="openpyxl", mode="a", if_sheet_exists="replace") as writer:
                    edited.to_excel(writer, sheet_name=sheet, index=False)
                st.success("Saved")
        with col2:
            with open(EXCEL_PATH, "rb") as f:
                st.download_button("⬇️ Download", f, file_name="banks.xlsx")
    else:
        st.error("Excel not found")

    st.markdown('</div>', unsafe_allow_html=True)
