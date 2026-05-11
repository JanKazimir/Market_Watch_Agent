import json
import datetime
from pathlib import Path
import streamlit as st
import pandas as pd

# ── Config ────────────────────────────────────────────────────────────────────

SNAPSHOTS_DIR = Path(__file__).parent.parent / "snapshots"
EXCEL_PATH = Path("/Users/fefe/Desktop/Market_Watch_Agent/data/list_of_banks_ref_new.xlsx")

st.set_page_config(page_title="Daily Market Watch", page_icon="📊", layout="wide")

# ── ADMIN SYSTEM ──────────────────────────────────────────────────────────────

ADMIN_PASSWORD = "admin123"  # change ça

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

    .kpi-box { background: #242424; border-radius: 10px; padding: 20px; text-align: center; }
    .kpi-box.high { background: #3d1a1a; border-left: 4px solid #c0392b; }
    .kpi-box.medium { background: #3d2e0a; border-left: 4px solid #d4800a; }
    .kpi-box.disc { background: #1a3d1a; border-left: 4px solid #27ae60; }
    .kpi-label { font-size: 13px; color: #aaa; margin-bottom: 6px; }
    .kpi-value { font-size: 42px; font-weight: bold; color: #fff; }

    .section-box { background: #242424; border-radius: 10px; padding: 20px 24px; margin-bottom: 16px; }
    .section-title { font-size: 16px; font-weight: bold; color: #fff; margin-bottom: 14px; }

    .news-item { padding: 12px 0; border-bottom: 1px solid #333; }
    .news-title a { color: #4da6ff; text-decoration: none; font-weight: bold; font-size: 14px; }
    .news-meta { color: #666; font-size: 12px; margin-top: 3px; }

    .change-row { display:flex; gap:10px; padding:8px 0; border-bottom:1px solid #333; }
    .badge { padding:2px 6px; border-radius:4px; font-size:11px; }
    .badge-HIGH { background:#c0392b; }
    .badge-MED { background:#d4800a; }
    .badge-LOW { background:#555; }
</style>
""", unsafe_allow_html=True)

# ── HELPERS ───────────────────────────────────────────────────────────────────

def load_json(path):
    if Path(path).exists():
        return json.loads(Path(path).read_text(encoding="utf-8"))
    return {}

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
    return f"{change.get('product_name','')} - {change.get('change_type','')}"

# ── SIDEBAR ───────────────────────────────────────────────────────────────────

with st.sidebar:
    st.markdown("### 📅 Date")
    dates = get_available_dates()
    if not dates:
        st.error("No data found")
        st.stop()

    selected_date = st.selectbox("Select date", dates)

# ── LOAD DATA ─────────────────────────────────────────────────────────────────

diff_all = load_json(SNAPSHOTS_DIR / f"diff_all_{selected_date}.json")
discrepancies = load_json(SNAPSHOTS_DIR / f"discrepancies_{selected_date}.json")
lecho = load_json(SNAPSHOTS_DIR / f"lecho_entreprises_banques_{selected_date}.json")

changes = diff_all.get("changes", [])
articles = lecho.get("articles", [])
discs = discrepancies.get("discrepancies", [])

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

# ── NEWS ──────────────────────────────────────────────────────────────────────

st.markdown('<div class="section-box"><div class="section-title">News headlines</div>', unsafe_allow_html=True)

if not articles:
    st.markdown('<div style="color:#666">🚫 No news today.</div>', unsafe_allow_html=True)

else:
    for article in articles[:6]:
        title = article.get("title", "")
        link = article.get("link", "#")
        pub = article.get("pub_date", "")

        try:
            dt = datetime.datetime.strptime(pub, "%a, %d %b %Y %H:%M:%S %Z")
            pub_fmt = dt.strftime("%d %b %Y")
        except:
            pub_fmt = pub[:16] if pub else ""

        st.markdown(f"""
        <div class="news-item">
            <div class="news-title"><a href="{link}" target="_blank">{title}</a></div>
            <div class="news-meta">lecho.be — {pub_fmt}</div>
        </div>
        """, unsafe_allow_html=True)

st.markdown('</div>', unsafe_allow_html=True)

# ── CHANGES ───────────────────────────────────────────────────────────────────

st.markdown('<div class="section-box"><div class="section-title">Changes</div>', unsafe_allow_html=True)

if not changes:
    st.markdown('<div style="color:#666">No changes detected.</div>', unsafe_allow_html=True)
else:
    for c in changes:
        impact = classify_impact(c)
        st.markdown(f"""
        <div class="change-row">
            <span class="badge badge-{impact}">{impact}</span>
            <span>{describe_change(c)}</span>
        </div>
        """, unsafe_allow_html=True)

st.markdown('</div>', unsafe_allow_html=True)

# ── DISCREPANCIES ────────────────────────────────────────────────────────────

st.markdown('<div class="section-box"><div class="section-title">Discrepancies</div>', unsafe_allow_html=True)

if not discs:
    st.markdown('<div style="color:#666">No discrepancies detected.</div>', unsafe_allow_html=True)
else:
    for d in discs:
        st.markdown(f"""
        <div style="padding:8px 0;border-bottom:1px solid #333">
            {d.get('product_name','')} — {d.get('field','')}
        </div>
        """, unsafe_allow_html=True)

st.markdown('</div>', unsafe_allow_html=True)

# ── EXCEL (ADMIN ONLY) ───────────────────────────────────────────────────────

if st.session_state.is_admin:

    st.markdown('<div class="section-box"><div class="section-title">Excel Editor (Admin)</div>', unsafe_allow_html=True)

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