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
    if ct == "condition_change":
        return "MED"
    if ct in ("pdf_link_added", "pdf_link_removed"):
        return "LOW"
    return "LOW"

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

    .section-box { background: #242424; border-radius: 10px; padding: 20px 24px; margin-bottom: 16px; }
    .section-title { font-size: 16px; font-weight: bold; color: #fff; margin-bottom: 14px; }

    .news-item { padding: 12px 0; border-bottom: 1px solid #333; }
    .news-title a { color: #4da6ff; text-decoration: none; font-weight: bold; font-size: 14px; }
    .news-meta { color: #666; font-size: 12px; margin-top: 3px; }

    .change-row { display:flex; gap:10px; padding:10px 0; border-bottom:1px solid #333; align-items:flex-start; }
    .change-row:last-child { border-bottom: none; }
    .badge { padding:2px 8px; border-radius:4px; font-size:11px; flex-shrink:0; margin-top:2px; }
    .badge-HIGH { background:#c0392b; }
    .badge-MED  { background:#d4800a; }
    .badge-LOW  { background:#555; }

    .change-body { display:flex; flex-direction:column; gap:3px; flex:1; }
    .change-main { font-size:14px; color:#e0e0e0; }
    .change-detail { font-size:12px; color:#888; }
    .change-arrow { color:#aaa; }
    .change-before { color:#c0392b; }
    .change-after  { color:#27ae60; }
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

# Toutes les changes viennent uniquement de diff_all
changes  = diff_all.get("changes", [])
discs    = discrepancies.get("discrepancies", [])
articles = lecho_ent.get("articles", []) + lecho_top.get("articles", [])

# Sépare les types de changes
rate_and_condition = [c for c in changes if c.get("change_type") in ("rate_change", "condition_change")]
pdf_changes        = [c for c in changes if c.get("change_type") in ("pdf_link_added", "pdf_link_removed")]
product_changes    = [c for c in changes if c.get("change_type") in ("product_added", "product_removed")]
news_from_diff     = [c for c in changes if c.get("change_type") == "new_article"]

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

# ── CHANGES ───────────────────────────────────────────────────────────────────

all_display_changes = rate_and_condition + product_changes + pdf_changes

st.markdown('<div class="section-box"><div class="section-title">Changes detected</div>', unsafe_allow_html=True)

if not all_display_changes:
    st.markdown('<div style="color:#666">No changes detected.</div>', unsafe_allow_html=True)
else:
    for c in all_display_changes:
        ct     = c.get("change_type", "")
        impact = classify_impact(c)
        bank   = c.get("bank", "")
        product = c.get("product_name", "")
        field   = c.get("field", "")
        before  = c.get("before")
        after   = c.get("after")

        if ct == "rate_change":
            main   = f"{bank} — {product}"
            detail = f"{field} : <span class='change-before'>{before}%</span> → <span class='change-after'>{after}%</span>"

        elif ct == "condition_change":
            main   = f"{bank} — {product}"
            detail = f"{field} : <span class='change-before'>{before}</span> → <span class='change-after'>{after}</span>"

        elif ct == "product_added":
            main   = f"{bank} — {product}"
            detail = "Nouveau produit ajouté"

        elif ct == "product_removed":
            main   = f"{bank} — {product}"
            detail = "Produit supprimé"

        elif ct == "pdf_link_added":
            main   = f"{bank} — PDF ajouté"
            detail = c.get("link_type", field)

        elif ct == "pdf_link_removed":
            main   = f"{bank} — PDF supprimé"
            detail = c.get("link_type", field)

        else:
            main   = ct
            detail = ""

        st.markdown(f"""
        <div class="change-row">
            <span class="badge badge-{impact}">{impact}</span>
            <div class="change-body">
                <span class="change-main">{main}</span>
                <span class="change-detail">{detail}</span>
            </div>
        </div>
        """, unsafe_allow_html=True)

st.markdown('</div>', unsafe_allow_html=True)

# ── NEWS HEADLINES ────────────────────────────────────────────────────────────

# Merge : articles des fichiers lecho + new_articles du diff
st.markdown('<div class="section-box"><div class="section-title">News headlines</div>', unsafe_allow_html=True)

# Affiche d'abord les new_articles du diff
for c in news_from_diff:
    title   = c.get("title", "")
    link    = c.get("link", "#")
    pub     = c.get("pub_date", "")
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

# Puis les articles des fichiers lecho (sans doublons)
diff_links = {c.get("link") for c in news_from_diff}
remaining  = [a for a in articles if a.get("link") not in diff_links]

if not remaining and not news_from_diff:
    st.markdown('<div style="color:#666">🚫 No news today.</div>', unsafe_allow_html=True)
else:
    for article in remaining[:6]:
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
            <div class="news-title"><a href="{link}" target="_blank">{title}</a></div>
            <div class="news-meta">lecho.be — {pub_fmt}</div>
        </div>
        """, unsafe_allow_html=True)

st.markdown('</div>', unsafe_allow_html=True)

# ── EXCEL (ADMIN ONLY) ────────────────────────────────────────────────────────

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
