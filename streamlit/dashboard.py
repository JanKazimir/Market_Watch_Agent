import json
import datetime
from pathlib import Path
import streamlit as st

# ── Config ────────────────────────────────────────────────────────────────────

SNAPSHOTS_DIR = Path(__file__).parent.parent / "snapshots"
st.set_page_config(page_title="Daily Market Watch", page_icon="📊", layout="wide")

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
    .kpi-box.high .kpi-value { color: #e74c3c; }
    .kpi-box.medium .kpi-value { color: #e67e22; }
    .kpi-box.disc .kpi-value { color: #2ecc71; }

    .section-box { background: #242424; border-radius: 10px; padding: 20px 24px; margin-bottom: 16px; }
    .section-title { font-size: 16px; font-weight: bold; color: #fff; margin-bottom: 14px; }

    .change-row {
        display: flex; align-items: center; gap: 12px;
        padding: 10px 0; border-bottom: 1px solid #333;
    }
    .change-row:last-child { border-bottom: none; }
    .badge { font-size: 11px; font-weight: bold; padding: 3px 8px; border-radius: 4px; min-width: 42px; text-align: center; }
    .badge-HIGH { background: #c0392b; color: white; }
    .badge-MED  { background: #d4800a; color: white; }
    .badge-LOW  { background: #555; color: #ccc; }
    .change-bank { font-weight: bold; color: #fff; min-width: 120px; }
    .change-desc { color: #ccc; font-size: 14px; flex: 1; }
    .change-source { color: #666; font-size: 12px; }

    .disc-item { background: #1e1e1e; border-radius: 8px; padding: 14px 16px; margin-bottom: 10px; }
    .disc-title { font-weight: bold; color: #fff; margin-bottom: 6px; }
    .disc-diff { color: #e67e22; font-size: 13px; font-weight: bold; margin-top: 4px; }

    .news-item { padding: 12px 0; border-bottom: 1px solid #333; }
    .news-item:last-child { border-bottom: none; }
    .news-title a { color: #4da6ff; text-decoration: none; font-weight: bold; font-size: 14px; }
    .news-meta { color: #666; font-size: 12px; margin-top: 3px; }
</style>
""", unsafe_allow_html=True)

# ── Helpers ───────────────────────────────────────────────────────────────────

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
    if ct == "condition_change":
        return "MED"
    return "LOW"

def describe_change(change):
    ct = change.get("change_type", "")
    field = change.get("field", "")
    before = change.get("before")
    after = change.get("after")
    product = change.get("product_name", "")
    title = change.get("title", "")

    if ct == "rate_change":
        return f"{product} — {field}: {before}% → {after}%"
    if ct == "condition_change":
        return f"{product} — {field}: {before} → {after}"
    if ct == "other_change":
        return f"{product} — {field} updated"
    if ct == "new_article":
        return f"New article: {title}"
    if ct == "article_updated":
        return f"Article updated: {title or after}"
    if ct in ("product_added", "product_removed"):
        return f"{ct.replace('_', ' ').title()}: {product}"
    return f"{ct}: {product or title}"

# ── Sidebar ───────────────────────────────────────────────────────────────────

with st.sidebar:
    st.markdown("### 📅 Date")
    dates = get_available_dates()
    if not dates:
        st.error(f"No diff_all_*.json found in:\n{SNAPSHOTS_DIR}")
        st.stop()
    selected_date = st.selectbox("Select date", dates)

# ── Load data ─────────────────────────────────────────────────────────────────

diff_all      = load_json(SNAPSHOTS_DIR / f"diff_all_{selected_date}.json")
discrepancies = load_json(SNAPSHOTS_DIR / f"discrepancies_{selected_date}.json")
lecho         = load_json(SNAPSHOTS_DIR / f"lecho_entreprises_banques_{selected_date}.json")

changes  = diff_all.get("changes", [])
sources  = [s["source"] for s in diff_all.get("sources", [])]
articles = lecho.get("articles", [])
discs    = discrepancies.get("discrepancies", [])

high_count = sum(1 for c in changes if classify_impact(c) == "HIGH")
med_count  = sum(1 for c in changes if classify_impact(c) == "MED")

# ── Header ────────────────────────────────────────────────────────────────────

st.markdown(f"""
<div class="header-box">
    <div class="header-logo">MW</div>
    <div>
        <div class="header-title">Daily Market Watch</div>
        <div class="header-sub">{selected_date} — {len(sources)} sources monitored</div>
    </div>
</div>
""", unsafe_allow_html=True)

# ── KPIs ──────────────────────────────────────────────────────────────────────

c1, c2, c3, c4 = st.columns(4)
with c1:
    st.markdown(f'<div class="kpi-box"><div class="kpi-label">Total changes</div><div class="kpi-value">{len(changes)}</div></div>', unsafe_allow_html=True)
with c2:
    st.markdown(f'<div class="kpi-box high"><div class="kpi-label">High impact</div><div class="kpi-value">{high_count}</div></div>', unsafe_allow_html=True)
with c3:
    st.markdown(f'<div class="kpi-box medium"><div class="kpi-label">Medium impact</div><div class="kpi-value">{med_count}</div></div>', unsafe_allow_html=True)
with c4:
    st.markdown(f'<div class="kpi-box disc"><div class="kpi-label">Discrepancies</div><div class="kpi-value">{len(discs)}</div></div>', unsafe_allow_html=True)

st.markdown("<br>", unsafe_allow_html=True)

# ── Changes + Discrepancies ───────────────────────────────────────────────────

col_left, col_right = st.columns([3, 2])

with col_left:
    st.markdown('<div class="section-box"><div class="section-title">Today\'s changes</div>', unsafe_allow_html=True)
    if not changes:
        st.markdown('<div style="color:#666">No changes detected.</div>', unsafe_allow_html=True)
    else:
        for c in changes:
            impact = classify_impact(c)
            bank = c.get("bank", c.get("source", "—"))
            st.markdown(f"""
            <div class="change-row">
                <span class="badge badge-{impact}">{impact}</span>
                <span class="change-bank">{bank}</span>
                <span class="change-desc">{describe_change(c)}</span>
                <span class="change-source">{c.get("source", "")}</span>
            </div>""", unsafe_allow_html=True)
    st.markdown('</div>', unsafe_allow_html=True)

with col_right:
    st.markdown('<div class="section-box"><div class="section-title">Data discrepancies</div>', unsafe_allow_html=True)
    if not discs:
        st.markdown('<div style="color:#666">No discrepancies detected.</div>', unsafe_allow_html=True)
    else:
        for d in discs:
            field = d.get("field", "")
            product = d.get("product_name", "")
            sources_data = d.get("sources", {})
            diff = d.get("difference")
            keys = list(sources_data.keys())
            v1 = sources_data.get(keys[0], "—") if keys else "—"
            v2 = sources_data.get(keys[1], "—") if len(keys) > 1 else "—"
            s1 = keys[0] if keys else ""
            s2 = keys[1] if len(keys) > 1 else ""
            diff_str = f"{diff}% difference" if diff else "value differs"
            st.markdown(f"""
            <div class="disc-item">
                <div class="disc-title">{product} — {field}</div>
                <div style="color:#aaa;font-size:13px">{s1}: <b style="color:#2ecc71">{v1}</b> &nbsp;|&nbsp; {s2}: <b style="color:#e74c3c">{v2}</b></div>
                <div class="disc-diff">{diff_str}</div>
            </div>""", unsafe_allow_html=True)
    st.markdown('</div>', unsafe_allow_html=True)

# ── News ──────────────────────────────────────────────────────────────────────

st.markdown('<div class="section-box"><div class="section-title">News headlines</div>', unsafe_allow_html=True)
if not articles:
    st.markdown('<div style="color:#666">No articles found.</div>', unsafe_allow_html=True)
else:
    for article in articles[:6]:
        title = article.get("title", "")
        link  = article.get("link", "#")
        pub   = article.get("pub_date", "")
        try:
            dt = datetime.datetime.strptime(pub, "%a, %d %b %Y %H:%M:%S %Z")
            pub_fmt = dt.strftime("%d %b %Y")
        except Exception:
            pub_fmt = pub[:16] if pub else ""
        st.markdown(f"""
        <div class="news-item">
            <div class="news-title"><a href="{link}" target="_blank">{title}</a></div>
            <div class="news-meta">lecho.be — {pub_fmt}</div>
        </div>""", unsafe_allow_html=True)
st.markdown('</div>', unsafe_allow_html=True)