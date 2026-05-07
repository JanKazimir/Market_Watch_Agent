"""
dashboard/app.py
----------------
ING Market Watch — Real-time Streamlit Dashboard

HOW FILE DISCOVERY WORKS:
  Every time the page refreshes, the app scans for the latest
  classified JSON file. New files appear automatically — no restart needed.

CONNECTION STRATEGIES (pick one, set via environment variable DATA_SOURCE):
  1. "local"   → reads from local data/outputs/ folder (default, for dev)
  2. "gdrive"  → reads from Google Drive folder (best for team sharing)
  3. "github"  → reads from a GitHub repo (good for hackathon demo)
  4. "gsheets" → reads from Google Sheets (best with Looker Studio combo)

Set your strategy:
  export DATA_SOURCE=local        # development
  export DATA_SOURCE=gdrive       # production with Google Drive
  export DATA_SOURCE=github       # production with GitHub

Run:
  streamlit run dashboard/app.py
"""

import streamlit as st
import json
import os
import glob
import requests
import pandas as pd
import time
from datetime import datetime
from pathlib import Path

# ── Page config ───────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="ING Market Watch",
    page_icon="🦁",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Styling ───────────────────────────────────────────────────────────────────
st.markdown("""
<style>
  @import url('https://fonts.googleapis.com/css2?family=IBM+Plex+Sans:wght@300;400;500;600;700&family=IBM+Plex+Mono:wght@400;500&display=swap');
  html, body, [class*="css"] { font-family: 'IBM Plex Sans', sans-serif; }
  :root {
    --ing-orange: #FF6200; --ing-dark: #1A1A2E;
    --high: #D7263D; --medium: #F4A261; --low: #2A9D8F;
  }
  .metric-card {
    background:white; border-radius:12px; padding:20px 24px;
    border-left:5px solid var(--ing-orange);
    box-shadow:0 2px 8px rgba(0,0,0,0.07); margin-bottom:8px;
  }
  .metric-number { font-size:2.4rem; font-weight:700; color:var(--ing-dark); line-height:1; }
  .metric-label  { font-size:0.82rem; color:#888; text-transform:uppercase; letter-spacing:0.08em; margin-top:4px; }
  .change-card {
    background:white; border-radius:10px; padding:16px 20px;
    margin-bottom:10px; box-shadow:0 1px 6px rgba(0,0,0,0.06); border-left:4px solid #ddd;
  }
  .change-card.HIGH   { border-left-color: var(--high); }
  .change-card.Medium { border-left-color: var(--medium); }
  .change-card.low    { border-left-color: var(--low); }
  .badge { display:inline-block; padding:2px 10px; border-radius:20px; font-size:0.72rem; font-weight:600; letter-spacing:0.05em; text-transform:uppercase; }
  .badge-HIGH   { background:#FDECEA; color:var(--high); }
  .badge-Medium { background:#FEF3E2; color:#C47A1A; }
  .badge-low    { background:#E6F4F1; color:var(--low); }
  .badge-group  { background:#EEF2FF; color:#4338CA; }
  .badge-source { background:#F1F5F9; color:#475569; }
  .header-bar {
    background:linear-gradient(135deg,var(--ing-dark) 0%,#16213E 100%);
    border-radius:14px; padding:28px 32px; color:white; margin-bottom:28px;
  }
  .header-bar h1 { font-size:1.8rem; font-weight:700; margin:0; }
  .header-bar p  { color:#AAB4C8; margin:4px 0 0 0; font-size:0.9rem; }
  .section-title {
    font-size:0.78rem; font-weight:600; text-transform:uppercase;
    letter-spacing:0.1em; color:#94A3B8; margin:24px 0 12px 0;
    padding-bottom:6px; border-bottom:1px solid #E2E8F0;
  }
  #MainMenu, header, footer { visibility:hidden; }
  .block-container { padding-top:1.5rem; }
</style>
""", unsafe_allow_html=True)


# ════════════════════════════════════════════════════════════════════════════
# SECTION 1 — DATA LOADERS
# ════════════════════════════════════════════════════════════════════════════

# ── Strategy 1: Local folder ──────────────────────────────────────────────────

def discover_local_files() -> dict:
    """
    Scans data/outputs/ for all *_classified*.json files.
    Returns {date_label: filepath_string}, newest first.

    This is the KEY to real-time updates:
      - No restart needed, ever.
      - Pipeline drops a new file → next page refresh finds it automatically.
      - @st.cache_data(ttl=60) means Streamlit re-reads from disk every 60s.
    """
    base = Path(__file__).resolve().parents[1]
    pattern = str(base / "data" / "outputs" / "*_classified*.json")
    files = sorted(glob.glob(pattern), reverse=True)

    result = {}
    for f in files:
        name = Path(f).stem
        date = name.split("_")[0]
        result[date] = f
    return result


@st.cache_data(ttl=60)  # ← Re-reads from disk every 60 seconds automatically
def load_local_file(filepath: str) -> dict:
    """Cached file reader. ttl=60 means stale data for max 60s — good enough."""
    with open(filepath, encoding="utf-8") as f:
        return json.load(f)


# ── Strategy 2: Google Drive ──────────────────────────────────────────────────

@st.cache_data(ttl=300)
def load_from_google_drive(folder_id: str) -> dict:
    """
    Downloads classified JSON files from a PUBLIC Google Drive folder.

    Setup (5 minutes):
      1. Put your data/outputs/ folder on Google Drive
      2. Right-click → Share → "Anyone with the link" → Viewer
      3. Copy the folder ID from the URL:
           drive.google.com/drive/folders/  ← THIS PART
      4. Get a free API key: console.cloud.google.com → Enable Drive API → Credentials
      5. Set env vars:
           export GDRIVE_FOLDER_ID="1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs"
           export GDRIVE_API_KEY="AIzaSy..."

    Your pipeline saves to local disk AND copies to Drive each morning.
    The dashboard reads from Drive — works from any computer, any browser.
    """
    api_key = os.environ.get("GDRIVE_API_KEY", "")
    try:
        # Step A: List JSON files in the folder
        list_url = (
            f"https://www.googleapis.com/drive/v3/files"
            f"?q='{folder_id}'+in+parents"
            f"+and+mimeType='application/json'"
            f"+and+name+contains+'classified'"
            f"&fields=files(id,name,modifiedTime)"
            f"&orderBy=modifiedTime+desc"
            f"&key={api_key}"
        )
        resp = requests.get(list_url, timeout=10)
        resp.raise_for_status()
        files = resp.json().get("files", [])

        # Step B: Download each file
        results = {}
        for file in files:
            date = file["name"].split("_")[0]
            download_url = (
                f"https://www.googleapis.com/drive/v3/files/{file['id']}"
                f"?alt=media&key={api_key}"
            )
            content = requests.get(download_url, timeout=10).json()
            results[date] = content

        return dict(sorted(results.items(), reverse=True))

    except Exception as e:
        st.error(f"❌ Google Drive error: {e}")
        return {}


# ── Strategy 3: GitHub repository ────────────────────────────────────────────

@st.cache_data(ttl=300)
def load_from_github(repo: str, folder: str, token: str = "") -> dict:
    """
    Reads classified JSON files committed to a GitHub repository.

    Setup (2 minutes for public repo):
      1. Your pipeline commits new files to GitHub each morning
         (add a git commit step to run_pipeline.py)
      2. Set env vars:
           export GITHUB_REPO="your-org/market-watch"
           export GITHUB_FOLDER="data/outputs"
           export GITHUB_TOKEN="ghp_..."   ← only needed for private repos

    This is the BEST option for Streamlit Cloud deployment because:
      - Streamlit Cloud already knows your GitHub repo
      - No extra services needed
      - New file committed → available in dashboard within 5 minutes
    """
    headers = {"Accept": "application/vnd.github.v3+json"}
    if token:
        headers["Authorization"] = f"token {token}"

    try:
        api_url = f"https://api.github.com/repos/{repo}/contents/{folder}"
        resp = requests.get(api_url, headers=headers, timeout=10)
        resp.raise_for_status()

        results = {}
        for file in resp.json():
            if "_classified" not in file.get("name", "") or not file["name"].endswith(".json"):
                continue
            date = file["name"].split("_")[0]
            content = requests.get(file["download_url"], headers=headers, timeout=10).json()
            results[date] = content

        return dict(sorted(results.items(), reverse=True))

    except Exception as e:
        st.error(f"❌ GitHub error: {e}")
        return {}


# ── Strategy 4: Google Sheets (CSV export) ────────────────────────────────────

@st.cache_data(ttl=120)
def load_from_gsheets(csv_url: str) -> dict:
    """
    Reads from a Google Sheet published as CSV.

    Setup (3 minutes):
      1. Your sheets_writer.py already writes to Sheets each morning
      2. In Google Sheets: File → Share → Publish to web
         → Select sheet "changes" → Format: CSV → Publish
      3. Copy the published URL
      4. export GSHEETS_CSV_URL="https://docs.google.com/spreadsheets/d/.../pub?..."

    Refreshes every 2 minutes. Google Sheets updates immediately when
    your pipeline writes to it — very close to real-time.
    """
    try:
        df = pd.read_csv(csv_url)
        results = {}
        for date, group in df.groupby("date"):
            results[str(date)] = group.to_dict("records")
        return dict(sorted(results.items(), reverse=True))
    except Exception as e:
        st.error(f"❌ Google Sheets error: {e}")
        return {}


# ════════════════════════════════════════════════════════════════════════════
# SECTION 2 — UNIFIED INTERFACE
# The dashboard only calls get_all_reports() — never the loaders directly.
# ════════════════════════════════════════════════════════════════════════════

DATA_SOURCE = os.environ.get("DATA_SOURCE", "local")


def get_all_reports() -> dict:
    """
    Returns {date: data} for all available reports.
    Picks the right loader based on DATA_SOURCE env var.
    """
    if DATA_SOURCE == "gdrive":
        return load_from_google_drive(os.environ.get("GDRIVE_FOLDER_ID", ""))

    elif DATA_SOURCE == "github":
        return load_from_github(
            repo=os.environ.get("GITHUB_REPO", ""),
            folder=os.environ.get("GITHUB_FOLDER", "data/outputs"),
            token=os.environ.get("GITHUB_TOKEN", ""),
        )

    elif DATA_SOURCE == "gsheets":
        return load_from_gsheets(os.environ.get("GSHEETS_CSV_URL", ""))

    else:  # local (default)
        file_map = discover_local_files()
        return {date: load_local_file(path) for date, path in file_map.items()}


def flatten_entries(data) -> list:
    """
    Handles BOTH data shapes:
      - Dict format from classify_facts.py:
          {"0": {"bank": ..., "classification": {"impact": ...}}}
      - List format from Google Sheets:
          [{"bank": ..., "impact": ..., ...}]
    Always returns a flat sorted list.
    """
    if isinstance(data, list):
        rows = data  # already flat (Google Sheets format)
    else:
        rows = []
        for key, entry in data.items():
            cl = entry.get("classification", {})
            rows.append({
                "id":                key,
                "source":            entry.get("source", ""),
                "bank":              entry.get("bank", "Unknown"),
                "before":            entry.get("before", ""),
                "after":             entry.get("after", ""),
                "taxonomy_group":    cl.get("taxonomy_group", "Other"),
                "taxonomy_category": cl.get("taxonomy_category", ""),
                "impact":            cl.get("impact", "low"),
                "description":       cl.get("description", ""),
            })

    order = {"HIGH": 0, "Medium": 1, "low": 2}
    rows.sort(key=lambda r: order.get(r.get("impact", "low"), 3))
    return rows


# ════════════════════════════════════════════════════════════════════════════
# SECTION 3 — SIDEBAR
# ════════════════════════════════════════════════════════════════════════════

with st.sidebar:
    st.markdown("### 🦁 ING Market Watch")
    source_label = {"local": "📁 Local", "gdrive": "☁️ Drive",
                    "github": "🐙 GitHub", "gsheets": "📊 Sheets"}
    st.caption(f"Data source: {source_label.get(DATA_SOURCE, DATA_SOURCE)}")
    st.markdown("---")

    all_reports = get_all_reports()

    if not all_reports:
        st.warning("No classified reports found.")
        st.stop()

    selected_date = st.selectbox(
        "📅 Report date",
        options=list(all_reports.keys()),
        index=0,
    )

    st.markdown("---")
    st.markdown("**Filters**")

    all_entries_unfiltered = flatten_entries(all_reports[selected_date])

    impact_filter = st.multiselect(
        "Impact", ["HIGH", "Medium", "low"], default=["HIGH", "Medium", "low"]
    )
    group_filter = st.multiselect(
        "Taxonomy group",
        ["Product & Structure", "Communication & Positioning",
         "News & External Signals", "Other"],
        default=["Product & Structure", "Communication & Positioning",
                 "News & External Signals", "Other"],
    )
    source_options = sorted({r.get("source", "") for r in all_entries_unfiltered if r.get("source")})
    source_filter = st.multiselect("Source", source_options, default=source_options)

    st.markdown("---")
    auto_refresh = st.toggle("🔄 Auto-refresh (60s)", value=False)
    if auto_refresh:
        st.caption("Page reloads every 60 seconds.")
        time.sleep(60)
        st.rerun()

    if st.button("🔄 Refresh now"):
        st.cache_data.clear()
        st.rerun()

    st.caption("Pipeline: Mon–Fri at 07:00 CET")


# ════════════════════════════════════════════════════════════════════════════
# SECTION 4 — MAIN CONTENT
# ════════════════════════════════════════════════════════════════════════════

all_entries = flatten_entries(all_reports[selected_date])

filtered = [
    r for r in all_entries
    if r.get("impact") in impact_filter
    and r.get("taxonomy_group") in group_filter
    and (not source_filter or r.get("source") in source_filter)
]

# ── Header ─────────────────────────────────────────────────────────────────────
st.markdown(f"""
<div class="header-bar">
  <h1>🦁 ING Market Watch</h1>
  <p>
    Daily competitive intelligence &nbsp;·&nbsp;
    <strong style="color:white">{selected_date}</strong>
    &nbsp;·&nbsp; {len(filtered)} of {len(all_entries)} changes
    &nbsp;·&nbsp; last refresh {datetime.utcnow().strftime("%H:%M UTC")}
  </p>
</div>
""", unsafe_allow_html=True)

# ── KPI cards ──────────────────────────────────────────────────────────────────
high   = sum(1 for r in all_entries if r.get("impact") == "HIGH")
medium = sum(1 for r in all_entries if r.get("impact") == "Medium")
low    = sum(1 for r in all_entries if r.get("impact") == "low")
banks  = len(set(r.get("bank", "") for r in all_entries))

c1, c2, c3, c4 = st.columns(4)
with c1:
    st.markdown(f'<div class="metric-card"><div class="metric-number">{len(all_entries)}</div><div class="metric-label">Total changes</div></div>', unsafe_allow_html=True)
with c2:
    st.markdown(f'<div class="metric-card" style="border-left-color:#D7263D"><div class="metric-number" style="color:#D7263D">{high}</div><div class="metric-label">🔴 High impact</div></div>', unsafe_allow_html=True)
with c3:
    st.markdown(f'<div class="metric-card" style="border-left-color:#F4A261"><div class="metric-number" style="color:#C47A1A">{medium}</div><div class="metric-label">🟡 Medium</div></div>', unsafe_allow_html=True)
with c4:
    st.markdown(f'<div class="metric-card" style="border-left-color:#2A9D8F"><div class="metric-number" style="color:#2A9D8F">{banks}</div><div class="metric-label">🏦 Banks tracked</div></div>', unsafe_allow_html=True)

# ── Trend chart (multi-day) ────────────────────────────────────────────────────
if len(all_reports) > 1:
    st.markdown('<div class="section-title">Trend — impact levels over time</div>', unsafe_allow_html=True)
    trend = []
    for d, data in sorted(all_reports.items()):
        rows = flatten_entries(data)
        trend.append({
            "date":   d,
            "HIGH":   sum(1 for r in rows if r.get("impact") == "HIGH"),
            "Medium": sum(1 for r in rows if r.get("impact") == "Medium"),
            "low":    sum(1 for r in rows if r.get("impact") == "low"),
        })
    trend_df = pd.DataFrame(trend).set_index("date")
    st.area_chart(trend_df, color=["#D7263D", "#F4A261", "#2A9D8F"], height=180)

# ── Bar charts ─────────────────────────────────────────────────────────────────
st.markdown('<div class="section-title">Breakdown — selected day</div>', unsafe_allow_html=True)
ch1, ch2 = st.columns(2)
df = pd.DataFrame(all_entries) if all_entries else pd.DataFrame()

with ch1:
    if not df.empty and "taxonomy_group" in df.columns:
        gc = df.groupby("taxonomy_group").size().reset_index(name="count")
        st.bar_chart(gc.set_index("taxonomy_group")["count"], color="#FF6200", height=200)
        st.caption("Changes by taxonomy group")

with ch2:
    if not df.empty and "bank" in df.columns:
        bc = df.groupby("bank").size().reset_index(name="count").sort_values("count", ascending=False).head(8)
        st.bar_chart(bc.set_index("bank")["count"], color="#1A1A2E", height=200)
        st.caption("Changes by bank")

# ── Change cards ───────────────────────────────────────────────────────────────
st.markdown('<div class="section-title">All detected changes</div>', unsafe_allow_html=True)

if not filtered:
    st.info("No changes match the current filters.")
else:
    for row in filtered:
        impact = row.get("impact", "low")
        before = row.get("before", "")
        after  = row.get("after", "")

        ba_html = ""
        if before or after:
            ba_html = f"""
            <div style="display:flex;gap:12px;margin-top:10px;font-family:'IBM Plex Mono',monospace;font-size:0.8rem;">
              <div style="background:#FEF2F2;color:#922B21;padding:4px 10px;border-radius:6px;">✕ {before or '—'}</div>
              <div style="color:#94A3B8;padding:4px 0;">→</div>
              <div style="background:#F0FDF4;color:#166534;padding:4px 10px;border-radius:6px;">✓ {after or '—'}</div>
            </div>"""

        st.markdown(f"""
        <div class="change-card {impact}">
          <div style="display:flex;justify-content:space-between;align-items:flex-start;gap:12px;">
            <div style="flex:1;">
              <div style="font-weight:600;font-size:0.97rem;color:#1A1A2E;margin-bottom:6px;">🏦 {row.get('bank','Unknown')}</div>
              <div style="color:#475569;font-size:0.88rem;line-height:1.5;">{row.get('description','')}</div>
              {ba_html}
            </div>
            <div style="display:flex;flex-direction:column;align-items:flex-end;gap:6px;min-width:120px;">
              <span class="badge badge-{impact}">{impact}</span>
              <span class="badge badge-source">{row.get('source','')}</span>
              <span class="badge badge-group">{row.get('taxonomy_category','')}</span>
            </div>
          </div>
        </div>
        """, unsafe_allow_html=True)

# ── Export ─────────────────────────────────────────────────────────────────────
with st.expander("📋 Export / raw data"):
    export_df = pd.DataFrame(filtered)
    if not export_df.empty:
        st.dataframe(export_df, use_container_width=True, height=250)
        st.download_button(
            "⬇️ Download CSV",
            export_df.to_csv(index=False).encode("utf-8"),
            file_name=f"market_watch_{selected_date}.csv",
            mime="text/csv",
        )