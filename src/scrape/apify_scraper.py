"""
Apify Bank Scraper (Excel-Driven)
==================================
Reads bank URLs from the research Excel (list_of_banks_ref.xlsx),
scrapes each page with Apify's Website Content Crawler, extracts
structured data with an LLM, and saves to our pipeline's JSON schema.

Setup:
    pip install apify-client openai openpyxl python-dotenv

Usage:
    python apify_scraper.py                        # scrape all banks
    python apify_scraper.py --list                 # list all sources
    python apify_scraper.py --news-only            # news pages only
    python apify_scraper.py --products-only        # product pages only
    python apify_scraper.py "Argenta"              # one bank only
"""

import json, os, re, sys, hashlib, datetime
from pathlib import Path

try:
    from apify_client import ApifyClient
except ImportError:
    print("ERROR: pip install apify-client")
    sys.exit(1)
try:
    import openpyxl
except ImportError:
    print("ERROR: pip install openpyxl")
    sys.exit(1)
try:
    from dotenv import load_dotenv

    load_dotenv()
except ImportError:
    pass

APIFY_API_TOKEN = os.environ.get("APIFY_API_TOKEN", "")
SNAPSHOT_DIR = Path("snapshots")
SNAPSHOT_DIR.mkdir(exist_ok=True)
SOURCES_FILE = Path("list_of_banks_ref.xlsx")
WEBSITE_CONTENT_CRAWLER = "apify/website-content-crawler"


def safe_str(v):
    return str(v).strip().strip('"').strip() if v else ""


def split_urls(v):
    t = safe_str(v)
    if not t:
        return []
    return [
        u.strip().strip('"').strip()
        for u in re.split(r"[;\n]+", t)
        if u.strip().startswith("http")
    ]


def make_source_key(bank, utype, idx=0):
    k = f"{re.sub(r'[^\\w]', '_', bank.lower()).strip('_')}_{utype}"
    return f"{k}_{idx}" if idx > 0 else k


def load_sources(filepath=SOURCES_FILE):
    if not filepath.exists():
        print(f"ERROR: {filepath} not found")
        sys.exit(1)
    wb = openpyxl.load_workbook(filepath, read_only=True)
    ws = wb.active
    sources = []
    for row in ws.iter_rows(min_row=2, values_only=True):
        bank = safe_str(row[0])
        if not bank:
            continue
        seen = set()
        news = safe_str(row[4]) if len(row) > 4 else ""
        if news and news.startswith("http"):
            sources.append(
                {
                    "source_key": make_source_key(bank, "news"),
                    "bank": bank,
                    "url": news,
                    "url_type": "news",
                    "product_type": None,
                }
            )
            seen.add(news)
        sav = safe_str(row[7]) if len(row) > 7 else ""
        if sav and sav.startswith("http") and sav not in seen:
            sources.append(
                {
                    "source_key": make_source_key(bank, "savings"),
                    "bank": bank,
                    "url": sav,
                    "url_type": "products",
                    "product_type": "regulated",
                }
            )
            seen.add(sav)
        reg = safe_str(row[14]) if len(row) > 14 else ""
        if reg and reg.startswith("http") and reg not in seen:
            sources.append(
                {
                    "source_key": make_source_key(bank, "regulated"),
                    "bank": bank,
                    "url": reg,
                    "url_type": "products",
                    "product_type": "regulated",
                }
            )
            seen.add(reg)
        for i, u in enumerate(split_urls(row[17]) if len(row) > 17 else []):
            if u not in seen:
                sources.append(
                    {
                        "source_key": make_source_key(bank, "product", i),
                        "bank": bank,
                        "url": u,
                        "url_type": "products",
                        "product_type": "regulated",
                    }
                )
                seen.add(u)
        for i, u in enumerate(split_urls(row[25]) if len(row) > 25 else []):
            if u not in seen:
                sources.append(
                    {
                        "source_key": make_source_key(bank, "term", i),
                        "bank": bank,
                        "url": u,
                        "url_type": "products",
                        "product_type": "unregulated",
                    }
                )
                seen.add(u)
    wb.close()
    nc = len([s for s in sources if s["url_type"] == "news"])
    pc = len([s for s in sources if s["url_type"] == "products"])
    print(
        f"[EXCEL] {len(sources)} URLs from {len(set(s['bank'] for s in sources))} banks ({pc} product, {nc} news)"
    )
    return sources


def scrape_with_apify(url):
    if not APIFY_API_TOKEN:
        print("ERROR: APIFY_API_TOKEN not set")
        sys.exit(1)
    client = ApifyClient(APIFY_API_TOKEN)
    print(f"  [APIFY] Crawling {url}...")
    run = client.actor(WEBSITE_CONTENT_CRAWLER).call(
        run_input={
            "startUrls": [{"url": url}],
            "maxCrawlPages": 1,
            "crawlerType": "playwright:firefox",
            "outputFormats": ["markdown"],
        }
    )
    results = list(client.dataset(run["defaultDatasetId"]).iterate_items())
    if not results:
        print(f"  [APIFY] No content")
        return ""
    content = results[0].get("markdown", results[0].get("text", ""))
    print(f"  [APIFY] {len(content)} characters")
    return content


PRODUCT_PROMPT = """You are a data extraction assistant for a Belgian banking market watch tool.
Given the raw text from a bank's savings page, extract ALL savings products.
For each: bank, product_name, base_rate (decimal), fidelity_premium (decimal, null for term), total_rate (decimal), account_type, min_deposit (euros, null if none), max_deposit (euros, null if none), conditions, duration (for term deposits, null for savings).
IMPORTANT: Return ONLY valid JSON array. Rates as decimals (0.75 not 0.75%). Belgian comma = dot (2,70% = 2.70). max_deposit is euro amount (500 not 500000). If no rates found return []."""

NEWS_PROMPT = """You are a data extraction assistant for a Belgian banking market watch tool.
Given the raw text from a bank's news/press page, extract ALL news articles.
For each: title, description (1-2 sentences), pub_date (YYYY-MM-DD if visible), link (full URL if visible, null otherwise).
IMPORTANT: Return ONLY valid JSON array. Only actual articles, not navigation. Include full URLs for clickability. If none found return []."""


def call_llm(prompt, raw_text, source):
    try:
        import openai
    except ImportError:
        print("  [LLM] openai not installed")
        return []
    api_key = os.environ.get("API_KEY", "")
    if not api_key:
        print("  [LLM] API_KEY not set")
        return []
    client = openai.OpenAI(api_key=api_key)
    if len(raw_text) > 15000:
        raw_text = raw_text[:15000] + "\n...[truncated]"
    print(f"  [LLM] {len(raw_text)} chars → gpt-4o-mini...")
    try:
        resp = client.chat.completions.create(
            model="gpt-4o-mini",
            temperature=0,
            messages=[
                {"role": "system", "content": prompt},
                {"role": "user", "content": f"Extract from {source}:\n\n{raw_text}"},
            ],
        )
        txt = resp.choices[0].message.content.strip()
        if txt.startswith("```"):
            txt = txt.split("\n", 1)[1].rsplit("```", 1)[0]
        items = json.loads(txt)
        print(f"  [LLM] {len(items)} items")
        return items
    except Exception as e:
        print(f"  [LLM] ERROR: {e}")
        return []


def find_prev(sk):
    for d in range(1, 8):
        fp = (
            SNAPSHOT_DIR
            / f"apify_{sk}_{(datetime.date.today()-datetime.timedelta(days=d)).isoformat()}.json"
        )
        if fp.exists():
            try:
                return (
                    json.loads(fp.read_text(encoding="utf-8")).get(
                        "raw_text_checksum", ""
                    ),
                    fp,
                )
            except:
                continue
    return "", None


def reuse(prev_path, sk):
    d = json.loads(prev_path.read_text(encoding="utf-8"))
    d["date"] = datetime.date.today().isoformat()
    d["timestamp"] = datetime.datetime.now().isoformat(timespec="seconds")
    d["reused_from"] = prev_path.name
    fp = SNAPSHOT_DIR / f"apify_{sk}_{d['date']}.json"
    fp.write_text(json.dumps(d, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"  [CACHE] Unchanged → reused {prev_path.name}")
    return d


def save_snapshot(source, items, raw_text, raw_cs):
    today = datetime.date.today().isoformat()
    now = datetime.datetime.now().isoformat(timespec="seconds")
    sk = source["source_key"]
    ut = source["url_type"]
    cs = hashlib.sha256(json.dumps(items, sort_keys=True).encode()).hexdigest()
    snap = {
        "source": sk,
        "url": source["url"],
        "timestamp": now,
        "date": today,
        "scraper": "apify",
        "bank": source["bank"],
        "raw_text_checksum": raw_cs,
    }
    if ut == "products":
        snap.update(
            {
                "product_type": source.get("product_type", "regulated"),
                "num_products": len(items),
                "checksum": cs,
                "products": items,
            }
        )
    else:
        for a in items:
            if not a.get("guid"):
                a["guid"] = hashlib.sha256(
                    (a.get("link") or a.get("title", "")).encode()
                ).hexdigest()[:16]
        snap.update(
            {
                "num_articles": len(items),
                "checksum": hashlib.sha256(
                    "|".join(a.get("guid", "") for a in items).encode()
                ).hexdigest(),
                "articles": items,
            }
        )
    (SNAPSHOT_DIR / f"apify_{sk}_{today}_raw.txt").write_text(
        raw_text, encoding="utf-8"
    )
    fp = SNAPSHOT_DIR / f"apify_{sk}_{today}.json"
    fp.write_text(json.dumps(snap, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"  [SAVE] → {fp}")
    return fp


def scrape_source(source):
    sk = source["source_key"]
    print(
        f"\n  {'—'*50}\n  {source['bank']} | {source['url_type']} | {sk}\n  {source['url']}"
    )
    raw = scrape_with_apify(source["url"])
    if not raw:
        return {}
    rc = hashlib.sha256(raw.encode()).hexdigest()
    pc, pp = find_prev(sk)
    if pc and rc == pc and pp:
        return reuse(pp, sk)
    print(f"  [NEW] Changed → LLM...")
    items = call_llm(
        NEWS_PROMPT if source["url_type"] == "news" else PRODUCT_PROMPT,
        raw,
        source["bank"],
    )
    save_snapshot(source, items, raw, rc)
    return {"items": items}


def main():
    print(
        f"{'='*60}\n  Apify Bank Scraper (Excel-Driven)\n  {datetime.date.today().isoformat()}\n{'='*60}"
    )
    if not APIFY_API_TOKEN:
        print("ERROR: APIFY_API_TOKEN not set. Add to .env")
        sys.exit(1)
    sources = load_sources()
    if not sources:
        print("No sources.")
        sys.exit(1)
    bf = None
    mode = "all"
    for a in sys.argv[1:]:
        if a == "--list":
            print(
                f"\n{'source_key':40s} {'bank':22s} {'type':10s} {'ptype':15s}\n{'-'*90}"
            )
            for s in sources:
                print(
                    f"{s['source_key']:40s} {s['bank']:22s} {s['url_type']:10s} {s.get('product_type') or '':15s}"
                )
            sys.exit(0)
        elif a == "--news-only":
            mode = "news-only"
        elif a == "--products-only":
            mode = "products-only"
        elif not a.startswith("--"):
            bf = a
    f = sources
    if bf:
        f = [s for s in f if bf.lower() in s["bank"].lower()]
        if not f:
            print(
                f"No '{bf}'. Banks: {', '.join(sorted(set(s['bank'] for s in sources)))}"
            )
            sys.exit(1)
    if mode == "news-only":
        f = [s for s in f if s["url_type"] == "news"]
    elif mode == "products-only":
        f = [s for s in f if s["url_type"] == "products"]
    if not f:
        print("Nothing to scrape.")
        sys.exit(0)
    ok = sum(1 for s in f if scrape_source(s))
    print(f"\n{'='*60}\n  DONE: {ok}/{len(f)}\n{'='*60}")


if __name__ == "__main__":
    main()