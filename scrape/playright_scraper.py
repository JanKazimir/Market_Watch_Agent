"""
Playwright Bank Scraper (Excel-Driven)
=======================================
Reads bank URLs from the research Excel (list_of_banks_ref.xlsx),
scrapes each page with Playwright, extracts structured data with an LLM,
and saves to our pipeline's JSON schema.

Reads these URL columns (skipping PDF columns):
  - news_page_url            → news articles
  - saving_accounts_url      → product data (regulated)
  - regulated_savings_url    → product data (regulated)
  - regulated_savings_individual_products_url → product data (regulated, may have multiple URLs)
  - term_accounts_url        → product data (unregulated, may have multiple URLs)

Setup:
    pip install playwright openai openpyxl python-dotenv
    playwright install chromium

Usage:
    python playwright_scraper.py                        # scrape all banks
    python playwright_scraper.py --list                 # list all sources
    python playwright_scraper.py --news-only            # news pages only
    python playwright_scraper.py --products-only        # product pages only
    python playwright_scraper.py "Argenta"              # one bank only
    python playwright_scraper.py "Argenta" --news-only  # one bank, news only
"""

import json
import os
import re
import sys
import hashlib
import datetime
from pathlib import Path

try:
    from playwright.sync_api import sync_playwright
except ImportError:
    print("ERROR: playwright not installed.")
    print("  pip install playwright")
    print("  playwright install chromium")
    sys.exit(1)

try:
    import openpyxl
except ImportError:
    print("ERROR: openpyxl not installed. pip install openpyxl")
    sys.exit(1)

try:
    from dotenv import load_dotenv

    load_dotenv()
except ImportError:
    pass

SNAPSHOT_DIR = Path("snapshots")
SNAPSHOT_DIR.mkdir(exist_ok=True)

SOURCES_FILE = Path("list_of_banks_ref.xlsx")

COOKIE_SELECTORS = [
    "button#accept-cookies",
    "button#onetrust-accept-btn-handler",
    "button.accept-all",
    "button.js-accept-cookies",
    "[data-action='accept']",
    "[data-testid='accept-cookies']",
    "button:has-text('Alles accepteren')",
    "button:has-text('Alle cookies accepteren')",
    "button:has-text('Accepter tout')",
    "button:has-text('Tout accepter')",
    "button:has-text('Accept all')",
    "button:has-text('Accept cookies')",
    "button:has-text('Akkoord')",
    "button:has-text('Ik ga akkoord')",
    "button:has-text('OK')",
    "button:has-text('Begrepen')",
    "button:has-text('Compris')",
]


def safe_str(value) -> str:
    if value is None:
        return ""
    return str(value).strip().strip('"').strip()


def split_urls(cell_value) -> list[str]:
    text = safe_str(cell_value)
    if not text:
        return []
    urls = re.split(r"[;\n]+", text)
    return [u.strip().strip('"').strip() for u in urls if u.strip().startswith("http")]


def make_source_key(bank_name: str, url_type: str, index: int = 0) -> str:
    bank_clean = re.sub(r"[^\w]", "_", bank_name.lower()).strip("_")
    key = f"{bank_clean}_{url_type}"
    if index > 0:
        key += f"_{index}"
    return key


def load_sources(filepath: Path = SOURCES_FILE) -> list[dict]:
    if not filepath.exists():
        print(f"ERROR: Sources file not found: {filepath}")
        sys.exit(1)

    wb = openpyxl.load_workbook(filepath, read_only=True)
    ws = wb.active
    sources = []

    for row in ws.iter_rows(min_row=2, values_only=True):
        bank_name = safe_str(row[0])
        if not bank_name:
            continue

        seen_urls = set()

        # news_page_url (column 4)
        news_url = safe_str(row[4]) if len(row) > 4 else ""
        if news_url and news_url.startswith("http"):
            sources.append(
                {
                    "source_key": make_source_key(bank_name, "news"),
                    "bank": bank_name,
                    "url": news_url,
                    "url_type": "news",
                    "product_type": None,
                }
            )
            seen_urls.add(news_url)

        # saving_accounts_url (column 7)
        savings_url = safe_str(row[7]) if len(row) > 7 else ""
        if (
            savings_url
            and savings_url.startswith("http")
            and savings_url not in seen_urls
        ):
            sources.append(
                {
                    "source_key": make_source_key(bank_name, "savings"),
                    "bank": bank_name,
                    "url": savings_url,
                    "url_type": "products",
                    "product_type": "regulated",
                }
            )
            seen_urls.add(savings_url)

        # regulated_savings_url (column 14)
        regulated_url = safe_str(row[14]) if len(row) > 14 else ""
        if (
            regulated_url
            and regulated_url.startswith("http")
            and regulated_url not in seen_urls
        ):
            sources.append(
                {
                    "source_key": make_source_key(bank_name, "regulated"),
                    "bank": bank_name,
                    "url": regulated_url,
                    "url_type": "products",
                    "product_type": "regulated",
                }
            )
            seen_urls.add(regulated_url)

        # regulated_savings_individual_products_url (column 17) — multiple
        individual_urls = split_urls(row[17]) if len(row) > 17 else []
        for idx, ind_url in enumerate(individual_urls):
            if ind_url not in seen_urls:
                sources.append(
                    {
                        "source_key": make_source_key(bank_name, "product", idx),
                        "bank": bank_name,
                        "url": ind_url,
                        "url_type": "products",
                        "product_type": "regulated",
                    }
                )
                seen_urls.add(ind_url)

        # term_accounts_url (column 25) — multiple
        term_urls = split_urls(row[25]) if len(row) > 25 else []
        for idx, term_url in enumerate(term_urls):
            if term_url not in seen_urls:
                sources.append(
                    {
                        "source_key": make_source_key(bank_name, "term", idx),
                        "bank": bank_name,
                        "url": term_url,
                        "url_type": "products",
                        "product_type": "unregulated",
                    }
                )
                seen_urls.add(term_url)

    wb.close()

    news_count = len([s for s in sources if s["url_type"] == "news"])
    product_count = len([s for s in sources if s["url_type"] == "products"])
    banks = len(set(s["bank"] for s in sources))

    print(f"[EXCEL] Loaded {len(sources)} URLs from {banks} banks")
    print(f"        {product_count} product pages, {news_count} news pages")
    return sources


def handle_cookies(page):
    for selector in COOKIE_SELECTORS:
        try:
            page.click(selector, timeout=2000)
            print(f"  [COOKIES] Accepted via: {selector}")
            page.wait_for_timeout(1000)
            return True
        except Exception:
            continue
    return False


def scrape_page(url: str, wait_ms: int = 5000) -> str:
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            locale="nl-BE",
            viewport={"width": 1920, "height": 1080},
        )
        page = context.new_page()

        print(f"  [PW] Navigating to {url}...")
        try:
            page.goto(url, wait_until="networkidle", timeout=30000)
        except Exception:
            try:
                page.goto(url, wait_until="domcontentloaded", timeout=30000)
            except Exception as e:
                print(f"  [PW] ERROR: {e}")
                browser.close()
                return ""

        handle_cookies(page)
        page.wait_for_timeout(wait_ms)

        page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        page.wait_for_timeout(2000)
        page.evaluate("window.scrollTo(0, 0)")

        text = page.inner_text("body")
        print(f"  [PW] Extracted {len(text)} characters")
        browser.close()
        return text


PRODUCT_PROMPT = """You are a data extraction assistant for a Belgian banking market watch tool.

Given the raw text content from a bank's savings page, extract ALL savings products mentioned.

For each product, extract:
- bank: The bank name
- product_name: The full product name
- base_rate: Base interest rate as a decimal (e.g. 0.75 for 0.75%)
- fidelity_premium: Fidelity/loyalty premium as a decimal (null for term deposits)
- total_rate: Total rate as a decimal
- account_type: Type of account (e.g. "Gereglementeerde spaarrekening" or "Termijnrekening")
- min_deposit: Minimum deposit in euros as a number (null if none)
- max_deposit: Maximum deposit in euros as a number (null if none)
- conditions: Any special conditions mentioned
- duration: For term deposits, the duration. null for savings accounts.

IMPORTANT:
- Return ONLY valid JSON array, no other text
- Use null for unknown fields
- Rates as decimals (0.75 not 0.75%)
- Belgian decimal separator is comma (2,70% = 2.70)
- max_deposit is the euro amount (500 not 500000 for 500 EUR/month limit)
- If no products or rates found, return []
"""

NEWS_PROMPT = """You are a data extraction assistant for a Belgian banking market watch tool.

Given the raw text from a bank's news or press page, extract ALL news articles or press releases.

For each article, extract:
- title: The headline
- description: First 1-2 sentences or summary
- pub_date: Publication date in "YYYY-MM-DD" format (if visible)
- link: Full URL to the article (if visible, otherwise null)

IMPORTANT:
- Return ONLY valid JSON array, no other text
- Only extract actual news articles, not navigation or menu items
- Include the full URL link for each article so users can click through
- If no articles found, return []
"""


def call_llm(prompt: str, raw_text: str, source: str) -> list[dict]:
    try:
        import openai
    except ImportError:
        print("  [LLM] openai not installed.")
        return []

    api_key = os.environ.get("OPENAI_API_KEY", "")
    if not api_key:
        print("  [LLM] OPENAI_API_KEY not set. Skipping.")
        return []

    client = openai.OpenAI(api_key=api_key)

    max_chars = 15000
    if len(raw_text) > max_chars:
        raw_text = raw_text[:max_chars] + "\n... [truncated]"

    print(f"  [LLM] Sending {len(raw_text)} chars to gpt-4o-mini...")

    try:
        response = client.chat.completions.create(
            model="gpt-4o-mini",
            temperature=0,
            messages=[
                {"role": "system", "content": prompt},
                {
                    "role": "user",
                    "content": f"Extract data from this {source} page:\n\n{raw_text}",
                },
            ],
        )
        result_text = response.choices[0].message.content.strip()
        if result_text.startswith("```"):
            result_text = result_text.split("\n", 1)[1]
            result_text = result_text.rsplit("```", 1)[0]
        items = json.loads(result_text)
        print(f"  [LLM] Extracted {len(items)} items")
        return items
    except json.JSONDecodeError as e:
        print(f"  [LLM] ERROR: Invalid JSON: {e}")
        return []
    except Exception as e:
        print(f"  [LLM] ERROR: {e}")
        return []


def find_previous_checksum(source_key: str) -> tuple[str, Path | None]:
    today = datetime.date.today()
    for days_ago in range(1, 8):
        prev_date = (today - datetime.timedelta(days=days_ago)).isoformat()
        filepath = SNAPSHOT_DIR / f"pw_{source_key}_{prev_date}.json"
        if filepath.exists():
            try:
                data = json.loads(filepath.read_text(encoding="utf-8"))
                return data.get("raw_text_checksum", ""), filepath
            except (json.JSONDecodeError, IOError):
                continue
    return "", None


def reuse_previous(prev_path: Path, source_key: str) -> dict:
    today = datetime.date.today().isoformat()
    now = datetime.datetime.now().isoformat(timespec="seconds")
    data = json.loads(prev_path.read_text(encoding="utf-8"))
    data["date"] = today
    data["timestamp"] = now
    data["reused_from"] = prev_path.name
    filepath = SNAPSHOT_DIR / f"pw_{source_key}_{today}.json"
    filepath.write_text(
        json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8"
    )
    print(f"  [CACHE] Unchanged → reused {prev_path.name}")
    return data


def save_snapshot(
    source: dict, items: list[dict], raw_text: str, raw_checksum: str
) -> Path:
    today = datetime.date.today().isoformat()
    now = datetime.datetime.now().isoformat(timespec="seconds")
    source_key = source["source_key"]
    url_type = source["url_type"]

    items_json = json.dumps(items, sort_keys=True)
    checksum = hashlib.sha256(items_json.encode()).hexdigest()

    snapshot = {
        "source": source_key,
        "url": source["url"],
        "timestamp": now,
        "date": today,
        "scraper": "playwright",
        "bank": source["bank"],
        "raw_text_checksum": raw_checksum,
    }

    if url_type == "products":
        snapshot["product_type"] = source.get("product_type", "regulated")
        snapshot["num_products"] = len(items)
        snapshot["checksum"] = checksum
        snapshot["products"] = items
    elif url_type == "news":
        for article in items:
            if not article.get("guid"):
                guid_base = article.get("link") or article.get("title", "")
                article["guid"] = hashlib.sha256(guid_base.encode()).hexdigest()[:16]
        guids = "|".join(a.get("guid", "") for a in items)
        snapshot["num_articles"] = len(items)
        snapshot["checksum"] = hashlib.sha256(guids.encode()).hexdigest()
        snapshot["articles"] = items

    raw_path = SNAPSHOT_DIR / f"pw_{source_key}_{today}_raw.txt"
    raw_path.write_text(raw_text, encoding="utf-8")

    filepath = SNAPSHOT_DIR / f"pw_{source_key}_{today}.json"
    filepath.write_text(
        json.dumps(snapshot, indent=2, ensure_ascii=False), encoding="utf-8"
    )
    print(f"  [SAVE] → {filepath}")
    return filepath


def scrape_source(source: dict) -> dict:
    source_key = source["source_key"]
    url = source["url"]
    url_type = source["url_type"]

    print(f"\n  {'—'*50}")
    print(f"  {source['bank']} | {url_type} | {source_key}")
    print(f"  {url}")

    raw_text = scrape_page(url)
    if not raw_text:
        print(f"  SKIPPED: No content")
        return {}

    raw_checksum = hashlib.sha256(raw_text.encode()).hexdigest()
    prev_checksum, prev_path = find_previous_checksum(source_key)

    if prev_checksum and raw_checksum == prev_checksum and prev_path:
        return reuse_previous(prev_path, source_key)

    print(f"  [NEW] Content changed. Running LLM...")

    if url_type == "news":
        items = call_llm(NEWS_PROMPT, raw_text, source["bank"])
    else:
        items = call_llm(PRODUCT_PROMPT, raw_text, source["bank"])

    save_snapshot(source, items, raw_text, raw_checksum)
    return {"items": items}


def main():
    print("=" * 60)
    print("  Playwright Bank Scraper (Excel-Driven)")
    print(f"  Date: {datetime.date.today().isoformat()}")
    print("=" * 60)

    sources = load_sources()
    if not sources:
        print("No sources found in Excel.")
        sys.exit(1)

    bank_filter = None
    mode = "all"

    for arg in sys.argv[1:]:
        if arg == "--list":
            print(
                f"\n{'source_key':40s} {'bank':22s} {'type':10s} {'product_type':15s}"
            )
            print("-" * 90)
            for s in sources:
                pt = s.get("product_type") or ""
                print(
                    f"{s['source_key']:40s} {s['bank']:22s} {s['url_type']:10s} {pt:15s}"
                )
            print(
                f"\nTotal: {len(sources)} URLs from {len(set(s['bank'] for s in sources))} banks"
            )
            sys.exit(0)
        elif arg == "--news-only":
            mode = "news-only"
        elif arg == "--products-only":
            mode = "products-only"
        elif not arg.startswith("--"):
            bank_filter = arg

    filtered = sources
    if bank_filter:
        filtered = [s for s in filtered if bank_filter.lower() in s["bank"].lower()]
        if not filtered:
            print(f"No sources for '{bank_filter}'. Available banks:")
            for bank in sorted(set(s["bank"] for s in sources)):
                print(f"  {bank}")
            sys.exit(1)
        print(f"\n[FILTER] Bank: {bank_filter} ({len(filtered)} URLs)")

    if mode == "news-only":
        filtered = [s for s in filtered if s["url_type"] == "news"]
        print(f"[FILTER] News only ({len(filtered)} URLs)")
    elif mode == "products-only":
        filtered = [s for s in filtered if s["url_type"] == "products"]
        print(f"[FILTER] Products only ({len(filtered)} URLs)")

    if not filtered:
        print("No URLs to scrape after filtering.")
        sys.exit(0)

    succeeded = 0
    failed = 0

    for source in filtered:
        try:
            result = scrape_source(source)
            if result:
                succeeded += 1
            else:
                failed += 1
        except Exception as e:
            print(f"  ERROR: {e}")
            failed += 1

    print(f"\n{'='*60}")
    print(f"  DONE: {succeeded}/{len(filtered)} succeeded")
    if failed:
        print(f"  Failed: {failed}")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()
