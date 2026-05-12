"""
Playwright Bank Scraper (Excel-Driven)
=======================================
Reads bank URLs from list_of_banks_ref_new.xlsx.

Excel columns:
  0: bank
  1: link_type
  2: notes
  3: scrape_frequency
  4: url
  5: url_status
  6: parent_url
  7: possible_new_link

Link types handled:
  - news                → news articles (LLM extraction)
  - regulated_savings   → product data (LLM extraction)
  - term_accounts       → product data (LLM extraction)
  - tarif_pdf           → PDF monitoring
  - regulated_pdf       → PDF monitoring
  - term_pdf            → PDF monitoring

PDF monitoring strategy:
  - With parent_url: scrape parent page, find all PDF links, compare daily
  - Without parent_url: HEAD request on direct PDF URL, checksum content

Usage:
    python playwright_scraper.py                        # scrape all
    python playwright_scraper.py --list                 # list all sources
    python playwright_scraper.py --news-only            # news pages only
    python playwright_scraper.py --products-only        # product pages only
    python playwright_scraper.py --pdfs-only            # check PDFs only
    python playwright_scraper.py "Argenta"              # one bank only
"""

import json
import os
import re
import sys
import hashlib
import datetime
import requests
from pathlib import Path

try:
    from playwright.sync_api import sync_playwright
except ImportError:
    print("ERROR: playwright not installed.")
    print("  pip install playwright && playwright install chromium")
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

# --- Configuration ---

SNAPSHOT_DIR = Path("data/snapshots")
SNAPSHOT_DIR.mkdir(parents=True, exist_ok=True)

SOURCES_FILE = Path("data/list_of_banks_ref_new.xlsx")

# Link types
NEWS_TYPES = {"news"}
PRODUCT_TYPES = {"regulated_savings", "term_accounts"}
PDF_TYPES = {"regulated_pdf", "term_pdf", "tarif_pdf"}

PRODUCT_TYPE_MAP = {
    "regulated_savings": "regulated",
    "term_accounts": "unregulated",
}

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


# ---------------------------------------------------------------------------
# EXCEL READER
# ---------------------------------------------------------------------------


def safe_str(value) -> str:
    if value is None:
        return ""
    return str(value).strip().strip('"').strip()


def make_source_key(bank_name: str, link_type: str, index: int = 0) -> str:
    bank_clean = re.sub(r"[^\w]", "_", bank_name.lower()).strip("_")
    type_clean = re.sub(r"[^\w]", "_", link_type.lower()).strip("_")
    key = f"{bank_clean}_{type_clean}"
    if index > 0:
        key += f"_{index}"
    return key


def load_sources(filepath: Path = SOURCES_FILE) -> list[dict]:
    """Load sources from Excel. Each row = one URL."""
    if not filepath.exists():
        print(f"ERROR: Sources file not found: {filepath}")
        sys.exit(1)

    wb = openpyxl.load_workbook(filepath, read_only=True)
    ws = wb.active
    sources = []
    key_counts = {}

    for row in ws.iter_rows(min_row=2, values_only=True):
        bank = safe_str(row[0])
        link_type = safe_str(row[1])
        url = safe_str(row[4]) if len(row) > 4 else ""
        parent_url = safe_str(row[6]) if len(row) > 6 else ""

        if not bank or not link_type:
            continue
        # Skip explanation row
        if "explanation" in bank.lower() or "this row" in bank.lower():
            continue
        if not url or not url.startswith("http"):
            continue

        # Determine what to do with this row
        if link_type in NEWS_TYPES:
            url_type = "news"
            scrape_url = url
            product_type = None
        elif link_type in PRODUCT_TYPES:
            url_type = "products"
            scrape_url = url
            product_type = PRODUCT_TYPE_MAP.get(link_type, "regulated")
        elif link_type in PDF_TYPES:
            url_type = "pdf_check"
            scrape_url = parent_url if parent_url.startswith("http") else url
            product_type = None
        else:
            continue

        # Create unique source_key
        base_key = f"{bank}|{link_type}"
        if base_key not in key_counts:
            key_counts[base_key] = 0
        else:
            key_counts[base_key] += 1

        source_key = make_source_key(bank, link_type, key_counts[base_key])

        sources.append(
            {
                "source_key": source_key,
                "bank": bank,
                "url": scrape_url,
                "pdf_url": url if link_type in PDF_TYPES else None,
                "parent_url": parent_url if parent_url.startswith("http") else None,
                "url_type": url_type,
                "link_type": link_type,
                "product_type": product_type,
            }
        )

    wb.close()

    news_count = len([s for s in sources if s["url_type"] == "news"])
    product_count = len([s for s in sources if s["url_type"] == "products"])
    pdf_count = len([s for s in sources if s["url_type"] == "pdf_check"])
    banks = len(set(s["bank"] for s in sources))

    print(f"[EXCEL] Loaded {len(sources)} URLs from {banks} banks")
    print(
        f"        {product_count} product, {news_count} news, {pdf_count} PDF check pages"
    )
    return sources


# ---------------------------------------------------------------------------
# PLAYWRIGHT SCRAPER
# ---------------------------------------------------------------------------


def handle_cookies(page):
    for selector in COOKIE_SELECTORS:
        try:
            page.click(selector, timeout=1000)
            print(f"  [COOKIES] Accepted via: {selector}")
            page.wait_for_timeout(1000)
            return True
        except Exception:
            continue
    return False


def scrape_page(url: str, wait_ms: int = 3000) -> str:
    """Scrape a page and return visible text."""
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            locale="nl-BE",
            viewport={"width": 1920, "height": 1080},
        )
        page = context.new_page()

        print(f"  [PW] Navigating to {url}...")
        try:
            page.goto(url, wait_until="networkidle", timeout=20000)
        except Exception:
            try:
                page.goto(url, wait_until="domcontentloaded", timeout=20000)
            except Exception as e:
                print(f"  [PW] ERROR: {e}")
                browser.close()
                return ""

        handle_cookies(page)
        page.wait_for_timeout(wait_ms)
        page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        page.wait_for_timeout(1000)
        page.evaluate("window.scrollTo(0, 0)")

        text = page.inner_text("body")
        print(f"  [PW] Extracted {len(text)} characters")
        browser.close()
        return text


def scrape_pdf_links(url: str, wait_ms: int = 3000) -> list[str]:
    """Scrape a page and extract all PDF links from it."""
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            locale="nl-BE",
            viewport={"width": 1920, "height": 1080},
        )
        page = context.new_page()

        print(f"  [PW] Navigating to {url}...")
        try:
            page.goto(url, wait_until="networkidle", timeout=20000)
        except Exception:
            try:
                page.goto(url, wait_until="domcontentloaded", timeout=20000)
            except Exception as e:
                print(f"  [PW] ERROR: {e}")
                browser.close()
                return []

        handle_cookies(page)
        page.wait_for_timeout(wait_ms)

        pdf_links = page.evaluate("""
            () => {
                const links = document.querySelectorAll('a[href]');
                const pdfs = [];
                for (const link of links) {
                    const href = link.href;
                    if (href && href.toLowerCase().endsWith('.pdf')) {
                        pdfs.push(href);
                    }
                }
                return [...new Set(pdfs)];
            }
        """)

        print(f"  [PW] Found {len(pdf_links)} PDF links on page")
        browser.close()
        return pdf_links


# ---------------------------------------------------------------------------
# LLM EXTRACTION
# ---------------------------------------------------------------------------

PRODUCT_PROMPT = """You are a data extraction assistant for a Belgian banking market watch tool.

Given the raw text content from a bank's savings page, extract ALL savings products mentioned.

For each product, extract:
- bank: The bank name
- product_name: The full product name
- base_rate: Base interest rate as a decimal (e.g. 0.75 for 0.75%)
- fidelity_premium: Fidelity/loyalty premium as a decimal (null for term deposits)
- total_rate: Total rate as a decimal
- account_type: Type of account
- min_deposit: Minimum deposit in euros as a number (null if none)
- max_deposit: Maximum deposit in euros as a number (null if none)
- conditions: Any special conditions mentioned
- duration: For term deposits, the duration. null for savings accounts.

IMPORTANT:
- Return ONLY valid JSON array, no other text
- Use null for unknown fields
- Rates as decimals (0.75 not 0.75%)
- Belgian decimal separator is comma (2,70% = 2.70)
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
- If no articles found, return []
"""


def call_llm(prompt: str, raw_text: str, source: str) -> list[dict]:
    try:
        import openai
    except ImportError:
        print("  [LLM] openai not installed.")
        return []

    api_key = os.environ.get("API_KEY", "")
    if not api_key:
        print("  [LLM] API_KEY not set. Skipping.")
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


# ---------------------------------------------------------------------------
# CHECKSUM CACHING
# ---------------------------------------------------------------------------


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
    print(f"  [CACHE] Unchanged, reused {prev_path.name}")
    return data


# ---------------------------------------------------------------------------
# SAVE SNAPSHOTS
# ---------------------------------------------------------------------------


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
        "link_type": source["link_type"],
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

    # Save raw text for debugging
    raw_path = SNAPSHOT_DIR / f"pw_{source_key}_{today}_raw.txt"
    raw_path.write_text(raw_text, encoding="utf-8")

    filepath = SNAPSHOT_DIR / f"pw_{source_key}_{today}.json"
    filepath.write_text(
        json.dumps(snapshot, indent=2, ensure_ascii=False), encoding="utf-8"
    )
    print(f"  [SAVE] {filepath}")
    return filepath


def save_pdf_snapshot(
    source: dict, pdf_links: list[str], pdf_checksum: str = None
) -> Path:
    """Save a snapshot of PDF links found on a page, or direct PDF checksum."""
    today = datetime.date.today().isoformat()
    now = datetime.datetime.now().isoformat(timespec="seconds")
    source_key = source["source_key"]

    links_str = "|".join(sorted(pdf_links))
    checksum = (
        hashlib.sha256(links_str.encode()).hexdigest()
        if pdf_links
        else (pdf_checksum or "")
    )

    snapshot = {
        "source": source_key,
        "url": source["url"],
        "timestamp": now,
        "date": today,
        "scraper": "playwright",
        "bank": source["bank"],
        "link_type": source["link_type"],
        "known_pdf_url": source.get("pdf_url", ""),
        "parent_url": source.get("parent_url", ""),
        "num_pdf_links": len(pdf_links),
        "checksum": checksum,
        "pdf_links": pdf_links,
    }

    filepath = SNAPSHOT_DIR / f"pw_{source_key}_{today}.json"
    filepath.write_text(
        json.dumps(snapshot, indent=2, ensure_ascii=False), encoding="utf-8"
    )
    print(f"  [SAVE] {filepath}")
    return filepath


# ---------------------------------------------------------------------------
# SCRAPE SOURCES
# ---------------------------------------------------------------------------


def scrape_source(source: dict) -> dict:
    """Scrape one source: products or news."""
    source_key = source["source_key"]
    url = source["url"]
    url_type = source["url_type"]

    print(f"\n  {'—'*50}")
    print(f"  {source['bank']} | {source['link_type']} | {source_key}")
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


def check_pdf(source: dict) -> dict:
    """Check PDF links. Uses parent page if available, otherwise checksums direct URL."""
    source_key = source["source_key"]
    parent_url = source.get("parent_url")
    pdf_url = source.get("pdf_url", "")

    print(f"\n  {'—'*50}")
    print(f"  {source['bank']} | {source['link_type']} | PDF CHECK")

    if parent_url:
        # Strategy 1: Scrape parent page for all PDF links
        print(f"  Parent page: {parent_url}")
        pdf_links = scrape_pdf_links(parent_url)

        if not pdf_links:
            print(f"  No PDF links found on parent page")
            save_pdf_snapshot(source, [])
            return {}

        # Compare with yesterday
        prev_checksum, prev_path = find_previous_checksum(source_key)
        links_str = "|".join(sorted(pdf_links))
        current_checksum = hashlib.sha256(links_str.encode()).hexdigest()

        if prev_checksum and current_checksum == prev_checksum:
            print(f"  [PDF] No changes, same {len(pdf_links)} PDF links")
            save_pdf_snapshot(source, pdf_links)
            return {}

        # Find what changed
        old_links = set()
        if prev_path:
            try:
                prev_data = json.loads(prev_path.read_text(encoding="utf-8"))
                old_links = set(prev_data.get("pdf_links", []))
            except (json.JSONDecodeError, IOError):
                pass

        new_links = set(pdf_links) - old_links
        removed_links = old_links - set(pdf_links)

        if new_links:
            print(f"  [PDF] NEW PDF links detected!")
            for link in new_links:
                print(f"    + {link}")
        if removed_links:
            print(f"  [PDF] Removed PDF links:")
            for link in removed_links:
                print(f"    - {link}")
        if not new_links and not removed_links and not prev_path:
            print(f"  [PDF] First run, {len(pdf_links)} PDF links recorded")

        save_pdf_snapshot(source, pdf_links)
        return {"new_pdfs": list(new_links), "removed_pdfs": list(removed_links)}

    else:
        # Strategy 2: Direct PDF URL checksum
        print(f"  Direct PDF: {pdf_url}")
        if not pdf_url or not pdf_url.startswith("http"):
            print(f"  SKIPPED: No valid PDF URL")
            return {}

        try:
            response = requests.head(pdf_url, timeout=15, allow_redirects=True)
            if response.status_code != 200:
                print(f"  [PDF] WARNING: HTTP {response.status_code} for {pdf_url}")
                save_pdf_snapshot(source, [pdf_url], pdf_checksum="DEAD_LINK")
                return {"status": "dead_link", "url": pdf_url}

            # Check content-length or etag for changes
            content_length = response.headers.get("content-length", "")
            etag = response.headers.get("etag", "")
            last_modified = response.headers.get("last-modified", "")
            header_checksum = hashlib.sha256(
                f"{content_length}|{etag}|{last_modified}".encode()
            ).hexdigest()

            prev_checksum, prev_path = find_previous_checksum(source_key)

            if prev_checksum and header_checksum == prev_checksum:
                print(f"  [PDF] No changes (same headers)")
                save_pdf_snapshot(source, [pdf_url], pdf_checksum=header_checksum)
                return {}

            if prev_path and not prev_checksum:
                print(f"  [PDF] First run, recording PDF headers")
            else:
                print(f"  [PDF] PDF may have changed! Headers differ from previous.")
                if last_modified:
                    print(f"    Last-Modified: {last_modified}")

            save_pdf_snapshot(source, [pdf_url], pdf_checksum=header_checksum)
            return {
                "status": "changed" if prev_checksum else "first_run",
                "url": pdf_url,
            }

        except requests.RequestException as e:
            print(f"  [PDF] ERROR: {e}")
            save_pdf_snapshot(source, [pdf_url], pdf_checksum="ERROR")
            return {"status": "error", "url": pdf_url, "error": str(e)}


# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------


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
                f"\n{'source_key':45s} {'bank':25s} {'link_type':20s} {'url_type':10s}"
            )
            print("-" * 105)
            for s in sources:
                print(
                    f"{s['source_key']:45s} {s['bank']:25s} {s['link_type']:20s} {s['url_type']:10s}"
                )
            print(
                f"\nTotal: {len(sources)} URLs from {len(set(s['bank'] for s in sources))} banks"
            )
            sys.exit(0)
        elif arg == "--news-only":
            mode = "news-only"
        elif arg == "--products-only":
            mode = "products-only"
        elif arg == "--pdfs-only":
            mode = "pdfs-only"
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
    elif mode == "pdfs-only":
        filtered = [s for s in filtered if s["url_type"] == "pdf_check"]
        print(f"[FILTER] PDF checks only ({len(filtered)} URLs)")

    if not filtered:
        print("No URLs to scrape after filtering.")
        sys.exit(0)

    succeeded = 0
    failed = 0
    pdf_changes = 0

    for source in filtered:
        try:
            if source["url_type"] == "pdf_check":
                result = check_pdf(source)
                if result and (
                    result.get("new_pdfs") or result.get("status") == "changed"
                ):
                    pdf_changes += 1
                succeeded += 1
            else:
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
    if pdf_changes:
        print(f"  PDF changes detected: {pdf_changes}")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()
