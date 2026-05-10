import datetime
import requests
from bs4 import BeautifulSoup
import json
import hashlib
from pathlib import Path


# ---------------------
# Config
# ---------------------
# URL = "https://www.bankshopper.be/vergelijk-spaarrekeningen/"
URL = "https://www.bankshopper.be/termijnrekeningen-vergelijken/" # modified here
SNAPSHOT_DIR = Path("data/snapshots")

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "nl-BE,nl;q=0.9,en;q=0.5",
}

# ----------------------
# Fetch raw html
# ----------------------
def fetch_html(url: str) -> str:
    response = requests.get(url, headers=HEADERS, timeout=30)
    response.raise_for_status()
    return response.text

def save_raw_html(html: str, date_str: str) -> Path:
    """Save raw HTML snapshot with date in filename."""
    SNAPSHOT_DIR.mkdir(parents=True, exist_ok=True)
    filepath = SNAPSHOT_DIR / f"bankshopper_term_acc_{date_str}.html"  # modified here
    filepath.write_text(html, encoding="utf-8")
    print(f"[SCRAPE] Saved raw HTML → {filepath} ({len(html):,} bytes)")
    return filepath

# ----------------------
# Parse html into JSON
# ---------------------
def parse_float(value: str | None) -> float | None:
    """Convert a numeric string (e.g. '2.00') to float, or None."""
    if not value:
        return None
    try:
        return float(value)
    except ValueError:
        return None

def clean_text(text: str | None) -> str | None:
    """Strip whitespace, return None for empty or dash-only values."""
    if not text:
        return None
    text = text.strip()
    return text if text and text != "-" else None

def get_detail_field(detail_block, label: str) -> str | None:
    """
    Extract a detail value from the label/value grid inside a product card.
    Each pair is: <div class="uk-text-bold">Label</div><div>Value</div>
    Some values are tooltip-only (text in an <i data-content="...">).
    """
    if not detail_block:
        return None
    for bold_div in detail_block.find_all("div", class_="uk-text-bold"):
        if bold_div.get_text(strip=True) == label:
            val_div = bold_div.find_next_sibling("div")
            if not val_div:
                return None
            # Check for tooltip (e.g. Extra voorwaarden, Fiscaliteit)
            tooltip = val_div.find("i", class_=lambda c: c and "tooltip" in c)
            if tooltip and tooltip.get("data-content"):
                return clean_text(tooltip["data-content"])
            return clean_text(val_div.get_text(strip=True))
    return None


def extract_products(html: str) -> list[dict]:
    """
    Parse the Bankshopper term-account comparison page.
    Each product is an <li class="bank-tabel-list-item"> with structured divs.
    """
    soup = BeautifulSoup(html, "lxml")
    products = []

    for item in soup.find_all("li", class_="bank-tabel-list-item"):
        # Duration from data attribute
        duration = item.get("data-termijn-duur")

        # Bank name: from div.bank data-content, fallback to img alt
        bank_div = item.find("div", class_="bank")
        bank_name = None
        if bank_div:
            bank_name = clean_text(bank_div.get("data-content"))
            if not bank_name:
                img = bank_div.find("img")
                if img:
                    bank_name = clean_text(img.get("alt"))

        # Product name
        title_div = item.find("div", class_="bank-tabel-list-item-title")
        product_name = title_div.get_text(strip=True) if title_div else None

        # Rates from data-content attributes
        bruto_div = item.find("div", class_="bruto")
        netto_div = item.find("div", class_="netto")
        base_rate = parse_float(bruto_div.get("data-content")) if bruto_div else None
        net_rate = parse_float(netto_div.get("data-content")) if netto_div else None

        # Min deposit from data-content
        min_div = item.find("div", class_="minimum")
        min_deposit_num = parse_float(min_div.get("data-content")) if min_div else None
        min_deposit = f"€ {min_deposit_num:,.2f}" if min_deposit_num is not None else None

        # Detail fields
        detail = item.find("div", class_="bank-tabel-item-meerinfo-block")

        product = {
            "product_name": product_name,
            "duration": duration,
            "base_rate": base_rate,
            "fidelity_premium": None,
            "net_rate": net_rate,
            "account_type": get_detail_field(detail, "Type"),
            "capitalisation": get_detail_field(detail, "Kapitalisatie"),
            "bank": bank_name or get_detail_field(detail, "Bank"),
            "group": get_detail_field(detail, "Groep"),
            "country_of_group": get_detail_field(detail, "Land van groep"),
            "min_deposit": min_deposit,
            "fiscaliteit": get_detail_field(detail, "Fiscaliteit"),
            "guarantee_fund": get_detail_field(detail, "Waarborgfonds"),
            "deposit_guarantee": get_detail_field(detail, "Waarborg deposito's"),
            "moody_rating": get_detail_field(detail, "Rating Moody's"),
            "sp_rating": get_detail_field(detail, "Rating S&P"),
            "fitch_rating": get_detail_field(detail, "Rating Fitch"),
            "conditions": get_detail_field(detail, "Extra voorwaarden"),
        }

        # Only add if we have at least a name and some rate
        if product_name and (base_rate is not None or net_rate is not None):
            products.append(product)

    return products

def compute_checksum(data: str) -> str:
    """Compute SHA-256 checksum of the content for change detection."""
    return hashlib.sha256(data.encode("utf-8")).hexdigest()

def build_snapshot(url: str, html: str, products: list[dict]) -> dict:
    """
    Build the final structured snapshot with metadata.
    This is the standardized output format for the pipeline.
    """
    return {
        "source": "bankshopper.be",
        "url": url,
        "timestamp": datetime.datetime.now().isoformat(),
        "date": datetime.date.today().isoformat(),
        "num_products": len(products),
        "checksum": compute_checksum(json.dumps(products, sort_keys=True)),
        "products": products,
    }


def save_snapshot(snapshot: dict, date_str: str) -> Path:
    """Save structured snapshot as JSON."""
    SNAPSHOT_DIR.mkdir(parents=True, exist_ok=True)
    filepath = (
        SNAPSHOT_DIR / f"bankshopper_term_acc_{date_str}.json")  # path modified here
    filepath.write_text(
        json.dumps(snapshot, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    print(f"[EXTRACT] Saved structured data → {filepath} ({snapshot['num_products']} products)")
    return filepath

# ---------------------------------------------------------------------------
# MAIN — Run the scraper + extractor pipeline
# ---------------------------------------------------------------------------
def main():
    today = datetime.date.today().isoformat()
    print(f"\n{'='*60}")
    print(f"  Bankshopper Scraper — {today}")
    print(f"{'='*60}\n")

    # Step 1: Fetch HTML
    print("[SCRAPE] Fetching HTML from bankshopper.be ...")
    try:
        html = fetch_html(URL)
    except requests.RequestException as e:
        print(f"[ERROR] Failed to fetch page: {e}")
        print("[INFO] If this keeps failing, check if the site blocks automated requests.")
        print("[INFO] You may need to use Playwright for browser-based scraping.")
        return

    save_raw_html(html, today)

    # Step 2: Extract products
    print("[EXTRACT] Parsing product data ...")
    products = extract_products(html)

    if not products:
        print("[WARNING] No products found! The HTML structure may have changed.")
        print("[INFO] Check the raw HTML file and update the selectors in extract_products().")
        return

    # Build and save structured snapshot
    snapshot = build_snapshot(URL, html, products)
    save_snapshot(snapshot, today)

    # Print summary
    print(f"\n--- Summary ---")
    print(f"Products found: {snapshot['num_products']}")
    print(f"Checksum: {snapshot['checksum'][:16]}...")
    print(f"\nSample products:")
    for p in products[:3]:
        print(f"  {p['bank']} — {p['product_name']} ({p['duration']})")
        print(f"    Bruto: {p['base_rate']}% | Netto: {p['net_rate']}%")
        print()


if __name__ == "__main__":
    main()
