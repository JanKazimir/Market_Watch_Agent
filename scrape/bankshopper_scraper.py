import datetime
import requests
from bs4 import BeautifulSoup
import json
import hashlib
import re
from pathlib import Path


#---------------------
# Config
#---------------------
URL = "https://www.bankshopper.be/vergelijk-spaarrekeningen/"
SNAPSHOT_DIR = Path("snapshots")

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "nl-BE,nl;q=0.9,en;q=0.5",
}

#----------------------
# Fetch raw html
#----------------------
def fetch_html(url: str) -> str:
    response = requests.get(url, headers=HEADERS, timeout=30)
    response.raise_for_status()
    return response.text

def save_raw_html(html: str, date_str: str) -> Path:
    """Save raw HTML snapshot with date in filename."""
    SNAPSHOT_DIR.mkdir(parents=True, exist_ok=True)
    filepath = SNAPSHOT_DIR / f"bankshopper_{date_str}.html"
    filepath.write_text(html, encoding="utf-8")
    print(f"[SCRAPE] Saved raw HTML → {filepath} ({len(html):,} bytes)")
    return filepath

#----------------------
# Parse html into JSON
#---------------------
def parse_percentage(text: str) -> float | None:
    """
    Convert percentage into float
    Return None if it fails
    """
    if not text:
        return None
    cleaned = text.strip().replace("%", "").replace(",", ".").strip()
    try:
        return float(cleaned)
    except ValueError:
        return None

def parse_max_deposit(text: str) -> str | None:
    """
    Extract max deposit info from text like
    Returns the original text cleaned up, or None if empty or dash
    """
    if not text or text.strip() in ("-", ""):
        return None
    return text.strip()

def extract_detail_field(detail_section, field_name: str) -> str | None:
    """
    Extract a value from the expanded detail section.
    The detail sections have a label/value structure.
    """
    if not detail_section:
        return None

    # Look for the field name in the detail text
    text = detail_section.get_text(separator="\n")
    lines = [line.strip() for line in text.split("\n") if line.strip()]

    for i, line in enumerate(lines):
        if field_name.lower() in line.lower():
            # The value is usually the next non-empty line
            if i + 1 < len(lines):
                return lines[i + 1].strip()
    return None


def extract_products(html: str) -> list[dict]:
    """
    Parse the Bankshopper savings comparison page and extract
    all product information into a list of structured dictionaries.

    Each product dict contains:
    - bank: str (bank name)
    - product_name: str (account name)
    - base_rate: float | None (basisrente)
    - fidelity_premium: float | None (getrouwheidspremie)
    - total_rate: float | None (totale rente)
    - account_type: str | None (gereglementeerd / niet-gereglementeerd)
    - group: str | None (banking group)
    - country_of_group: str | None
    - guarantee_fund: str | None
    - deposit_guarantee: str | None
    - min_deposit: str | None
    - max_deposit: str | None
    - open_online: str | None
    - management_costs: str | None
    - moody_rating: str | None
    - sp_rating: str | None
    - fitch_rating: str | None
    - product_sheet_url: str | None (link to essential saver info PDF)
    """
    soup = BeautifulSoup(html, "lxml")
    products = []

    all_items = soup.find_all("li")

    for item in all_items:
        # Skip items that don't look like product cards
        img = item.find("img")
        if not img:
            continue

        item_text = item.get_text(separator="\n", strip=True)

        # Must contain rate-related keywords to be a product item
        if "Basisrente" not in item_text and "Totaal" not in item_text:
            continue

        product = {}

        # --- Bank name: from the img alt attribute ---
        product["bank"] = img.get("alt", "").strip() or img.get("title", "").strip()

        # --- Product name: usually the first significant text after the logo ---
        # Find all text nodes and look for the account name
        text_lines = [
            line.strip()
            for line in item_text.split("\n")
            if line.strip()
            and line.strip() not in ("Toon details", "Verberg details", "Open online")
            and "%" not in line
            and line.strip() != product["bank"]
        ]

        # The product name is typically the first meaningful text line
        product["product_name"] = None
        for line in text_lines:
            if line.lower() not in (
                "basisrente", "getrouwheidspremie", "totaal",
                "type", "bank", "groep", "land van groep",
                "waarborgfonds", "waarborg deposito's",
                "minimum inlage", "maximum inlage",
                "online openen", "beheerskosten", "fiscaliteit",
                "openen in een agentschap", "extra voorwaarden",
                "essentiële spaardersinformatie",
                "rating moody's", "rating s&p", "rating fitch",
                "rentegarantie", "neen", "ja", "-"
            ) and not line.startswith("€") and not line.startswith("Moody"):
                product["product_name"] = line
                break

        # --- Rates: find percentage values ---
        # Extract all percentage values in order
        pct_pattern = re.compile(r"(\d+[.,]\d+)\s*%")
        percentages = pct_pattern.findall(item_text)

        # Map percentages to their labels
        # The text follows pattern: value% \n label \n value% \n label \n value% \n label
        rate_lines = item_text.split("\n")
        rate_lines = [l.strip() for l in rate_lines if l.strip()]

        product["base_rate"] = None
        product["fidelity_premium"] = None
        product["total_rate"] = None

        for i, line in enumerate(rate_lines):
            if "Basisrente" in line:
                # Look backwards for the percentage
                for j in range(i - 1, max(i - 3, -1), -1):
                    if j >= 0 and "%" in rate_lines[j]:
                        product["base_rate"] = parse_percentage(rate_lines[j])
                        break
            elif "Getrouwheidspremie" in line:
                for j in range(i - 1, max(i - 3, -1), -1):
                    if j >= 0 and "%" in rate_lines[j]:
                        product["fidelity_premium"] = parse_percentage(rate_lines[j])
                        break
            elif line == "Totaal":
                for j in range(i - 1, max(i - 3, -1), -1):
                    if j >= 0 and "%" in rate_lines[j]:
                        product["total_rate"] = parse_percentage(rate_lines[j])
                        break

        # If we couldn't map rates by label, fall back to positional
        if product["total_rate"] is None and len(percentages) >= 1:
            vals = [float(p.replace(",", ".")) for p in percentages]
            # The total is usually the largest or the last of the header rates
            if len(vals) >= 3:
                product["base_rate"] = product["base_rate"] or vals[0]
                product["fidelity_premium"] = product["fidelity_premium"] or vals[1]
                product["total_rate"] = product["total_rate"] or vals[2]
            elif len(vals) == 2:
                product["base_rate"] = product["base_rate"] or vals[0]
                product["total_rate"] = product["total_rate"] or vals[1]

        # --- Detail fields ---
        product["account_type"] = extract_detail_field(item, "Type")
        product["group"] = extract_detail_field(item, "Groep")
        product["country_of_group"] = extract_detail_field(item, "Land van groep")
        product["guarantee_fund"] = extract_detail_field(item, "Waarborgfonds")
        product["deposit_guarantee"] = extract_detail_field(item, "Waarborg deposito")
        product["min_deposit"] = parse_max_deposit(extract_detail_field(item, "Minimum inlage"))
        product["max_deposit"] = parse_max_deposit(extract_detail_field(item, "Maximum inlage"))
        product["open_online"] = extract_detail_field(item, "Online openen")
        product["management_costs"] = extract_detail_field(item, "Beheerskosten")
        product["moody_rating"] = extract_detail_field(item, "Rating Moody")
        product["sp_rating"] = extract_detail_field(item, "Rating S&P")
        product["fitch_rating"] = extract_detail_field(item, "Rating Fitch")

        # --- Product sheet URL ---
        product["product_sheet_url"] = None
        esi_link = item.find("a", string=re.compile(r"Essentiële spaardersinformatie", re.IGNORECASE))
        if esi_link:
            product["product_sheet_url"] = esi_link.get("href")

        # Only add if we got at least a bank name and some rate data
        if product["bank"] and (product["total_rate"] is not None or product["base_rate"] is not None):
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
    filepath = SNAPSHOT_DIR / f"bankshopper_{date_str}.json"
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
        print(f"  {p['bank']} — {p['product_name']}")
        print(f"    Base: {p['base_rate']}% | Fidelity: {p['fidelity_premium']}% | Total: {p['total_rate']}%")
        print()


if __name__ == "__main__":
    main()