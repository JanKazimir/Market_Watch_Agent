
import requests
from bs4 import BeautifulSoup
import json
import hashlib
import datetime
import re
from pathlib import Path

SNAPSHOT_DIR = Path.home("snapshots")

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

KBC_META = {
    "group": "KBC Group",
    "country_of_group": "Belgique",
    "guarantee_fund": "Belgique",
    "deposit_guarantee": "€ 100.000,00",
    "min_deposit": None,
    "max_deposit": None,
    "open_online": "Oui",
    "management_costs": "Gratuit",
    "moody_rating": "Aa3",
    "sp_rating": "A+",
    "fitch_rating": None,
}

# ── Helpers ───────────────────────────────────────────────────────────────────

def parse_rate(text):
    match = re.search(r"[\d,\.]+", text)
    return float(match.group().replace(",", ".")) if match else None

def get_soup(url, lang="en"):
    headers = {**HEADERS, "Accept-Language": "fr-BE,fr;q=0.9" if lang == "fr" else "en-BE,en;q=0.9"}
    r = requests.get(url.split("?")[0], headers=headers)
    return BeautifulSoup(r.text, "html.parser")

def get_pdf(soup):
    tag = soup.find("a", class_="download-item__text-wrapper__link")
    return tag["href"] if tag else None

def make_product(product_name, product_type, base, bonus, pdf, extra=None):
    product = {
        "bank": "KBC",
        "product_name": product_name,
        "base_rate": base,
        "fidelity_premium": bonus,
        "total_rate": round((base or 0) + (bonus or 0), 2),
        "account_type": product_type,
        **KBC_META,
        "product_sheet_url": pdf,
    }
    if extra:
        product.update(extra)
    return product

# ── Scrapers ──────────────────────────────────────────────────────────────────

def scrape_simple(url, product_type, lang):
    print(f"[SCRAPE] {url}")
    soup = get_soup(url, lang)

    h1 = soup.find("h1")
    product_name = h1.get_text(strip=True) if h1 else None
    base, bonus = None, None

    table = soup.find("div", {"data-component-type": "table"})
    if table:
        for tr in table.find_all("tr"):
            cells = tr.find_all("td")
            if len(cells) == 2:
                label = cells[0].get_text(strip=True).lower()
                value = cells[1].get_text(strip=True)
                if "base" in label:
                    base = parse_rate(value)
                elif any(w in label for w in ["fidélité", "fidelity", "loyalty"]):
                    bonus = parse_rate(value)

    return [make_product(product_name, product_type, base, bonus, get_pdf(soup))]


def scrape_reglementes(url, lang):
    print(f"[SCRAPE] {url}")
    soup = get_soup(url, lang)
    products = []

    for tbody in soup.find_all("tbody"):
        product_name = None
        pdf_tag = tbody.find("a", href=lambda h: h and ".pdf" in h)
        pdf = pdf_tag["href"] if pdf_tag else None

        for tr in tbody.find_all("tr"):
            cells = tr.find_all("td")

            if len(cells) == 1 and cells[0].get("colspan") == "5":
                text = cells[0].get_text(strip=True)
                if any(c.isdigit() for c in text[:3]):
                    product_name = text
                continue

            if cells and cells[0].get_text(strip=True) == "Catégorie":
                continue

            if len(cells) == 5 and product_name:
                base = parse_rate(cells[1].get_text())
                bonus = parse_rate(cells[2].get_text())
                products.append(make_product(
                    product_name=product_name,
                    product_type="Compte d'épargne réglementé",
                    base=base,
                    bonus=bonus,
                    pdf=pdf,
                    extra={
                        "category": cells[0].get_text(strip=True),
                        "conditions": cells[4].get_text(strip=True),
                    }
                ))

    return products

# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    all_products = []
    date_str = datetime.date.today().isoformat()

    all_products += scrape_reglementes(
        "https://www.kbc.be/particuliers/fr/epargner/comptes-epargne-reglementes.html", "fr"
    )

    simple_pages = [
        ("https://www.kbc.be/retail/en/savings/savings-accounts/savings-account-for-third-parties.html", "Savings Account for Third Parties", "en"),
        ("https://www.kbc.be/retail/en/savings/savings-accounts/savings-accounts.html", "Savings Account", "en"),
        ("https://www.kbc.be/retail/en/savings/savings-accounts/security-deposit-account.html", "Security Deposit Account", "en"),
    ]

    for url, product_type, lang in simple_pages:
        all_products += scrape_simple(url, product_type, lang)

    snapshot = {
        "source": "kbc.be",
        "url": "https://www.kbc.be",
        "timestamp": datetime.datetime.now().isoformat(),
        "date": date_str,
        "num_products": len(all_products),
        "checksum": hashlib.sha256(json.dumps(all_products, sort_keys=True).encode()).hexdigest(),
        "products": all_products,
    }

    SNAPSHOT_DIR.mkdir(parents=True, exist_ok=True)
    filepath = SNAPSHOT_DIR / f"kbc2_{date_str}.json"
    filepath.write_text(json.dumps(snapshot, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"\n✅ {len(all_products)} produits → {filepath}")

if __name__ == "__main__":
    main()