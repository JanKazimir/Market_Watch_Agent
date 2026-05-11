## Module that reads and backs up the excel for pdf links, downloads them PDFs, checksums them, and keeps only new versions.

import csv
import hashlib
import json
import requests
import datetime
import re
import shutil
import pandas as pd
from pathlib import Path



from pathlib import Path


# Should have a way to update the pdf link by looking at the new url from the url scraping diff.

#
## CONFIG
#

today = datetime.date.today().isoformat()
BASE_DIR = Path(__file__).parent.parent.parent

# EXCEL

EXCEL_SOURCE = "list_of_banks_ref_new.xlsx"
EXCEL_PATH = BASE_DIR/ "data" / EXCEL_SOURCE
BACKUP_DIR = BASE_DIR / "data" / "excel_backups"
OUTPUT_PATH = BASE_DIR / "data" / "pdf_links_csv" / f"{today}_pdf_list.csv"

# PDF SCRAPPER
DATA_DIR = BASE_DIR / "data"
CSV_PATH = DATA_DIR / "pdf_links_csv" / f"{today}_pdf_list.csv"
LATEST_DIR = DATA_DIR / "pdfs" / "latest"
ARCHIVE_DIR = DATA_DIR / "pdfs" / "archive"
OUTPUT_DIR = DATA_DIR / "outputs"

print(BASE_DIR)


#
## Read excel
#

## Reads the excel reference file and outputs a CSV of PDF links to scrape.
##
## Output columns:
##   source_key   : e.g. argenta_saving, argenta_terms
##   bank         : e.g. Argenta
##   url          : the pdf url to scrape
##   product_type : regulated / unregulated / tarif
##   language     : nl / fr (inferred from url)
##   pdf_source   : parent page where the pdf download link lives



# link_type values in the "Links" sheet that correspond to PDFs
# maps link_type -> (source_key suffix, product_type)
PDF_LINK_TYPES = {
    "regulated_pdf": ("saving",  "regulated"),
    "term_pdf":      ("terms",   "unregulated"),
    "tarif_pdf":     ("tarif",   "tarif"),
}


#
# Helper Functions
#

def backup_excel():
    """Copy the source excel to excel_backups/ with today's date appended."""
    BACKUP_DIR.mkdir(parents=True, exist_ok=True)
    stem = EXCEL_PATH.stem
    backup_path = BACKUP_DIR / f"{stem}_{today}{EXCEL_PATH.suffix}"
    shutil.copy2(EXCEL_PATH, backup_path)
    print("Source Excel backed-up.")
    return backup_path


def load_pdf_links() -> pd.DataFrame:
    """Load the 'Links' sheet and keep only PDF rows."""
    df = pd.read_excel(EXCEL_PATH, sheet_name="Links")
    df = df[df["bank"].notna()]
    df = df[df["link_type"].isin(PDF_LINK_TYPES)]
    return df


def detect_language(url):
    if not url:
        return ""
    match = re.search(r'/(fr|nl)(/|-|$)', str(url))
    return match.group(1) if match else ""


def make_source_key(bank_name, suffix):
    slug = re.sub(r'[^a-z0-9]+', '_', bank_name.lower()).strip('_')
    return f"{slug}_{suffix}"


def build_pdf_list(df: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for _, row in df.iterrows():
        bank = row["bank"]
        suffix, product_type = PDF_LINK_TYPES[row["link_type"]]
        url = str(row["url"]).strip().strip('"')
        parent = str(row.get("parent_url", "")).strip().strip('"') if pd.notna(row.get("parent_url")) else ""
        lang = detect_language(url) or detect_language(parent)

        rows.append({
            "source_key": make_source_key(bank, suffix),
            "bank": bank,
            "url": url,
            "product_type": product_type,
            "language": lang,
            "pdf_source": parent,
        })

    return pd.DataFrame(rows)





#
## PDF SCRAPPER
#


def checksum(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def is_pdf_url(url: str) -> bool:
    """Check if a string looks like a URL pointing to a PDF."""
    return url.startswith("http") and url.lower().endswith(".pdf")


def is_webpage_url(url: str) -> bool:
    """A URL that is valid but not a PDF."""
    return url.startswith("http") and not url.lower().endswith(".pdf")


def load_links(csv_path: Path) -> list[dict]:
    with open(csv_path, newline="") as f:
        return list(csv.DictReader(f))


def download_and_check(rows: list[dict], latest_dir: Path = LATEST_DIR, archive_dir: Path = ARCHIVE_DIR) -> dict:
    """Download PDFs, checksum, compare to last version. Returns report dict."""
    latest_dir.mkdir(parents=True, exist_ok=True)
    archive_dir.mkdir(parents=True, exist_ok=True)

    results = []
    counts = {"unchanged": 0, "updated": 0, "not_a_link": 0, "webpage": 0, "dead_link": 0, "error": 0}

    for row in rows:
        url = row.get("url", "").strip()
        entry = {**row}

        # Not a URL at all
        if not url.startswith("http"):
            entry["status"] = "not_a_link"
            entry["detail"] = f"Value is not a URL: {url}"
            counts["not_a_link"] += 1
            results.append(entry)
            continue

        # URL but not a PDF (webpage)
        if is_webpage_url(url):
            entry["status"] = "webpage"
            entry["detail"] = "URL points to a webpage, not a PDF"
            counts["webpage"] += 1
            results.append(entry)
            continue

        # It's a PDF URL — try to download
        filename = url.split("/")[-1]
        filepath = latest_dir / filename

        try:
            response = requests.get(url, timeout=30)
            if response.status_code != 200:
                entry["status"] = "dead_link"
                entry["detail"] = f"HTTP {response.status_code}"
                counts["dead_link"] += 1
                results.append(entry)
                continue

            # Check content-type as a sanity check
            content_type = response.headers.get("content-type", "")
            if "pdf" not in content_type and "octet-stream" not in content_type:
                entry["status"] = "dead_link"
                entry["detail"] = f"Expected PDF but got content-type: {content_type}"
                counts["dead_link"] += 1
                results.append(entry)
                continue

            new_hash = checksum(response.content)

            if filepath.exists():
                old_hash = checksum(filepath.read_bytes())
                if new_hash == old_hash:
                    counts["unchanged"] += 1
                    continue

                # Archive the old version before overwriting
                filepath.rename(archive_dir / f"{today}_{filename}")

            # New or changed — save it
            filepath.write_bytes(response.content)
            entry["status"] = "updated"
            entry["detail"] = "New PDF" if not (archive_dir / f"{today}_{filename}").exists() else "Content changed"
            counts["updated"] += 1

        except requests.RequestException as e:
            entry["status"] = "error"
            entry["detail"] = str(e)
            counts["error"] += 1

        results.append(entry)

    return {
        "date": today,
        "summary": counts,
        "entries": results,
    }


def main():
    ## Read Excel, back it up, 
    backup_excel()
    df = load_pdf_links()
    pdf_df = build_pdf_list(df)
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    pdf_df.to_csv(OUTPUT_PATH, index=False)
    print(pdf_df.to_string())
    print(f"\nWrote {len(pdf_df)} rows to {OUTPUT_PATH}")
    
    
    print("Starting pdf...")
    rows = load_links(CSV_PATH)
    print("Downloading and checking pdf files...")
    report = download_and_check(rows)

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    output_path = OUTPUT_DIR / f"{today}_pdf_diff.json"
    output_path.write_text(json.dumps(report, indent=2, ensure_ascii=False))

    s = report["summary"]
    print(f"Updated: {s['updated']}  Unchanged: {s['unchanged']}  "
          f"Dead links: {s['dead_link']}  Webpages: {s['webpage']}  "
          f"Not a link: {s['not_a_link']}  Errors: {s['error']}")
    print(f"Report saved to {output_path}")


if __name__ == "__main__":
    main()
