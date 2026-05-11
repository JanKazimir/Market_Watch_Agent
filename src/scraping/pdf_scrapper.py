## Module that downloads PDFs, checksums them, and keeps only new versions
# Expects an existing {today}_pdf_list.csv file to work from

import csv
import hashlib
import json
import requests
import datetime

from pathlib import Path


# Should have a way to update the pdf link by looking at the new url from the url scraping diff.

#
## CONFIG
#

today = datetime.date.today().isoformat()
BASE_DIR = Path(__file__).parent.parent.parent
DATA_DIR = BASE_DIR / "data"
CSV_PATH = DATA_DIR / "pdf_links_csv" / f"{today}_pdf_list.csv"
LATEST_DIR = DATA_DIR / "pdfs" / "latest"
ARCHIVE_DIR = DATA_DIR / "pdfs" / "archive"
OUTPUT_DIR = DATA_DIR / "outputs"




HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "application/pdf,*/*",
}


def checksum(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()



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

        # Try to download — content-type check below decides if it's really a PDF
        filename = url.split("/")[-1].split("?")[0]
        if not filename.lower().endswith(".pdf"):
            filename += ".pdf"
        filepath = latest_dir / filename

        try:
            response = requests.get(url, timeout=30, headers=HEADERS)
            if response.status_code != 200:
                entry["status"] = "dead_link"
                entry["detail"] = f"HTTP {response.status_code}"
                counts["dead_link"] += 1
                results.append(entry)
                continue

            # Check content-type as a sanity check
            content_type = response.headers.get("content-type", "")
            if "pdf" not in content_type and "octet-stream" not in content_type:
                entry["status"] = "webpage"
                entry["detail"] = f"Not a PDF (content-type: {content_type})"
                counts["webpage"] += 1
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
