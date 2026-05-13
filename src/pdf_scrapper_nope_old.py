## Module that downloads PDFs, checksums them, and keeps only new versions

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
DATA_DIR = Path(__file__).parent.parent / "data"
CSV_PATH = DATA_DIR / "pdf_links_csv" / f"{today}_pdf_list.csv"
LATEST_DIR = DATA_DIR / "pdfs" / "latest"
ARCHIVE_DIR = DATA_DIR / "pdfs" / "archive"
OUTPUT_DIR = DATA_DIR / "outputs"


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
