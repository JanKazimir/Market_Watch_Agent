"""
Unified Diff Module
====================
Compares two consecutive daily snapshots and detects meaningful changes.
Handles ANY source type automatically:
  - Product data (Bankshopper, KBC, etc.) → rate changes, condition changes, new/removed products
  - News articles (l'Echo RSS, etc.) → new/removed articles

Usage:
    python diff.py snapshots/bankshopper_2026-05-04.json snapshots/bankshopper_2026-05-05.json
    python diff.py snapshots/lecho_entreprises_banques_2026-05-04.json snapshots/lecho_entreprises_banques_2026-05-05.json

Output:
    snapshots/diff_SOURCE_OLDDATE_to_NEWDATE.json
"""

import json
import sys
import datetime
from pathlib import Path


SNAPSHOT_DIR = Path("data/snapshots")


# ---------------------------------------------------------------------------
# LOADING & DETECTION
# ---------------------------------------------------------------------------

def load_snapshot(filepath: str) -> dict:
    """Load a snapshot JSON file."""
    path = Path(filepath)
    if not path.exists():
        print(f"ERROR: File not found: {filepath}")
        sys.exit(1)
    return json.loads(path.read_text(encoding="utf-8"))


def detect_data_type(snapshot: dict) -> str:
    """Detect whether snapshot contains products or news articles."""
    if "products" in snapshot:
        return "products"
    elif "articles" in snapshot:
        return "news"
    else:
        raise ValueError(
            f"Unknown snapshot format from source '{snapshot.get('source', '?')}'. "
            f"Expected 'products' or 'articles' key in JSON."
        )


def get_source_prefix(snapshot: dict) -> str:
    """Extract a short source prefix for filenames.
    e.g. 'bankshopper.be' → 'bankshopper', 'lecho.be' → 'lecho'
    """
    source = snapshot.get("source", "unknown")
    # Remove .be, .com etc.
    prefix = source.split(".")[0]
    # If there's a feed name (e.g. lecho RSS), include it
    feed = snapshot.get("feed")
    if feed:
        prefix = f"{prefix}_{feed}"
    return prefix


# ---------------------------------------------------------------------------
# PRODUCT DIFF (Bankshopper, KBC, etc.)
# ---------------------------------------------------------------------------

# Fields that matter for rate comparisons
RATE_FIELDS = ["base_rate", "fidelity_premium", "total_rate"]

# Fields that matter for condition comparisons
CONDITION_FIELDS = ["min_deposit", "max_deposit", "account_type"]

# Credit rating fields
RATING_FIELDS = ["moody_rating", "sp_rating", "fitch_rating"]

# Other fields worth tracking
OTHER_FIELDS = [
    "deposit_guarantee",    # e.g. "€ 100.000,00"
    "guarantee_fund",       # e.g. "België"
    "open_online",          # e.g. "Ja" / "Neen"
    "management_costs",     # e.g. "Neen"
    "group",                # e.g. "ING Groep nv"
    "country_of_group",     # e.g. "Nederland"
    "product_sheet_url",    # link to official PDF — change may signal updated T&Cs
]

# Map each field to its change_type for the output
FIELD_TO_CHANGE_TYPE = {}
for f in RATE_FIELDS:
    FIELD_TO_CHANGE_TYPE[f] = "rate_change"
for f in CONDITION_FIELDS:
    FIELD_TO_CHANGE_TYPE[f] = "condition_change"
for f in RATING_FIELDS:
    FIELD_TO_CHANGE_TYPE[f] = "rating_change"
for f in OTHER_FIELDS:
    FIELD_TO_CHANGE_TYPE[f] = "other_change"

# All fields to compare
ALL_PRODUCT_FIELDS = RATE_FIELDS + CONDITION_FIELDS + RATING_FIELDS + OTHER_FIELDS


def make_product_key(product: dict) -> str:
    """Create a unique key for a product: bank + product_name."""
    bank = product.get("bank", "").strip()
    name = product.get("product_name", "").strip()
    return f"{bank}|{name}"


def diff_products(old_snapshot: dict, new_snapshot: dict) -> dict:
    """Compare two product snapshots. Returns the changes list and summary."""
    # Build lookup dictionaries: key → product
    old_products = {}
    for p in old_snapshot.get("products", []):
        key = make_product_key(p)
        old_products[key] = p

    new_products = {}
    for p in new_snapshot.get("products", []):
        key = make_product_key(p)
        new_products[key] = p

    changes = []
    # Count changes per category
    change_counts = {
        "rate_changes": 0,
        "condition_changes": 0,
        "rating_changes": 0,
        "other_changes": 0,
        "products_added": 0,
        "products_removed": 0,
    }

    # --- Check for changes and removals ---
    for key, old_p in old_products.items():
        if key not in new_products:
            # Product was removed
            changes.append({
                "change_type": "product_removed",
                "bank": old_p.get("bank", ""),
                "product_name": old_p.get("product_name", ""),
                "field": None,
                "before": {f: old_p.get(f) for f in ALL_PRODUCT_FIELDS if old_p.get(f) is not None},
                "after": None,
            })
            change_counts["products_removed"] += 1
            continue

        # Product exists in both — compare ALL tracked fields
        new_p = new_products[key]

        for field in ALL_PRODUCT_FIELDS:
            old_val = old_p.get(field)
            new_val = new_p.get(field)
            if old_val != new_val:
                change_type = FIELD_TO_CHANGE_TYPE[field]
                changes.append({
                    "change_type": change_type,
                    "bank": old_p.get("bank", ""),
                    "product_name": old_p.get("product_name", ""),
                    "field": field,
                    "before": old_val,
                    "after": new_val,
                })
                # Increment the right counter (e.g. "rate_change" → "rate_changes")
                count_key = change_type + "s"
                change_counts[count_key] += 1

    # --- Check for new products ---
    for key, new_p in new_products.items():
        if key not in old_products:
            changes.append({
                "change_type": "new_product",
                "bank": new_p.get("bank", ""),
                "product_name": new_p.get("product_name", ""),
                "field": None,
                "before": None,
                "after": {f: new_p.get(f) for f in ALL_PRODUCT_FIELDS if new_p.get(f) is not None},
            })
            change_counts["products_added"] += 1

    return changes, change_counts


# ---------------------------------------------------------------------------
# NEWS DIFF (l'Echo RSS, bank newsrooms, etc.)
# ---------------------------------------------------------------------------

def diff_news(old_snapshot: dict, new_snapshot: dict) -> dict:
    """Compare two news snapshots. Returns the changes list and summary."""
    # Build lookup dictionaries: guid → article
    old_articles = {}
    for a in old_snapshot.get("articles", []):
        guid = a.get("guid", a.get("link", ""))
        old_articles[guid] = a

    new_articles = {}
    for a in new_snapshot.get("articles", []):
        guid = a.get("guid", a.get("link", ""))
        new_articles[guid] = a

    changes = []
    new_count = 0
    updated_count = 0

    # New articles (in today but not yesterday)
    for guid, article in new_articles.items():
        if guid not in old_articles:
            changes.append({
                "change_type": "new_article",
                "title": article.get("title", ""),
                "description": article.get("description", ""),
                "link": article.get("link", ""),
                "pub_date": article.get("pub_date", ""),
                "guid": guid,
            })
            new_count += 1
        else:
            # Article exists in both — check if title or description changed
            old_a = old_articles[guid]
            for field in ["title", "description"]:
                old_val = old_a.get(field, "")
                new_val = article.get(field, "")
                if old_val != new_val:
                    changes.append({
                        "change_type": "article_updated",
                        "field": field,
                        "before": old_val,
                        "after": new_val,
                        "title": article.get("title", ""),
                        "link": article.get("link", ""),
                        "guid": guid,
                    })
                    updated_count += 1

    # Removed articles — skipped (just RSS feed rotation, not useful)

    summary = {
        "new_articles": new_count,
        "articles_updated": updated_count,
    }

    return changes, summary


# ---------------------------------------------------------------------------
# UNIFIED DIFF RUNNER
# ---------------------------------------------------------------------------

def run_diff(old_snapshot: dict, new_snapshot: dict) -> dict:
    """Run the appropriate diff based on data type. Returns the full report."""

    # Validate: both must be from the same source
    old_source = old_snapshot.get("source", "")
    new_source = new_snapshot.get("source", "")
    if old_source != new_source:
        print(f"ERROR: Source mismatch! Old='{old_source}', New='{new_source}'")
        print("Both snapshots must be from the same source.")
        sys.exit(1)

    # Detect data type
    data_type = detect_data_type(new_snapshot)

    # Build the base report
    report = {
        "source": new_source,
        "data_type": data_type,
        "date_old": old_snapshot.get("date", ""),
        "date_new": new_snapshot.get("date", ""),
        "generated_at": datetime.datetime.now().isoformat(timespec="seconds"),
        "checksum_old": old_snapshot.get("checksum", ""),
        "checksum_new": new_snapshot.get("checksum", ""),
        "checksums_match": False,
        "total_changes": 0,
        "summary": {},
        "changes": [],
    }

    # Checksum shortcut: if identical, no changes
    if old_snapshot.get("checksum") == new_snapshot.get("checksum"):
        report["checksums_match"] = True
        if data_type == "products":
            report["summary"] = {
                "rate_changes": 0, "condition_changes": 0,
                "rating_changes": 0, "other_changes": 0,
                "products_added": 0, "products_removed": 0,
            }
        else:
            report["summary"] = {"new_articles": 0, "articles_updated": 0}
        return report

    # Run the appropriate diff
    if data_type == "products":
        changes, summary = diff_products(old_snapshot, new_snapshot)
    elif data_type == "news":
        changes, summary = diff_news(old_snapshot, new_snapshot)

    report["changes"] = changes
    report["summary"] = summary
    report["total_changes"] = len(changes)

    return report


# ---------------------------------------------------------------------------
# SAVE & PRINT
# ---------------------------------------------------------------------------

def save_diff_report(report: dict) -> Path:
    """Save the diff report to the snapshots folder."""
    SNAPSHOT_DIR.mkdir(exist_ok=True)

    source_prefix = report["source"].split(".")[0]
    old_date = report["date_old"]
    new_date = report["date_new"]

    filepath = SNAPSHOT_DIR / f"diff_{source_prefix}_{old_date}_to_{new_date}.json"
    filepath.write_text(
        json.dumps(report, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    print(f"[DIFF] Saved report → {filepath}")
    return filepath


def print_summary(report: dict):
    """Print a human-readable summary to stdout."""
    print(f"\n{'='*60}")
    print(f"  DIFF REPORT: {report['date_old']} → {report['date_new']}")
    print(f"{'='*60}")
    print(f"  Source:          {report['source']}")
    print(f"  Data type:       {report['data_type']}")
    print(f"  Checksums match: {report['checksums_match']}")
    print(f"  Total changes:   {report['total_changes']}")

    # Print summary based on type
    summary = report["summary"]
    if report["data_type"] == "products":
        print(f"    Rate changes:      {summary.get('rate_changes', 0)}")
        print(f"    Condition changes:  {summary.get('condition_changes', 0)}")
        print(f"    Rating changes:    {summary.get('rating_changes', 0)}")
        print(f"    Other changes:     {summary.get('other_changes', 0)}")
        print(f"    Products added:     {summary.get('products_added', 0)}")
        print(f"    Products removed:   {summary.get('products_removed', 0)}")
    elif report["data_type"] == "news":
        print(f"    New articles:      {summary.get('new_articles', 0)}")
        print(f"    Articles updated:  {summary.get('articles_updated', 0)}")

    if report["total_changes"] == 0:
        print(f"\n  No changes detected. Quiet day.")
        return

    # Print individual changes
    print(f"\n--- Changes ---\n")
    for i, change in enumerate(report["changes"], 1):
        ctype = change["change_type"]

        if ctype in ("rate_change", "condition_change", "rating_change", "other_change"):
            print(f"  [{i}] {change['bank']} — {change['product_name']}")
            print(f"      Type:   {ctype}")
            print(f"      Field:  {change['field']}")
            print(f"      Before: {change['before']}")
            print(f"      After:  {change['after']}")

        elif ctype == "new_product":
            print(f"  [{i}] {change['bank']} — {change['product_name']}")
            print(f"      Type: NEW PRODUCT")
            print(f"      Details: {change['after']}")

        elif ctype == "product_removed":
            print(f"  [{i}] {change['bank']} — {change['product_name']}")
            print(f"      Type: PRODUCT REMOVED")
            print(f"      Last known rates: {change['before']}")

        elif ctype == "new_article":
            print(f"  [{i}] NEW ARTICLE")
            print(f"      Title: {change['title']}")
            print(f"      {change['description'][:100]}...")
            print(f"      Link:  {change['link']}")

        elif ctype == "article_updated":
            print(f"  [{i}] ARTICLE UPDATED")
            print(f"      Title: {change['title']}")
            print(f"      Field: {change['field']}")
            print(f"      Before: {change['before'][:80]}...")
            print(f"      After:  {change['after'][:80]}...")

        print()


# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------

def find_snapshot_pairs(snapshot_dir: Path = SNAPSHOT_DIR) -> list[tuple[Path, Path]]:
    """Find pairs of consecutive snapshots from each source automatically.

    Groups snapshot files by source prefix, sorts by date,
    and returns the two most recent files for each source.
    """
    if not snapshot_dir.exists():
        return []

    # Find all snapshot JSON files (exclude diff_ and classified_ and report_ files)
    all_files = sorted(snapshot_dir.glob("*.json"))
    snapshot_files = [
        f for f in all_files
        if not f.name.startswith(("diff_", "classified_", "report_", "discrepancies_"))
    ]

    if not snapshot_files:
        return []

    # Group files by source prefix (everything before the date)
    # e.g. "bankshopper_2026-05-04.json" → prefix "bankshopper"
    # e.g. "lecho_entreprises_banques_2026-05-04.json" → prefix "lecho_entreprises_banques"
    import re
    date_pattern = re.compile(r"^(.+)_(\d{4}-\d{2}-\d{2})$")

    groups = {}
    for f in snapshot_files:
        name = f.stem  # filename without .json
        match = date_pattern.match(name)
        if not match:
            continue

        prefix = match.group(1)
        date_str = match.group(2)

        # Validate it's a real date
        try:
            datetime.date.fromisoformat(date_str)
        except ValueError:
            continue

        if prefix not in groups:
            groups[prefix] = []
        groups[prefix].append(f)

    # For each source, take the two most recent files
    pairs = []
    for prefix, files in groups.items():
        # Sort by filename (dates sort correctly in YYYY-MM-DD format)
        files.sort()
        if len(files) >= 2:
            pairs.append((files[-2], files[-1]))  # second-to-last, last

    return pairs


def merge_reports(reports: list[dict]) -> dict:
    """Merge multiple per-source diff reports into one combined daily report."""
    if not reports:
        return {}

    # Use the most recent date across all reports
    date_new = max(r["date_new"] for r in reports)
    date_old = min(r["date_old"] for r in reports)

    # Collect all changes, tagging each with its source
    all_changes = []
    for report in reports:
        source = report["source"]
        data_type = report["data_type"]
        for change in report["changes"]:
            # Add source info to each change so we know where it came from
            tagged_change = change.copy()
            tagged_change["source"] = source
            tagged_change["data_type"] = data_type
            all_changes.append(tagged_change)

    # Build combined summary
    combined_summary = {
        "rate_changes": 0,
        "condition_changes": 0,
        "rating_changes": 0,
        "other_changes": 0,
        "products_added": 0,
        "products_removed": 0,
        "new_articles": 0,
        "articles_updated": 0,
    }
    for report in reports:
        for key, value in report["summary"].items():
            if key in combined_summary:
                combined_summary[key] += value

    # Build the per-source breakdown
    sources = []
    for report in reports:
        sources.append({
            "source": report["source"],
            "data_type": report["data_type"],
            "date_old": report["date_old"],
            "date_new": report["date_new"],
            "checksums_match": report["checksums_match"],
            "total_changes": report["total_changes"],
            "summary": report["summary"],
        })

    combined = {
        "combined": True,
        "date_old": date_old,
        "date_new": date_new,
        "generated_at": datetime.datetime.now().isoformat(timespec="seconds"),
        "num_sources": len(reports),
        "sources": sources,
        "total_changes": len(all_changes),
        "summary": combined_summary,
        "changes": all_changes,
    }

    return combined


def save_combined_report(report: dict) -> Path:
    """Save the combined diff report."""
    SNAPSHOT_DIR.mkdir(exist_ok=True)
    date_new = report["date_new"]
    filepath = SNAPSHOT_DIR / f"diff_all_{date_new}.json"
    filepath.write_text(
        json.dumps(report, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    print(f"\n[DIFF] Saved combined report → {filepath}")
    return filepath


def print_combined_summary(report: dict):
    """Print a summary of the combined report."""
    print(f"\n{'='*60}")
    print(f"  COMBINED DAILY DIFF: {report['date_new']}")
    print(f"{'='*60}")
    print(f"  Sources processed:   {report['num_sources']}")
    print(f"  Total changes:       {report['total_changes']}")

    summary = report["summary"]
    # Only print categories that have changes
    product_changes = (
        summary["rate_changes"] + summary["condition_changes"] +
        summary["rating_changes"] + summary["other_changes"] +
        summary["products_added"] + summary["products_removed"]
    )
    news_changes = summary["new_articles"] + summary["articles_updated"]

    if product_changes > 0:
        print(f"\n  Product changes:")
        if summary["rate_changes"]:     print(f"    Rate changes:      {summary['rate_changes']}")
        if summary["condition_changes"]:print(f"    Condition changes:  {summary['condition_changes']}")
        if summary["rating_changes"]:   print(f"    Rating changes:    {summary['rating_changes']}")
        if summary["other_changes"]:    print(f"    Other changes:     {summary['other_changes']}")
        if summary["products_added"]:   print(f"    Products added:     {summary['products_added']}")
        if summary["products_removed"]: print(f"    Products removed:   {summary['products_removed']}")

    if news_changes > 0:
        print(f"\n  News changes:")
        if summary["new_articles"]:     print(f"    New articles:      {summary['new_articles']}")
        if summary["articles_updated"]: print(f"    Articles updated:  {summary['articles_updated']}")

    if report["total_changes"] == 0:
        print(f"\n  All quiet across all sources.")

    # Per-source breakdown
    print(f"\n  Per source:")
    for s in report["sources"]:
        status = "no changes" if s["checksums_match"] else f"{s['total_changes']} change(s)"
        print(f"    {s['source']} ({s['data_type']}): {status}")


def main():
    if len(sys.argv) == 3:
        # Manual mode: two files specified
        old_file = sys.argv[1]
        new_file = sys.argv[2]

        print(f"[DIFF] Loading old snapshot: {old_file}")
        old_snapshot = load_snapshot(old_file)

        print(f"[DIFF] Loading new snapshot: {new_file}")
        new_snapshot = load_snapshot(new_file)

        report = run_diff(old_snapshot, new_snapshot)
        save_diff_report(report)
        print_summary(report)
        return report

    elif len(sys.argv) == 1:
        # Auto mode: find snapshot pairs automatically
        print(f"[DIFF] Auto-discovery mode — scanning {SNAPSHOT_DIR}/ for snapshot pairs...")
        pairs = find_snapshot_pairs()

        if not pairs:
            print("[DIFF] No snapshot pairs found. Need at least 2 snapshots from the same source.")
            print(f"       Looking in: {SNAPSHOT_DIR}/")
            sys.exit(1)

        print(f"[DIFF] Found {len(pairs)} source(s) with snapshot pairs.\n")

        all_reports = []
        for old_file, new_file in pairs:
            print(f"{'—'*60}")
            print(f"[DIFF] Comparing: {old_file.name}  →  {new_file.name}")

            old_snapshot = load_snapshot(str(old_file))
            new_snapshot = load_snapshot(str(new_file))

            report = run_diff(old_snapshot, new_snapshot)
            save_diff_report(report)
            print_summary(report)
            all_reports.append(report)

        # Merge all reports into one combined daily diff
        if all_reports:
            combined = merge_reports(all_reports)
            save_combined_report(combined)
            print_combined_summary(combined)

        return all_reports

    else:
        print("Usage:")
        print("  python diff.py                          (auto-discover all snapshot pairs)")
        print("  python diff.py <old.json> <new.json>    (compare two specific files)")
        print()
        print("Examples:")
        print("  python diff.py")
        print("  python diff.py snapshots/bankshopper_2026-05-04.json snapshots/bankshopper_2026-05-05.json")
        sys.exit(1)


if __name__ == "__main__":
    main()
