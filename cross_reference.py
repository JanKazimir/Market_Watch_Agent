"""
Compares product data across different sources on the SAME day
to find discrepancies.

This is different from diff.py which compares the SAME source across days.

"""

import json
import sys
import re
import datetime
from pathlib import Path

SNAPSHOT_DIR = Path("snapshots")

# Minimum rate difference to flag (in percentage points)
# flag anything above 0.01% difference
RATE_THRESHOLD = 0.01

# Fields to cross-reference
RATE_FIELDS = ["base_rate", "fidelity_premium", "total_rate"]
CONDITION_FIELDS = ["min_deposit", "max_deposit", "account_type"]


# ---------------------------------------------------------------------------
# PRODUCT MATCHING
# ---------------------------------------------------------------------------

# Common words that sources add/remove inconsistently
NOISE_WORDS = [
    "spaarrekening",
    "savings",
    "account",
    "compte",
    "épargne",
    "rekening",
    "sparen",
    "spaar",
]


def normalize_name(name: str) -> str:
    """Normalize a product name for matching.

    Strips numbering, hyphens, extra spaces, and common variations.
    """
    if not name:
        return ""

    # Lowercase
    result = name.lower().strip()

    # Remove leading numbering like "1. " or "2. "
    result = re.sub(r"^\d+\.\s*", "", result)

    # Remove hyphens and extra spaces
    result = result.replace("-", " ").replace("  ", " ").strip()

    return result


def strip_noise(name: str) -> str:
    """Remove common noise words to get the product name.
    'kbc start2save spaarrekening' → 'kbc start2save'
    """
    words = name.split()
    core = [w for w in words if w not in NOISE_WORDS]
    return " ".join(core).strip()


def normalize_bank(bank: str) -> str:
    """Normalize bank name for matching across sources."""
    if not bank:
        return ""
    return bank.lower().strip()


def make_match_key(product: dict) -> str:
    """Create a normalized key for matching across sources."""
    bank = normalize_bank(product.get("bank", ""))
    name = normalize_name(product.get("product_name", ""))
    return f"{bank}|{name}"


def make_fuzzy_key(product: dict) -> str:
    """Create a fuzzy key with noise words stripped."""
    bank = normalize_bank(product.get("bank", ""))
    name = normalize_name(product.get("product_name", ""))
    core = strip_noise(name)
    # Also strip the bank name from the product name if it's there
    # e.g. "kbc start2save" → "start2save" (bank is already in the key)
    core_words = core.split()
    if core_words and core_words[0] == bank:
        core = " ".join(core_words[1:])
    return f"{bank}|{core}"


def build_master_lookup(snapshots: list[dict]) -> dict:
    """Build a master lookup matching products across sources.

    Uses two-pass matching:
    1. Exact normalized name match
    2. Fuzzy match (noise words stripped) for unmatched products
    """
    # Pass 1: Group by exact normalized key
    exact_master = {}
    for snapshot in snapshots:
        source = snapshot.get("source", "unknown")
        for product in snapshot.get("products", []):
            key = make_match_key(product)
            if key not in exact_master:
                exact_master[key] = {}
            exact_master[key][source] = product

    # Find products that only appear in one source (unmatched)
    matched_keys = {k for k, v in exact_master.items() if len(v) >= 2}
    unmatched = {}  # fuzzy_key → list of (source, product, exact_key)

    for key, sources_data in exact_master.items():
        if len(sources_data) < 2:
            for source, product in sources_data.items():
                fkey = make_fuzzy_key(product)
                if fkey not in unmatched:
                    unmatched[fkey] = []
                unmatched[fkey].append((source, product, key))

    # Pass 2: Fuzzy match unmatched products
    master = dict(exact_master)  # Start with exact matches

    for fkey, entries in unmatched.items():
        if len(entries) >= 2:
            # Multiple sources have this fuzzy key → they're the same product
            # Merge them under one key
            merge_key = entries[0][2]  # Use first product's exact key

            # Remove old separate entries
            for source, product, old_key in entries:
                if old_key in master and len(master[old_key]) == 1:
                    del master[old_key]

            # Create merged entry
            if merge_key not in master:
                master[merge_key] = {}
            for source, product, old_key in entries:
                master[merge_key][source] = product

    return master


def find_product_snapshots(date: str, snapshot_dir: Path = SNAPSHOT_DIR) -> list[Path]:
    """Find all product snapshot files for a given date."""
    if not snapshot_dir.exists():
        return []

    date_pattern = re.compile(r"^(.+)_(\d{4}-\d{2}-\d{2})\.json$")
    files = []

    for f in sorted(snapshot_dir.glob("*.json")):
        # Skip diff, classified, report, and discrepancy files
        if f.name.startswith(("diff_", "classified_", "report_", "discrepancies_")):
            continue

        match = date_pattern.match(f.name)
        if not match:
            continue

        file_date = match.group(2)
        if file_date != date:
            continue

        # Check if it's a product file (not news)
        try:
            data = json.loads(f.read_text(encoding="utf-8"))
            if "products" in data:
                files.append(f)
        except (json.JSONDecodeError, IOError):
            continue

    return files


# ---------------------------------------------------------------------------
# CROSS-REFERENCE LOGIC
# ---------------------------------------------------------------------------


def cross_reference(snapshots: list[dict]) -> dict:
    """Compare products across multiple sources and find discrepancies."""
    if len(snapshots) < 2:
        return {
            "error": "Need at least 2 product sources to cross-reference",
            "num_sources": len(snapshots),
        }

    # Build a master lookup: match_key - { source: product_data }
    master = build_master_lookup(snapshots)

    # Find products that appear in multiple sources
    discrepancies = []
    matches_checked = 0

    for key, sources_data in master.items():
        if len(sources_data) < 2:
            # Only in one source - can't cross-reference
            continue

        matches_checked += 1
        source_names = list(sources_data.keys())

        # Compare every pair of sources
        for i in range(len(source_names)):
            for j in range(i + 1, len(source_names)):
                source_a = source_names[i]
                source_b = source_names[j]
                product_a = sources_data[source_a]
                product_b = sources_data[source_b]

                # Check rate fields
                for field in RATE_FIELDS:
                    val_a = product_a.get(field)
                    val_b = product_b.get(field)

                    # Skip if either is None
                    if val_a is None or val_b is None:
                        continue

                    # Convert to float for comparison
                    try:
                        float_a = float(val_a)
                        float_b = float(val_b)
                    except (ValueError, TypeError):
                        continue

                    diff = abs(float_a - float_b)
                    if diff >= RATE_THRESHOLD:
                        discrepancies.append(
                            {
                                "type": "rate_discrepancy",
                                "bank": product_a.get("bank", ""),
                                "product_name": product_a.get("product_name", ""),
                                "field": field,
                                "sources": {
                                    source_a: float_a,
                                    source_b: float_b,
                                },
                                "difference": round(diff, 4),
                                "product_name_per_source": {
                                    source_a: product_a.get("product_name", ""),
                                    source_b: product_b.get("product_name", ""),
                                },
                            }
                        )

                # Check condition fields
                for field in CONDITION_FIELDS:
                    val_a = product_a.get(field)
                    val_b = product_b.get(field)

                    # Skip if both are None
                    if val_a is None and val_b is None:
                        continue

                    if val_a != val_b:
                        discrepancies.append(
                            {
                                "type": "condition_discrepancy",
                                "bank": product_a.get("bank", ""),
                                "product_name": product_a.get("product_name", ""),
                                "field": field,
                                "sources": {
                                    source_a: val_a,
                                    source_b: val_b,
                                },
                                "product_name_per_source": {
                                    source_a: product_a.get("product_name", ""),
                                    source_b: product_b.get("product_name", ""),
                                },
                            }
                        )

    # Sort: rate discrepancies first, then by difference (largest first)
    discrepancies.sort(
        key=lambda d: (
            0 if d["type"] == "rate_discrepancy" else 1,
            -d.get("difference", 0),
        )
    )

    # Build report
    today = datetime.date.today().isoformat()
    now = datetime.datetime.now().isoformat(timespec="seconds")

    report = {
        "date": snapshots[0].get("date", today),
        "generated_at": now,
        "num_sources": len(snapshots),
        "sources": [s.get("source", "unknown") for s in snapshots],
        "products_matched_across_sources": matches_checked,
        "total_discrepancies": len(discrepancies),
        "rate_discrepancies": len(
            [d for d in discrepancies if d["type"] == "rate_discrepancy"]
        ),
        "condition_discrepancies": len(
            [d for d in discrepancies if d["type"] == "condition_discrepancy"]
        ),
        "discrepancies": discrepancies,
    }

    return report


# ---------------------------------------------------------------------------
# SAVE & PRINT
# ---------------------------------------------------------------------------


def save_report(report: dict) -> Path:
    """Save cross-reference report to snapshots folder."""
    SNAPSHOT_DIR.mkdir(exist_ok=True)
    date = report.get("date", datetime.date.today().isoformat())
    filepath = SNAPSHOT_DIR / f"discrepancies_{date}.json"
    filepath.write_text(
        json.dumps(report, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    print(f"\n[XREF] Saved report → {filepath}")
    return filepath


def print_report(report: dict):
    """Print a human-readable summary."""
    print(f"\n{'='*60}")
    print(f"  CROSS-REFERENCE REPORT: {report['date']}")
    print(f"{'='*60}")
    print(f"  Sources compared:    {', '.join(report['sources'])}")
    print(f"  Products matched:    {report['products_matched_across_sources']}")
    print(f"  Total discrepancies: {report['total_discrepancies']}")
    print(f"    Rate mismatches:   {report['rate_discrepancies']}")
    print(f"    Condition mismatches: {report['condition_discrepancies']}")

    if report["total_discrepancies"] == 0:
        print(f"\n  All sources agree. No discrepancies found.")
        return

    print(f"\n--- Discrepancies ---\n")
    for i, d in enumerate(report["discrepancies"], 1):
        bank = d["bank"]
        product = d["product_name"]
        field = d["field"]
        sources = d["sources"]

        if d["type"] == "rate_discrepancy":
            diff = d["difference"]
            print(f"  [{i}] {bank} — {product}")
            print(f"      Field: {field}")
            for src, val in sources.items():
                print(f"      {src}: {val}%")
            print(f"      Difference: {diff} percentage points")

            # Flag if names differ across sources
            names = d.get("product_name_per_source", {})
            unique_names = set(names.values())
            if len(unique_names) > 1:
                print(f"      Note: Name varies across sources:")
                for src, name in names.items():
                    print(f'        {src}: "{name}"')

        elif d["type"] == "condition_discrepancy":
            print(f"  [{i}] {bank} — {product}")
            print(f"      Field: {field}")
            for src, val in sources.items():
                print(f"      {src}: {val}")

        print()


# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------


def main():
    if len(sys.argv) == 1:
        # Auto mode: find today's product snapshots
        today = datetime.date.today().isoformat()
        print(f"[XREF] Looking for product snapshots for {today}...")
        files = find_product_snapshots(today)

        if len(files) < 2:
            # Try yesterday
            yesterday = (datetime.date.today() - datetime.timedelta(days=1)).isoformat()
            print(f"[XREF] Not enough for today. Trying {yesterday}...")
            files = find_product_snapshots(yesterday)

        if len(files) < 2:
            print(f"[XREF] Need at least 2 product snapshots from the same day.")
            print(
                f"       Found {len(files)} file(s). Add more sources to cross-reference."
            )
            sys.exit(1)

        print(f"[XREF] Found {len(files)} product sources:")
        for f in files:
            print(f"       {f.name}")

        snapshots = [json.loads(f.read_text(encoding="utf-8")) for f in files]

    elif len(sys.argv) == 2 and re.match(r"\d{4}-\d{2}-\d{2}", sys.argv[1]):
        # Date mode: find snapshots for a specific date
        date = sys.argv[1]
        print(f"[XREF] Looking for product snapshots for {date}...")
        files = find_product_snapshots(date)

        if len(files) < 2:
            print(f"[XREF] Need at least 2 product snapshots for {date}.")
            print(f"       Found {len(files)} file(s).")
            sys.exit(1)

        print(f"[XREF] Found {len(files)} product sources:")
        for f in files:
            print(f"       {f.name}")

        snapshots = [json.loads(f.read_text(encoding="utf-8")) for f in files]

    else:
        # Manual mode: specific files
        files = sys.argv[1:]
        if len(files) < 2:
            print("Usage:")
            print(
                "  python cross_reference.py                        (auto-find today's snapshots)"
            )
            print("  python cross_reference.py 2026-05-05             (specific date)")
            print("  python cross_reference.py file1.json file2.json  (specific files)")
            sys.exit(1)

        snapshots = []
        for f in files:
            path = Path(f)
            if not path.exists():
                print(f"ERROR: File not found: {f}")
                sys.exit(1)
            snapshots.append(json.loads(path.read_text(encoding="utf-8")))

    # Run cross-reference
    report = cross_reference(snapshots)
    save_report(report)
    print_report(report)
    return report


if __name__ == "__main__":
    main()
