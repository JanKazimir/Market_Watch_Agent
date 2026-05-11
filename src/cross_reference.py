"""
Cross-Reference Module
=======================
Compares product data across DIFFERENT data providers on the SAME day.

This is different from diff.py which compares the SAME source across days.

Smart comparison rules:
  - Aggregator vs direct bank scraper (Bankshopper vs KBC website)
  - Aggregator vs aggregator (Bankshopper vs Spaargids)
  - Skips same-bank comparisons (different pages from same bank)
  - Skips different product types (regulated vs term)
  - Handles Dutch/French language differences
  - Focuses on rate discrepancies (what ING cares about)
  - Condition discrepancies only for numeric values (avoids language false positives)
"""

import json
import sys
import re
import datetime
from pathlib import Path

SNAPSHOT_DIR = Path("data/snapshots")
RATE_THRESHOLD = 0.01
RATE_FIELDS = ["base_rate", "fidelity_premium", "total_rate"]

# Language normalization for product names
NAME_TRANSLATIONS = {
    "spaarrekening": "savings",
    "compte d'épargne": "savings",
    "épargne": "savings",
    "sparen": "savings",
    "spaar": "savings",
    "rekening": "account",
    "compte": "account",
    "termijn": "term",
    "terme": "term",
    "getrouwheid": "fidelity",
    "fidélité": "fidelity",
    "groei": "growth",
    "croissance": "growth",
    "klassiek": "classic",
    "classique": "classic",
}

AGGREGATOR_PREFIXES = ["bankshopper", "spaargids", "guide_epargne", "mes_finances"]


def normalize_product_name(name):
    if not name:
        return ""
    result = (
        re.sub(r"^\d+\.\s*", "", name.lower().strip())
        .replace("-", " ")
        .replace("  ", " ")
        .strip()
    )
    for original, replacement in NAME_TRANSLATIONS.items():
        result = result.replace(original, replacement)
    noise = ["savings", "account", "de", "d'", "le", "la", "het", "van", "du"]
    return " ".join(w for w in result.split() if w not in noise).strip()


def normalize_bank(bank):
    if not bank:
        return ""
    result = bank.lower().strip()
    for old, new in [
        ("bnp paribas fortis", "bnp"),
        ("banque triodos", "triodos"),
        ("triodos bank", "triodos"),
    ]:
        result = result.replace(old, new)
    return result


def get_provider(source):
    s = source.lower()
    for prefix in AGGREGATOR_PREFIXES:
        if prefix in s:
            return f"aggregator:{prefix}"
    bank = re.sub(r"^pw_", "", s)
    bank = re.sub(
        r"_(regulated_savings|saving_accounts|savings|term_accounts|term|news|product|regulated).*$",
        "",
        bank,
    )
    return f"direct:{bank}"


def should_compare(source_a, source_b):
    pa, pb = get_provider(source_a), get_provider(source_b)
    if pa == pb:
        return False
    if pa.startswith("direct:") and pb.startswith("direct:"):
        return False
    ta = "unregulated" if "term" in source_a.lower() else "regulated"
    tb = "unregulated" if "term" in source_b.lower() else "regulated"
    if ta != tb:
        return False
    return True


def make_match_key(product):
    bank = normalize_bank(product.get("bank", ""))
    name = normalize_product_name(product.get("product_name", ""))
    bank_words = bank.split()
    name_words = name.split()
    while name_words and bank_words and name_words[0] in bank_words:
        name_words.pop(0)
    return f"{bank}|{' '.join(name_words).strip()}"


def cross_reference(snapshots):
    if len(snapshots) < 2:
        return {"error": "Need at least 2 sources", "num_sources": len(snapshots)}

    # Build master lookup
    groups = {}
    for snap in snapshots:
        source = snap.get("source", "unknown")
        for product in snap.get("products", []):
            key = make_match_key(product)
            if key not in groups:
                groups[key] = {}
            groups[key][source] = product

    discrepancies, matches_checked, pairs_skipped = [], 0, 0

    for key, sources_data in groups.items():
        if len(sources_data) < 2:
            continue
        source_names = list(sources_data.keys())

        for i in range(len(source_names)):
            for j in range(i + 1, len(source_names)):
                sa, sb = source_names[i], source_names[j]
                if not should_compare(sa, sb):
                    pairs_skipped += 1
                    continue

                matches_checked += 1
                pa, pb = sources_data[sa], sources_data[sb]

                for field in RATE_FIELDS:
                    va, vb = pa.get(field), pb.get(field)
                    if va is None or vb is None:
                        continue
                    try:
                        fa, fb = float(va), float(vb)
                    except (ValueError, TypeError):
                        continue
                    diff = abs(fa - fb)
                    if diff >= RATE_THRESHOLD:
                        discrepancies.append(
                            {
                                "type": "rate_discrepancy",
                                "bank": pa.get("bank", ""),
                                "product_name": pa.get("product_name", ""),
                                "field": field,
                                "sources": {sa: fa, sb: fb},
                                "difference": round(diff, 4),
                                "product_name_per_source": {
                                    sa: pa.get("product_name", ""),
                                    sb: pb.get("product_name", ""),
                                },
                                "provider_a": get_provider(sa),
                                "provider_b": get_provider(sb),
                            }
                        )

                for field in ["min_deposit", "max_deposit"]:
                    va, vb = pa.get(field), pb.get(field)
                    if va is None and vb is None:
                        continue
                    try:
                        na = float(re.sub(r"[^\d.]", "", str(va))) if va else None
                        nb = float(re.sub(r"[^\d.]", "", str(vb))) if vb else None
                    except (ValueError, TypeError):
                        na, nb = None, None
                    if na is not None and nb is not None and na != nb:
                        discrepancies.append(
                            {
                                "type": "condition_discrepancy",
                                "bank": pa.get("bank", ""),
                                "product_name": pa.get("product_name", ""),
                                "field": field,
                                "sources": {sa: va, sb: vb},
                                "product_name_per_source": {
                                    sa: pa.get("product_name", ""),
                                    sb: pb.get("product_name", ""),
                                },
                                "provider_a": get_provider(sa),
                                "provider_b": get_provider(sb),
                            }
                        )

    discrepancies.sort(
        key=lambda d: (
            0 if d["type"] == "rate_discrepancy" else 1,
            -d.get("difference", 0),
        )
    )

    return {
        "date": snapshots[0].get("date", datetime.date.today().isoformat()),
        "generated_at": datetime.datetime.now().isoformat(timespec="seconds"),
        "num_sources": len(snapshots),
        "sources": [s.get("source", "unknown") for s in snapshots],
        "products_matched_across_sources": matches_checked,
        "pairs_skipped_same_provider": pairs_skipped,
        "total_discrepancies": len(discrepancies),
        "rate_discrepancies": len(
            [d for d in discrepancies if d["type"] == "rate_discrepancy"]
        ),
        "condition_discrepancies": len(
            [d for d in discrepancies if d["type"] == "condition_discrepancy"]
        ),
        "discrepancies": discrepancies,
    }


def save_report(report):
    SNAPSHOT_DIR.mkdir(exist_ok=True)
    fp = (
        SNAPSHOT_DIR
        / f"discrepancies_{report.get('date', datetime.date.today().isoformat())}.json"
    )
    fp.write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"\n[XREF] Saved report → {fp}")
    return fp


def print_report(report):
    print(f"\n{'='*60}")
    print(f"  CROSS-REFERENCE REPORT: {report['date']}")
    print(f"{'='*60}")
    print(f"  Sources compared:      {len(report['sources'])}")
    print(f"  Products matched:      {report['products_matched_across_sources']}")
    print(f"  Pairs skipped (same):  {report.get('pairs_skipped_same_provider', 0)}")
    print(f"  Total discrepancies:   {report['total_discrepancies']}")
    print(f"    Rate mismatches:     {report['rate_discrepancies']}")
    print(f"    Condition mismatches: {report['condition_discrepancies']}")

    if report["total_discrepancies"] == 0:
        print(f"\n  All sources agree. No discrepancies found.")
        return

    print(f"\n--- Discrepancies ---\n")
    for i, d in enumerate(report["discrepancies"], 1):
        print(f"  [{i}] {d['bank']} — {d['product_name']}")
        print(f"      Field: {d['field']}")
        for src, val in d["sources"].items():
            print(
                f"      {src} ({get_provider(src)}): {val}{'%' if d['type'] == 'rate_discrepancy' else ''}"
            )
        if d.get("difference"):
            print(f"      Difference: {d['difference']} percentage points")
        names = d.get("product_name_per_source", {})
        if len(set(names.values())) > 1:
            print(f"      Note: Name varies:")
            for src, name in names.items():
                print(f'        {src}: "{name}"')
        print()


def find_product_snapshots(date, snapshot_dir=SNAPSHOT_DIR):
    if not snapshot_dir.exists():
        return []
    pattern = re.compile(r"^(.+)_(\d{4}-\d{2}-\d{2})\.json$")
    files = []
    for f in sorted(snapshot_dir.glob("*.json")):
        if f.name.startswith(("diff_", "classified_", "report_", "discrepancies_")):
            continue
        if "_pdf_" in f.name or f.name.endswith("_pdf.json"):
            continue
        m = pattern.match(f.name)
        if not m or m.group(2) != date:
            continue
        try:
            data = json.loads(f.read_text(encoding="utf-8"))
            if "products" in data:
                files.append(f)
        except (json.JSONDecodeError, IOError):
            continue
    return files


def main():
    if len(sys.argv) == 1:
        today = datetime.date.today().isoformat()
        print(f"[XREF] Looking for product snapshots for {today}...")
        files = find_product_snapshots(today)
        if len(files) < 2:
            yesterday = (datetime.date.today() - datetime.timedelta(days=1)).isoformat()
            print(f"[XREF] Not enough for today. Trying {yesterday}...")
            files = find_product_snapshots(yesterday)
        if len(files) < 2:
            print(f"[XREF] Need at least 2 product snapshots from the same day.")
            sys.exit(1)
    elif len(sys.argv) == 2 and re.match(r"\d{4}-\d{2}-\d{2}", sys.argv[1]):
        date = sys.argv[1]
        print(f"[XREF] Looking for product snapshots for {date}...")
        files = find_product_snapshots(date)
        if len(files) < 2:
            print(f"[XREF] Need at least 2 product snapshots for {date}.")
            sys.exit(1)
    else:
        files = sys.argv[1:]
        if len(files) < 2:
            print("Usage: python cross_reference.py [date] [file1.json file2.json]")
            sys.exit(1)
        snapshots = [json.loads(Path(f).read_text(encoding="utf-8")) for f in files]
        report = cross_reference(snapshots)
        save_report(report)
        print_report(report)
        return

    print(f"[XREF] Found {len(files)} product sources")
    snapshots = [json.loads(f.read_text(encoding="utf-8")) for f in files]
    report = cross_reference(snapshots)
    save_report(report)
    print_report(report)


if __name__ == "__main__":
    main()
