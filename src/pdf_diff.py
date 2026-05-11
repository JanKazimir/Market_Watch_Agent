"""
PDF Diff Module
===============
Compares two PDFs and outputs the differences as JSON.
Handles both body text and tables (real tables from Word, not images).

Usage:
    from src.pdf_diff import diff_pdfs
    result = diff_pdfs("old.pdf", "new.pdf")

Output structure:
    {
        "old_file": "old.pdf",
        "new_file": "new.pdf",
        "text_diffs": [ { "context_before", "removed", "added", "context_after" } ],
        "table_diffs": [ { "table_index", "change_type", ... } ],
        "tables_added": [ ... ],
        "tables_removed": [ ... ],
    }
"""

import difflib
import json
import logging
import re
import sys
from pathlib import Path

import pdfplumber


# ---------------------------------------------------------------------------
# EXTRACTION
# ---------------------------------------------------------------------------

def _clean_line(line: str) -> str:
    """Normalise whitespace in a single line."""
    return re.sub(r"\s+", " ", line).strip()


def _is_noise(line: str, page_num: int, total_pages: int) -> bool:
    """Return True for lines that are likely headers/footers/page numbers."""
    stripped = line.strip()
    if not stripped:
        return True
    # Pure page numbers
    if re.fullmatch(r"\d{1,4}", stripped):
        return True
    # Common patterns: "Page 3 of 10", "3/10", "- 3 -"
    if re.fullmatch(r"(page\s*)?\d{1,4}\s*(of|/)\s*\d{1,4}", stripped, re.IGNORECASE):
        return True
    if re.fullmatch(r"-\s*\d{1,4}\s*-", stripped):
        return True
    return False


def extract_pdf_content(pdf_path: str) -> dict:
    """Extract text lines and tables from a PDF.

    Returns:
        {
            "text_lines": [str, ...],          # cleaned body text lines
            "tables": [[[str, ...], ...], ...], # list of tables, each a list of rows
        }
    """
    text_lines = []
    tables = []

    # Suppress noisy pdfminer warnings about malformed font metadata
    logging.getLogger("pdfminer").setLevel(logging.ERROR)

    with pdfplumber.open(pdf_path) as pdf:
        total_pages = len(pdf.pages)

        for page_num, page in enumerate(pdf.pages, start=1):
            # --- Tables ---
            page_tables = page.extract_tables() or []
            # Collect bounding boxes of tables so we can exclude that text
            table_bboxes = []
            for t in page.find_tables():
                table_bboxes.append(t.bbox)

            for raw_table in page_tables:
                cleaned_rows = []
                for row in raw_table:
                    cleaned_row = [
                        _clean_line(cell) if cell else ""
                        for cell in row
                    ]
                    cleaned_rows.append(cleaned_row)
                if cleaned_rows:
                    tables.append(cleaned_rows)

            # --- Text (outside tables) ---
            # Crop out table regions to avoid double-counting
            text_page = page
            for bbox in table_bboxes:
                # Filter chars outside this table bbox
                text_page = text_page.outside_bbox(bbox)

            raw_text = text_page.extract_text() or ""
            for line in raw_text.split("\n"):
                cleaned = _clean_line(line)
                if not _is_noise(cleaned, page_num, total_pages):
                    text_lines.append(cleaned)

    return {"text_lines": text_lines, "tables": tables}


# ---------------------------------------------------------------------------
# TEXT DIFF
# ---------------------------------------------------------------------------

def diff_text(old_lines: list[str], new_lines: list[str], context: int = 3) -> list[dict]:
    """Produce text diffs with surrounding context lines.

    Each diff entry:
        {
            "context_before": [str, ...],
            "removed": [str, ...],
            "added": [str, ...],
            "context_after": [str, ...],
        }
    """
    matcher = difflib.SequenceMatcher(None, old_lines, new_lines)
    diffs = []

    for tag, i1, i2, j1, j2 in matcher.get_opcodes():
        if tag == "equal":
            continue

        ctx_before = old_lines[max(0, i1 - context): i1]
        ctx_after = new_lines[j2: j2 + context]
        removed = old_lines[i1:i2] if tag in ("replace", "delete") else []
        added = new_lines[j1:j2] if tag in ("replace", "insert") else []

        diffs.append({
            "context_before": ctx_before,
            "removed": removed,
            "added": added,
            "context_after": ctx_after,
        })

    return diffs


# ---------------------------------------------------------------------------
# TABLE DIFF
# ---------------------------------------------------------------------------

def _table_signature(table: list[list[str]]) -> str:
    """Simple fingerprint: first row joined. Helps match tables across versions."""
    if not table:
        return ""
    return "|".join(table[0])


def _match_tables(
    old_tables: list[list[list[str]]],
    new_tables: list[list[list[str]]],
) -> list[tuple[int | None, int | None]]:
    """Match old tables to new tables by header similarity, then by position.

    Returns list of (old_index | None, new_index | None) pairs.
    None on either side means added or removed.
    """
    matched_old = set()
    matched_new = set()
    pairs = []

    # Pass 1: match by identical header row
    old_sigs = {i: _table_signature(t) for i, t in enumerate(old_tables)}
    new_sigs = {i: _table_signature(t) for i, t in enumerate(new_tables)}

    for oi, osig in old_sigs.items():
        if not osig:
            continue
        for ni, nsig in new_sigs.items():
            if ni in matched_new:
                continue
            if osig == nsig:
                pairs.append((oi, ni))
                matched_old.add(oi)
                matched_new.add(ni)
                break

    # Pass 2: match remaining by position order
    remaining_old = [i for i in range(len(old_tables)) if i not in matched_old]
    remaining_new = [i for i in range(len(new_tables)) if i not in matched_new]

    for oi, ni in zip(remaining_old, remaining_new):
        pairs.append((oi, ni))
        matched_old.add(oi)
        matched_new.add(ni)

    # Leftovers
    for oi in range(len(old_tables)):
        if oi not in matched_old:
            pairs.append((oi, None))
    for ni in range(len(new_tables)):
        if ni not in matched_new:
            pairs.append((None, ni))

    return pairs


def diff_tables(
    old_tables: list[list[list[str]]],
    new_tables: list[list[list[str]]],
) -> dict:
    """Compare tables cell-by-cell.

    Returns:
        {
            "cell_changes": [
                { "table_index", "row", "col", "header", "before", "after" }
            ],
            "rows_added": [
                { "table_index", "row_index", "cells" }
            ],
            "rows_removed": [
                { "table_index", "row_index", "cells" }
            ],
            "tables_added": [ [[row], ...] ],
            "tables_removed": [ [[row], ...] ],
        }
    """
    pairs = _match_tables(old_tables, new_tables)

    cell_changes = []
    rows_added = []
    rows_removed = []
    tables_added = []
    tables_removed = []

    for oi, ni in pairs:
        if oi is None:
            tables_added.append(new_tables[ni])
            continue
        if ni is None:
            tables_removed.append(old_tables[oi])
            continue

        old_t = old_tables[oi]
        new_t = new_tables[ni]

        # Use the display index (new side if available)
        table_idx = ni

        # Figure out header row for labelling
        header = old_t[0] if old_t else new_t[0] if new_t else []

        max_rows = max(len(old_t), len(new_t))
        for r in range(max_rows):
            old_row = old_t[r] if r < len(old_t) else None
            new_row = new_t[r] if r < len(new_t) else None

            if old_row is None:
                rows_added.append({
                    "table_index": table_idx,
                    "row_index": r,
                    "cells": new_row,
                })
                continue
            if new_row is None:
                rows_removed.append({
                    "table_index": table_idx,
                    "row_index": r,
                    "cells": old_row,
                })
                continue

            # Compare cell by cell
            max_cols = max(len(old_row), len(new_row))
            for c in range(max_cols):
                old_val = old_row[c] if c < len(old_row) else ""
                new_val = new_row[c] if c < len(new_row) else ""
                if old_val != new_val:
                    col_header = header[c] if c < len(header) else f"col_{c}"
                    cell_changes.append({
                        "table_index": table_idx,
                        "row": r,
                        "col": c,
                        "header": col_header,
                        "before": old_val,
                        "after": new_val,
                    })

    return {
        "cell_changes": cell_changes,
        "rows_added": rows_added,
        "rows_removed": rows_removed,
        "tables_added": tables_added,
        "tables_removed": tables_removed,
    }


# ---------------------------------------------------------------------------
# MAIN FUNCTION
# ---------------------------------------------------------------------------

def diff_pdfs(old_path: str, new_path: str, context: int = 3) -> dict:
    """Compare two PDFs and return a JSON-serialisable diff.

    Args:
        old_path: path to the old/previous PDF
        new_path: path to the new/current PDF
        context: number of surrounding lines to include for text diffs

    Returns:
        dict ready to be serialised to JSON or sent to an LLM.
    """
    old_content = extract_pdf_content(old_path)
    new_content = extract_pdf_content(new_path)

    text_diffs = diff_text(old_content["text_lines"], new_content["text_lines"], context)
    table_result = diff_tables(old_content["tables"], new_content["tables"])

    has_changes = bool(
        text_diffs
        or table_result["cell_changes"]
        or table_result["rows_added"]
        or table_result["rows_removed"]
        or table_result["tables_added"]
        or table_result["tables_removed"]
    )

    return {
        "old_file": str(old_path),
        "new_file": str(new_path),
        "has_changes": has_changes,
        "text_diffs": text_diffs,
        "table_diffs": table_result["cell_changes"],
        "table_rows_added": table_result["rows_added"],
        "table_rows_removed": table_result["rows_removed"],
        "tables_added": table_result["tables_added"],
        "tables_removed": table_result["tables_removed"],
    }


pdf_test_1 = "data/pdfs/latest/diff_test_1.pdf"
pdf_test_2 = "data/pdfs/latest/diff_test_2.pdf"

#diff_pdf_test = diff_pdfs(pdf_test_1, pdf_test_2, 3)
#print(diff_pdf_test)

# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python -m src.pdf_diff <old.pdf> <new.pdf>")
        sys.exit(1)

    old_pdf = sys.argv[1]
    new_pdf = sys.argv[2]

    for p in (old_pdf, new_pdf):
        if not Path(p).exists():
            print(f"ERROR: file not found: {p}")
            sys.exit(1)

    result = diff_pdfs(old_pdf, new_pdf)
    print(json.dumps(result, indent=2, ensure_ascii=False))
