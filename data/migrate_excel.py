# util script to migrate the excel to the new format
# Converts the wide (32-column) layout into a tall format:
#   bank | link_type | url | scrape_frequency | notes | source_url

import re
import pandas as pd
from pathlib import Path

excel_file = Path(__file__).parent / "list_of_banks_to_migrate.xlsx"
output_file = Path(__file__).parent / "list_of_banks_ref_new.xlsx"


# Each entry: (excel_column_for_url, link_type, freq_column, notes_column, source_column)
LINK_COLUMNS = [
    ("news_page_url",                           "news",                         "news_page_url_scrape_frequency",                       "news_page_url_notes",                          None),
    ("saving_accounts_url",                     "saving_accounts",              "saving_accounts_url_scrape_frequency",                 "saving_accounts_url_notes",                    None),
    ("tarif_pdf_url",                           "tarif_pdf",                    "tarif_pdf_source_scrape_frequency",                    "tarif_pdf_notes",                              "tarif_pdf_source"),
    ("regulated_savings_url",                   "regulated_savings",            "regulated_savings_url_scrape_frequency",                "regulated_savings_url_notes",                   None),
    ("regulated_savings_individual_products_url","regulated_savings_products",   "regulated_savings_individual_products_url_scrape_frequency", "regulated_savings_individual_products_url_notes", None),
    ("regulated_savings_pdf",                   "regulated_savings_pdf",        "regulated_savings_pdf_scrape_frequency",                "regulated_savings_pdf_notes",                   "regulated_savings_pdf_source"),
    ("term_accounts_url",                       "term_accounts",                "term_accounts_url_scrape_frequency",                   "term_accounts_ulr_notes",                      None),
    ("term_accounts_pdf",                       "term_accounts_pdf",            "term_accounts_pdf_scrape_frequency",                   "term_accounts_pdf_notes",                      "term_accounts_pdf_source"),
]


def split_urls(cell):
    """Split a cell that may contain multiple URLs separated by ; or newlines."""
    if pd.isna(cell) or not str(cell).strip():
        return []
    return [u.strip().strip('"') for u in re.split(r'[;\n]+', str(cell)) if u.strip()]


def is_url(text):
    """Check if a string looks like a URL."""
    return bool(re.match(r'https?://', text.strip().strip('"')))


def load_and_migrate():
    df = pd.read_excel(excel_file)
    df = df[df["bank_name"].notna()]
    df = df[~df["bank_name"].str.contains("DATA URL", na=False)]

    # --- Links sheet (tall format) ---
    link_rows = []
    for _, row in df.iterrows():
        bank = row["bank_name"]

        for url_col, link_type, freq_col, notes_col, source_col in LINK_COLUMNS:
            urls = split_urls(row.get(url_col))
            sources = split_urls(row.get(source_col)) if source_col else []
            freq = row.get(freq_col) if freq_col else None
            notes = row.get(notes_col) if notes_col else None

            # Handle edge case: URLs accidentally put in the notes column
            if not urls and notes and not pd.isna(notes) and is_url(str(notes).split(";")[0].split("\n")[0].strip()):
                urls = split_urls(notes)
                notes = None

            # Clean up freq
            if pd.notna(freq):
                freq = int(freq)
            else:
                freq = None

            # Clean up notes
            if pd.isna(notes) or not str(notes).strip():
                notes = None

            for i, url in enumerate(urls):
                source = sources[i] if i < len(sources) else (sources[0] if len(sources) == 1 else None)
                link_rows.append({
                    "bank": bank,
                    "link_type": link_type,
                    "url": url,
                    "scrape_frequency": freq,
                    "notes": notes if i == 0 else None,  # only attach notes to first URL
                    "source_url": source,
                })

    links_df = pd.DataFrame(link_rows)

    # --- Banks sheet (metadata) ---
    banks_df = df[["bank_name", "holding_group", "belgian_website_url"]].copy()
    banks_df.columns = ["bank", "holding_group", "website"]
    # Grab website-level notes
    website_notes = df["webiste note"].where(df["webiste note"].notna(), None)
    banks_df["notes"] = website_notes.values
    # Also grab saving_accounts_url_notes where there's no saving_accounts_url (general notes)
    for i, (_, row) in enumerate(df.iterrows()):
        if pd.isna(row.get("saving_accounts_url")) and pd.notna(row.get("saving_accounts_url_notes")):
            existing = banks_df.iloc[i]["notes"]
            extra = row["saving_accounts_url_notes"]
            if pd.notna(existing) and str(existing).strip():
                banks_df.iat[i, 3] = f"{existing}; {extra}"
            else:
                banks_df.iat[i, 3] = extra

    # --- Write output ---
    with pd.ExcelWriter(output_file, engine="openpyxl") as writer:
        banks_df.to_excel(writer, sheet_name="Banks", index=False)
        links_df.to_excel(writer, sheet_name="Links", index=False)

    print(f"Banks sheet: {len(banks_df)} rows")
    print(f"Links sheet: {len(links_df)} rows")
    print(f"\nWrote to {output_file}")
    print("\n--- Banks ---")
    print(banks_df.to_string(index=False))
    print("\n--- Links (first 30) ---")
    print(links_df.head(30).to_string(index=False))


if __name__ == "__main__":
    load_and_migrate()
