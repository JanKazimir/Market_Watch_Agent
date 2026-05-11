## Reads the excel reference file and outputs a CSV of PDF links to scrape.
##
## Output columns:
##   source_key   : e.g. argenta_saving, argenta_terms
##   bank         : e.g. Argenta
##   url          : the pdf url to scrape
##   product_type : regulated / unregulated / tarif
##   language     : nl / fr (inferred from url)
##   pdf_source   : parent page where the pdf download link lives


import re
import shutil
import pandas as pd
from pathlib import Path
import datetime


#
# CONFIG
#

today = datetime.date.today().isoformat()

EXCEL_SOURCE = "list_of_banks_ref_new.xlsx"
EXCEL_PATH = Path(__file__).parent.parent / "data" / EXCEL_SOURCE
BACKUP_DIR = Path(__file__).parent.parent / "data" / "excel_backups"
OUTPUT_PATH = Path(__file__).parent.parent / "data" / "pdf_links_csv" / f"{today}_pdf_list.csv"

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


if __name__ == "__main__":
    backup_excel()
    df = load_pdf_links()
    pdf_df = build_pdf_list(df)
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    pdf_df.to_csv(OUTPUT_PATH, index=False)
    print(pdf_df.to_string())
    print(f"\nWrote {len(pdf_df)} rows to {OUTPUT_PATH}")
