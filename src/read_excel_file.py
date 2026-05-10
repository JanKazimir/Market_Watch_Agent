## This is a script to read the excell file and take the values into a useable format for the rest of the pipeline.

## Target is a csv of these :
#source_key : crelan_saving or crelan_terms
#bank: Crelan
#url: the url
#product_type: regulated or unrtegulated
#language: nl fr
#news_url: if the url is news


## This is a script to read the excell file and take the values into a useable format for the rest of the pipeline.


# imports
import re
import pandas as pd
from pathlib import Path
import importlib.util

import datetime
import json
import csv


#
# CONFIG
#

today = datetime.date.today().isoformat()

EXCEL_PATH = Path(__file__).parent.parent / "data" / "list_of_banks_ref_new.xlsx"
OUTPUT_PATH = Path(__file__).parent.parent / "data" / "pdf_links_csv" / f"{today}_pdf_list.csv"



# the excell file has these columns (32 total, 40 rows):
#   bank_name, holding_group, belgian_website_url, webiste note,
#   news_page_url, saving_accounts_url,
#   regulated_savings_url, regulated_savings_individual_products_url, regulated_savings_pdf,
#   term_accounts_url, term_accounts_pdf,
#   tarif_pdf_url,
#   + scrape_frequency and notes columns for each
#   + a separator column "UNREGULATED BEGINS"
#
# WATCH OUT:
#   - Some URL cells contain multiple URLs separated by ";\n"
#   - Some cells have stray quotes around URLs: "https://..."
#   - The "UNREGULATED BEGINS" column is just a visual separator (all NaN), skip it
#   - Language (nl/fr) is NOT in the excel — must be inferred from the URL path


## Target columns for pdfs:
#   source_key : e.g. argenta_saving, argenta_terms
#   bank       : e.g. Argenta
#   url        : the pdf url to scrape
#   product_type : regulated / unregulated
#   language   : nl / fr (inferred from url)
#   pdf_source   : source_url of the pdf download link


#
## Helper Functions:
#

# load the excell, put it in a pandas df.
def load_excel() -> pd.DataFrame:
    df = pd.read_excel(EXCEL_PATH)
    df = df.drop(columns=["UNREGULATED BEGINS"])
    df = df[df["bank_name"].notna()]
    df = df[~df["bank_name"].str.contains("DATA URL", na=False)]
    return df


list_of_cols = ['bank_name', 'holding_group', 'belgian_website_url', 'webiste note', 'news_page_url', 'news_page_url_scrape_frequency', 'news_page_url_notes', 'saving_accounts_url', 'saving_accounts_url_scrape_frequency', 'saving_accounts_url_notes', 'tarif_pdf_url', 'tarif_pdf_source_scrape_frequency', 'tarif_pdf_notes', 'tarif_pdf_source', 'regulated_savings_url', 'regulated_savings_url_scrape_frequency', 'regulated_savings_url_notes', 'regulated_savings_individual_products_url', 'regulated_savings_individual_products_url_scrape_frequency', 'regulated_savings_individual_products_url_notes', 'regulated_savings_pdf', 'regulated_savings_pdf_scrape_frequency', 'regulated_savings_pdf_notes', 'regulated_savings_pdf_source', 'term_accounts_url', 'term_accounts_url_scrape_frequency', 'term_accounts_ulr_notes', 'term_accounts_pdf', 'term_accounts_pdf_scrape_frequency', 'term_accounts_pdf_notes', 'term_accounts_pdf_source']

# df.head(3)
#print(df.columns.tolist())
#print(f"\n{len(df)} rows loaded")
#print(df.head(3))

# excel column -> (source_key suffix, product_type)
PDF_COLUMNS = {
    "regulated_savings_pdf":  ("saving",  "regulated"),
    "term_accounts_pdf":      ("terms",   "unregulated"),
    "tarif_pdf_url":          ("tarif",   "tarif"),
}

# matching source columns (same order as PDF_COLUMNS)
PDF_SOURCE_COLUMNS = {
    "regulated_savings_pdf":  "regulated_savings_pdf_source",
    "term_accounts_pdf":      "term_accounts_pdf_source",
    "tarif_pdf_url":          "tarif_pdf_source",
}


def split_urls(cell):
    if pd.isna(cell):
        return []
    return [u.strip().strip('"') for u in re.split(r';\s*', str(cell)) if u.strip()]


def detect_language(url):
    match = re.search(r'/(fr|nl)(/|-|$)', url)
    return match.group(1) if match else ""


def make_source_key(bank_name, suffix):
    slug = re.sub(r'[^a-z0-9]+', '_', bank_name.lower()).strip('_')
    return f"{slug}_{suffix}"


def build_pdf_list(df: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for _, row in df.iterrows():
        bank = row["bank_name"]

        for pdf_col, (suffix, product_type) in PDF_COLUMNS.items():
            urls = split_urls(row[pdf_col])
            sources = split_urls(row[PDF_SOURCE_COLUMNS[pdf_col]])

            for i, url in enumerate(urls):
                source = sources[i] if i < len(sources) else ""
                lang = detect_language(url) or detect_language(source)
                rows.append({
                    "source_key": make_source_key(bank, suffix),
                    "bank": bank,
                    "url": url,
                    "product_type": product_type,
                    "language": lang,
                    "pdf_source": source,
                })

    return pd.DataFrame(rows)



if __name__ == "__main__":
    df = load_excel()
    pdf_df = build_pdf_list(df)
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    pdf_df.to_csv(OUTPUT_PATH, index=False)
    print(pdf_df.to_string())
    print(f"\nWrote {len(pdf_df)} rows to {OUTPUT_PATH}")
