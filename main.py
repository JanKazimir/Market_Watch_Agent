"""
Pipeline Orchestrator
======================
Runs the full Daily Market Watch pipeline in sequence:
  1. Read URLs from Excel
  2. Scrape (Playwright + PDF monitor)
  3. Diff (detect changes across all sources)
  4. Cross-reference (compare sources against each other)
  5. Classify (LLM classification using ING taxonomy)
  6. Report + Email (generate summary and send to team)

Usage:
    python main.py

Logs:
    data/logs/YYYY-MM-DD_pipeline.log
"""

import subprocess
import datetime
from pathlib import Path
import os
from dotenv import load_dotenv


# ----------------------------------------------------------
# CONFIG
# ----------------------------------------------------------

BASE_DIR = Path(__file__).resolve().parent
LOG_DIR = BASE_DIR / "data" / "logs"
SRC_DIR = BASE_DIR / "src"
SCRAPE_DIR = SRC_DIR / "scrape"

today = datetime.date.today().isoformat()
log_path = LOG_DIR / f"{today}_pipeline.log"

test_function_path = SRC_DIR / "test_function.py"

## Set up scripts:
read_excel_file = SRC_DIR / "read_excel_file.py"

## Scrapping scripts:
playright_scraper = SCRAPE_DIR / "playright_scraper.py"
# apify_scraper = SCRAPE_DIR / "apify_scraper.py"         ---- Remove the comment to activate apify scraper ---
bankshopper_scraper = SCRAPE_DIR / "bankshopper_scraper.py"
bankshopper_term_scraper = SCRAPE_DIR / "bankshopper_scraper_term_acc.py"
lecho_rss = SCRAPE_DIR / "lecho_rss.py"
pdf_scrapper = SCRAPE_DIR / "pdf_scrapper.py"

## Diff and Cross-check:
diff_function = SRC_DIR / "diff.py"
cross_reference = SRC_DIR / "cross_reference.py"

# Classify
classify_script = SRC_DIR / "llm" / "classify_facts.py"


# --------------------------------#
# Functions:
# --------------------------------#

def run_step(script, log_file=log_path):
    """Run a Python script as subprocess and log its output."""
    name = Path(script).stem
    now = datetime.datetime.now().strftime("%H:%M:%S")
    log_file.write(f"\n{'='*50}\n")
    log_file.write(f"[{now}] Starting: {name}\n")
    log_file.write(f"{'='*50}\n")

    result = subprocess.run(
        ["python", script],
        capture_output=True,
        text=True,
    )

    for line in result.stdout.splitlines():
        ts = datetime.datetime.now().strftime("%H:%M:%S")
        log_file.write(f"[{ts}] {name} | {line}\n")

    if result.stderr:
        for line in result.stderr.splitlines():
            ts = datetime.datetime.now().strftime("%H:%M:%S")
            log_file.write(f"[{ts}] {name} | ERROR | {line}\n")

    now = datetime.datetime.now().strftime("%H:%M:%S")
    status = "OK" if result.returncode == 0 else f"FAILED ({result.returncode})"
    log_file.write(f"[{now}] {name} | {status}\n")

    return result.returncode == 0


# ---------------------------------------------------------------------------
# MAIN PIPELINE
# ---------------------------------------------------------------------------

def main():
    # Create a log dir if it does not exist:
    LOG_DIR.mkdir(parents=True, exist_ok=True)

    # opens the log file, run each scripts, capture console outputs to log.
    with open(log_path, "a") as log_file:
        start_time = datetime.datetime.now()
        print(f"Pipeline started at {start_time.strftime('%H:%M:%S')}")
        log_file.write(f"\n{'#'*50}\n")
        log_file.write(f"Pipeline started: {start_time.isoformat()}\n")
        log_file.write(f"{'#'*50}\n")

        ## Reloading links from excel
        #print("[1/6] Retrieving links from Excel...")
        #run_step(read_excel_file, log_file)

        ##
        ## SCRAPPING:
        ##
        print("[2/6] Scraping...")

        # Bankshopper (regulated savings)
        print("  Bankshopper savings scraping...")
        run_step(bankshopper_scraper, log_file)

        # Bankshopper (term accounts)
        print("  Bankshopper term accounts scraping...")
        run_step(bankshopper_term_scraper, log_file)

        # L'Echo RSS (news headlines)
        print("  L'Echo RSS feed...")
        run_step(lecho_rss, log_file)

        # Playwright scraper
        print("  Playwright scraping (this may take a few minutes)...")
        run_step(playright_scraper, log_file)

        # PDF
        #print("Getting pdfs...")
        #run_step(pdf_scrapper, log_file)
        #print("Pdf downloaded and checked")

        # Apify : they prefer playright
        # print("Apify scraping begun, this will take a few minutes.")
        # run_step(apify_scraper, log_file)
        # print("Apify scraping done.")

        # Getting Diffs and cross differences.
        print("[3/6] Detecting changes...")
        run_step(diff_function, log_file)

        print("[4/6] Cross-referencing sources...")
        run_step(cross_reference, log_file)

        # Classify
        print("[5/6] Classifying changes with LLM...")
        run_step(classify_script, log_file)

        # Report + Email
        print("[6/6] Generating report and sending email...")
        try:
            from src.summary_utils import (
                save_classification_summary,
                get_latest_classified_file,
                send_summary_email,
            )

            load_dotenv(dotenv_path=BASE_DIR / ".env")
            sender = os.environ.get("EMAIL_SENDER")
            password = os.environ.get("EMAIL_PASSWORD")

            if not sender or not password:
                print(
                    "  WARNING: EMAIL_SENDER or EMAIL_PASSWORD not set. Skipping email."
                )
                log_file.write(f"[REPORT] Email credentials missing. Skipped.\n")
            else:
                latest_json = get_latest_classified_file(
                    str(BASE_DIR / "data" / "outputs")
                )
                txt_path = save_classification_summary(latest_json)
                print(f"  Report saved: {txt_path}")
                log_file.write(f"[REPORT] Saved: {txt_path}\n")

                team = ["hoanghanhtien97@gmail.com"]
                result = send_summary_email(txt_path, team, sender, password)
                print(f"  {result}")
                log_file.write(f"[EMAIL] {result}\n")

        except Exception as e:
            print(f"  Error in report/email: {e}")
            log_file.write(f"[REPORT] ERROR: {e}\n")

        # ---- Done ----
        end_time = datetime.datetime.now()
        duration = end_time - start_time
        print(f"\nPipeline finished in {duration.total_seconds():.1f} seconds")
        log_file.write(f"\n{'#'*50}\n")
        log_file.write(f"Pipeline finished: {end_time.isoformat()}\n")
        log_file.write(f"Duration: {duration.total_seconds():.1f} seconds\n")
        log_file.write(f"{'#'*50}\n")


if __name__ == "__main__":
    main()

"""
import os
from pathlib import Path
from dotenv import load_dotenv
from summary_utils import (
    save_classification_summary,
    get_latest_classified_file,
    send_summary_email)

load_dotenv() # Add this line - it loads the .env file into the system
# 1. Get the absolute path to the folder containing main.py
env_path = Path(__file__).resolve().parent / ".env"

# 2. Force load the .env from that specific path
load_dotenv(dotenv_path=env_path)

# DEBUG: Let's verify immediately
SENDER = os.environ.get("EMAIL_SENDER")
PASSWORD = os.environ.get("EMAIL_PASSWORD")

print(f"DEBUG: env_path used: {env_path}")
print(f"DEBUG: Preparing to send from {SENDER}")

try:
    # Get newest JSON
    latest_json = get_latest_classified_file("data/outputs")

    # Save TXT (Ensure your save_classification_summary returns the FILE PATH string)
    txt_path = save_classification_summary(latest_json)
    print(f"DEBUG: TXT Path created: {txt_path}")

    # CALLING THE FUNCTION: Pass the variables here
    team = ["hoanghanhtien97@gmail.com"]
    result = send_summary_email(txt_path, team, SENDER, PASSWORD)
    print(result)

except Exception as e:
    print(f"⚠️ Error: {e}")

"""
