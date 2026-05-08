## Main function that runs the whole pipeline. 
## We are logging results to file using subprocesses. (/data/logs/{today}_pipeline.log)


import subprocess
import datetime
from pathlib import Path



## CONFIG:
BASE_DIR = Path(__file__).resolve().parent
LOG_DIR = BASE_DIR / "data" / "logs"
SRC_DIR = BASE_DIR / "src" 
SCRAPE_DIR = BASE_DIR / "scrape" 
SCRAPING_DIR = SRC_DIR / "scraping"

today = datetime.date.today().isoformat()
log_path = LOG_DIR / f"{today}_pipeline.log"

test_function_path = SRC_DIR / "test_function.py"

## Set up scripts:
read_excel_file = SRC_DIR / "read_excel_file.py"

## Scrapping scripts:
playright_scraper = SCRAPE_DIR / "playright_scraper.py"
apify_scraper = SCRAPE_DIR / "apify_scraper.py"
pdf_scrapper = SCRAPING_DIR / "pdf_scrapper.py"

## Diff and Cross-check:
diff_function = BASE_DIR / "diff.py"
cross_reference = BASE_DIR / "cross_reference.py"



# --------------------------------#
# Functions:
# --------------------------------#

## Function to run a script, and write its console to file.
def run_step(script, log_file=log_path):
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


#def run(script):
#print(f"[RUN] {script}")
#result = subprocess.run(["python", script], capture_output=True, text=True)
#if result.returncode != 0:
#print(f"❌ Error in {script}:\n{result.stderr}")
#else:
#print(f"✅ {script} done")



def main():
    # Create a log dir if it does not exist:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    
    # opens the log file, run each scripts, capture console outputs to log.
    with open(log_path, "a") as log_file:
        print("Pipeline started...")
        
        
        ## Reloading links from excell
        print("Retrieving links from Excel.")
        run_step(read_excel_file, log_file)
        
        ##
        ## SCRAPPING:
        ##
        print("Begin Scrapping")
        
        # PDF
        print("Getting pdfs...")
        #run_step(pdf_scrapper, log_file)
        print("Pdf downloaded and checked")
        
        ## HTML
        print("Getting html pages...")
        
        # Playright
        print("Playright scraping begun, this will take a few minutes.")
        #run_step(playright_scraper)
        print("Playright scraping done.")
        
        # Apify : they prefer playright
        #print("Apify scraping begun, this will take a few minutes.")
        #run_step(apify_scraper, log_file)
        #print("Apify scraping done.")
        
        # Getting Diffs and cross differences.
        print("Looking for differences...")
        run_step(diff_function, log_file)
        print("...cross referencing...")
        run_step(cross_reference, log_file)
        
        

        # 
        
        print("Pipeline finished")
        


if __name__ == "__main__":
    main()
