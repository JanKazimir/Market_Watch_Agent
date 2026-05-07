## Main function that runs the whole pipeline. 
## We are logging results to file using subprocesses.


import subprocess
import datetime
from pathlib import Path


## Paths
BASE_DIR = Path.parent
LOG_DIR = Path("data/outputs")
today = datetime.date.today().isoformat()
log_path = LOG_DIR / f"{today}_pipeline.log"

def run_step(script, log_file):
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



## Main function that runs the whole pipeline. 
## We are logging results to file using subprocesses.


import subprocess
import datetime
from pathlib import Path


## CONFIG:
BASE_DIR = Path(__file__).resolve().parent
LOG_DIR = BASE_DIR / "data" / "logs"
SRC_DIR = BASE_DIR / "src" 
today = datetime.date.today().isoformat()
log_path = LOG_DIR / f"{today}_pipeline.log"


## Scripts Paths:
test_function_path = SRC_DIR / "test_function.py"


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





def main():
    print("testing main...")
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    with open(log_path, "a") as log_file:
        run_step(test_function_path, log_file)


if __name__ == "__main__":
    main()

import os
from dotenv import load_dotenv # Add this line

load_dotenv() # Add this line - it loads the .env file into the system


from summary_utils import (
    save_classification_summary, 
    get_latest_classified_file, 
    send_summary_email
)

# Configuration
OUTPUT_DIRECTORY = "data/outputs"
TEAM_EMAILS = ["hoanghanhtien97@gmail.com"]

try:
    # STEP 1: Find the latest JSON
    latest_json = get_latest_classified_file(OUTPUT_DIRECTORY)
    print(f"📂 Found JSON: {latest_json}")
    
    # STEP 2: Save the TXT file and get its path
    # We use a variable to capture the path created inside the function
    # Note: You might want to update save_classification_summary to return the path!
    save_result = save_classification_summary(latest_json)
    print(save_result)
    
    # STEP 3: Identify the TXT file just created
    # Based on our logic, it's in 'daily_summaries'
    import datetime
    today_str = datetime.datetime.now().strftime("%Y-%m-%d")
    latest_txt = f"daily_summaries/Summary_{today_str}.txt"

    # STEP 4: Send the email
    email_result = send_summary_email(latest_txt, TEAM_EMAILS)
    print(email_result)

except Exception as e:
    print(f"⚠️ Process failed: {e}")

print(f"DEBUG: Sender is {os.environ.get('EMAIL_SENDER')}")
print(f"DEBUG: Password is set: {'Yes' if os.environ.get('EMAIL_PASSWORD') else 'No'}")