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



def main():
    print("testing main...")
    run_step()


if __name__ == "__main__":
    main()
