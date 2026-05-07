def main():
    print("Hello from market-watch-agent!")


if __name__ == "__main__":
    main()

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