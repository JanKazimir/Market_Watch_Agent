def main():
    print("Hello from market-watch-agent!")


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