def main():
    print("Hello from market-watch-agent!")


if __name__ == "__main__":
    main()

from summary_utils import save_classification_summary, get_latest_classified_file

# 1. Define where your JSON files are stored
OUTPUT_DIRECTORY = "data/outputs"

try:
    # 2. Find the newest file automatically
    latest_json = get_latest_classified_file(OUTPUT_DIRECTORY)
    print(f"📂 Processing: {latest_json}")
    
    # 3. Generate and save the TXT summary
    result = save_classification_summary(latest_json)
    print(result)

except FileNotFoundError as e:
    print(f"❌ Error: {e}")
except Exception as e:
    print(f"⚠️ An unexpected error occurred: {e}")