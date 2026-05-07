def main():
    print("Hello from market-watch-agent!")


if __name__ == "__main__":
    main()

from summary_utils import save_classification_summary

result_message = save_classification_summary("data/outputs/2026-05-06_classified_test.json")
print(result_message)