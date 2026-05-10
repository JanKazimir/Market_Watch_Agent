import feedparser
import json
import hashlib
import re
import datetime
from pathlib import Path

# Configuration
SNAPSHOT_DIR = Path("data/snapshots")
FEEDS = {
    "top_stories": "https://www.lecho.be/rss/top_stories.xml",
    "entreprises_banques": "https://www.lecho.be/rss/entreprises_banques.xml"
}

WATCHLIST = [
    "bnp paribas", "fortis", "kbc", "belfius", "argenta", "crelan",
    "beobank", "vdk", "nagelmackers", "axa", "fintro", "deutsche bank",
    "keytrade", "medirect", "santander", "nibc", "bunq", "hello bank",
    "izola", "cph", "bankb", "triodos", "europabank", "unicredit",
    "épargne", "placement", "investissement", "banque", "ing", "taux"
]

def compute_checksum(data: str) -> str:
    """Compute SHA-256 checksum of the content for change detection."""
    return hashlib.sha256(data.encode("utf-8")).hexdigest()

def build_snapshot(feed_name: str, url: str, articles: list[dict]) -> dict:
    """
    Build the final structured snapshot with metadata.
    Standardized output format for the pipeline.
    """
    return {
        "source": "lecho.be",
        "feed": feed_name,
        "url": url,
        "timestamp": datetime.datetime.now().isoformat(),
        "date": datetime.date.today().isoformat(),
        "num_articles": len(articles),
        "checksum": compute_checksum(json.dumps(articles, sort_keys=True)),
        "articles": articles,
    }

def save_snapshot(snapshot: dict, date_str: str, feed_name: str) -> Path:
    """Save structured snapshot as JSON."""
    SNAPSHOT_DIR.mkdir(parents=True, exist_ok=True)
    # Changed name from bankshopper to lecho
    filepath = SNAPSHOT_DIR / f"lecho_{feed_name}_{date_str}.json"
    filepath.write_text(
        json.dumps(snapshot, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    return filepath

def run_monitor():
    date_str = datetime.date.today().isoformat()

    for feed_name, url in FEEDS.items():
        feed = feedparser.parse(url)
        filtered_articles = []

        for entry in feed.entries:
            title = entry.title
            description = getattr(entry, 'summary', "")
            content_to_scan = (title + " " + description).lower()

            # Exact word boundary check to avoid 'shopping' matching 'ing'
            is_match = any(re.search(r'\b' + re.escape(key) + r'\b', content_to_scan) for key in WATCHLIST)

            if is_match:
                filtered_articles.append({
                    "guid": getattr(entry, 'id', entry.link),
                    "title": title,
                    "description": description,
                    "link": entry.link,
                    "pub_date": getattr(entry, 'published', ""),
                    "category": getattr(entry, 'category', "News")
                })

        # Build the standardized snapshot
        snapshot = build_snapshot(feed_name, url, filtered_articles)

        # Save to disk
        saved_path = save_snapshot(snapshot, date_str, feed_name)
        print(f"Successfully saved {feed_name} snapshot to: {saved_path}")

if __name__ == "__main__":
    run_monitor()