"""
send_digest.py
--------------
Reads the latest classified JSON and sends a short highlight email.

HOW TO USE:
  1. Copy this file into your project (same folder as classify_facts.py)

  2. Set 3 environment variables in your terminal:
       On Mac/Linux:
         export EMAIL_SENDER="yourname@gmail.com"
         export EMAIL_PASSWORD="xxxx xxxx xxxx xxxx"
         export EMAIL_RECIPIENTS="colleague@ing.be,boss@ing.be"

       On Windows:
         set EMAIL_SENDER=yourname@gmail.com
         set EMAIL_PASSWORD=xxxx xxxx xxxx xxxx
         set EMAIL_RECIPIENTS=colleague@ing.be,boss@ing.be

  3. Add 2 lines at the bottom of classify_facts.py:
       from send_digest import send_digest
       send_digest()

  4. Or test it standalone right now:
       python send_digest.py

  NOTE: EMAIL_PASSWORD must be a Gmail App Password, NOT your normal Gmail password.
  Get one here: Gmail → Google Account → Security → 2-Step Verification → App Passwords
"""

import json
import os
import glob
import smtplib
from pathlib import Path
from datetime import datetime
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart


# ─────────────────────────────────────────────────────────────────────────────
# SETTINGS — edit these to control what appears in the email
# ─────────────────────────────────────────────────────────────────────────────

MAX_HIGH_SHOWN   = 5   # max HIGH impact items to show (show all if small number)
MAX_MEDIUM_SHOWN = 3   # max Medium items to show
SHOW_LOW_DETAIL  = False  # False = just show count, True = list them all

# Where your classified files are saved
# This path works if send_digest.py is in the same folder as classify_facts.py
OUTPUT_DIR = Path(__file__).resolve().parent / "data" / "outputs"


# ─────────────────────────────────────────────────────────────────────────────
# STEP 1 — Find and load the latest classified file
# ─────────────────────────────────────────────────────────────────────────────

def load_latest_file() -> tuple[dict, str]:
    """
    Automatically finds the newest *_classified*.json in data/outputs/.
    You never need to type the filename manually.

    Returns:
        data     → the full classified dict
        date_str → the date from the filename e.g. "2026-05-06"
    """
    pattern = str(OUTPUT_DIR / "*_classified*.json")
    files   = sorted(glob.glob(pattern), reverse=True)  # newest first

    if not files:
        # If no file found in the outputs folder, check if a test file
        # was passed via environment variable (useful for testing)
        test_file = os.environ.get("TEST_CLASSIFIED_FILE", "")
        if test_file and Path(test_file).exists():
            files = [test_file]
        else:
            raise FileNotFoundError(
                f"\nNo classified file found.\n"
                f"Expected location: {OUTPUT_DIR}/*_classified*.json\n"
                f"Make sure classify_facts.py has run first, or set:\n"
                f"  TEST_CLASSIFIED_FILE=/path/to/your/file.json"
            )

    latest_path = files[0]

    # Extract date from filename: "2026-05-06_classified_test.json" → "2026-05-06"
    filename = Path(latest_path).stem            # removes .json
    date_str = filename.split("_")[0]            # takes the first part before _
    if not date_str[0].isdigit():                # if filename doesn't start with date
        date_str = datetime.utcnow().strftime("%Y-%m-%d")  # fall back to today

    with open(latest_path, encoding="utf-8") as f:
        data = json.load(f)

    print(f"  Loaded: {Path(latest_path).name}  ({len(data)} entries)")
    return data, date_str


# ─────────────────────────────────────────────────────────────────────────────
# STEP 2 — Extract highlights from the data
# ─────────────────────────────────────────────────────────────────────────────

def format_before_after(entry: dict) -> str:
    """
    Builds a readable 'X → Y' string for product changes.
    Handles both numeric rates (adds %) and text values.
    Skips URLs — too long and not useful in an email.
    """
    before = entry.get("before", "")
    after  = entry.get("after",  "")
    field  = entry.get("field",  "")

    # Skip URL fields — not useful in an email
    if field in ("product_sheet_url", "url", "link"):
        return ""

    # Skip if before or after is missing
    if before == "" or after == "":
        return ""

    # If the field is a rate, add % symbol for clarity
    rate_fields = ("base_rate", "total_rate", "fidelity_rate", "rate", "taux")
    if field in rate_fields or "rate" in field.lower():
        return f"    {before}%  →  {after}%"

    # For text values, show them directly
    # But skip if they look like garbled text (long strings with random chars)
    before_str = str(before)
    after_str  = str(after)
    if len(after_str) > 80:   # too long for email
        return ""

    return f"    {before_str}  →  {after_str}"


def extract_highlights(data: dict) -> dict:
    """
    Reads every entry and splits into 3 buckets: HIGH, Medium, low.

    YOUR real JSON has two types of entries:
      - data_type = "products"  → bank product changes (rates, conditions)
      - data_type = "news"      → news articles from lecho.be

    Both are handled here and shown separately in the email.
    """
    high   = []
    medium = []
    low    = []

    for key, entry in data.items():
        cl        = entry.get("classification", {})
        impact    = cl.get("impact", "low")
        data_type = entry.get("data_type", "products")

        # ── Build a clean row depending on data type ──────────────────────
        if data_type == "news":
            # News article — show title and link
            title = entry.get("title", "") or entry.get("before", "")
            title = title.strip()
            if len(title) < 5:   # skip malformed titles like " conditions"
                title = cl.get("description", "")

            row = {
                "type":        "news",
                "title":       title,
                "link":        entry.get("link", ""),
                "description": cl.get("description", ""),
                "source":      entry.get("source", ""),
                "change_type": entry.get("change_type", ""),
                "before_after": "",   # not applicable for news
            }

        else:
            # Product change — show bank, product, before/after values
            row = {
                "type":        "product",
                "bank":        entry.get("bank", "Unknown"),
                "product":     entry.get("product_name", ""),
                "field":       entry.get("field", ""),
                "description": cl.get("description", ""),
                "source":      entry.get("source", ""),
                "change_type": entry.get("change_type", ""),
                "before_after": format_before_after(entry),
            }

        # Put in the right bucket
        if impact == "HIGH":
            high.append(row)
        elif impact == "Medium":
            medium.append(row)
        else:
            low.append(row)

    return {"HIGH": high, "Medium": medium, "low": low}


# ─────────────────────────────────────────────────────────────────────────────
# STEP 3 — Build the email text
# ─────────────────────────────────────────────────────────────────────────────

def build_email_body(highlights: dict, date_str: str, dashboard_url: str = "") -> str:
    """
    Assembles the plain text email from the highlights dict.
    Separates product changes from news articles for clarity.

    The output looks like this:

    ING Market Watch — 2026-05-06
    2 urgent · 2 to monitor · 4 low

    🔴 REQUIRES ATTENTION (2)
    [Product] Argenta e-spaar — Rate change
      Argenta decreased total rate for e-spaar from 2.5% to 0.5%.
      2.5%  →  0.5%

    [News] Marc Raisère (Belfius): "Free money for everyone"
      Belfius announced a €1000 donation to customers.
      🔗 https://www.lecho.be/...

    🟡 MONITOR (2)
    ...
    """
    high   = highlights["HIGH"]
    medium = highlights["Medium"]
    low    = highlights["low"]

    lines = []

    # ── Header ─────────────────────────────────────────────────────────────
    lines.append(f"ING Market Watch — {date_str}")
    lines.append(
        f"{len(high)} urgent · {len(medium)} to monitor · {len(low)} low"
    )
    lines.append("")

    # ── Helper: format one item (product or news) ──────────────────────────
    def format_item(item: dict) -> list[str]:
        item_lines = []
        if item["type"] == "news":
            item_lines.append(f"  [News] {item['title']}")
            item_lines.append(f"    {item['description']}")
            if item.get("link"):
                item_lines.append(f"    🔗 {item['link']}")
        else:
            label = f"  [{item['bank']}] {item['product']}"
            item_lines.append(label)
            item_lines.append(f"    {item['description']}")
            if item.get("before_after"):
                item_lines.append(item["before_after"])
        return item_lines

    # ── HIGH section ────────────────────────────────────────────────────────
    if high:
        lines.append(f"🔴 REQUIRES ATTENTION ({len(high)})")
        for item in high[:MAX_HIGH_SHOWN]:
            lines.extend(format_item(item))
            lines.append("")   # blank line between items
        if len(high) > MAX_HIGH_SHOWN:
            lines.append(f"  ... and {len(high) - MAX_HIGH_SHOWN} more")
            lines.append("")

    # ── MEDIUM section ──────────────────────────────────────────────────────
    if medium:
        lines.append(f"🟡 MONITOR ({len(medium)})")
        for item in medium[:MAX_MEDIUM_SHOWN]:
            lines.extend(format_item(item))
            lines.append("")
        if len(medium) > MAX_MEDIUM_SHOWN:
            lines.append(f"  ... and {len(medium) - MAX_MEDIUM_SHOWN} more")
            lines.append("")

    # ── LOW section (count only, or full list) ──────────────────────────────
    if low:
        if SHOW_LOW_DETAIL:
            lines.append(f"🟢 LOW IMPACT ({len(low)})")
            for item in low:
                lines.extend(format_item(item))
                lines.append("")
        else:
            lines.append(
                f"🟢 LOW IMPACT: {len(low)} minor change(s) — "
                + ("see dashboard for details" if dashboard_url else "check classified file for details")
            )
            lines.append("")

    # ── Dashboard link ──────────────────────────────────────────────────────
    if dashboard_url:
        lines.append(f"👉 Full dashboard: {dashboard_url}")
    else:
        lines.append("💡 No dashboard yet — add DASHBOARD_URL once Streamlit is deployed")
    lines.append("")

    # ── Footer ──────────────────────────────────────────────────────────────
    lines.append("─" * 42)
    lines.append("ING Market Watch — automated daily digest")
    lines.append("Sources: bankshopper.be · kbc.be · lecho.be")

    return "\n".join(lines)


# ─────────────────────────────────────────────────────────────────────────────
# STEP 4 — Send via Gmail
# ─────────────────────────────────────────────────────────────────────────────

def send_email(subject: str, body: str, recipients: list[str]) -> bool:
    """
    Sends a plain text email using Gmail.
    Uses only Python's built-in smtplib — no extra packages needed.
    """
    sender   = os.environ.get("EMAIL_SENDER",   "")
    password = os.environ.get("EMAIL_PASSWORD",  "")

    if not sender or not password:
        print("\n❌ Missing email credentials.")
        print("   Set these environment variables:")
        print("     export EMAIL_SENDER='yourname@gmail.com'")
        print("     export EMAIL_PASSWORD='xxxx xxxx xxxx xxxx'  ← Gmail App Password")
        return False

    msg            = MIMEMultipart()
    msg["From"]    = f"ING Market Watch <{sender}>"
    msg["To"]      = ", ".join(recipients)
    msg["Subject"] = subject
    msg.attach(MIMEText(body, "plain", "utf-8"))

    try:
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
            server.login(sender, password)
            server.sendmail(sender, recipients, msg.as_string())
        print(f"  ✅ Email sent to: {', '.join(recipients)}")
        return True

    except smtplib.SMTPAuthenticationError:
        print("  ❌ Gmail authentication failed.")
        print("     EMAIL_PASSWORD must be an App Password, not your Gmail login password.")
        print("     Get one: Gmail → Account → Security → 2-Step Verification → App Passwords")
        return False

    except smtplib.SMTPRecipientsRefused:
        print(f"  ❌ One or more recipient addresses were rejected: {recipients}")
        return False

    except Exception as e:
        print(f"  ❌ Unexpected error: {e}")
        return False


# ─────────────────────────────────────────────────────────────────────────────
# MAIN — wire everything together
# ─────────────────────────────────────────────────────────────────────────────

def send_digest(
    classified_data: dict  = None,
    date_str: str          = None,
    recipients: list[str]  = None,
    dashboard_url: str     = None,
):
    """
    Full pipeline: load → extract → format → send.

    Can be called in 2 ways:

    Way 1 — standalone (loads latest file automatically):
        send_digest()

    Way 2 — from classify_facts.py (pass data directly, no file search needed):
        from send_digest import send_digest
        send_digest(classified_data=result, date_str=today)

    Arguments (all optional — falls back to env vars):
        classified_data  pass the dict directly from classify_facts.py
        date_str         date string for subject line e.g. "2026-05-06"
        recipients       list of email addresses
        dashboard_url    your Streamlit URL (shown as link in email)
    """
    print("\n── Sending Market Watch digest ──────────────────────")

    # Load data — either passed directly or from latest file
    if classified_data is not None:
        data = classified_data
        if date_str is None:
            date_str = datetime.utcnow().strftime("%Y-%m-%d")
        print(f"  Using data passed directly ({len(data)} entries)")
    else:
        data, date_str = load_latest_file()

    # Read recipients from env var if not passed as argument
    if recipients is None:
        env_rcpt   = os.environ.get("EMAIL_RECIPIENTS", "")
        recipients = [r.strip() for r in env_rcpt.split(",") if r.strip()]

    if not recipients:
        print("  ❌ No recipients. Set EMAIL_RECIPIENTS or pass a list.")
        print("     export EMAIL_RECIPIENTS='one@ing.be,two@ing.be'")
        return

    # Read dashboard URL from env var if not passed
    if dashboard_url is None:
        dashboard_url = os.environ.get("DASHBOARD_URL", "")

    # Build highlights and email text
    highlights = extract_highlights(data)
    body       = build_email_body(highlights, date_str, dashboard_url)
    high_count = len(highlights["HIGH"])
    total      = sum(len(v) for v in highlights.values())
    subject    = f"📊 Market Watch {date_str} — {high_count} urgent, {total} total changes"

    # Always print preview to terminal first
    print("\n" + "=" * 52)
    print("  EMAIL PREVIEW")
    print("=" * 52)
    print(f"  To:      {', '.join(recipients)}")
    print(f"  Subject: {subject}")
    print("─" * 52)
    print(body)
    print("=" * 52)

    # Send
    send_email(subject, body, recipients)
    print("────────────────────────────────────────────────────\n")


# ─────────────────────────────────────────────────────────────────────────────
# Run directly: python send_digest.py
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    send_digest()