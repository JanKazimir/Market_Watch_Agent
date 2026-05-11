import json
import datetime
from pathlib import Path
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

def classify_impact(change):
    ct = change.get("change_type", "")
    if ct == "rate_change":
        diff = abs((change.get("after") or 0) - (change.get("before") or 0))
        return "HIGH" if diff >= 0.1 else "MEDIUM"
    if ct in ("product_added", "product_removed"):
        return "HIGH"
    if ct == "condition_change":
        return "MEDIUM"
    return "LOW"

def format_change(i, change):
    ct = change.get("change_type", "")
    bank = change.get("bank", "")
    product = change.get("product_name", change.get("title", ""))
    source = change.get("source", "")
    before = change.get("before")
    after = change.get("after")
    field = change.get("field", "")

    # Titre
    if ct == "product_added":
        title = f"[{i}] {bank} — {product} (NEW)"
        type_str = "New product added"
        detail = f"Total rate: {after}%"
        tip = "→ New competitor product. Monitor positioning."
    elif ct == "product_removed":
        title = f"[{i}] {bank} — {product}"
        type_str = "Product removed"
        detail = f"Was {before}% total rate — now withdrawn"
        tip = "→ Displaced savers looking for alternatives. Opportunity."
    elif ct == "rate_change":
        title = f"[{i}] {bank} — {product}"
        type_str = "Rate change"
        detail = f"{field}: {before}% → {after}%"
        tip = f"→ {'Increase' if after > before else 'Decrease'} of {round(abs(after - before), 2)}bps."
    elif ct == "condition_change":
        title = f"[{i}] {bank} — {product}"
        type_str = "Condition change"
        detail = f"{field}: {before} → {after}"
        tip = "→ Conditions updated."
    elif ct == "new_article":
        title = f"[{i}] {change.get('title', '')}"
        type_str = "New article"
        detail = change.get("description", "")[:80] + "..."
        tip = f"→ {source}"
    else:
        title = f"[{i}] {bank or ''} — {product}"
        type_str = ct.replace("_", " ").title()
        detail = f"{field}: {before} → {after}"
        tip = ""

    lines = [
        title,
        f"    Type: {type_str}",
        f"    Source: {source}",
        f"    {detail}",
    ]
    if tip:
        lines.append(f"    {tip}")
    return "\n".join(lines)

def build_report(diff_all_path, executive_summary=None):
    data = json.loads(Path(diff_all_path).read_text(encoding="utf-8"))

    date = data.get("date_new", datetime.date.today().isoformat())
    sources = [s["source"] for s in data.get("sources", [])]
    changes = data.get("changes", [])

    high    = [c for c in changes if classify_impact(c) == "HIGH"]
    medium  = [c for c in changes if classify_impact(c) == "MEDIUM"]
    low     = [c for c in changes if classify_impact(c) == "LOW"]

    lines = []

    # Header
    lines.append(f"=== Daily Market Watch — {date} ===")
    lines.append(f"Sources: {', '.join(sources)}")
    lines.append(f"Changes detected: {len(changes)} ({len(high)} High · {len(medium)} Medium · {len(low)} Low)")
    lines.append("\n" + "─" * 40)

    # Executive summary
    lines.append("\nEXECUTIVE SUMMARY\n")
    if executive_summary:
        lines.append(executive_summary)
    else:
        lines.append("(No summary available)")
    lines.append("\n" + "─" * 40)

    # High impact
    if high:
        lines.append("\nHIGH IMPACT\n")
        for i, c in enumerate(high, 1):
            lines.append(format_change(i, c))
            lines.append("")

    # Medium impact
    if medium:
        lines.append("MEDIUM IMPACT\n")
        for i, c in enumerate(medium, len(high) + 1):
            lines.append(format_change(i, c))
            lines.append("")

    # Low impact
    if low:
        lines.append("LOW IMPACT\n")
        for i, c in enumerate(low, len(high) + len(medium) + 1):
            lines.append(format_change(i, c))
            lines.append("")

    return "\n".join(lines)



GMAIL_USER = "gaterafernand@gmail.com"
GMAIL_PASSWORD = "hioa spho duuj iqwz"

RECIPIENTS = ["jan.kazimirowski@gmail.com"]

def send_report(report_text, date):

    subject = f"Daily Market Watch — {date}"

    msg = MIMEMultipart()
    msg["From"] = GMAIL_USER
    msg["To"] = ", ".join(RECIPIENTS)
    msg["Subject"] = subject

    msg.attach(MIMEText(report_text, "plain"))

    try:
        server = smtplib.SMTP("smtp.gmail.com", 587)

        server.starttls()

        server.login(GMAIL_USER, GMAIL_PASSWORD)

        server.sendmail(
            GMAIL_USER,
            RECIPIENTS,
            msg.as_string()
        )

        server.quit()

        print(f"✅ Report envoyé à {RECIPIENTS}")

    except Exception as e:
        print("❌ Erreur SMTP :", e)


# ── Et tu branches les deux ensemble ──────────────────────────────────────────

if __name__ == "__main__":
    date = "2026-05-06"
    report = build_report(f"snapshots/diff_all_{date}.json")
    send_report(report, date)