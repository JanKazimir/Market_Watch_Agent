import smtplib
import os
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

def send_daily_summary(summary_data: list, recipient_emails: list[str]) -> None:
    """
    Sends a recap of today's classified facts to the team.
    
    Args:
        summary_data (list): List of dicts containing 'title' and 'impact'.
        recipient_emails (list): List of email addresses.
    """
    # 1. Setup credentials (Using Environment Variables for security)
    sender_email = os.environ.get("htien.hoang56@gmail.com")
    app_password = os.environ.get("swsqbjzywpgsnxyy") # Gmail App Password
    
    if not sender_email or not app_password:
        print("Error: Email credentials not found in environment variables.")
        return

    # 2. Build the Email Content
    subject = f"🚀 Daily Market Highlights - {len(summary_data)} New Updates"
    
    # Create a simple text recap
    body = "Hello Team,\n\nHere are the highlights from today's market classification:\n\n"
    for item in summary_data:
        impact_emoji = "🔴" if item.get('impact') == "HIGH" else "🟡"
        body += f"{impact_emoji} {item.get('title')}\n"
    
    body += "\n👉 View full details on the Dashboard: https://your-streamlit-link.com"
    body += "\n\nBest regards,\nMarket Watch Bot"

    # 3. Assemble and Send
    msg = MIMEMultipart()
    msg['From'] = f"Market Watch <{sender_email}>"
    msg['To'] = ", ".join(recipient_emails)
    msg['Subject'] = subject
    msg.attach(MIMEText(body, 'plain'))

    try:
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
            server.login(sender_email, app_password)
            server.sendmail(sender_email, recipient_emails, msg.as_string())
        print("✅ Summary email sent successfully!")
    except Exception as e:
        print(f"❌ Failed to send email: {e}")