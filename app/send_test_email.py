import os, smtplib
from email.message import EmailMessage
from pathlib import Path
from dotenv import load_dotenv

load_dotenv(Path(__file__).with_name(".env"))
SMTP_HOST = os.getenv("SMTP_HOST")
SMTP_PORT = int(os.getenv("SMTP_PORT", "587"))
SMTP_STARTTLS = os.getenv("SMTP_STARTTLS", "true").lower() == "true"
SMTP_USER = os.getenv("SMTP_USER")
SMTP_PASS = os.getenv("SMTP_PASS")
SMTP_TO = [e.strip() for e in os.getenv("SMTP_TO","").split(";") if e.strip()]

msg = EmailMessage()
msg["From"] = SMTP_USER
msg["To"] = ", ".join(SMTP_TO)
msg["Subject"] = "FinTx SMTP test"
msg.set_content("Hello from FinTx! If you received this, SMTP is configured.")

if SMTP_STARTTLS:
    with smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=30) as s:
        s.starttls()
        s.login(SMTP_USER, SMTP_PASS)
        s.send_message(msg)
else:
    import smtplib as _s
    with _s.SMTP_SSL(SMTP_HOST, SMTP_PORT, timeout=30) as s:
        s.login(SMTP_USER, SMTP_PASS)
        s.send_message(msg)

print("Test email sent.")
