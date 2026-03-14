from __future__ import annotations

import os
import smtplib
from email.message import EmailMessage
from pathlib import Path

from dotenv import load_dotenv


load_dotenv()

OUTPUT_DIR = Path("output")

DOCX_PATH = OUTPUT_DIR / "weekly_market_packet.docx"
TXT_PATH = OUTPUT_DIR / "weekly_market_packet.txt"

CHART_PATHS = [
    OUTPUT_DIR / "spx_trend.png",
    OUTPUT_DIR / "yield_curve.png",
    OUTPUT_DIR / "credit_spreads.png",
]

SMTP_SERVER = os.getenv("SMTP_SERVER", "").strip()
SMTP_PORT = int(os.getenv("SMTP_PORT", "587"))
SMTP_USERNAME = os.getenv("SMTP_USERNAME", "").strip()
SMTP_PASSWORD = os.getenv("SMTP_PASSWORD", "").strip()
EMAIL_FROM = os.getenv("EMAIL_FROM", SMTP_USERNAME).strip()
EMAIL_TO = os.getenv("EMAIL_TO", "").strip()


def attach_file(msg: EmailMessage, path: Path, mime_main: str, mime_sub: str) -> None:
    if not path.exists():
        print(f"[WARN] File not found, skipping attachment: {path}")
        return

    with open(path, "rb") as f:
        data = f.read()

    msg.add_attachment(
        data,
        maintype=mime_main,
        subtype=mime_sub,
        filename=path.name,
    )


def build_email_body() -> str:
    return (
        "Attached is this week's market packet in DOCX format, along with the chart files.\n\n"
        "Included sections:\n"
        "- Executive Summary\n"
        "- Market Overview\n"
        "- Equity Market Trends\n"
        "- Rates and Macro Backdrop\n"
        "- Institutional Signals\n"
        "- Top Risks\n"
        "- Closing Takeaways\n\n"
        "Best,\n"
        "Weekly Market Packet Automation"
    )


def send_email() -> None:
    if not SMTP_SERVER or not SMTP_USERNAME or not SMTP_PASSWORD or not EMAIL_FROM or not EMAIL_TO:
        raise ValueError("Missing SMTP or email environment variables.")

    if not DOCX_PATH.exists():
        raise FileNotFoundError(f"DOCX file not found: {DOCX_PATH}")

    msg = EmailMessage()
    msg["Subject"] = "Weekly Market Packet"
    msg["From"] = EMAIL_FROM
    msg["To"] = EMAIL_TO
    msg.set_content(build_email_body())

    attach_file(
        msg,
        DOCX_PATH,
        "application",
        "vnd.openxmlformats-officedocument.wordprocessingml.document",
    )

    if TXT_PATH.exists():
        attach_file(msg, TXT_PATH, "text", "plain")

    for chart_path in CHART_PATHS:
        if chart_path.exists():
            attach_file(msg, chart_path, "image", "png")

    with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
        server.starttls()
        server.login(SMTP_USERNAME, SMTP_PASSWORD)
        server.send_message(msg)

    print("[OK] Weekly market email sent.")


def main() -> None:
    print("[INFO] Sending weekly market email...")
    send_email()


if __name__ == "__main__":
    main()
