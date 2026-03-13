import csv
import json
import os
import smtplib
import sys
from dataclasses import dataclass
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from html import escape
from pathlib import Path
from typing import Dict, List, Optional

from branding_layer import BrandingProfile, build_signature_block, safe_color
from compliance_layer import DEFAULT_DISCLAIMER, apply_compliance_filter, save_compliance_report

BASE_DIR = Path(__file__).resolve().parent
OUTPUT_DIR = BASE_DIR / "output"
SUBSCRIBERS_CSV = Path(os.getenv("SUBSCRIBERS_CSV", BASE_DIR / "advisor_subscribers.csv"))

SMTP_HOST = os.getenv("SMTP_HOST", "")
SMTP_PORT = int(os.getenv("SMTP_PORT", "587"))
SMTP_USERNAME = os.getenv("SMTP_USERNAME", "")
SMTP_PASSWORD = os.getenv("SMTP_PASSWORD", "")
SMTP_USE_TLS = os.getenv("SMTP_USE_TLS", "true").lower() == "true"
EMAIL_FROM = os.getenv("EMAIL_FROM", SMTP_USERNAME)
EMAIL_FROM_NAME = os.getenv("EMAIL_FROM_NAME", "What You Need To Know About the Markets")
REPLY_TO = os.getenv("REPLY_TO", EMAIL_FROM)
TEST_MODE = os.getenv("EMAIL_TEST_MODE", "false").lower() == "true"
TEST_RECIPIENT = os.getenv("EMAIL_TEST_RECIPIENT", "")
GLOBAL_DISCLAIMER = os.getenv("EMAIL_DISCLAIMER", DEFAULT_DISCLAIMER)


@dataclass
class Subscriber:
    email: str
    advisor_name: str = ""
    firm_name: str = ""
    active: bool = True
    linkedin_enabled: bool = True
    sender_display_name: str = ""
    brand_primary_color: str = "#0B1F3A"
    brand_secondary_color: str = "#F4F6F8"
    logo_url: str = ""
    website_url: str = ""
    phone: str = ""
    email_signature: str = ""
    custom_disclaimer: str = ""
    subject_prefix: str = ""
    cta_text: str = ""

    def branding_profile(self) -> BrandingProfile:
        return BrandingProfile(
            advisor_name=self.advisor_name,
            firm_name=self.firm_name,
            sender_display_name=self.sender_display_name,
            brand_primary_color=safe_color(self.brand_primary_color, "#0B1F3A"),
            brand_secondary_color=safe_color(self.brand_secondary_color, "#F4F6F8"),
            logo_url=self.logo_url,
            website_url=self.website_url,
            phone=self.phone,
            email_signature=self.email_signature,
            custom_disclaimer=self.custom_disclaimer,
            subject_prefix=self.subject_prefix,
            cta_text=self.cta_text,
        )


def load_subscribers(path: Path) -> List[Subscriber]:
    if not path.exists():
        raise FileNotFoundError(f"Subscriber file not found: {path}")

    subscribers: List[Subscriber] = []
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        required = {"email", "advisor_name", "firm_name", "active", "linkedin_enabled"}
        missing = required - set(reader.fieldnames or [])
        if missing:
            raise ValueError(f"Subscriber CSV missing required columns: {sorted(missing)}")
        for row in reader:
            email = (row.get("email") or "").strip()
            if not email:
                continue
            subscribers.append(
                Subscriber(
                    email=email,
                    advisor_name=(row.get("advisor_name") or "").strip(),
                    firm_name=(row.get("firm_name") or "").strip(),
                    active=(row.get("active") or "true").strip().lower() == "true",
                    linkedin_enabled=(row.get("linkedin_enabled") or "true").strip().lower() == "true",
                    sender_display_name=(row.get("sender_display_name") or "").strip(),
                    brand_primary_color=(row.get("brand_primary_color") or "#0B1F3A").strip(),
                    brand_secondary_color=(row.get("brand_secondary_color") or "#F4F6F8").strip(),
                    logo_url=(row.get("logo_url") or "").strip(),
                    website_url=(row.get("website_url") or "").strip(),
                    phone=(row.get("phone") or "").strip(),
                    email_signature=(row.get("email_signature") or "").strip(),
                    custom_disclaimer=(row.get("custom_disclaimer") or "").strip(),
                    subject_prefix=(row.get("subject_prefix") or "").strip(),
                    cta_text=(row.get("cta_text") or "").strip(),
                )
            )
    return [s for s in subscribers if s.active]


def latest_commentary_json(output_dir: Path) -> Path:
    candidates = sorted(output_dir.glob("commentary_*.json"))
    if not candidates:
        raise FileNotFoundError(f"No commentary JSON files found in {output_dir}")
    return candidates[-1]


def latest_market_csv(output_dir: Path) -> Optional[Path]:
    candidates = sorted(output_dir.glob("weekly_market_packet_*.csv"))
    return candidates[-1] if candidates else None


def load_commentary(path: Path) -> Dict[str, object]:
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def extract_week_ending(commentary_path: Path) -> str:
    stem = commentary_path.stem
    if stem.startswith("commentary_"):
        return stem.replace("commentary_", "")
    return ""


def build_html_email(content: Dict[str, object], week_ending: str, subscriber: Subscriber) -> str:
    profile = subscriber.branding_profile()
    body = escape(str(content.get("client_email_body", ""))).replace("\n", "<br>")
    newsletter = escape(str(content.get("newsletter_body", ""))).replace("\n", "<br>")
    linkedin = escape(str(content.get("linkedin_post", ""))).replace("\n", "<br>")
    talking_points = content.get("advisor_talking_points", []) or []
    talking_points_html = "".join(f"<li>{escape(str(point))}</li>" for point in talking_points)

    greeting = subscriber.advisor_name or "Advisor"
    linkedin_section = ""
    if subscriber.linkedin_enabled:
        linkedin_section = (
            f"<h3 style='margin-bottom:8px;color:{profile.brand_primary_color};'>LinkedIn Post</h3>"
            f"<p>{linkedin}</p>"
        )

    logo_html = ""
    if profile.logo_url:
        logo_html = (
            f'<img src="{escape(profile.logo_url)}" alt="Logo" '
            f'style="max-height:48px;max-width:180px;display:block;margin-bottom:12px;">'
        )

    signature_html = build_signature_block(profile)
    disclaimer = escape(subscriber.custom_disclaimer or str(content.get("compliance_disclaimer") or GLOBAL_DISCLAIMER))

    return f"""
    <html>
      <body style="font-family: Arial, sans-serif; line-height: 1.55; color: #111; background:{profile.brand_secondary_color}; padding: 24px;">
        <div style="max-width: 800px; margin: 0 auto; background: #fff; border: 1px solid #d9e0e7; border-radius: 12px; overflow: hidden;">
          <div style="background:{profile.brand_primary_color}; color:#fff; padding:24px;">
            {logo_html}
            <h2 style="margin:0 0 4px 0;">What You Need To Know About the Markets</h2>
            <p style="margin:0; opacity:.92;"><strong>Week ending:</strong> {escape(week_ending)}</p>
          </div>
          <div style="padding: 24px;">
            <p>Hello {escape(greeting)},</p>
            <p>Your weekly market commentary package is ready.</p>
            <h3 style="margin-bottom:8px;color:{profile.brand_primary_color};">Client Email</h3>
            <p>{body}</p>
            <h3 style="margin-bottom:8px;color:{profile.brand_primary_color};">Newsletter Version</h3>
            <p>{newsletter}</p>
            {linkedin_section}
            <h3 style="margin-bottom:8px;color:{profile.brand_primary_color};">Advisor Talking Points</h3>
            <ul>{talking_points_html}</ul>
            <p style="margin-top:20px;">{escape(profile.resolved_cta_text)}</p>
            <div style="margin-top:24px; padding-top:16px; border-top:1px solid #e6ebf0; font-size:14px; color:#334155;">{signature_html}</div>
          </div>
          <div style="padding: 16px 24px; background:#f8fafc; border-top:1px solid #e6ebf0; font-size:12px; color:#475569;">{disclaimer}</div>
        </div>
      </body>
    </html>
    """.strip()


def build_plaintext_email(content: Dict[str, object], week_ending: str, subscriber: Subscriber) -> str:
    profile = subscriber.branding_profile()
    lines: List[str] = []
    lines.append("What You Need To Know About the Markets")
    lines.append(f"Week ending: {week_ending}")
    lines.append("")
    lines.append(f"Hello {subscriber.advisor_name or 'Advisor'},")
    lines.append("")
    lines.append("Your weekly market commentary package is ready.")
    if subscriber.firm_name:
        lines.append(f"Firm: {subscriber.firm_name}")
    lines.append("")
    lines.append("CLIENT EMAIL")
    lines.append(str(content.get("client_email_body", "")))
    lines.append("")
    lines.append("NEWSLETTER VERSION")
    lines.append(str(content.get("newsletter_body", "")))
    if subscriber.linkedin_enabled:
        lines.append("")
        lines.append("LINKEDIN POST")
        lines.append(str(content.get("linkedin_post", "")))
    lines.append("")
    lines.append("ADVISOR TALKING POINTS")
    for point in content.get("advisor_talking_points", []) or []:
        lines.append(f"- {point}")
    lines.append("")
    lines.append(profile.resolved_cta_text)
    lines.append("")
    if subscriber.email_signature:
        lines.append(subscriber.email_signature)
    else:
        if subscriber.advisor_name:
            lines.append(subscriber.advisor_name)
        if subscriber.firm_name:
            lines.append(subscriber.firm_name)
        if subscriber.website_url:
            lines.append(subscriber.website_url)
        if subscriber.phone:
            lines.append(subscriber.phone)
    lines.append("")
    lines.append(subscriber.custom_disclaimer or str(content.get("compliance_disclaimer") or GLOBAL_DISCLAIMER))
    return "\n".join(lines)


def build_message(subscriber: Subscriber, subject: str, html_body: str, text_body: str) -> MIMEMultipart:
    profile = subscriber.branding_profile()
    message = MIMEMultipart("alternative")
    final_subject = subject
    if profile.resolved_subject_prefix:
        final_subject = f"[{profile.resolved_subject_prefix}] {subject}"
    message["Subject"] = final_subject
    from_name = profile.resolved_sender_name or EMAIL_FROM_NAME
    message["From"] = f"{from_name} <{EMAIL_FROM}>" if from_name else EMAIL_FROM
    message["To"] = subscriber.email
    message["Reply-To"] = REPLY_TO
    message.attach(MIMEText(text_body, "plain", "utf-8"))
    message.attach(MIMEText(html_body, "html", "utf-8"))
    return message


def send_messages(messages: List[MIMEMultipart]) -> None:
    if not SMTP_HOST or not SMTP_USERNAME or not SMTP_PASSWORD:
        raise RuntimeError("Missing SMTP settings. Check SMTP_HOST, SMTP_USERNAME, SMTP_PASSWORD, and EMAIL_FROM.")
    with smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=60) as server:
        server.ehlo()
        if SMTP_USE_TLS:
            server.starttls()
            server.ehlo()
        server.login(SMTP_USERNAME, SMTP_PASSWORD)
        for message in messages:
            server.sendmail(EMAIL_FROM, [message["To"]], message.as_string())


def main() -> int:
    commentary_path = latest_commentary_json(OUTPUT_DIR)
    market_csv_path = latest_market_csv(OUTPUT_DIR)
    content = load_commentary(commentary_path)
    content = apply_compliance_filter(content, disclaimer=GLOBAL_DISCLAIMER)
    week_ending = extract_week_ending(commentary_path)
    save_compliance_report(OUTPUT_DIR / f"compliance_report_{week_ending}.json", content)

    subject = str(content.get("client_email_subject") or "What You Need To Know About the Markets")
    subscribers = load_subscribers(SUBSCRIBERS_CSV)

    if TEST_MODE:
        if not TEST_RECIPIENT:
            raise RuntimeError("EMAIL_TEST_MODE is true, but EMAIL_TEST_RECIPIENT is empty.")
        subscribers = [
            Subscriber(
                email=TEST_RECIPIENT,
                advisor_name="Test Advisor",
                firm_name="Test Firm",
                sender_display_name="Test Firm",
            )
        ]

    if not subscribers:
        raise RuntimeError("No active subscribers found.")

    messages: List[MIMEMultipart] = []
    for subscriber in subscribers:
        html_body = build_html_email(content, week_ending, subscriber)
        text_body = build_plaintext_email(content, week_ending, subscriber)
        messages.append(build_message(subscriber, subject, html_body, text_body))

    send_messages(messages)

    print("Email send complete.")
    print(f"COMMENTARY_JSON: {commentary_path}")
    if market_csv_path:
        print(f"MARKET_CSV: {market_csv_path}")
    print(f"RECIPIENT_COUNT: {len(messages)}")
    print(f"TEST_MODE: {TEST_MODE}")
    print(f"COMPLIANCE_REPORT: {OUTPUT_DIR / f'compliance_report_{week_ending}.json'}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
