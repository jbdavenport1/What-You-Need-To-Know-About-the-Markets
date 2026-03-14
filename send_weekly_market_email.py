import os
import json
import csv
import smtplib

from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication


OUTPUT_DIR = "output"
SUBSCRIBERS_CSV = "advisor_subscribers.csv"

SMTP_HOST = os.getenv("SMTP_HOST")
SMTP_PORT = int(os.getenv("SMTP_PORT", "587"))
SMTP_USERNAME = os.getenv("SMTP_USERNAME")
SMTP_PASSWORD = os.getenv("SMTP_PASSWORD")
EMAIL_FROM = os.getenv("EMAIL_FROM")
EMAIL_TEST_RECIPIENT = os.getenv("EMAIL_TEST_RECIPIENT")


def latest_commentary_json():
    files = [
        f for f in os.listdir(OUTPUT_DIR)
        if f.startswith("commentary_") and f.endswith(".json")
    ]

    if not files:
        raise RuntimeError("No commentary JSON files found.")

    files.sort(reverse=True)
    return os.path.join(OUTPUT_DIR, files[0])


def load_commentary(path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def load_subscribers():
    subscribers = []

    with open(SUBSCRIBERS_CSV, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)

        required = ["advisor_name", "firm_name", "email", "active"]

        for col in required:
            if col not in (reader.fieldnames or []):
                raise RuntimeError(f"Missing column in subscriber CSV: {col}")

        for row in reader:
            if row["active"].strip().lower() != "true":
                continue
            subscribers.append(row)

    return subscribers


def write_deliverable_files(commentary):
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    week = commentary.get("week_ending", "weekly")

    files = {}

    files["client_email"] = os.path.join(OUTPUT_DIR, f"client_email_{week}.txt")
    files["newsletter"] = os.path.join(OUTPUT_DIR, f"newsletter_{week}.txt")
    files["linkedin"] = os.path.join(OUTPUT_DIR, f"linkedin_post_{week}.txt")
    files["talking_points"] = os.path.join(OUTPUT_DIR, f"advisor_talking_points_{week}.txt")

    with open(files["client_email"], "w", encoding="utf-8") as f:
        f.write(commentary.get("client_email", ""))

    newsletter_text = "\n\n".join(
        [
            commentary.get("market_dashboard", {}).get("summary", ""),
            commentary.get("market_summary", ""),
            commentary.get("what_drove_markets", ""),
            commentary.get("under_the_surface", ""),
            commentary.get("investor_implications", ""),
            commentary.get("bottom_line", ""),
        ]
    ).strip()

    with open(files["newsletter"], "w", encoding="utf-8") as f:
        f.write(newsletter_text)

    with open(files["linkedin"], "w", encoding="utf-8") as f:
        f.write(commentary.get("linkedin_post", ""))

    with open(files["talking_points"], "w", encoding="utf-8") as f:
        points = commentary.get("advisor_talking_points", [])
        if isinstance(points, list):
            for p in points:
                f.write(f"- {p}\n")
        else:
            f.write(str(points))

    return files


def chart_files_in_output():
    preferred = [
        "market_dashboard.png",
        "sector_leadership.png",
        "rates_curve.png",
        "breadth_credit.png",
    ]

    found = []

    for name in preferred:
        path = os.path.join(OUTPUT_DIR, name)
        if os.path.exists(path):
            found.append(path)

    return found


def attach_file(msg, path):
    filename = os.path.basename(path)

    with open(path, "rb") as f:
        part = MIMEApplication(f.read(), Name=filename)

    part["Content-Disposition"] = f'attachment; filename="{filename}"'
    msg.attach(part)


def format_talking_points(commentary):
    points = commentary.get("advisor_talking_points", [])

    if isinstance(points, list):
        return "\n".join([f"- {p}" for p in points])

    return str(points)


def format_client_faq(commentary):
    faq = commentary.get("client_faq", [])

    if not isinstance(faq, list) or len(faq) == 0:
        return ""

    lines = []
    for item in faq:
        q = item.get("question", "").strip()
        a = item.get("answer", "").strip()
        if q:
            lines.append(f"Q: {q}")
        if a:
            lines.append(f"A: {a}")
        lines.append("")

    return "\n".join(lines).strip()


def build_email_body(commentary, subscriber, chart_paths):
    advisor_name = subscriber.get("advisor_name", "")
    firm_name = subscriber.get("firm_name", "")
    week_ending = commentary.get("week_ending", "")

    dashboard_summary = commentary.get("market_dashboard", {}).get("summary", "")
    market_summary = commentary.get("market_summary", "")
    what_drove = commentary.get("what_drove_markets", "")
    under_surface = commentary.get("under_the_surface", "")
    investor_implications = commentary.get("investor_implications", "")
    client_email = commentary.get("client_email", "")
    linkedin = commentary.get("linkedin_post", "")
    risk_watch = commentary.get("risk_watch", "")
    bottom_line = commentary.get("bottom_line", "")

    talking_points_text = format_talking_points(commentary)
    faq_text = format_client_faq(commentary)

    chart_list = "\n".join([f"- {os.path.basename(p)}" for p in chart_paths]) if chart_paths else "- None attached"

    body = f"""What You Need To Know About the Markets

Advisor: {advisor_name}
Firm: {firm_name}
Week Ending: {week_ending}

Your weekly deliverables are attached.

Text Deliverables Attached
- Client Email
- Newsletter
- LinkedIn Post
- Advisor Talking Points

Charts Attached
{chart_list}

MARKET DASHBOARD
{dashboard_summary}

MARKET SUMMARY
{market_summary}

WHAT DROVE MARKETS
{what_drove}

UNDER THE SURFACE
{under_surface}

INVESTOR IMPLICATIONS
{investor_implications}

ADVISOR TALKING POINTS
{talking_points_text}

CLIENT EMAIL
{client_email}

LINKEDIN POST
{linkedin}

CLIENT FAQ
{faq_text}

RISK WATCH
{risk_watch}

BOTTOM LINE
{bottom_line}
"""

    return body


def send_messages(subscribers, commentary, deliverable_files, chart_paths):
    if not SMTP_HOST:
        raise RuntimeError("SMTP_HOST missing")
    if not SMTP_USERNAME:
        raise RuntimeError("SMTP_USERNAME missing")
    if not SMTP_PASSWORD:
        raise RuntimeError("SMTP_PASSWORD missing")
    if not EMAIL_FROM:
        raise RuntimeError("EMAIL_FROM missing")

    server = smtplib.SMTP(SMTP_HOST, SMTP_PORT)
    server.starttls()
    server.login(SMTP_USERNAME, SMTP_PASSWORD)

    for sub in subscribers:
        to_addr = EMAIL_TEST_RECIPIENT or sub["email"]

        print(f"Sending email to: {to_addr}")

        msg = MIMEMultipart()
        msg["From"] = EMAIL_FROM
        msg["To"] = to_addr
        msg["Subject"] = f"[{sub['firm_name']}] What You Need To Know About the Markets"

        body = build_email_body(commentary, sub, chart_paths)
        msg.attach(MIMEText(body, "plain", "utf-8"))

        for path in deliverable_files.values():
            attach_file(msg, path)

        for path in chart_paths:
            attach_file(msg, path)

        server.sendmail(EMAIL_FROM, [to_addr], msg.as_string())

        print(f"Email sent to: {to_addr}")

    server.quit()


def main():
    commentary_path = latest_commentary_json()
    commentary = load_commentary(commentary_path)
    subscribers = load_subscribers()
    deliverable_files = write_deliverable_files(commentary)
    chart_paths = chart_files_in_output()

    print(f"Found {len(chart_paths)} chart files.")
    for path in chart_paths:
        print(f"Chart attachment: {path}")

    send_messages(subscribers, commentary, deliverable_files, chart_paths)


if __name__ == "__main__":
    main()

