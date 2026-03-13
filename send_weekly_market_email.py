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

        for r in required:
            if r not in reader.fieldnames:
                raise RuntimeError(f"Missing column in subscriber CSV: {r}")

        for row in reader:

            if row["active"].lower() != "true":
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

    with open(files["newsletter"], "w", encoding="utf-8") as f:
        f.write(commentary.get("market_summary", ""))

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


def attach_file(msg, path):

    filename = os.path.basename(path)

    with open(path, "rb") as f:
        part = MIMEApplication(f.read(), Name=filename)

    part["Content-Disposition"] = f'attachment; filename="{filename}"'

    msg.attach(part)


def build_email_body(commentary, subscriber):

    client_email = commentary.get("client_email", "")
    newsletter = commentary.get("market_summary", "")
    linkedin = commentary.get("linkedin_post", "")

    points = commentary.get("advisor_talking_points", [])

    if isinstance(points, list):
        points_text = "\n".join([f"- {p}" for p in points])
    else:
        points_text = str(points)

    body = f"""
What You Need To Know About the Markets

Advisor: {subscriber.get("advisor_name")}
Firm: {subscriber.get("firm_name")}
Week Ending: {commentary.get("week_ending")}

Your weekly deliverables are attached.

CLIENT EMAIL
{client_email}

NEWSLETTER VERSION
{newsletter}

LINKEDIN POST
{linkedin}

ADVISOR TALKING POINTS
{points_text}
"""

    return body


def send_messages(subscribers, commentary, files):

    if not SMTP_HOST:
        raise RuntimeError("SMTP_HOST missing")

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

        body = build_email_body(commentary, sub)

        msg.attach(MIMEText(body, "plain", "utf-8"))

        for f in files.values():
            attach_file(msg, f)

        server.sendmail(EMAIL_FROM, [to_addr], msg.as_string())

        print(f"Email sent to: {to_addr}")

    server.quit()


def main():

    commentary_path = latest_commentary_json()

    commentary = load_commentary(commentary_path)

    subscribers = load_subscribers()

    files = write_deliverable_files(commentary)

    send_messages(subscribers, commentary, files)


if __name__ == "__main__":
    main()
