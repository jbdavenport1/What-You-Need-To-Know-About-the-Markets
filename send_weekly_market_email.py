import os
import json
import csv
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders

OUTPUT_DIR = "output"
SUBSCRIBERS_CSV = "advisor_subscribers.csv"

SMTP_HOST = os.getenv("SMTP_HOST")
SMTP_PORT = int(os.getenv("SMTP_PORT", "587"))
SMTP_USERNAME = os.getenv("SMTP_USERNAME")
SMTP_PASSWORD = os.getenv("SMTP_PASSWORD")
EMAIL_FROM = os.getenv("EMAIL_FROM")
EMAIL_TEST_RECIPIENT = os.getenv("EMAIL_TEST_RECIPIENT")


def latest_commentary_json(output_dir):
    files = [
        f for f in os.listdir(output_dir)
        if f.startswith("commentary_") and f.endswith(".json")
    ]

    if not files:
        raise FileNotFoundError("No commentary JSON files found")

    files.sort(reverse=True)
    return os.path.join(output_dir, files[0])


def load_commentary(path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def load_subscribers(csv_path):

    subscribers = []

    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)

        required = {"advisor_name", "firm_name", "email", "active", "linkedin_enabled"}

        missing = required - set(reader.fieldnames)

        if missing:
            raise ValueError(f"Subscriber CSV missing required columns: {sorted(missing)}")

        for row in reader:

            if row["active"].lower() != "true":
                continue

            subscribers.append(row)

    return subscribers


def write_deliverable_files(commentary, output_dir):

    os.makedirs(output_dir, exist_ok=True)

    week = commentary.get("week_ending", "weekly")

    client_email_path = os.path.join(output_dir, f"client_email_{week}.txt")
    newsletter_path = os.path.join(output_dir, f"newsletter_{week}.txt")
    linkedin_path = os.path.join(output_dir, f"linkedin_post_{week}.txt")
    talking_points_path = os.path.join(output_dir, f"talking_points_{week}.txt")

    with open(client_email_path, "w", encoding="utf-8") as f:
        f.write(commentary.get("client_email", ""))

    with open(newsletter_path, "w", encoding="utf-8") as f:
        f.write(commentary.get("newsletter_version", ""))

    with open(linkedin_path, "w", encoding="utf-8") as f:
        f.write(commentary.get("linkedin_version", ""))

    with open(talking_points_path, "w", encoding="utf-8") as f:
        for line in commentary.get("advisor_talking_points", []):
            f.write(f"- {line}\n")

    return [
        client_email_path,
        newsletter_path,
        linkedin_path,
        talking_points_path,
    ]


def attach_file(msg, path):

    with open(path, "rb") as f:

        part = MIMEBase("application", "octet-stream")
        part.set_payload(f.read())

    encoders.encode_base64(part)

    part.add_header(
        "Content-Disposition",
        f'attachment; filename="{os.path.basename(path)}"',
    )

    msg.attach(part)


def send_messages(subscribers, commentary, attachments):

    if not SMTP_HOST or not SMTP_USERNAME or not SMTP_PASSWORD or not EMAIL_FROM:
        raise RuntimeError(
            "Missing SMTP settings. Check SMTP_HOST, SMTP_USERNAME, SMTP_PASSWORD, and EMAIL_FROM."
        )

    server = smtplib.SMTP(SMTP_HOST, SMTP_PORT)
    server.starttls()
    server.login(SMTP_USERNAME, SMTP_PASSWORD)

    for sub in subscribers:

        to_addr = EMAIL_TEST_RECIPIENT or sub["email"]

        msg = MIMEMultipart()
        msg["From"] = EMAIL_FROM
        msg["To"] = to_addr
        msg["Subject"] = commentary.get("subject", "Weekly Market Commentary")

        body = f"""
Your weekly market commentary is ready.

Attached deliverables:

Client Email
Newsletter Version
LinkedIn Post
Advisor Talking Points

Firm: {sub['firm_name']}
Advisor: {sub['advisor_name']}
"""

        msg.attach(MIMEText(body, "plain"))

        for file in attachments:
            attach_file(msg, file)

        server.sendmail(EMAIL_FROM, to_addr, msg.as_string())

        print(f"Sent email to {to_addr}")

    server.quit()


def main():

    commentary_path = latest_commentary_json(OUTPUT_DIR)

    commentary = load_commentary(commentary_path)

    subscribers = load_subscribers(SUBSCRIBERS_CSV)

    attachments = write_deliverable_files(commentary, OUTPUT_DIR)

    send_messages(subscribers, commentary, attachments)


if __name__ == "__main__":
    main()
