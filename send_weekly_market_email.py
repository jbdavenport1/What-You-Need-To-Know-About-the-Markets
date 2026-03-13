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


def latest_commentary_json(output_dir):
    files = [
        f for f in os.listdir(output_dir)
        if f.startswith("commentary_") and f.endswith(".json")
    ]

    if not files:
        raise FileNotFoundError(f"No commentary JSON files found in {output_dir}")

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
        missing = required - set(reader.fieldnames or [])

        if missing:
            raise ValueError(f"Subscriber CSV missing required columns: {sorted(missing)}")

        for row in reader:
            if row["active"].strip().lower() != "true":
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
        f.write(commentary.get("market_summary", ""))

    with open(linkedin_path, "w", encoding="utf-8") as f:
        f.write(commentary.get("linkedin_post", ""))

    with open(talking_points_path, "w", encoding="utf-8") as f:
        talking_points = commentary.get("advisor_talking_points", [])
        if isinstance(talking_points, list):
            for line in talking_points:
                f.write(f"- {line}\n")
        else:
            f.write(str(talking_points))

    return [
        client_email_path,
        newsletter_path,
        linkedin_path,
        talking_points_path,
    ]


def attach_file(msg, path):
    filename = os.path.basename(path)

    if path.endswith(".txt"):
        with open(path, "r", encoding="utf-8") as f:
            part = MIMEText(f.read(), "plain", "utf-8")
        part.add_header("Content-Disposition", f'attachment; filename="{filename}"')
    else:
        with open(path, "rb") as f:
            part = MIMEApplication(f.read(), Name=filename)
        part.add_header("Content-Disposition", f'attachment; filename="{filename}"')

    msg.attach(part)


def build_email_body(commentary, subscriber):
    advisor_name = subscriber.get("advisor_name", "")
    firm_name = subscriber.get("firm_name", "")

    client_email = commentary.get("client_email", "").strip()
    market_summary = commentary.get("market_summary", "").strip()
    linkedin_post = commentary.get("linkedin_post", "").strip()
    talking_points = commentary.get("advisor_talking_points", [])

    if isinstance(talking_points, list):
        talking_points_text = "\n".join([f"- {item}" for item in talking_points])
    else:
        talking_points_text = str(talking_points)

    body = f"""What You Need To Know About the Markets

Weekly deliverables are ready.

Advisor: {advisor_name}
Firm: {firm_name}
Week Ending: {commentary.get("week_ending", "")}

Attached files:
- Client Email
- Newsletter
- LinkedIn Post
- Advisor Talking Points

Inline preview below.

CLIENT EMAIL
{client_email}

NEWSLETTER VERSION
{market_summary}

LINKEDIN POST
{linkedin_post}

ADVISOR TALKING POINTS
{talking_points_text}
"""

    return body


def send_messages(subscribers, commentary, attachments):
    if not SMTP_HOST or not SMTP_USERNAME or not SMTP_PASSWORD or not EMAIL_FROM:
        raise RuntimeError("Missing SMTP settings. Check SMTP_HOST, SMTP_USERNAME, SMTP_PASSWORD, and EMAIL_FROM.")

    server = smtplib.SMTP(SMTP_HOST, SMTP_PORT)
    server.starttls()
    server.login(SMTP_USERNAME, SMTP_PASSWORD)

    for sub in subscribers:
        to_addr = EMAIL_TEST_RECIPIENT or sub["email"]

        subject = f"[{sub['firm_name']}] What You Need To Know About the Markets"

        msg = MIMEMultipart()
        msg["From"] = f"{sub['firm_name']} <{EMAIL_FROM}>"
        msg["To"] = to_addr
        msg["Subject"] = subject
        msg["Reply-To"] = EMAIL_FROM

        body = build_email_body(commentary, sub)
        msg.attach(MIMEText(body, "plain", "utf-8"))

        for file in attachments:
            attach_file(msg, file)

        server.sendmail(EMAIL_FROM, [to_addr], msg.as_string())
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
