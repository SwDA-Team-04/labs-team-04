import os
import time
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pymongo import MongoClient
from dotenv import load_dotenv
from bson.objectid import ObjectId

# 1. Initialize Configuration
load_dotenv()
client = MongoClient(os.getenv("MONGODB_URI"))
db = client.mzinga  # Connect to the mzinga database


# 5. Recursive function: Serialise Slate AST to HTML string
def slate_to_html(nodes):
    if not nodes:
        return ""
    html = ""
    for node in nodes:
        # Handle text (leaf) nodes
        if 'text' in node:
            text = node['text']
            if node.get('bold'): text = f"<b>{text}</b>"
            if node.get('italic'): text = f"<i>{text}</i>"
            html += text
        # Handle element nodes
        elif 'type' in node:
            children_html = slate_to_html(node.get('children', []))
            tag_map = {
                'paragraph': 'p', 'h1': 'h1', 'h2': 'h2',
                'ul': 'ul', 'li': 'li', 'link': 'a'
            }
            tag = tag_map.get(node['type'], 'div')
            # Handle links specifically
            if node['type'] == 'link':
                url = node.get('url', '#')
                html += f'<a href="{url}">{children_html}</a>'
            else:
                html += f'<{tag}>{children_html}</{tag}>'
    return html


# 4. Resolve recipient email addresses
def resolve_emails(refs):
    emails = []
    if not refs: return []
    for ref in refs:
        # Resolve relationship by querying the users collection
        user = db.users.find_one({"_id": ObjectId(ref['value'])})
        if user and 'email' in user:
            emails.append(user['email'])
    return emails


def run_worker():
    poll_interval = int(os.getenv("POLL_INTERVAL_SECONDS", 5))
    print(f"Worker started. Polling interval: {poll_interval}s...")

    while True:
        # 2 & 3. Poll and Claim the document (Atomic update)
        doc = db.communications.find_one_and_update(
            {"status": "pending"},
            {"$set": {"status": "processing"}},
            sort=[("createdAt", 1)]
        )

        if not doc:
            time.sleep(poll_interval)
            continue

        try:
            print(f"Processing document ID: {doc['_id']}")

            # 4. Resolve recipients
            to_list = resolve_emails(doc.get('tos', []))
            cc_list = resolve_emails(doc.get('ccs', []))
            bcc_list = resolve_emails(doc.get('bccs', []))

            if not to_list:
                raise Exception("No valid recipient addresses found")

            # 5. Serialise body to HTML
            html_content = slate_to_html(doc.get('body', []))

            # 6. Build and send the email via smtplib
            msg = MIMEMultipart()
            msg['Subject'] = doc.get('subject', '(No Subject)')
            msg['From'] = os.getenv("EMAIL_FROM")
            msg['To'] = ", ".join(to_list)
            if cc_list: msg['Cc'] = ", ".join(cc_list)

            msg.attach(MIMEText(html_content, 'html'))

            # Connect to SMTP server (MailHog)
            with smtplib.SMTP(os.getenv("SMTP_HOST"), int(os.getenv("SMTP_PORT"))) as server:
                all_recipients = to_list + cc_list + bcc_list
                server.sendmail(msg['From'], all_recipients, msg.as_string())

            # 7. Write back success result
            db.communications.update_one({"_id": doc['_id']}, {"$set": {"status": "sent"}})
            print(f"Successfully sent to: {to_list}")

        except Exception as e:
            # 7. Write back failed result and log error
            print(f"Failed to process: {str(e)}")
            db.communications.update_one(
                {"_id": doc['_id']},
                {"$set": {"status": "failed", "error": str(e)}}
            )


if __name__ == "__main__":
    run_worker()