import os
import smtplib
import gzip
from email.message import EmailMessage
from typing import List, Tuple, Optional

Attachment = Tuple[str, bytes, str]  # (filename, content_bytes, mime_type)

def send_email(subject: str, body: str, attachments: Optional[List[Attachment]] = None) -> None:
    smtp_host = os.environ["SMTP_HOST"]
    smtp_port = int(os.environ.get("SMTP_PORT", "587"))
    smtp_user = os.environ["SMTP_USER"]
    smtp_pass = os.environ["SMTP_PASS"]
    to_email = os.environ["REPORT_EMAIL_TO"]

    msg = EmailMessage()
    msg["From"] = smtp_user
    msg["To"] = to_email
    msg["Subject"] = subject
    msg.set_content(body)

    attachments = attachments or []
    for filename, content, mime in attachments:
        maintype, subtype = mime.split("/", 1)
        msg.add_attachment(content, maintype=maintype, subtype=subtype, filename=filename)

    with smtplib.SMTP(smtp_host, smtp_port, timeout=60) as s:
        s.starttls()
        s.login(smtp_user, smtp_pass)
        s.send_message(msg)

def maybe_gzip_csv(name: str, csv_bytes: bytes, threshold_bytes: int = 4_500_000) -> Attachment:
    """
    If CSV is big, gzip it so email doesn't fail.
    (Many providers reject >10–25MB; gzip helps a lot.)
    """
    if len(csv_bytes) < threshold_bytes:
        return (name, csv_bytes, "text/csv")

    gz_name = name + ".gz"
    gz_bytes = gzip.compress(csv_bytes, compresslevel=6)
    return (gz_name, gz_bytes, "application/gzip")
