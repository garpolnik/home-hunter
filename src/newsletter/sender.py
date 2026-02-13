import logging
import os
import smtplib
import ssl
from datetime import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

from src.config import AppConfig

logger = logging.getLogger(__name__)


class EmailSender:
    def __init__(self, config: AppConfig):
        self.config = config
        self.smtp_host = os.environ.get("SMTP_HOST", "mail.privateemail.com")
        self.smtp_port = int(os.environ.get("SMTP_PORT", "465"))
        self.smtp_user = os.environ.get("SMTP_USER", "")
        self.smtp_password = os.environ.get("SMTP_PASSWORD", "")

    def send(self, html_content: str, new_count: int = 0, db_recipients: list[str] | None = None) -> int:
        """Send the newsletter to all configured + approved DB recipients. Returns count of emails sent."""
        if not self.smtp_user or not self.smtp_password:
            logger.warning("SMTP credentials not set. Skipping email send.")
            preview_path = "data/latest_newsletter.html"
            with open(preview_path, "w") as f:
                f.write(html_content)
            logger.info(f"Newsletter HTML saved to {preview_path} for preview")
            return 0

        # Merge config recipients with approved DB subscribers, deduplicated
        all_recipients = list(dict.fromkeys(
            self.config.newsletter.recipients + (db_recipients or [])
        ))

        if not all_recipients:
            logger.warning("No recipients configured. Skipping email send.")
            return 0

        logger.info(f"Sending newsletter to {len(all_recipients)} recipients")

        now = datetime.now()
        subject = self.config.newsletter.subject_template.format(
            new_count=new_count,
            month=now.strftime("%B"),
            year=now.year,
        )

        sent = 0
        context = ssl.create_default_context()

        try:
            if self.smtp_port == 465:
                server = smtplib.SMTP_SSL(self.smtp_host, self.smtp_port, context=context)
            else:
                server = smtplib.SMTP(self.smtp_host, self.smtp_port)
                server.starttls(context=context)
            server.login(self.smtp_user, self.smtp_password)
        except Exception:
            logger.exception("Failed to connect to SMTP server")
            return 0

        try:
            for recipient in all_recipients:
                try:
                    msg = MIMEMultipart("alternative")
                    msg["Subject"] = subject
                    msg["From"] = f"{self.config.newsletter.from_name} <{self.config.newsletter.from_email}>"
                    msg["To"] = recipient
                    msg.attach(MIMEText(html_content, "html"))

                    server.sendmail(self.config.newsletter.from_email, recipient, msg.as_string())
                    logger.info(f"Email sent to {recipient}")
                    sent += 1
                except Exception:
                    logger.exception(f"Failed to send email to {recipient}")
        finally:
            server.quit()

        return sent
