import logging
import os
from datetime import datetime

from src.config import AppConfig

logger = logging.getLogger(__name__)


class EmailSender:
    def __init__(self, config: AppConfig):
        self.config = config
        self.api_key = os.environ.get("SENDGRID_API_KEY")

    def send(self, html_content: str, new_count: int = 0) -> int:
        """Send the newsletter to all configured recipients. Returns count of emails sent."""
        if not self.api_key:
            logger.warning("SENDGRID_API_KEY not set. Skipping email send.")
            # Save HTML locally for preview
            preview_path = "data/latest_newsletter.html"
            with open(preview_path, "w") as f:
                f.write(html_content)
            logger.info(f"Newsletter HTML saved to {preview_path} for preview")
            return 0

        if not self.config.newsletter.recipients:
            logger.warning("No recipients configured. Skipping email send.")
            return 0

        import sendgrid
        from sendgrid.helpers.mail import Content, Email, Mail, To

        sg = sendgrid.SendGridAPIClient(api_key=self.api_key)

        now = datetime.now()
        subject = self.config.newsletter.subject_template.format(
            new_count=new_count,
            month=now.strftime("%B"),
            year=now.year,
        )

        sent = 0
        for recipient in self.config.newsletter.recipients:
            try:
                message = Mail(
                    from_email=Email(self.config.newsletter.from_email, self.config.newsletter.from_name),
                    to_emails=To(recipient),
                    subject=subject,
                    html_content=Content("text/html", html_content),
                )
                response = sg.send(message)
                logger.info(f"Email sent to {recipient}: status {response.status_code}")
                sent += 1
            except Exception:
                logger.exception(f"Failed to send email to {recipient}")

        return sent
