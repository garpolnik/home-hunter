import logging
import os
from datetime import datetime

from src.config import AppConfig

logger = logging.getLogger(__name__)


class SendGridSender:
    """Send newsletter emails via the SendGrid API."""

    def __init__(self, config: AppConfig):
        self.config = config
        self.api_key = os.environ.get("SENDGRID_API_KEY", "")
        self.from_email = config.newsletter.from_email
        self.from_name = config.newsletter.from_name

    def send(
        self,
        html_content: str,
        recipient: str,
        new_count: int = 0,
        map_url: str = "",
    ) -> int:
        """Send a newsletter email to a single recipient via SendGrid.

        Returns 1 on success, 0 on failure.
        """
        if not self.api_key:
            logger.warning("SENDGRID_API_KEY not set. Skipping email send.")
            return 0

        try:
            from sendgrid import SendGridAPIClient
            from sendgrid.helpers.mail import Mail
        except ImportError:
            logger.error("sendgrid package not installed. Run: pip install sendgrid")
            return 0

        now = datetime.now()
        subject = self.config.newsletter.subject_template.format(
            new_count=new_count,
            month=now.strftime("%B"),
            year=now.year,
        )

        # Inject "View Your Map" button before closing </body> tag
        if map_url:
            map_link_html = (
                '<div style="text-align:center;margin:20px 0;">'
                f'<a href="{map_url}" style="background:#3182ce;color:white;'
                'padding:12px 24px;border-radius:6px;text-decoration:none;'
                'font-weight:bold;">View Your Interactive Map</a></div>'
            )
            html_content = html_content.replace("</body>", f"{map_link_html}</body>")

        message = Mail(
            from_email=(self.from_email, self.from_name),
            to_emails=recipient,
            subject=subject,
            html_content=html_content,
        )

        try:
            sg = SendGridAPIClient(api_key=self.api_key)
            response = sg.send(message)
            if response.status_code in (200, 201, 202):
                logger.info(f"Email sent to {recipient} via SendGrid (status {response.status_code})")
                return 1
            else:
                logger.error(f"SendGrid returned status {response.status_code} for {recipient}")
                return 0
        except Exception:
            logger.exception(f"SendGrid send failed for {recipient}")
            return 0

    def send_welcome(self, recipient: str, dashboard_url: str) -> int:
        """Send a welcome email to a newly approved user with their private link."""
        if not self.api_key:
            logger.warning("SENDGRID_API_KEY not set. Skipping welcome email.")
            return 0

        try:
            from sendgrid import SendGridAPIClient
            from sendgrid.helpers.mail import Mail
        except ImportError:
            logger.error("sendgrid package not installed")
            return 0

        subject = "Welcome to Home Deal Finder!"
        html = f"""\
<html><body style="font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;margin:0;padding:0;background:#f4f4f4;">
<div style="max-width:600px;margin:0 auto;background:#fff;border-radius:8px;overflow:hidden;">
  <div style="background:#1a365d;color:white;padding:24px 32px;">
    <h1 style="margin:0;font-size:24px;">Home Deal Finder</h1>
    <p style="margin:8px 0 0;opacity:0.85;">Your search has been approved!</p>
  </div>
  <div style="padding:32px;">
    <p>Your personalized home search is now active. We're running your first scan now.</p>
    <p>Bookmark your private dashboard to check for updates anytime:</p>
    <div style="text-align:center;margin:24px 0;">
      <a href="{dashboard_url}" style="background:#3182ce;color:white;padding:14px 28px;
        border-radius:6px;text-decoration:none;font-weight:bold;font-size:16px;">
        View Your Dashboard</a>
    </div>
    <p style="color:#718096;font-size:13px;">This link is private to you. Don't share it publicly.</p>
    <p style="color:#718096;font-size:13px;">You'll also receive monthly email updates with new listings
       matching your criteria.</p>
  </div>
  <div style="background:#f7fafc;padding:16px 32px;text-align:center;font-size:12px;color:#a0aec0;border-top:1px solid #e2e8f0;">
    Home Deal Finder &mdash; For personal use only
  </div>
</div>
</body></html>"""

        message = Mail(
            from_email=(self.from_email, self.from_name),
            to_emails=recipient,
            subject=subject,
            html_content=html,
        )

        try:
            sg = SendGridAPIClient(api_key=self.api_key)
            response = sg.send(message)
            if response.status_code in (200, 201, 202):
                logger.info(f"Welcome email sent to {recipient}")
                return 1
            else:
                logger.error(f"SendGrid welcome email status {response.status_code} for {recipient}")
                return 0
        except Exception:
            logger.exception(f"SendGrid welcome email failed for {recipient}")
            return 0
