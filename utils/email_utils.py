import os
import smtplib

from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from dotenv import load_dotenv

from etl.config import logging

load_dotenv()


def alert_admin(message: str, subject: str = "Alerte ETL") -> bool:
    """
    Sends an alert email to the admin via SMTP.

    Args:
        message: The alert message content
        subject: The email subject line (default: "Alerte Scraping")

    Returns:
        bool: True if email was sent successfully, False otherwise
    """
    logging.error(f"ALERT ADMIN: {message}")
    logging.info(f"Sending alert email to admin...")

    # Get email configuration from environment
    email_config = {
        "sender_email": os.getenv("EMAIL_ADDRESS"),
        "app_password": os.getenv("EMAIL_PASSWORD"),
        "receiver_email": os.getenv("RECIPIENT_EMAIL"),
        "smtp_server": os.getenv("SMTP_SERVER"),
        "smtp_port": os.getenv("SMTP_PORT"),
    }

    # Validate required email configuration
    required_fields = [
        "sender_email",
        "app_password",
        "receiver_email",
        "smtp_server",
        "smtp_port",
    ]
    for field in required_fields:
        if not email_config.get(field):
            logging.warning(f"Missing email configuration: {field}. Alert not sent.")
            return False

    try:
        # Create email message
        msg = MIMEMultipart()
        msg["From"] = email_config["sender_email"]
        msg["To"] = email_config["receiver_email"]
        msg["Subject"] = subject
        msg.attach(MIMEText(message))

        # Send email
        with smtplib.SMTP(
            email_config["smtp_server"], int(email_config["smtp_port"])
        ) as smtp:
            smtp.starttls()
            smtp.login(email_config["sender_email"], email_config["app_password"])
            smtp.send_message(msg)

        logging.info("Alert email sent successfully.")
        return True

    except smtplib.SMTPAuthenticationError:
        logging.error("SMTP authentication failed. Check credentials.")
        return False
    except smtplib.SMTPException as e:
        logging.error(f"SMTP error occurred: {e}")
        return False
    except Exception as e:
        logging.error(f"Failed to send alert email: {e}")
        return False
