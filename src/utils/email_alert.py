"""Email alert service for schema drift notifications."""
from __future__ import annotations

import logging
import os
import smtplib
import textwrap
from datetime import datetime, timezone
from email.message import EmailMessage
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from src.utils.config import EmailAlertConfig


def send_schema_alert(
    cfg: "EmailAlertConfig",
    source_name: str,
    missing_columns: list[str],
    logger: logging.Logger,
) -> None:
    """Send an email alert when required columns are missing from a data source.

    The sender password is resolved from the ``EMAIL_ALERT_PASSWORD`` environment
    variable first; the config value is used only as a fallback.  If neither is
    set the alert is skipped and a warning is logged instead of crashing the
    pipeline.

    Args:
        cfg: Parsed email alert config section from pipeline.yaml.
        source_name: Name of the data source where drift was detected.
        missing_columns: Sorted list of required columns that are absent.
        logger: Pipeline logger used to record send success or failure.
    """
    password = os.environ.get("EMAIL_ALERT_PASSWORD") or cfg.sender_password
    if not password:
        logger.warning(
            "Email alert skipped: no sender password found in "
            "EMAIL_ALERT_PASSWORD env var or config."
        )
        return

    detected_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    subject = f"[NovaCart] Schema Alert — missing columns in '{source_name}'"
    body = textwrap.dedent(f"""\
        NovaCart Pipeline — Schema Drift Alert
        ========================================

        Time Detected : {detected_at}
        Source        : {source_name}
        Missing Cols  : {", ".join(missing_columns)}

        Detail
        ------
        The schema check on source '{source_name}' found the following required
        column(s) absent from the incoming data:

          {chr(10).join(f"  • {col}" for col in missing_columns)}

        The pipeline has been halted for this source.  Please investigate the
        upstream data feed and rerun once the columns are restored.

        -- NovaCart automated alert
    """)

    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = cfg.sender_email
    msg["To"] = ", ".join(cfg.recipients)
    msg.set_content(body)

    try:
        with smtplib.SMTP(cfg.smtp_server, cfg.smtp_port) as smtp:
            smtp.ehlo()
            smtp.starttls()
            smtp.login(cfg.sender_email, password)
            smtp.send_message(msg)
        logger.info(
            "Schema alert email sent to %s for source '%s', missing=%s",
            cfg.recipients,
            source_name,
            missing_columns,
        )
    except (smtplib.SMTPException, OSError) as exc:
        logger.error(
            "Failed to send schema alert email for source '%s': %s",
            source_name,
            exc,
        )
