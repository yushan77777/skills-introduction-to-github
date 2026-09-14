"""
E-mail notifications for the ATM E-Journal ETL.

Used by the Airflow DAG (failure and success callbacks) and available to the ETL
itself. All settings - host, port, TLS, sender, recipients, subjects and the SMTP
password - come from the ``mail:`` section of the configuration file.

Sending is best effort: a notification that cannot be delivered is logged, never
raised into the caller, so a mail outage does not turn a successful ETL run into
a failed Airflow task.
"""

from __future__ import annotations

import logging
import smtplib
from email.message import EmailMessage
from email.utils import formatdate
from typing import Any, Dict, List, Optional, Sequence

logger = logging.getLogger("atm_ejournal.mail")


class MailSender:
    """Thin SMTP wrapper around the ``mail:`` configuration section."""

    def __init__(self, cfg):
        self.cfg = cfg
        self.enabled = cfg.get_bool("mail.MAIL_ENABLED", True)
        self.host = str(cfg.get("mail.SMTP_HOST", "") or "")
        self.port = cfg.get_int("mail.SMTP_PORT", 25)
        self.use_tls = cfg.get_bool("mail.SMTP_USE_TLS", False)
        self.user = str(cfg.get("mail.SMTP_USER", "") or "")
        self.timeout = cfg.get_int("mail.SMTP_TIMEOUT_SECONDS", 30)
        self.mail_from = str(cfg.get("mail.MAIL_FROM", "") or "")
        self.mail_to = cfg.get_list("mail.MAIL_TO")
        self.mail_cc = cfg.get_list("mail.MAIL_CC")
        self.password = str(cfg.get("mail.SMTP_PASSWORD", "") or "")

    # -- sending ------------------------------------------------------------ #

    def send(self, subject: str, body: str,
             to: Optional[Sequence[str]] = None,
             cc: Optional[Sequence[str]] = None) -> bool:
        """Send a plain-text mail. Returns True when it was handed to the SMTP server."""
        if not self.enabled:
            logger.info("mail disabled (mail.MAIL_ENABLED) - not sending: %s", subject)
            return False
        recipients: List[str] = list(to or self.mail_to)
        copies: List[str] = list(cc or self.mail_cc)
        if not self.host or not self.mail_from or not recipients:
            logger.warning("mail not sent, incomplete configuration "
                           "(SMTP_HOST / MAIL_FROM / MAIL_TO): %s", subject)
            return False

        message = EmailMessage()
        message["Subject"] = subject
        message["From"] = self.mail_from
        message["To"] = ", ".join(recipients)
        if copies:
            message["Cc"] = ", ".join(copies)
        message["Date"] = formatdate(localtime=True)
        message.set_content(body)

        try:
            with smtplib.SMTP(self.host, self.port, timeout=self.timeout) as server:
                if self.use_tls:
                    server.starttls()
                if self.user:
                    server.login(self.user, self.password)
                server.send_message(message, to_addrs=recipients + copies)
            logger.info("notification sent to %s: %s", ", ".join(recipients), subject)
            return True
        except Exception as exc:                       # noqa: BLE001 - never fail the ETL
            logger.error("notification could not be sent (%s): %s", type(exc).__name__, exc)
            return False

    # -- convenience -------------------------------------------------------- #

    def send_success(self, summary: Dict[str, Any], subject: Optional[str] = None) -> bool:
        subject = subject or str(self.cfg.get("mail.MAIL_SUBJECT_SUCCESS",
                                              "ATM E-Journal ETL SUCCESS"))
        return self.send(subject, format_success_body(summary))

    def send_failure(self, context: Dict[str, Any], subject: Optional[str] = None) -> bool:
        subject = subject or str(self.cfg.get("mail.MAIL_SUBJECT_FAILURE",
                                              "ATM E-Journal ETL FAILED"))
        return self.send(subject, format_failure_body(context))


# --------------------------------------------------------------------------- #
# Message bodies (no credentials, ever)
# --------------------------------------------------------------------------- #


def _line(label: str, value: Any, width: int = 28) -> str:
    return f"{label + ':':<{width}} {'' if value is None else value}"


def format_success_body(summary: Dict[str, Any]) -> str:
    """Success mail built from the metrics the ETL actually recorded."""
    batches = summary.get("batches") or []
    lines = [
        "ATM E-Journal ETL SUCCESS",
        "=" * 60,
        _line("DAG", summary.get("dag_id")),
        _line("Run ID", summary.get("run_id")),
        _line("Execution Date", summary.get("execution_date")),
        _line("ETL", summary.get("etl_name")),
        _line("Environment", summary.get("environment")),
        _line("Start Time", summary.get("start_time")),
        _line("End Time", summary.get("end_time")),
        _line("Total Execution Time", summary.get("duration_human")),
        "",
        _line("Files discovered", summary.get("files_discovered")),
        _line("Previously processed", summary.get("files_previously_processed")),
        _line("Files processed", summary.get("files_processed")),
        _line("Files failed", summary.get("files_failed")),
        _line("Number of batches", summary.get("batches_processed")),
        _line("Records processed", summary.get("records_processed")),
        _line("Records loaded into Greenplum", summary.get("records_loaded")),
        _line("Greenplum table", summary.get("greenplum_table")),
        _line("Status", summary.get("status", "SUCCESS")),
        "",
        _line("Log location", summary.get("log_path")),
        _line("Run summary", summary.get("summary_path")),
    ]
    if batches:
        lines += ["", "Batches", "-" * 60,
                  f"{'BATCH':<14}{'FILES':>7}{'RECORDS':>10}{'LOADED':>10}{'SECONDS':>10}  STATUS"]
        for batch in batches:
            lines.append(f"{str(batch.get('batch_id', '')):<14}"
                         f"{batch.get('file_count', 0):>7}"
                         f"{batch.get('records', 0):>10}"
                         f"{batch.get('rows_loaded', 0):>10}"
                         f"{batch.get('duration_seconds', 0):>10.1f}"
                         f"  {batch.get('status', '')}")
    return "\n".join(lines)


def format_failure_body(context: Dict[str, Any]) -> str:
    """Failure mail - carries what is needed to find the problem, nothing secret."""
    lines = [
        "ATM E-Journal ETL FAILED",
        "=" * 60,
        _line("DAG", context.get("dag_id")),
        _line("Run ID", context.get("run_id")),
        _line("Execution Date", context.get("execution_date")),
        _line("Task", context.get("task_id")),
        _line("Try number", context.get("try_number")),
        _line("ETL", context.get("etl_name")),
        _line("Environment", context.get("environment")),
        _line("Start Time", context.get("start_time")),
        _line("Failure Time", context.get("failure_time")),
        _line("Failed stage", context.get("stage")),
        _line("Batch ID", context.get("batch_id")),
        _line("Files discovered", context.get("files_discovered")),
        _line("Files processed", context.get("files_processed")),
        _line("Files failed", context.get("files_failed")),
        _line("Batches completed", context.get("batches_processed")),
        "",
        "Error",
        "-" * 60,
        str(context.get("error") or "see the log for details"),
        "",
        "Stack trace",
        "-" * 60,
        str(context.get("stack_trace") or "(not available)"),
        "",
        _line("Log location", context.get("log_path")),
        _line("Airflow log", context.get("airflow_log_url")),
        "",
        "Recovery: the files of the failed batch were NOT marked as processed; the next "
        "run picks them up again. Batches already completed are not reprocessed.",
    ]
    return "\n".join(lines)
