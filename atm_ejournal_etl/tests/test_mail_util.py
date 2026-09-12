"""SMTP notifications: configuration, credentials and message content."""

from __future__ import annotations

import smtplib

import pytest

from mail_util import MailSender, format_failure_body, format_success_body


class FakeSMTP:
    sent = []
    raise_on_send = False

    def __init__(self, host, port, timeout=None):
        self.host, self.port, self.timeout = host, port, timeout
        self.tls = False
        self.login_args = None

    def __enter__(self):
        return self

    def __exit__(self, *exc_info):
        return False

    def starttls(self):
        self.tls = True

    def login(self, user, password):
        self.login_args = (user, password)

    def send_message(self, message, to_addrs=None):
        if FakeSMTP.raise_on_send:
            raise smtplib.SMTPException("mail server unavailable")
        FakeSMTP.sent.append({"message": message, "to_addrs": to_addrs,
                              "host": self.host, "port": self.port, "tls": self.tls,
                              "login": self.login_args})


@pytest.fixture(autouse=True)
def fake_smtp(monkeypatch):
    FakeSMTP.sent = []
    FakeSMTP.raise_on_send = False
    monkeypatch.setattr(smtplib, "SMTP", FakeSMTP)
    return FakeSMTP


def _enable_mail(cfg, **overrides):
    section = cfg._data["mail"]                                    # noqa: SLF001
    section["MAIL_ENABLED"] = True
    section.update(overrides)
    return cfg


def test_mail_disabled_sends_nothing(cfg, fake_smtp):
    assert MailSender(cfg).send("subject", "body") is False
    assert fake_smtp.sent == []


def test_send_uses_configured_server_and_recipients(cfg, fake_smtp):
    _enable_mail(cfg, SMTP_HOST="mail.example.com", SMTP_PORT=2525,
                 MAIL_TO="a@example.com,b@example.com", MAIL_CC="c@example.com")
    assert MailSender(cfg).send("ETL SUCCESS", "all good") is True

    sent = fake_smtp.sent[0]
    assert (sent["host"], sent["port"]) == ("mail.example.com", 2525)
    assert sent["to_addrs"] == ["a@example.com", "b@example.com", "c@example.com"]
    assert sent["message"]["Subject"] == "ETL SUCCESS"
    assert sent["message"]["From"] == "etl@example.com"
    assert sent["message"].get_content().strip() == "all good"


def test_incomplete_configuration_is_reported_not_raised(cfg, fake_smtp, caplog):
    _enable_mail(cfg, SMTP_HOST="", MAIL_TO="")
    assert MailSender(cfg).send("subject", "body") is False
    assert "incomplete configuration" in caplog.text


def test_smtp_failure_never_breaks_the_caller(cfg, fake_smtp, caplog):
    _enable_mail(cfg)
    fake_smtp.raise_on_send = True
    assert MailSender(cfg).send("subject", "body") is False
    assert "notification could not be sent" in caplog.text


def test_smtp_password_is_decrypted_through_the_project_mechanism(cfg, fake_smtp, tmp_path):
    import pickle

    from cryptography.fernet import Fernet
    from encryption_util import SecretResolver

    key_path = tmp_path / "encryption.pkl"
    key = Fernet.generate_key()
    key_path.write_bytes(pickle.dumps(key))

    _enable_mail(cfg, SMTP_USER="etl_user", SMTP_USE_TLS=True,
                 SMTP_PASSWORD=Fernet(key).encrypt(b"smtp-pass").decode(),
                 SMTP_PICKLE=str(key_path))
    resolver = SecretResolver(modules=["not_installed"], default_pickle=str(key_path))

    assert MailSender(cfg, secret_resolver=resolver).send("s", "b") is True
    sent = fake_smtp.sent[0]
    assert sent["tls"] is True
    assert sent["login"] == ("etl_user", "smtp-pass")


# --------------------------------------------------------------------------- #
# Bodies
# --------------------------------------------------------------------------- #


def test_success_body_reports_the_recorded_metrics():
    body = format_success_body({
        "dag_id": "atm_ejournal_etl", "run_id": "20260912", "status": "SUCCESS",
        "files_discovered": 10000, "files_previously_processed": 9500,
        "files_processed": 500, "files_failed": 0, "batches_processed": 1,
        "records_processed": 1234, "records_loaded": 1234, "duration_human": "00:05:00",
        "batches": [{"batch_id": "BATCH_0001", "file_count": 500, "records": 1234,
                     "rows_loaded": 1234, "duration_seconds": 42.0, "status": "SUCCESS"}],
    })
    assert "ATM E-Journal ETL SUCCESS" in body
    assert "Files discovered:            10000" in body
    assert "BATCH_0001" in body


def test_failure_body_has_the_troubleshooting_fields():
    body = format_failure_body({
        "dag_id": "atm_ejournal_etl", "task_id": "run_atm_ejournal_etl",
        "stage": "parquet_write", "batch_id": "BATCH_0003",
        "error": "ParquetStageError: disk full", "stack_trace": "Traceback ...",
        "log_path": "/etl/logs/atm_ejournal_etl_20260912.log",
    })
    for expected in ("ATM E-Journal ETL FAILED", "parquet_write", "BATCH_0003",
                     "disk full", "Traceback", "Log location", "Recovery"):
        assert expected in body
