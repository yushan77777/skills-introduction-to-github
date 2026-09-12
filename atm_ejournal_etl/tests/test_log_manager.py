"""Per-batch logging and the retention / 1 GB sweep."""

from __future__ import annotations

import logging
import os
import time

import pytest

from log_manager import BYTES_PER_GB, LogManager, cleanup_logs, get_logger


def _write_log(directory, name, size_bytes=1024, age_days=0.0):
    path = os.path.join(directory, name)
    with open(path, "wb") as handle:
        handle.write(b"x" * size_bytes)
    if age_days:
        old = time.time() - age_days * 86400
        os.utime(path, (old, old))
    return path


# --------------------------------------------------------------------------- #
# Log creation
# --------------------------------------------------------------------------- #


def test_run_and_batch_logs_are_created(cfg):
    manager = LogManager(cfg, run_id="20260912_200000")
    run_log = manager.start_run()
    try:
        logger = get_logger("test")
        logger.info("ETL started")
        with manager.batch_log("BATCH_0001") as batch_log:
            logger.info("batch message")
        assert os.path.isfile(run_log)
        assert os.path.basename(run_log) == "atm_ejournal_etl_20260912_200000.log"
        assert os.path.isfile(batch_log)
        assert "BATCH_0001" in os.path.basename(batch_log)

        run_text = open(run_log).read()
        batch_text = open(batch_log).read()
        assert "ETL started" in run_text
        assert "batch message" in batch_text and "ETL started" not in batch_text
        # the batch id is stamped on every record of the batch
        assert "[BATCH_0001] batch message" in batch_text
    finally:
        manager.close()


def test_logging_continues_when_the_batch_file_cannot_be_opened(cfg, monkeypatch):
    manager = LogManager(cfg, run_id="R")
    run_log = manager.start_run()
    try:
        def broken(*args, **kwargs):
            raise OSError("read-only file system")
        monkeypatch.setattr(logging, "FileHandler", broken)
        with manager.batch_log("BATCH_0002"):
            get_logger("test").info("still logged to the run log")
        assert "still logged to the run log" in open(run_log).read()
    finally:
        manager.close()


# --------------------------------------------------------------------------- #
# Retention
# --------------------------------------------------------------------------- #


def test_logs_older_than_retention_are_deleted(tmp_path):
    directory = str(tmp_path)
    old = _write_log(directory, "old.log", age_days=400)
    recent = _write_log(directory, "recent.log", age_days=10)

    report = cleanup_logs(directory, retention_days=365, max_log_size_gb=10)

    assert not os.path.exists(old)
    assert os.path.exists(recent)
    assert report.deleted_by_age == 1
    assert report.deleted_by_size == 0


def test_size_limit_deletes_oldest_first(tmp_path):
    directory = str(tmp_path)
    oldest = _write_log(directory, "a.log", size_bytes=4000, age_days=3)
    middle = _write_log(directory, "b.log", size_bytes=4000, age_days=2)
    newest = _write_log(directory, "c.log", size_bytes=4000, age_days=1)

    # 10 KB limit expressed in GB
    report = cleanup_logs(directory, retention_days=365,
                          max_log_size_gb=10000 / BYTES_PER_GB)

    assert not os.path.exists(oldest)
    assert os.path.exists(middle) and os.path.exists(newest)
    assert report.deleted_by_size == 1
    assert report.size_after_bytes <= 10000


def test_active_log_is_never_deleted(tmp_path):
    directory = str(tmp_path)
    active = _write_log(directory, "active.log", size_bytes=8000, age_days=500)
    other = _write_log(directory, "other.log", size_bytes=8000, age_days=400)

    report = cleanup_logs(directory, retention_days=1,
                          max_log_size_gb=1000 / BYTES_PER_GB,
                          active_files=[active])

    assert os.path.exists(active)
    assert not os.path.exists(other)
    assert report.skipped_active >= 1


def test_deletion_errors_are_logged_not_raised(tmp_path, monkeypatch, caplog):
    directory = str(tmp_path)
    _write_log(directory, "old.log", age_days=400)

    def refuse(path):
        raise OSError("permission denied")
    monkeypatch.setattr(os, "remove", refuse)

    with caplog.at_level(logging.WARNING):
        report = cleanup_logs(directory, retention_days=365, max_log_size_gb=1)
    assert report.errors == 1
    assert "could not delete" in caplog.text


def test_cleanup_on_missing_directory_is_a_no_op(tmp_path):
    report = cleanup_logs(str(tmp_path / "absent"), retention_days=365, max_log_size_gb=1)
    assert report.scanned_files == 0


def test_manager_cleanup_protects_current_run(cfg):
    manager = LogManager(cfg, run_id="NOW")
    run_log = manager.start_run()
    try:
        log_dir = os.path.dirname(run_log)
        stale = _write_log(log_dir, "atm_ejournal_etl_OLD.log", size_bytes=2048, age_days=400)
        with manager.batch_log("BATCH_0001"):
            pass
        report = manager.cleanup()
        assert not os.path.exists(stale)
        assert os.path.exists(run_log)
        assert report.deleted_by_age == 1
    finally:
        manager.close()


def test_cleanup_can_be_disabled(cfg):
    cfg._data["logging"]["LOG_CLEANUP_ENABLED"] = False        # noqa: SLF001
    manager = LogManager(cfg, run_id="NOW2")
    run_log = manager.start_run()
    try:
        stale = _write_log(os.path.dirname(run_log), "atm_ejournal_etl_OLD2.log", age_days=800)
        report = manager.cleanup()
        assert os.path.exists(stale)
        assert report.scanned_files == 0
    finally:
        manager.close()
