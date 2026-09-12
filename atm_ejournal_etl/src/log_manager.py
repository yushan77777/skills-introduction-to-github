"""
Logging for the ATM E-Journal ETL: one log file per run, one per batch, and an
automatic retention/size sweep of the log directory.

    logs/
        atm_ejournal_etl_20260912_200000.log     <- whole run
        batch_BATCH_0001_20260912_200001.log     <- one file per batch
        batch_BATCH_0002_20260912_201015.log

Every record carries the batch it belongs to (``%(batch_id)s``), so the run log
stays readable even though batches write to their own file as well.

Retention (``logging.LOG_RETENTION_DAYS`` and ``logging.MAX_LOG_SIZE_GB``) runs
at the start of the ETL, before any batch work:

    delete logs older than the retention period
        -> measure the directory
        -> while it is over the limit, delete the oldest log

The log files of the current run are never deleted, deletion errors are logged
and counted instead of aborting the ETL, and the sweep only ever touches files
that match the configured log file naming.
"""

from __future__ import annotations

import glob
import logging
import os
import time
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, Iterable, List, Optional

ROOT_LOGGER_NAME = "atm_ejournal"
BYTES_PER_GB = 1024 ** 3

DEFAULT_LOG_FORMAT = "%(asctime)s %(levelname)-8s %(name)s [%(batch_id)s] %(message)s"


class BatchContextFilter(logging.Filter):
    """Adds ``batch_id`` to every record so the format string always resolves."""

    def __init__(self, batch_id: str = "-"):
        super().__init__()
        self.batch_id = batch_id

    def filter(self, record: logging.LogRecord) -> bool:
        if not hasattr(record, "batch_id"):
            record.batch_id = self.batch_id
        return True


@dataclass
class LogCleanupReport:
    """What the retention sweep did - recorded in the run summary."""

    scanned_files: int = 0
    deleted_by_age: int = 0
    deleted_by_size: int = 0
    bytes_reclaimed: int = 0
    size_before_bytes: int = 0
    size_after_bytes: int = 0
    errors: int = 0
    skipped_active: int = 0
    deleted_files: List[str] = field(default_factory=list)

    def as_dict(self) -> Dict[str, object]:
        data = self.__dict__.copy()
        data["deleted_files"] = self.deleted_files[:50]     # keep the summary small
        return data


def _safe_stat(path: str):
    try:
        return os.stat(path)
    except OSError:
        return None


def cleanup_logs(log_dir: str,
                 retention_days: int = 365,
                 max_log_size_gb: float = 1.0,
                 active_files: Optional[Iterable[str]] = None,
                 patterns: Iterable[str] = ("*.log", "*.log.*"),
                 logger: Optional[logging.Logger] = None) -> LogCleanupReport:
    """
    Apply the retention policy to ``log_dir``.

    Deletes logs older than ``retention_days``, then - while the directory is
    larger than ``max_log_size_gb`` - deletes the oldest remaining log. Files in
    ``active_files`` (the current run's logs) are never deleted.
    """
    logger = logger or logging.getLogger(f"{ROOT_LOGGER_NAME}.logs")
    report = LogCleanupReport()

    if not os.path.isdir(log_dir):
        logger.debug("log cleanup skipped, directory does not exist: %s", log_dir)
        return report

    protected = {os.path.realpath(path) for path in (active_files or []) if path}

    candidates = []
    for pattern in patterns:
        for path in glob.glob(os.path.join(log_dir, pattern)):
            if not os.path.isfile(path):
                continue
            stat = _safe_stat(path)
            if stat is None:
                continue
            candidates.append((path, stat.st_mtime, stat.st_size))

    # A file matched by several patterns must only be counted once.
    unique = {os.path.realpath(path): (path, mtime, size)
              for path, mtime, size in candidates}
    entries = sorted(unique.values(), key=lambda item: item[1])
    report.scanned_files = len(entries)
    report.size_before_bytes = sum(size for _, _, size in entries)

    def delete(path: str, size: int, reason: str) -> bool:
        if os.path.realpath(path) in protected:
            report.skipped_active += 1
            logger.debug("log retention: keeping active log %s", os.path.basename(path))
            return False
        try:
            os.remove(path)
        except OSError as exc:
            report.errors += 1
            logger.warning("log retention: could not delete %s (%s): %s",
                           os.path.basename(path), reason, exc)
            return False
        report.bytes_reclaimed += size
        report.deleted_files.append(os.path.basename(path))
        logger.info("log retention: deleted %s (%s, %.1f MB)",
                    os.path.basename(path), reason, size / 1024 / 1024)
        return True

    # ---- 1. age ---------------------------------------------------------- #
    cutoff = time.time() - max(int(retention_days), 0) * 86400
    survivors = []
    for path, mtime, size in entries:
        if retention_days > 0 and mtime < cutoff:
            if delete(path, size, f"older than {retention_days} days"):
                report.deleted_by_age += 1
                continue
        survivors.append((path, mtime, size))

    # ---- 2. total size --------------------------------------------------- #
    limit_bytes = int(float(max_log_size_gb) * BYTES_PER_GB)
    total = sum(size for _, _, size in survivors)
    if limit_bytes > 0 and total > limit_bytes:
        logger.warning("log directory is %.2f GB, above the configured %.2f GB limit - "
                       "removing the oldest logs", total / BYTES_PER_GB, float(max_log_size_gb))
        for path, _mtime, size in survivors:          # oldest first
            if total <= limit_bytes:
                break
            if delete(path, size, "log directory over size limit"):
                report.deleted_by_size += 1
                total -= size
        if total > limit_bytes:
            logger.warning("log directory is still %.2f GB after cleanup (active logs and "
                           "undeletable files cannot be removed)", total / BYTES_PER_GB)
    report.size_after_bytes = total

    logger.info("log retention: %d file(s) scanned, %d removed by age, %d removed by size, "
                "%.1f MB reclaimed, %d error(s)",
                report.scanned_files, report.deleted_by_age, report.deleted_by_size,
                report.bytes_reclaimed / 1024 / 1024, report.errors)
    return report


class LogManager:
    """
    Owns the run log, the per-batch logs and the retention sweep.

    Typical use (see :mod:`etl_runner`)::

        log_manager = LogManager(cfg)
        log_manager.start_run(run_id)
        log_manager.cleanup()
        with log_manager.batch_log("BATCH_0001"):
            ...                       # everything logged here also lands in the batch file
    """

    def __init__(self, cfg, run_id: Optional[str] = None):
        self.cfg = cfg
        self.log_dir = cfg.path("logging.LOG_PATH", "logs")
        self.level = str(cfg.get("logging.LOG_LEVEL", "INFO")).upper()
        self.console_level = str(cfg.get("logging.CONSOLE_LOG_LEVEL", "INFO")).upper()
        self.run_prefix = str(cfg.get("logging.LOG_FILE_PREFIX", "atm_ejournal_etl"))
        self.batch_prefix = str(cfg.get("logging.BATCH_LOG_FILE_PREFIX", "batch"))
        self.log_format = str(cfg.get("logging.LOG_FORMAT", DEFAULT_LOG_FORMAT))
        self.run_id = run_id or datetime.now().strftime("%Y%m%d_%H%M%S")

        self.logger = logging.getLogger(ROOT_LOGGER_NAME)
        self.context_filter = BatchContextFilter()
        self.run_log_path: Optional[str] = None
        self.batch_log_paths: List[str] = []
        self._run_handler: Optional[logging.Handler] = None
        self._console_handler: Optional[logging.Handler] = None

    # -- run level --------------------------------------------------------- #

    def start_run(self) -> str:
        """Attach the run log file and console handler. Returns the log path."""
        os.makedirs(self.log_dir, exist_ok=True)
        self.run_log_path = os.path.join(self.log_dir, f"{self.run_prefix}_{self.run_id}.log")

        formatter = logging.Formatter(self.log_format)
        self.logger.setLevel(getattr(logging, self.level, logging.INFO))
        self.logger.propagate = False
        self.logger.addFilter(self.context_filter)

        self._run_handler = logging.FileHandler(self.run_log_path, encoding="utf-8")
        self._run_handler.setFormatter(formatter)
        self._run_handler.setLevel(getattr(logging, self.level, logging.INFO))
        self._run_handler.addFilter(self.context_filter)
        self.logger.addHandler(self._run_handler)

        self._console_handler = logging.StreamHandler()
        self._console_handler.setFormatter(formatter)
        self._console_handler.setLevel(getattr(logging, self.console_level, logging.INFO))
        self._console_handler.addFilter(self.context_filter)
        self.logger.addHandler(self._console_handler)

        return self.run_log_path

    def cleanup(self) -> LogCleanupReport:
        """Run the retention policy, protecting this run's log files."""
        if not self.cfg.get_bool("logging.LOG_CLEANUP_ENABLED", True):
            self.logger.info("log retention is disabled (logging.LOG_CLEANUP_ENABLED)")
            return LogCleanupReport()
        active = [path for path in [self.run_log_path, *self.batch_log_paths] if path]
        return cleanup_logs(
            log_dir=self.log_dir,
            retention_days=self.cfg.get_int("logging.LOG_RETENTION_DAYS", 365),
            max_log_size_gb=self.cfg.get_float("logging.MAX_LOG_SIZE_GB", 1.0),
            active_files=active,
            logger=self.logger,
        )

    def close(self) -> None:
        """Detach the run handlers - the ETL can be embedded in a longer process."""
        for handler in (self._run_handler, self._console_handler):
            if handler is not None:
                try:
                    self.logger.removeHandler(handler)
                    handler.close()
                except Exception:                      # noqa: BLE001 - shutdown must not fail
                    pass
        self._run_handler = None
        self._console_handler = None
        self.logger.removeFilter(self.context_filter)

    # -- batch level ------------------------------------------------------- #

    @contextmanager
    def batch_log(self, batch_id: str):
        """
        Route everything logged inside the block to an additional per-batch file
        and stamp every record with ``batch_id``.
        """
        os.makedirs(self.log_dir, exist_ok=True)
        stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        path = os.path.join(self.log_dir, f"{self.batch_prefix}_{batch_id}_{stamp}.log")
        handler: Optional[logging.Handler] = None
        previous = self.context_filter.batch_id
        try:
            handler = logging.FileHandler(path, encoding="utf-8")
            handler.setFormatter(logging.Formatter(self.log_format))
            handler.setLevel(getattr(logging, self.level, logging.INFO))
            handler.addFilter(self.context_filter)
            self.logger.addHandler(handler)
            self.batch_log_paths.append(path)
        except OSError as exc:
            # A batch must not fail because its dedicated log file cannot be
            # opened - the run log still records everything.
            self.logger.error("could not open batch log file %s: %s", path, exc)
            handler = None
        self.context_filter.batch_id = batch_id
        try:
            yield path
        finally:
            self.context_filter.batch_id = previous
            if handler is not None:
                try:
                    self.logger.removeHandler(handler)
                    handler.close()
                except Exception:                      # noqa: BLE001
                    pass


def get_logger(name: Optional[str] = None) -> logging.Logger:
    """Child logger under the ETL root, so it inherits the run/batch handlers."""
    return logging.getLogger(ROOT_LOGGER_NAME if not name else f"{ROOT_LOGGER_NAME}.{name}")
