"""
Parquet staging between Spark and Greenplum.

Each batch is materialised to its own parquet directory before anything is sent
to Greenplum::

    parquet/
        batch_0001/        <- one batch, written then re-read
        batch_0002/

Doing it this way means the transformed batch is never held as an in-memory
DataFrame across the load: the parse runs once, lands on disk, and the Greenplum
writer reads the parquet back. It also gives the load a stable, re-readable
input - if the Greenplum step fails, the parquet is still there to retry from or
to inspect.

Cleanup is deliberately conservative: a batch directory is removed only after
Greenplum has confirmed the load *and* the tracking CSV has been updated.
Parquet belonging to a failed batch is kept (``PARQUET_KEEP_FAILED_BATCHES``)
and swept later by age (``PARQUET_FAILED_RETENTION_DAYS``).
"""

from __future__ import annotations

import logging
import os
import shutil
import time
from typing import Optional, Tuple

logger = logging.getLogger("atm_ejournal.parquet")


class ParquetStageError(Exception):
    """Raised when a batch parquet cannot be written, validated or read back."""


class ParquetStage:
    """Owns the intermediate parquet directory for one ETL."""

    def __init__(self, cfg, spark=None, subdir: Optional[str] = None):
        self.cfg = cfg
        self.spark = spark
        self.root = cfg.path("parquet.PARQUET_PATH", "parquet")
        if subdir:
            # Keeps the parquet of two runs apart when it is not deleted after
            # the load (manual runs with keep_parquet=True).
            self.root = os.path.join(self.root, str(subdir))
        self.compression = str(cfg.get("parquet.PARQUET_COMPRESSION", "snappy"))
        self.coalesce_partitions = cfg.get_int("parquet.PARQUET_COALESCE_PARTITIONS", 0)
        self.cleanup_enabled = cfg.get_bool("parquet.PARQUET_CLEANUP_ENABLED", True)
        self.keep_failed = cfg.get_bool("parquet.PARQUET_KEEP_FAILED_BATCHES", True)
        self.failed_retention_days = cfg.get_int("parquet.PARQUET_FAILED_RETENTION_DAYS", 7)

    # -- paths ------------------------------------------------------------- #

    def batch_dir(self, batch_id: str) -> str:
        return os.path.join(self.root, batch_id.lower())

    def ensure_root(self) -> str:
        if _is_local(self.root):
            os.makedirs(self.root, exist_ok=True)
        return self.root

    # -- write / validate / read ------------------------------------------- #

    def write_batch(self, dataframe, batch_id: str) -> Tuple[str, int]:
        """
        Write one batch and return ``(path, row_count)``.

        The row count comes from reading the parquet back, not from counting the
        DataFrame first: counting before the write would run the whole parse
        twice. ``PARQUET_COALESCE_PARTITIONS`` keeps a 500-file batch from
        landing as hundreds of tiny parquet files.
        """
        self.ensure_root()
        path = self.batch_dir(batch_id)
        to_write = dataframe
        if self.coalesce_partitions > 0:
            try:
                current = dataframe.rdd.getNumPartitions()
            except Exception:                          # noqa: BLE001 - partitioning is advisory
                current = None
            if current is not None and current > self.coalesce_partitions:
                to_write = dataframe.coalesce(self.coalesce_partitions)

        logger.info("parquet write started | batch=%s | path=%s | compression=%s",
                    batch_id, path, self.compression)
        started = time.time()
        try:
            (to_write.write
             .mode("overwrite")                        # a retried batch overwrites its own dir
             .option("compression", self.compression)
             .parquet(path))
        except Exception as exc:                       # noqa: BLE001 - re-raised as stage error
            raise ParquetStageError(f"parquet write failed for {batch_id} at {path}: {exc}") from exc

        row_count = self.validate_batch(path, batch_id)
        logger.info("parquet write completed | batch=%s | rows=%d | %.1fs",
                    batch_id, row_count, time.time() - started)
        return path, row_count

    def validate_batch(self, path: str, batch_id: str) -> int:
        """
        Confirm the parquet is complete and readable, and return its row count.

        Checks the ``_SUCCESS`` marker Spark writes on a clean commit, that at
        least one data file exists, and that the directory can be read back with
        the expected schema.
        """
        if self.spark is None:
            raise ParquetStageError("parquet validation needs a SparkSession")

        if _is_local(path):
            if not os.path.isdir(path):
                raise ParquetStageError(f"parquet directory missing after write: {path}")
            names = os.listdir(path)
            if "_SUCCESS" not in names:
                raise ParquetStageError(f"parquet commit marker (_SUCCESS) missing in {path} - "
                                        f"batch {batch_id} is incomplete")
            if not any(name.endswith(".parquet") for name in names):
                logger.warning("batch %s produced no parquet data file (no withdrawal "
                               "records in these journals)", batch_id)

        try:
            count = self.read_batch(path).count()
        except Exception as exc:                       # noqa: BLE001
            raise ParquetStageError(f"parquet written for {batch_id} could not be read back "
                                    f"from {path}: {exc}") from exc
        logger.info("parquet validated | batch=%s | rows=%d", batch_id, count)
        return count

    def read_batch(self, path: str):
        """Read a batch parquet back for the Greenplum load."""
        if self.spark is None:
            raise ParquetStageError("parquet read needs a SparkSession")
        return self.spark.read.parquet(path)

    # -- cleanup ------------------------------------------------------------ #

    def cleanup_batch(self, batch_id: str, success: bool = True) -> bool:
        """
        Remove a batch directory. Only call this once Greenplum has confirmed
        the load and the tracking CSV has been updated.
        """
        path = self.batch_dir(batch_id)
        if not success and self.keep_failed:
            logger.warning("parquet kept for troubleshooting (failed batch %s): %s",
                           batch_id, path)
            return False
        if not self.cleanup_enabled:
            logger.info("parquet cleanup disabled (parquet.PARQUET_CLEANUP_ENABLED) - "
                        "keeping %s", path)
            return False
        return self._delete(path, reason=f"batch {batch_id} completed")

    def cleanup_stale_batches(self, keep_batch_ids: Optional[set] = None) -> int:
        """
        Sweep parquet directories left behind by failed batches once they are
        older than ``PARQUET_FAILED_RETENTION_DAYS``. Returns how many went.
        """
        if self.failed_retention_days <= 0 or not _is_local(self.root):
            return 0
        if not os.path.isdir(self.root):
            return 0
        keep = {batch.lower() for batch in (keep_batch_ids or set())}
        cutoff = time.time() - self.failed_retention_days * 86400
        removed = 0
        for name in sorted(os.listdir(self.root)):
            path = os.path.join(self.root, name)
            if not os.path.isdir(path) or name.lower() in keep:
                continue
            try:
                if os.path.getmtime(path) >= cutoff:
                    continue
            except OSError:
                continue
            if self._delete(path, reason=f"older than {self.failed_retention_days} days"):
                removed += 1
        if removed:
            logger.info("parquet cleanup: %d stale batch directory/ies removed", removed)
        return removed

    def prepare_for_batch(self, batch_id: str) -> str:
        """
        Clear any leftover directory for this batch id so the next batch starts
        from a clean location (a re-run of the same batch id must not mix rows).
        """
        path = self.batch_dir(batch_id)
        if _is_local(path) and os.path.isdir(path):
            logger.warning("removing leftover parquet directory before batch %s: %s",
                           batch_id, path)
            self._delete(path, reason="leftover from an earlier run")
        return path

    # -- internals ---------------------------------------------------------- #

    def _delete(self, path: str, reason: str) -> bool:
        try:
            if _is_local(path):
                if os.path.isdir(path):
                    shutil.rmtree(path)
                elif os.path.exists(path):
                    os.remove(path)
                else:
                    return False
            else:
                self._delete_hadoop(path)
            logger.info("parquet removed (%s): %s", reason, path)
            return True
        except Exception as exc:                       # noqa: BLE001 - never fail the ETL
            logger.error("could not remove parquet directory %s (%s): %s", path, reason, exc)
            return False

    def _delete_hadoop(self, path: str) -> None:
        if self.spark is None:
            raise ParquetStageError(f"cannot delete non-local path without Spark: {path}")
        jvm = self.spark._jvm                                        # noqa: SLF001
        hadoop_path = jvm.org.apache.hadoop.fs.Path(path)
        filesystem = hadoop_path.getFileSystem(
            self.spark._jsc.hadoopConfiguration())                   # noqa: SLF001
        if filesystem.exists(hadoop_path):
            filesystem.delete(hadoop_path, True)


def _is_local(path: str) -> bool:
    """True for a plain filesystem path (no hdfs://, s3a://, ... scheme)."""
    lowered = str(path).lower()
    if lowered.startswith("file:"):
        return False
    return "://" not in lowered
