"""
Manual runners for the ATM E-Journal ETL.

Two entry points, both built from the same pieces the scheduled ETL uses, for
when you want to drive the load by hand from a notebook instead of through
Airflow:

``run_batches``
    Batch-wise: take the next unprocessed journal files, parse them, write one
    parquet per batch, load that parquet into Greenplum, record the files in
    ``processed_files.csv``, clean the parquet up, repeat. Stops after
    ``max_batches`` (or when nothing is left).

``load_parquet_path``
    Given a path - one ``.parquet`` file, a Spark-style parquet directory, or a
    folder holding several of them - write the whole thing into Greenplum.
    ``only_new=True`` (the default) skips the parquet files that a previous call
    already loaded, which are tracked in ``processed/loaded_parquet.csv``, so a
    re-run inserts only what has not been inserted yet.

``process_path``
    The two combined for one directory of journals: parse everything under
    ``path`` that is not in ``processed_files.csv`` and load it, in batches.

Nothing here re-implements the ETL: the parsing, the tracking, the parquet
staging and the Greenplum write are the project's own functions, so a manual run
and a scheduled run behave identically and share the same bookkeeping.
"""

from __future__ import annotations

import csv
import logging
import os
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Callable, Dict, Iterable, List, Optional

from file_registry import (DiscoveredFile, ProcessedFileRegistry, discover_files,
                           format_batch_id, iter_batches)
from local_parser import count_parquet_rows, write_batch_parquet
from parquet_stage import ParquetStage

logger = logging.getLogger("atm_ejournal.manual")

LOADED_PARQUET_COLUMNS = ["parquet_path", "rows_loaded", "batch_id", "run_id", "loaded_date"]


class ManualLoadError(Exception):
    """Raised when a manual batch or path load cannot be completed."""


def new_run_id() -> str:
    """
    A run id that is unique even when two manual calls start in the same second.

    It matters: rows carry ETL_RUN_ID + BATCH_ID, and a repeated batch of the
    same run is treated as a retry - its previous rows are removed before the
    insert. Two calls sharing a run id would therefore delete each other's rows.
    """
    return f"{datetime.now().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:4]}"


@dataclass
class BatchReport:
    """One batch of a manual run - printed by the notebook, returned to the caller."""

    batch_id: str
    files: int = 0
    records: int = 0
    rows_loaded: int = 0
    files_marked: int = 0
    files_failed: int = 0
    duration_seconds: float = 0.0
    parquet_path: str = ""
    status: str = "SUCCESS"
    error: str = ""

    def as_dict(self) -> Dict[str, Any]:
        return self.__dict__.copy()


# --------------------------------------------------------------------------- #
# Tracking of the parquet files that were already inserted
# --------------------------------------------------------------------------- #


class LoadedParquetRegistry:
    """
    Append-only record of the parquet paths that have been written to Greenplum.

    It is what makes ``load_parquet_path(..., only_new=True)`` load *only what
    has not been inserted yet* when it is pointed at a folder again.
    """

    def __init__(self, csv_path: str):
        self.csv_path = csv_path
        self._paths: Optional[set] = None

    def loaded_paths(self, refresh: bool = False) -> set:
        if self._paths is not None and not refresh:
            return self._paths
        paths = set()
        if os.path.exists(self.csv_path):
            try:
                with open(self.csv_path, newline="", encoding="utf-8", errors="replace") as handle:
                    for row in csv.DictReader(handle):
                        value = (row.get("parquet_path") or "").strip()
                        if value:
                            paths.add(os.path.abspath(value))
            except OSError as exc:
                raise ManualLoadError(f"could not read {self.csv_path}: {exc}") from exc
        self._paths = paths
        return paths

    def contains(self, path: str) -> bool:
        return os.path.abspath(path) in self.loaded_paths()

    def append(self, path: str, rows_loaded: int, batch_id: str = "", run_id: str = "") -> None:
        os.makedirs(os.path.dirname(os.path.abspath(self.csv_path)), exist_ok=True)
        is_new = not os.path.exists(self.csv_path)
        with open(self.csv_path, "a", newline="", encoding="utf-8") as handle:
            writer = csv.writer(handle)
            if is_new:
                writer.writerow(LOADED_PARQUET_COLUMNS)
            writer.writerow([os.path.abspath(path), rows_loaded, batch_id, run_id,
                             datetime.now().strftime("%Y-%m-%d %H:%M:%S")])
            handle.flush()
            os.fsync(handle.fileno())
        if self._paths is not None:
            self._paths.add(os.path.abspath(path))


# --------------------------------------------------------------------------- #
# Shared helpers
# --------------------------------------------------------------------------- #


def build_spark(cfg, spark=None):
    """Reuse a session when one is passed in, otherwise build it from the config."""
    if spark is not None:
        return spark
    from spark_session import build_spark_session                             # noqa: PLC0415

    return build_spark_session(cfg)


def build_loader(cfg, spark, loader=None):
    """Reuse a loader when one is passed in (the tests pass a double)."""
    if loader is not None:
        if getattr(loader, "spark", None) is None:
            loader.spark = spark
        return loader
    from greenplum_loader import GreenplumLoader                              # noqa: PLC0415

    return GreenplumLoader(cfg, spark=spark)


def unprocessed_files(cfg, registry: ProcessedFileRegistry,
                      input_path: Optional[str] = None) -> Iterable[DiscoveredFile]:
    """Lazy stream of the journal files that are not in ``processed_files.csv``."""
    processed = registry.load_keys()
    key_mode = registry.key_mode
    for item in discover_files(
            input_path=input_path or cfg.path("input.INPUT_PATH"),
            patterns=cfg.get_list("input.FILE_PATTERN"),
            folder_depth=cfg.get_int("input.ATM_FOLDER_DEPTH", 1),
            min_file_age_seconds=cfg.get_int("input.MIN_FILE_AGE_SECONDS", 0),
            sniff_unknown_extensions=cfg.get_bool("input.SNIFF_UNKNOWN_EXTENSIONS", False)):
        if item.key(key_mode) not in processed:
            yield item


def _registry(cfg) -> ProcessedFileRegistry:
    return ProcessedFileRegistry(
        cfg.path("tracking.PROCESSED_FILES_CSV", "processed/processed_files.csv"),
        key_mode=str(cfg.get("tracking.FILE_KEY_MODE", "path")))


def _loaded_registry(cfg) -> LoadedParquetRegistry:
    tracking_dir = os.path.dirname(cfg.path("tracking.PROCESSED_FILES_CSV",
                                            "processed/processed_files.csv"))
    return LoadedParquetRegistry(os.path.join(tracking_dir, "loaded_parquet.csv"))


# --------------------------------------------------------------------------- #
# 1. Batch-wise: parse -> parquet -> Greenplum -> mark processed
# --------------------------------------------------------------------------- #


def run_batches(cfg,
                batch_size: Optional[int] = None,
                max_batches: Optional[int] = None,
                input_path: Optional[str] = None,
                run_id: Optional[str] = None,
                spark=None,
                loader=None,
                keep_parquet: bool = False,
                on_batch: Optional[Callable[[BatchReport], None]] = None) -> List[BatchReport]:
    """
    Process the unprocessed journals batch by batch and insert each batch into
    Greenplum.

    Parameters
    ----------
    batch_size:
        Files per batch (default: ``input.BATCH_SIZE``).
    max_batches:
        Stop after this many batches. ``None`` or ``0`` processes everything left.
    input_path:
        Override ``input.INPUT_PATH`` for this call.
    keep_parquet:
        Keep each batch's parquet instead of deleting it after the load.
    on_batch:
        Called with every :class:`BatchReport` as soon as the batch finishes -
        the notebook uses it to print progress while the run is going on.

    A batch that fails is reported and the run continues with the next one; its
    files stay unprocessed, exactly as in the scheduled ETL.
    """
    batch_size = cfg.get_int("input.BATCH_SIZE") if batch_size is None else int(batch_size)
    if batch_size < 1:
        raise ManualLoadError(f"batch_size must be >= 1, got {batch_size}")
    run_id = run_id or new_run_id()

    registry = _registry(cfg)
    registry.ensure_file()
    # Kept parquet is filed under the run id so two runs cannot overwrite each
    # other's batch_0001; when it is deleted after the load the layout is the
    # usual parquet/batch_nnnn.
    stage = ParquetStage(cfg, None, subdir=run_id if keep_parquet else None)
    stage.ensure_root()

    reports: List[BatchReport] = []
    spark_session = None
    gp_loader = None

    for sequence, batch in enumerate(
            iter_batches(unprocessed_files(cfg, registry, input_path), batch_size), start=1):
        if max_batches and len(reports) >= max_batches:
            logger.info("stopping after %d batch(es) as requested", max_batches)
            break

        batch_id = format_batch_id(sequence)
        report = BatchReport(batch_id=batch_id, files=len(batch))
        started = time.time()
        try:
            # -- parse this batch into its own parquet ----------------------- #
            parquet_path = stage.prepare_for_batch(batch_id)
            parsed = write_batch_parquet(batch, cfg, batch_id, run_id, parquet_path)
            report.parquet_path = parquet_path
            report.records = count_parquet_rows(parquet_path)
            report.files_failed = len(parsed.failures)
            failed_keys = {entry.get("file_key") for entry in parsed.failures}
            loadable = [item for item in batch
                        if item.key(registry.key_mode) not in failed_keys]

            # -- insert into Greenplum (JVM side only) ----------------------- #
            if spark_session is None:
                spark_session = build_spark(cfg, spark)
                stage.spark = spark_session
                gp_loader = build_loader(cfg, spark_session, loader)
            dataframe = spark_session.read.parquet(parquet_path)
            load_result = gp_loader.load_batch(dataframe, batch_id, run_id,
                                               file_count=len(loadable),
                                               parquet_path=parquet_path,
                                               expected_rows=report.records)
            report.rows_loaded = load_result.rows_loaded

            # -- only now: mark the files, then drop the parquet ------------- #
            report.files_marked = registry.append_batch(loadable, batch_id, run_id,
                                                        records=report.records)
            if not keep_parquet:
                stage.cleanup_batch(batch_id, success=True)
        except Exception as exc:                        # noqa: BLE001 - one batch never stops the run
            report.status = "FAILED"
            report.error = f"{type(exc).__name__}: {exc}"
            logger.error("batch %s failed: %s - its files stay unprocessed",
                         batch_id, report.error)
        finally:
            report.duration_seconds = round(time.time() - started, 1)
            reports.append(report)
            if on_batch is not None:
                on_batch(report)

    if not reports:
        logger.info("no unprocessed files found under %s",
                    input_path or cfg.path("input.INPUT_PATH"))
    return reports


def process_path(cfg, path: str, **kwargs) -> List[BatchReport]:
    """
    Parse and load everything under ``path`` that is not processed yet.

    The same as :func:`run_batches` with ``input_path=path`` and no batch limit -
    "take this folder and load whatever has not been loaded from it".
    """
    if not os.path.isdir(path):
        raise ManualLoadError(f"not a directory: {path}")
    kwargs.setdefault("max_batches", None)
    return run_batches(cfg, input_path=path, **kwargs)


# --------------------------------------------------------------------------- #
# 2. Path -> Greenplum: write whole parquet files that were not inserted yet
# --------------------------------------------------------------------------- #


def find_parquet_targets(path: str) -> List[str]:
    """
    What to load from ``path``:

    * a ``.parquet`` file            -> that file
    * a Spark parquet directory      -> that directory (loaded as one unit)
    * a folder of the above          -> each of them, sorted
    """
    path = os.path.abspath(path)
    if os.path.isfile(path):
        if not path.endswith(".parquet"):
            raise ManualLoadError(f"not a parquet file: {path}")
        return [path]
    if not os.path.isdir(path):
        raise ManualLoadError(f"path does not exist: {path}")

    entries = sorted(os.listdir(path))
    if any(name.endswith(".parquet") for name in entries):
        return [path]                                   # a parquet directory itself

    targets: List[str] = []
    for name in entries:
        child = os.path.join(path, name)
        if os.path.isdir(child):
            if any(inner.endswith(".parquet") for inner in os.listdir(child)):
                targets.append(child)
        elif name.endswith(".parquet"):
            targets.append(child)
    if not targets:
        raise ManualLoadError(f"no parquet file or directory found under {path}")
    return targets


@dataclass
class PathLoadReport:
    """What :func:`load_parquet_path` did with one parquet target."""

    parquet_path: str
    rows: int = 0
    rows_loaded: int = 0
    duration_seconds: float = 0.0
    status: str = "SUCCESS"
    error: str = ""

    def as_dict(self) -> Dict[str, Any]:
        return self.__dict__.copy()


def load_parquet_path(cfg,
                      path: str,
                      only_new: bool = True,
                      batch_id: Optional[str] = None,
                      run_id: Optional[str] = None,
                      spark=None,
                      loader=None,
                      record: bool = True,
                      stamp_ids: bool = True,
                      on_target: Optional[Callable[[PathLoadReport], None]] = None
                      ) -> List[PathLoadReport]:
    """
    Write the parquet found at ``path`` into Greenplum - the whole of it.

    ``only_new=True`` (default) skips targets already listed in
    ``processed/loaded_parquet.csv``, so pointing this at the same folder again
    inserts only the parquet files that were not inserted before. Set it to
    ``False`` to reload everything under the path.

    The write goes through the same :class:`greenplum_loader.GreenplumLoader` the
    ETL uses, so the configured load strategy, the batch control row and the row
    count verification all apply.

    ``stamp_ids`` (default) rewrites ``ETL_RUN_ID`` and ``BATCH_ID`` with the ids
    of *this* load before writing - the parquet still carries the ids it was
    parsed under, and without the rewrite the control row, the row count check
    and the retry guard would all look for rows that do not carry them. It is a
    ``withColumn(lit(...))``, evaluated inside the JVM, so it ships no Python
    code. Set it to ``False`` to load the rows exactly as they are in the file;
    the row count is then not verified.
    """
    targets = find_parquet_targets(path)
    run_id = run_id or new_run_id()
    loaded = _loaded_registry(cfg)
    reports: List[PathLoadReport] = []

    pending = [target for target in targets
               if not (only_new and loaded.contains(target))]
    skipped = len(targets) - len(pending)
    if skipped:
        logger.info("%d parquet target(s) already loaded - skipping them (only_new=True)",
                    skipped)
    if not pending:
        logger.info("nothing left to load under %s", path)
        return reports

    spark_session = build_spark(cfg, spark)
    gp_loader = build_loader(cfg, spark_session, loader)

    for index, target in enumerate(pending, start=1):
        target_batch_id = batch_id or f"PATH_{index:04d}"
        report = PathLoadReport(parquet_path=target)
        started = time.time()
        try:
            dataframe = spark_session.read.parquet(target)
            report.rows = dataframe.count()
            if stamp_ids:
                from pyspark.sql.functions import lit                  # noqa: PLC0415

                dataframe = (dataframe
                             .withColumn("ETL_RUN_ID", lit(run_id))
                             .withColumn("BATCH_ID", lit(target_batch_id)))
            logger.info("loading %s (%d row(s)) into Greenplum as %s",
                        target, report.rows, target_batch_id)
            result = gp_loader.load_batch(dataframe, target_batch_id, run_id,
                                          file_count=0, parquet_path=target,
                                          expected_rows=report.rows if stamp_ids else None)
            report.rows_loaded = result.rows_loaded
            if record:
                loaded.append(target, report.rows_loaded, target_batch_id, run_id)
        except Exception as exc:                        # noqa: BLE001 - reported per target
            report.status = "FAILED"
            report.error = f"{type(exc).__name__}: {exc}"
            logger.error("loading %s failed: %s", target, report.error)
        finally:
            report.duration_seconds = round(time.time() - started, 1)
            reports.append(report)
            if on_target is not None:
                on_target(report)
    return reports


# --------------------------------------------------------------------------- #
# Reporting helper used by the notebook
# --------------------------------------------------------------------------- #


def print_reports(reports: List[Any]) -> None:
    """One line per batch/target, plus a total."""
    if not reports:
        print("nothing to do")
        return
    first = reports[0]
    if isinstance(first, BatchReport):
        print(f"{'BATCH':<12}{'FILES':>7}{'RECORDS':>10}{'LOADED':>10}{'MARKED':>8}"
              f"{'FAILED':>8}{'SECONDS':>9}  STATUS")
        for report in reports:
            print(f"{report.batch_id:<12}{report.files:>7}{report.records:>10}"
                  f"{report.rows_loaded:>10}{report.files_marked:>8}{report.files_failed:>8}"
                  f"{report.duration_seconds:>9.1f}  {report.status}"
                  + (f"  {report.error}" if report.error else ""))
        print(f"{'TOTAL':<12}{sum(r.files for r in reports):>7}"
              f"{sum(r.records for r in reports):>10}"
              f"{sum(r.rows_loaded for r in reports):>10}"
              f"{sum(r.files_marked for r in reports):>8}"
              f"{sum(r.files_failed for r in reports):>8}"
              f"{sum(r.duration_seconds for r in reports):>9.1f}")
    else:
        print(f"{'ROWS':>10}{'LOADED':>10}{'SECONDS':>9}  PARQUET")
        for report in reports:
            print(f"{report.rows:>10}{report.rows_loaded:>10}{report.duration_seconds:>9.1f}  "
                  f"{report.parquet_path}"
                  + (f"  {report.status}: {report.error}" if report.error else ""))
        print(f"{sum(r.rows_loaded for r in reports):>10} row(s) loaded from "
              f"{len(reports)} parquet target(s)")
