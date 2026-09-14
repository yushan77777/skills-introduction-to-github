"""
ATM E-Journal ETL - batch orchestrator.

    Input files
        -> processed-file check        (processed/processed_files.csv)
        -> unprocessed files
        -> batch selection             (input.BATCH_SIZE)
        -> Spark ETL                   (existing parser, on the executors)
        -> Parquet                     (parquet/batch_nnnn)
        -> Greenplum                   (staging -> target, one transaction)
        -> processed-file tracking     (append-only, after the commit)
        -> parquet cleanup
        -> next batch

The order matters: nothing is marked SUCCESS before Greenplum has committed, and
no parquet is deleted before the tracking CSV has been appended to. A batch that
fails leaves the tracking CSV untouched, so the next run picks its files up
again while every batch that already completed is skipped.

Run it with :mod:`run_etl` (``python3 src/run_etl.py --etl atm_ejournal``) or
from the notebook in ``notebooks/run_atm_ejournal_etl.ipynb``.
"""

from __future__ import annotations

import json
import logging
import os
import sys
import time
import traceback
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from config_loader import EtlConfig, load_config                      # noqa: E402
from file_registry import (DiscoveredFile, PendingBatchStore,          # noqa: E402
                           ProcessedFileRegistry, discover_files,
                           format_batch_id, iter_batches)
from log_manager import LogManager, get_logger                        # noqa: E402
from parquet_stage import ParquetStage, ParquetStageError            # noqa: E402

logger = get_logger("etl")


class EtlStageError(Exception):
    """A failure attributed to one named stage of one batch."""

    def __init__(self, message: str, stage: str, batch_id: Optional[str] = None):
        super().__init__(message)
        self.stage = stage
        self.batch_id = batch_id


@dataclass
class BatchOutcome:
    """Per-batch metrics, written to the run summary and the success e-mail."""

    batch_id: str
    file_count: int = 0
    records: int = 0
    rows_loaded: int = 0
    files_marked: int = 0
    files_failed: int = 0
    duration_seconds: float = 0.0
    parquet_path: str = ""
    status: str = "PENDING"
    stage: str = ""
    error: str = ""

    def as_dict(self) -> Dict[str, Any]:
        return self.__dict__.copy()


@dataclass
class RunSummary:
    """Everything the Airflow e-mails and the audit trail need from a run."""

    run_id: str
    etl_name: str
    environment: str = ""
    status: str = "RUNNING"
    start_time: str = ""
    end_time: str = ""
    duration_seconds: float = 0.0
    duration_human: str = ""
    input_path: str = ""
    batch_size: int = 0
    files_discovered: int = 0
    files_previously_processed: int = 0
    files_to_process: int = 0
    batches_planned: int = 0
    batches_processed: int = 0
    batches_failed: int = 0
    files_processed: int = 0
    files_failed: int = 0
    records_processed: int = 0
    records_loaded: int = 0
    greenplum_table: str = ""
    log_path: str = ""
    summary_path: str = ""
    dag_id: str = ""
    execution_date: str = ""
    stage: str = ""
    batch_id: str = ""
    error: str = ""
    stack_trace: str = ""
    parser_stats: Dict[str, int] = field(default_factory=dict)
    log_cleanup: Dict[str, Any] = field(default_factory=dict)
    recovered_batches: List[Dict[str, Any]] = field(default_factory=list)
    failed_files: List[Dict[str, str]] = field(default_factory=list)
    batches: List[Dict[str, Any]] = field(default_factory=list)

    def as_dict(self) -> Dict[str, Any]:
        return {key: value for key, value in self.__dict__.items()}


def _human_duration(seconds: float) -> str:
    seconds = int(seconds)
    hours, remainder = divmod(seconds, 3600)
    minutes, secs = divmod(remainder, 60)
    return f"{hours:02d}:{minutes:02d}:{secs:02d}"


class AtmEjournalEtl:
    """
    One ETL execution.

    ``run()`` is restartable: it recovers in-flight batches from a previous run,
    skips files already in the tracking CSV, and processes the rest in batches of
    ``input.BATCH_SIZE``.
    """

    def __init__(self,
                 cfg: EtlConfig,
                 run_id: Optional[str] = None,
                 dry_run: bool = False,
                 max_batches: Optional[int] = None):
        self.cfg = cfg
        self.run_id = (run_id or os.environ.get("ATM_ETL_RUN_ID")
                       or datetime.now().strftime("%Y%m%d_%H%M%S"))
        self.dry_run = dry_run
        self.max_batches = (max_batches if max_batches is not None
                            else cfg.get_int("input.MAX_BATCHES_PER_RUN", 0))

        self.log_manager = LogManager(cfg, run_id=self.run_id)
        self.summary = RunSummary(run_id=self.run_id, etl_name=cfg.etl_name,
                                  environment=cfg.environment)
        self.registry = ProcessedFileRegistry(
            cfg.path("tracking.PROCESSED_FILES_CSV", "processed/processed_files.csv"),
            key_mode=str(cfg.get("tracking.FILE_KEY_MODE", "path")))
        self.pending = PendingBatchStore(cfg.path("tracking.PENDING_DIR", "processed/pending"))
        self.spark = None
        self.parquet: Optional[ParquetStage] = None
        self.loader = None

    # -- entry point -------------------------------------------------------- #

    def run(self) -> RunSummary:
        started = time.time()
        self.summary.start_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        self.summary.dag_id = os.environ.get("ATM_ETL_DAG_ID", "")
        self.summary.execution_date = os.environ.get("ATM_ETL_EXECUTION_DATE", "")

        log_path = self.log_manager.start_run()
        self.summary.log_path = log_path
        try:
            self._log_run_header()
            self.summary.log_cleanup = self.log_manager.cleanup().as_dict()

            self._prepare_stage("startup")
            self._cleanup_stale_parquet()
            self._recover_pending_batches()
            self._process_all_batches()

            self.summary.status = "SUCCESS" if not self.summary.batches_failed else "FAILED"
            if self.summary.batches_failed:
                self.summary.error = (f"{self.summary.batches_failed} batch(es) failed - see the "
                                      "batch logs; their files stay unprocessed")
        except EtlStageError as exc:
            self._record_failure(exc, stage=exc.stage, batch_id=exc.batch_id)
        except Exception as exc:                       # noqa: BLE001 - reported, then re-raised
            self._record_failure(exc, stage=self.summary.stage or "run")
        finally:
            self.summary.duration_seconds = round(time.time() - started, 1)
            self.summary.duration_human = _human_duration(self.summary.duration_seconds)
            self.summary.end_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            self._log_run_footer()
            self.summary.summary_path = self._write_summary()
            self._stop_spark()
            self.log_manager.close()
        return self.summary

    # -- stages ------------------------------------------------------------- #

    def _log_run_header(self) -> None:
        logger.info("=" * 78)
        logger.info("ATM E-Journal ETL started | etl=%s | run_id=%s | environment=%s",
                    self.cfg.etl_name, self.run_id, self.cfg.environment)
        logger.info("start time      : %s", self.summary.start_time)
        logger.info("configuration   : %s", self.cfg.config_path)
        logger.info("input directory : %s", self.cfg.path("input.INPUT_PATH"))
        logger.info("batch size      : %s", self.cfg.get_int("input.BATCH_SIZE"))
        logger.info("parquet path    : %s", self.cfg.path("parquet.PARQUET_PATH"))
        logger.info("tracking CSV    : %s", self.registry.csv_path)
        logger.info("greenplum       : %s -> %s.%s (user=%s)",
                    self.cfg.get("greenplum.GREENPLUM_URL"),
                    self.cfg.get("greenplum.GREENPLUM_SCHEMA"),
                    self.cfg.get("greenplum.GREENPLUM_TABLE"),
                    self.cfg.get("greenplum.GREENPLUM_USER"))
        logger.info("load strategy   : %s", self.cfg.get("greenplum.GREENPLUM_LOAD_STRATEGY"))
        logger.info("dry run         : %s", self.dry_run)
        logger.debug("effective configuration (secrets masked): %s",
                     json.dumps(self.cfg.safe_dump(), indent=2, default=str))
        logger.info("=" * 78)
        self.summary.input_path = self.cfg.path("input.INPUT_PATH")
        self.summary.batch_size = self.cfg.get_int("input.BATCH_SIZE")
        self.summary.greenplum_table = (f"{self.cfg.get('greenplum.GREENPLUM_SCHEMA')}."
                                        f"{self.cfg.get('greenplum.GREENPLUM_TABLE')}")

    def _prepare_stage(self, stage: str) -> None:
        self.summary.stage = stage
        logger.debug("stage: %s", stage)

    def _ensure_spark(self):
        """Start Spark on first use - an empty run never starts a cluster job."""
        if self.spark is not None:
            return self.spark
        from spark_session import build_spark_session, ship_python_modules    # noqa: PLC0415

        self._prepare_stage("spark_session")
        self.spark = build_spark_session(self.cfg)
        ship_python_modules(self.spark)
        self.parquet = ParquetStage(self.cfg, self.spark)
        self.parquet.ensure_root()
        return self.spark

    def _ensure_loader(self):
        if self.loader is None:
            from greenplum_loader import GreenplumLoader                      # noqa: PLC0415
            self.loader = GreenplumLoader(self.cfg, spark=self.spark)
        elif self.loader.spark is None:
            self.loader.spark = self.spark
        return self.loader

    def _cleanup_stale_parquet(self) -> None:
        """
        Sweep parquet left behind by batches that failed in earlier runs, once it
        is older than ``PARQUET_FAILED_RETENTION_DAYS``. Directories named in a
        pending marker are kept - they may still be needed for recovery.
        """
        try:
            stage = self.parquet or ParquetStage(self.cfg, self.spark)
            keep = {str(marker.get("batch_id", "")) for marker in self.pending.list_pending()}
            stage.cleanup_stale_batches(keep_batch_ids=keep)
        except Exception as exc:                       # noqa: BLE001 - housekeeping only
            logger.warning("stale parquet sweep skipped: %s", exc)

    # -- recovery ----------------------------------------------------------- #

    def _recover_pending_batches(self) -> None:
        """
        Resolve batches that were in flight when a previous run died.

        A pending marker means "the Greenplum load for this batch was started".
        The batch control table in Greenplum is the authority: if the batch
        committed, the tracking CSV is completed now (no reload); if it did not,
        the marker is cleared and the files are simply picked up again.
        """
        self._prepare_stage("recovery")
        markers = self.pending.list_pending()
        if not markers:
            logger.debug("no pending batch markers - nothing to recover")
            return
        logger.warning("%d pending batch marker(s) found from an earlier run - resolving them "
                       "against the Greenplum batch control table", len(markers))
        if self.dry_run:
            logger.warning("dry run: pending markers are left untouched")
            return

        # Spark is started first: without psycopg2 on the edge node the control
        # connection borrows the JDBC driver from the Spark JVM.
        self._ensure_spark()
        loader = self._ensure_loader()
        for marker in markers:
            batch_id = str(marker.get("batch_id", "UNKNOWN"))
            run_id = str(marker.get("run_id", ""))
            files = PendingBatchStore.files_from_marker(marker)
            with self.log_manager.batch_log(f"{batch_id}_recovery"):
                rows = loader.is_batch_committed(run_id, batch_id)
                if rows is not None:
                    logger.warning("recovery | batch=%s run=%s committed %d row(s) in Greenplum "
                                   "but was not recorded in the tracking CSV - completing the "
                                   "bookkeeping now (no reload)", batch_id, run_id, rows)
                    marked = self.registry.append_batch(files, batch_id, run_id, records=rows)
                    self.pending.remove(batch_id)
                    if self.parquet is None:
                        self.parquet = ParquetStage(self.cfg, self.spark)
                    self.parquet.cleanup_batch(batch_id, success=True)
                    self.summary.recovered_batches.append(
                        {"batch_id": batch_id, "run_id": run_id, "rows": rows,
                         "files_marked": marked, "action": "COMPLETED_TRACKING"})
                    self.summary.files_processed += marked
                    self.summary.records_loaded += rows
                else:
                    logger.warning("recovery | batch=%s run=%s never committed in Greenplum - "
                                   "its %d file(s) stay unprocessed and will be picked up by "
                                   "this run", batch_id, run_id, len(files))
                    self.pending.remove(batch_id)
                    self.summary.recovered_batches.append(
                        {"batch_id": batch_id, "run_id": run_id, "rows": 0,
                         "files_marked": 0, "action": "REPROCESS"})

    # -- discovery and batching --------------------------------------------- #

    def _discovery_stream(self):
        """Lazy stream of files that still need processing."""
        return discover_files(
            input_path=self.cfg.path("input.INPUT_PATH"),
            patterns=self.cfg.get_list("input.FILE_PATTERN"),
            folder_depth=self.cfg.get_int("input.ATM_FOLDER_DEPTH", 1),
            min_file_age_seconds=self.cfg.get_int("input.MIN_FILE_AGE_SECONDS", 0),
            sniff_unknown_extensions=self.cfg.get_bool("input.SNIFF_UNKNOWN_EXTENSIONS", False),
        )

    def _prescan(self) -> None:
        """
        Count what is in the directory before processing, for the log and the
        success e-mail. Metadata only - no file is opened. Can be switched off
        (``input.PRESCAN_ENABLED``) for directories where even a metadata walk
        is expensive.
        """
        if not self.cfg.get_bool("input.PRESCAN_ENABLED", True):
            logger.info("file pre-scan disabled (input.PRESCAN_ENABLED)")
            return
        key_mode = self.registry.key_mode
        processed_keys = self.registry.load_keys()
        discovered = 0
        already = 0
        for item in self._discovery_stream():
            discovered += 1
            if item.key(key_mode) in processed_keys:
                already += 1
        batch_size = self.cfg.get_int("input.BATCH_SIZE")
        remaining = discovered - already
        self.summary.files_discovered = discovered
        self.summary.files_previously_processed = already
        self.summary.files_to_process = remaining
        self.summary.batches_planned = (remaining + batch_size - 1) // batch_size

        logger.info("file discovery | total files discovered : %d", discovered)
        logger.info("file discovery | previously processed   : %d", already)
        logger.info("file discovery | remaining to process   : %d", remaining)
        logger.info("file discovery | batch size             : %d", batch_size)
        logger.info("file discovery | number of batches      : %d", self.summary.batches_planned)

    def _unprocessed_stream(self):
        key_mode = self.registry.key_mode
        processed_keys = self.registry.load_keys()
        for item in self._discovery_stream():
            if item.key(key_mode) in processed_keys:
                continue
            yield item

    def _process_all_batches(self) -> None:
        self._prepare_stage("discovery")
        self.registry.ensure_file()
        self._prescan()

        batch_size = self.cfg.get_int("input.BATCH_SIZE")
        processed_batches = 0

        for sequence, batch in enumerate(iter_batches(self._unprocessed_stream(), batch_size),
                                         start=1):
            if self.max_batches and processed_batches >= self.max_batches:
                logger.warning("stopping after %d batch(es) (input.MAX_BATCHES_PER_RUN) - "
                               "the remaining files are processed by the next run",
                               self.max_batches)
                break
            batch_id = format_batch_id(sequence)
            outcome = self._process_batch(batch_id, batch)
            self.summary.batches.append(outcome.as_dict())
            processed_batches += 1
            if outcome.status in ("SUCCESS", "DRY_RUN"):
                self.summary.batches_processed += 1
            else:
                self.summary.batches_failed += 1

        if not self.summary.batches:
            logger.info("no unprocessed files found - nothing to do")

    # -- one batch ----------------------------------------------------------- #

    def _process_batch(self, batch_id: str, batch: List[DiscoveredFile]) -> BatchOutcome:
        outcome = BatchOutcome(batch_id=batch_id, file_count=len(batch))
        started = time.time()

        with self.log_manager.batch_log(batch_id):
            logger.info("-" * 78)
            logger.info("batch started | batch=%s | files=%d | start=%s",
                        batch_id, len(batch), datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
            logger.info("batch files   | first=%s | last=%s",
                        batch[0].relative_path, batch[-1].relative_path)
            try:
                outcome = self._run_batch_stages(batch_id, batch, outcome)
                if outcome.status != "DRY_RUN":
                    outcome.status = "SUCCESS"
                outcome.duration_seconds = round(time.time() - started, 1)
                logger.info("batch completed successfully | batch=%s | files processed=%d | "
                            "records=%d | rows loaded=%d | duration=%.1fs",
                            batch_id, outcome.files_marked, outcome.records,
                            outcome.rows_loaded, outcome.duration_seconds)
            except Exception as exc:                   # noqa: BLE001 - one batch never kills the run
                outcome.status = "FAILED"
                outcome.duration_seconds = round(time.time() - started, 1)
                outcome.stage = getattr(exc, "stage", self.summary.stage)
                outcome.error = f"{type(exc).__name__}: {exc}"
                logger.error("batch FAILED | batch=%s | stage=%s | files=%d | successful files "
                             "in this batch=0 | failed files=%d | duration=%.1fs",
                             batch_id, outcome.stage, len(batch), len(batch),
                             outcome.duration_seconds)
                logger.error("batch %s error: %s", batch_id, outcome.error)
                logger.error("batch %s stack trace:\n%s", batch_id, traceback.format_exc())
                logger.warning("batch %s: no file was marked as processed, the next run will "
                               "retry them", batch_id)
                if self.parquet is not None:
                    self.parquet.cleanup_batch(batch_id, success=False)
        return outcome

    def _run_batch_stages(self, batch_id: str, batch: List[DiscoveredFile],
                          outcome: BatchOutcome) -> BatchOutcome:
        from spark_parser import build_batch_dataframe                        # noqa: PLC0415
        from spark_session import default_parse_partitions                    # noqa: PLC0415

        self._ensure_spark()
        assert self.parquet is not None

        # ---- 1. parse ------------------------------------------------------ #
        self._prepare_stage("spark_read")
        self.summary.batch_id = batch_id
        logger.info("reading text files | batch=%s | files=%d | partitions=%d",
                    batch_id, len(batch), default_parse_partitions(self.cfg, len(batch)))
        logger.info("transformations started | batch=%s", batch_id)
        dataframe, stats_accumulator, failure_accumulator = build_batch_dataframe(
            self.spark, batch, self.cfg, batch_id, self.run_id,
            num_partitions=default_parse_partitions(self.cfg, len(batch)))

        # ---- 2. parquet ---------------------------------------------------- #
        self._prepare_stage("parquet_write")
        self.parquet.prepare_for_batch(batch_id)
        try:
            parquet_path, record_count = self.parquet.write_batch(dataframe, batch_id)
        except ParquetStageError as exc:
            raise EtlStageError(str(exc), stage="parquet_write", batch_id=batch_id) from exc
        outcome.parquet_path = parquet_path
        outcome.records = record_count
        logger.info("transformations completed | batch=%s | records=%d", batch_id, record_count)

        # Parser counters and per-file failures are now available (the parquet
        # write was the action that ran the parse).
        stats = dict(stats_accumulator.value or {})
        for key, value in stats.items():
            self.summary.parser_stats[key] = self.summary.parser_stats.get(key, 0) + value
        logger.info("parser counters | batch=%s | %s", batch_id, json.dumps(stats, sort_keys=True))

        failures = list(failure_accumulator.value or [])
        failed_keys = {entry.get("file_key") for entry in failures}
        if failures:
            outcome.files_failed = len(failures)
            self.summary.files_failed += len(failures)
            for entry in failures:
                logger.error("file failed to parse | batch=%s | file=%s\n%s",
                             batch_id, entry.get("path"), entry.get("error"))
                self.summary.failed_files.append({"batch_id": batch_id,
                                                  "path": str(entry.get("path")),
                                                  "error": str(entry.get("error"))[:500]})
            logger.warning("batch %s: %d file(s) failed to parse and will NOT be marked as "
                           "processed", batch_id, len(failures))

        loadable = [item for item in batch
                    if item.key(self.registry.key_mode) not in failed_keys]

        if self.dry_run:
            logger.warning("dry run | batch=%s | parquet written to %s, Greenplum load and "
                           "processed-file tracking skipped", batch_id, parquet_path)
            outcome.status = "DRY_RUN"
            return outcome

        # ---- 3. pending marker (written before anything reaches Greenplum) -- #
        self._prepare_stage("pending_marker")
        self.pending.write(batch_id, self.run_id, loadable, parquet_path, record_count)

        # ---- 4. Greenplum load from the parquet, not from memory ----------- #
        self._prepare_stage("greenplum_load")
        loader = self._ensure_loader()
        parquet_dataframe = self.parquet.read_batch(parquet_path)
        logger.info("Greenplum load started | batch=%s | source=%s | expected rows=%d",
                    batch_id, parquet_path, record_count)
        try:
            load_result = loader.load_batch(parquet_dataframe, batch_id, self.run_id,
                                            file_count=len(loadable),
                                            parquet_path=parquet_path,
                                            expected_rows=record_count)
        except Exception as exc:                       # noqa: BLE001
            raise EtlStageError(f"{type(exc).__name__}: {exc}", stage="greenplum_load",
                                batch_id=batch_id) from exc
        outcome.rows_loaded = load_result.rows_loaded
        logger.info("Greenplum load completed | batch=%s | rows loaded=%d | rows replaced=%d | "
                    "strategy=%s | %.1fs", batch_id, load_result.rows_loaded,
                    load_result.rows_deleted, load_result.strategy,
                    load_result.duration_seconds)
        for warning in load_result.warnings:
            logger.warning("Greenplum load warning | batch=%s | %s", batch_id, warning)
        self.summary.records_processed += record_count
        self.summary.records_loaded += max(load_result.rows_loaded, 0)

        # ---- 5. tracking (only now, after the commit) ---------------------- #
        self._prepare_stage("processed_tracking")
        try:
            outcome.files_marked = self.registry.append_batch(
                loadable, batch_id, self.run_id, records=record_count)
        except Exception as exc:                       # noqa: BLE001
            # The data is committed; the marker stays so the next run can finish
            # the bookkeeping from the control table instead of reloading.
            logger.error("batch %s: Greenplum load committed but the processed-file CSV could "
                         "not be updated (%s). The pending marker is kept - the next run will "
                         "complete the tracking from the Greenplum batch control table without "
                         "reloading the data.", batch_id, exc)
            raise EtlStageError(f"{type(exc).__name__}: {exc}", stage="processed_tracking",
                                batch_id=batch_id) from exc
        self.summary.files_processed += outcome.files_marked

        # ---- 6. cleanup ----------------------------------------------------- #
        self._prepare_stage("parquet_cleanup")
        self.pending.remove(batch_id)
        self.parquet.cleanup_batch(batch_id, success=True)
        return outcome

    # -- finishing ---------------------------------------------------------- #

    def _record_failure(self, exc: Exception, stage: str, batch_id: Optional[str] = None) -> None:
        self.summary.status = "FAILED"
        self.summary.stage = stage
        self.summary.batch_id = batch_id or self.summary.batch_id
        self.summary.error = f"{type(exc).__name__}: {exc}"
        self.summary.stack_trace = traceback.format_exc()
        logger.error("ETL FAILED | stage=%s | batch=%s | %s",
                     stage, self.summary.batch_id or "-", self.summary.error)
        logger.error("stack trace:\n%s", self.summary.stack_trace)

    def _log_run_footer(self) -> None:
        logger.info("=" * 78)
        logger.info("ATM E-Journal ETL finished | status=%s | duration=%s",
                    self.summary.status, self.summary.duration_human)
        logger.info("files discovered=%d | previously processed=%d | processed now=%d | "
                    "failed=%d", self.summary.files_discovered,
                    self.summary.files_previously_processed, self.summary.files_processed,
                    self.summary.files_failed)
        logger.info("batches processed=%d | batches failed=%d | records=%d | rows loaded=%d",
                    self.summary.batches_processed, self.summary.batches_failed,
                    self.summary.records_processed, self.summary.records_loaded)
        logger.info("=" * 78)

    def _write_summary(self) -> str:
        directory = self.cfg.path("tracking.RUN_SUMMARY_DIR", "processed/run_summary")
        try:
            os.makedirs(directory, exist_ok=True)
            path = os.path.join(directory, f"{self.cfg.etl_name}_{self.run_id}.json")
            payload = self.summary.as_dict()
            payload["summary_path"] = path
            with open(path, "w", encoding="utf-8") as handle:
                json.dump(payload, handle, indent=2, default=str)
            latest = os.path.join(directory, f"{self.cfg.etl_name}_latest.json")
            with open(latest, "w", encoding="utf-8") as handle:
                json.dump(payload, handle, indent=2, default=str)
            logger.info("run summary written: %s", path)
            return path
        except OSError as exc:
            logger.error("run summary could not be written to %s: %s", directory, exc)
            return ""

    def _stop_spark(self) -> None:
        if self.spark is None:
            return
        from spark_session import stop_spark_session                          # noqa: PLC0415
        stop_spark_session(self.spark)
        self.spark = None


def run_etl(config_path: Optional[str] = None,
            etl_name: Optional[str] = None,
            run_id: Optional[str] = None,
            dry_run: bool = False,
            max_batches: Optional[int] = None,
            overrides: Optional[Dict[str, Any]] = None) -> RunSummary:
    """Convenience entry point used by the CLI, the DAG and the notebook."""
    cfg = load_config(config_path, etl_name, overrides=overrides)
    return AtmEjournalEtl(cfg, run_id=run_id, dry_run=dry_run, max_batches=max_batches).run()
