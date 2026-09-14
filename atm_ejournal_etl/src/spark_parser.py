"""
Spark bridge for the existing ATM e-journal parser.

The parsing business logic is **not** re-implemented here. Every file is handed
to the functions already shipped with the project -
``read_ejournal_file`` -> ``extract_transaction_blocks`` -> ``deduplicate_attempts`` -
only now they run on the executors, one file at a time, instead of on the driver
for the whole directory.

Why per-file de-duplication is equivalent to the folder-level call the pandas
version makes: a retry chain is built from records sharing a ``SESSION_ID``, and
``extract_transaction_blocks`` namespaces every session id with the file it came
from (``<file>#S00001``). Records from two different files can therefore never
belong to the same chain, so collapsing retries per file yields exactly the same
rows as collapsing them per ATM folder - while keeping memory bounded by one
file instead of one folder.

The resulting rows are typed with a fixed :data:`WITHDRAWAL_SCHEMA` so the
parquet layout - and the Greenplum table - stay stable from batch to batch. The
pandas version's dynamic ``NOTES_<value>`` columns cannot be part of a fixed
schema; the same information is carried by ``DENOMINATION`` and by
``DENOM_BREAKDOWN`` (JSON), from which per-note columns or the long format can
be derived in SQL.
"""

from __future__ import annotations

import functools
import json
import logging
import os
import traceback
from datetime import datetime
from typing import Any, Dict, Iterable, Iterator, List, Optional, Tuple

logger = logging.getLogger("atm_ejournal.parse")


class SparkSerializationError(Exception):
    """
    Spark could not serialise the Python code of the job.

    Almost always a PySpark/Python version mismatch on the driver rather than a
    problem with this ETL - :mod:`check_environment` explains which.
    """


def build_schema():
    """Fixed output schema (deferred import so the module loads without Spark)."""
    from pyspark.sql.types import (BooleanType, DateType, DoubleType, IntegerType,
                                   LongType, StringType, StructField, StructType,
                                   TimestampType)

    return StructType([
        StructField("ATM_NO", StringType(), True),
        StructField("TRANSACTION_DATETIME", TimestampType(), True),
        StructField("DATE", DateType(), True),
        StructField("TIME", StringType(), True),
        StructField("RESPONSE_DATETIME", TimestampType(), True),
        StructField("ACCOUNT_NO", StringType(), True),
        StructField("CARD_NO", StringType(), True),
        StructField("AMOUNT", DoubleType(), True),
        StructField("REQUESTED_AMOUNT", DoubleType(), True),
        StructField("CURRENCY", StringType(), True),
        StructField("STATUS", StringType(), True),
        StructField("RESPONSE_CODE", StringType(), True),
        StructField("ACTION_CODE", StringType(), True),
        StructField("TRANSACTION_REF", StringType(), True),
        StructField("AUX_SEQ", StringType(), True),
        StructField("TRACE_ID", StringType(), True),
        StructField("TERMINAL_ID", StringType(), True),
        StructField("CARD_SCHEME", StringType(), True),
        StructField("FAST_CASH", BooleanType(), True),
        StructField("DISPENSE_RESULT", StringType(), True),
        StructField("DISPENSED_AMOUNT", DoubleType(), True),
        StructField("DENOMINATION", StringType(), True),
        StructField("DENOM_BREAKDOWN", StringType(), True),
        StructField("NOTES_COUNT", IntegerType(), True),
        StructField("DENOM_AMOUNT", DoubleType(), True),
        StructField("DENOM_MATCHES_AMOUNT", BooleanType(), True),
        StructField("PLANNED_DENOMINATION", StringType(), True),
        StructField("MIX_NUMBER", StringType(), True),
        StructField("CASH_TAKEN", BooleanType(), True),
        StructField("TRX_ERROR", StringType(), True),
        StructField("ATTEMPT_NO", IntegerType(), True),
        StructField("ATTEMPT_COUNT", IntegerType(), True),
        StructField("IS_RETRY", BooleanType(), True),
        StructField("SESSION_ID", StringType(), True),
        StructField("TXN_SEQ", IntegerType(), True),
        StructField("PARSE_CONFIDENT", BooleanType(), True),
        StructField("RAW_BLOCK", StringType(), True),
        StructField("SOURCE_FILE", StringType(), True),
        StructField("SOURCE_PATH", StringType(), True),
        StructField("SOURCE_FILE_KEY", StringType(), True),
        StructField("SOURCE_LINE", LongType(), True),
        StructField("BATCH_ID", StringType(), True),
        StructField("ETL_RUN_ID", StringType(), True),
        StructField("ETL_NAME", StringType(), True),
        StructField("LOAD_TS", TimestampType(), True),
    ])


#: Column order of the rows produced by :func:`parse_journal_file`.
COLUMN_ORDER = [
    "ATM_NO", "TRANSACTION_DATETIME", "DATE", "TIME", "RESPONSE_DATETIME",
    "ACCOUNT_NO", "CARD_NO", "AMOUNT", "REQUESTED_AMOUNT", "CURRENCY", "STATUS",
    "RESPONSE_CODE", "ACTION_CODE", "TRANSACTION_REF", "AUX_SEQ", "TRACE_ID",
    "TERMINAL_ID", "CARD_SCHEME", "FAST_CASH", "DISPENSE_RESULT", "DISPENSED_AMOUNT",
    "DENOMINATION", "DENOM_BREAKDOWN", "NOTES_COUNT", "DENOM_AMOUNT",
    "DENOM_MATCHES_AMOUNT", "PLANNED_DENOMINATION", "MIX_NUMBER", "CASH_TAKEN",
    "TRX_ERROR", "ATTEMPT_NO", "ATTEMPT_COUNT", "IS_RETRY", "SESSION_ID", "TXN_SEQ",
    "PARSE_CONFIDENT", "RAW_BLOCK", "SOURCE_FILE", "SOURCE_PATH", "SOURCE_FILE_KEY",
    "SOURCE_LINE", "BATCH_ID", "ETL_RUN_ID", "ETL_NAME", "LOAD_TS",
]

#: Parser counters aggregated across the batch (subset of ParseStats).
STAT_KEYS = (
    "files_processed", "files_failed", "lines_read", "lines_unrecognised",
    "card_sessions", "withdrawal_records_detected", "successful_withdrawals",
    "failed_withdrawals", "unknown_status_withdrawals", "withdrawals_with_denomination",
    "successful_without_denomination", "denomination_amount_mismatches",
    "removed_repeat_attempts", "records_unparsed",
)


def _as_bool(value: Any) -> Optional[bool]:
    return None if value is None else bool(value)


def _as_int(value: Any) -> Optional[int]:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _as_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def record_to_row(record: Dict[str, Any],
                  file_key: str,
                  batch_id: str,
                  run_id: str,
                  etl_name: str,
                  load_ts: datetime) -> Tuple:
    """Map one parser record onto :data:`COLUMN_ORDER`."""
    transaction_dt = record.get("TRANSACTION_DATETIME")
    breakdown = record.get("DENOM_BREAKDOWN")
    amount = _as_float(record.get("AMOUNT"))
    denom_amount = _as_float(record.get("DENOM_AMOUNT"))
    matches = None
    if denom_amount is not None and amount is not None:
        matches = abs(denom_amount - amount) <= 0.01

    return (
        record.get("ATM_NO"),
        transaction_dt,
        transaction_dt.date() if isinstance(transaction_dt, datetime) else None,
        transaction_dt.strftime("%H:%M:%S") if isinstance(transaction_dt, datetime) else None,
        record.get("RESPONSE_DATETIME"),
        record.get("ACCOUNT_NO"),
        record.get("CARD_NO"),
        amount,
        _as_float(record.get("REQUESTED_AMOUNT")),
        record.get("CURRENCY"),
        record.get("STATUS"),
        record.get("RESPONSE_CODE"),
        record.get("ACTION_CODE"),
        record.get("TRANSACTION_REF"),
        record.get("AUX_SEQ"),
        record.get("TRACE_ID"),
        record.get("TERMINAL_ID"),
        record.get("CARD_SCHEME"),
        _as_bool(record.get("FAST_CASH")),
        record.get("DISPENSE_RESULT"),
        _as_float(record.get("DISPENSED_AMOUNT")),
        record.get("DENOMINATION"),
        json.dumps({str(k): int(v) for k, v in breakdown.items()}, sort_keys=True)
        if isinstance(breakdown, dict) and breakdown else None,
        _as_int(record.get("NOTES_COUNT")),
        denom_amount,
        matches,
        record.get("PLANNED_DENOMINATION"),
        record.get("MIX_NUMBER"),
        _as_bool(record.get("CASH_TAKEN")),
        record.get("TRX_ERROR"),
        _as_int(record.get("ATTEMPT_NO")),
        _as_int(record.get("ATTEMPT_COUNT")),
        _as_bool(record.get("IS_RETRY")),
        record.get("SESSION_ID"),
        _as_int(record.get("TXN_SEQ")),
        _as_bool(record.get("PARSE_CONFIDENT")),
        record.get("RAW_BLOCK"),
        record.get("SOURCE_FILE"),
        record.get("SOURCE_PATH"),
        file_key,
        _as_int(record.get("SOURCE_LINE")),
        batch_id,
        run_id,
        etl_name,
        load_ts,
    )


def parse_journal_file(path: str,
                       atm_no: str,
                       file_key: str,
                       options: Dict[str, Any]) -> Tuple[List[Tuple], Dict[str, int], Optional[str]]:
    """
    Parse one journal file with the existing parser.

    Returns ``(rows, stats, error)``; ``error`` is ``None`` on success and a
    short description otherwise. Raising is deliberately avoided so one bad file
    cannot fail a whole batch - but the file is reported and never marked as
    processed.
    """
    from atm_ejournal_parser import (ParseStats, deduplicate_attempts,        # noqa: PLC0415
                                     extract_transaction_blocks, read_ejournal_file)

    stats = ParseStats()
    try:
        lines = read_ejournal_file(path, stats, encoding=options.get("encoding", "latin-1"))
        records = extract_transaction_blocks(lines, path, stats)
        for record in records:
            record["ATM_NO"] = str(atm_no)
            # Same namespacing the folder-level parser applies, so session ids
            # stay unique across ATMs.
            record["SESSION_ID"] = f"{atm_no}|{record['SESSION_ID']}"

        kept = deduplicate_attempts(
            records,
            stats,
            keep_last_failure=options.get("keep_last_failure", True),
            link_failed_across_amounts=options.get("link_failed_across_amounts", False),
            retry_window_seconds=options.get("retry_window_seconds", 180),
        )
        if not options.get("keep_unparsed_records", True):
            kept = [record for record in kept if record.get("PARSE_CONFIDENT", True)]

        load_ts = options["load_ts"]
        rows = [record_to_row(record, file_key, options["batch_id"], options["run_id"],
                              options["etl_name"], load_ts)
                for record in kept]
        stats.files_processed += 1
        return rows, {key: getattr(stats, key, 0) for key in STAT_KEYS}, None
    except Exception as exc:                           # noqa: BLE001 - reported per file
        stats.files_failed += 1
        detail = f"{os.path.basename(path)}: {type(exc).__name__}: {exc}"
        return ([], {key: getattr(stats, key, 0) for key in STAT_KEYS},
                f"{detail}\n{traceback.format_exc(limit=5)}")


# --------------------------------------------------------------------------- #
# Executor-side entry points
#
# Everything Spark has to ship is defined at module level: a nested closure or a
# class defined inside a function is serialised *by value* (its byte code is
# pickled), which is exactly what breaks on a PySpark whose bundled cloudpickle
# is older than the Python it runs on. Module level objects are pickled *by
# reference* instead - the executor simply imports this module, which the ETL
# ships with ``addPyFile``.
# --------------------------------------------------------------------------- #


def parse_partition(partition: Iterable[Tuple[str, str, str]],
                    options: Optional[Dict[str, Any]] = None,
                    stats_accumulator=None,
                    failure_accumulator=None) -> Iterator[Tuple]:
    """
    Parse one partition of ``(path, atm_no, file_key)`` tuples.

    Bound to its arguments with :func:`functools.partial` before it is handed to
    ``mapPartitions``. Counters and per-file failures travel back to the driver
    through accumulators, so no extra Spark action is needed to collect them. A
    failed file is reported with its file key, so the driver can exclude exactly
    that file from the processed-file tracking while the rest of the batch still
    loads.
    """
    options = options or {}
    for path, atm_no, file_key in partition:
        rows, stats, error = parse_journal_file(path, atm_no, file_key, options)
        if stats_accumulator is not None:
            stats_accumulator.add(stats)
        if error and failure_accumulator is not None:
            failure_accumulator.add([{"file_key": file_key, "path": path, "error": error}])
        for row in rows:
            yield row


def make_partition_parser(options: Dict[str, Any], stats_accumulator, failure_accumulator):
    """Bind :func:`parse_partition` to its arguments (picklable by reference)."""
    return functools.partial(parse_partition,
                             options=options,
                             stats_accumulator=stats_accumulator,
                             failure_accumulator=failure_accumulator)


# --------------------------------------------------------------------------- #
# Accumulators
# --------------------------------------------------------------------------- #

try:                                                   # module stays importable
    from pyspark import AccumulatorParam as _AccumulatorParam
except Exception:                                      # noqa: BLE001 - no Spark installed
    _AccumulatorParam = object                         # type: ignore


class DictAccumulatorParam(_AccumulatorParam):
    """Sums the parser counters of every task into one dict."""

    def zero(self, value):
        return dict(value)

    def addInPlace(self, left, right):
        for key, amount in (right or {}).items():
            left[key] = left.get(key, 0) + amount
        return left


class ListAccumulatorParam(_AccumulatorParam):
    """Collects the per-file failures reported by the tasks."""

    def zero(self, value):
        return list(value)

    def addInPlace(self, left, right):
        left.extend(right or [])
        return left


def build_accumulators(spark):
    """Create the (stats, failures) accumulators used by a batch parse."""
    stats = spark.sparkContext.accumulator({key: 0 for key in STAT_KEYS},
                                           DictAccumulatorParam())
    failures = spark.sparkContext.accumulator([], ListAccumulatorParam())
    return stats, failures


def build_batch_dataframe(spark, files, cfg, batch_id: str, run_id: str,
                          num_partitions: Optional[int] = None):
    """
    Turn a batch of :class:`file_registry.DiscoveredFile` into a Spark DataFrame.

    Only the file *paths* are distributed; the journal text itself is read on
    the executor that parses it, so neither the driver nor a single executor
    ever holds the whole batch.
    """
    options = {
        "encoding": str(cfg.get("input.ENCODING", "latin-1")),
        "keep_last_failure": cfg.get_bool("parser.KEEP_LAST_FAILURE", True),
        "link_failed_across_amounts": cfg.get_bool("parser.LINK_FAILED_ACROSS_AMOUNTS", False),
        "retry_window_seconds": cfg.get_int("parser.RETRY_WINDOW_SECONDS", 180),
        "keep_unparsed_records": cfg.get_bool("parser.KEEP_UNPARSED_RECORDS", True),
        "batch_id": batch_id,
        "run_id": run_id,
        "etl_name": cfg.etl_name,
        "load_ts": datetime.now(),
    }

    key_mode = str(cfg.get("tracking.FILE_KEY_MODE", "path"))
    payload = [(item.path, item.atm_no, item.key(key_mode)) for item in files]
    partitions = num_partitions or max(1, min(len(payload), 8))

    stats_accumulator, failure_accumulator = build_accumulators(spark)
    rdd = (spark.sparkContext
           .parallelize(payload, numSlices=partitions)
           .mapPartitions(make_partition_parser(options, stats_accumulator, failure_accumulator)))

    try:
        dataframe = spark.createDataFrame(rdd, schema=build_schema())
    except Exception as exc:                           # noqa: BLE001 - diagnosed, then re-raised
        from check_environment import describe_serialization_failure, is_serialization_failure

        if is_serialization_failure(exc):
            raise SparkSerializationError(describe_serialization_failure(exc)) from exc
        raise
    return dataframe, stats_accumulator, failure_accumulator
