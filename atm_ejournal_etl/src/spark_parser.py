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


#: Note values that get their own ``NOTES_<value>`` column (the bill-wise
#: breakdown), overridable with ``parser.NOTE_DENOMINATIONS``.
DEFAULT_NOTE_VALUES = (5000, 2000, 1000, 500, 100, 50, 20)

#: The output schema, described once and built for both engines:
#: :func:`build_schema` (Spark ``StructType``) and
#: :func:`local_parser.arrow_schema` (PyArrow). Keeping one description means the
#: parquet a batch produces is identical whichever engine wrote it.
BASE_SCHEMA_FIELDS = [
    ("ATM_NO", "string"),
    ("TRANSACTION_DATETIME", "timestamp"),
    ("DATE", "date"),
    ("TIME", "string"),
    ("RESPONSE_DATETIME", "timestamp"),
    ("ACCOUNT_NO", "string"),
    ("CARD_NO", "string"),
    ("AMOUNT", "double"),
    ("REQUESTED_AMOUNT", "double"),
    ("CURRENCY", "string"),
    ("STATUS", "string"),
    ("RESPONSE_CODE", "string"),
    ("ACTION_CODE", "string"),
    ("TRANSACTION_REF", "string"),
    ("AUX_SEQ", "string"),
    ("TRACE_ID", "string"),
    ("TERMINAL_ID", "string"),
    ("CARD_SCHEME", "string"),
    ("FAST_CASH", "boolean"),
    ("DISPENSE_RESULT", "string"),
    ("DISPENSED_AMOUNT", "double"),
    ("DENOMINATION", "string"),
    ("DENOM_BREAKDOWN", "string"),
    ("NOTES_COUNT", "int"),
    ("DENOM_AMOUNT", "double"),
    ("DENOM_MATCHES_AMOUNT", "boolean"),
    ("PLANNED_DENOMINATION", "string"),
    ("MIX_NUMBER", "string"),
    ("CASH_TAKEN", "boolean"),
    ("TRX_ERROR", "string"),
    ("ATTEMPT_NO", "int"),
    ("ATTEMPT_COUNT", "int"),
    ("IS_RETRY", "boolean"),
    ("SESSION_ID", "string"),
    ("TXN_SEQ", "int"),
    ("PARSE_CONFIDENT", "boolean"),
    ("RAW_BLOCK", "string"),
    ("SOURCE_FILE", "string"),
    ("SOURCE_PATH", "string"),
    ("SOURCE_FILE_KEY", "string"),
    ("SOURCE_LINE", "long"),
    ("BATCH_ID", "string"),
    ("ETL_RUN_ID", "string"),
    ("ETL_NAME", "string"),
    ("LOAD_TS", "timestamp"),
]


def note_column(value: int) -> str:
    """``5000`` -> ``NOTES_5000``."""
    return f"NOTES_{int(value)}"


def normalise_note_values(note_values=None) -> List[int]:
    """Clean, de-duplicate and sort a configured note list (highest first)."""
    if note_values is None:
        return list(DEFAULT_NOTE_VALUES)
    values = []
    for value in note_values:
        try:
            number = int(str(value).strip())
        except (TypeError, ValueError):
            continue
        if number > 0 and number not in values:
            values.append(number)
    return sorted(values, reverse=True) or list(DEFAULT_NOTE_VALUES)


def schema_fields(note_values=None) -> List[Tuple[str, str]]:
    """
    The full field list: the base columns, then one ``NOTES_<value>`` column per
    configured note plus ``NOTES_OTHER`` for anything the ATM dispensed that is
    not in the list - so a denomination is never silently dropped.
    """
    values = normalise_note_values(note_values)
    notes = [(note_column(value), "int") for value in values] + [("NOTES_OTHER", "int")]
    # inserted right after DENOM_AMOUNT, where the pandas parser put them
    anchor = [name for name, _ in BASE_SCHEMA_FIELDS].index("DENOM_MATCHES_AMOUNT")
    return BASE_SCHEMA_FIELDS[:anchor] + notes + BASE_SCHEMA_FIELDS[anchor:]


#: Field list with the default note values - what the module level
#: ``COLUMN_ORDER`` describes.
SCHEMA_FIELDS = schema_fields()


def build_schema(note_values=None):
    """The output schema as a Spark ``StructType`` (deferred pyspark import)."""
    from pyspark.sql.types import (BooleanType, DateType, DoubleType, IntegerType,
                                   LongType, StringType, StructField, StructType,
                                   TimestampType)

    spark_types = {
        "string": StringType(),
        "boolean": BooleanType(),
        "int": IntegerType(),
        "long": LongType(),
        "double": DoubleType(),
        "date": DateType(),
        "timestamp": TimestampType(),
    }
    return StructType([StructField(name, spark_types[kind], True)
                       for name, kind in schema_fields(note_values)])


def column_order(note_values=None) -> List[str]:
    """Column order of the rows produced by :func:`parse_journal_file`."""
    return [name for name, _kind in schema_fields(note_values)]


#: Column order with the default note values.
COLUMN_ORDER = column_order()

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


def breakdown_counts(breakdown: Any, note_values: List[int]) -> Dict[Any, Optional[int]]:
    """
    ``{5000: 9, 1000: 3}`` -> ``{5000: 9, 1000: 3, 500: 0, ..., "OTHER": 0}``.

    ``None`` for every bucket when the record has no breakdown at all (a failed
    withdrawal dispensed nothing), so "no notes" stays distinguishable from
    "zero notes of this value".
    """
    if not isinstance(breakdown, dict) or not breakdown:
        return {value: None for value in note_values} | {"OTHER": None}

    counts: Dict[Any, Optional[int]] = {value: 0 for value in note_values}
    other = 0
    for raw_value, raw_count in breakdown.items():
        try:
            value, count = int(raw_value), int(raw_count)
        except (TypeError, ValueError):
            continue
        if value in counts:
            counts[value] = (counts[value] or 0) + count
        else:
            other += count
    counts["OTHER"] = other
    return counts


def record_to_row(record: Dict[str, Any],
                  file_key: str,
                  batch_id: str,
                  run_id: str,
                  etl_name: str,
                  load_ts: datetime,
                  note_values=None) -> Tuple:
    """
    Map one parser record onto the column order.

    ``DENOM_BREAKDOWN`` (``{5000: 9, 1000: 3}``) becomes both the JSON column and
    one count per configured note value - the bill-wise breakdown the pandas
    parser produced as ``NOTES_5000``, ``NOTES_1000``, ... Notes outside the
    configured list are summed into ``NOTES_OTHER``.
    """
    values = normalise_note_values(note_values)
    counts = breakdown_counts(record.get("DENOM_BREAKDOWN"), values)
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
        *(counts[value] for value in values),
        counts["OTHER"],
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
                              options["etl_name"], load_ts, options.get("note_values"))
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
        "note_values": normalise_note_values(cfg.get_list("parser.NOTE_DENOMINATIONS")
                                             or None),
    }

    key_mode = str(cfg.get("tracking.FILE_KEY_MODE", "path"))
    payload = [(item.path, item.atm_no, item.key(key_mode)) for item in files]
    partitions = num_partitions or max(1, min(len(payload), 8))

    stats_accumulator, failure_accumulator = build_accumulators(spark)
    rdd = (spark.sparkContext
           .parallelize(payload, numSlices=partitions)
           .mapPartitions(make_partition_parser(options, stats_accumulator, failure_accumulator)))

    try:
        dataframe = spark.createDataFrame(rdd, schema=build_schema(options["note_values"]))
    except Exception as exc:                           # noqa: BLE001 - diagnosed, then re-raised
        from check_environment import describe_serialization_failure, is_serialization_failure

        if is_serialization_failure(exc):
            raise SparkSerializationError(describe_serialization_failure(exc)) from exc
        raise
    return dataframe, stats_accumulator, failure_accumulator
