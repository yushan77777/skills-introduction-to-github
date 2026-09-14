"""
Local parse engine: journals -> parquet without shipping Python code to Spark.

Why this engine exists
----------------------
PySpark serialises every Python function it sends to the executors with the
cloudpickle version it bundles. On PySpark 3.1.x (cloudpickle 1.6) running under
Python 3.11 that serialisation cannot work at all - ``rdd.mapPartitions``,
``createDataFrame`` and Python UDFs all fail on the driver with
``PicklingError: ... IndexError: tuple index out of range``. Only work that stays
inside the JVM survives.

So this engine keeps Python out of Spark entirely:

    journal files --(existing parser, on this host)--> rows
                  --(PyArrow)--> parquet/batch_nnnn
                  --(spark.read.parquet, JVM only)--> Greenplum JDBC write

Spark is still used for what it is good at here - reading the parquet and the
bulk JDBC write - and neither of those ships a single byte of Python code.

Memory
------
Rows are streamed into the parquet file in chunks
(``parser.PARSE_WRITE_CHUNK_RECORDS``), so a batch never has to fit in memory:
the footprint is one chunk plus one journal file being parsed, whatever
``BATCH_SIZE`` is.

Parallelism
-----------
Files are parsed by a process pool (``parser.PARSE_WORKERS``), which uses the
standard library's pickle on a module level function - not cloudpickle - and so
works on any Python/PySpark combination. ``PARSE_WORKERS: 1`` parses in-process,
which is also the automatic fallback when a pool cannot be started.
"""

from __future__ import annotations

import logging
import os
import time
from concurrent.futures import ProcessPoolExecutor
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, Iterable, Iterator, List, Optional, Tuple

from spark_parser import COLUMN_ORDER, SCHEMA_FIELDS, STAT_KEYS, parse_journal_file

logger = logging.getLogger("atm_ejournal.local_parse")

#: Written into the parquet so a batch can be traced back to the engine.
ENGINE_NAME = "local"


class LocalParseError(Exception):
    """Raised when a batch cannot be parsed or written locally."""


@dataclass
class LocalParseResult:
    """What a locally parsed batch produced."""

    parquet_path: str
    record_count: int = 0
    file_count: int = 0
    duration_seconds: float = 0.0
    stats: Dict[str, int] = field(default_factory=dict)
    failures: List[Dict[str, str]] = field(default_factory=list)

    def as_dict(self) -> Dict[str, Any]:
        return self.__dict__.copy()


# --------------------------------------------------------------------------- #
# Schema
# --------------------------------------------------------------------------- #


def arrow_schema():
    """The output schema as a PyArrow schema, built from the same description."""
    import pyarrow as pa                                # noqa: PLC0415

    arrow_types = {
        "string": pa.string(),
        "boolean": pa.bool_(),
        "int": pa.int32(),
        "long": pa.int64(),
        "double": pa.float64(),
        "date": pa.date32(),
        # microseconds: what Spark reads back as a TIMESTAMP without conversion
        "timestamp": pa.timestamp("us"),
    }
    return pa.schema([pa.field(name, arrow_types[kind], nullable=True)
                      for name, kind in SCHEMA_FIELDS])


def require_pyarrow():
    """Import PyArrow with an actionable message when it is missing."""
    try:
        import pyarrow                                  # noqa: PLC0415
        import pyarrow.parquet                          # noqa: PLC0415, F401
        return pyarrow
    except ImportError as exc:                          # pragma: no cover - env specific
        raise LocalParseError(
            "the local parse engine writes parquet with PyArrow, which is not installed. "
            "Install it in the ETL virtualenv:  pip install pyarrow   "
            "(or set parser.PARSE_ENGINE: spark to parse on the cluster instead)") from exc


# --------------------------------------------------------------------------- #
# Parsing
# --------------------------------------------------------------------------- #


def parse_one(task: Tuple[str, str, str, Dict[str, Any]]):
    """
    Parse a single file. Module level and taking one tuple, so a process pool can
    ship it with the standard library's pickle (by reference, no cloudpickle).
    """
    path, atm_no, file_key, options = task
    rows, stats, error = parse_journal_file(path, atm_no, file_key, options)
    return path, file_key, rows, stats, error


def _iter_parsed(tasks: List[Tuple[str, str, str, Dict[str, Any]]],
                 workers: int) -> Iterator[Tuple[str, str, List[Tuple], Dict[str, int],
                                                 Optional[str]]]:
    """Parse the batch, in a process pool when one can be started."""
    if workers <= 1 or len(tasks) <= 1:
        for task in tasks:
            yield parse_one(task)
        return
    try:
        with ProcessPoolExecutor(max_workers=workers) as pool:
            # chunksize 1: journal files differ wildly in size, so hand them out
            # one at a time instead of pre-splitting the list evenly.
            for outcome in pool.map(parse_one, tasks, chunksize=1):
                yield outcome
    except Exception as exc:                            # noqa: BLE001 - degraded, not fatal
        logger.warning("process pool unavailable (%s: %s) - parsing in this process instead",
                       type(exc).__name__, exc)
        for task in tasks:
            yield parse_one(task)


def build_options(cfg, batch_id: str, run_id: str) -> Dict[str, Any]:
    """Parser options for one batch - plain data, identical to the Spark engine."""
    return {
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


def resolve_workers(cfg, file_count: int) -> int:
    """``PARSE_WORKERS`` (0 = one per core), never more than the files in the batch."""
    configured = cfg.get_int("parser.PARSE_WORKERS", 0)
    if configured <= 0:
        configured = os.cpu_count() or 1
    return max(1, min(configured, max(file_count, 1)))


# --------------------------------------------------------------------------- #
# Writing
# --------------------------------------------------------------------------- #


def _rows_to_arrow_table(rows: List[Tuple], schema):
    """Columnar conversion of a chunk of rows (no pandas dependency)."""
    import pyarrow as pa                                # noqa: PLC0415

    columns = [[row[index] for row in rows] for index in range(len(COLUMN_ORDER))]
    return pa.Table.from_arrays(
        [pa.array(column, type=schema.field(index).type)
         for index, column in enumerate(columns)],
        schema=schema)


def write_batch_parquet(files: Iterable[Any], cfg, batch_id: str, run_id: str,
                        parquet_path: str) -> LocalParseResult:
    """
    Parse a batch on this host and stream it into ``parquet_path``.

    The directory is written the way Spark writes one - a ``part-*.parquet`` file
    plus an empty ``_SUCCESS`` marker - so everything downstream (validation,
    ``spark.read.parquet``, the Greenplum load, cleanup) is unchanged.
    """
    pyarrow = require_pyarrow()
    import pyarrow.parquet as parquet                   # noqa: PLC0415

    files = list(files)
    key_mode = str(cfg.get("tracking.FILE_KEY_MODE", "path"))
    options = build_options(cfg, batch_id, run_id)
    workers = resolve_workers(cfg, len(files))
    chunk_records = max(1, cfg.get_int("parser.PARSE_WRITE_CHUNK_RECORDS", 50000))
    compression = str(cfg.get("parquet.PARQUET_COMPRESSION", "snappy"))

    tasks = [(item.path, item.atm_no, item.key(key_mode), options) for item in files]
    schema = arrow_schema()
    result = LocalParseResult(parquet_path=parquet_path, file_count=len(files),
                              stats={key: 0 for key in STAT_KEYS})
    started = time.time()

    os.makedirs(parquet_path, exist_ok=True)
    data_file = os.path.join(parquet_path, f"part-00000-{batch_id.lower()}.parquet")
    logger.info("local parse started | batch=%s | files=%d | workers=%d | chunk=%d records",
                batch_id, len(files), workers, chunk_records)

    writer = None
    buffer: List[Tuple] = []

    def flush() -> None:
        nonlocal writer, buffer
        if not buffer:
            return
        if writer is None:
            writer = parquet.ParquetWriter(data_file, schema, compression=compression,
                                           # 'spark' keeps the file readable by every
                                           # Spark version without extra options
                                           flavor="spark")
        writer.write_table(_rows_to_arrow_table(buffer, schema))
        buffer = []

    try:
        for path, file_key, rows, stats, error in _iter_parsed(tasks, workers):
            for key, amount in (stats or {}).items():
                result.stats[key] = result.stats.get(key, 0) + amount
            if error:
                result.failures.append({"file_key": file_key, "path": path, "error": error})
                logger.error("file failed to parse | batch=%s | file=%s\n%s",
                             batch_id, path, error)
                continue
            buffer.extend(rows)
            result.record_count += len(rows)
            if len(buffer) >= chunk_records:
                flush()
        flush()
    except Exception as exc:                            # noqa: BLE001
        raise LocalParseError(f"local parse failed for {batch_id}: {exc}") from exc
    finally:
        if writer is not None:
            writer.close()

    if writer is None:
        # No rows at all: write an empty file so the schema is still readable.
        parquet.write_table(pyarrow.Table.from_batches([], schema=schema), data_file,
                            compression=compression, flavor="spark")

    # The commit marker Spark writes, so validation and cleanup treat the two
    # engines identically.
    with open(os.path.join(parquet_path, "_SUCCESS"), "w", encoding="utf-8"):
        pass

    result.duration_seconds = time.time() - started
    logger.info("local parse completed | batch=%s | records=%d | failed files=%d | %.1fs",
                batch_id, result.record_count, len(result.failures), result.duration_seconds)
    return result


def count_parquet_rows(parquet_path: str) -> int:
    """Row count straight from the parquet footers - no Spark, no full read."""
    require_pyarrow()
    import pyarrow.parquet as parquet                   # noqa: PLC0415

    if not os.path.isdir(parquet_path):
        raise LocalParseError(f"parquet directory missing: {parquet_path}")
    total = 0
    found = False
    for name in sorted(os.listdir(parquet_path)):
        if not name.endswith(".parquet"):
            continue
        found = True
        total += parquet.ParquetFile(os.path.join(parquet_path, name)).metadata.num_rows
    if not found:
        raise LocalParseError(f"no parquet data file in {parquet_path}")
    return total
