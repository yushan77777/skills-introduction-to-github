"""
The local parse engine: journals -> parquet on this host, no Python sent to Spark.

This is the engine that works on a PySpark whose cloudpickle cannot serialise the
interpreter's byte code (for example Spark 3.1.3 with Python 3.11), so most of
these tests deliberately need no SparkSession at all.
"""

from __future__ import annotations

import json
import os
import pickle

import pytest

import fixtures
from file_registry import discover_files
from local_parser import (LocalParseError, arrow_schema, build_options, count_parquet_rows,
                          parse_one, resolve_workers, write_batch_parquet)
from spark_parser import COLUMN_ORDER, SCHEMA_FIELDS

pyarrow = pytest.importorskip("pyarrow", reason="pyarrow is not installed")
import pyarrow.parquet as parquet_reader                              # noqa: E402


def _files(root):
    return list(discover_files(root, patterns=["*.TXT"]))


@pytest.fixture
def local_cfg(etl_home, input_tree):
    from config_loader import load_config

    return load_config(fixtures.write_config(etl_home, batch_size=10, parse_engine="local"),
                       "atm_ejournal")


# --------------------------------------------------------------------------- #
# Schema
# --------------------------------------------------------------------------- #


def test_arrow_schema_matches_the_column_order():
    schema = arrow_schema()
    assert schema.names == COLUMN_ORDER
    assert len(schema) == len(SCHEMA_FIELDS)


@pytest.mark.spark
def test_arrow_and_spark_schemas_agree():
    """One description, two engines - the parquet must be identical either way."""
    pytest.importorskip("pyspark", reason="pyspark is not installed")
    from spark_parser import build_schema

    spark_fields = [(field.name, type(field.dataType).__name__) for field in build_schema().fields]
    expected = {
        "string": "StringType", "boolean": "BooleanType", "int": "IntegerType",
        "long": "LongType", "double": "DoubleType", "date": "DateType",
        "timestamp": "TimestampType",
    }
    assert spark_fields == [(name, expected[kind]) for name, kind in SCHEMA_FIELDS]


# --------------------------------------------------------------------------- #
# Parsing and writing
# --------------------------------------------------------------------------- #


def test_writes_a_spark_shaped_parquet_directory(local_cfg, etl_home, tmp_path):
    files = _files(os.path.join(etl_home, "ATM_EJOURNALS"))
    target = str(tmp_path / "batch_0001")

    result = write_batch_parquet(files, local_cfg, "BATCH_0001", "RUN1", target)

    assert result.record_count == 8                    # 4 files x 2 withdrawals
    assert result.failures == []
    assert os.path.isfile(os.path.join(target, "_SUCCESS"))
    assert any(name.endswith(".parquet") for name in os.listdir(target))
    assert count_parquet_rows(target) == 8

    table = parquet_reader.read_table(target)
    assert table.schema.names == COLUMN_ORDER
    row = {name: table.column(name)[0].as_py() for name in COLUMN_ORDER}
    assert row["ATM_NO"] == "ATM001"
    assert row["AMOUNT"] == 50000.0
    assert row["STATUS"] == "SUCCESS"
    assert row["DENOMINATION"] == "5000x10"
    assert row["NOTES_COUNT"] == 10
    assert row["TIME"] == "06:42:30"
    assert row["BATCH_ID"] == "BATCH_0001"
    assert row["ETL_RUN_ID"] == "RUN1"
    assert json.loads(row["DENOM_BREAKDOWN"]) == {"5000": 10}


def test_chunked_writing_keeps_memory_bounded(local_cfg, etl_home, tmp_path):
    """A small chunk size must produce several row groups, not several files."""
    local_cfg._data["parser"]["PARSE_WRITE_CHUNK_RECORDS"] = 2          # noqa: SLF001
    files = _files(os.path.join(etl_home, "ATM_EJOURNALS"))
    target = str(tmp_path / "batch_chunks")

    result = write_batch_parquet(files, local_cfg, "BATCH_0001", "RUN1", target)

    data_files = [name for name in os.listdir(target) if name.endswith(".parquet")]
    assert len(data_files) == 1
    metadata = parquet_reader.ParquetFile(os.path.join(target, data_files[0])).metadata
    assert metadata.num_rows == result.record_count == 8
    assert metadata.num_row_groups > 1                  # written incrementally


def test_a_failed_file_is_reported_and_the_rest_still_land(local_cfg, etl_home, tmp_path):
    files = _files(os.path.join(etl_home, "ATM_EJOURNALS"))
    missing = files[0]
    os.remove(missing.path)                             # vanished after discovery
    target = str(tmp_path / "batch_partial")

    result = write_batch_parquet(files, local_cfg, "BATCH_0001", "RUN1", target)

    assert len(result.failures) == 1
    assert result.failures[0]["file_key"] == missing.key("path")
    assert result.record_count == 6                     # the other three files
    assert count_parquet_rows(target) == 6
    assert result.stats["files_failed"] == 1
    assert result.stats["files_processed"] == 3


def test_empty_batch_still_writes_a_readable_parquet(local_cfg, tmp_path):
    target = str(tmp_path / "batch_empty")

    result = write_batch_parquet([], local_cfg, "BATCH_0001", "RUN1", target)

    assert result.record_count == 0
    assert count_parquet_rows(target) == 0
    assert parquet_reader.read_table(target).schema.names == COLUMN_ORDER


def test_parse_one_is_picklable_by_the_standard_library():
    """The process pool must not need cloudpickle - that is the whole point."""
    assert "<locals>" not in parse_one.__qualname__
    assert pickle.loads(pickle.dumps(parse_one)) is parse_one


def test_worker_count_is_bounded_by_the_batch(local_cfg):
    local_cfg._data["parser"]["PARSE_WORKERS"] = 8                      # noqa: SLF001
    assert resolve_workers(local_cfg, 3) == 3
    assert resolve_workers(local_cfg, 100) == 8
    local_cfg._data["parser"]["PARSE_WORKERS"] = 0                      # noqa: SLF001
    assert resolve_workers(local_cfg, 100) == (os.cpu_count() or 1)


def test_parsing_falls_back_to_this_process_when_no_pool_can_start(local_cfg, etl_home,
                                                                   tmp_path, monkeypatch, caplog):
    import local_parser

    local_cfg._data["parser"]["PARSE_WORKERS"] = 4                      # noqa: SLF001

    class RefusingPool:
        def __init__(self, *args, **kwargs):
            raise OSError("cannot fork")

    monkeypatch.setattr(local_parser, "ProcessPoolExecutor", RefusingPool)
    files = _files(os.path.join(etl_home, "ATM_EJOURNALS"))

    result = write_batch_parquet(files, local_cfg, "BATCH_0001", "RUN1",
                                 str(tmp_path / "batch_fallback"))

    assert result.record_count == 8
    assert "parsing in this process instead" in caplog.text


def test_multiprocess_parsing_produces_the_same_rows(local_cfg, etl_home, tmp_path):
    files = _files(os.path.join(etl_home, "ATM_EJOURNALS"))
    local_cfg._data["parser"]["PARSE_WORKERS"] = 1                      # noqa: SLF001
    single = write_batch_parquet(files, local_cfg, "B", "RUN1", str(tmp_path / "single"))
    local_cfg._data["parser"]["PARSE_WORKERS"] = 3                      # noqa: SLF001
    pooled = write_batch_parquet(files, local_cfg, "B", "RUN1", str(tmp_path / "pooled"))

    assert single.record_count == pooled.record_count
    columns = ["ATM_NO", "TRANSACTION_DATETIME", "AMOUNT", "STATUS", "SOURCE_FILE"]
    as_rows = lambda path: sorted(                                      # noqa: E731
        map(str, parquet_reader.read_table(path, columns=columns).to_pylist()))
    assert as_rows(str(tmp_path / "single")) == as_rows(str(tmp_path / "pooled"))


def test_options_match_the_spark_engine(local_cfg):
    options = build_options(local_cfg, "BATCH_0001", "RUN1")
    assert options["encoding"] == "latin-1"
    assert options["batch_id"] == "BATCH_0001"
    assert options["run_id"] == "RUN1"
    assert options["etl_name"] == "atm_ejournal"
    assert set(options) >= {"keep_last_failure", "link_failed_across_amounts",
                            "retry_window_seconds", "keep_unparsed_records", "load_ts"}


def test_count_rows_reports_a_missing_or_empty_directory(tmp_path):
    with pytest.raises(LocalParseError, match="parquet directory missing"):
        count_parquet_rows(str(tmp_path / "nope"))
    (tmp_path / "empty").mkdir()
    with pytest.raises(LocalParseError, match="no parquet data file"):
        count_parquet_rows(str(tmp_path / "empty"))


def test_missing_pyarrow_is_reported_with_the_remedy(monkeypatch):
    import builtins

    import local_parser

    real_import = builtins.__import__

    def refuse(name, *args, **kwargs):
        if name.startswith("pyarrow"):
            raise ImportError("no module named pyarrow")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", refuse)
    with pytest.raises(LocalParseError, match="pip install pyarrow"):
        local_parser.require_pyarrow()
