"""
Environment preflight, and the serialization shape the executors require.

The failure these tests protect against: a PySpark whose bundled cloudpickle is
older than the interpreter cannot serialise Python *closures* (it fails with
``IndexError: tuple index out of range``), while module level functions, which
are pickled by reference, still work. Everything the ETL ships to the executors
is therefore defined at module level - and the preflight tells the operator when
the installation itself cannot ship Python code at all.
"""

from __future__ import annotations

import functools
import os
import pickle
import sys

import pytest

from check_environment import (EnvironmentError_, check_code_serialization,
                               describe_environment, describe_serialization_failure,
                               is_serialization_failure, pyspark_supports_python,
                               verify_python_serialization)


# --------------------------------------------------------------------------- #
# Version matrix
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize("python_version,pyspark_version,expected", [
    ((3, 11), "3.3.1", False),          # the reported failure
    ((3, 11), "3.2.4", False),
    ((3, 11), "3.4.0", True),
    ((3, 11), "3.5.3", True),
    ((3, 11), "4.0.0", True),
    ((3, 10), "3.3.1", True),
    ((3, 12), "3.4.1", False),
    ((3, 12), "3.5.0", True),
    ((3, 9), "3.1.2", True),
])
def test_pyspark_supports_python(python_version, pyspark_version, expected):
    assert pyspark_supports_python(pyspark_version, python_version) is expected


def test_support_is_undecided_without_a_version():
    assert pyspark_supports_python("", (3, 11)) is None          # pyspark not importable
    assert pyspark_supports_python("not-a-version", (3, 11)) is None
    assert pyspark_supports_python("3.5.0", (4, 2)) is None      # Python not in the table


def test_support_defaults_to_the_installed_versions():
    pytest.importorskip("pyspark", reason="pyspark is not installed")
    assert pyspark_supports_python() is True                     # this environment is fine


# --------------------------------------------------------------------------- #
# Facts and checks
# --------------------------------------------------------------------------- #


def test_describe_environment_reports_the_versions():
    facts = describe_environment()
    assert facts["python_version"].startswith("3.")
    assert facts["python_executable"]
    assert "java_version" in facts
    assert not str(facts["java_version"]).startswith("Picked up")


@pytest.mark.spark
def test_code_serialization_works_here():
    pytest.importorskip("pyspark", reason="pyspark is not installed")
    ok, detail = check_code_serialization()
    assert ok, detail
    assert verify_python_serialization() is True


def test_serialization_failures_are_recognised():
    assert is_serialization_failure(
        pickle.PicklingError("Could not serialize object: IndexError: tuple index out of range"))
    assert is_serialization_failure(RuntimeError("TypeError: code() argument 13 must be str"))
    assert not is_serialization_failure(ValueError("no such file"))

    # also when it is wrapped in another exception
    try:
        try:
            raise pickle.PicklingError("Could not serialize object: IndexError: tuple index "
                                       "out of range")
        except pickle.PicklingError as inner:
            raise RuntimeError("batch failed") from inner
    except RuntimeError as outer:
        assert is_serialization_failure(outer)


def test_failure_description_names_the_remedy(monkeypatch):
    import check_environment

    monkeypatch.setattr(check_environment, "describe_environment", lambda: {
        "python_version": "3.11.5", "python_executable": "/venv/bin/python3",
        "pyspark_version": "3.3.1", "pyspark_path": "/venv/lib/python3.11/site-packages/pyspark",
        "cloudpickle_version": "2.0.0", "java_version": "openjdk 11", "spark_home": "",
    })
    monkeypatch.setattr(check_environment.sys, "version_info", (3, 11, 5))

    message = check_environment.describe_serialization_failure(
        RuntimeError("IndexError: tuple index out of range"))

    assert "pyspark     : 3.3.1" in message
    assert "cloudpickle : 2.0.0" in message
    assert "does not support Python 3.11.5" in message
    assert 'pip install "pyspark>=3.4"' in message
    assert "sc.parallelize" in message                 # it is not this ETL's bug


def test_verify_raises_when_serialization_is_broken(monkeypatch):
    import check_environment

    monkeypatch.setattr(check_environment, "check_code_serialization",
                        lambda: (False, "IndexError: tuple index out of range"))
    with pytest.raises(EnvironmentError_, match="could not serialise the Python code"):
        verify_python_serialization()
    assert verify_python_serialization(raise_on_failure=False) is False


# --------------------------------------------------------------------------- #
# What the ETL ships to the executors
# --------------------------------------------------------------------------- #


def test_everything_shipped_to_the_executors_is_module_level():
    """A ``<locals>`` in the qualname means cloudpickle serialises it by value."""
    import spark_parser

    shipped = [spark_parser.parse_partition, spark_parser.parse_journal_file,
               spark_parser.record_to_row, spark_parser.DictAccumulatorParam,
               spark_parser.ListAccumulatorParam]
    for obj in shipped:
        assert "<locals>" not in obj.__qualname__, f"{obj.__qualname__} is not module level"
        assert getattr(sys.modules[obj.__module__], obj.__qualname__) is obj


def test_partition_parser_is_a_partial_of_a_module_level_function():
    import spark_parser

    worker = spark_parser.make_partition_parser({"encoding": "latin-1"}, None, None)
    assert isinstance(worker, functools.partial)
    assert worker.func is spark_parser.parse_partition
    # stdlib pickle can only do this by reference - which is the point
    assert pickle.loads(pickle.dumps(worker)).func is spark_parser.parse_partition


def test_partition_parser_still_parses(tmp_path):
    import fixtures
    import spark_parser
    from datetime import datetime

    path = str(tmp_path / "ATM001" / "EJ.TXT")
    fixtures.write_journal(path, transactions=2)
    options = {"encoding": "latin-1", "keep_last_failure": True,
               "link_failed_across_amounts": False, "retry_window_seconds": 180,
               "keep_unparsed_records": True, "batch_id": "BATCH_0001", "run_id": "RUN1",
               "etl_name": "atm_ejournal", "load_ts": datetime(2026, 9, 14)}

    worker = spark_parser.make_partition_parser(options, None, None)
    rows = list(worker(iter([(path, "ATM001", "ATM001/EJ.TXT")])))

    assert len(rows) == 2
    assert rows[0][spark_parser.COLUMN_ORDER.index("ATM_NO")] == "ATM001"


def test_accumulators_are_updated_by_the_worker(tmp_path):
    import fixtures
    import spark_parser
    from datetime import datetime

    class Accumulator:
        def __init__(self, value):
            self.value = value

        def add(self, other):
            if isinstance(self.value, dict):
                for key, amount in other.items():
                    self.value[key] = self.value.get(key, 0) + amount
            else:
                self.value.extend(other)

    stats, failures = Accumulator({}), Accumulator([])
    good = fixtures.write_journal(str(tmp_path / "ATM001" / "EJ.TXT"), transactions=1)
    missing = str(tmp_path / "ATM001" / "GONE.TXT")
    options = {"encoding": "latin-1", "batch_id": "B", "run_id": "R",
               "etl_name": "atm_ejournal", "load_ts": datetime(2026, 9, 14)}

    worker = spark_parser.make_partition_parser(options, stats, failures)
    rows = list(worker(iter([(good, "ATM001", "k1"), (missing, "ATM001", "k2")])))

    assert len(rows) == 1
    assert stats.value["files_processed"] == 1
    assert stats.value["files_failed"] == 1
    assert [entry["file_key"] for entry in failures.value] == ["k2"]


@pytest.mark.spark
def test_serialization_failure_is_reported_with_the_remedy(monkeypatch, spark, cfg, tmp_path):
    """A PicklingError from Spark becomes a message naming the cause and the fix."""
    import fixtures
    import spark_parser
    from file_registry import discover_files

    root = str(tmp_path / "ATM_EJOURNALS")
    fixtures.build_input_tree(root, atms=1, files_per_atm=1, transactions=1)
    files = list(discover_files(root, patterns=["*.TXT"]))

    def broken(*args, **kwargs):
        raise pickle.PicklingError("Could not serialize object: IndexError: tuple index "
                                   "out of range")
    monkeypatch.setattr(spark, "createDataFrame", broken)

    with pytest.raises(spark_parser.SparkSerializationError) as error:
        spark_parser.build_batch_dataframe(spark, files, cfg, "BATCH_0001", "RUN1")

    assert "Spark could not serialise the Python code" in str(error.value)


def test_preflight_failure_stops_the_run_with_a_named_stage(etl_home, config_path, monkeypatch):
    """The ETL fails fast, before a cluster application is started."""
    import check_environment
    import fixtures
    from config_loader import load_config
    from etl_runner import AtmEjournalEtl

    fixtures.build_input_tree(os.path.join(etl_home, "ATM_EJOURNALS"),
                              atms=1, files_per_atm=1, transactions=1)
    monkeypatch.setattr(check_environment, "check_code_serialization",
                        lambda: (False, "IndexError: tuple index out of range"))

    summary = AtmEjournalEtl(load_config(config_path, "atm_ejournal"), run_id="ENV1").run()

    assert summary.status == "FAILED"
    assert summary.batches[0]["stage"] == "environment_check"
    assert "could not serialise the Python code" in summary.batches[0]["error"]
    # nothing was marked as processed
    assert not os.path.exists(os.path.join(etl_home, "processed", "processed_files.csv")) or \
        open(os.path.join(etl_home, "processed", "processed_files.csv")).read().count("\n") <= 1
